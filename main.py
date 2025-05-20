import streamlit as st
import pandas as pd
import io
import matplotlib.pyplot as plt
import json
import os
import streamlit as st
import streamlit_authenticator as stauth

# Controle de acesso

# Aqui começa o conteúdo da sua aplicação

# --- Funções auxiliares ---
def limpar_valores(texto):
    return texto.replace(",", "")

def carregar_dados(dados_colados):
    dados = pd.read_csv(io.StringIO(limpar_valores(dados_colados)), sep="\t")
    dados["Datetime"] = pd.to_datetime(dados["Date"] + " " + dados["Time"], dayfirst=True)
    dados = dados.sort_values("Datetime")

    colunas_originais = [
        "MM_MPTF_QGBT-03_KWH.PresentValue", "MM_GAHO_QLFE-01-01_KWH.PresentValue",
        "MM_MAIW_QGBT-GERAL_KWH.PresentValue", "MM_MPTF_QGBT-01_KWH.PresentValue",
        "MM_MPTF_QGBT-02_KWH.PresentValue", "MM_MPTF_CEAG_KWH.PresentValue",
        "MM_SEOB_QGBT-01-01_KWH.PresentValue", "MM_OFFI_QGBT-01_KWH.PresentValue",
        "MM_EBPC_QLF-01-01_KWH.PresentValue", "KWH_PCCB_SEPAM-S40-01.PresentValue",
        "MM_OFFI_QGBT-01-02_KWH.PresentValue"
    ]

    novos_rotulos = {
        "MM_MPTF_QGBT-03_KWH.PresentValue": "MP&L",
        "MM_GAHO_QLFE-01-01_KWH.PresentValue": "GAHO",
        "MM_MAIW_QGBT-GERAL_KWH.PresentValue": "MAIW",
        "MM_MPTF_QGBT-01_KWH.PresentValue": "QGBT1-MPTF",
        "MM_MPTF_QGBT-02_KWH.PresentValue": "QGBT2-MPTF",
        "MM_MPTF_CEAG_KWH.PresentValue": "CAG",
        "MM_SEOB_QGBT-01-01_KWH.PresentValue": "SEOB",
        "MM_OFFI_QGBT-01_KWH.PresentValue": "OFFICE",
        "MM_EBPC_QLF-01-01_KWH.PresentValue": "EBPC",
        "KWH_PCCB_SEPAM-S40-01.PresentValue": "PCCB",
        "MM_OFFI_QGBT-01-02_KWH.PresentValue": "PMDC-OFFICE"
    }

    dados = dados.rename(columns=novos_rotulos)
    medidores = list(novos_rotulos.values())
    dados[medidores] = dados[medidores].astype(float)

    consumo = dados[["Datetime"] + medidores].copy()
    for col in medidores:
        consumo[col] = consumo[col].diff().abs()
    consumo = consumo.dropna()

    consumo["TRIM&FINAL"] = consumo["QGBT1-MPTF"] + consumo["QGBT2-MPTF"]
    consumo["OFFICE + CANTEEN"] = consumo["OFFICE"] - consumo["PMDC-OFFICE"]
    consumo = consumo.drop(columns=["QGBT1-MPTF", "QGBT2-MPTF"])

    return consumo

# --- Menu lateral ---
pagina = st.sidebar.selectbox("Escolha a página:", [
    "Home", "Graphs by Meter", "Consumption Limits", "Dashboard"
])
st.markdown("<div style='font-size:20px; text-align:left;'>Application for managing electricity consumption Brazil- JLR (kWh)</div>", unsafe_allow_html=True)
#st.title("Aplicativo para gestão de consumo de energia elétrica - JLR (kWh)")

dados_colados = st.text_area("Paste the data here (tabulated):", height=300)

if dados_colados:
    try:
        consumo = carregar_dados(dados_colados)

        datas_disponiveis = consumo["Datetime"].dt.date.unique()
        data_selecionada = st.selectbox("Selecione a data", sorted(datas_disponiveis, reverse=True))
        dados_dia = consumo[consumo["Datetime"].dt.date == data_selecionada]
        
        

        if dados_dia.empty:
            st.warning("Nenhum dado disponível para a data selecionada.")
            st.stop()

        horas = dados_dia["Datetime"].dt.hour
        medidores_disponiveis = [col for col in dados_dia.columns if col != "Datetime"]

        # Página 1 - Principal
        if pagina == "Home":
            medidores_selecionados = st.multiselect("Select the meters:", medidores_disponiveis, default=medidores_disponiveis)

            fig, ax = plt.subplots(figsize=(16, 6))
            for medidor in medidores_selecionados:
                ax.plot(horas, dados_dia[medidor], label=medidor)

                if "limites_por_medidor" in st.session_state and medidor in st.session_state.limites_por_medidor:
                    ax.plot(range(24), st.session_state.limites_por_medidor[medidor], linestyle="--", color="red", label=f"Limite - {medidor}")

            ax.set_title(f"Hourly consumption in {data_selecionada} (kWh)")
            ax.set_xlabel("Hora do dia")
            ax.set_ylabel("Consumo (kWh)")
            ax.set_xticks(range(0, 24))
            ax.legend(loc="lower center", bbox_to_anchor=(0.5, -0.4), ncol=3, fontsize='small')
            plt.xticks(rotation=45)
            st.pyplot(fig)

            st.markdown("### Hourly consumption (kWh)")
            st.dataframe(
                dados_dia.set_index("Datetime")[medidores_selecionados].round(2).style.set_properties(**{"text-align": "center"}),
                use_container_width=True
            )

            st.markdown("### Total Consumption per Meter (kWh)")
            totais = dados_dia[medidores_disponiveis].sum().round(2).to_frame(name="Consumo Total (kWh)")
            st.dataframe(totais.style.set_properties(**{"text-align": "center"}), use_container_width=True)

        # Página 2 - Gráficos por Medidor
        elif pagina == "Graphs by Meter":
            st.markdown("### Individual Charts with Limit Curve")
            cores = plt.cm.get_cmap("tab10", len(medidores_disponiveis))
            for idx, medidor in enumerate(medidores_disponiveis):
                fig, ax = plt.subplots(figsize=(12, 4))
                ax.plot(horas, dados_dia[medidor], label="Consumo", color=cores(idx))

                limite_total = 0
                consumo_total = dados_dia[medidor].sum()

                if "limites_por_medidor" in st.session_state and medidor in st.session_state.limites_por_medidor:
                    limites = st.session_state.limites_por_medidor[medidor]
                    ax.plot(range(24), limites, label="Limite", linestyle="--", color="red")
                    limite_total = sum(limites)

                ax.set_title(f"{medidor} - Consumption per hour (kWh)")
                ax.set_xlabel("Time of day")
                ax.set_ylabel("Consumption (kWh)")
                ax.set_xticks(range(0, 24))
                ax.legend(fontsize='small')
                st.pyplot(fig)

                st.markdown(f"**Resume - {medidor}**")
                resumo_df = pd.DataFrame({
                    "Soma dos Limites (kWh)": [round(limite_total, 2)],
                    "Soma do Consumo (kWh)": [round(consumo_total, 2)]
                }, index=["Total"])

                def highlight_excesso(val):
                    consumo = resumo_df["Sum of Consumption (kWh)"].values[0]
                    limite = resumo_df["Sum of Limits (kWh)"].values[0]
                    return ["", "background-color: red; color: white"] if consumo > limite else ["", ""]

                styled = resumo_df.style.set_properties(**{"text-align": "center"}).apply(highlight_excesso, axis=1)
                st.dataframe(styled, use_container_width=True)

        # Página 3 - Configuração de Limites
        elif pagina == "Consumption Limits":
            st.markdown("### Configure the hourly limits for each meter")

            if "limites_por_medidor" not in st.session_state:
                st.session_state.limites_por_medidor = {m: [5.0]*24 for m in medidores_disponiveis}

            uploaded_file = st.file_uploader("Carregar limites a partir de um arquivo JSON", type="json")
            if uploaded_file is not None:
                try:
                    st.session_state.limites_por_medidor = json.load(uploaded_file)
                    st.success("Limites carregados com sucesso!")
                except Exception as e:
                    st.error(f"Erro ao carregar limites: {e}")

            for medidor in medidores_disponiveis:
                st.markdown(f"#### {medidor}")
                st.markdown(f"##### Limites por hora para {medidor}")
                cols = st.columns(6)
                novos_valores = []
                for i in range(24):
                    with cols[i % 6]:
                        valor = st.number_input(
                            f"{i}h", min_value=0.0, max_value=800.0,
                            value=float(st.session_state.limites_por_medidor.get(medidor, [5.0]*24)[i]),
                            step=0.5, key=f"{medidor}_{i}"
                        )
                        novos_valores.append(valor)
                st.session_state.limites_por_medidor[medidor] = novos_valores

            if st.button("Salvar limites horários"):
                with open("limites_salvos.json", "w") as f:
                    json.dump(st.session_state.limites_por_medidor, f)
                st.success("Limites salvos com sucesso!")

            limites_json = json.dumps(st.session_state.limites_por_medidor, indent=2)
            st.download_button("Download dos limites", data=limites_json, file_name="limites_por_medidor.json", mime="application/json")

        # Página 4 - Dashboard
        elif pagina == "Dashboard":
            st.markdown("### Dashboard - Graphs by Meter")
            cores = plt.cm.get_cmap("tab10", len(medidores_disponiveis))

            for i in range(0, len(medidores_disponiveis), 4):
                cols = st.columns(4)
                for j in range(4):
                    if i + j < len(medidores_disponiveis):
                        medidor = medidores_disponiveis[i + j]
                        with cols[j]:
                            fig, ax = plt.subplots(figsize=(5, 3))
                            ax.plot(horas, dados_dia[medidor], label="Consumo", color=cores(i + j))

                            if "limites_por_medidor" in st.session_state and medidor in st.session_state.limites_por_medidor:
                                limites = st.session_state.limites_por_medidor[medidor]
                                ax.plot(range(24), limites, label="Limite", linestyle="--", color="red")

                            ax.set_title(medidor)
                            ax.set_xticks(range(0, 24))
                            ax.set_xlabel("Hora")
                            ax.set_ylabel("kWh")
                            ax.legend(fontsize="x-small")
                            st.pyplot(fig)


    except Exception as e:
        st.error(f"Error processing the data: {e}")
