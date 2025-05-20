
import streamlit as st
import pandas as pd
import io
import matplotlib.pyplot as plt
import json
import os

# Funções auxiliares
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

# Menu lateral
st.sidebar.title("Menu")
pagina = st.sidebar.selectbox("Escolha a página:", ["Home", "Graphs by Meter", "Consumption Limits", "Dashboard"])

# Seção de idioma
idioma = st.sidebar.selectbox("Idioma / Language", ["Português", "English"])

# Função para traduzir textos
def traduzir(texto):
    traducoes = {
        "Português": {
            "Paste the data here (tabulated):": "Cole os dados aqui (tabulados):",
            "Select the meters:": "Selecione os medidores:",
            "Hourly consumption in": "Consumo horário em",
            "Time of day": "Hora do dia",
            "Consumption (kWh)": "Consumo (kWh)",
            "Configure the hourly limits for each meter": "Configure os limites horários para cada medidor",
            "Upload limits from a JSON file": "Carregar limites a partir de um arquivo JSON",
            "Save hourly limits": "Salvar limites horários",
            "Download limits": "Download dos limites",
            "Dashboard - Graphs by Meter": "Dashboard - Gráficos por Medidor",
            "Error processing the data:": "Erro ao processar os dados:"
        },
        "English": {
            "Paste the data here (tabulated):": "Paste the data here (tabulated):",
            "Select the meters:": "Select the meters:",
            "Hourly consumption in": "Hourly consumption in",
            "Time of day": "Time of day",
            "Consumption (kWh)": "Consumption (kWh)",
            "Configure the hourly limits for each meter": "Configure the hourly limits for each meter",
            "Upload limits from a JSON file": "Upload limits from a JSON file",
            "Save hourly limits": "Save hourly limits",
            "Download limits": "Download limits",
            "Dashboard - Graphs by Meter": "Dashboard - Graphs by Meter",
            "Error processing the data:": "Error processing the data:"
        }
    }
    return traducoes[idioma].get(texto, texto)

# Caixa de texto para colar os dados
with st.sidebar.expander(traduzir("Paste the data here (tabulated):")):
    dados_colados = st.text_area(traduzir("Paste the data here (tabulated):"), height=300)

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
            medidores_selecionados = st.multiselect(traduzir("Select the meters:"), medidores_disponiveis, default=medidores_disponiveis)

            fig, ax = plt.subplots(figsize=(16, 6))
            for medidor in medidores_selecionados:
                ax.plot(horas, dados_dia[medidor], label=medidor)

            if "limites_por_medidor" in st.session_state and medidor in st.session_state.limites_por_medidor:
                ax.plot(range(24), st.session_state.limites_por_medidor[medidor], linestyle="--", color="red", label=f"Limite - {medidor}")

            ax.set_title(f"{traduzir('Hourly consumption in')} {data_selecionada} (kWh)")
            ax.set_xlabel(traduzir("Time of day"))
            ax.set_ylabel(traduzir("Consumption (kWh)"))
            ax.set_xticks(range(0, 24))
            ax.legend(loc="lower center", bbox_to_anchor=(0.5, -0.4), ncol=3, fontsize='small')
            plt.xticks(rotation=45)
            st.pyplot(fig)

            st.markdown("### " + traduzir("Hourly consumption (kWh)"))
            st.dataframe(
                dados_dia.set_index("Datetime")[medidores_selecionados].round(2).style.set_properties(**{"text-align": "center"}),
                use_container_width=True
            )

            st.markdown("### " + traduzir("Total Consumption per Meter (kWh)"))
            totais = dados_dia[medidores_disponiveis].sum().round(2).to_frame(name=traduzir("Total Consumption (kWh)"))
            st.dataframe(totais.style.set_properties(**{"text-align": "center"}), use_container_width=True)

        # Página 2 - Gráficos por Medidor
        elif pagina == "Graphs by Meter":
            st.markdown("### " + traduzir("Individual Charts with Limit Curve"))
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

                ax.set_title(f"{medidor} - {traduzir('Consumption per hour (kWh)')}")
                ax.set_xlabel(traduzir("Time of day"))
                ax.set_ylabel(traduzir("Consumption (kWh)"))
                ax.set_xticks(range(0, 24))
                ax.legend(fontsize='small')
                st.pyplot(fig)

                st.markdown(f"**{traduzir('Resume')} - {medidor}**")
                resumo_df = pd.DataFrame({
                    traduzir("Sum of Limits (kWh)"): [round(limite_total, 2)],
                    traduzir("Sum of Consumption (kWh)"): [round(consumo_total, 2)]
                }, index=["Total"])

                def highlight_excesso(val):
                    consumo = resumo_df[traduzir("Sum of Consumption (kWh)")].values[0]
                    limite = resumo_df[traduzir("Sum of Limits (kWh)")].values[0]
                    return ["", "background-color: red; color: white"] if consumo > limite else ["", ""]

                styled = resumo_df.style.set_properties(**{"text-align": "center"}).apply(highlight_excesso, axis=1)
                st.dataframe(styled, use_container_width=True)

        # Página 3 - Configuração de Limites
        elif pagina == "Consumption Limits":
            st.markdown("### " + traduzir("Configure the hourly limits for each meter"))

            if "limites_por_medidor" not in st.session_state:
                st.session_state.limites_por_medidor = {m: [5.0]*24 for m in medidores_disponiveis}

            uploaded_file = st.file_uploader(traduzir("Upload limits from a JSON file"), type="json")
            if uploaded_file is not None:
                try:
                    st.session_state.limites_por_medidor = json.load(uploaded_file)
                    st.success(traduzir("Limits loaded successfully!"))
                except Exception as e:
                    st.error(f"{traduzir('Error loading limits:')} {e}")

            for medidor in medidores_disponiveis:
                st.markdown(f"#### {medidor}")
                st.markdown(f"##### {traduzir('Hourly limits for')} {medidor}")
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

            if st.button(traduzir("Save hourly limits")):
                with open("limites_salvos.json", "w") as f:
                    json.dump(st.session_state.limites_por_medidor, f)
                st.success(traduzir("Limits saved successfully!"))

            limites_json = json.dumps(st.session_state.limites_por_medidor, indent=2)
            st.download_button(traduzir("Download limits"), data=limites_json, file_name="limites_por_medidor.json", mime="application/json")

        # Página 4 - Dashboard
        elif pagina == "Dashboard":
            st.markdown("### " + traduzir("Dashboard - Graphs by Meter"))
            cores = plt.cm.get_cmap("tab10", len(medidores_disponiveis))

            for idx, medidor in enumerate(medidores_disponiveis):
                fig, ax = plt.subplots(figsize=(12, 4))
                ax.plot(horas, dados_dia[medidor], label="Consumo", color=cores(idx))
                if "limites_por_medidor" in st.session_state and medidor in st.session_state.limites_por_medidor:
                    limites = st.session_state.limites_por_medidor[medidor]
                    ax.plot(range(24), limites, label="Limite", linestyle="--", color="red")
                ax.set_title(medidor)
                ax.set_xticks(range(0, 24))
                ax.set_xlabel("Hora")
                ax.set_ylabel("kWh")
                ax.legend(fontsize="small")
                st.pyplot(fig)

    except Exception as e:
        st.error(f"{traduzir('Error processing the data:')} {e}")
