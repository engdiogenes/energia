import streamlit as st
import pandas as pd
import io
import json
import plotly.graph_objects as go
import datetime
from streamlit_calendar import calendar

st.set_page_config(layout="wide", page_title="Monitor de Energia")

st.markdown("""
    <style>
        .block-container {
            padding-top: 0rem;
            padding-bottom: 1rem;
        }
        header, footer {
            visibility: hidden;
        }
    </style>
""", unsafe_allow_html=True)


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
    consumo["Área Produtiva"] = consumo["MP&L"] + consumo["GAHO"] + consumo["CAG"] + consumo["SEOB"] + consumo["EBPC"] + \
                                consumo["PMDC-OFFICE"] + consumo["TRIM&FINAL"] + consumo["OFFICE + CANTEEN"] + 13.75
    consumo = consumo.drop(columns=["QGBT1-MPTF", "QGBT2-MPTF"])

    return consumo


st.title(" Monitoramento de Consumo de Energia")

with st.sidebar:
    st.header(" Entrada de Dados")
    dados_colados = st.text_area("Cole os dados aqui (tabulados):", height=300)
    idioma = st.selectbox("Idioma / Language", ["Português", "English"])

if dados_colados:
    try:
        with st.spinner("Processando os dados..."):
            consumo = carregar_dados(dados_colados)

        datas_disponiveis = consumo["Datetime"].dt.date.unique()
        data_selecionada = st.sidebar.date_input("Selecione a data", value=max(datas_disponiveis),
                                                 min_value=min(datas_disponiveis),
                                                 max_value=max(datas_disponiveis))

        dados_dia = consumo[consumo["Datetime"].dt.date == data_selecionada]
        horas = dados_dia["Datetime"].dt.hour
        medidores_disponiveis = [col for col in dados_dia.columns if col != "Datetime"]

        if "limites_por_medidor" not in st.session_state:
            st.session_state.limites_por_medidor = {m: [5.0] * 24 for m in medidores_disponiveis}

        tabs = st.tabs([" Viso Geral", " Por Medidor", " Limites", " Dashboard", " Calendário"])

        # TABS 1 - VISO GERAL
        with tabs[0]:
            st.subheader(f" Consumo horário em {data_selecionada.strftime('%d/%m/%Y')}")
            medidores_selecionados = st.multiselect("Selecione os medidores:", medidores_disponiveis,
                                                    default=medidores_disponiveis)

            fig = go.Figure()
            for medidor in medidores_selecionados:
                fig.add_trace(go.Scatter(
                    x=dados_dia["Datetime"].dt.strftime("%H:%M"),
                    y=dados_dia[medidor],
                    mode="lines+markers",
                    name=medidor
                ))

            fig.update_layout(
                xaxis_title="Hora do dia",
                yaxis_title="Consumo (kWh)",
                template="plotly_white",
                height=500,
                legend=dict(orientation="h", y=-0.3, x=0.5, xanchor="center")
            )
            st.plotly_chart(fig, use_container_width=True)

            # Gráfico de consumo de cada prédio/dia para as áreas produtivas
            st.subheader(" Consumo Diário por Medidor")
            consumo_diario = consumo.copy()
            consumo_diario["Data"] = consumo_diario["Datetime"].dt.date
            consumo_agrupado = consumo_diario.groupby("Data")[medidores_disponiveis].sum().reset_index()

            medidores_calendario = st.multiselect("Selecione os medidores para o calendário:", medidores_disponiveis,
                                                  default=medidores_disponiveis)
            fig = go.Figure()

            for medidor in medidores_calendario:
                fig.add_trace(go.Bar(
                    x=consumo_agrupado["Data"],
                    y=consumo_agrupado[medidor],
                    name=medidor
                ))

                fig.update_layout(
                    barmode="stack",
                    xaxis_title="Data",
                    yaxis_title="Consumo Total (kWh)",
                    template="plotly_white",
                    height=500,
                    legend=dict(orientation="h", y=-0.3, x=0.5, xanchor="center")
                )
            st.plotly_chart(fig, use_container_width=True)

            # Tabela de consumo horário dos prédios
            st.markdown("###  Consumo por hora")
            st.dataframe(dados_dia.set_index("Datetime")[medidores_selecionados].round(2), use_container_width=True)

        # TABS 2 - POR MEDIDOR
        with tabs[1]:
            st.subheader(" Gráficos por Medidor com Curva de Limite")
            for medidor in medidores_disponiveis:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=horas,
                    y=dados_dia[medidor],
                    mode="lines+markers",
                    name="Consumo"
                ))

                limites = st.session_state.limites_por_medidor.get(medidor, [5.0] * 24)
                fig.add_trace(go.Scatter(
                    x=list(range(24)),
                    y=limites,
                    mode="lines",
                    name="Limite",
                    line=dict(dash="dash", color="red")
                ))

                fig.update_layout(title=medidor, xaxis_title="Hora", yaxis_title="kWh", height=300,
                                  template="plotly_white")
                st.plotly_chart(fig, use_container_width=True)

        # TABS 3 - CONFIGURAR LIMITES
        with tabs[2]:
            st.subheader(" Configuração de Limites por Hora")
            uploaded_file = st.file_uploader(" Carregar limites (JSON)", type="json")
            if uploaded_file:
                st.session_state.limites_por_medidor = json.load(uploaded_file)
                st.success("Limites carregados com sucesso.")

            for medidor in medidores_disponiveis:
                with st.expander(f" {medidor}"):
                    cols = st.columns(6)
                    novos = []
                    for i in range(24):
                        with cols[i % 6]:
                            novos.append(
                                st.number_input(f"{i}h", value=st.session_state.limites_por_medidor[medidor][i],
                                                min_value=0.0, max_value=2000.0, step=0.5, key=f"{medidor}_{i}"))
                    st.session_state.limites_por_medidor[medidor] = novos

            st.download_button(" Baixar Limites", json.dumps(st.session_state.limites_por_medidor, indent=2),
                               file_name="limites.json", mime="application/json")

        # TABS 4 - DASHBOARD
        with tabs[3]:
            st.subheader(" Painel Resumo")

            colunas = st.columns(4)
            for idx, medidor in enumerate(medidores_disponiveis):
                with colunas[idx % 4]:
                    valor = round(dados_dia[medidor].sum(), 2)
                    limite = round(sum(st.session_state.limites_por_medidor[medidor]), 2)
                    excedido = valor > limite
                    st.metric(label=f"{medidor}", value=f"{valor} kWh", delta=f"{valor - limite:.2f} kWh",
                              delta_color="inverse" if excedido else "inverse")

            st.divider()
            st.subheader(" Grficos de Consumo vs Limite")

            # Criar 3 linhas com 4 colunas cada
            linhas = [st.columns(4) for _ in range(3)]

            # Inserir grficos nos slots
            for idx, medidor in enumerate(medidores_disponiveis):
                linha = idx // 4
                coluna = idx % 4
                with linhas[linha][coluna]:
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=horas,
                        y=dados_dia[medidor],
                        mode="lines+markers",
                        name="Consumo",
                        line=dict(color="blue")
                    ))
                    limites = st.session_state.limites_por_medidor.get(medidor, [5.0] * 24)
                    fig.add_trace(go.Scatter(
                        x=list(range(24)),
                        y=limites,
                        mode="lines",
                        name="Limite",
                        line=dict(color="red", dash="dash")
                    ))
                    fig.update_layout(
                        title=medidor,
                        xaxis_title="Hora",
                        yaxis_title="kWh",
                        height=350,
                        template="plotly_white",
                        legend=dict(orientation="h", y=-0.3, x=0.5, xanchor="center")
                    )
                    st.plotly_chart(fig, use_container_width=True, key=f"plot_{medidor}")

            # Preencher espaos vazios com placeholders
            total_graficos = len(medidores_disponiveis)
            total_posicoes = 12
            if total_graficos < total_posicoes:
                for idx in range(total_graficos, total_posicoes):
                    linha = idx // 4
                    coluna = idx % 4
                    with linhas[linha][coluna]:
                        st.markdown("### Espao reservado")
#TABS 5 - CALENDÁRIO
        with tabs[4]:
            st.markdown("### Visualização Semanal da Área Produtiva")
            consumo["Data"] = consumo["Datetime"].dt.date
            dias_unicos = consumo["Data"].unique()
            dias_unicos = sorted(dias_unicos)

            target_limit = 500
            max_consumo = consumo["Área Produtiva"].max()
            dias_mes = pd.date_range(start=min(dias_unicos), end=max(dias_unicos), freq="D")
            semanas = [dias_mes[i:i+7] for i in range(0, len(dias_mes), 7)]

        for semana in semanas:
            cols = st.columns(7)
        for i, dia in enumerate(semana):
            with cols[i]:
                st.caption(dia.strftime('%d/%m'))
                dados_dia = consumo[consumo["Datetime"].dt.date == dia.date()]
            if not dados_dia.empty:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=dados_dia["Datetime"].dt.strftime("%H:%M"),
                    y=dados_dia["Área Produtiva"],
                    mode="lines",
                    line=dict(color="green"),
                ))
                fig.add_trace(go.Scatter(
                    x=dados_dia["Datetime"].dt.strftime("%H:%M"),
                    y=[target_limit] * len(dados_dia),
                    mode="lines",
                    line=dict(color="red", dash="dash"),
                    showlegend=False
                ))
                fig.update_layout(
                    margin=dict(l=0, r=0, t=0, b=0),
                    height=120,
                    xaxis=dict(showticklabels=False),
                    yaxis=dict(showticklabels=False, range=[0, max_consumo]),
                    showlegend=False
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.markdown("_Sem dados_")

        with tabs[4]:
            st.subheader("Calendário Interativo de Consumo")
            # st.title("Monitoramento de Consumo de Energia")

            with st.sidebar:
                st.header("Entrada de Dados")
                dados_colados = st.text_area("Cole os dados aqui (tabulados):", height=300)
                idioma = st.selectbox("Idioma / Language", ["Português", "English"])

            if dados_colados:
                try:
                    with st.spinner("Processando os dados..."):
                        consumo = carregar_dados(dados_colados)

                    st.subheader("Calendário com Curva de Consumo da Área Produtiva")

                    consumo["Data"] = consumo["Datetime"].dt.date
                    dias_unicos = consumo["Data"].unique()
                    dias_unicos = sorted(dias_unicos)

                    # Limite diário e escala fixa
                    target_limit = 500
                    max_consumo = consumo["Área Produtiva"].max()

                    dias_mes = pd.date_range(start=min(dias_unicos), end=max(dias_unicos), freq="D")
                    semanas = [dias_mes[i:i + 7] for i in range(0, len(dias_mes), 7)]

                    for semana in semanas:
                        cols = st.columns(7)
                        for i, dia in enumerate(semana):
                            with cols[i]:
                                st.caption(dia.strftime('%d/%m'))
                                dados_dia = consumo[consumo["Datetime"].dt.date == dia.date()]
                                if not dados_dia.empty:
                                    fig = go.Figure()
                                    fig.add_trace(go.Scatter(
                                        x=dados_dia["Datetime"].dt.strftime("%H:%M"),
                                        y=dados_dia["Área Produtiva"],
                                        mode="lines",
                                        line=dict(color="green"),

                                    ))
                                    fig.add_trace(go.Scatter(
                                        x=dados_dia["Datetime"].dt.strftime("%H:%M"),
                                        y=[target_limit] * len(dados_dia),
                                        mode="lines",
                                        line=dict(color="red", dash="dash"),
                                        showlegend=False

                                    ))
                                    fig.update_layout(
                                        margin=dict(l=0, r=0, t=0, b=0),
                                        height=120,
                                        xaxis=dict(showticklabels=False),
                                        yaxis=dict(showticklabels=False, range=[0, max_consumo]),
                                        showlegend=False
                                    )
                                    st.plotly_chart(fig, use_container_width=True)
                                else:
                                    st.markdown("_Sem dados_")
                except Exception as e:
                    st.error(f"Erro ao processar os dados: {e}")

    except Exception as e:
        st.error(f"Erro ao processar os dados: {e}")
