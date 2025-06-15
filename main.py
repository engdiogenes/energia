import streamlit as st
import pandas as pd
import io
import json
import plotly.graph_objects as go
import datetime
from streamlit_calendar import calendar
import fpdf
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from statsmodels.tsa.arima.model import ARIMA
from datetime import timedelta
import streamlit.components.v1 as components




st.set_page_config(layout="wide", page_title="Monitor de Energia")

# Caminho padrão do JSON
CAMINHO_JSON_PADRAO = "limites_padrao.json"

# Carregar automaticamente os limites se o arquivo existir
if os.path.exists(CAMINHO_JSON_PADRAO):
    try:
        limites_df = pd.read_json(CAMINHO_JSON_PADRAO)
        limites_df["Timestamp"] = pd.to_datetime(limites_df["Timestamp"], dayfirst=True)
        limites_df["Data"] = limites_df["Timestamp"].dt.date
        limites_df["Hora"] = limites_df["Timestamp"].dt.hour
        st.session_state.limites_df = limites_df
        st.session_state.limites_por_medidor_horario = {
            medidor: list(
                limites_df[limites_df["Data"] == limites_df["Data"].min()].sort_values("Hora")[medidor].values)
            for medidor in limites_df.columns
            if medidor not in ["Timestamp", "Data", "Hora"]
        }
    except Exception as e:
        st.warning(f"Erro ao carregar limites padrão: {e}")

st.markdown("""
    <style>
    .block-container {
        padding-top: 1rem;
        padding-bottom: 1rem;
    }
    header, footer {
        visibility: hidden;
    }
    [data-testid="stSidebar"] {
        background-color: #f0f2f6;
        padding: 1.5rem 1rem;
    }
    [data-testid="stSidebar"] > div:first-child {
        padding-top: 0rem;
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


# st.title(" Energy data analyser")

with st.sidebar:
    #st.sidebar.image("logo.png", width=360)
    # st.logo("logo.png", size="Large", link=None, icon_image=None)
    st.header(" Data Input")
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials


    def obter_dados_do_google_sheets():
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds_dict = st.secrets["google_sheets"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)

        client = gspread.authorize(creds)

        sheet = client.open("dados_energia_bms").sheet1
        dados = sheet.get_all_values()

        # Converte para texto tabulado (como se fosse colado manualmente)
        linhas = ["\t".join(linha) for linha in dados]
        texto_tabulado = "\n".join(linhas)
        return texto_tabulado


    origem_dados = st.radio("Escolha a origem dos dados:", ["Google Sheets", "Colar manualmente"])

    if origem_dados == "Google Sheets":
        dados_colados = obter_dados_do_google_sheets()
        # Converter os dados colados em DataFrame temporário para extrair a última data
        df_temp = pd.read_csv(io.StringIO(limpar_valores(dados_colados)), sep="\t")
        df_temp["Datetime"] = pd.to_datetime(df_temp["Date"] + " " + df_temp["Time"], dayfirst=True)
        ultima_data = df_temp["Datetime"].max()

        # Exibir no Streamlit
        if pd.notna(ultima_data):
          st.sidebar.markdown(f"📅 **Última atualização:** {ultima_data.strftime('%d/%m/%Y %H:%M')}")
        else:
            st.sidebar.warning("Não foi possível determinar a última data de atualização.")

    else:
        dados_colados = st.text_area("Cole os dados aqui (tabulados):", height=300)


    # Campo para inserir e-mail
    to_email = st.text_input("Destinatário do e-mail")
    # Botão para enviar o relatório por e-mail
    if st.button("✉️ Enviar por E-mail", key="enviar_email_sidebar", use_container_width=True):
        if not to_email:
            st.warning("Por favor, insira o e-mail do destinatário.")
        else:
            try:
                EMAIL = st.secrets["email"]["address"]
                PASSWORD = st.secrets["email"]["password"]

                msg = MIMEMultipart()
                msg["From"] = EMAIL
                msg["To"] = to_email
                msg["Subject"] = "Relatório de Consumo Energético"

                # Verifica se algum medidor ultrapassou o limite diário
                medidores_excedidos = []
                dados_dia = st.session_state.consumo[
                    st.session_state.consumo["Datetime"].dt.date == st.session_state.data_selecionada]
                for medidor in st.session_state.limites_por_medidor_horario:
                    if medidor in dados_dia.columns:
                        consumo_total = dados_dia[medidor].sum()
                        limite_total = sum(st.session_state.limites_por_medidor_horario[medidor])
                        if consumo_total > limite_total:
                            medidores_excedidos.append(
                                f"- {medidor}: {consumo_total:.2f} kWh (limite: {limite_total:.2f} kWh)")

                # Corpo do e-mail
                body = f"""
                Resumo do Dia {st.session_state.data_selecionada.strftime('%d/%m/%Y')}:

                Consumo Geral: {st.session_state.consumo_geral:.2f} kWh
                Limite Geral: {st.session_state.limite_geral:.2f} kWh
                Saldo do Dia (Geral): {st.session_state.saldo_geral:.2f} kWh

                Consumo da Área Produtiva: {st.session_state.consumo_area:.2f} kWh
                Limite da Área Produtiva: {st.session_state.limites_area:.2f} kWh
                Saldo do Dia (Área Produtiva): {st.session_state.saldo_area:.2f} kWh
                """

                # Adiciona alerta se houver medidores excedidos
                if medidores_excedidos:
                    body += "\n⚠️ Alerta: Os seguintes medidores ultrapassaram seus limites diários:\n"
                    body += "\n".join(medidores_excedidos)

                msg.attach(MIMEText(body, "plain"))

                with smtplib.SMTP("smtp.gmail.com", 587) as server:
                    server.starttls()
                    server.login(EMAIL, PASSWORD)
                    server.send_message(msg)

                st.success("E-mail enviado com sucesso!")
            except Exception as e:
                st.error(f"Erro ao enviar e-mail: {e}")

if dados_colados:
    try:
        with st.spinner("Processando os dados..."):
            consumo = carregar_dados(dados_colados)
            st.session_state.consumo = consumo
            consumo_completo = consumo.copy()

            datas_disponiveis = consumo["Datetime"].dt.date.unique()
            data_selecionada = st.sidebar.date_input(
                "Selecione a data",
                value=max(datas_disponiveis),
                min_value=min(datas_disponiveis),
                max_value=max(datas_disponiveis)
            )
            # Créditos e data no rodapé da sidebar (logo após o campo de data)
            st.sidebar.markdown(
                f"""
                <hr style="margin-top: 2rem; margin-bottom: 0.5rem;">
                <div style='font-size: 0.8rem; color: gray; text-align: center;'>
                    Desenvolvido por <strong>Diógenes Oliveira</strong>
                </div>
                """,
                unsafe_allow_html=True
            )

            st.session_state.data_selecionada = data_selecionada

            # 🔄 Atualizar os limites por medidor e hora com base na nova data selecionada
            if "limites_df" in st.session_state:
                limites_df = st.session_state.limites_df
                limites_dia_df = limites_df[limites_df["Data"] == data_selecionada]
                st.session_state.limites_por_medidor_horario = {
                    medidor: list(limites_dia_df.sort_values("Hora")[medidor].values)
                    for medidor in limites_dia_df.columns
                    if medidor not in ["Timestamp", "Data", "Hora"]
                }

            st.session_state.data_selecionada = data_selecionada

            dados_dia = consumo[consumo["Datetime"].dt.date == data_selecionada]
            horas = dados_dia["Datetime"].dt.hour
            medidores_disponiveis = [col for col in dados_dia.columns if col != "Datetime"]

            tabs = st.tabs([" Overview", " Per meter", " Daily targets", " Dashboard", " Calender", " Conversion ", " Month prediction "])

            # TABS 1 - VISÃO GERAL
            with tabs[0]:
                st.subheader(f"Report of the day:  {data_selecionada.strftime('%d/%m/%Y')}")
                # Cálculos
                Consumo_gab = 300
                consumo_area = dados_dia["Área Produtiva"].sum()
                consumo_pccb = dados_dia["PCCB"].sum() if "PCCB" in dados_dia else 0
                consumo_maiw = dados_dia["MAIW"].sum() if "MAIW" in dados_dia else 0
                consumo_geral = consumo_area + consumo_pccb + consumo_maiw + Consumo_gab

                # Determina até que hora há dados disponíveis
                ultima_hora_disponivel = dados_dia["Datetime"].dt.hour.max()

                # Calcula limites apenas até a última hora com dados
                limites_area = sum(
                    st.session_state.limites_por_medidor_horario.get(medidor, [0] * 24)[h]
                    for h in range(ultima_hora_disponivel + 1)
                    for medidor in [
                        "MP&L", "GAHO", "CAG", "SEOB", "EBPC", "PMDC-OFFICE", "OFFICE + CANTEEN", "TRIM&FINAL"
                    ]
                    if medidor in st.session_state.limites_por_medidor_horario
                ) + 13.75 * (ultima_hora_disponivel + 1)

                limite_pccb = sum(
                    st.session_state.limites_por_medidor_horario.get("PCCB", [0] * 24)[:ultima_hora_disponivel + 1])
                limite_maiw = sum(
                    st.session_state.limites_por_medidor_horario.get("MAIW", [0] * 24)[:ultima_hora_disponivel + 1])
                limite_geral = limites_area + limite_pccb + limite_maiw + Consumo_gab

                # Deltas e saldos
                delta_geral = consumo_geral - limite_geral
                delta_area = consumo_area - limites_area
                saldo_geral = limite_geral - consumo_geral
                saldo_area = limites_area - consumo_area

                # Salvar no session_state para uso posterior (ex: no corpo do e-mail)
                st.session_state.consumo_geral = consumo_geral
                st.session_state.limite_geral = limite_geral
                st.session_state.saldo_geral = saldo_geral
                st.session_state.consumo_area = consumo_area
                st.session_state.limites_area = limites_area
                st.session_state.saldo_area = saldo_area

                # Layout em 3 colunas por linha
                col1, col2, col3 = st.columns(3)
                col4, col5, col6 = st.columns(3)

                col1.metric("🎯 Daily Target -  Full Plant", f"{limite_geral:.2f} kWh")
                col2.metric("⚡ Daily Consumption - Full Plant", f"{consumo_geral:.2f} kWh",
                            delta=f"{delta_geral:.2f} kWh",
                            delta_color="normal" if delta_geral == 0 else ("inverse" if delta_geral < 0 else "off"))
                col3.metric("📉 Balance of the Day (Ful Plant)", f"{saldo_geral:.2f} kWh", delta_color="inverse")

                col4.metric("🎯 Daily Target -  Productive areas", f"{limites_area:.2f} kWh")
                col5.metric("🏭 Daily Consumption - Productive areas", f"{consumo_area:.2f} kWh",
                            delta=f"{delta_area:.2f} kWh",
                            delta_color="normal" if delta_area == 0 else ("inverse" if delta_area < 0 else "off"))
                col6.metric("📉 Balance of the Day (Productive area)", f"{saldo_area:.2f} kWh", delta_color="inverse")

                st.divider()

                st.subheader(f" Consumo horário em {data_selecionada.strftime('%d/%m/%Y')}")
                medidores_selecionados = st.multiselect(
                    "Selecione os medidores:",
                    medidores_disponiveis,
                    default=[m for m in medidores_disponiveis if m != "Área Produtiva"]
                )

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
                st.plotly_chart(fig, use_container_width=True, key=f"grafico_{medidor}")
                st.divider()
                # Gráfico de consumo de cada prédio/dia para as áreas produtivas
                st.subheader(" Consumo Diário por Medidor")
                consumo_diario = consumo.copy()
                consumo_diario["Data"] = consumo_diario["Datetime"].dt.date
                consumo_agrupado = consumo_diario.groupby("Data")[medidores_disponiveis].sum().reset_index()
                medidores_calendario = st.multiselect(
                    "Selecione os medidores para o calendário:",
                    medidores_disponiveis,
                    default=[m for m in medidores_disponiveis if m != "Área Produtiva"]
                )

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

                st.plotly_chart(fig, use_container_width=True, key=f"graf_{medidor}")

                st.divider()

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

                    limites = st.session_state.limites_por_medidor_horario.get(medidor, [5.0] * 24)
                    fig.add_trace(go.Scatter(
                        x=list(range(24)),
                        y=limites,
                        mode="lines",
                        name="Limite",
                        line=dict(dash="dash", color="red")
                    ))

                    fig.update_layout(title=medidor, xaxis_title="Hora", yaxis_title="kWh", height=300,
                                      template="plotly_white")
                    st.plotly_chart(fig, use_container_width=True, key=f"chart_{medidor}")
                    st.divider()

            # TABS 3 - CONFIGURAR LIMITES
            with tabs[2]:
                st.subheader(" Limites Horários Carregados")

                if "limites_df" in st.session_state:
                    st.dataframe(
                        st.session_state.limites_df.sort_values("Timestamp").reset_index(drop=True),
                        use_container_width=True
                    )
                    st.download_button(
                        "Baixar Limites JSON",
                        st.session_state.limites_df.to_json(orient="records", date_format="iso", indent=2),
                        file_name="limites_horarios_completos.json",
                        mime="application/json"
                    )
                else:
                    st.warning("Nenhum limite foi carregado.")

            # TABS 3 - DASHBOARD
            with tabs[3]:
                st.subheader(" Painel Resumo")
                colunas = st.columns(4)
                for idx, medidor in enumerate(medidores_disponiveis):
                    with colunas[idx % 4]:
                        valor = round(dados_dia[medidor].sum(), 2)
                        limite = round(sum(st.session_state.limites_por_medidor_horario[medidor]), 2)
                        excedido = valor > limite
                        st.metric(
                            label=f"{medidor}",
                            value=f"{valor} kWh",
                            delta=f"{valor - limite:.2f} kWh",
                            delta_color="inverse" if excedido else "inverse"
                        )

                st.divider()
                st.subheader(" Gráficos de Consumo vs Limite")
                linhas = [st.columns(4) for _ in range(3)]
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
                        limites = st.session_state.limites_por_medidor_horario.get(medidor, [5.0] * 24)
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
                        st.plotly_chart(fig, use_container_width=True, key=f"grafi_{medidor}")

            # TABS 4 - CALENDÁRIO
            with tabs[4]:
                st.subheader("Calendário Interativo de Consumo da Área Produtiva")
                consumo_completo["Data"] = consumo_completo["Datetime"].dt.date
                dias_unicos = sorted(consumo_completo["Data"].unique())
                dias_mes = pd.date_range(start=min(dias_unicos), end=max(dias_unicos), freq="D")
                semanas = [dias_mes[i:i + 7] for i in range(0, len(dias_mes), 7)]
                max_consumo = consumo_completo["Área Produtiva"].max()

                for semana in semanas:
                    cols = st.columns(7)
                    for i, dia in enumerate(semana):
                        with cols[i]:
                            st.caption(dia.strftime('%d/%m'))
                            dados_dia = consumo_completo[consumo_completo["Datetime"].dt.date == dia.date()]
                            if not dados_dia.empty:
                                # Obter limites do JSON para o dia específico
                                if "limites_df" in st.session_state:
                                    limites_dia_df = st.session_state.limites_df[
                                        st.session_state.limites_df["Data"] == dia.date()
                                        ]
                                    limites_area_dia = [
                                        sum(
                                            limites_dia_df[limites_dia_df["Hora"] == h][medidor].values[0]
                                            if medidor in limites_dia_df.columns and not
                                            limites_dia_df[limites_dia_df["Hora"] == h][medidor].empty
                                            else 0
                                            for medidor in
                                            ["MP&L", "GAHO", "CAG", "SEOB", "EBPC", "PMDC-OFFICE", "OFFICE + CANTEEN",
                                             "TRIM&FINAL"]
                                        ) + 13.75
                                        for h in range(24)
                                    ]
                                else:
                                    limites_area_dia = [0] * 24

                                fig = go.Figure()
                                fig.add_trace(go.Scatter(
                                    x=dados_dia["Datetime"].dt.strftime("%H:%M"),
                                    y=dados_dia["Área Produtiva"],
                                    mode="lines",
                                    line=dict(color="green"),
                                ))
                                fig.add_trace(go.Scatter(
                                    x=dados_dia["Datetime"].dt.strftime("%H:%M"),
                                    y=[limites_area_dia[dt.hour] for dt in dados_dia["Datetime"]],
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

            # TABS 5 - CALENDÁRIO
            with tabs[5]:
                st.title("or CSV para JSON - Limites Horários por Medidor")
                uploaded_file = st.file_uploader("Faça upload do arquivo CSV", type="csv")
                if uploaded_file is not None:
                    try:
                        # Lê o CSV com codificação ISO-8859-1
                        df = pd.read_csv(uploaded_file, encoding="ISO-8859-1")

                        st.subheader("Pré-visualização do CSV")
                        st.dataframe(df)

                        # Usa as duas primeiras colunas como Data e Hora
                        data_col, hora_col = df.columns[0], df.columns[1]

                        # Cria coluna de timestamp
                        df["Timestamp"] = pd.to_datetime(df[data_col] + " " + df[hora_col], dayfirst=True)
                        df["Timestamp"] = df["Timestamp"].dt.strftime("%d/%m/%Y %H:%M")

                        # Adiciona sufixo incremental para timestamps duplicados
                        df["Timestamp"] = df["Timestamp"] + df.groupby("Timestamp").cumcount().apply(
                            lambda x: f" #{x + 1}" if x > 0 else "")

                        # Define o índice e remove colunas originais
                        df.set_index("Timestamp", inplace=True)
                        df.drop(columns=[data_col, hora_col], inplace=True)

                        # Converte para JSON
                        json_data = df.reset_index().to_dict(orient="records")

                        st.subheader("JSON Gerado")
                        st.json(json_data)

                        # Permite download
                        json_str = json.dumps(json_data, indent=2, ensure_ascii=False)

                        st.download_button("Baixar JSON", json_str, file_name="limites_horarios.json",
                                           mime="application/json")
                    except Exception as e:
                        st.error(f"Erro ao processar os dados: {e}")
            # TABS 6 - PREVISÃO MENSAL
            with tabs[6]:
                st.title("📅 Month Prediction")

                if "limites_df" in st.session_state and "data_selecionada" in st.session_state and "consumo" in st.session_state:
                    limites_df = st.session_state.limites_df.copy()
                    data_ref = st.session_state.data_selecionada
                    df_consumo = st.session_state.consumo.copy()

                    colunas_area_produtiva = [
                        "MP&L", "GAHO", "CAG", "SEOB", "EBPC", "PMDC-OFFICE", "OFFICE + CANTEEN", "TRIM&FINAL"
                    ]

                    limites_df["Data"] = pd.to_datetime(limites_df["Data"])
                    limites_mes = limites_df[
                        (limites_df["Data"].dt.month == data_ref.month) &
                        (limites_df["Data"].dt.year == data_ref.year)
                        ]

                    # Consumo máximo previsto
                    consumo_max_mes = limites_mes[colunas_area_produtiva].sum().sum()
                    dias_mes = limites_mes["Data"].dt.date.nunique()
                    adicional_fixo_mes = dias_mes * 24 * 13.75
                    consumo_max_mes += adicional_fixo_mes

                    # Consumo previsto
                    df_consumo["Datetime"] = pd.to_datetime(df_consumo["Datetime"])
                    consumo_ate_agora = df_consumo[
                        (df_consumo["Datetime"].dt.month == data_ref.month) &
                        (df_consumo["Datetime"].dt.year == data_ref.year) &
                        (df_consumo["Datetime"].dt.date <= data_ref)
                        ]["Área Produtiva"].sum()

                    limites_restantes = limites_mes[limites_mes["Data"].dt.date > data_ref]
                    targets_restantes = limites_restantes[colunas_area_produtiva].sum().sum()
                    adicional_restante = limites_restantes["Data"].dt.date.nunique() * 24 * 13.75
                    consumo_previsto_mes = consumo_ate_agora + targets_restantes + adicional_restante

                    # Métricas
                    col1, col2 = st.columns(2)
                    col1.metric("🔋 Consumo máximo previsto para o mês (área produtiva)", f"{consumo_max_mes:.2f} kWh")
                    col2.metric("🔮 Consumo previsto para o mês (baseado no consumo atual + targets restantes)",
                                f"{consumo_previsto_mes:.2f} kWh")

                    # Tabela de previsão diária
                    st.subheader("📋 Previsão e Consumo Diário da Área Produtiva")
                    datas_unicas = sorted(limites_mes["Data"].dt.date.unique())
                    dados_tabela = []

                    for dia in datas_unicas:
                        limites_dia = limites_mes[limites_mes["Data"].dt.date == dia]
                        target_dia = limites_dia[colunas_area_produtiva].sum().sum() + 24 * 13.75
                        consumo_dia = df_consumo[df_consumo["Datetime"].dt.date == dia]["Área Produtiva"].sum()
                        saldo = target_dia - consumo_dia

                        dados_tabela.append({
                            "Data": dia.strftime("%Y-%m-%d"),
                            "Consumo Previsto (kWh)": round(target_dia, 2),
                            "Consumo Real (kWh)": round(consumo_dia, 2),
                            "Saldo do Dia (kWh)": round(saldo, 2)
                        })

                    df_tabela = pd.DataFrame(dados_tabela)

                    # Simulação de Monte Carlo - Gráfico Interativo com Plotly (com faixa de confiança)
                    st.subheader("📈 Simulação de Monte Carlo - Consumo Diário Futuro com Faixa de Confiança")

                    df_consumo["Data"] = pd.to_datetime(df_consumo["Datetime"]).dt.date
                    historico_diario = df_consumo[
                        (pd.to_datetime(df_consumo["Datetime"]).dt.month == data_ref.month) &
                        (pd.to_datetime(df_consumo["Datetime"]).dt.year == data_ref.year)
                        ].groupby("Data")["Área Produtiva"].sum()

                    if len(historico_diario) >= 5:
                        media = historico_diario.mean()
                        desvio = historico_diario.std()
                        dias_futuros = [datetime.strptime(d, "%Y-%m-%d").date() for d in df_tabela["Data"] if
                                        datetime.strptime(d, "%Y-%m-%d").date() > data_ref]
                        n_simulacoes = 1000
                        simulacoes = [np.random.normal(loc=media, scale=desvio, size=len(dias_futuros)) for _ in
                                      range(n_simulacoes)]
                        simulacoes = np.array(simulacoes)
                        media_simulada = simulacoes.mean(axis=0)
                        p5 = np.percentile(simulacoes, 5, axis=0)
                        p95 = np.percentile(simulacoes, 95, axis=0)

                        fig = go.Figure()

                        # Consumo real
                        fig.add_trace(go.Scatter(
                            x=historico_diario.index,
                            y=historico_diario.values,
                            mode='lines+markers',
                            name='Consumo Real',
                            line=dict(color='blue')
                        ))

                        # Faixa de confiança
                        fig.add_trace(go.Scatter(
                            x=dias_futuros,
                            y=p95,
                            mode='lines',
                            name='Percentil 95%',
                            line=dict(width=0),
                            showlegend=False
                        ))
                        fig.add_trace(go.Scatter(
                            x=dias_futuros,
                            y=p5,
                            mode='lines',
                            name='Faixa de Confiança 90%',
                            fill='tonexty',
                            fillcolor='rgba(255,165,0,0.2)',
                            line=dict(width=0)
                        ))

                        # Média das simulações
                        fig.add_trace(go.Scatter(
                            x=dias_futuros,
                            y=media_simulada,
                            mode='lines+markers',
                            name='Previsão Média',
                            line=dict(color='orange', dash='dash')
                        ))

                        # Meta diária
                        fig.add_trace(go.Scatter(
                            x=list(historico_diario.index) + dias_futuros,
                            y=[1250] * (len(historico_diario) + len(dias_futuros)),
                            mode='lines',
                            name='Meta Diária',
                            line=dict(color='green', dash='dot')
                        ))

                        fig.update_layout(
                            title='Previsão de Consumo com Monte Carlo - Área Produtiva',
                            xaxis_title='Data',
                            yaxis_title='Consumo Diário (kWh)',
                            legend_title='Legenda',
                            template='plotly_white'
                        )

                        st.plotly_chart(fig, use_container_width=True)

                        # Diagnóstico inteligente
                        saldo_total = historico_diario.sum() + media_simulada.sum() - 1250 * (
                                    len(historico_diario) + len(dias_futuros))
                        variabilidade = np.std(simulacoes)

                        if saldo_total < 0:
                            diagnostico = "A previsão indica que o consumo total da área produtiva deve ultrapassar a meta mensal de energia elétrica."
                        else:
                            diagnostico = "A previsão sugere que o consumo total da área produtiva deve permanecer dentro da meta mensal de energia elétrica."

                        legenda = (
                            f"O consumo real apresenta variações em torno da meta diária. "
                            f"A simulação de Monte Carlo mostra uma variabilidade de aproximadamente {variabilidade:.1f} kWh "
                            f"entre as trajetórias simuladas. {diagnostico}"
                        )

                        st.markdown(f"**📌 Diagnóstico Inteligente:** {legenda}")

                        # Análise interpretativa baseada nas simulações
                        targets_futuros = df_tabela[
                            df_tabela["Data"].apply(lambda d: datetime.strptime(d, "%Y-%m-%d").date() > data_ref)][
                            "Consumo Previsto (kWh)"].values

                        em_alta = 0
                        em_baixa = 0
                        estaveis = 0

                        for sim in simulacoes:
                            total_simulado = np.sum(sim)
                            total_target = np.sum(targets_futuros)
                            diferenca = total_simulado - total_target

                            if diferenca > 0.05 * total_target:
                                em_alta += 1
                            elif diferenca < -0.05 * total_target:
                                em_baixa += 1
                            else:
                                estaveis += 1

                        if em_alta > em_baixa and em_alta > estaveis:
                            tendencia = "alta"
                            risco = "há risco de ultrapassar os limites mensais de consumo"
                        elif em_baixa > em_alta and em_baixa > estaveis:
                            tendencia = "baixa"
                            risco = "há folga no consumo em relação ao limite"
                        else:
                            tendencia = "estável"
                            risco = "o consumo está dentro da faixa esperada"

                        st.markdown(f"""
                        ### 🔍 **Análise da Previsão de Consumo da Área Produtiva**

                        Com base nas simulações de Monte Carlo realizadas:

                        - **{em_alta}** simulações indicam tendência de alta no consumo  
                        - **{em_baixa}** simulações indicam tendência de queda  
                        - **{estaveis}** simulações indicam estabilidade  

                        📉 A tendência geral é **{tendencia}**, o que sugere que **{risco}**.
                        """)

                        import plotly.graph_objects as go
                        from datetime import timedelta

                        # Verifica se os dados estão disponíveis
                        if 'consumo' in st.session_state:
                            df = st.session_state.consumo.copy()
                            df['Data'] = df['Datetime'].dt.date
                            df_diario = df.groupby('Data')['Área Produtiva'].sum().reset_index()
                            df_diario['Data'] = pd.to_datetime(df_diario['Data'])
                            serie_historica = pd.Series(df_diario['Área Produtiva'].values, index=df_diario['Data'])

                            # ARIMA
                            modelo_arima = ARIMA(serie_historica, order=(1, 1, 1)).fit()
                            previsao_arima = modelo_arima.forecast(steps=30)
                            datas_futuras = pd.date_range(start=serie_historica.index[-1] + timedelta(days=1),
                                                          periods=30)

                            # Monte Carlo
                            simulacoes = 1000
                            simulacoes_mc = np.random.normal(loc=serie_historica.mean(), scale=serie_historica.std(),
                                                             size=(simulacoes, 30))
                            media_mc = simulacoes_mc.mean(axis=0)
                            p5 = np.percentile(simulacoes_mc, 5, axis=0)
                            p95 = np.percentile(simulacoes_mc, 95, axis=0)

                            # Meta
                            meta_diaria = 1250

                            # Gráfico Plotly
                            fig = go.Figure()
                            fig.add_trace(
                                go.Scatter(x=serie_historica.index, y=serie_historica.values, name='Consumo Real',
                                           line=dict(color='blue')))
                            fig.add_trace(go.Scatter(x=datas_futuras, y=previsao_arima, name='Previsão ARIMA',
                                                     line=dict(color='orange', dash='dash')))
                            fig.add_trace(go.Scatter(x=datas_futuras, y=media_mc, name='Monte Carlo (média)',
                                                     line=dict(color='green', dash='dot')))
                            fig.add_trace(go.Scatter(x=np.concatenate([datas_futuras, datas_futuras[::-1]]),
                                                     y=np.concatenate([p95, p5[::-1]]),
                                                     fill='toself', fillcolor='rgba(0,255,0,0.1)',
                                                     line=dict(color='rgba(255,255,255,0)'),
                                                     name='Monte Carlo (90% intervalo)'))
                            fig.add_trace(go.Scatter(x=[serie_historica.index.min(), datas_futuras[-1]],
                                                     y=[meta_diaria, meta_diaria], name='Meta Diária',
                                                     line=dict(color='crimson', dash='dot')))

                            fig.update_layout(title='🔍 Previsão de Consumo de Energia: ARIMA vs Monte Carlo',
                                              xaxis_title='Data', yaxis_title='Consumo (kWh)',
                                              legend=dict(orientation='h', y=1.02, x=1, xanchor='right'),
                                              template='plotly_white')

                            st.plotly_chart(fig, use_container_width=True)

                            #Gráfico de Comparativo Diário de novas metas

                            import plotly.graph_objects as go
                            import pandas as pd

                            st.subheader("📊 Comparativo Diário: Consumo Real vs Metas Originais e Ajustadas")

                            # Preparar dados
                            df_consumo = st.session_state.consumo.copy()
                            df_consumo["Data"] = df_consumo["Datetime"].dt.date
                            consumo_diario = df_consumo.groupby("Data")["Área Produtiva"].sum().reset_index()

                            df_limites = st.session_state.limites_df.copy()
                            df_limites["Data"] = pd.to_datetime(df_limites["Data"]).dt.date

                            # Filtrar mês e ano selecionado
                            mes = st.session_state.data_selecionada.month
                            ano = st.session_state.data_selecionada.year
                            df_limites = df_limites[
                                (pd.to_datetime(df_limites["Data"]).dt.month == mes) &
                                (pd.to_datetime(df_limites["Data"]).dt.year == ano)
                                ]
                            consumo_diario = consumo_diario[
                                (pd.to_datetime(consumo_diario["Data"]).dt.month == mes) &
                                (pd.to_datetime(consumo_diario["Data"]).dt.year == ano)
                                ]

                            # Calcular meta diária
                            colunas_area = ["MP&L", "GAHO", "CAG", "SEOB", "EBPC", "PMDC-OFFICE", "OFFICE + CANTEEN",
                                            "TRIM&FINAL"]
                            df_limites["Meta Horária"] = df_limites[colunas_area].sum(axis=1)
                            meta_diaria_df = df_limites.groupby("Data")["Meta Horária"].sum().reset_index()
                            meta_diaria_df.rename(columns={"Meta Horária": "Meta Original"}, inplace=True)

                            # Mesclar com consumo real
                            df_plot = meta_diaria_df.merge(consumo_diario, on="Data", how="left")
                            df_plot.rename(columns={"Área Produtiva": "Consumo Real"}, inplace=True)

                            # Calcular nova meta ajustada
                            hoje = st.session_state.data_selecionada
                            df_plot["Nova Meta Ajustada"] = df_plot["Meta Original"]

                            mask_passado = df_plot["Data"] <= hoje
                            mask_futuro = df_plot["Data"] > hoje

                            meta_total = df_plot["Meta Original"].sum()
                            consumo_real = df_plot.loc[mask_passado, "Consumo Real"].sum()
                            saldo = meta_total - consumo_real
                            dias_restantes = mask_futuro.sum()

                            if dias_restantes > 0:
                                nova_meta_valor = saldo / dias_restantes
                                df_plot.loc[mask_passado, "Nova Meta Ajustada"] = df_plot.loc[
                                    mask_passado, "Consumo Real"]
                                df_plot.loc[mask_futuro, "Nova Meta Ajustada"] = nova_meta_valor

                                # Ajuste final para garantir igualdade exata
                                diferenca_final = meta_total - df_plot["Nova Meta Ajustada"].sum()
                                if abs(diferenca_final) > 0.01:
                                    idx_ultimo = df_plot[mask_futuro].index[-1]
                                    df_plot.loc[idx_ultimo, "Nova Meta Ajustada"] += diferenca_final

                            # Gráfico interativo
                            fig = go.Figure()
                            fig.add_trace(go.Scatter(
                                x=df_plot["Data"], y=df_plot["Consumo Real"],
                                mode='lines+markers', name='Consumo Real Diário', line=dict(color='blue')
                            ))
                            fig.add_trace(go.Scatter(
                                x=df_plot["Data"], y=df_plot["Meta Original"],
                                mode='lines', name='Meta Original Diária', line=dict(dash='dash', color='black')
                            ))
                            fig.add_trace(go.Scatter(
                                x=df_plot["Data"], y=df_plot["Nova Meta Ajustada"],
                                mode='lines', name='Nova Meta Ajustada', line=dict(dash='dot', color='orange')
                            ))
                            fig.update_layout(
                                title='Consumo Diário da Área Produtiva vs Metas (Mês Selecionado)',
                                xaxis_title='Data',
                                yaxis_title='Energia (kWh)',
                                legend_title='Legenda',
                                hovermode='x unified',
                                template='plotly_white'
                            )
                            st.plotly_chart(fig, use_container_width=True)

                            # Métricas
                            st.markdown("### 📈 Resumo das Metas Mensais")
                            col1, col2 = st.columns(2)
                            col1.metric("🎯 Meta Mensal Original (kWh)", f"{df_plot['Meta Original'].sum():,.0f}")
                            col2.metric("🛠️ Meta Mensal Ajustada (kWh)", f"{df_plot['Nova Meta Ajustada'].sum():,.0f}")


                            #--------------------------
                            # Verifica se os dados estão disponíveis
                            if 'consumo' in st.session_state and 'data_selecionada' in st.session_state:
                                df = st.session_state.consumo.copy()
                                df['Datetime'] = pd.to_datetime(df['Datetime'])
                                df.set_index('Datetime', inplace=True)

                                data_base = pd.to_datetime(st.session_state.data_selecionada)
                                past_hours = 96
                                future_hours = 24

                                # Seleciona as últimas 48 horas antes da data base
                                start_time = data_base - timedelta(hours=past_hours)
                                df_past = df.loc[start_time:data_base]

                                if 'Área Produtiva' in df_past.columns and len(df_past) >= past_hours:
                                    y_hist = df_past['Área Produtiva'].tail(past_hours).values
                                    time_hist = df_past.tail(past_hours).index

                                    # Simular 100 trajetórias futuras
                                    n_simulations = 1000
                                    future_simulations = [
                                        y_hist[-1] + np.cumsum(np.random.normal(loc=0.1, scale=0.5, size=future_hours))
                                        for _ in range(n_simulations)
                                    ]
                                    time_future = pd.date_range(start=data_base + timedelta(hours=1),
                                                                periods=future_hours, freq='H')

                                    # Gráfico principal
                                    fig = go.Figure()

                                    # Linha preta do histórico
                                    fig.add_trace(go.Scatter(
                                        x=time_hist,
                                        y=y_hist,
                                        mode='lines',
                                        name='Histórico',
                                        line=dict(color='black')
                                    ))

                                    # Linhas coloridas das simulações futuras
                                    for sim in future_simulations:
                                        fig.add_trace(go.Scatter(
                                            x=time_future,
                                            y=sim,
                                            mode='lines',
                                            line=dict(color='rgba(0,100,255,0.2)'),
                                            showlegend=False
                                        ))

                                    # Distribuição lateral
                                    final_values = [sim[-1] for sim in future_simulations]
                                    hist_y = np.linspace(min(final_values), max(final_values), 50)
                                    hist_x = np.histogram(final_values, bins=hist_y)[0]
                                    hist_x = hist_x / max(hist_x) * 6  # escala para largura visual

                                    fig.add_trace(go.Scatter(
                                        x=[time_future[-1] + timedelta(hours=1)] * len(hist_y),
                                        y=hist_y,
                                        mode='markers+lines',
                                        marker=dict(size=hist_x, color='goldenrod', opacity=0.6),
                                        line=dict(width=0),
                                        name='Distribuição final'
                                    ))

                                    # Layout
                                    fig.update_layout(
                                        title="Forecasts com Monte Carlo Sampling",
                                        xaxis_title="Tempo",
                                        yaxis_title="Consumo de Energia - Área Produtiva",
                                        template="plotly_white"
                                    )

                                    st.plotly_chart(fig, use_container_width=True)
                                else:
                                    st.warning("Não há dados suficientes ou a coluna 'Área Produtiva' está ausente.")
                            else:
                                st.warning("Dados de consumo ou data selecionada não encontrados.")





                        else:
                            st.warning("Dados de consumo não encontrados em st.session_state.")



                with open("relatorio_month_prediction.html", "r", encoding="utf-8") as f:
                    html_content = f.read()

                st.markdown("### 📘 Relatório Técnico Detalhado")
                components.html(html_content, height=1000, scrolling=True)




    except Exception as e:
        st.error(f"Erro ao processar os dados: {e}")



