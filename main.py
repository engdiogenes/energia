import streamlit as st
import pandas as pd
import io
import json
import datetime
from streamlit_calendar import calendar
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from statsmodels.tsa.arima.model import ARIMA
from datetime import timedelta
import streamlit.components.v1 as components
from streamlit_agraph import agraph, Node, Edge, Config
import matplotlib.colors as mcolors
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import numpy as np
from matplotlib import cm
import plotly.graph_objects as go
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.neighbors import KNeighborsRegressor
from sklearn.svm import SVR
from sklearn.metrics import mean_absolute_error, mean_squared_error
import plotly.graph_objects as go
import numpy as np

st.set_page_config(
    page_title="PowerTrack",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

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
    # Garante que números como "4,303,339.00" são lidos corretamente como 4303339.00
    return texto.replace(",", "")


def carregar_dados(dados_colados):
    dados = pd.read_csv(io.StringIO(limpar_valores(dados_colados)), sep="\t")
    dados["Datetime"] = pd.to_datetime(dados["Date"] + " " + dados["Time"], dayfirst=True)
    # Importante: Classificar os dados em ordem cronológica ascendente para o cálculo de diff()
    dados = dados.sort_values("Datetime").reset_index(drop=True)

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
    dados[medidores] = dados[medidores].astype(float) # Garante que os valores são floats para os cálculos

    # Define o módulo para a correção do wrap-around (relevante se houver o comportamento de contador de 32 bits, mas não para resets bruscos)
    MODULUS_VALUE = 2**32 # 4294967296.0 - O valor máximo teórico para um contador de 32 bits antes do "estouro".
    
    # Define um limite razoável para o consumo horário máximo.
    # Qualquer diferença de leitura maior que este valor será considerada uma anomalia (reset, erro).
    # AJUSTE ESTE VALOR COM BASE NA CAPACIDADE REAL MÁXIMA DE CONSUMO POR HORA DOS SEUS MEDIDORES!
    MAX_PLAUSIBLE_HOURLY_CONSUMPTION = 10_000 # Exemplo: 10.000 kWh por hora.
                                               # Salto de 787k ou 3.5M kWh é muito maior que isso e será capturado.

    consumo = dados[["Datetime"] + medidores].copy()

    for col in medidores:
        # Calcula a diferença bruta entre leituras consecutivas (current - previous)
        diff_raw = consumo[col].diff()

        # Aplica a lógica de correção:
        # Se a diferença for negativa (contador diminuiu, o que não é consumo), considera 0.
        # Se a diferença for positiva mas absurdamente grande, considera 0 (reset/anomalia).
        # Caso contrário, é consumo normal.
        def calculate_adjusted_consumption(raw_diff_val):
            if pd.isna(raw_diff_val):
                return np.nan # Mantém NaN para a primeira leitura, ou se o diff for NaN

            # Caso 1: Diferença negativa. Medidor diminuiu, o que é um erro ou reset. Considera consumo 0.
            # A lógica de wrap-around (x + MODULUS_VALUE if x < 0) só seria aplicada se o "wrap" levasse a um valor negativo
            # mas o contador continuasse incrementando a partir de lá. No seu caso, o salto para o negativo é acompanhado
            # de um salto posterior massivo para o positivo, que não é um wrap-around clássico.
            if raw_diff_val < 0:
                # Se desejar tratar um "wrap-around" de MaxPos para MinNeg:
                # potential_wrap = raw_diff_val + MODULUS_VALUE
                # if potential_wrap > 0 and potential_wrap < MAX_PLAUSIBLE_HOURLY_CONSUMPTION:
                #    return potential_wrap
                # else:
                #    return 0.0 # Ainda assim, se for um valor não plausível mesmo após wrap, zera.
                
                # Para os dados observados, a simples diminuição (raw_diff_val < 0) não é consumo.
                return 0.0

            # Caso 2: Diferença positiva, mas maior que o limite plausível. Isso é um salto de reset ou anomalia.
            elif raw_diff_val > MAX_PLAUSIBLE_HOURLY_CONSUMPTION:
                return 0.0

            # Caso 3: Diferença positiva e dentro do limite plausível. É consumo normal.
            else:
                return raw_diff_val

        consumo[col] = diff_raw.apply(calculate_adjusted_consumption)

    # Remove a primeira linha que terá NaN devido à operação diff() e ao calculate_adjusted_consumption
    # Usa `subset=medidores` para garantir que apenas as colunas de medidores sejam verificadas para NaN.
    consumo = consumo.dropna(subset=medidores)
    
    # Seus cálculos adicionais que dependem dos medidores já corrigidos
    consumo["TRIM&FINAL"] = consumo["QGBT1-MPTF"] + consumo["QGBT2-MPTF"]
    # Para "OFFICE + CANTEEN", se "OFFICE" for um medidor geral e "PMDC-OFFICE" um sub-medidor,
    # a diferença é o consumo da "CANTEEN". Se o resultado der negativo, significa que PMDC-OFFICE
    # consumiu mais que OFFICE, o que pode indicar erro ou bidirecionalidade.
    # Garanto que o resultado não seja negativo para consumo.
    consumo["OFFICE + CANTEEN"] = (consumo["OFFICE"] - consumo["PMDC-OFFICE"]).apply(lambda x: max(0.0, x))
    consumo["Área Produtiva"] = consumo["MP&L"] + consumo["GAHO"] + consumo["CAG"] + consumo["SEOB"] + consumo["EBPC"] + \
                                consumo["PMDC-OFFICE"] + consumo["TRIM&FINAL"] + consumo["OFFICE + CANTEEN"] + 13.75 # 13.75 é um valor constante por período
    consumo = consumo.drop(columns=["QGBT1-MPTF", "QGBT2-MPTF"]) # Drop only if these aren't needed downstream for other calcs
    return consumo


# st.title(" Energy data analyser")

with st.sidebar:
    # st.sidebar.image("logo.png", width=360)
    # st.logo("logo.png", size="Large", link=None, icon_image=None)
    st.markdown("""
        <h1 style='font-size: 28px; color: #262730; margin-bottom: 1rem;'>⚡ PowerTrack</h1>
    """, unsafe_allow_html=True)



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


    origem_dados = st.radio("Choose the data source:", ["Google Sheets", "Colar manualmente"])

    if origem_dados == "Google Sheets":
        dados_colados = obter_dados_do_google_sheets()
        # Converter os dados colados em DataFrame temporário para extrair a última data
        df_temp = pd.read_csv(io.StringIO(limpar_valores(dados_colados)), sep="\t")
        df_temp["Datetime"] = pd.to_datetime(df_temp["Date"] + " " + df_temp["Time"], dayfirst=True)
        if not df_temp.empty:
            df_temp["Datetime"] = pd.to_datetime(df_temp["Date"] + " " + df_temp["Time"], dayfirst=True)
            ultima_data = df_temp["Datetime"].max()
            st.sidebar.markdown(f"📅 **Last update:** {ultima_data.strftime('%d/%m/%Y %H:%M')}")
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

            # Lista de datas disponíveis
            datas_disponiveis = sorted(consumo["Datetime"].dt.date.unique())

            # Recuperar ou inicializar a data selecionada
            if "data_selecionada" not in st.session_state:
                st.session_state.data_selecionada = datas_disponiveis[-1]

            # Navegação por botões
            col_a, col_b, col_c = st.sidebar.columns([1, 2, 1])
            with col_a:
                if st.button("◀", key="dia_anterior"):
                    idx = datas_disponiveis.index(st.session_state.data_selecionada)
                    if idx > 0:
                        st.session_state.data_selecionada = datas_disponiveis[idx - 1]
            with col_c:
                if st.button("▶", key="dia_posterior"):
                    idx = datas_disponiveis.index(st.session_state.data_selecionada)
                    if idx < len(datas_disponiveis) - 1:
                        st.session_state.data_selecionada = datas_disponiveis[idx + 1]

            # Campo de seleção de data
            data_selecionada = st.sidebar.date_input(
                "Select the date",
                value=st.session_state.data_selecionada,
                min_value=min(datas_disponiveis),
                max_value=max(datas_disponiveis)
            )
            st.session_state.data_selecionada = data_selecionada

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

            tabs = st.tabs(["Overview", "Per meter", "Targets", "Dashboard", "Calendar", "Conversion ",
                            "Month prediction", "Meter's layout", "Ml prediction"])

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

                # Gráfico de consumo de cada prédio/dia para as áreas produtivas
                st.subheader(" Daily Consumption per Meter")
                consumo_diario = consumo.copy()
                consumo_diario["Data"] = consumo_diario["Datetime"].dt.date
                consumo_agrupado = consumo_diario.groupby("Data")[medidores_disponiveis].sum().reset_index()
                medidores_calendario = st.multiselect(
                    "Select the gauges for the calendar:",
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
                # consumo diário do Mês

                # Carregar os dados do Google Sheets (substitua 'dados_colados' pela variável real)
                df = pd.read_csv(io.StringIO(limpar_valores(dados_colados)), sep="\t")
                df["Datetime"] = pd.to_datetime(df["Date"] + " " + df["Time"], dayfirst=True)
                df = df.sort_values("Datetime").reset_index(drop=True)

                # Identificar colunas de medidores
                colunas_medidores = [col for col in df.columns if col not in ["Date", "Time", "Datetime"]]

                # Calcular consumo horário por diferença
                df_consumo = df[["Datetime"] + colunas_medidores].copy()
                
                # REPLICANDO A LÓGICA DE TRATAMENTO DE ANOMALIAS PARA ESTE DATAFRAME TEMPORÁRIO
                # O mesmo MAX_PLAUSIBLE_HOURLY_CONSUMPTION deve ser usado.
                # Assumindo que 10_000 kWh/hora é um limite seguro para consumo plausível.
                LOCAL_MAX_PLAUSIBLE_HOURLY_CONSUMPTION = 10_000 
                # Função para aplicar a lógica
                def apply_local_consumption_logic(raw_diff_val):
                    if pd.isna(raw_diff_val):
                        return np.nan
                    if raw_diff_val < 0:
                        return 0.0
                    elif raw_diff_val > LOCAL_MAX_PLAUSIBLE_HOURLY_CONSUMPTION:
                        return 0.0
                    else:
                        return raw_diff_val

                for col in colunas_medidores:
                    df_consumo[col] = df_consumo[col].diff().apply(apply_local_consumption_logic)

                df_consumo = df_consumo.dropna().reset_index(drop=True)
                df_consumo["Data"] = df_consumo["Datetime"].dt.date

                # Contar quantas diferenças por dia (consumos horários)
                contagem_por_dia = df_consumo.groupby("Data").size()

                # Considerar apenas dias com 24 diferenças (ou seja, 25 leituras)
                dias_completos = contagem_por_dia[contagem_por_dia == 24].index
                df_filtrado = df_consumo[df_consumo["Data"].isin(dias_completos)]

                # Agregar consumo diário
                df_diario = df_filtrado.groupby("Data")[colunas_medidores].sum().reset_index()

                # Exibir o resultado
                st.subheader("📅 Daily consumption for the month")
                st.dataframe(df_diario, use_container_width=True)
                # fim consumo diário do Mês

                st.divider()
            with tabs[1]:
                st.subheader(" Graphs by Meter with Limit Curve")
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
                st.subheader(" Loaded Time Limits")

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
                    # Exibir metas mensais de consumo da área produtiva em MWh
                    st.subheader("📊 Monthly Consumption Targets for the Production Area (in MWh)")

                    df_limites = st.session_state.limites_df.copy()
                    df_limites["Data"] = pd.to_datetime(df_limites["Data"])

                    colunas_area = ["MP&L", "GAHO", "CAG", "SEOB", "EBPC", "PMDC-OFFICE", "OFFICE + CANTEEN",
                                    "TRIM&FINAL"]
                    df_limites["Meta Horária"] = df_limites[colunas_area].sum(axis=1) + 13.75
                    df_limites["Meta Diária"] = df_limites["Meta Horária"]  # já é por hora

                    df_limites["Ano"] = df_limites["Data"].dt.year
                    df_limites["Mês"] = df_limites["Data"].dt.month

                    meta_mensal_df = df_limites.groupby(["Ano", "Mês"])["Meta Diária"].sum().reset_index()
                    meta_mensal_df["Meta Mensal (MWh)"] = (meta_mensal_df["Meta Diária"] / 1000).round(2)
                    meta_mensal_df = meta_mensal_df.drop(columns=["Meta Diária"])

                    # Aplicar estilo para centralizar os valores
                    styled_df = meta_mensal_df.style.set_properties(**{
                        'text-align': 'center'
                    }).set_table_styles([{
                        'selector': 'th',
                        'props': [('text-align', 'center')]
                    }])

                    st.dataframe(styled_df, use_container_width=True)



                else:
                    st.warning("Nenhum limite foi carregado.")

            # TABS 3 - DASHBOARD
            with tabs[3]:
                st.subheader(" Summary Panel")
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
                st.subheader(" Consumption vs. Limit Charts")
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
                st.subheader("Interactive Consumption Calendar for the Production Area")

                st.markdown("""
                This section provides a visual overview of daily energy consumption in the production area.
                Each day is represented with a mini chart showing hourly usage compared to predefined limits.
                Use this calendar to identify patterns, detect anomalies, and monitor energy efficiency over time. 3 month a go only.
                """)

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
                st.title("CSV to JSON - Hourly Limits per Meter")
                st.markdown("""
                This section is reserved for the application creator and is intended solely for configuring and converting hourly consumption limits. 
                It allows the transformation of CSV files containing per-meter hourly limits into a JSON format compatible with the PowerTrack system. 
                This functionality ensures that reference data is properly structured and ready for use in the platform’s analysis and forecasting tools.
                """)

                uploaded_file = st.file_uploader("Upload the CSV file", type="csv")
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
                    # Verificar se o mês está completo (todos os dias com consumo real)
                    dias_com_consumo = set(
                        df_consumo[df_consumo["Datetime"].dt.month == data_ref.month]["Datetime"].dt.date.unique())
                    dias_esperados = set(limites_mes["Data"].dt.date.unique())
                    mes_completo = dias_esperados.issubset(dias_com_consumo)

                    if mes_completo:
                        consumo_previsto_mes = df_consumo[
                            (df_consumo["Datetime"].dt.month == data_ref.month) &
                            (df_consumo["Datetime"].dt.year == data_ref.year)
                            ]["Área Produtiva"].sum()
                    else:
                        consumo_previsto_mes = consumo_ate_agora + targets_restantes + adicional_restante

                    # Métricas
                    col1, col2 = st.columns(2)
                    col1.metric("🔋 Actual consumption accumulated up to the selected date (production area)", f"{consumo_max_mes:.2f} kWh")
                    col2.metric("🔮 Expected consumption for the month (based on current consumption + remaining targets)",
                                f"{consumo_previsto_mes:.2f} kWh")
                    # Calcular soma dos targets da área produtiva até o dia selected (mês atual)
                    targets_ate_hoje = limites_mes[limites_mes["Data"].dt.date <= data_ref][
                        colunas_area_produtiva].sum().sum()
                    adicional_ate_hoje = limites_mes[limites_mes["Data"].dt.date <= data_ref][
                                             "Data"].dt.date.nunique() * 24 * 13.75
                    meta_ate_hoje = targets_ate_hoje + adicional_ate_hoje

                    # Calcular consumo real da área produtiva até o dia selected (mês atual)
                    consumo_real_ate_hoje = df_consumo[
                        (df_consumo["Datetime"].dt.month == data_ref.month) &
                        (df_consumo["Datetime"].dt.year == data_ref.year) &
                        (df_consumo["Datetime"].dt.date <= data_ref)
                        ]["Área Produtiva"].sum()

                    # Exibir métricas adicionais
                    col3, col4 = st.columns(2)
                    col3.metric("🎯 Target accumulated up to the selected date (production area)", f"{meta_ate_hoje:,.0f} kWh")
                    col4.metric("⚡ Actual consumption accumulated up to the selected date (production area)",
                                f"{consumo_real_ate_hoje:,.0f} kWh")

                    # Estimativa total com base no padrão atual de consumo
                    df_consumo["Data"] = pd.to_datetime(df_consumo["Datetime"]).dt.date
                    df_diario = df_consumo.groupby("Data")["Área Produtiva"].sum().reset_index()
                    df_diario["Data"] = pd.to_datetime(df_diario["Data"])

                    # Filtrar mês de referência
                    df_mes = df_diario[
                        (df_diario["Data"].dt.month == data_ref.month) &
                        (df_diario["Data"].dt.year == data_ref.year)
                        ]

                    consumo_ate_hoje = df_mes["Área Produtiva"].sum()
                    dias_consumidos = df_mes["Data"].nunique()
                    media_diaria = consumo_ate_hoje / dias_consumidos if dias_consumidos > 0 else 0
                    dias_no_mes = pd.Period(data_ref.strftime("%Y-%m")).days_in_month
                    dias_restantes = dias_no_mes - dias_consumidos
                    consumo_estimado_total = consumo_ate_hoje + (media_diaria * dias_restantes)

                    # Calcular meta mensal real
                    colunas_area_produtiva = ["MP&L", "GAHO", "CAG", "SEOB", "EBPC", "PMDC-OFFICE", "OFFICE + CANTEEN",
                                              "TRIM&FINAL"]
                    df_limites["Meta Horária"] = df_limites[colunas_area_produtiva].sum(axis=1) + 13.75
                    meta_mensal = df_limites[
                        (df_limites["Data"].dt.month == data_ref.month) &
                        (df_limites["Data"].dt.year == data_ref.year)
                        ]["Meta Horária"].sum()

                    consumo_ate_hoje = df_mes["Área Produtiva"].sum()
                    dias_consumidos = df_mes["Data"].nunique()
                    media_diaria = consumo_ate_hoje / dias_consumidos if dias_consumidos > 0 else 0
                    dias_no_mes = pd.Period(data_ref.strftime("%Y-%m")).days_in_month
                    dias_restantes = dias_no_mes - dias_consumidos
                    consumo_estimado_total = consumo_ate_hoje + (media_diaria * dias_restantes)

                    # Calcular meta mensal real
                    df_limites["Data"] = pd.to_datetime(df_limites["Data"])
                    df_limites["Meta Horária"] = df_limites[colunas_area_produtiva].sum(axis=1) + 13.75
                    meta_mensal = df_limites[
                        (df_limites["Data"].dt.month == data_ref.month) &
                        (df_limites["Data"].dt.year == data_ref.year)
                        ]["Meta Horária"].sum()

                    # Exibir métrica
                    delta_estimado = consumo_estimado_total - meta_mensal
                    st.metric(
                        label="📈 Estimativa de consumo com Base no Padrão Atual",
                        value=f"{consumo_estimado_total:,.0f} kWh",
                        delta=f"{delta_estimado:,.0f} kWh",
                        delta_color="inverse" if delta_estimado < 0 else "normal"
                    )

                    consumo_ate_hoje = df_mes["Área Produtiva"].sum()
                    dias_consumidos = df_mes["Data"].nunique()
                    media_diaria = consumo_ate_hoje / dias_consumidos if dias_consumidos > 0 else 0
                    dias_no_mes = pd.Period(data_ref.strftime("%Y-%m")).days_in_month
                    dias_restantes = dias_no_mes - dias_consumidos
                    consumo_estimado_total = consumo_ate_hoje + (media_diaria * dias_restantes)

                    # Calcular meta mensal real
                    df_limites["Data"] = pd.to_datetime(df_limites["Data"])
                    df_limites["Meta Horária"] = df_limites[colunas_area_produtiva].sum(axis=1) + 13.75
                    meta_mensal = df_limites[
                        (df_limites["Data"].dt.month == data_ref.month) &
                        (df_limites["Data"].dt.year == data_ref.year)
                        ]["Meta Horária"].sum()

                    # Tabela de previsão diária
                    st.subheader("📋Forecast and Daily Consumption of the Production Area")
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
                    st.subheader("📈 Monte Carlo Simulation - Future Daily Consumption with Confidence Interval")

                    df_consumo["Data"] = pd.to_datetime(df_consumo["Datetime"]).dt.date
                    historico_diario = df_consumo[
                        (pd.to_datetime(df_consumo["Datetime"]).dt.month == data_ref.month) &
                        (pd.to_datetime(df_consumo["Datetime"]).dt.year == data_ref.year)
                        ].groupby("Data")["Área Produtiva"].sum()

                    if len(historico_diario) >= 2:
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
                        # Meta diária real a partir do JSON
                        df_limites = st.session_state.limites_df.copy()
                        df_limites["Data"] = pd.to_datetime(df_limites["Data"]).dt.date

                        colunas_area = ["MP&L", "GAHO", "CAG", "SEOB", "EBPC", "PMDC-OFFICE", "OFFICE + CANTEEN",
                                        "TRIM&FINAL"]
                        df_limites["Meta Horária"] = df_limites[colunas_area].sum(axis=1) + 13.75
                        meta_diaria_df = df_limites.groupby("Data")["Meta Horária"].sum().reset_index()

                        # Filtrar apenas o mês e ano da data selecionada
                        data_base = st.session_state.data_selecionada
                        meta_diaria_df["Data"] = pd.to_datetime(meta_diaria_df["Data"], errors='coerce')
                        meta_diaria_df = meta_diaria_df.dropna(subset=["Data"])

                        meta_diaria_df = meta_diaria_df[
                            (meta_diaria_df["Data"].dt.month == data_base.month) &
                            (meta_diaria_df["Data"].dt.year == data_base.year)
                            ]

                        meta_diaria_df.columns = ["Data", "Meta Horária"]

                        # Adicionar linha de metas reais ao gráfico
                        fig.add_trace(go.Scatter(
                            x=meta_diaria_df["Data"],
                            y=meta_diaria_df["Meta Horária"],
                            mode='lines',
                            name='Meta Diária Real',
                            line=dict(color='green', dash='dot')
                        ))

                        fig.update_layout(
                            title='Consumption Forecasting with Monte Carlo - Production Area',
                            xaxis_title='Data',
                            yaxis_title='Consumo Diário (kWh)',
                            legend_title='Legenda',
                            template='plotly_white'
                        )

                        st.plotly_chart(fig, use_container_width=True)

                        # Diagnóstico inteligente
                        # Calcular metas reais do JSON para os dias do histórico e futuros
                        df_limites = st.session_state.limites_df.copy()
                        df_limites["Data"] = pd.to_datetime(df_limites["Data"]).dt.date

                        colunas_area = ["MP&L", "GAHO", "CAG", "SEOB", "EBPC", "PMDC-OFFICE", "OFFICE + CANTEEN",
                                        "TRIM&FINAL"]
                        df_limites["Meta Horária"] = df_limites[colunas_area].sum(axis=1) + 13.75
                        meta_diaria_df = df_limites.groupby("Data")["Meta Horária"].sum().reset_index()
                        meta_diaria_df["Data"] = pd.to_datetime(meta_diaria_df["Data"], errors='coerce')

                        # Filtrar metas para os dias do histórico e futuros
                        datas_relevantes = list(historico_diario.index) + dias_futuros
                        meta_total = meta_diaria_df[meta_diaria_df["Data"].isin(datas_relevantes)]["Meta Horária"].sum()

                        # Novo saldo total com base nas metas reais
                        saldo_total = historico_diario.sum() + media_simulada.sum() - meta_total

                        variabilidade = np.std(simulacoes)

                        if saldo_total > 0:
                            diagnostico = "The forecast indicates that total consumption in the productive area is expected to exceed the monthly electricity target."
                        else:
                            diagnostico = "A previsão sugere que o consumo total da área produtiva deve permanecer dentro da meta mensal de energia elétrica."

                        legenda = (
                            f"Actual consumption varies around the daily target."
                            f"The Monte Carlo simulation shows a variability of approximately {variabilidade:.1f} kWh "
                            f"between the simulated trajectories. {diagnostico}"
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
                            tendencia = "high"
                            risco = "there is a risk of exceeding the monthly consumption limits"
                        elif em_baixa > em_alta and em_baixa > estaveis:
                            tendencia = "low"
                            risco = "there is a gap in consumption in relation to the limit"
                        else:
                            tendencia = "stable"
                            risco = "consumption is within the expected range"

                        st.markdown(f"""
                        ### 🔍 **Analysis of Consumption Forecast for the Production Area**

                        Com base nas simulações de Monte Carlo realizadas:

                        - **{em_alta}** simulations indicate upward trend in consumption  
                        - **{em_baixa}** simulations indicate downward trend  
                        - **{estaveis}** simulations indicate stability  

                        📉 The general trend is **{tendencia}**, which suggests that **{risco}**.
                        """)



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

                            # Meta diária real a partir do JSON
                            df_limites = st.session_state.limites_df.copy()
                            df_limites["Data"] = pd.to_datetime(df_limites["Data"]).dt.date

                            colunas_area = ["MP&L", "GAHO", "CAG", "SEOB", "EBPC", "PMDC-OFFICE", "OFFICE + CANTEEN",
                                            "TRIM&FINAL"]
                            df_limites["Meta Horária"] = df_limites[colunas_area].sum(axis=1) + 13.75
                            meta_diaria_df = df_limites.groupby("Data")["Meta Horária"].sum().reset_index()

                            # Adicionar linha de metas reais ao gráfico
                            fig.add_trace(go.Scatter(
                                x=meta_diaria_df["Data"],
                                y=meta_diaria_df["Meta Horária"],
                                mode='lines',
                                name='Meta Diária Real',
                                line=dict(color='crimson', dash='dot')
                            ))

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

                            # Meta diária real a partir do JSON
                            df_limites = st.session_state.limites_df.copy()
                            df_limites["Data"] = pd.to_datetime(df_limites["Data"]).dt.date

                            colunas_area = ["MP&L", "GAHO", "CAG", "SEOB", "EBPC", "PMDC-OFFICE", "OFFICE + CANTEEN",
                                            "TRIM&FINAL"]
                            df_limites["Meta Horária"] = df_limites[colunas_area].sum(axis=1) + 13.75
                            meta_diaria_df = df_limites.groupby("Data")["Meta Horária"].sum().reset_index()

                            # Garantir que a linha da meta vá até o fim do mês da data selected
                            data_base = st.session_state.data_selecionada
                            ultimo_dia_mes = datetime(data_base.year, data_base.month + 1, 1) - timedelta(
                                days=1) if data_base.month < 12 else datetime(data_base.year, 12, 31)

                            datas_completas = pd.date_range(start=meta_diaria_df["Data"].min(),
                                                            end=ultimo_dia_mes.date(), freq='D')
                            meta_diaria_df = meta_diaria_df.set_index("Data").reindex(datas_completas).fillna(
                                method='ffill').reset_index()
                            meta_diaria_df.columns = ["Data", "Meta Horária"]

                            # Adicionar linha de metas reais ao gráfico
                            fig.add_trace(go.Scatter(
                                x=meta_diaria_df["Data"],
                                y=meta_diaria_df["Meta Horária"],
                                mode='lines',
                                name='Meta Diária Real',
                                line=dict(color='crimson', dash='dot')
                            ))

                            fig.update_layout(title='🔍 Energy Consumption Forecasting: ARIMA vs Monte Carlo',
                                              xaxis_title='Data', yaxis_title='Consumo (kWh)',
                                              legend=dict(orientation='h', y=1.02, x=1, xanchor='right'),
                                              template='plotly_white')

                            st.plotly_chart(fig, use_container_width=True)

                            # Gráfico de Comparativo Diário de novas metas

                            import plotly.graph_objects as go
                            import pandas as pd

                            st.subheader(
                                "📊 Daily Comparison: Actual Consumption vs. Original and Adjusted Targets (Proportional Distribution)")

                            # Preparar dados
                            df_consumo = st.session_state.consumo.copy()
                            df_consumo["Data"] = df_consumo["Datetime"].dt.date
                            consumo_diario = df_consumo.groupby("Data")["Área Produtiva"].sum().reset_index()

                            df_limites = st.session_state.limites_df.copy()
                            df_limites["Data"] = pd.to_datetime(df_limites["Data"]).dt.date

                            # Filtrar mês e ano selected
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
                            df_limites["Meta Horária"] = df_limites[colunas_area].sum(axis=1) + 13.75
                            meta_diaria_df = df_limites.groupby("Data")["Meta Horária"].sum().reset_index()
                            meta_diaria_df.rename(columns={"Meta Horária": "Meta Original"}, inplace=True)

                            # Mesclar com consumo real
                            df_plot = meta_diaria_df.merge(consumo_diario, on="Data", how="left")
                            df_plot.rename(columns={"Área Produtiva": "Consumo Real"}, inplace=True)

                            # Calcular nova meta ajustada proporcional ao perfil de consumo
                            hoje = st.session_state.data_selecionada
                            df_plot["Nova Meta Ajustada"] = df_plot["Meta Original"]

                            mask_passado = df_plot["Data"] <= hoje
                            mask_futuro = df_plot["Data"] > hoje

                            meta_total = df_plot["Meta Original"].sum()
                            consumo_real = df_plot.loc[mask_passado, "Consumo Real"].sum()
                            saldo = meta_total - consumo_real

                            if mask_futuro.sum() > 0:
                                consumo_estimado = df_plot.loc[mask_futuro, "Meta Original"]
                                proporcoes = consumo_estimado / consumo_estimado.sum()
                                df_plot.loc[mask_passado, "Nova Meta Ajustada"] = df_plot.loc[
                                    mask_passado, "Consumo Real"]
                                df_plot.loc[mask_futuro, "Nova Meta Ajustada"] = proporcoes * saldo

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
                                title='Consumo Diário da Área Produtiva vs Metas (Distribuição Proporcional)',
                                xaxis_title='Data',
                                yaxis_title='Energia (kWh)',
                                legend_title='Legenda',
                                hovermode='x unified',
                                template='plotly_white'
                            )
                            st.plotly_chart(fig, use_container_width=True)
                            # Diagnóstico Interativo - Climatização Extra
                            st.subheader("🧠 Interactive Diagnosis - Extra Air Conditioning")

                            # Cálculo do saldo de energia até o momento
                            saldo_energia = meta_ate_hoje - consumo_real_ate_hoje

                            if saldo_energia >= 0:
                                horas_extras = saldo_energia / 785
                                dias_extras = horas_extras / 8
                                st.success(f"""
                                ✅ To date, there is a positive balance of **{saldo_energia:,.0f} kWh** energy.
                                This allows approximately **{horas_extras:.1f} horas** monthly air conditioning extras,
                                which is equivalent to approximately **{dias_extras:.1f} dias** complete with additional air conditioning.
                                """)
                            else:
                                horas_a_economizar = abs(saldo_energia) / 785
                                dias_a_economizar = horas_a_economizar / 8
                                st.error(f"""
                                ⚠️ Consumption in the production area to date has exceeded the target by **{abs(saldo_energia):,.0f} kWh**.
                                To return to the monthly limit, it will be necessary to save approximately**{horas_a_economizar:.1f} hours**
                                air conditioning, which represents approximately **{dias_a_economizar:.1f} dias** for continuous use.
                                """)

                            # Métricas
                            st.markdown("### 📈 Resumo das Metas Mensais")
                            col1, col2 = st.columns(2)
                            col1.metric("🎯 Meta Mensal Original (kWh)", f"{df_plot['Meta Original'].sum():,.0f}")
                            col2.metric("🛠️ Meta Mensal Ajustada (kWh)", f"{df_plot['Nova Meta Ajustada'].sum():,.0f}")

                            # --------------------------


                            # Forecast Interativo com Monte Carlo
                            st.subheader("📈 Forecast Interativo com Monte Carlo")

                            if 'consumo' in st.session_state and 'data_selecionada' in st.session_state:
                                df = st.session_state.consumo.copy()
                                df['Datetime'] = pd.to_datetime(df['Datetime'])
                                df.set_index('Datetime', inplace=True)

                                data_base = pd.to_datetime(st.session_state.data_selecionada)
                                past_hours = 96
                                future_hours = 48

                                start_time = data_base - timedelta(hours=past_hours)
                                df_past = df.loc[start_time:data_base]

                                if 'Área Produtiva' in df_past.columns and len(df_past) >= past_hours:
                                    y_hist = df_past['Área Produtiva'].tail(past_hours).values
                                    time_hist = df_past.tail(past_hours).index

                                    # Simular 100 trajetórias futuras
                                    n_simulations = 500
                                    future_simulations = [
                                        y_hist[-1] + np.cumsum(np.random.normal(loc=0.1, scale=0.5, size=future_hours))
                                        for _ in range(n_simulations)
                                    ]
                                    time_future = pd.date_range(start=data_base + timedelta(hours=1),
                                                                periods=future_hours, freq='H')

                                    # Paleta de cores variadas
                                    cmap = cm.get_cmap('tab20', n_simulations)
                                    colors = [f'rgba({int(r * 255)},{int(g * 255)},{int(b * 255)},0.4)' for r, g, b, _
                                              in cmap(np.linspace(0, 1, n_simulations))]

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
                                    for sim, color in zip(future_simulations, colors):
                                        fig.add_trace(go.Scatter(
                                            x=time_future,
                                            y=sim,
                                            mode='lines',
                                            line=dict(color=color),
                                            showlegend=False
                                        ))

                                    # Histograma lateral real
                                    final_values = [sim[-1] for sim in future_simulations]
                                    hist_counts, hist_bins = np.histogram(final_values, bins=30)
                                    bin_centers = 0.5 * (hist_bins[:-1] + hist_bins[1:])
                                    max_count = max(hist_counts)
                                    x_offset = time_future[-1] + timedelta(hours=1)

                                    for count, y in zip(hist_counts, bin_centers):
                                        fig.add_trace(go.Scatter(
                                            x=[x_offset, x_offset + timedelta(minutes=30 * count / max_count)],
                                            y=[y, y],
                                            mode='lines',
                                            line=dict(color='goldenrod', width=6),
                                            showlegend=False
                                        ))

                                    fig.update_layout(
                                        title="Forecasts com Monte Carlo Sampling",
                                        xaxis_title="Tempo",
                                        yaxis_title="Consumo de Energia - Área Produtiva",
                                        template="plotly_white"
                                    )

                                    st.plotly_chart(fig, use_container_width=True)
                                else:
                                    st.warning("There is insufficient data or the ‘Production Area’ column is missing.")
                            else:
                                st.warning("Consumption data or selected date not found.")

                        else:
                            st.warning("Consumption data not found in st.session_state.")

                with open("relatorio_month_prediction.html", "r", encoding="utf-8") as f:
                    html_content = f.read()

                st.markdown("### 📘 Relatório Técnico Detalhado")
                components.html(html_content, height=1000, scrolling=True)


            with tabs[7]:  # ou ajuste o índice conforme necessário
                st.subheader("📍 Meter's Layout")

                data_ref = st.session_state.data_selecionada
                df_mes = st.session_state.consumo[
                    (st.session_state.consumo["Datetime"].dt.month == data_ref.month) &
                    (st.session_state.consumo["Datetime"].dt.year == data_ref.year)
                    ]

                medidores = [
                    "MP&L", "GAHO", "MAIW", "CAG", "SEOB", "EBPC",
                    "PMDC-OFFICE", "TRIM&FINAL", "OFFICE + CANTEEN", "PCCB"
                ]
                # Medidores da área produtiva
                medidores_produtivos = [
                    "MP&L", "GAHO", "MAIW", "CAG", "SEOB", "EBPC",
                    "PMDC-OFFICE", "TRIM&FINAL", "OFFICE + CANTEEN"
                ]

                # DataFrame de consumo e data selected
                df = st.session_state.consumo
                data_ref = st.session_state.data_selecionada

                # Filtrar o mês e ano da data selected
                df_mes = df[
                    (df["Datetime"].dt.month == data_ref.month) &
                    (df["Datetime"].dt.year == data_ref.year)
                    ]

                # Calcular o consumo total da área produtiva
                consumo_total_produtivo = df_mes[medidores_produtivos].sum().sum()

                # Exibir o resultado
                st.metric("🔧 Total consumption of the productive area in the month", f"{consumo_total_produtivo:,.0f} kWh")

                consumo_por_medidor = df_mes[medidores].sum().to_dict()

                # Normalização para tamanho e cor
                valores = list(consumo_por_medidor.values())
                min_val, max_val = min(valores), max(valores)
                norm = mcolors.Normalize(vmin=min_val, vmax=max_val)
                cmap = cm.get_cmap('tab10')


                def tamanho_no(valor):
                    return 20 + 30 * ((valor - min_val) / (max_val - min_val)) if max_val > min_val else 30


                def cor_no(idx):
                    rgba = cmap(idx % 10)
                    return mcolors.to_hex(rgba)


                nodes = [
                    Node(id="Full Plant", label="Full Plant", size=50, color="#1f77b4"),
                    Node(id="PRODUCTIVE AREAS", label="PRODUCTIVE AREAS", size=35, color="#2ca02c"),
                    Node(id="THIRD PARTS", label="THIRD PARTS", size=35, color="#ff7f0e"),
                ]

                for idx, nome in enumerate(medidores):
                    consumo = consumo_por_medidor.get(nome, 0)
                    label = f"{'Emissions Lab' if nome == 'PCCB' else nome}\n{consumo:,.0f} kWh"
                    size = tamanho_no(consumo)
                    color = cor_no(idx)
                    nodes.append(Node(id=nome, label=label, size=size, color=color))

                edges = [
                            Edge(source="Full Plant", target="PRODUCTIVE AREAS"),
                            Edge(source="Full Plant", target="THIRD PARTS"),
                        ] + [
                            Edge(source="PRODUCTIVE AREAS", target=nome) for nome in medidores if nome != "PCCB"
                        ] + [
                            Edge(source="THIRD PARTS", target="PCCB")
                        ]

                config = Config(width=1000, height=600, directed=True, hierarchical=True)
                agraph(nodes=nodes, edges=edges, config=config)

            with tabs[8]:
                st.subheader("⚙️ ML prediction")

                # Converter dados horários em consumo diário
                df_diario = st.session_state.consumo.copy()
                df_diario["date"] = pd.to_datetime(df_diario["Datetime"]).dt.date
                df_diario = df_diario.groupby("date")["Área Produtiva"].sum().reset_index()
                df_diario.columns = ["date", "consumption"]

                selected_day = st.date_input("Select a day for prediction", value=st.session_state.data_selecionada)

                df = df_diario.sort_values("date")
                df["day_num"] = (pd.to_datetime(df["date"]) - pd.to_datetime(df["date"]).min()).dt.days
                selected_day = pd.to_datetime(selected_day)
                selected_day_num = (selected_day - pd.to_datetime(df["date"]).min()).days

                X = df[["day_num"]]
                y = df["consumption"]
                X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

                models = {
                    "Random Forest": RandomForestRegressor(n_estimators=100, random_state=42),
                    "Linear Regression": LinearRegression(),
                    "Gradient Boosting": GradientBoostingRegressor(n_estimators=100, random_state=42),
                    "K-Nearest Neighbors": KNeighborsRegressor(n_neighbors=3),
                    "Support Vector Regression": SVR()
                }

                results = []
                fig = go.Figure()

                selected_month = selected_day.month
                selected_year = selected_day.year
                df_month = df[(pd.to_datetime(df["date"]).dt.month == selected_month) & (
                            pd.to_datetime(df["date"]).dt.year == selected_year)]

                fig.add_trace(
                    go.Scatter(x=df_month["date"], y=df_month["consumption"], mode='lines+markers', name='Real'))

                for name, model in models.items():
                    model.fit(X_train, y_train)
                    y_pred = model.predict(X_test)
                    mae = mean_absolute_error(y_test, y_pred)
                    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
                    prediction = model.predict([[selected_day_num]])[0]
                    y_fit = model.predict(X)

                    fit_df = pd.DataFrame({"date": df["date"], "fit": y_fit})
                    fit_df_month = fit_df[(pd.to_datetime(fit_df["date"]).dt.month == selected_month) & (
                                pd.to_datetime(fit_df["date"]).dt.year == selected_year)]

                    fig.add_trace(
                        go.Scatter(x=fit_df_month["date"], y=fit_df_month["fit"], mode='lines', name=f'{name} Fit'))
                    fig.add_trace(go.Scatter(x=[selected_day], y=[prediction], mode='markers',
                                             name=f'{name} Prediction', marker=dict(size=10)))

                    rmse_norm = rmse / y_test.mean()
                    accuracy = max(0, 1 - rmse_norm)

                    results.append({
                        "Model": name,
                        "MAE": round(mae, 2),
                        "RMSE": round(rmse, 2),
                        "Accuracy (%)": round(accuracy * 100, 2),
                        "Prediction for selected day": round(prediction, 2)
                    })

                fig.update_layout(title="Energy Consumption Forecast (Selected Month)",
                                  xaxis_title="Date",
                                  yaxis_title="Consumption (kWh)",
                                  legend_title="Legend")
                st.plotly_chart(fig)

                results_df = pd.DataFrame(results).sort_values(by="Accuracy (%)", ascending=False).reset_index(
                    drop=True)
                st.subheader("📊 Model Performance and Predictions")
                st.dataframe(results_df, use_container_width=True)


    except Exception as e:
        st.error(f"Erro ao processar os dados: {e}")
