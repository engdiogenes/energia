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

# Definindo colunas da área produtiva globalmente para reuso
colunas_area_produtiva = [
    "MP&L", "GAHO", "CAG", "SEOB", "EBPC", "PMDC-OFFICE", "OFFICE + CANTEEN", "TRIM&FINAL"
]

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

        # Pré-cálculo do perfil horário e meta diária da Área Produtiva a partir do template de limites
        # Isso assume que o `limites_por_medidor_horario` (que representa um dia de template) está disponível.
        # Ele é usado como um perfil para desagregar previsões diárias em horárias.
        hourly_target_profile_productive_area = []
        for h in range(24):
            hourly_sum = 0
            for medidor in colunas_area_produtiva:
                if medidor in st.session_state.limites_por_medidor_horario and h < len(
                        st.session_state.limites_por_medidor_horario[medidor]):
                    hourly_sum += st.session_state.limites_por_medidor_horario[medidor][h]
            hourly_sum += 13.75  # Add the fixed value per hour
            hourly_target_profile_productive_area.append(hourly_sum)

        st.session_state.hourly_target_profile_productive_area = hourly_target_profile_productive_area
        st.session_state.typical_daily_target_from_template = sum(hourly_target_profile_productive_area)
        if st.session_state.typical_daily_target_from_template == 0:
            st.session_state.hourly_profile_percentages = [1 / 24] * 24  # Fallback to uniform if targets sum to zero
        else:
            st.session_state.hourly_profile_percentages = [x / st.session_state.typical_daily_target_from_template for x
                                                           in hourly_target_profile_productive_area]

    except Exception as e:
        st.warning(f"Erro ao carregar limites padrão: {e}")

st.markdown("""
    <style>
    .block-container {
        padding-top: 1rem;
        padding-bottom: 1rem;
    }
    /* Removida a linha que ocultava o cabeçalho e rodapé - ISSO CAUSAVA O PROBLEMA! */
    /* header, footer { visibility: hidden; } */ 
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


def get_daily_productive_area_target(target_date, limites_df, colunas_area_produtiva):
    """
    Calcula a meta diária da Área Produtiva para uma dada data.
    Se a data não estiver em limites_df, usa o template diário típico.
    """
    day_targets_df = limites_df[limites_df['Data'] == target_date]

    # Se há targets específicos para esta data, usa-os
    if not day_targets_df.empty:
        daily_total = 0
        for h in range(24):
            hourly_sum = 0
            # Encontra a linha para a hora específica
            hourly_row = day_targets_df[day_targets_df['Hora'] == h]
            if not hourly_row.empty:
                for medidor in colunas_area_produtiva:
                    if medidor in hourly_row.columns:
                        hourly_sum += hourly_row[medidor].iloc[0]  # Assume que há apenas uma entrada por hora
                hourly_sum += 13.75  # Adiciona o valor constante fixo por hora
            daily_total += hourly_sum
        return daily_total
    # Se não há targets específicos, usa o template diário típico pré-calculado
    elif 'typical_daily_target_from_template' in st.session_state:
        return st.session_state.typical_daily_target_from_template
    else:
        return 0  # Valor padrão se não houver template nem dados específicos


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
    dados[medidores] = dados[medidores].astype(float)  # Garante que os valores são floats para os cálculos

    # Define o módulo para a correção do wrap-around (relevante se houver o comportamento de contador de 32 bits, mas não para resets bruscos)
    MODULUS_VALUE = 2 ** 32  # 4294967296.0 - O valor máximo teórico para um contador de 32 bits antes do "estouro".

    # Define um limite razoável para o consumo horário máximo.
    # Qualquer diferença de leitura maior que este valor será considerada uma anomalia (reset, erro).
    # AJUSTE ESTE VALOR COM BASE NA CAPACIDADE REAL MÁXIMA DE CONSUMO POR HORA DOS SEUS MEDIDORES!
    MAX_PLAUSIBLE_HOURLY_CONSUMPTION = 10_000  # Exemplo: 10.000 kWh por hora.
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
                return np.nan  # Mantém NaN para a primeira leitura, ou se o diff for NaN

            # Caso 1: Diferença negativa. Medidor diminuiu, o que é um erro ou reset. Considera consumo 0.
            if raw_diff_val < 0:
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
                                consumo["PMDC-OFFICE"] + consumo["TRIM&FINAL"] + consumo[
                                    "OFFICE + CANTEEN"] + 13.75  # 13.75 é um valor constante por período
    consumo = consumo.drop(
        columns=["QGBT1-MPTF", "QGBT2-MPTF"])  # Drop only if these aren't needed downstream for other calcs
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

                # Re-cálculo do perfil horário e meta diária da Área Produtiva a partir do template de limites
                # Isso é crucial para a aba de ML que usa este perfil para desagregação
                hourly_target_profile_productive_area = []
                for h in range(24):
                    hourly_sum = 0
                    for medidor in colunas_area_produtiva:
                        if medidor in st.session_state.limites_por_medidor_horario and h < len(
                                st.session_state.limites_por_medidor_horario[medidor]):
                            hourly_sum += st.session_state.limites_por_medidor_horario[medidor][h]
                    hourly_sum += 13.75  # Add the fixed value per hour
                    hourly_target_profile_productive_area.append(hourly_sum)

                st.session_state.hourly_target_profile_productive_area = hourly_target_profile_productive_area
                st.session_state.typical_daily_target_from_template = sum(hourly_target_profile_productive_area)
                if st.session_state.typical_daily_target_from_template == 0:
                    st.session_state.hourly_profile_percentages = [
                                                                      1 / 24] * 24  # Fallback to uniform if targets sum to zero
                else:
                    st.session_state.hourly_profile_percentages = [
                        x / st.session_state.typical_daily_target_from_template for x in
                        hourly_target_profile_productive_area]

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

                    # colunas_area já definida globalmente

                    df_limites["Meta Horária"] = df_limites[colunas_area_produtiva].sum(axis=1) + 13.75
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
                """, unsafe_allow_html=True)

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
                """, unsafe_allow_html=True)

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
                # colunas_area_produtiva já definida globalmente

                # DataFrame de consumo e data selected
                df = st.session_state.consumo
                data_ref = st.session_state.data_selecionada

                # Filtrar o mês e ano da data selected
                df_mes = df[
                    (df["Datetime"].dt.month == data_ref.month) &
                    (df["Datetime"].dt.year == data_ref.year)
                    ]

                # Calcular o consumo total da área produtiva
                consumo_total_produtivo = df_mes[colunas_area_produtiva].sum().sum()

                # Exibir o resultado
                st.metric("🔧 Total consumption of the productive area in the month",
                          f"{consumo_total_produtivo:,.0f} kWh")

                # Ajustar a lista de medidores para o mapa, garantindo que colunas_area_produtiva esteja incluída
                medidores_para_mapa = list(set(colunas_area_produtiva + ["PCCB"]))  # PCCB é o "THIRD PARTS"

                consumo_por_medidor = df_mes[medidores_para_mapa].sum().to_dict()

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

                # Adiciona PCCB como um nó separado para 'THIRD PARTS'
                pccb_consumo = consumo_por_medidor.get("PCCB", 0)
                nodes.append(
                    Node(id="PCCB", label=f"Emissions Lab\n{pccb_consumo:,.0f} kWh", size=tamanho_no(pccb_consumo),
                         color=cor_no(len(medidores_para_mapa) - 1)))

                for idx, nome in enumerate(colunas_area_produtiva):  # Itera apenas sobre medidores produtivos
                    consumo = consumo_por_medidor.get(nome, 0)
                    label = f"{nome}\n{consumo:,.0f} kWh"
                    size = tamanho_no(consumo)
                    color = cor_no(idx)
                    nodes.append(Node(id=nome, label=label, size=size, color=color))

                edges = [
                            Edge(source="Full Plant", target="PRODUCTIVE AREAS"),
                            Edge(source="Full Plant", target="THIRD PARTS"),
                            Edge(source="THIRD PARTS", target="PCCB")  # Conecta PCCB a THIRD PARTS
                        ] + [
                            Edge(source="PRODUCTIVE AREAS", target=nome) for nome in colunas_area_produtiva
                        ]

                config = Config(width=1000, height=600, directed=True, hierarchical=True)
                agraph(nodes=nodes, edges=edges, config=config)

            with tabs[8]:
                st.subheader("⚙️ ML Prediction for Productive Area Consumption")

                if 'consumo' not in st.session_state or 'limites_df' not in st.session_state:
                    st.warning("Please load consumption data and limits first to enable ML prediction.")
                    st.stop()

                # 1. Prepare Data for ML
                df_consumo_area = st.session_state.consumo.copy()
                df_consumo_area['date'] = df_consumo_area['Datetime'].dt.date
                df_consumo_area_daily = df_consumo_area.groupby('date')['Área Produtiva'].sum().reset_index()
                df_consumo_area_daily.rename(columns={'Área Produtiva': 'consumption'}, inplace=True)

                # Adiciona targets diários
                df_consumo_area_daily['daily_target'] = df_consumo_area_daily['date'].apply(
                    lambda d: get_daily_productive_area_target(d, st.session_state.limites_df, colunas_area_produtiva)
                )

                # Adiciona day_num
                df_consumo_area_daily['day_num'] = (pd.to_datetime(df_consumo_area_daily['date']) - pd.to_datetime(
                    df_consumo_area_daily['date']).min()).dt.days

                # Ordena para consistência da série temporal
                df_consumo_area_daily = df_consumo_area_daily.sort_values('date').reset_index(drop=True)

                st.markdown("### 📈 Daily Consumption: Actual, Target & Prediction")

                # Input do usuário para o horizonte de previsão
                prediction_days_count = st.slider("Number of days to predict", 1, 30, 7)

                # Define a data de corte para os dados de treinamento (data selecionada na sidebar)
                cutoff_date = st.session_state.data_selecionada

                # Prepara os dados de treinamento (todos os dados até e incluindo cutoff_date)
                df_train = df_consumo_area_daily[df_consumo_area_daily['date'] <= cutoff_date].copy()

                # Gera datas futuras para previsão
                last_train_date = df_train['date'].max()
                future_dates = pd.date_range(start=last_train_date + timedelta(days=1), periods=prediction_days_count,
                                             freq='D').date

                # Cria DataFrame para futuras previsões com day_num e daily_target
                df_predict_future = pd.DataFrame({'date': future_dates})
                df_predict_future['day_num'] = (pd.to_datetime(df_predict_future['date']) - pd.to_datetime(
                    df_consumo_area_daily['date']).min()).dt.days

                # Atribui targets diários para dias futuros (recorre ao template típico se não houver limite específico)
                df_predict_future['daily_target'] = df_predict_future['date'].apply(
                    lambda d: get_daily_productive_area_target(d, st.session_state.limites_df, colunas_area_produtiva)
                )

                if df_train.empty:
                    st.warning(
                        "Not enough historical data to train the models. Please ensure data is loaded for dates prior to the selected day.")
                else:
                    X_train = df_train[['day_num', 'daily_target']]
                    y_train = df_train['consumption']

                    X_predict_future = df_predict_future[['day_num', 'daily_target']]

                    models = {
                        "Random Forest": RandomForestRegressor(n_estimators=100, random_state=42),
                        "Gradient Boosting": GradientBoostingRegressor(n_estimators=100, random_state=42),
                        "Linear Regression": LinearRegression()
                    }

                    daily_predictions = {}
                    model_metrics = []

                    for name, model in models.items():
                        try:
                            model.fit(X_train, y_train)
                            # Previsões para dias futuros
                            predictions_future = model.predict(X_predict_future)
                            daily_predictions[name] = pd.Series(predictions_future, index=df_predict_future['date'])

                            # Avalia performance no conjunto de treinamento para uma métrica geral
                            y_train_pred = model.predict(X_train)
                            mae = mean_absolute_error(y_train, y_train_pred)
                            rmse = np.sqrt(mean_squared_error(y_train, y_train_pred))

                            rmse_norm = rmse / y_train.mean() if y_train.mean() != 0 else 0
                            accuracy = max(0, 1 - rmse_norm)

                            model_metrics.append({
                                "Model": name,
                                "MAE (Train)": round(mae, 2),
                                "RMSE (Train)": round(rmse, 2),
                                "Accuracy (Train %)": round(accuracy * 100, 2)
                            })
                        except Exception as e:
                            st.warning(f"Could not train {name} model: {e}")
                            daily_predictions[name] = pd.Series([np.nan] * prediction_days_count,
                                                                index=df_predict_future['date'])

                    if model_metrics:
                        st.dataframe(pd.DataFrame(model_metrics).sort_values(by="Accuracy (Train %)", ascending=False),
                                     use_container_width=True)

                    # Plot das Previsões Diárias
                    fig_daily = go.Figure()

                    # Consumo Real (até a data de corte)
                    fig_daily.add_trace(go.Scatter(
                        x=df_train['date'],
                        y=df_train['consumption'],
                        mode='lines+markers',
                        name='Actual Consumption',
                        line=dict(color='blue')
                    ))

                    # Targets Diários (todos os targets disponíveis no histórico e futuros)
                    # Mescla o df_train (que tem as datas do histórico) e o df_predict_future (que tem as datas futuras)
                    # para criar um único DataFrame com todas as datas e seus respectivos daily_target
                    full_target_df = pd.concat([df_train[['date', 'daily_target']], df_predict_future[
                        ['date', 'daily_target']]]).drop_duplicates().sort_values('date')

                    fig_daily.add_trace(go.Scatter(
                        x=full_target_df['date'],
                        y=full_target_df['daily_target'],
                        mode='lines',
                        name='Daily Target',
                        line=dict(color='green', dash='dot')
                    ))

                    # Previsões
                    for name, preds in daily_predictions.items():
                        if not preds.isnull().all():
                            fig_daily.add_trace(go.Scatter(
                                x=preds.index,
                                y=preds.values,
                                mode='lines+markers',
                                name=f'Predicted ({name})',
                                line=dict(dash='dash')
                            ))

                    fig_daily.update_layout(
                        title='Daily Consumption: Actual, Target & Prediction',
                        xaxis_title='Date',
                        yaxis_title='Consumption (kWh)',
                        legend_title='Legend',
                        template='plotly_white'
                    )
                    st.plotly_chart(fig_daily, use_container_width=True)

                    st.markdown("### 📊 Hourly Consumption Prediction for a Future Day")

                    # Seleciona o melhor modelo para desagregação horária
                    if model_metrics:
                        best_model_name = \
                        pd.DataFrame(model_metrics).sort_values(by="Accuracy (Train %)", ascending=False).iloc[0][
                            'Model']
                        best_daily_predictions = daily_predictions.get(best_model_name, pd.Series())
                    else:
                        best_daily_predictions = pd.Series()  # Série vazia se nenhum modelo foi treinado

                    if not best_daily_predictions.empty:
                        # Permite que o usuário selecione qual dia predito deseja ver a quebra horária
                        future_day_options = list(best_daily_predictions.index)
                        if future_day_options:
                            selected_future_day_for_hourly = st.selectbox(
                                "Select a predicted day to view hourly breakdown:",
                                future_day_options,
                                format_func=lambda d: d.strftime('%Y-%m-%d')
                            )

                            predicted_daily_value = best_daily_predictions.loc[selected_future_day_for_hourly]

                            # Desagrega o valor diário predito em valores horários com base no perfil
                            if 'hourly_profile_percentages' in st.session_state and st.session_state.hourly_profile_percentages and st.session_state.typical_daily_target_from_template > 0:
                                # Ajusta o perfil para a soma diária prevista, se o perfil não for zero
                                predicted_hourly_values = [predicted_daily_value * p for p in
                                                           st.session_state.hourly_profile_percentages]
                            else:
                                predicted_hourly_values = [
                                                              predicted_daily_value / 24] * 24  # Fallback para distribuição uniforme se não houver perfil

                            # Obtém o perfil de target horário para referência
                            hourly_target_profile = st.session_state.hourly_target_profile_productive_area if 'hourly_target_profile_productive_area' in st.session_state else [
                                                                                                                                                                                   0] * 24

                            # Plot das previsões horárias
                            fig_hourly = go.Figure()
                            fig_hourly.add_trace(go.Scatter(
                                x=list(range(24)),
                                y=predicted_hourly_values,
                                mode='lines+markers',
                                name='Predicted Hourly Consumption',
                                line=dict(color='orange')
                            ))
                            fig_hourly.add_trace(go.Scatter(
                                x=list(range(24)),
                                y=hourly_target_profile,
                                mode='lines',
                                name='Hourly Target',
                                line=dict(color='green', dash='dot')
                            ))

                            fig_hourly.update_layout(
                                title=f'Hourly Consumption Prediction for {selected_future_day_for_hourly.strftime("%Y-%m-%d")}',
                                xaxis_title='Hour of Day',
                                yaxis_title='Consumption (kWh)',
                                legend_title='Legend',
                                template='plotly_white'
                            )
                            st.plotly_chart(fig_hourly, use_container_width=True)
                        else:
                            st.info("No future days predicted to show hourly breakdown.")
                    else:
                        st.info("No daily predictions available to disaggregate into hourly values.")

    except Exception as e:
        st.error(f"Erro ao processar os dados: {e}")

