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
        limites_df = pd.read_json(CAMINHO_JSON_PADAO)
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

# Função principal para carregar e pré-processar os dados
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
    
    # Renomeia as colunas de acordo com os novos rótulos
    dados = dados.rename(columns=novos_rotulos)
    
    # Seleciona apenas as colunas dos medidores e a coluna Datetime
    medidores = list(novos_rotulos.values())
    consumo = dados[["Datetime"] + medidores].copy()

    # Garante que os valores dos medidores sejam float
    consumo[medidores] = consumo[medidores].astype(float)

    # Calcula a diferença de consumo horária para cada medidor
    # A correção está aqui: .clip(lower=0) no lugar de .abs()
    for col in medidores:
        consumo[col] = consumo[col].diff().clip(lower=0) # Garante que o consumo nunca seja negativo

    # Remove a primeira linha (que terá NaN após o diff)
    consumo = consumo.dropna()
    
    # Recria as colunas agregadas
    consumo["TRIM&FINAL"] = consumo["QGBT1-MPTF"] + consumo["QGBT2-MPTF"]
    consumo["OFFICE + CANTEEN"] = consumo["OFFICE"] - consumo["PMDC-OFFICE"]
    consumo["Área Produtiva"] = consumo["MP&L"] + consumo["GAHO"] + consumo["CAG"] + consumo["SEOB"] + consumo["EBPC"] + \
                                consumo["PMDC-OFFICE"] + consumo["TRIM&FINAL"] + consumo["OFFICE + CANTEEN"] + 13.75
    
    # Remove as colunas de medidores individuais que foram agregadas, se desejar
    # Se você precisar das colunas individuais em outras abas, remova esta linha
    consumo = consumo.drop(columns=["QGBT1-MPTF", "QGBT2-MPTF", "OFFICE", "PMDC-OFFICE"])
    
    return consumo


# --- Início da interface Streamlit ---
# st.session_state initialization (assuming it's done elsewhere or needs to be added)
if 'data_selecionada' not in st.session_state:
    st.session_state.data_selecionada = datetime.today().date()
if 'consumo_df' not in st.session_state:
    st.session_state.consumo_df = None
if 'df_gs_full' not in st.session_state:
    st.session_state.df_gs_full = None
if 'auth_successful' not in st.session_state:
    st.session_state.auth_successful = False
if 'limites_df' not in st.session_state:
    st.session_state.limites_df = pd.DataFrame(columns=["Timestamp", "Data", "Hora"] + [f"Medidor_{i}" for i in range(11)])
if 'limites_por_medidor_horario' not in st.session_state:
    st.session_state.limites_por_medidor_horario = {}


# --- Sidebar ---
st.sidebar.title("Configurações do PowerTrack")

with st.sidebar:
    st.image("https://www.adapta.com.br/wp-content/uploads/2024/07/logo_adapta_azul.png", width=200) # Ajuste o caminho ou URL da imagem
    st.title("PowerTrack - Monitoramento de Energia")

    data_source_option = st.radio("Selecione a fonte de dados:", ("Google Sheets", "Colar Dados CSV"))

    if data_source_option == "Google Sheets":
        st.subheader("Conectar ao Google Sheets")
        json_keyfile = st.secrets["gcp_service_account"]["google_credentials_json"]

        try:
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            creds = ServiceAccountCredentials.from_json_keyfile_dict(json_keyfile, scope)
            client = gspread.authorize(creds)
            st.session_state.auth_successful = True
        except Exception as e:
            st.error(f"Erro de autenticação Google Sheets: {e}")
            st.session_state.auth_successful = False

        if st.session_state.auth_successful:
            sheet_url = st.text_input("URL da planilha Google Sheets:", st.secrets["gcp_service_account"]["sheet_url"])
            if sheet_url:
                try:
                    spreadsheet = client.open_by_url(sheet_url)
                    worksheet = spreadsheet.worksheet("Dados") # Assumindo que a aba se chama "Dados"
                    st.session_state.df_gs_full = pd.DataFrame(worksheet.get_all_records())
                    
                    # Convertendo o DataFrame do Google Sheets para o formato esperado por carregar_dados
                    df_gs_str = st.session_state.df_gs_full.to_csv(sep='\t', index=False)
                    st.session_state.consumo_df = carregar_dados(df_gs_str)
                    
                    st.success("Dados do Google Sheets carregados com sucesso!")
                    st.write(f"Última atualização: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
                except Exception as e:
                    st.error(f"Erro ao carregar dados da planilha: {e}. Verifique a URL e permissões.")
                    st.session_state.consumo_df = None
            else:
                st.info("Por favor, insira a URL da planilha Google Sheets.")
        else:
            st.warning("Falha na autenticação do Google Sheets. Verifique suas credenciais.")

    else: # Colar Dados CSV
        st.subheader("Colar Dados CSV")
        dados_colados = st.text_area("Cole seus dados CSV brutos aqui (separador TAB, ponto como decimal):", height=300)
        if dados_colados:
            try:
                st.session_state.consumo_df = carregar_dados(dados_colados)
                st.success("Dados carregados com sucesso!")
            except Exception as e:
                st.error(f"Erro ao processar os dados colados: {e}. Verifique o formato CSV.")
                st.session_state.consumo_df = None
        else:
            st.info("Cole os dados brutos do seu medidor acima para começar.")

    st.markdown("---")
    st.subheader("Enviar Relatório por E-mail")
    email_destinatario = st.text_input("E-mail do destinatário:")
    if st.button("Enviar Relatório Diário por E-mail"):
        if email_destinatario and st.session_state.consumo_df is not None:
            # Implementar a lógica de geração e envio de e-mail aqui
            # Esta parte do código precisa dos detalhes do servidor SMTP e da lógica do relatório
            st.warning("Funcionalidade de envio de e-mail não implementada na íntegra neste exemplo.")
            # Exemplo de como você enviaria:
            # send_email(email_destinatario, "Relatório Diário PowerTrack", "Conteúdo do relatório...")
        else:
            st.error("Por favor, insira um e-mail e carregue os dados primeiro.")

# --- Conteúdo Principal (Tabs) ---
if st.session_state.consumo_df is not None:
    # Garantir que st.session_state.data_selecionada esteja no DataFrame carregado
    if st.session_state.consumo_df['Datetime'].min().date() > st.session_state.data_selecionada or \
       st.session_state.consumo_df['Datetime'].max().date() < st.session_state.data_selecionada:
        st.session_state.data_selecionada = st.session_state.consumo_df['Datetime'].min().date()

    data_selecionada_input = st.date_input("Selecione a data para análise:", value=st.session_state.data_selecionada,
                                           min_value=st.session_state.consumo_df['Datetime'].min().date(),
                                           max_value=st.session_state.consumo_df['Datetime'].max().date())
    st.session_state.data_selecionada = data_selecionada_input

    aba_overview, aba_per_meter, aba_targets, aba_dashboard, aba_calendar, aba_conversion, aba_month_prediction, aba_meter_layout, aba_ml_prediction = st.tabs(
        ["Overview", "Per meter", "Targets", "Dashboard", "Calendar", "Conversion", "Month prediction", "Meter's layout", "ML prediction"]
    )

    with aba_overview:
        st.header("Visão Geral do Consumo")
        # Filtrar dados para o dia selecionado
        df_dia = st.session_state.consumo_df[st.session_state.consumo_df["Datetime"].dt.date == st.session_state.data_selecionada].copy()

        if not df_dia.empty:
            # Cálculo do consumo total da planta e área produtiva
            consumo_total_planta = df_dia.drop(columns=["Datetime"]).sum().sum() # Soma de todos os medidores do dia
            consumo_area_produtiva = df_dia["Área Produtiva"].sum()

            # Obter limites diários para o dia selecionado (se disponível)
            limites_dia = st.session_state.limites_df[st.session_state.limites_df["Data"] == st.session_state.data_selecionada]
            
            # Ajustar a meta diária da planta para 24 horas caso o arquivo de limites seja menor
            meta_diaria_planta = 0
            if not limites_dia.empty:
                # Filtrar colunas relevantes para a soma (excluindo 'Timestamp', 'Data', 'Hora')
                cols_para_soma = [col for col in limites_dia.columns if col not in ["Timestamp", "Data", "Hora"]]
                meta_diaria_planta = limites_dia[cols_para_soma].sum().sum() # Soma dos limites horários de todos os medidores

            meta_area_produtiva = st.session_state.limites_df[st.session_state.limites_df["Data"] == st.session_state.limites_df["Data"].min()]["Área Produtiva"].sum()

            col1, col2 = st.columns(2)
            with col1:
                st.metric(label="Consumo Total da Planta (kWh) - Dia Selecionado", value=f"{consumo_total_planta:.2f}")
                st.metric(label="Meta Total da Planta (kWh) - Dia Selecionado", value=f"{meta_diaria_planta:.2f}")
                delta_planta = consumo_total_planta - meta_diaria_planta
                saldo_planta = meta_diaria_planta - consumo_total_planta
                st.metric(label="Delta (Consumo - Meta) (kWh)", value=f"{delta_planta:.2f}", delta=f"{delta_planta:.2f}")
                st.metric(label="Saldo (Meta - Consumo) (kWh)", value=f"{saldo_planta:.2f}", delta=f"{saldo_planta:.2f}", delta_color="inverse")

            with col2:
                st.metric(label="Consumo Área Produtiva (kWh) - Dia Selecionado", value=f"{consumo_area_produtiva:.2f}")
                st.metric(label="Meta Área Produtiva (kWh) - Dia Selecionado", value=f"{meta_area_produtiva:.2f}")
                delta_produtiva = consumo_area_produtiva - meta_area_produtiva
                saldo_produtiva = meta_area_produtiva - consumo_area_produtiva
                st.metric(label="Delta (Consumo - Meta) (kWh)", value=f"{delta_produtiva:.2f}", delta=f"{delta_produtiva:.2f}")
                st.metric(label="Saldo (Meta - Consumo) (kWh)", value=f"{saldo_produtiva:.2f}", delta=f"{saldo_produtiva:.2f}", delta_color="inverse")

            st.subheader("Consumo Diário por Medidor")
            medidores_disponiveis = [col for col in st.session_state.consumo_df.columns if col not in ["Datetime", "Date", "Time"]]
            medidores_selecionados = st.multiselect(
                "Selecione os medidores para exibir:",
                options=medidores_disponiveis,
                default=[m for m in ["MP&L", "GAHO", "MAIW", "TRIM&FINAL", "OFFICE + CANTEEN", "CAG", "SEOB", "EBPC", "PCCB", "Área Produtiva"] if m in medidores_disponiveis]
            )

            if medidores_selecionados:
                df_mes_plot = st.session_state.consumo_df[st.session_state.consumo_df["Datetime"].dt.month == st.session_state.data_selecionada.month]
                df_mes_plot = df_mes_plot.groupby(df_mes_plot["Datetime"].dt.date)[medidores_selecionados].sum().reset_index()
                df_mes_plot.rename(columns={"Datetime": "Date"}, inplace=True)

                fig_daily_consumption = go.Figure()
                for medidor in medidores_selecionados:
                    fig_daily_consumption.add_trace(go.Bar(
                        x=df_mes_plot["Date"],
                        y=df_mes_plot[medidor],
                        name=medidor
                    ))
                fig_daily_consumption.update_layout(
                    barmode='stack',
                    title='Consumo Diário por Medidor (Mês Atual)',
                    xaxis_title='Data',
                    yaxis_title='Consumo (kWh)',
                    legend_title='Medidor'
                )
                st.plotly_chart(fig_daily_consumption, use_container_width=True)
            else:
                st.info("Selecione pelo menos um medidor para exibir o gráfico de consumo diário.")

            st.subheader("Consumo Diário Detalhado (Mês Selecionado)")
            df_mes_sum = st.session_state.consumo_df[st.session_state.consumo_df["Datetime"].dt.month == st.session_state.data_selecionada.month].copy()
            df_mes_sum['Data'] = df_mes_sum['Datetime'].dt.date
            df_mes_sum_pivot = df_mes_sum.groupby('Data')[medidores_disponiveis].sum().reset_index()
            st.dataframe(df_mes_sum_pivot, use_container_width=True)

        else:
            st.warning("Não há dados para a data selecionada.")


    with aba_per_meter:
        st.header("Consumo Horário por Medidor")
        if not df_dia.empty:
            df_hora = df_dia.copy()
            df_hora["Hora"] = df_hora["Datetime"].dt.hour
            
            medidores_horario = [col for col in st.session_state.consumo_df.columns if col not in ["Datetime", "Date", "Time"]]

            if not medidores_horario:
                st.warning("Nenhum medidor encontrado para exibição.")
            else:
                num_medidores = len(medidores_horario)
                num_cols = 2 # Definir número de colunas para exibição dos gráficos
                cols = st.columns(num_cols)

                # Carregar os limites horários do dia de referência
                limites_ref = st.session_state.limites_df[st.session_state.limites_df["Data"] == st.session_state.limites_df["Data"].min()].set_index("Hora")
                
                for i, medidor in enumerate(medidores_horario):
                    with cols[i % num_cols]:
                        st.subheader(f"Medidor: {medidor}")
                        fig = go.Figure()

                        # Adicionar consumo horário
                        fig.add_trace(go.Scatter(x=df_hora["Hora"], y=df_hora[medidor], mode='lines+markers', name='Consumo (kWh)'))

                        # Adicionar linha de limite horário se disponível
                        if medidor in limites_ref.columns:
                            fig.add_trace(go.Scatter(x=limites_ref.index, y=limites_ref[medidor], mode='lines', name='Limite (kWh)', line=dict(dash='dot')))
                        
                        fig.update_layout(
                            title=f'Consumo Horário - {medidor} ({st.session_state.data_selecionada.strftime("%d/%m/%Y")})',
                            xaxis_title='Hora do Dia',
                            yaxis_title='Consumo (kWh)',
                            hovermode="x unified"
                        )
                        st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Não há dados para a data selecionada para análise horária.")

    with aba_targets:
        st.header("Configuração de Limites e Metas")
        if st.session_state.limites_df is not None and not st.session_state.limites_df.empty:
            st.subheader("Limites Horários Carregados")
            st.dataframe(st.session_state.limites_df, use_container_width=True)

            csv_limites = st.session_state.limites_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Baixar Limites (JSON)",
                data=json.dumps(st.session_state.limites_df.to_dict(orient="records")), # Garante JSON formatado
                file_name="limites_atuais.json",
                mime="application/json"
            )
            
            st.subheader("Metas Mensais da Área Produtiva")
            # Assumindo que a meta diária da área produtiva é a soma dos limites horários da área produtiva para um dia
            # e que o mês tem 30 dias de referência (ajuste conforme a necessidade)
            meta_diaria_area_produtiva = st.session_state.limites_df[st.session_state.limites_df["Data"] == st.session_state.limites_df["Data"].min()]["Área Produtiva"].sum()
            
            # Número de dias no mês selecionado
            dias_no_mes = (st.session_state.data_selecionada.replace(month=st.session_state.data_selecionada.month % 12 + 1, day=1) - timedelta(days=1)).day
            
            meta_mensal_area_produtiva_kwh = meta_diaria_area_produtiva * dias_no_mes
            meta_mensal_area_produtiva_mwh = meta_mensal_area_produtiva_kwh / 1000

            st.metric(label=f"Meta Mensal (MWh) para Área Produtiva ({st.session_state.data_selecionada.strftime('%B %Y')})", 
                      value=f"{meta_mensal_area_produtiva_mwh:.2f} MWh")

        else:
            st.info("Nenhum limite de consumo carregado. Carregue um arquivo JSON ou defina-o na aba 'Conversion'.")

    with aba_dashboard:
        st.header("Dashboard de Medidores")
        if not df_dia.empty:
            df_hora = df_dia.copy()
            df_hora["Hora"] = df_hora["Datetime"].dt.hour
            medidores_dashboard = [col for col in st.session_state.consumo_df.columns if col not in ["Datetime", "Date", "Time"]]

            if not medidores_dashboard:
                st.warning("Nenhum medidor encontrado para exibição no dashboard.")
            else:
                num_medidores_dashboard = len(medidores_dashboard)
                num_cols_dashboard = 3 # Colunas para o layout do dashboard
                cols_dashboard = st.columns(num_cols_dashboard)

                limites_ref = st.session_state.limites_df[st.session_state.limites_df["Data"] == st.session_state.limites_df["Data"].min()].set_index("Hora")

                for i, medidor in enumerate(medidores_dashboard):
                    with cols_dashboard[i % num_cols_dashboard]:
                        st.subheader(medidor)
                        consumo_total_medidor = df_hora[medidor].sum()
                        meta_medidor = 0
                        if medidor in limites_ref.columns:
                            meta_medidor = limites_ref[medidor].sum()

                        st.metric(label=f"Consumo Total ({st.session_state.data_selecionada.strftime('%d/%m')})", value=f"{consumo_total_medidor:.2f} kWh")
                        st.metric(label="Meta Diária", value=f"{meta_medidor:.2f} kWh")
                        delta_medidor = consumo_total_medidor - meta_medidor
                        st.metric(label="Delta", value=f"{delta_medidor:.2f} kWh", delta=f"{delta_medidor:.2f} kWh")

                        fig_medidor = go.Figure()
                        fig_medidor.add_trace(go.Scatter(x=df_hora["Hora"], y=df_hora[medidor], mode='lines+markers', name='Consumo'))
                        if medidor in limites_ref.columns:
                            fig_medidor.add_trace(go.Scatter(x=limites_ref.index, y=limites_ref[medidor], mode='lines', name='Limite', line=dict(dash='dot')))
                        
                        fig_medidor.update_layout(
                            title=f'Consumo Horário {medidor}',
                            xaxis_title='Hora',
                            yaxis_title='Consumo (kWh)',
                            height=300,
                            margin=dict(l=20, r=20, t=40, b=20),
                            showlegend=False
                        )
                        st.plotly_chart(fig_medidor, use_container_width=True)
        else:
            st.warning("Não há dados para a data selecionada para exibição no dashboard.")

    with aba_calendar:
        st.header("Consumo no Calendário")
        if st.session_state.consumo_df is not None and not st.session_state.consumo_df.empty:
            df_calendar = st.session_state.consumo_df.copy()
            df_calendar["Data"] = df_calendar["Datetime"].dt.date
            df_calendar["Hora"] = df_calendar["Datetime"].dt.hour

            start_date = df_calendar["Data"].min()
            end_date = df_calendar["Data"].max()

            current_month = st.session_state.data_selecionada.month
            current_year = st.session_state.data_selecionada.year

            # Filter data for the current month
            df_month = df_calendar[(df_calendar["Datetime"].dt.month == current_month) & 
                                  (df_calendar["Datetime"].dt.year == current_year)].copy()
            
            # Aggregate consumption by date and hour for plotting
            df_daily_hourly_consumption = df_month.groupby(["Data", "Hora"])["Área Produtiva"].sum().reset_index()

            # Get hourly limits for 'Área Produtiva' from the first day in limites_df
            limites_ap_horario = st.session_state.limites_df[st.session_state.limites_df["Data"] == st.session_state.limites_df["Data"].min()]
            if not limites_ap_horario.empty and 'Área Produtiva' in limites_ap_horario.columns:
                limites_ap_horario = limites_ap_horario.set_index('Hora')['Área Produtiva']
            else:
                limites_ap_horario = pd.Series([0]*24, index=range(24)) # Default to zero if not found

            # Create a calendar grid
            first_day_of_month = datetime(current_year, current_month, 1).date()
            last_day_of_month = (datetime(current_year, current_month % 12 + 1, 1) - timedelta(days=1)).date() if current_month < 12 else datetime(current_year + 1, 1, 1) - timedelta(days=1)
            
            week_days = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"] # Adjust as per your locale for start of week
            
            # Determine the starting day of the week for the first day of the month
            # Python's weekday() returns 0 for Monday, 6 for Sunday
            first_day_weekday = first_day_of_month.weekday() # 0 is Monday, 6 is Sunday

            # Create a list of all dates in the month, padded for the calendar grid
            calendar_dates = []
            # Pad beginning of month
            for _ in range(first_day_weekday):
                calendar_dates.append(None) # Use None for empty cells
            
            current_date_iter = first_day_of_month
            while current_date_iter <= last_day_of_month:
                calendar_dates.append(current_date_iter)
                current_date_iter += timedelta(days=1)
            
            # Pad end of month to fill the last week
            while len(calendar_dates) % 7 != 0:
                calendar_dates.append(None)
            
            rows = [calendar_dates[i:i+7] for i in range(0, len(calendar_dates), 7)]

            st.markdown(f"### Consumo da Área Produtiva - {datetime(current_year, current_month, 1).strftime('%B %Y')}")
            
            st.markdown(f'<div style="display: grid; grid-template-columns: repeat(7, 1fr); gap: 5px;">'
                        f'{"".join([f"<div style="font-weight: bold; text-align: center;">{d}</div>" for d in week_days])}', 
                        unsafe_allow_html=True)
            
            for week in rows:
                st.markdown('<div style="display: grid; grid-template-columns: repeat(7, 1fr); gap: 5px;">', unsafe_allow_html=True)
                for day_date in week:
                    st.markdown('<div style="border: 1px solid #ccc; padding: 5px; height: 180px; display: flex; flex-direction: column; justify-content: space-between;">', unsafe_allow_html=True)
                    if day_date:
                        st.markdown(f'<div style="font-weight: bold; text-align: center;">{day_date.day}</div>', unsafe_allow_html=True)
                        
                        df_day_plot = df_daily_hourly_consumption[df_daily_hourly_consumption["Data"] == day_date]
                        
                        if not df_day_plot.empty:
                            fig_mini = go.Figure()
                            fig_mini.add_trace(go.Scatter(x=df_day_plot["Hora"], y=df_day_plot["Área Produtiva"], 
                                                        mode='lines', name='Consumo', line=dict(color='blue')))
                            fig_mini.add_trace(go.Scatter(x=limites_ap_horario.index, y=limites_ap_horario.values, 
                                                        mode='lines', name='Limite', line=dict(color='red', dash='dot')))
                            
                            fig_mini.update_layout(
                                autosize=True,
                                margin=dict(l=0, r=0, t=0, b=0),
                                height=120,
                                showlegend=False,
                                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                                plot_bgcolor='rgba(0,0,0,0)',
                                paper_bgcolor='rgba(0,0,0,0)'
                            )
                            st.plotly_chart(fig_mini, use_container_width=True, config={'displayModeBar': False})
                        else:
                            st.markdown("<div style='height: 120px; display: flex; align-items: center; justify-content: center; font-size: 0.8em; color: #888;'>Sem dados</div>", unsafe_allow_html=True)
                    else:
                        st.markdown('<div style="height: 100%; background-color: #f8f8f8;"></div>', unsafe_allow_html=True)
                    st.markdown('</div>', unsafe_allow_html=True)
                st.markdown('</div>', unsafe_allow_html=True)
            
            st.markdown('</div>', unsafe_allow_html=True) # Close main grid container


        else:
            st.info("Carregue os dados para visualizar o calendário de consumo.")


    with aba_conversion:
        st.header("Ferramentas de Conversão de Dados")
        st.subheader("CSV para JSON de Limites Horários")
        st.write("Esta ferramenta ajuda você a converter um arquivo CSV (com colunas 'Hora' e nomes dos medidores) em um formato JSON compatível para os limites horários.")

        uploaded_file = st.file_uploader("Arraste e solte ou clique para carregar um arquivo CSV de limites", type=["csv"])

        if uploaded_file is not None:
            try:
                # Tenta ler o CSV, considerando que a primeira coluna pode ser a hora
                df_upload_limites = pd.read_csv(uploaded_file)
                
                # Assume que a coluna de hora se chama 'Hora' ou 'time' ou 'Tempo'
                # ou é a primeira coluna sem um nome de medidor óbvio
                if 'Hora' in df_upload_limites.columns:
                    df_upload_limites['Hora'] = df_upload_limites['Hora'].astype(str)
                elif 'time' in df_upload_limites.columns:
                    df_upload_limites = df_upload_limites.rename(columns={'time': 'Hora'})
                    df_upload_limites['Hora'] = df_upload_limites['Hora'].astype(str)
                elif 'Tempo' in df_upload_limites.columns:
                    df_upload_limites = df_upload_limites.rename(columns={'Tempo': 'Hora'})
                    df_upload_limites['Hora'] = df_upload_limites['Hora'].astype(str)
                else:
                    # Se não encontrar uma coluna de hora óbvia, tenta usar a primeira coluna
                    # e garante que tenha 24 entradas para cada medidor
                    if df_upload_limites.shape[0] != 24:
                         st.warning("O arquivo CSV de limites deve ter 24 linhas (uma para cada hora).")
                    
                    df_upload_limites.insert(0, 'Hora', [f"{h:02d}:00:00" for h in range(24)])
                    
                # Adicionar uma coluna 'Timestamp' e 'Data' para compatibilidade
                # Usaremos uma data de referência fixa, já que os limites são "padrão" por hora
                data_referencia = datetime(2025, 7, 23).date() # Data arbitrária para os limites
                df_upload_limites['Data'] = data_referencia
                df_upload_limites['Timestamp'] = df_upload_limites.apply(lambda row: datetime.combine(row['Data'], datetime.strptime(row['Hora'], '%H:%M:%S').time()), axis=1)

                # Reordenar colunas para corresponder à estrutura esperada
                cols_ordered = ["Timestamp", "Data", "Hora"] + [col for col in df_upload_limites.columns if col not in ["Timestamp", "Data", "Hora"]]
                df_upload_limites = df_upload_limites[cols_ordered]

                st.subheader("Pré-visualização dos Limites Convertidos:")
                st.dataframe(df_upload_limites, use_container_width=True)

                json_output = df_upload_limites.to_json(orient="records", indent=4, date_format="iso")
                st.download_button(
                    label="Baixar Limites em JSON",
                    data=json_output,
                    file_name="limites_convertidos.json",
                    mime="application/json"
                )
                
                st.info("Você pode baixar este arquivo JSON e renomeá-lo para 'limites_padrao.json' na pasta do seu projeto para que ele seja carregado automaticamente.")
                
            except Exception as e:
                st.error(f"Erro ao converter CSV: {e}. Certifique-se de que o CSV tem as colunas corretas e o formato esperado.")
                st.info("O CSV deve ter uma coluna para 'Hora' (ou similar) e colunas para cada medidor (ex: 'MP&L', 'Área Produtiva').")
        
        st.subheader("Dados Brutos para Formato Processado (para debug)")
        st.write("Esta ferramenta exibe como os dados brutos são processados após a etapa de cálculo de consumo (diff().clip(lower=0)).")
        
        raw_data_input = st.text_area("Cole seus dados CSV brutos aqui (separador TAB, ponto como decimal) para visualizar o processamento:", height=200, key="raw_data_conversion")
        if raw_data_input:
            try:
                processed_df_preview = carregar_dados(raw_data_input)
                st.dataframe(processed_df_preview, use_container_width=True)
                st.success("Dados processados e exibidos.")
            except Exception as e:
                st.error(f"Erro ao processar dados brutos: {e}. Verifique o formato.")


    with aba_month_prediction:
        st.header("Previsão e Simulação Mensal")
        if st.session_state.consumo_df is not None and not st.session_state.consumo_df.empty:
            df_mes = st.session_state.consumo_df.copy()
            df_mes["Data"] = df_mes["Datetime"].dt.date
            df_mes_area_produtiva = df_mes.groupby("Data")["Área Produtiva"].sum().reset_index()

            # Filtrar para o mês atual
            df_mes_area_produtiva = df_mes_area_produtiva[
                (df_mes_area_produtiva["Data"].apply(lambda x: x.month) == st.session_state.data_selecionada.month) &
                (df_mes_area_produtiva["Data"].apply(lambda x: x.year) == st.session_state.data_selecionada.year)
            ].copy()

            if df_mes_area_produtiva.empty:
                st.warning("Não há dados de consumo para a Área Produtiva no mês selecionado.")
            else:
                data_atual = st.session_state.data_selecionada
                primeiro_dia_mes = data_atual.replace(day=1)
                ultimo_dia_mes = (primeiro_dia_mes + timedelta(days=31)).replace(day=1) - timedelta(days=1)
                
                dias_no_mes = ultimo_dia_mes.day
                dias_consumidos = (data_atual - primeiro_dia_mes).days + 1
                
                # Meta mensal da Área Produtiva (em kWh)
                meta_diaria_area_produtiva = st.session_state.limites_df[st.session_state.limites_df["Data"] == st.session_state.limites_df["Data"].min()]["Área Produtiva"].sum()
                meta_mensal_kwh = meta_diaria_area_produtiva * dias_no_mes

                consumo_acumulado = df_mes_area_produtiva["Área Produtiva"].sum()
                
                st.subheader("Resumo da Previsão Mensal")
                col_m1, col_m2, col_m3 = st.columns(3)
                col_m1.metric("Meta Mensal (kWh)", f"{meta_mensal_kwh:.2f}")
                col_m2.metric("Consumo Acumulado (kWh)", f"{consumo_acumulado:.2f}")
                
                # Previsão simples por média
                consumo_medio_diario_atual = consumo_acumulado / dias_consumidos if dias_consumidos > 0 else 0
                previsao_total_mes = consumo_medio_diario_atual * dias_no_mes
                
                saldo_para_meta_mensal = meta_mensal_kwh - consumo_acumulado
                dias_restantes = dias_no_mes - dias_consumidos

                col_m3.metric("Previsão Final do Mês (kWh)", f"{previsao_total_mes:.2f}")

                st.subheader("Detalhes Diários: Consumo vs. Meta (Área Produtiva)")
                
                # Criar DataFrame para exibição
                df_previsao_detalhada = pd.DataFrame(index=pd.date_range(start=primeiro_dia_mes, end=ultimo_dia_mes, freq='D'))
                df_previsao_detalhada['Data'] = df_previsao_detalhada.index.date
                df_previsao_detalhada['Meta Diária (kWh)'] = meta_diaria_area_produtiva

                # Consumo real
                df_previsao_detalhada = df_previsao_detalhada.merge(df_mes_area_produtiva.rename(columns={"Área Produtiva": "Consumo Real (kWh)"}), on="Data", how="left")
                df_previsao_detalhada["Consumo Real (kWh)"] = df_previsao_detalhada["Consumo Real (kWh)"].fillna(0) # Zera consumo para dias futuros
                
                # Saldo Diário
                df_previsao_detalhada['Saldo Diário (kWh)'] = df_previsao_detalhada['Meta Diária (kWh)'] - df_previsao_detalhada['Consumo Real (kWh)']
                
                st.dataframe(df_previsao_detalhada, use_container_width=True)

                st.subheader("Simulação de Monte Carlo para Consumo Diário Futuro (Área Produtiva)")
                # Filtrar apenas os dias com consumo real para base da simulação
                df_dias_reais = df_mes_area_produtiva[df_mes_area_produtiva["Data"] <= data_atual]
                
                if df_dias_reais.shape[0] < 5: # Precisa de alguns pontos para a simulação
                    st.warning("Dados insuficientes para realizar a simulação de Monte Carlo. São necessários pelo menos 5 dias de consumo real.")
                else:
                    consumos_historicos = df_dias_reais["Área Produtiva"].values
                    num_simulacoes = st.slider("Número de Simulações", 100, 1000, 500, step=100)
                    
                    simulacoes_futuras = []
                    for _ in range(num_simulacoes):
                        simulacao_dia_a_dia = []
                        for i in range(dias_restantes):
                            # Amostra um consumo diário aleatório do histórico
                            consumo_previsto_dia = np.random.choice(consumos_historicos)
                            simulacao_dia_a_dia.append(consumo_previsto_dia)
                        simulacoes_futuras.append(simulacao_dia_a_a)

                    # Converter para numpy array para facilitar cálculos de percentil
                    simulacoes_futuras = np.array(simulacoes_futuras)

                    # Calcular percentis para cada dia futuro
                    previsao_media = np.mean(simulacoes_futuras, axis=0)
                    previsao_p5 = np.percentile(simulacoes_futuras, 5, axis=0)
                    previsao_p95 = np.percentile(simulacoes_futuras, 95, axis=0)

                    # Criar datas futuras
                    datas_futuras = [data_atual + timedelta(days=i) for i in range(1, dias_restantes + 1)]
                    
                    fig_monte_carlo = go.Figure()

                    # Consumo real
                    fig_monte_carlo.add_trace(go.Scatter(x=df_dias_reais["Data"], y=df_dias_reais["Área Produtiva"],
                                                        mode='lines+markers', name='Consumo Real',
                                                        line=dict(color='blue', width=2)))

                    # Previsão média
                    fig_monte_carlo.add_trace(go.Scatter(x=datas_futuras, y=previsao_media,
                                                        mode='lines', name='Previsão Média',
                                                        line=dict(color='green', dash='dash', width=2)))
                    
                    # Intervalo de confiança (P5 e P95)
                    fig_monte_carlo.add_trace(go.Scatter(x=datas_futuras + datas_futuras[::-1],
                                                        y=list(previsao_p95) + list(previsao_p5[::-1]),
                                                        fill='toself',
                                                        fillcolor='rgba(0,100,80,0.2)',
                                                        line=dict(color='rgba(255,255,255,0)'),
                                                        name='Intervalo de Previsão (5-95%)',
                                                        showlegend=True))
                    
                    # Meta diária
                    fig_monte_carlo.add_trace(go.Scatter(x=pd.date_range(start=primeiro_dia_mes, end=ultimo_dia_mes, freq='D'),
                                                        y=[meta_diaria_area_produtiva] * dias_no_mes,
                                                        mode='lines', name='Meta Diária',
                                                        line=dict(color='red', dash='dot')))

                    fig_monte_carlo.update_layout(
                        title='Previsão de Consumo Diário (Área Produtiva) com Monte Carlo',
                        xaxis_title='Data',
                        yaxis_title='Consumo (kWh)',
                        hovermode='x unified'
                    )
                    st.plotly_chart(fig_monte_carlo, use_container_width=True)

                    st.subheader("Diagnóstico da Simulação")
                    simulacoes_acumuladas_totais = []
                    for sim in simulacoes_futuras:
                        simulacoes_acumuladas_totais.append(consumo_acumulado + np.sum(sim))
                    
                    previsoes_finais_mes = np.array(simulacoes_acumuladas_totais)
                    
                    # Calcular a probabilidade de estourar a meta
                    prob_estourar = np.sum(previsoes_finais_mes > meta_mensal_kwh) / num_simulacoes
                    
                    st.info(f"Com base em {num_simulacoes} simulações, há uma **{prob_estourar:.2%}** de chance de o consumo total da Área Produtiva **exceder a meta mensal de {meta_mensal_kwh:.2f} kWh**.")
                    if prob_estourar > 0.5:
                        st.warning("Atenção: Alta probabilidade de exceder a meta mensal. Ajustes podem ser necessários.")
                    else:
                        st.success("Boas notícias: Baixa probabilidade de exceder a meta mensal com o padrão de consumo atual.")
                
                st.subheader("Previsão de Consumo Futuro (ARIMA vs Monte Carlo)")
                if df_mes_area_produtiva.shape[0] > 7: # Precisa de mais dados para ARIMA
                    # Preparar dados para ARIMA (diários)
                    ts_arima = df_mes_area_produtiva.set_index('Data')['Área Produtiva']
                    
                    # Definir período para ARIMA (ex: semanal)
                    try:
                        model = ARIMA(ts_arima, order=(5,1,0)) # Exemplo de ordem (p,d,q)
                        model_fit = model.fit()
                        
                        # Previsão ARIMA para os dias restantes do mês
                        forecast_arima = model_fit.predict(start=len(ts_arima), end=len(ts_arima) + dias_restantes -1, typ='levels')
                        forecast_arima.index = datas_futuras # Atribuir as datas futuras ao forecast
                        
                        fig_arima_mc = go.Figure()
                        fig_arima_mc.add_trace(go.Scatter(x=df_dias_reais["Data"], y=df_dias_reais["Área Produtiva"],
                                                        mode='lines+markers', name='Consumo Real',
                                                        line=dict(color='blue', width=2)))
                        fig_arima_mc.add_trace(go.Scatter(x=datas_futuras, y=previsao_media,
                                                        mode='lines', name='Previsão MC (Média)',
                                                        line=dict(color='green', dash='dash', width=2)))
                        fig_arima_mc.add_trace(go.Scatter(x=forecast_arima.index, y=forecast_arima.values,
                                                        mode='lines', name='Previsão ARIMA',
                                                        line=dict(color='purple', dash='dot', width=2)))
                        
                        fig_arima_mc.update_layout(
                            title='Previsão de Consumo Futuro: Monte Carlo vs ARIMA',
                            xaxis_title='Data',
                            yaxis_title='Consumo (kWh)',
                            hovermode='x unified'
                        )
                        st.plotly_chart(fig_arima_mc, use_container_width=True)
                        
                    except Exception as e:
                        st.warning(f"Não foi possível aplicar o modelo ARIMA: {e}. Pode ser necessário mais dados ou ajustar a ordem do modelo.")
                else:
                    st.info("São necessários mais dados históricos para treinar o modelo ARIMA (mínimo de 7 dias de dados no mês).")

                st.subheader("Comparativo Diário: Consumo Real vs. Metas (Área Produtiva)")
                # Consumo Real
                df_plot_comp = df_mes_area_produtiva.copy()
                df_plot_comp = df_plot_comp.set_index('Data')
                
                # Meta Diária Original
                df_plot_comp['Meta Diária Original'] = meta_diaria_area_produtiva
                
                # Nova Meta Ajustada (para o saldo restante)
                # Calcule o saldo restante do mês
                saldo_restante_kwh = meta_mensal_kwh - consumo_acumulado
                
                if dias_restantes > 0:
                    nova_meta_diaria_ajustada = saldo_restante_kwh / dias_restantes
                else:
                    nova_meta_diaria_ajustada = 0 # Todo o mês já passou
                
                # Criar uma série para a meta ajustada para os dias futuros
                meta_ajustada_serie = pd.Series(index=pd.date_range(start=data_atual + timedelta(days=1), end=ultimo_dia_mes, freq='D'),
                                                data=nova_meta_diaria_ajustada)
                
                df_plot_comp['Meta Diária Ajustada'] = np.nan
                df_plot_comp.loc[df_plot_comp.index <= data_atual, 'Meta Diária Ajustada'] = df_plot_comp['Consumo Real (kWh)'] # Dias passados: consumo real
                df_plot_comp.loc[df_plot_comp.index > data_atual, 'Meta Diária Ajustada'] = meta_ajustada_serie # Dias futuros: meta ajustada
                
                fig_metas = go.Figure()
                fig_metas.add_trace(go.Scatter(x=df_plot_comp.index, y=df_plot_comp['Consumo Real (kWh)'], mode='lines+markers', name='Consumo Real'))
                fig_metas.add_trace(go.Scatter(x=df_plot_comp.index, y=df_plot_comp['Meta Diária Original'], mode='lines', name='Meta Diária Original', line=dict(dash='dot', color='red')))
                fig_metas.add_trace(go.Scatter(x=df_plot_comp.index, y=df_plot_comp['Meta Diária Ajustada'], mode='lines', name='Meta Diária Ajustada (Para Saldo Restante)', line=dict(dash='dash', color='orange')))
                
                fig_metas.update_layout(
                    title='Consumo Real Diário vs. Metas (Área Produtiva)',
                    xaxis_title='Data',
                    yaxis_title='Consumo (kWh)',
                    hovermode='x unified'
                )
                st.plotly_chart(fig_metas, use_container_width=True)

                st.subheader("Diagnóstico Inteligente: Saldo de Energia e Climatização Extra")
                if saldo_restante_kwh > 0:
                    st.success(f"Você tem um saldo positivo de **{saldo_restante_kwh:.2f} kWh** para o restante do mês.")
                    st.markdown(f"Isso equivale a aproximadamente **{saldo_restante_kwh / meta_diaria_area_produtiva:.2f} dias** de consumo médio adicional da Área Produtiva dentro da meta, ou a **{saldo_restante_kwh / 13.75:.2f} horas** de climatização extra (baseado no valor de 13.75 kWh/h de climatização, se for o caso).")
                else:
                    st.error(f"Você está com um saldo negativo de **{abs(saldo_restante_kwh):.2f} kWh** para o restante do mês.")
                    st.markdown(f"Isso significa que você precisaria economizar **{abs(saldo_restante_kwh):.2f} kWh** para atingir a meta, ou reduzir o consumo em aproximadamente **{abs(saldo_restante_kwh) / meta_diaria_area_produtiva:.2f} dias** de consumo médio da Área Produtiva.")
        else:
            st.info("Carregue os dados de consumo para realizar a previsão e simulação mensal.")

    with aba_meter_layout:
        st.header("Layout dos Medidores e Fluxo de Energia")
        st.write("Visualize a estrutura hierárquica dos medidores e o consumo de cada área.")

        if st.session_state.consumo_df is not None and not st.session_state.consumo_df.empty:
            df_layout = st.session_state.consumo_df.copy()
            df_layout_day = df_layout[df_layout["Datetime"].dt.date == st.session_state.data_selecionada]

            if df_layout_day.empty:
                st.warning("Não há dados para o dia selecionado para o layout dos medidores.")
            else:
                consumo_medio_dia = df_layout_day.drop(columns=["Datetime"]).sum()
                
                # Normalizar consumo para tamanho e cor dos nós
                max_consumo = consumo_medio_dia.max()
                min_consumo = consumo_medio_dia.min()
                
                def get_normalized_value(value, min_val, max_val):
                    if max_val == min_val: return 0.5
                    return (value - min_val) / (max_val - min_val)

                nodes = []
                edges = []

                # Definição dos nós base
                nodes.append(Node(id="Full Plant", label="Full Plant", size=80, color=mcolors.to_hex([0.8, 0.8, 1.0]))) # Light blue

                # Adiciona nós para Áreas Produtivas e Terceiros
                nodes.append(Node(id="PRODUCTIVE AREAS", label="Áreas Produtivas", size=70, color=mcolors.to_hex([0.6, 0.8, 1.0]))) # Medium blue
                edges.append(Edge(source="Full Plant", target="PRODUCTIVE AREAS", type="arrow"))

                nodes.append(Node(id="THIRD PARTS", label="Terceiros", size=60, color=mcolors.to_hex([0.8, 0.6, 1.0]))) # Light purple
                edges.append(Edge(source="Full Plant", target="THIRD PARTS", type="arrow"))

                # Adiciona medidores individuais
                medidores_individuais = [col for col in df_layout_day.columns if col not in ["Datetime", "Date", "Time", "TRIM&FINAL", "OFFICE + CANTEEN", "Área Produtiva"]]
                
                # Mapeamento de medidor para seu grupo (para as arestas)
                group_map = {
                    "MP&L": "PRODUCTIVE AREAS", "GAHO": "PRODUCTIVE AREAS", "MAIW": "PRODUCTIVE AREAS",
                    "QGBT1-MPTF": "PRODUCTIVE AREAS", "QGBT2-MPTF": "PRODUCTIVE AREAS", "CAG": "PRODUCTIVE AREAS",
                    "SEOB": "PRODUCTIVE AREAS", "EBPC": "PRODUCTIVE AREAS", "PCCB": "PRODUCTIVE AREAS",
                    "OFFICE": "THIRD PARTS", "PMDC-OFFICE": "THIRD PARTS"
                }
                
                # Consumo para coloração e dimensionamento (valores positivos)
                consumo_para_plot = consumo_medio_dia.apply(lambda x: max(0, x)) # Garante valores positivos para normalização
                
                if not consumo_para_plot.empty:
                    max_con_plot = consumo_para_plot.max()
                    min_con_plot = consumo_para_plot.min()
                    
                    # Cria um mapa de cores (vermelho para mais consumo, verde para menos)
                    cmap = cm.get_cmap('RdYlGn_r') # Red-Yellow-Green (reversed, so red is high)

                    for medidor in medidores_individuais:
                        consumo_val = consumo_medio_dia.get(medidor, 0)
                        normalized_size = get_normalized_value(max(0, consumo_val), min_con_plot, max_con_plot) # Size based on positive consumption
                        size = 30 + normalized_size * 50 # Escala de 30 a 80
                        
                        color_norm = get_normalized_value(max(0, consumo_val), min_con_plot, max_con_plot)
                        color = mcolors.to_hex(cmap(color_norm))
                        
                        nodes.append(Node(id=medidor, label=f"{medidor}\n({consumo_val:.0f} kWh)", size=size, color=color))

                        if medidor in group_map:
                            edges.append(Edge(source=group_map[medidor], target=medidor, type="arrow"))
                        elif "QGBT1-MPTF" in medidor or "QGBT2-MPTF" in medidor: # Exemplo de tratamento para os agregados
                             edges.append(Edge(source="PRODUCTIVE AREAS", target=medidor, type="arrow"))
                        elif "OFFICE" in medidor:
                            edges.append(Edge(source="THIRD PARTS", target=medidor, type="arrow"))

                    # Adiciona arestas para as colunas agregadas se elas existem no consumo_medio_dia
                    if "TRIM&FINAL" in consumo_medio_dia.index:
                        nodes.append(Node(id="TRIM&FINAL", label=f"TRIM&FINAL\n({consumo_medio_dia['TRIM&FINAL']:.0f} kWh)", 
                                        size=30 + get_normalized_value(consumo_medio_dia['TRIM&FINAL'], min_con_plot, max_con_plot) * 50, 
                                        color=mcolors.to_hex(cmap(get_normalized_value(consumo_medio_dia['TRIM&FINAL'], min_con_plot, max_con_plot)))))
                        edges.append(Edge(source="PRODUCTIVE AREAS", target="TRIM&FINAL", type="arrow"))
                    
                    if "OFFICE + CANTEEN" in consumo_medio_dia.index:
                        nodes.append(Node(id="OFFICE + CANTEEN", label=f"OFFICE + CANTEEN\n({consumo_medio_dia['OFFICE + CANTEEN']:.0f} kWh)", 
                                        size=30 + get_normalized_value(consumo_medio_dia['OFFICE + CANTEEN'], min_con_plot, max_con_plot) * 50, 
                                        color=mcolors.to_hex(cmap(get_normalized_value(consumo_medio_dia['OFFICE + CANTEEN'], min_con_plot, max_con_plot)))))
                        edges.append(Edge(source="THIRD PARTS", target="OFFICE + CANTEEN", type="arrow"))

                    if "Área Produtiva" in consumo_medio_dia.index:
                        normalized_size_ap = get_normalized_value(consumo_medio_dia['Área Produtiva'], min_con_plot, max_con_plot)
                        size_ap = 40 + normalized_size_ap * 50
                        color_ap = mcolors.to_hex(cmap(normalized_size_ap))
                        
                        # Atualiza o nó da Área Produtiva com o valor de consumo e cor dinâmica
                        for node in nodes:
                            if node.id == "PRODUCTIVE AREAS":
                                node.label = f"Áreas Produtivas\n({consumo_medio_dia['Área Produtiva']:.0f} kWh)"
                                node.size = size_ap
                                node.color = color_ap
                                break

                    # Configuração do gráfico de grafo
                    config = Config(width=700, 
                                    height=500, 
                                    graphviz_layout="dot", # ou "neato", "fdp", "circo"
                                    # Configurações de física para evitar sobreposição excessiva
                                    physics={"enabled": True, "barnesHut": {"gravitationalConstant": -2000, "centralGravity": 0.3, "springLength": 95, "springConstant": 0.04, "damping": 0.09, "avoidOverlap": 0.5}},
                                    directed=True,
                                    nodeHighlightBehavior=True, 
                                    highlightColor="#F7A7A6",
                                    collapsible=True,
                                    maxZoom=2.0,
                                    minZoom=0.5
                                    )
                    
                    agraph(nodes=nodes, edges=edges, config=config)

                else:
                    st.info("Dados de consumo insuficientes para gerar o layout dos medidores.")
        else:
            st.info("Carregue os dados para visualizar o layout dos medidores.")

    with aba_ml_prediction:
        st.header("Previsão de Consumo Diário (Machine Learning)")
        if st.session_state.consumo_df is not None and not st.session_state.consumo_df.empty:
            df_ml = st.session_state.consumo_df.copy()
            df_ml["date"] = df_ml["Datetime"].dt.date
            df_ml["time"] = df_ml["Datetime"].dt.time # Mantém a coluna time, caso precise
            
            # Agrupar por data para obter o consumo diário da Área Produtiva
            df_diario = df_ml.groupby("date")["Área Produtiva"].sum().reset_index()
            df_diario.columns = ["date", "consumption"]

            # Permitir selecionar uma data para prever
            selected_day = st.date_input("Selecione um dia para previsão:", value=st.session_state.data_selecionada)

            # Preparar dados para o modelo de ML
            df = df_diario.sort_values("date")
            df["day_num"] = (pd.to_datetime(df["date"]) - pd.to_datetime(df["date"]).min()).dt.days
            selected_day_dt = pd.to_datetime(selected_day)
            selected_day_num = (selected_day_dt - pd.to_datetime(df["date"]).min()).days

            X = df[["day_num"]]
            y = df["consumption"]

            if len(X) < 2:
                st.warning("Dados insuficientes para treinamento do modelo de ML (mínimo de 2 pontos de dados diários).")
            elif len(X) <= 5:
                st.warning("Poucos dados para um treinamento robusto de ML. A precisão pode ser limitada.")
                X_train, X_test, y_train, y_test = X, X, y, y # Usar todos os dados para teste e treino
            else:
                X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

            models = {
                "Random Forest": RandomForestRegressor(n_estimators=100, random_state=42),
                "Linear Regression": LinearRegression(),
                "Gradient Boosting": GradientBoostingRegressor(n_estimators=100, random_state=42),
                "K-Nearest Neighbors": KNeighborsRegressor(n_neighbors=min(3, len(X_train)-1 if len(X_train)>1 else 1)), # Garante que n_neighbors não seja maior que o número de amostras
                "Support Vector Regression": SVR()
            }

            results = []
            fig = go.Figure()

            selected_month = selected_day_dt.month
            selected_year = selected_day_dt.year
            df_month_plot = df[(pd.to_datetime(df["date"]).dt.month == selected_month) & (
                        pd.to_datetime(df["date"]).dt.year == selected_year)]

            # Adicionar consumo real ao gráfico
            fig.add_trace(
                go.Scatter(x=df_month_plot["date"], y=df_month_plot["consumption"], mode='lines+markers', name='Consumo Real', line=dict(color='blue')))

            for name, model in models.items():
                if len(X_train) > 0 and len(y_train) > 0: # Check if training data exists
                    try:
                        model.fit(X_train, y_train)
                        y_pred = model.predict(X_test)
                        mae = mean_absolute_error(y_test, y_pred)
                        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
                        
                        # Prever para o dia selecionado
                        # Precisa verificar se selected_day_num está dentro do range de day_num para X_train ou se o modelo pode extrapolar
                        # Para extrapolação, o modelo LinearRegression é o mais adequado. Outros podem ser limitados.
                        # Para um único ponto de previsão, deve-se passar um array 2D
                        prediction = model.predict(np.array([[selected_day_num]]))[0] 
                        
                        y_fit = model.predict(X)

                        fit_df = pd.DataFrame({"date": df["date"], "fit": y_fit})
                        fit_df_month = fit_df[(pd.to_datetime(fit_df["date"]).dt.month == selected_month) & (
                                    pd.to_datetime(fit_df["date"]).dt.year == selected_year)]

                        fig.add_trace(
                            go.Scatter(x=fit_df_month["date"], y=fit_df_month["fit"], mode='lines', name=f'{name} Fit'))
                        
                        fig.add_trace(go.Scatter(x=[selected_day_dt], y=[prediction], mode='markers',
                                                 name=f'{name} Previsão ({selected_day_dt.strftime("%d/%m")})', 
                                                 marker=dict(size=10, symbol='star', color='black')))

                        rmse_norm = rmse / y_test.mean() if y_test.mean() != 0 else np.inf # Evita divisão por zero
                        accuracy = max(0, 1 - rmse_norm)

                        results.append({
                            "Modelo": name,
                            "MAE": round(mae, 2),
                            "RMSE": round(rmse, 2),
                            "Acurácia (%)": round(accuracy * 100, 2),
                            "Previsão para o dia selecionado": round(prediction, 2)
                        })
                    except Exception as e:
                        st.warning(f"Erro ao treinar/prever com o modelo {name}: {e}. Pode ser dados insuficientes ou incompatíveis.")
                        results.append({
                            "Modelo": name,
                            "MAE": "N/A",
                            "RMSE": "N/A",
                            "Acurácia (%)": "N/A",
                            "Previsão para o dia selecionado": "N/A"
                        })
                else:
                    st.info(f"Dados de treinamento insuficientes para o modelo {name}.")
                    results.append({
                        "Modelo": name,
                        "MAE": "N/A",
                        "RMSE": "N/A",
                        "Acurácia (%)": "N/A",
                        "Previsão para o dia selecionado": "N/A"
                    })

            fig.update_layout(title="Previsão de Consumo de Energia (Mês Selecionado)",
                              xaxis_title="Data",
                              yaxis_title="Consumo (kWh)",
                              legend_title="Legenda")
            st.plotly_chart(fig, use_container_width=True)

            results_df = pd.DataFrame(results)
            if not results_df.empty:
                results_df = results_df.sort_values(by="Acurácia (%)", ascending=False).reset_index(
                    drop=True)
                st.subheader("📊 Desempenho e Previsões dos Modelos")
                st.dataframe(results_df, use_container_width=True)
            else:
                st.info("Nenhum resultado de modelo para exibir.")


        else:
            st.info("Carregue os dados para realizar a previsão com Machine Learning.")

# Código para send_email (apenas um placeholder, precisa ser configurado)
def send_email(recipient_email, subject, body, attachment_path=None):
    sender_email = os.getenv("SMTP_USERNAME")
    sender_password = os.getenv("SMTP_PASSWORD")
    smtp_server = os.getenv("SMTP_SERVER")
    smtp_port = int(os.getenv("SMTP_PORT", 587)) # Default to 587 if not set
    
    if not all([sender_email, sender_password, smtp_server]):
        st.error("Credenciais SMTP incompletas. Verifique as variáveis de ambiente.")
        return False

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    if attachment_path:
        try:
            with open(attachment_path, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename= {os.path.basename(attachment_path)}",
            )
            msg.attach(part)
        except Exception as e:
            st.error(f"Erro ao anexar arquivo: {e}")
            return False

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()  # Upgrade connection to secure
        server.login(sender_email, sender_password)
        text = msg.as_string()
        server.sendmail(sender_email, recipient_email, text)
        server.quit()
        st.success("E-mail enviado com sucesso!")
        return True
    except Exception as e:
        st.error(f"Falha ao enviar e-mail: {e}. Verifique as credenciais SMTP e a conectividade.")
        return False
