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
import plotly.express as px # Adicionado: Importação de plotly.express para px.bar
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.neighbors import KNeighborsRegressor
from sklearn.svm import SVR
from sklearn.metrics import mean_absolute_error, mean_squared_error
# Removidos imports duplicados: plotly.graph_objects e numpy

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
        # CORREÇÃO: `CAMINho_JSON_PADRAO` para `CAMINHO_JSON_PADRAO`
        limites_df = pd.read_json(CAMINHO_JSON_PADRAO) 
        limites_df["Timestamp"] = pd.to_datetime(limites_df["Timestamp"], dayfirst=True)
        limites_df["Data"] = limites_df["Timestamp"].dt.date
        limites_df["Hora"] = limites_df["Timestamp"].dt.hour
        st.session_state.limites_df = limites_df
        # O dicionário `limites_por_medidor_horario` precisa ser mais robusto aqui
        # para lidar com todos os medidores dinamicamente, ou predefinir os medidores.
        # Por simplicidade, vou usar um loop pelos medidores mapeados em `novos_rotulos`
        # ou os que estão no arquivo JSON.
        
        # Garante que `limites_por_medidor_horario` seja populado corretamente
        st.session_state.limites_por_medidor_horario = {}
        for col in limites_df.columns:
            if col not in ["Timestamp", "Data", "Hora"]:
                # Assume que o JSON tem 24 entradas para cada medidor para representar as horas
                st.session_state.limites_por_medidor_horario[col] = list(limites_df[col])
        
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
    """Remove vírgulas de strings para conversão numérica."""
    return texto.replace(",", "")

# Função principal para carregar e pré-processar os dados
def carregar_dados(dados_colados):
    """
    Carrega dados de consumo de energia de um CSV, pré-processa-os,
    calcula o consumo horário e adiciona colunas agregadas.
    """
    # Converte para CSV string para reuso da função carregar_dados
    # Isso é necessário porque `io.StringIO` espera uma string.
    # Se `dados_colados` já vier de um `df.to_csv()`, pode ser necessário
    # ajustar o `sep` ou garantir que o `limpar_valores` esteja correto para o formato de entrada.
    # Para dados colados diretamente (como no text_area), `sep="\t"` é geralmente para tab-separated values.
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
    # Filtra as colunas que realmente existem no DataFrame de entrada
    cols_to_rename = {old_name: new_name for old_name, new_name in novos_rotulos.items() if old_name in dados.columns}
    dados = dados.rename(columns=cols_to_rename)
    
    # Atualiza a lista de medidores para incluir apenas os que foram renomeados e estão presentes
    medidores = list(cols_to_rename.values())
    
    # Converte os valores dos medidores para float. `errors='coerce'` transforma erros em NaN.
    for col in medidores:
        if col in dados.columns: # Garante que a coluna existe após o rename
            dados[col] = pd.to_numeric(dados[col], errors='coerce')

    consumo = dados[["Datetime"] + medidores].copy()

    # CRÍTICA: Substituindo .abs() por .clip(lower=0) para tratar anomalias
    for col in medidores:
        # Aplica .diff() apenas se houver mais de uma linha de dados
        if len(consumo[col]) > 1:
            consumo[col] = consumo[col].diff().clip(lower=0) # Garante que o consumo seja sempre >= 0
        else: # Se houver apenas uma linha, a diferença é NaN. Trata como 0.
            consumo[col] = 0

    consumo = consumo.dropna(subset=medidores) # Remove linhas onde os medidores ficaram NaN após o diff/coerce
    
    # Adicionando colunas agregadas.
    # É importante verificar se as colunas base para as agregações existem.
    # Se QGBT1-MPTF ou QGBT2-MPTF não existirem, 'TRIM&FINAL' será NaN ou erro.
    # Usaremos .get() com valor padrão 0 para evitar KeyError, mas isso pode mascarar dados faltantes.
    # Uma abordagem mais robusta seria verificar a existência das colunas primeiro.
    
    consumo["TRIM&FINAL"] = consumo.get("QGBT1-MPTF", 0) + consumo.get("QGBT2-MPTF", 0)
    consumo["OFFICE + CANTEEN"] = consumo.get("OFFICE", 0) - consumo.get("PMDC-OFFICE", 0)
    
    # Para "Área Produtiva", certifique-se de que todas as colunas envolvidas existem.
    # Um tratamento mais robusto seria mapear cada nome para sua coluna, verificando a existência.
    consumo["Área Produtiva"] = (
        consumo.get("MP&L", 0) + consumo.get("GAHO", 0) + consumo.get("CAG", 0) + 
        consumo.get("SEOB", 0) + consumo.get("EBPC", 0) + consumo.get("PMDC-OFFICE", 0) + 
        consumo.get("TRIM&FINAL", 0) + consumo.get("OFFICE + CANTEEN", 0) + 13.75 # 13.75 é uma constante?
    )
    
    # Removendo colunas que foram agregadas, se elas existirem
    cols_to_drop = [col for col in ["QGBT1-MPTF", "QGBT2-MPTF"] if col in consumo.columns]
    if cols_to_drop:
        consumo = consumo.drop(columns=cols_to_drop)
    
    return consumo

# --- Função de Envio de E-mail ---
# Esta função precisa estar definida no escopo global para ser chamada pelo botão na sidebar.
def send_email(recipient_email, subject, body, attachment_path=None):
    """Envia um e-mail com conteúdo HTML ou texto simples e anexo opcional."""
    sender_email = os.getenv("SMTP_USERNAME")
    sender_password = os.getenv("SMTP_PASSWORD")
    smtp_server = os.getenv("SMTP_SERVER")
    smtp_port = int(os.getenv("SMTP_PORT", 587)) # Default to 587 if not set
    
    if not all([sender_email, sender_password, smtp_server]):
        st.error("Credenciais SMTP incompletas. Verifique as variáveis de ambiente (SMTP_USERNAME, SMTP_PASSWORD, SMTP_SERVER, SMTP_PORT).")
        return False

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain')) # Pode ser 'html' se o corpo for HTML

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
        st.error(f"Falha ao enviar e-mail: {e}. Verifique as credenciais SMTP, a conectividade e as permissões de porta.")
        return False


# --- Sidebar (Barra Lateral) ---
with st.sidebar:
    st.image("https://images.unsplash.com/photo-1549646875-01e4695e6912?q=80&w=2670&auto=format&fit=crop&ixlib=rb-4.0.3&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D", width=250) # Substitua pela sua logo
    st.title("PowerTrack ⚡")
    st.markdown("Monitoramento e Previsão de Consumo de Energia")

    st.header("Configurações de Dados")
    data_source_option = st.radio("Selecione a fonte de dados:", ("Google Sheets", "Colar CSV"))

    df = None # Inicializa df como None

    if data_source_option == "Google Sheets":
        st.info("Conectando ao Google Sheets...")
        try:
            # Configuração do Google Sheets
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            creds_json = {
                "type": st.secrets["gcp_service_account"]["type"],
                "project_id": st.secrets["gcp_service_account"]["project_id"],
                "private_key_id": st.secrets["gcp_service_account"]["private_key_id"],
                "private_key": st.secrets["gcp_service_account"]["private_key"],
                "client_email": st.secrets["gcp_service_account"]["client_email"],
                "client_id": st.secrets["gcp_service_account"]["client_id"],
                "auth_uri": st.secrets["gcp_service_account"]["auth_uri"],
                "token_uri": st.secrets["gcp_service_account"]["token_uri"],
                "auth_provider_x509_cert_url": st.secrets["gcp_service_account"]["auth_provider_x509_cert_url"],
                "client_x509_cert_url": st.secrets["gcp_service_account"]["client_x509_cert_url"],
                "universe_domain": st.secrets["gcp_service_account"]["universe_domain"]
            }
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
            client = gspread.authorize(creds)

            # Abra a planilha pelo nome (substitua 'NomeDaSuaPlanilha' pelo nome real da sua planilha)
            sheet = client.open("PowerTrack_Dados_Medidores").sheet1
            data = sheet.get_all_values()
            
            # Converte a lista de listas em um DataFrame do Pandas
            header = data[0]
            df_gs = pd.DataFrame(data[1:], columns=header)
            
            # Converte para CSV string para reuso da função carregar_dados
            csv_string_gs = df_gs.to_csv(index=False, sep='\t')
            df = carregar_dados(csv_string_gs)
            
            st.success("Dados carregados do Google Sheets!")
            st.write(f"Última atualização: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        except Exception as e:
            st.error(f"Erro ao carregar dados do Google Sheets: {e}")
            st.info("Verifique se as credenciais estão corretas no `secrets.toml` ou se o nome da planilha está certo.")

    elif data_source_option == "Colar CSV":
        dados_colados_input = st.text_area("Cole seus dados CSV tabulados aqui:", height=300, 
                                            placeholder="Date\tTime\tMM_MPTF_QGBT-03_KWH.PresentValue\t...")
        if st.button("Carregar Dados Manuais"):
            if dados_colados_input:
                try:
                    df = carregar_dados(dados_colados_input)
                    st.success("Dados carregados com sucesso!")
                except Exception as e:
                    st.error(f"Erro ao carregar dados: {e}. Verifique o formato do CSV.")
            else:
                st.warning("Cole os dados CSV para carregar.")

    # Se df foi carregado com sucesso, permite seleção da data
    if df is not None and not df.empty:
        df["date"] = pd.to_datetime(df["Datetime"].dt.date)
        df["time"] = df["Datetime"].dt.time
        
        # Filtro de data na sidebar
        min_date = df["date"].min()
        max_date = df["date"].max()
        
        # Garante que a data selecionada esteja dentro do range de dados
        if "selected_date" not in st.session_state or not (st.session_state.selected_date >= min_date and st.session_state.selected_date <= max_date):
            st.session_state.selected_date = max_date # Define a última data como padrão

        selected_date = st.date_input("Selecione a Data para Análise:", 
                                       value=st.session_state.selected_date,
                                       min_value=min_date, 
                                       max_value=max_date,
                                       key="date_selector")
        st.session_state.selected_date = selected_date # Atualiza o estado da sessão

        df_filtered = df[df["date"] == pd.to_datetime(selected_date)]

        # --- Envio de E-mail ---
        st.markdown("---")
        st.subheader("Envio de Relatório por E-mail")
        recipient_email = st.text_input("E-mail do destinatário:", value="seu_email@example.com") # Coloque um email padrão
        if st.button("Enviar Relatório do Dia"):
            if recipient_email and "@" in recipient_email:
                if df_filtered.empty:
                    st.warning("Não há dados para a data selecionada para enviar relatório.")
                else:
                    # Gera um resumo do dia para o email
                    total_plant_consumption_day = df_filtered["Área Produtiva"].sum() # Exemplo
                    email_body = f"Relatório de Consumo de Energia para o dia {selected_date.strftime('%d/%m/%Y')}:\n\n"
                    email_body += f"Consumo Total da Área Produtiva: {total_plant_consumption_day:.2f} kWh\n\n"
                    
                    # Você pode adicionar mais detalhes aqui, como consumo por medidor
                    # e comparação com limites para gerar alertas no email.
                    
                    if "limites_por_medidor_horario" in st.session_state:
                        # Exemplo de lógica de alerta (adapte conforme seus limites)
                        for medidor in [col for col in df.columns if col not in ["Datetime", "date", "time"]]:
                            if medidor in st.session_state.limites_por_medidor_horario:
                                limites_medidor = st.session_state.limites_por_medidor_horario[medidor]
                                
                                # Verifica se há dados filtrados para o medidor antes de acessar
                                if medidor in df_filtered.columns:
                                    # Ajusta para pegar a hora do dia para o agrupamento
                                    consumo_horario_medidor = df_filtered.set_index(df_filtered["Datetime"].dt.hour)[medidor]
                                    
                                    excesso_horas = []
                                    for hour, consumption_value in consumo_horario_medidor.items():
                                        if hour < len(limites_medidor) and consumption_value > limites_medidor[hour]:
                                            excesso_horas.append(f"  Hora {hour:02d}:00: Consumo {consumption_value:.2f} kWh > Limite {limites_medidor[hour]:.2f} kWh")
                                    
                                    if excesso_horas:
                                        email_body += f"\n🚨 Alerta para {medidor}:\n" + "\n".join(excesso_horas)
                    
                    try:
                        send_email(recipient_email, f"Relatório PowerTrack - {selected_date.strftime('%d/%m/%Y')}", email_body)
                    except Exception as e: # Catch all exceptions from send_email
                         st.error(f"Erro inesperado ao enviar e-mail: {e}. Verifique o console para mais detalhes.")
            else:
                st.error("Por favor, insira um endereço de e-mail válido.")

# --- Tabs (Abas Principais) ---
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
    "Overview", "Per Meter", "Targets", "Dashboard", "Calendar", 
    "Conversion", "Month Prediction", "ML Prediction"
])

# Verifica se df_filtered está definido e não vazio antes de usar
if 'df_filtered' not in locals() or df_filtered.empty:
    st.info("Por favor, carregue os dados na barra lateral para começar a análise.")
else:
    # Obtém a lista de medidores dinamicamente
    # Exclui as colunas de data/hora e as agregadas para obter os medidores originais ou principais
    # Recomenda-se manter uma lista predefinida dos "main_medidores" que você quer apresentar
    main_medidores = ["MP&L", "GAHO", "MAIW", "CAG", "SEOB", "OFFICE", "EBPC", "PCCB", "PMDC-OFFICE"]
    
    with tab1: # Overview
        st.header("📊 Visão Geral do Consumo - " + selected_date.strftime("%d/%m/%Y"))

        # Cálculo dos valores para os cards de resumo
        total_plant_consumption_day = df_filtered["Área Produtiva"].sum()
        
        # Exemplo de como usar limites (adapte para seus casos reais)
        # Supondo que você tenha uma meta diária total para a Área Produtiva
        # Isso precisaria ser mais sofisticado para ser uma meta real, talvez somando limites horários
        meta_diaria_area_produtiva = sum(st.session_state.limites_por_medidor_horario.get("Área Produtiva", [0]*24)) # Exemplo simples

        delta_area_produtiva = total_plant_consumption_day - meta_diaria_area_produtiva
        
        # O delta no st.metric mostra a diferença do valor atual para um valor base (que aqui é o delta_area_produtiva)
        # Para mostrar se está acima ou abaixo da meta, o delta deve ser (valor_atual - meta)
        # Se você quer um "saldo" (quanto falta ou sobrou), pode ser meta - valor_atual
        
        # Se delta_area_produtiva for positivo, está acima da meta (ruim). Se negativo, abaixo (bom).
        # A cor "inverse" fará com que positivo (acima da meta) seja vermelho e negativo (abaixo da meta) seja verde.
        
        col_overview1, col_overview2, col_overview3 = st.columns(3)
        with col_overview1:
            st.metric("Consumo Total (Área Produtiva)", f"{total_plant_consumption_day:.2f} kWh")
        with col_overview2:
            st.metric("Meta Diária (Área Produtiva)", f"{meta_diaria_area_produtiva:.2f} kWh")
        with col_overview3:
            st.metric("Diferença da Meta", f"{delta_area_produtiva:.2f} kWh", delta=f"{delta_area_produtiva:.2f} kWh", delta_color="inverse")
            

        st.subheader("Consumo Diário por Medidor (Gráfico de Barras Empilhadas)")
        
        # Permite selecionar quais medidores visualizar
        # Crie uma lista de medidores para o multiselect, excluindo as colunas de tempo e as agregadas finais
        all_possible_plot_cols = [col for col in df.columns if col not in ["Datetime", "date", "time", "TRIM&FINAL", "OFFICE + CANTEEN"]]
        
        selected_overview_medidores = st.multiselect(
            "Selecione os medidores para o gráfico:", 
            options=all_possible_plot_cols,
            default=[col for col in all_possible_plot_cols if col in ["Área Produtiva", "MP&L", "GAHO", "MAIW"]] # Exemplo de default
        )

        if selected_overview_medidores:
            df_daily_sum = df.groupby("date")[selected_overview_medidores].sum().reset_index()
            fig_overview_stacked = px.bar(
                df_daily_sum, 
                x="date", 
                y=selected_overview_medidores, 
                title="Consumo Diário por Medidor",
                labels={"value": "Consumo (kWh)", "date": "Data", "variable": "Medidor"},
                hover_name="date"
            )
            fig_overview_stacked.update_layout(xaxis_title="Data", yaxis_title="Consumo (kWh)")
            st.plotly_chart(fig_overview_stacked, use_container_width=True)
        else:
            st.info("Selecione pelo menos um medidor para ver o gráfico de consumo diário.")

        st.subheader("Consumo Diário Detalhado (Tabela)")
        st.dataframe(df_filtered.set_index("Datetime"), use_container_width=True)

    with tab2: # Per Meter
        st.header("📈 Consumo Detalhado por Medidor - " + selected_date.strftime("%d/%m/%Y"))
        
        # Filtra `main_medidores` para garantir que existam no df_filtered
        available_main_medidores = [m for m in main_medidores if m in df_filtered.columns]
        
        if not available_main_medidores:
            st.warning("Nenhum medidor principal disponível nos dados carregados para análise detalhada.")
            medidor_selecionado = None
        else:
            medidor_selecionado = st.selectbox("Selecione o Medidor:", available_main_medidores)
        
        if medidor_selecionado:
            df_meter_day = df_filtered[["Datetime", medidor_selecionado]].copy()
            df_meter_day["Hora"] = df_meter_day["Datetime"].dt.hour
            df_meter_day["Consumo"] = df_meter_day[medidor_selecionado]

            fig_meter = go.Figure()
            fig_meter.add_trace(go.Scatter(x=df_meter_day["Hora"], y=df_meter_day["Consumo"], 
                                         mode='lines+markers', name='Consumo Real (kWh)'))
            
            # Adiciona os limites se existirem
            if medidor_selecionado in st.session_state.limites_por_medidor_horario:
                limites = st.session_state.limites_por_medidor_horario[medidor_selecionado]
                # Garante que os limites tenham o mesmo número de pontos de hora (0-23)
                horas_limites = list(range(len(limites))) # Assumindo 24h de limites
                fig_meter.add_trace(go.Scatter(x=horas_limites, y=limites, mode='lines', name='Limite (kWh)',
                                                line=dict(dash='dash', color='red')))
            
            fig_meter.update_layout(title=f"Consumo Horário de {medidor_selecionado}",
                                    xaxis_title="Hora do Dia",
                                    yaxis_title="Consumo (kWh)",
                                    xaxis=dict(tickmode='linear', dtick=1),
                                    legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01))
            st.plotly_chart(fig_meter, use_container_width=True)
        else:
            st.info("Selecione um medidor para ver o detalhe.")
            
    with tab3: # Targets
        st.header("🎯 Metas e Limites de Consumo")
        st.subheader("Limites Horários Carregados")
        if "limites_df" in st.session_state and not st.session_state.limites_df.empty:
            st.dataframe(st.session_state.limites_df.set_index("Timestamp"), use_container_width=True)
            
            # Botão para download do JSON de limites
            json_limites = st.session_state.limites_df.to_json(orient="records", date_format="iso", indent=4)
            st.download_button(
                label="Baixar limites como JSON",
                data=json_limites,
                file_name="limites_atuais.json",
                mime="application/json"
            )
        else:
            st.info("Nenhum limite horário padrão carregado. Verifique o arquivo `limites_padrao.json` ou se ele contém dados válidos.")

        st.subheader("Metas Mensais da Área Produtiva (MWh)")
        
        if "limites_por_medidor_horario" in st.session_state and "Área Produtiva" in st.session_state.limites_por_medidor_horario:
            limites_ap_diarios_kwh = sum(st.session_state.limites_por_medidor_horario["Área Produtiva"])
            
            import calendar
            # Calcula o número de dias no mês da data selecionada
            num_days_in_month = calendar.monthrange(selected_date.year, selected_date.month)[1]
            
            meta_mensal_ap_kwh = limites_ap_diarios_kwh * num_days_in_month
            meta_mensal_ap_mwh = meta_mensal_ap_kwh / 1000
            st.metric(f"Meta Mensal Estimada (Área Produtiva) para {selected_date.strftime('%B %Y')}", f"{meta_mensal_ap_mwh:.2f} MWh")
        else:
            st.info("Metas para 'Área Produtiva' não configuradas nos limites. Verifique `limites_padrao.json`.")

    with tab4: # Dashboard
        st.header("🖥️ Dashboard de Consumo")
        
        # Painel de Resumo por Medidor
        st.subheader("Resumo por Medidor (" + selected_date.strftime("%d/%m/%Y") + ")")
        
        # Filtra apenas os medidores que têm dados no df_filtered
        # Cuidado para não incluir 'Área Produtiva' aqui se for uma coluna agregada para o resumo
        display_medidores_on_dashboard = [m for m in main_medidores if m in df_filtered.columns]
        if "Área Produtiva" in df_filtered.columns:
            display_medidores_on_dashboard.insert(0, "Área Produtiva") # Coloca a área produtiva no início
        
        cols_dashboard = st.columns(4) # Define sempre 4 colunas
        
        for i, medidor in enumerate(display_medidores_on_dashboard):
            with cols_dashboard[i % 4]: # Garante que as colunas se repetem a cada 4 medidores
                consumo_medidor_dia = df_filtered[medidor].sum()
                limite_medidor_dia = sum(st.session_state.limites_por_medidor_horario.get(medidor, [0]*24))
                
                # Se não houver limite, o delta não faz sentido
                if limite_medidor_dia > 0:
                    delta_medidor = consumo_medidor_dia - limite_medidor_dia
                    st.metric(f"**{medidor}**", f"{consumo_medidor_dia:.2f} kWh", 
                            delta=f"{delta_medidor:.2f} kWh", delta_color="inverse")
                else:
                    st.metric(f"**{medidor}**", f"{consumo_medidor_dia:.2f} kWh")

        st.subheader("Consumo Horário vs. Limite por Medidor")
        for medidor in display_medidores_on_dashboard:
            df_meter_day = df_filtered[["Datetime", medidor]].copy()
            df_meter_day["Hora"] = df_meter_day["Datetime"].dt.hour
            df_meter_day["Consumo"] = df_meter_day[medidor]

            fig_meter_dashboard = go.Figure()
            fig_meter_dashboard.add_trace(go.Scatter(x=df_meter_day["Hora"], y=df_meter_day["Consumo"], 
                                                      mode='lines+markers', name='Consumo Real'))
            
            if medidor in st.session_state.limites_por_medidor_horario:
                limites = st.session_state.limites_por_medidor_horario[medidor]
                horas = list(range(len(limites)))
                fig_meter_dashboard.add_trace(go.Scatter(x=horas, y=limites, mode='lines', name='Limite',
                                                        line=dict(dash='dash', color='red')))
            
            fig_meter_dashboard.update_layout(title=f"Consumo Horário de {medidor}",
                                              xaxis_title="Hora do Dia",
                                              yaxis_title="Consumo (kWh)",
                                              xaxis=dict(tickmode='linear', dtick=1),
                                              height=300,
                                              margin=dict(l=40, r=20, t=50, b=40))
            st.plotly_chart(fig_meter_dashboard, use_container_width=True)


    with tab5: # Calendar
        st.header("📅 Calendário de Consumo")
        
        # Selecionar o mês do calendário
        current_month = st.date_input("Selecione o mês do calendário:", 
                                       value=st.session_state.selected_date.replace(day=1),
                                       format="DD/MM/YYYY")

        # Gerar os dias do mês selecionado
        first_day_of_month = current_month.replace(day=1)
        last_day_of_month = (first_day_of_month + timedelta(days=32)).replace(day=1) - timedelta(days=1) # Garante que pega o último dia do mês
        
        # O número de "espaços" em branco antes do primeiro dia do mês
        start_weekday = first_day_of_month.weekday() # Monday=0, Sunday=6

        # Criar uma lista de todos os dias no mês
        all_days_in_month = [first_day_of_month + timedelta(days=i) for i in range((last_day_of_month - first_day_of_month).days + 1)]

        # Preencher com dias em branco no início para alinhar com a semana
        calendar_days = [''] * start_weekday + all_days_in_month

        # Preencher com dias em branco no final para completar a última semana
        # O número de elementos para completar até o próximo múltiplo de 7
        remaining_fill = len(calendar_days) % 7
        if remaining_fill != 0:
            calendar_days.extend([''] * (7 - remaining_fill))

        week_days = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]

        # Cabeçalho do calendário (dias da semana)
        st.markdown(
            "<div style='display: grid; grid-template-columns: repeat(7, 1fr); gap: 5px;'>" +
            # CORREÇÃO DA SINTAXE DO F-STRING AQUI: Aspas simples para o estilo interno
            "".join([f"<div style='font-weight: bold; text-align: center;'>{d}</div>" for d in week_days]) + 
            "</div>", unsafe_allow_html=True
        )

        # Grade dos dias do calendário
        st.markdown("<div style='display: grid; grid-template-columns: repeat(7, 1fr); gap: 5px;'>", unsafe_allow_html=True)
        for day_obj in calendar_days:
            if day_obj == '':
                st.markdown("<div></div>", unsafe_allow_html=True) # Espaço em branco para dias fora do mês
            else:
                # Filtra os dados para o dia específico (garante que está comparando apenas a parte da data)
                day_df_calendar = df[df['date'] == pd.to_datetime(day_obj.date())]

                st.markdown(f"<div style='border: 1px solid #ccc; padding: 5px; height: 180px;'>", unsafe_allow_html=True)
                st.markdown(f"<div style='font-weight: bold; text-align: center;'>{day_obj.day}</div>", unsafe_allow_html=True)

                if not day_df_calendar.empty:
                    # Calcula o consumo por hora para a Área Produtiva
                    day_consumption = day_df_calendar.groupby(day_df_calendar['Datetime'].dt.hour)['Área Produtiva'].sum().reset_index()
                    day_consumption.columns = ['Hora', 'Consumo']

                    # Adiciona os limites da Área Produtiva para comparação
                    limites_ap = st.session_state.limites_por_medidor_horario.get("Área Produtiva", [0]*24)
                    
                    fig_day = go.Figure()
                    fig_day.add_trace(go.Bar(x=day_consumption['Hora'], y=day_consumption['Consumo'], name='Consumo'))
                    
                    # Adiciona os limites como linha
                    horas_limite = list(range(len(limites_ap)))
                    fig_day.add_trace(go.Scatter(x=horas_limite, y=limites_ap, mode='lines', 
                                                 name='Limite', line=dict(dash='dash', color='red')))

                    fig_day.update_layout(
                        margin=dict(l=0, r=0, t=0, b=0), # Margens mínimas para gráficos pequenos
                        height=120, # Altura reduzida
                        showlegend=False,
                        xaxis=dict(showticklabels=False, showgrid=False, zeroline=False), # Oculta rótulos e linhas do eixo X
                        yaxis=dict(showticklabels=False, showgrid=False, zeroline=False)  # Oculta rótulos e linhas do eixo Y
                    )
                    st.plotly_chart(fig_day, use_container_width=True, config={'displayModeBar': False})
                else:
                    st.markdown("<div style='text-align: center; color: #888;'>Sem dados</div>", unsafe_allow_html=True)
                st.markdown("</div>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with tab6: # Conversion
        st.header("⚙️ Ferramenta de Conversão de CSV para JSON de Limites")
        st.write("Use esta ferramenta para converter um arquivo CSV de limites horários em um JSON compatível com o PowerTrack.")
        st.info("O CSV deve conter uma coluna 'Timestamp' e as colunas de medidores (ex: 'Área Produtiva', 'MP&L'), com 24 linhas para representar as 24 horas do dia.")

        uploaded_file = st.file_uploader("Arraste e solte seu arquivo CSV aqui", type=["csv"])

        if uploaded_file is not None:
            try:
                # Lê o CSV
                df_conversion = pd.read_csv(uploaded_file)
                st.write("CSV carregado com sucesso! Primeiras linhas:")
                st.dataframe(df_conversion)

                # Converte para JSON
                # Assumindo que o CSV tem colunas como 'Timestamp', 'medidor1', 'medidor2'
                json_output = df_conversion.to_json(orient="records", date_format="iso", indent=4)

                st.subheader("JSON Gerado:")
                st.code(json_output, language="json")

                st.download_button(
                    label="Baixar JSON dos Limites",
                    data=json_output,
                    file_name="limites_gerados.json",
                    mime="application/json"
                )
                st.success("JSON gerado e pronto para download!")

            except Exception as e:
                st.error(f"Erro ao processar o arquivo CSV: {e}. Verifique o formato do arquivo.")
        else:
            st.info("Aguardando o upload de um arquivo CSV de limites.")

    with tab7: # Month Prediction
        st.header("🗓️ Previsão de Consumo Mensal")
        
        st.subheader("Situação Atual do Mês")
        # Filtra os dados para o mês e ano da data selecionada na sidebar
        current_month_data = df[
            (df['date'].dt.month == selected_date.month) & 
            (df['date'].dt.year == selected_date.year)
        ]
        
        if not current_month_data.empty:
            
            # Reutiliza a meta mensal estimada da Área Produtiva
            if "limites_por_medidor_horario" in st.session_state and "Área Produtiva" in st.session_state.limites_por_medidor_horario:
                limites_ap_diarios_kwh = sum(st.session_state.limites_por_medidor_horario["Área Produtiva"])
                
                import calendar
                num_days_in_month = calendar.monthrange(selected_date.year, selected_date.month)[1]
                meta_mensal_ap_kwh = limites_ap_diarios_kwh * num_days_in_month
                
                # Dias que já passaram no mês até a data selecionada
                days_passed = selected_date.day 
                
                # Consumo acumulado até o dia selecionado
                consumption_so_far = current_month_data[current_month_data['date'] <= selected_date]["Área Produtiva"].sum()
                
                # Dias restantes no mês (do dia seguinte à selected_date até o fim do mês)
                remaining_days_in_month = num_days_in_month - days_passed
                
                # Previsão simples baseada na média diária até agora
                avg_daily_consumption = consumption_so_far / days_passed if days_passed > 0 else 0
                predicted_total_consumption = consumption_so_far + (avg_daily_consumption * remaining_days_in_month)
                
                st.metric("Consumo Acumulado no Mês (Área Produtiva)", f"{consumption_so_far:.2f} kWh")
                st.metric("Meta Mensal (Área Produtiva)", f"{meta_mensal_ap_kwh:.2f} kWh")
                st.metric("Previsão Total do Mês (Área Produtiva)", f"{predicted_total_consumption:.2f} kWh", 
                          delta=f"{predicted_total_consumption - meta_mensal_ap_kwh:.2f} kWh", delta_color="inverse")

                st.subheader("Simulação de Monte Carlo para Previsão Diária Futura")
                
                n_simulations = st.slider("Número de Simulações:", 100, 10000, 1000)
                
                # Coleta dados históricos do consumo diário da Área Produtiva
                daily_consumption_ap = df.groupby(df['date'])['Área Produtiva'].sum().reset_index()
                
                if not daily_consumption_ap.empty and len(daily_consumption_ap) > 1:
                    # Calcula as variações diárias. Usamos diferenças absolutas ou percentuais
                    # Aqui, a sugestão é usar a diferença em kWh diretamente, não percentual
                    daily_changes = daily_consumption_ap['Área Produtiva'].diff().dropna()
                    
                    if not daily_changes.empty:
                        simulated_consumptions = []
                        # Pega o último consumo diário disponível nos dados históricos
                        last_known_daily_consumption = daily_consumption_ap[daily_consumption_ap['date'] == selected_date]['Área Produtiva'].iloc[0] if selected_date in daily_consumption_ap['date'].values else daily_consumption_ap['Área Produtiva'].iloc[-1]
                        
                        for _ in range(n_simulations):
                            # Inicia o caminho da simulação com o último consumo diário conhecido (do selected_date)
                            sim_path_daily = [last_known_daily_consumption] 
                            for _ in range(remaining_days_in_month):
                                # Amostra uma mudança aleatória dos históricos de mudanças
                                change = np.random.choice(daily_changes) 
                                next_consumption = sim_path_daily[-1] + change
                                sim_path_daily.append(max(0, next_consumption)) # Garante que o consumo não seja negativo
                            
                            # Adiciona os consumos simulados para os dias futuros (exclui o primeiro, que é o último conhecido)
                            simulated_consumptions.append(sim_path_daily[1:]) 
                        
                        simulated_consumptions_df = pd.DataFrame(simulated_consumptions).T
                        
                        # Calcula média e percentis para cada dia futuro
                        mean_path = simulated_consumptions_df.mean(axis=1)
                        p5_path = simulated_consumptions_df.quantile(0.05, axis=1)
                        p95_path = simulated_consumptions_df.quantile(0.95, axis=1)
                        
                        # Datas futuras para o gráfico (do dia seguinte ao selected_date até o fim do mês)
                        future_dates = [selected_date + timedelta(days=i+1) for i in range(remaining_days_in_month)]
                        
                        fig_monte_carlo = go.Figure()
                        
                        # Adiciona o consumo real até o selected_date
                        current_month_daily_consumption = current_month_data.groupby('date')['Área Produtiva'].sum().reset_index()
                        fig_monte_carlo.add_trace(go.Scatter(x=current_month_daily_consumption['date'], 
                                                             y=current_month_daily_consumption['Área Produtiva'], 
                                                             mode='lines+markers', name='Consumo Real (diário)'))
                        
                        # Adiciona a previsão média
                        fig_monte_carlo.add_trace(go.Scatter(x=future_dates, y=mean_path, mode='lines', name='Previsão Média',
                                                             line=dict(color='blue')))
                        
                        # Adiciona os intervalos de confiança (se houver dados suficientes)
                        if not simulated_consumptions_df.empty and len(simulated_consumptions_df.columns) > 1:
                            fig_monte_carlo.add_trace(go.Scatter(x=future_dates + future_dates[::-1],
                                                                y=list(p95_path) + list(p5_path.iloc[::-1]), # Ajustado para .iloc
                                                                fill='toself',
                                                                fillcolor='rgba(0,100,80,0.2)',
                                                                line=dict(color='rgba(255,255,255,0)'),
                                                                hoverinfo="skip",
                                                                name='Intervalo de Confiança (90%)'))
                        
                        fig_monte_carlo.update_layout(title="Simulação de Monte Carlo para Consumo Diário da Área Produtiva",
                                                      xaxis_title="Data", yaxis_title="Consumo (kWh)")
                        st.plotly_chart(fig_monte_carlo, use_container_width=True)
                        
                        # Diagnóstico Inteligente
                        # Soma o consumo já ocorrido com a soma das médias diárias futuras
                        total_predicted_consumption_monte_carlo = consumption_so_far + mean_path.sum() 
                        
                        if total_predicted_consumption_monte_carlo > meta_mensal_ap_kwh:
                            st.warning(f"🚨 **Alerta:** A previsão de Monte Carlo indica que o consumo total do mês "
                                       f"({total_predicted_consumption_monte_carlo:.2f} kWh) pode exceder a meta "
                                       f"mensal ({meta_mensal_ap_kwh:.2f} kWh) em "
                                       f"{total_predicted_consumption_monte_carlo - meta_mensal_ap_kwh:.2f} kWh.")
                        else:
                            st.success(f"✅ **Ótimo:** A previsão de Monte Carlo indica que o consumo total do mês "
                                       f"({total_predicted_consumption_monte_carlo:.2f} kWh) está dentro da meta "
                                       f"mensal ({meta_mensal_ap_kwh:.2f} kWh), com uma folga de "
                                       f"{meta_mensal_ap_kwh - total_predicted_consumption_monte_carlo:.2f} kWh.")

                    else:
                        st.info("Dados históricos insuficientes para calcular variações diárias para Monte Carlo. Certifique-se de ter pelo menos dois dias de dados de consumo.")
                else:
                    st.info("Dados históricos insuficientes para simulação de Monte Carlo. O Monte Carlo requer dados diários para prever tendências.")
            else:
                st.info("Meta mensal para 'Área Produtiva' não configurada nos limites. Verifique `limites_padrao.json`.")

        else:
            st.info("Não há dados para o mês selecionado para realizar a previsão mensal. Verifique se os dados cobrem o mês selecionado.")

    with tab8: # ML Prediction
        st.header("�� Previsão de Consumo com Machine Learning")

        st.subheader("Configuração da Previsão de ML")
        
        # Cria um DataFrame auxiliar para a previsão de ML
        # Agrega o consumo horário para consumo diário para a área produtiva
        if df is not None and not df.empty:
            df_ml = df.groupby(df['date'])['Área Produtiva'].sum().reset_index()
            df_ml.columns = ['date', 'consumption']
            
            # Converte as datas para um formato numérico que os modelos de ML podem usar (e.g., dias desde uma data base)
            # A data base será a primeira data do DataFrame de ML
            if not df_ml.empty:
                base_date_ml = df_ml['date'].min()
                df_ml['date_num'] = (df_ml['date'] - base_date_ml).dt.days
            else:
                st.warning("Não há dados suficientes para preparar o conjunto de dados de ML.")
                df_ml_ready = False # Flag para pular a seção de ML
            
            if not df_ml.empty: # Verifica novamente se df_ml não está vazio após o processamento
                # Define features (X) e target (y)
                X = df_ml[['date_num']]
                y = df_ml['consumption']

                # Divide os dados em conjuntos de treinamento e teste
                # Garante que haja dados suficientes para split (pelo menos 2 para treinamento, 1 para teste)
                if len(X) >= 3: # Regra geral para split: 2 para treino, 1 para teste
                    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
                    df_ml_ready = True
                else:
                    st.warning("Dados insuficientes para dividir em treino e teste. Usando todos os dados para treinamento e pulando a avaliação do modelo.")
                    X_train, y_train = X, y # Treina com tudo
                    X_test, y_test = pd.DataFrame(), pd.Series() # Sem dados de teste para avaliação
                    df_ml_ready = True # Pode proceder com o treinamento, mas sem avaliação de performance
            else:
                df_ml_ready = False


            # Modelos de Machine Learning
            models = {
                "Linear Regression": LinearRegression(),
                "Random Forest": RandomForestRegressor(random_state=42),
                "Gradient Boosting": GradientBoostingRegressor(random_state=42),
                "K-Neighbors": KNeighborsRegressor(),
                "Support Vector Machine": SVR()
            }
            
            # Seleção do dia para previsão
            selected_day_ml = st.date_input("Selecione o dia para prever o consumo:", value=datetime.today().date())
            
            # Converte a data selecionada para o formato numérico
            if df_ml_ready:
                selected_day_num = (selected_day_ml - base_date_ml).days
                selected_day_dt = pd.to_datetime(selected_day_ml) # Usado para o gráfico
            else:
                st.info("Não é possível realizar previsões de ML sem dados históricos suficientes.")
                selected_day_num = 0
                selected_day_dt = pd.to_datetime(selected_day_ml) # Apenas para evitar erro, valor não significativo

            st.subheader("Resultados da Previsão")

            results = []
            fig = go.Figure()

            if df_ml_ready:
                # Filtra os dados do mês selecionado para o gráfico
                selected_month_ml = selected_day_ml.month
                selected_year_ml = selected_day_ml.year
                df_month_ml = df_ml[(df_ml["date"].dt.month == selected_month_ml) & 
                                    (df_ml["date"].dt.year == selected_year_ml)]

                fig.add_trace(go.Scatter(x=df_month_ml["date"], y=df_month_ml["consumption"], 
                                        mode='lines+markers', name='Consumo Real (Área Produtiva)'))

                for name, model in models.items():
                    try:
                        if not X_train.empty and not y_train.empty:
                            model.fit(X_train, y_train)
                            
                            # Avaliação do modelo apenas se houver dados de teste
                            mae, rmse, accuracy = "N/A", "N/A", "N/A"
                            if not X_test.empty and not y_test.empty:
                                y_pred = model.predict(X_test)
                                mae = round(mean_absolute_error(y_test, y_pred), 2)
                                rmse = round(np.sqrt(mean_squared_error(y_test, y_pred)), 2)
                                rmse_norm = rmse / y_test.mean() if y_test.mean() != 0 else np.inf
                                accuracy = round(max(0, 1 - rmse_norm) * 100, 2)
                            
                            prediction = model.predict(np.array([[selected_day_num]]))[0] # Previsão para o dia selecionado
                            
                            # Gera o fit para todos os dados históricos para plotar
                            y_fit_all = model.predict(X) 
                            fit_df = pd.DataFrame({"date": df_ml["date"], "fit": y_fit_all})
                            fit_df_month = fit_df[(fit_df["date"].dt.month == selected_month_ml) & 
                                                (fit_df["date"].dt.year == selected_year_ml)]

                            fig.add_trace(go.Scatter(x=fit_df_month["date"], y=fit_df_month["fit"], 
                                                    mode='lines', name=f'Fit {name}'))
                            
                            # Adiciona a previsão do dia selecionado como um marcador
                            fig.add_trace(go.Scatter(x=[selected_day_dt], y=[prediction], mode='markers',
                                                     name=f'Previsão {name} ({selected_day_dt.strftime("%d/%m")})', 
                                                     marker=dict(size=10, symbol='star', color='black')))

                            results.append({
                                "Modelo": name,
                                "MAE": mae,
                                "RMSE": rmse,
                                "Acurácia (%)": accuracy,
                                "Previsão para o dia selecionado": round(prediction, 2)
                            })
                        else:
                            st.info(f"Dados de treinamento insuficientes para o modelo {name}.")
                            results.append({
                                "Modelo": name,
                                "MAE": "N/A", "RMSE": "N/A", "Acurácia (%)": "N/A", "Previsão para o dia selecionado": "N/A"
                            })

                    except Exception as e:
                        st.warning(f"Erro ao treinar/prever com o modelo {name}: {e}. Pode ser dados insuficientes ou incompatíveis.")
                        results.append({
                            "Modelo": name,
                            "MAE": "Erro", "RMSE": "Erro", "Acurácia (%)": "Erro", "Previsão para o dia selecionado": "Erro"
                        })
                
                fig.update_layout(title="Previsão de Consumo de Energia (Mês Selecionado)",
                                  xaxis_title="Data",
                                  yaxis_title="Consumo (kWh)",
                                  legend_title="Legenda")
                st.plotly_chart(fig, use_container_width=True)

                results_df = pd.DataFrame(results)
                if not results_df.empty:
                    # Ordena apenas se houver valores válidos para ordenar
                    if "Acurácia (%)" in results_df.columns and pd.api.types.is_numeric_dtype(results_df["Acurácia (%)"]):
                        results_df = results_df.sort_values(by="Acurácia (%)", ascending=False).reset_index(drop=True)
                    
                    st.subheader("📊 Desempenho e Previsões dos Modelos")
                    st.dataframe(results_df, use_container_width=True)
                else:
                    st.info("Nenhum resultado de modelo para exibir.")
            else:
                st.info("Não há dados suficientes ou prontos para realizar a previsão com Machine Learning.")
        else:
            st.info("Carregue os dados para realizar a previsão com Machine Learning.")
