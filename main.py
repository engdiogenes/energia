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
    st.sidebar.image("logo.png", width=360)
    # st.logo("logo.png", size="Large", link=None, icon_image=None)
    st.header(" Entrada de Dados")
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
    else:
        dados_colados = st.text_area("Cole os dados aqui (tabulados):", height=300)

    # Créditos e data no final da sidebar
    if "data_selecionada" in st.session_state:
        data_formatada = st.session_state.data_selecionada.strftime('%d/%m/%Y')
    else:
        data_formatada = "Não selecionada"

    st.markdown(
        f"""
        <hr style="margin-top: 2rem; margin-bottom: 0.5rem;">
        <div style='font-size: 0.8rem; color: gray; text-align: center;'>
            Data selecionada: <strong>{data_formatada}</strong><br>
            Desenvolvido por <strong>Diógenes Oliveira</strong>
        </div>
        """,
        unsafe_allow_html=True
    )

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

            tabs = st.tabs([" Visão Geral", " Por Medidor", " Limites", " Dashboard", " Calendário", " Conversão "])

            # TABS 1 - VISÃO GERAL
            with tabs[0]:
                st.subheader(f"Resumo do Dia {data_selecionada.strftime('%d/%m/%Y')}")
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

                col1.metric("🎯 Target Diário Geral", f"{limite_geral:.2f} kWh")
                col2.metric("⚡ Consumo Real Geral", f"{consumo_geral:.2f} kWh",
                            delta=f"{delta_geral:.2f} kWh",
                            delta_color="normal" if delta_geral == 0 else ("inverse" if delta_geral < 0 else "off"))
                col3.metric("📉 Saldo do Dia (Geral)", f"{saldo_geral:.2f} kWh", delta_color="inverse")

                col4.metric("🎯 Target Área Produtiva", f"{limites_area:.2f} kWh")
                col5.metric("🏭 Consumo Área Produtiva", f"{consumo_area:.2f} kWh",
                            delta=f"{delta_area:.2f} kWh",
                            delta_color="normal" if delta_area == 0 else ("inverse" if delta_area < 0 else "off"))
                col6.metric("📉 Saldo do Dia (Área Produtiva)", f"{saldo_area:.2f} kWh", delta_color="inverse")

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
                st.title("Conversor CSV para JSON - Limites Horários por Medidor")
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

    except Exception as e:
        st.error(f"Erro ao processar os dados: {e}")
