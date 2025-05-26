import matplotlib.pyplot as plt
import pandas as pd
from fpdf import FPDF
import datetime

def gerar_relatorio_pdf(consumo, limites_por_medidor_horario, data_selecionada):
    pdf = FPDF()

    # Capa
    pdf.add_page()
    pdf.set_font("Arial", 'B', 24)
    pdf.set_text_color(0, 51, 102)
    pdf.cell(200, 20, txt="Relatório de Consumo Energético", ln=True, align="C")
    pdf.set_font("Arial", 'I', 16)
    pdf.cell(200, 10, txt=f"Data: {data_selecionada.strftime('%d/%m/%Y')}", ln=True, align="C")
    try:
        pdf.image("logo.png", x=10, y=8, w=33)
    except:
        pass

    # Página de métricas
    pdf.add_page()
    pdf.set_font("Arial", 'B', 16)
    pdf.set_fill_color(230, 230, 250)
    pdf.cell(200, 10, txt="Métricas da Visão Geral", ln=True, align="L", fill=True)
    pdf.set_font("Arial", size=12)
    pdf.set_text_color(0, 0, 0)

    consumo_area = consumo["Área Produtiva"].sum()
    consumo_pccb = consumo["PCCB"].sum() if "PCCB" in consumo else 0
    consumo_maiw = consumo["MAIW"].sum() if "MAIW" in consumo else 0
    consumo_geral = consumo_area + consumo_pccb + consumo_maiw + 300

    limites_area = sum(
        limites_por_medidor_horario.get(medidor, [0] * 24)[h]
        for h in range(24)
        for medidor in ["MP&L", "GAHO", "CAG", "SEOB", "EBPC", "PMDC-OFFICE", "OFFICE + CANTEEN", "TRIM&FINAL"]
    ) + 13.75 * 24

    limite_pccb = sum(limites_por_medidor_horario.get("PCCB", [0] * 24))
    limite_maiw = sum(limites_por_medidor_horario.get("MAIW", [0] * 24))
    limite_geral = limites_area + limite_pccb + limite_maiw + 300

    delta_geral = consumo_geral - limite_geral
    delta_area = consumo_area - limites_area
    saldo_geral = limite_geral - consumo_geral
    saldo_area = limites_area - consumo_area

    for label, valor in [
        ("Consumo Geral", consumo_geral),
        ("Limite Geral", limite_geral),
        ("Delta Geral", delta_geral),
        ("Saldo Geral", saldo_geral),
        ("Consumo Área Produtiva", consumo_area),
        ("Limite Área Produtiva", limites_area),
        ("Delta Área Produtiva", delta_area),
        ("Saldo Área Produtiva", saldo_area)
    ]:
        pdf.cell(200, 10, txt=f"{label}: {valor:.2f} kWh", ln=True)

    # Gráfico de consumo horário
    pdf.add_page()
    pdf.set_font("Arial", 'B', 16)
    pdf.cell(200, 10, txt="Gráfico de Consumo Horário", ln=True, align="C")
    plt.figure(figsize=(10, 5))
    for medidor in consumo.columns:
        if medidor != "Datetime":
            plt.plot(consumo["Datetime"], consumo[medidor], label=medidor)
    plt.xlabel("Hora do Dia")
    plt.ylabel("Consumo (kWh)")
    plt.legend()
    plt.title("Consumo Horário")
    plt.savefig("consumo_horario.png", bbox_inches="tight", facecolor="white")
    plt.close()
    pdf.image("consumo_horario.png", x=10, y=30, w=190)

    # Gráfico de consumo diário
    pdf.add_page()
    pdf.set_font("Arial", 'B', 16)
    pdf.cell(200, 10, txt="Gráfico de Consumo Diário", ln=True, align="C")
    consumo_diario = consumo.copy()
    consumo_diario["Data"] = consumo_diario["Datetime"].dt.date
    consumo_diario = consumo_diario.drop(columns=["Datetime"])
    consumo_agrupado = consumo_diario.groupby("Data").sum().reset_index()
    plt.figure(figsize=(10, 5))
    for medidor in consumo_agrupado.columns:
        if medidor != "Data":
            plt.bar(consumo_agrupado["Data"], consumo_agrupado[medidor], label=medidor)
    plt.xlabel("Data")
    plt.ylabel("Consumo Total (kWh)")
    plt.legend()
    plt.title("Consumo Diário")
    plt.savefig("consumo_diario.png", bbox_inches="tight", facecolor="white")
    plt.close()
    pdf.image("consumo_diario.png", x=10, y=30, w=190)

    # Tabela de consumo por hora
    pdf.add_page()
    pdf.set_font("Arial", 'B', 16)
    pdf.cell(200, 10, txt="Tabela de Consumo por Hora", ln=True, align="C")
    pdf.set_font("Arial", size=8)
    for i in range(len(consumo)):
        row = consumo.iloc[i]
        texto = f"{row['Datetime']} | " + " | ".join(f"{col}: {row[col]:.2f}" for col in consumo.columns if col != "Datetime")
        pdf.multi_cell(0, 5, txt=texto)

    # Rodapé
    pdf.set_y(-15)
    pdf.set_font("Arial", 'I', 8)
    pdf.cell(0, 10, f"Gerado em {datetime.datetime.now().strftime('%d/%m/%Y %H:%M')}", 0, 0, 'C')

    # Salvar
    pdf.output("relatorio_consumo_energetico.pdf")
