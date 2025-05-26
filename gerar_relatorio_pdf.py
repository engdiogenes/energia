import matplotlib.pyplot as plt
import pandas as pd
from fpdf import FPDF
import datetime

def gerar_relatorio_pdf(consumo, limites_por_medidor_horario, data_selecionada):
    pdf = FPDF()

    # Capa
    pdf.add_page()
    pdf.set_font("Arial", size=24)
    pdf.cell(200, 20, txt="Relatório de Consumo Energético", ln=True, align="C")
    pdf.set_font("Arial", size=16)
    pdf.cell(200, 10, txt=f"Data: {data_selecionada.strftime('%d/%m/%Y')}", ln=True, align="C")

    # Página de métricas
    pdf.add_page()
    pdf.set_font("Arial", size=12)
    pdf.cell(200, 10, txt="Métricas da Visão Geral", ln=True, align="L")

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

    pdf.cell(200, 10, txt=f"Consumo Geral: {consumo_geral:.2f} kWh", ln=True)
    pdf.cell(200, 10, txt=f"Limite Geral: {limite_geral:.2f} kWh", ln=True)
    pdf.cell(200, 10, txt=f"Delta Geral: {delta_geral:.2f} kWh", ln=True)
    pdf.cell(200, 10, txt=f"Saldo Geral: {saldo_geral:.2f} kWh", ln=True)
    pdf.cell(200, 10, txt=f"Consumo Área Produtiva: {consumo_area:.2f} kWh", ln=True)
    pdf.cell(200, 10, txt=f"Limite Área Produtiva: {limites_area:.2f} kWh", ln=True)
    pdf.cell(200, 10, txt=f"Delta Área Produtiva: {delta_area:.2f} kWh", ln=True)
    pdf.cell(200, 10, txt=f"Saldo Área Produtiva: {saldo_area:.2f} kWh", ln=True)

    # Gráfico de consumo horário
    pdf.add_page()
    pdf.cell(200, 10, txt="Gráfico de Consumo Horário", ln=True)
    plt.figure(figsize=(10, 5))
    for medidor in consumo.columns:
        if medidor != "Datetime":
            plt.plot(consumo["Datetime"], consumo[medidor], label=medidor)
    plt.xlabel("Hora do Dia")
    plt.ylabel("Consumo (kWh)")
    plt.legend()
    plt.title("Consumo Horário")
    plt.savefig("consumo_horario.png")
    plt.close()
    pdf.image("consumo_horario.png", x=10, y=30, w=190)

    # Gráfico de consumo diário
    pdf.add_page()
    pdf.cell(200, 10, txt="Gráfico de Consumo Diário", ln=True)
    consumo_diario = consumo.copy()
    consumo_diario["Data"] = consumo_diario["Datetime"].dt.date
    consumo_agrupado = consumo_diario.groupby("Data").sum().reset_index()
    plt.figure(figsize=(10, 5))
    for medidor in consumo_agrupado.columns:
        if medidor != "Data":
            plt.bar(consumo_agrupado["Data"], consumo_agrupado[medidor], label=medidor)
    plt.xlabel("Data")
    plt.ylabel("Consumo Total (kWh)")
    plt.legend()
    plt.title("Consumo Diário")
    plt.savefig("consumo_diario.png")
    plt.close()
    pdf.image("consumo_diario.png", x=10, y=30, w=190)

    # Tabela de consumo por hora
    pdf.add_page()
    pdf.cell(200, 10, txt="Tabela de Consumo por Hora", ln=True)
    pdf.set_font("Arial", size=8)
    for i in range(len(consumo)):
        row = consumo.iloc[i]
        texto = f"{row['Datetime']} | " + " | ".join(f"{col}: {row[col]:.2f}" for col in consumo.columns if col != "Datetime")
        pdf.multi_cell(0, 5, txt=texto)

    # Salvar PDF
    pdf.output("relatorio_consumo_energetico.pdf")
