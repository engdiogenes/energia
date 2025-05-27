from fpdf import FPDF
import pandas as pd
import matplotlib.pyplot as plt
import datetime

def gerar_relatorio_pdf(consumo, limites_df, data_selecionada):
    # Filtrar dados de consumo e limites pela data selecionada
    consumo_dia = consumo[consumo["Datetime"].dt.date == data_selecionada]
    limites_dia = limites_df[limites_df["Data"] == data_selecionada]

    # Preparar dados para gráficos
    consumo_area = consumo_dia["Área Produtiva"].tolist()
    limite_area = [
        limites_dia[limites_dia["Hora"] == h]["Área Produtiva"].values[0]
        if not limites_dia[limites_dia["Hora"] == h].empty else 0
        for h in range(24)
    ]

    medidores = consumo.columns.tolist()
    medidores.remove("Datetime")
    medidores.remove("Área Produtiva")

    consumo_medidores = [consumo_dia[medidor].sum() for medidor in medidores]
    limite_medidores = [
        sum(limites_dia[limites_dia["Hora"] == h][medidor].values)
        if medidor in limites_dia.columns else 0
        for medidor in medidores
        for h in range(24)
    ]

    dias_semana = consumo["Datetime"].dt.day_name().unique().tolist()
    consumo_dias = [
        consumo[consumo["Datetime"].dt.day_name() == dia]["Área Produtiva"].sum()
        for dia in dias_semana
    ]

    # Criar PDF
    pdf = FPDF()

    # Página 1 - Capa
    pdf.add_page()
    pdf.set_font("Arial", size=24)
    pdf.cell(200, 10, txt="Relatório de Consumo Energético", ln=True, align="C")
    pdf.set_font("Arial", size=18)
    pdf.cell(200, 10, txt=f"Data: {data_selecionada.strftime('%d/%m/%Y')}", ln=True, align="C")
    pdf.set_font("Arial", size=12)
    pdf.set_y(-15)
    pdf.cell(0, 10, "Desenvolvido por Diógenes Oliveira - Eng. Eletricista - Jaguar Land Rover Brasil", 0, 0, "C")

    # Página 2 - Resumo Diário
    pdf.add_page()
    pdf.set_font("Arial", size=18)
    pdf.cell(200, 10, txt="Resumo Diário", ln=True, align="C")
    pdf.set_font("Arial", size=12)
    pdf.cell(200, 10, txt=f"Consumo Total: {sum(consumo_area)} kWh", ln=True)
    pdf.cell(200, 10, txt=f"Limite Total: {sum(limite_area)} kWh", ln=True)

    plt.figure(figsize=(10, 5))
    plt.plot(consumo_area, label="Consumo")
    plt.plot(limite_area, label="Limite", linestyle="--")
    plt.xlabel("Hora do Dia")
    plt.ylabel("kWh")
    plt.title("Consumo vs Limite - Área Produtiva")
    plt.legend()
    plt.grid(True)
    plt.savefig("consumo_vs_limite.png")
    plt.close()
    pdf.image("consumo_vs_limite.png", x=10, y=60, w=190)

    # Página 3 - Dashboard
    pdf.add_page()
    pdf.set_font("Arial", size=18)
    pdf.cell(200, 10, txt="Dashboard", ln=True, align="C")
    pdf.set_font("Arial", size=12)
    for medidor, consumo_val, limite_val in zip(medidores, consumo_medidores, limite_medidores):
        pdf.cell(200, 10, txt=f"{medidor}: Consumo = {consumo_val:.2f} kWh, Limite = {limite_val:.2f} kWh", ln=True)

    for medidor, consumo_val, limite_val in zip(medidores, consumo_medidores, limite_medidores):
        plt.figure(figsize=(10, 5))
        plt.plot([consumo_val] * 24, label="Consumo")
        plt.plot([limite_val] * 24, label="Limite", linestyle="--")
        plt.xlabel("Hora do Dia")
        plt.ylabel("kWh")
        plt.title(f"Consumo vs Limite - {medidor}")
        plt.legend()
        plt.grid(True)
        filename = f"grafico_{medidor}.png"
        plt.savefig(filename)
        plt.close()
        pdf.add_page()
        pdf.image(filename, x=10, y=20, w=190)

    # Página 4 - Calendário
    pdf.add_page()
    pdf.set_font("Arial", size=18)
    pdf.cell(200, 10, txt="Calendário de Consumo", ln=True, align="C")
    plt.figure(figsize=(10, 5))
    plt.bar(dias_semana, consumo_dias)
    plt.xlabel("Dia da Semana")
    plt.ylabel("kWh")
    plt.title("Consumo da Área Produtiva por Dia da Semana")
    plt.grid(True)
    plt.savefig("consumo_por_dia.png")
    plt.close()
    pdf.image("consumo_por_dia.png", x=10, y=60, w=190)

    # Salvar PDF
    pdf.output("relatorio_consumo_energetico.pdf")
    print("Relatório gerado com sucesso.")
