from fpdf import FPDF
import pandas as pd
import matplotlib.pyplot as plt
import datetime

# Simulated data
data_selecionada = datetime.date.today()
consumo_area = [100 + i * 50 for i in range(24)]
limite_area = [120 + i * 50 for i in range(24)]
medidores = ["MP&L", "GAHO", "CAG", "SEOB", "EBPC", "PMDC-OFFICE", "OFFICE + CANTEEN", "TRIM&FINAL"]
consumo_medidores = [100, 150, 200, 250, 300, 350, 400, 450]
limite_medidores = [120, 160, 210, 260, 310, 360, 410, 460]
dias_semana = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
consumo_dias = [700, 800, 900, 1000, 1100, 1200, 1300]

# Create PDF
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
for medidor, consumo, limite in zip(medidores, consumo_medidores, limite_medidores):
    pdf.cell(200, 10, txt=f"{medidor}: Consumo = {consumo} kWh, Limite = {limite} kWh", ln=True)

for medidor, consumo, limite in zip(medidores, consumo_medidores, limite_medidores):
    plt.figure(figsize=(10, 5))
    plt.plot([consumo] * 24, label="Consumo")
    plt.plot([limite] * 24, label="Limite", linestyle="--")
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
