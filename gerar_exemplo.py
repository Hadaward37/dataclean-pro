"""
gerar_exemplo.py
────────────────
Gera um arquivo Excel "sujo" de exemplo para testar o DataClean Pro.
Replica exatamente a estrutura descrita no desafio.

Uso: python gerar_exemplo.py
Saída: vendas_brutas_exemplo.xlsx
"""
import pandas as pd
import openpyxl
from openpyxl import Workbook

wb = Workbook()
ws = wb.active
ws.title = "Dados"

# ── 3 linhas de cabeçalho inúteis ─────────────────────────────────────────
ws.append(["RELATÓRIO MENSAL DE VENDAS", None, None, None, None, None, None, None])
ws.append(["Gerado em: 01/01/2024", None, None, None, None, None, None, None])
ws.append(["Confidencial - Uso Interno", None, None, None, None, None, None, None])

# ── Linha 4: cabeçalho real ───────────────────────────────────────────────
ws.append(["ID", "Cliente", "Produto", "Gênero", "Endereço", "Data Venda", "Valor", "Observações"])

# ── Dados sujos ───────────────────────────────────────────────────────────
registros = [
    [1, "SANTOS, Carlos",   "iPhone 15 - Apple",        "M", "Rua das Flores, 100, São Paulo, SP, Brasil",     "15/01/2024", "R$ 8.999,00", "Pagamento à vista"],
    [2, "OLIVEIRA, Ana",    "Galaxy S24 - Samsung",      "F", "Av. Paulista, 500, Campinas, SP, Brasil",        "16/01/2024", "R$ 7.499,00", None],
    [3, "FERREIRA, João",   "MacBook Air - Apple",       "M", "Rua XV, 200, Curitiba, PR, Brasil",              "17/01/2024", "R$ 12.500,00", "Parcelado 12x"],
    [None, None, None, None, None, None, None, None],  # linha em branco
    [4, "LIMA, Maria",      "Notebook Ideapad - Lenovo", "F", "Rua do Comércio, 88, Porto Alegre, RS, Brasil", "18/01/2024", "R$ 3.299,00", None],
    [5, "COSTA, Pedro",     "AirPods Pro - Apple",       "M", "Av. Brasil, 777, Belo Horizonte, MG, Brasil",   "19/01/2024", "R$ 1.899,00", "Troca por garantia"],
    [6, "ROCHA, Fernanda",  "Watch Series 9 - Apple",    "F", "Rua das Laranjeiras, 33, Rio de Janeiro, RJ, Brasil", "20/01/2024", "R$ 3.099,00", None],
    [7, "ALVES, Ricardo",   "Pixel 8 - Google",          "M", "Av. Independência, 400, Recife, PE, Brasil",    "21/01/2024", "R$ 5.499,00", None],
    [None, None, None, None, None, None, None, None],  # linha em branco
    [8, "MARTINS, Beatriz", "Tab S9 - Samsung",          "F", "Rua Tiradentes, 90, Fortaleza, CE, Brasil",     "22/01/2024", "R$ 4.799,00", "Enviado por Sedex"],
    [9, "SOUSA, Gabriel",   "Moto Edge 40 - Motorola",   "M", "Av. Goiás, 150, Goiânia, GO, Brasil",           "23/01/2024", "R$ 2.199,00", None],
    [10, "PEREIRA, Julia",  "Redmi Note 13 - Xiaomi",    "F", "Rua Amazonas, 55, Manaus, AM, Brasil",          "24/01/2024", "R$ 1.299,00", None],
]

for r in registros:
    ws.append(r)

# ── Coluna extra totalmente em branco (coluna I) ──────────────────────────
# (já vazia por padrão)

wb.save("vendas_brutas_exemplo.xlsx")
print("✅ Arquivo de exemplo gerado: vendas_brutas_exemplo.xlsx")
print("   Configure o app com 'Linhas para pular = 3' e faça o upload!")