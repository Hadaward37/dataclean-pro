# ⚡ DataClean Pro — ETL Inteligente para Contabilidade

Automatize a limpeza de planilhas do Excel em segundos.  
Inspirado no método Power Query da Hashtag Treinamentos — agora em Python, escalável e vendável.

---

## 🚀 Instalação Rápida

```bash
pip install -r requirements.txt
```

---

## 📦 Estrutura do Projeto

```
etl_app/
├── app.py            ← Web App Streamlit (produto final)
├── etl_engine.py     ← Motor ETL (CLI · Watchdog · Lambda)
├── requirements.txt
└── README.md
```

---

## 1️⃣ Web App (Streamlit)

```bash
streamlit run app.py
```

Acesse: **http://localhost:8501**

Funcionalidades:

- Upload de `.xlsx`, `.xls` ou `.csv`
- Configuração visual de todas as transformações
- Preview da base limpa
- Download de `Base_Vendas_Limpa.xlsx` formatado

---

## 2️⃣ Linha de Comando (CLI)

```bash
# Processar um arquivo
python etl_engine.py vendas_brutas.xlsx

# Especificar arquivo de saída
python etl_engine.py vendas_brutas.xlsx minha_saida.xlsx

# Com arquivo de configuração JSON
python etl_engine.py vendas.xlsx saida.xlsx config.json
```

### config.json de exemplo

```json
{
  "skip_rows": 3,
  "col_produto": ["Produto", "Item"],
  "col_cliente": ["Cliente", "Nome"],
  "col_genero": ["Gênero", "Sexo"],
  "col_endereco": ["Endereço"],
  "col_observacoes": ["Observações", "Obs"],
  "delimitador_produto": " - ",
  "colunas_data": ["Data", "Data Venda"],
  "colunas_valor": ["Valor", "Total", "Preço"]
}
```

---

## 3️⃣ Watchdog (Monitoramento de Pasta)

Coloque arquivos na pasta `./input` e eles serão processados automaticamente:

```bash
# Padrão: ./input → ./output
python etl_engine.py --watch

# Caminhos personalizados
python etl_engine.py --watch /caminho/entrada /caminho/saida
```

Instale o watchdog:

```bash
pip install watchdog
```

---

## 4️⃣ AWS Lambda

```python
from etl_engine import lambda_handler

# O evento chega com o arquivo em base64
result = lambda_handler({
    "file_base64": "<base64 do arquivo>",
    "file_name": "vendas.xlsx",
    "config": {
        "skip_rows": 3,
        "colunas_valor": ["Valor", "Total"]
    }
})
```

**Integração com S3:**

```python
import boto3, base64
from etl_engine import lambda_handler

def handler(event, context):
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=event["bucket"], Key=event["key"])
    file_bytes = base64.b64encode(obj["Body"].read()).decode()
    return lambda_handler({"file_base64": file_bytes, "file_name": event["key"]})
```

---

## 5️⃣ Google Cloud Function

```python
# main.py para GCF
import base64
from etl_engine import lambda_handler

def etl_function(request):
    data = request.get_json()
    result = lambda_handler(data)
    return result["body"], result["statusCode"]
```

Deploy:

```bash
gcloud functions deploy etl_function \
  --runtime python311 \
  --trigger-http \
  --allow-unauthenticated
```

---

## 🔄 Transformações Aplicadas

| #   | Etapa                 | Descrição                                                                      |
| --- | --------------------- | ------------------------------------------------------------------------------ |
| 1   | Limpeza Estrutural    | Pula N linhas, remove linhas/colunas 100% vazias, remove coluna de Observações |
| 2   | Split de Produto      | `"iPhone 15 - Apple"` → `Produto="iPhone 15"`, `Marca="Apple"`                 |
| 3   | Normalização de Nomes | `"SANTOS, Carlos"` → `"Carlos Santos"`                                         |
| 4   | Mapeamento de Gênero  | `"M"` → `"Masculino"`, `"F"` → `"Feminino"`                                    |
| 5   | Extração de Cidade    | `"Rua X, 123, São Paulo, SP, Brasil"` → `Cidade="São Paulo"`                   |
| 6   | Tipagem               | Datas → `datetime`, Valores → `float64`                                        |

---

## 💼 Para Vender para Escritórios de Contabilidade

- **Web App**: hospede no Streamlit Cloud (gratuito) ou em servidor próprio
- **SaaS**: adicione autenticação com `streamlit-authenticator` + banco PostgreSQL
- **API**: exponha o `lambda_handler` via FastAPI para integrações com ERP
- **White-label**: renomeie e customize o visual por cliente

---

Desenvolvido com 🐍 Python · Pandas · Streamlit · OpenPyXL
