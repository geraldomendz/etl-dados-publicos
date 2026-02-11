
# Pipeline ETL – Dados Públicos do Portal da Transparência

Este projeto implementa um **pipeline ETL completo** utilizando **dados públicos do Portal da Transparência**, com o objetivo de consolidar conhecimentos em **Python, Pandas, SQL e Engenharia de Dados**.

O pipeline segue uma **arquitetura em camadas (Bronze, Silver e Gold)**, garantindo organização, rastreabilidade e possibilidade de evolução para cenários de produção.

---

## Arquitetura do Projeto

```
etl_dados_publicos/
│
├── src/
│   ├── a_extract.py        # Extração de dados (Bronze)
│   ├── b_transform.py     # Transformação e normalização (Silver)
│   ├── c_load.py          # Carga no MySQL
│   ├── main.py            # Orquestração do pipeline
│   └── logger_config.py   # Configuração de logs
│
├── data/
│   ├── bronze/            # Dados brutos (JSON)
│   └── silver/            # Dados tratados (Parquet)
│
├── sql/
│   ├── ddl.sql            # Estrutura das tabelas (DDL)
│   └── gold_views.sql     # Views analíticas (Gold)
│
├── logs/
│   └── .gitkeep
│
├── .env.example           # Exemplo de variáveis de ambiente
├── requirements.txt       # Dependências do projeto
├── .gitignore
└── README.md
```

---

## Camada Bronze – Extração

A camada **Bronze** armazena os dados **exatamente como vêm da fonte**, sem transformações.

### Fontes utilizadas

* **Portal da Transparência – API oficial**

  * Tipos de transferência de despesas
  * Documentos de despesas públicas

### Características

* Autenticação via **API Key**
* Paginação controlada
* Persistência em **JSON**
* Rastreabilidade garantida

Arquivos gerados:

* `despesas_tipos_transferencia_YYYY_MM_DD.json`
* `despesas_documentos_YYYY_MM_DD.json`

---

## Camada Silver – Transformação

A camada **Silver** é responsável por:

* Limpeza
* Padronização
* Normalização
* Conversão para formato analítico

### Transformações realizadas

* Normalização de JSON aninhado (`pd.json_normalize`)
* Padronização de nomes de colunas
* Tratamento de valores inválidos (`"-"`, `null`)
* Criação de chave de documento (`doc_key`)
* Conversão para **Parquet (formato colunar)**

Arquivos gerados:

* `dim_tipo_transferencia_YYYY_MM_DD.parquet`
* `fato_documentos_despesa_YYYY_MM_DD.parquet`

---

## Camada Gold – Camada Analítica

A camada **Gold** é composta por **views SQL**, prontas para consumo por ferramentas de BI ou análises exploratórias.

### Views disponíveis

#### 🔹 Valor total por tipo de transferência

```sql
vw_total_valor_por_tipo_transferencia
```

#### 🔹 Quantidade de documentos por mês

```sql
vw_documentos_por_mes
```

#### 🔹 Auditoria e rastreabilidade

```sql
vw_auditoria_documentos
```

Essas views permitem responder perguntas de negócio sem impactar as tabelas base.

---

## Banco de Dados

* **MySQL**
* Modelagem em estrela:

  * Dimensão: `dim_tipo_transferencia`
  * Fato: `fato_documento_despesa`
* Uso de **ON DUPLICATE KEY UPDATE** para garantir **idempotência**

---

## Orquestração

O pipeline é orquestrado pelo arquivo:

```bash
src/main.py
```

Ele executa, em ordem:

1. Extract (Bronze)
2. Transform (Silver)
3. Load (MySQL)

O pipeline pode ser executado múltiplas vezes sem duplicar dados.

---

## Logs

O projeto utiliza o módulo `logging` com:

* Logs no console
* Logs persistidos em `logs/etl.log`
* Registro de erros com stacktrace
* Rotação automática de arquivos

Exemplo de log:

```
2026-02-08 09:55:08 | INFO | etl | Pipeline concluído em 1.7s
```

---

## Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto com base em `.env.example`.

Principais variáveis:

```env
PT_API_KEY=xxxxxxxxxxxxxxxx
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=senha
DB_NAME=etl_dados_publicos

DOCS_UNIDADE_GESTORA=175004
DOCS_GESTAO=00001
DOCS_DATA_EMISSAO=13/01/2023
DOCS_FASE=2
DOCS_MAX_PAGINAS=1
DOCS_PAGE_SIZE=50
```

---

## ▶ Como Executar o Projeto

### 1️⃣ Criar ambiente virtual

```bash
python -m venv .venv
```

### 2️⃣ Ativar ambiente

```bash
# Windows
.\.venv\Scripts\Activate.ps1
```

### 3️⃣ Instalar dependências

```bash
pip install -r requirements.txt
```

### 4️⃣ Executar pipeline

```bash
python src/main.py
```

---

## Conceitos Aplicados

* ETL (Extract, Transform, Load)
* Arquitetura Medalhão (Bronze / Silver / Gold)
* Engenharia de Dados com Python, Pandas e Banco de Dados


---

## Autor

Projeto desenvolvido por **Geraldo Mendes de Pontes Neto**,
Estudando de Sistemas de Informações pela UFPB.



