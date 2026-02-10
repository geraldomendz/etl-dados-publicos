# Pipeline ETL – Dados Públicos (Portal da Transparência)

Projeto de **estudo e portfólio** que implementa um pipeline ETL completo utilizando dados públicos do Portal da Transparência, com foco em **Engenharia de Dados**, boas práticas de desenvolvimento e organização de pipelines.

O projeto foi desenvolvido com **Python, Pandas, Parquet e MySQL**, aplicando a **Arquitetura Medalhão (Bronze / Silver / Gold)** para separar ingestão, transformação, carga e consumo analítico.

---

## Objetivo do Projeto

- Consolidar conhecimentos em **ETL e Engenharia de Dados**
- Trabalhar com **dados públicos reais**
- Aplicar conceitos de **arquitetura de dados**, **modelagem dimensional** e **idempotência**
- Construir um projeto organizado e reproduzível, voltado para **portfólio de estágio**

---

## Arquitetura

etl_dados_publicos/
│
├── src/
│   ├── a_extract.py        # Extração dos dados (Bronze)
│   ├── b_transform.py     # Transformação e limpeza (Silver)
│   ├── c_load.py          # Carga no MySQL
│   ├── main.py            # Orquestração do pipeline
│   └── logger_config.py   # Configuração de logs
│
├── data/
│   ├── bronze/            # Dados brutos (JSON)
│   └── silver/            # Dados tratados (Parquet)
│
├── sql/
│   ├── ddl.sql            # Criação das tabelas
│   └── gold_views.sql     # Views analíticas (Gold)
│
├── logs/
│   └── .gitkeep
│
├── .env.example
├── requirements.txt
└── README.md

---

## Camadas do Pipeline

### Bronze – Extração
- Consumo da API oficial do Portal da Transparência  
- Autenticação via API Key  
- Paginação controlada  

Os dados são armazenados em **JSON**, sem qualquer transformação, garantindo rastreabilidade.

---

### Silver – Transformação
- Normalização de estruturas JSON  
- Padronização de nomes de colunas  
- Tratamento de valores inválidos e nulos  
- Criação de chave de negócio (`doc_key`)  
- Conversão para **Parquet** (formato colunar e otimizado)  

---

### Gold – Consumo Analítico
- Criação de **views SQL** no MySQL  
- Dados prontos para análise e visualização em BI  
- Sem reprocessamento dos dados base  

---

## Banco de Dados

- MySQL  

Modelagem dimensional aplicada:
- `dim_tipo_transferencia`
- `fato_documento_despesa`

Características:
- Carga com **UPSERT** (`ON DUPLICATE KEY UPDATE`)
- Pipeline **idempotente** (pode ser executado múltiplas vezes sem duplicar dados)

---

## Logs e Monitoramento

- Logs em arquivo (`logs/etl.log`) e console  
- Registro de início, fim e duração do pipeline  
- Registro de erros com stacktrace  
- Rotação automática de arquivos de log  

---

## Variáveis de Ambiente

As configurações sensíveis são gerenciadas via variáveis de ambiente.

Crie um arquivo `.env` na raiz do projeto utilizando `.env.example` como base.

Exemplo:

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

---

## Como Executar o Projeto

### 1. Criar ambiente virtual
python -m venv .venv

### 2. Ativar o ambiente

Windows:
..venv\Scripts\Activate.ps1

### 3. Instalar dependências
pip install -r requirements.txt

### 4. Executar o pipeline
python src/main.py


O pipeline executa automaticamente:
- Extração (Bronze)  
- Transformação (Silver)  
- Carga no MySQL  
- Criação da camada Gold (Views)  

As views analíticas também podem ser criadas manualmente executando:
sql/gold_views.sql


---

## Exemplos de Análises

- Valor total por tipo de transferência  
- Quantidade de documentos por mês  
- Auditoria e rastreabilidade por arquivo de origem  

---

## Conceitos e Tecnologias Aplicadas

- ETL  
- Arquitetura Medalhão  
- Modelagem Dimensional  
- Idempotência  
- Rastreabilidade de dados  
- Dados Públicos  
- Parquet  
- Logging  
- Python, Pandas e SQL  

---

## Autor

Projeto desenvolvido por **Geraldo Mendes**,  
estudante de **Sistemas de Informação (UFPB)**,  
com foco em **aprendizado prático e preparação para estágio em Engenharia de Dados**.



