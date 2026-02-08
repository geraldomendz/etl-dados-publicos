
import os
from pathlib import Path
import pandas as pd
import mysql.connector
from dotenv import load_dotenv

# Caminhos e ambiente
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path = BASE_DIR / ".env")
SILVER_DIR = BASE_DIR / "data" / "silver"

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")


def get_latesd_file(pattern: str) -> Path:
    # Retorna o arquivo mais recente por data de modificação
    files = sorted(SILVER_DIR.glob(pattern), key = lambda p: p.stat().st_mtime, reverse = True)
    if not files:
        raise FileNotFoundError(f"Nenhum arquivo parquet encontrado em silver com padrão: {pattern}")
    return files[0]


def load_dim_tipo_transferencia():
    # Ler parquet mais recente
    parquet_file = get_latesd_file("dim_tipo_transferencia_*.parquet")
    print(f"Lendo silver: {parquet_file.name}")

    df = pd.read_parquet(parquet_file)

    # Conectar no Mysql
    conn = mysql.connector.connect(
        host = DB_HOST,
        port = DB_PORT,
        user = DB_USER,
        password = DB_PASSWORD,
        database = DB_NAME
    )
    cursor = conn.cursor() # Executador de comandos sql

    # Criar tabela se não existir
    cursor.execute ("""
                   CREATE TABLE IF NOT EXISTS DIM_TIPO_TRANSFERENCIA (
                   id INT PRIMARY KEY,
                   descricao VARCHAR(255))
                   """ )

    # Inserir Dados
    sql = """
          INSERT INTO dim_tipo_transferencia (id, descricao)
          VALUES (%s, %s)
          ON DUPLICATE KEY UPDATE
             descricao = VALUES(descricao)
         """
    
    data = list(df.itertuples(index = False, name = None))
    cursor.executemany(sql, data)

    conn.commit()

    print(f"Tudo certo! {cursor.rowcount} registros carregados na tabela dim_tipo_transferencia")

    cursor.close()
    conn.close()


def load_fato_documentos_despesa():
    parquet_file = get_latesd_file("fato_documentos_despesa_*.parquet")
    print(f"Lendo silver: {parquet_file.name}")

    df = pd.read_parquet(parquet_file)

    conn = mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    cursor = conn.cursor()

    # nome consistente (singular)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fato_documento_despesa (
            doc_key VARCHAR(100) PRIMARY KEY,
            ano INT NULL,
            mes INT NULL,
            valor DECIMAL(18,2) NULL,
            tipo_transferencia_id INT NULL,
            raw_json_ref VARCHAR(255) NULL
        )
    """)

    def pick(col, default=None):
        return df[col] if col in df.columns else default

    doc_key = pick("doc_key")
    ano = pick("ano")
    mes = pick("mes")

    # valor pode vir com nome diferente
    if "valor" in df.columns:
        valor = df["valor"]
    if valor is not None:
        valor = pd.to_numeric(valor, errors="coerce")
    if "valorDocumento" in df.columns:
        valor = df["valorDocumento"]
    
    else:
        valor = None

    # tipo de transferência
    tipo_id = pick("tipoTransferencia_id")

    raw_ref = parquet_file.name

    rows = []
    for i in range(len(df)):
        rows.append((
            str(doc_key.iloc[i]) if doc_key is not None and pd.notna(doc_key.iloc[i]) else str(i),
            int(ano.iloc[i]) if ano is not None and pd.notna(ano.iloc[i]) else None,
            int(mes.iloc[i]) if mes is not None and pd.notna(mes.iloc[i]) else None,
            float(valor.iloc[i]) if valor is not None and pd.notna(valor.iloc[i]) else None,int(tipo_id.iloc[i]) if tipo_id is not None and pd.notna(tipo_id.iloc[i]) else None,
            raw_ref
        ))

    sql = """
        INSERT INTO fato_documento_despesa
            (doc_key, ano, mes, valor, tipo_transferencia_id, raw_json_ref)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            ano = VALUES(ano),
            mes = VALUES(mes),
            valor = VALUES(valor),
            tipo_transferencia_id = VALUES(tipo_transferencia_id),
            raw_json_ref = VALUES(raw_json_ref)
    """
    cursor.executemany(sql, rows)

    conn.commit()
    print(f"Tudo certo! {cursor.rowcount} registros inseridos/atualizados em fato_documento_despesa")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    load_dim_tipo_transferencia()
    load_fato_documentos_despesa()




















if __name__== "__main__":
    load_dim_tipo_transferencia()




