
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
    cursor = conn.cursor()

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


if __name__== "__main__":
    load_dim_tipo_transferencia()




