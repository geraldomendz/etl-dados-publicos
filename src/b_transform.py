
import logging
import json
from pathlib import Path
from datetime import datetime
import pandas as pd

# Configurações
logger = logging.getLogger("etl")
BASE_DIR = Path(__file__).resolve().parent.parent
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"

def get_latesd_file(pattern: str) -> Path:
    # Retorna o arquivo mais recente por data de modificação
    files = sorted(BRONZE_DIR.glob(pattern), key = lambda p: p.stat().st_mtime, reverse = True)
    if not files:
        logger.error(f"Nenhum arquivo encontrado em bronze com padrão: {pattern}")
        raise FileNotFoundError(f"Nenhum arquivo encontrado em bronze com padrão: {pattern}")
    return files[0]


def transform_tipos_transferencia():
    # Lê o JSON bruto (bronze) de tipos de transferência, faz padronizações simples e salva na camada silver
    SILVER_DIR.mkdir(parents = True, exist_ok = True)

    bronze_file = get_latesd_file("despesas_tipos_transferencia_*.json")
    logger.info(f"Lendo bronze: {bronze_file.name}")

    # Ler JSON (lista de objetos)
    with open(bronze_file, "r", encoding = "utf-8") as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    # Padronizar nomes de colunas
    df.columns = [c.strip().lower() for c in df.columns]

    # Selecionar/garantir colunas esperadas
    expected = {"id","descricao"}
    missing = expected - set(df.columns)

    if missing:
        logger.error(f"Colunas esperadas não encontradas: {missing}. Colunas atuais: {list(df.columns)}")
        raise ValueError(f"Colunas esperadas não encontradas: {missing}. Colunas atuais: {list(df.columns)}")
    
    df = df[["id","descricao"]].copy()

    # Limpeza básica
    df["descricao"] = df["descricao"].astype(str).str.strip()
    df = df.dropna(subset = ["id","descricao"])
    df = df.drop_duplicates(subset = ["id"])

    # Tipos 
    df["id"] = pd.to_numeric(df["id"], errors = "raise").astype("int64")

    # Salvando na silver em parquet
    data_ref = datetime.now().strftime("%Y_%m_%d")
    out_file = SILVER_DIR / f"dim_tipo_transferencia_{data_ref}.parquet"
    df.to_parquet(out_file, index = False)

    logger.info(f"Silver salvo em: {out_file} ({len(df)} linhas)")
    return out_file


def transform_documento_despesa():
    # Lê JSON de documentos (bronze) e salva em parquet (silver)
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    files = sorted(
        BRONZE_DIR.glob("despesas_documentos_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )
    if not files:
        logger.error("Nenhum arquivo despesas_documentos_*.json encontrado na bronze.")
        raise FileNotFoundError("Nenhum arquivo despesas_documentos_*.json encontrado na bronze.")

    in_file = files[0]
    logger.info(f"Lendo bronze: {in_file.name}")

    with open(in_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.json_normalize(data)

    # Colunas que gostaría de manter (se existirem)
    wanted = [
        "codigo", "id", "documento", "numeroDocumento",
        "ano", "mes",
        "valor", "valorDocumento",
        "orgao", "orgaoSuperior", "unidadeGestora",
        "data", "dataEmissao",
        "tipoTransferencia.id", "tipoTransferencia.descricao"
    ]

    cols = [c for c in wanted if c in df.columns]
    df = df[cols] if cols else df

    # Padroniza nomes (troca . por _)
    df.columns = [c.replace(".", "_") for c in df.columns]

    # Cria uma chave do documento (para usar como PK no MySQL depois)
    if "codigo" in df.columns:
        df["doc_key"] = df["codigo"].astype(str)
    elif "id" in df.columns:
        df["doc_key"] = df["id"].astype(str)
    else:
        df["doc_key"] = df.index.astype(str)  # fallback

    out_ref = datetime.now().strftime("%Y_%m_%d")
    out_file = SILVER_DIR / f"fato_documentos_despesa_{out_ref}.parquet"

    df.to_parquet(out_file, index=False)
    logger.info(f"Silver salvo em: {out_file} ({len(df)} linhas)")

    return out_file


if __name__ == "__main__":
    transform_tipos_transferencia()
    transform_documento_despesa()

    







       