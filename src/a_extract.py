
import logging
import os 
import json
import requests 
from datetime import datetime
from pathlib import Path 
from dotenv import load_dotenv 

# Configurações 
logger = logging.getLogger("etl")
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

API_BASE = "https://api.portaldatransparencia.gov.br/api-de-dados"
API_KEY = os.getenv("PT_API_KEY","") 
BRONZE_DIR = BASE_DIR / "data" / "bronze"

DOCS_UNIDADE_GESTORA = os.getenv("DOCS_UNIDADE_GESTORA")
DOCS_GESTAO = os.getenv("DOCS_GESTAO")
DOCS_DATA_EMISSAO = os.getenv("DOCS_DATA_EMISSAO")  
DOCS_FASE = os.getenv("DOCS_FASE")
DOCS_MAX_PAGINAS = int(os.getenv("DOCS_MAX_PAGINAS","1"))
DOCS_PAGE_SIZE = int(os.getenv("DOCS_PAGE_SIZE","50"))



# Extrai um recurso de despesar via API e salva JSON bruto na camada bronze 

def extract_tipos_transferencia():

    if not API_KEY: # Valida se a chave existe
        logger.error("PT_API_KEY não encontrada no .env")
        raise RuntimeError("PT_API_KEY não encontrada")
    
    BRONZE_DIR.mkdir(parents=True, exist_ok=True) # Garante que a pasta existe

    data_ref = datetime.now().strftime("%Y_%m_%d")

    nome_arquivo = f"despesas_tipos_transferencia_{data_ref}.json"

    caminho_arquivo = BRONZE_DIR / nome_arquivo

    url = f"{API_BASE}/despesas/tipo-transferencia" # EndPoint que retorna tipos de transferência

    headers = {"chave-api-dados": API_KEY, "Accept": "application/json"}

    logger.info(f"Chamando API: {url}")

    resp = requests.get(url, headers=headers, timeout=60) # Faz a requisição HTTP

    if resp.status_code != 200: # Validação de resposta (salva se a API dizer OK)
        logger.error(f"Erro na API: status={resp.status_code} body={resp.text[:300]}")
        raise RuntimeError(f"Erro na API: status={resp.status_code} body={resp.text[:300]}")

    with open(caminho_arquivo, "w", encoding="utf-8") as f:
        json.dump(resp.json(), f, ensure_ascii=False, indent=2)

    logger.info(f"OK! Arquivo salvo em: {caminho_arquivo}")



# Extrai documentos de despesas (paginado) e salvo em JSON (bronze)

def extract_documentos_despesa():

    if not API_KEY:
        logger.error("PT_API_KEY não encontrada no .env")
        raise RuntimeError("PT_API_KEY não encontrada no .env")
    
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    endpoint = f"{API_BASE}/despesas/documentos"
    headers = {"chave-api-dados": API_KEY, "Accept": "application/json"}

    data_ref = datetime.now().strftime("%Y_%m_%d")
    out_file = BRONZE_DIR / f"despesas_documentos_{data_ref}.json"

    all_rows = []

    if not DOCS_UNIDADE_GESTORA:
        logger.error("DOCS_UNIDADE_GESTORA não encontrada no .env")
        raise RuntimeError("DOCS_UNIDADE_GESTORA não encontrada no .env")
    if not DOCS_GESTAO:
        logger.error("DOCS_GESTAO não encontrada no .env (ex: 00001)")
        raise RuntimeError("DOCS_GESTAO não encontrada no .env")
    if not DOCS_DATA_EMISSAO:
        logger.error("DOCS_DATA_EMISSAO não encontrada no .env (formato DD/MM/AAAA)")
        raise RuntimeError("DOCS_DATA_EMISSAO não encontrada no .env (formato DD/MM/AAAA)")
    if not DOCS_FASE:
        logger.error("DOCS_FASE não encontrada no .env")
        raise RuntimeError("DOCS_FASE não encontrada no .env")

    for pagina in range(1, DOCS_MAX_PAGINAS + 1):
        params = {
        "unidadeGestora": DOCS_UNIDADE_GESTORA,
        "gestao": DOCS_GESTAO,
        "dataEmissao": DOCS_DATA_EMISSAO,
        "fase": int(DOCS_FASE),
        "pagina": pagina,
        "tamanhoPagina": DOCS_PAGE_SIZE,
    }

        logger.info(f"Chamando API: {endpoint} | pagina = {pagina}")
        logger.info("PARAMS: %s", params)

        resp = requests.get(endpoint, headers=headers, params=params, timeout = 60)

        if resp.status_code != 200:
            logger.error(f"Erro na API (documentos): status={resp.status_code} body={resp.text[:300]}")
            raise RuntimeError(f"Erro na API (documentos): status={resp.status_code} body={resp.text[:300]}")
        
        payload = resp.json()
        
        if not payload:
            logger.info("Sem mais registros (payload vazio). Parando paginação.")
            break
        
        all_rows.extend(payload)

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(all_rows, f, ensure_ascii=False, indent=2)

    logger.info(f"OK! {len(all_rows)} registros salvos em: {out_file}")


if __name__ == "__main__":
    extract_tipos_transferencia()
    extract_documentos_despesa()
    

