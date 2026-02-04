
#imports

import os # Usado p/ acessar variáveis de ambiente
import json
import requests # Usado p/ fazer requisições
from datetime import datetime
from pathlib import Path # Representar caminhos de arquivos/pastas como objetos
from dotenv import load_dotenv # Carrega variáveis do arquivo .env

# Configurações 

load_dotenv(dotenv_path=Path(".env"))
API_BASE = "https://api.portaldatransparencia.gov.br/api-de-dados"
API_KEY = os.getenv("PT_API_KEY","") 
bronze_path = Path("data/bronze")


#Extrai um recurso de despesar via API e salva JSON bruto na camada bronze 

def extract_tipos_transferencia():

    if not API_KEY: # Valida se a chave existe
        raise RuntimeError("PT_API_KEY não encontrada no .env")
    
    bronze_path.mkdir(parents=True, exist_ok=True) # Garante que a pasta existe

    data_ref = datetime.now().strftime("%Y_%m_%d")

    nome_arquivo = f"despesas_tipos_transferencia_{data_ref}.json"

    caminho_arquivo = bronze_path / nome_arquivo

    url = f"{API_BASE}/despesas/tipo-transferencia" # EndPoint que retorna tipos de transferência

    headers = {"chave-api-dados": API_KEY, "Accept": "application/json"}

    print(f"Chamando API: {url}")

    resp = requests.get(url, headers=headers, timeout=60) # Faz a requisição HTTP

    if resp.status_code != 200: # Validação de resposta (salva se a API dizer OK)
        raise RuntimeError(f"Erro na API: status={resp.status_code} body={resp.text[:300]}")

    with open(caminho_arquivo, "w", encoding="utf-8") as f:
        json.dump(resp.json(), f, ensure_ascii=False, indent=2)

    print(f"OK! Arquivo salvo em: {caminho_arquivo}")


if __name__ == "__main__":
    extract_tipos_transferencia()
