
from datetime import datetime
from a_extract import extract_tipos_transferencia
from b_transform import transform_tipos_transferencia
from c_load import load_dim_tipo_transferencia

def run_pipeline():
    start = datetime.now()
    print(f"\n Iniciando pipeline ETL - {start.strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Extract bronze
    print("EXTRACT (Bronze)")
    extract_tipos_transferencia()
    print("Extração finalizada\n")

    # Transform silver
    print("TRANSFORM (Silver)")
    transform_tipos_transferencia()
    print("Transform finalizado\n")

    # Load (Mysql)
    print("LOAD (Mysql)")
    load_dim_tipo_transferencia()
    print("Load finalizado\n")

    end = datetime.now()
    print(f"Pipeline concluído em {(end-start).total_seconds():.1f}s\n")

if __name__ == "__main__":
    run_pipeline()