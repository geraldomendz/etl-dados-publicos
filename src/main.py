
from logger_config import setup_logger

from datetime import datetime
from a_extract import extract_tipos_transferencia, extract_documentos_despesa
from b_transform import transform_tipos_transferencia, transform_documento_despesa
from c_load import load_dim_tipo_transferencia, load_fato_documentos_despesa

logger = setup_logger()

def run_pipeline():
    start = datetime.now()
    logger.info("Iniciando pipeline ETL - %s", start.strftime("%Y-%m-%d %H:%M:%S"))


    try:
        # Extract bronze
        logger.info("EXTRACT (Bronze) - iniciando")
        extract_tipos_transferencia()
        extract_documentos_despesa()
        logger.info("EXTRACT (Bronze) - finalizado")

        # Transform silver
        logger.info("TRANSFORM (Silver) - iniciando")
        transform_tipos_transferencia()
        transform_documento_despesa()
        logger.info("TRANSFORM (Silver) - finalizado")

        # Load (Mysql)
        logger.info("LOAD (MySQL) - iniciando")
        load_dim_tipo_transferencia()
        load_fato_documentos_despesa()
        logger.info("LOAD (MySQL) - finalizado")

        end = datetime.now()
        logger.info("Pipeline concluído em %.1fs", (end - start).total_seconds())

    except Exception:
          # Loga erro + stacktrace
        logger.exception("Falha no pipeline")
        raise


if __name__ == "__main__":
    run_pipeline()