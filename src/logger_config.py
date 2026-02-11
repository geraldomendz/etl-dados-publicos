
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logger(name: str = "etl", log_file: str = "etl.log") -> logging.Logger:
    """
    Configura logger com saída no console + arquivo em logs/ (com rotação).
    """
    base_dir = Path(__file__).resolve().parent.parent
    logs_dir = base_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Evita duplicar handlers se chamar setup_logger mais de uma vez
    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)

    # Arquivo com rotação 
    fh = RotatingFileHandler(
        logs_dir / log_file,
        maxBytes=5_000_000,
        backupCount=3,
        encoding="utf-8",
    )
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)

    logger.addHandler(sh)
    logger.addHandler(fh)
    return logger
