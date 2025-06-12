import logging
import sys
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime

def setup_logger():
    """Настройка системы логирования"""
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # Создаем директорию для логов если не существует
    os.makedirs("logs", exist_ok=True)

    # Настройка корневого логгера
    logging.basicConfig(
        level=getattr(logging, log_level),
        format=log_format,
        handlers=[
            # Вывод в консоль
            logging.StreamHandler(sys.stdout),
            # Вывод в файл с ротацией
            RotatingFileHandler(
                "logs/collector.log",
                maxBytes=10*1024*1024,  # 10MB
                backupCount=5
            )
        ]
    )

    logger = logging.getLogger(__name__)
    logger.info(f"Logger initialized with level: {log_level}")

class CollectorLogger:
    """Специализированный логгер для коллекторов"""

    def __init__(self, collector_name: str):
        self.logger = logging.getLogger(f"collector.{collector_name}")
        self.collector_name = collector_name

    def info(self, message: str, **kwargs):
        self.logger.info(f"[{self.collector_name}] {message}", extra=kwargs)

    def error(self, message: str, error: Exception = None, **kwargs):
        if error:
            self.logger.error(f"[{self.collector_name}] {message}: {error}", extra=kwargs)
        else:
            self.logger.error(f"[{self.collector_name}] {message}", extra=kwargs)

    def warning(self, message: str, **kwargs):
        self.logger.warning(f"[{self.collector_name}] {message}", extra=kwargs)

    def debug(self, message: str, **kwargs):
        self.logger.debug(f"[{self.collector_name}] {message}", extra=kwargs)
