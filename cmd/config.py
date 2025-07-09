import os
from dataclasses import dataclass
from typing import List
from dotenv import load_dotenv

load_dotenv()

@dataclass
class DatabaseConfig:
    host: str = os.getenv("PG_HOST", "localhost")
    port: int = int(os.getenv("PG_PORT", "5432"))
    user: str = os.getenv("PG_USER", "postgres")
    password: str = os.getenv("PG_PASSWORD", "")
    database: str = os.getenv("PG_DATABASE", "deribit_trades")

    def get_connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

@dataclass
class CollectorConfig:
    deribit_enabled: bool = os.getenv("DERIBIT_ENABLED", "true").lower() == "true"
    okx_enabled: bool = os.getenv("OKX_ENABLED", "true").lower() == "true"
    binance_enabled: bool = os.getenv("BINANCE_ENABLED", "false").lower() == "true"
    bybit_enabled: bool = os.getenv("BYBIT_ENABLED", "false").lower() == "true"
    cme_enabled: bool = os.getenv("CME_ENABLED", "false").lower() == "true"
    ohlc_enabled: bool = os.getenv("OHLC_ENABLED", "true").lower() == "true"

    collection_interval: int = int(os.getenv("COLLECTION_INTERVAL", "60"))

@dataclass
class TelegramConfig:
    bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    admin_chat_id: str = os.getenv("TELEGRAM_ADMIN_CHAT_ID", "")
    notifications_enabled: bool = os.getenv("TELEGRAM_NOTIFICATIONS_ENABLED", "true").lower() == "true"
    alert_cooldown: int = int(os.getenv("TELEGRAM_ALERT_COOLDOWN", "300"))

@dataclass
class Config:
    database: DatabaseConfig
    collectors: CollectorConfig
    telegram: TelegramConfig

    def __init__(self):
        self.database = DatabaseConfig()
        self.collectors = CollectorConfig()
        self.telegram = TelegramConfig()

    def validate(self) -> List[str]:
        errors = []

        if not self.database.user:
            errors.append("PG_USER is required")

        if not self.database.password:
            errors.append("PG_PASSWORD is required")

        if not self.database.database:
            errors.append("PG_DATABASE is required")

        if self.collectors.collection_interval <= 0:
            errors.append("COLLECTION_INTERVAL must be greater than 0")

        if self.telegram.notifications_enabled:
            if not self.telegram.bot_token:
                errors.append("TELEGRAM_BOT_TOKEN is required when notifications are enabled")

            if not self.telegram.admin_chat_id:
                errors.append("TELEGRAM_ADMIN_CHAT_ID is required when notifications are enabled")

        return errors

config = Config()
