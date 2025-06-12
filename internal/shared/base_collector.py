from abc import ABC, abstractmethod
import asyncio
from typing import Dict, Any, List
from .logger import CollectorLogger
from cmd.db import db_manager

class BaseCollector(ABC):
    """Базовый класс для всех коллекторов"""

    def __init__(self, name: str):
        self.name = name
        self.logger = CollectorLogger(name)
        self.running = False
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'total_trades_saved': 0
        }

    async def init(self):
        """Инициализация коллектора"""
        self.logger.info("Initializing collector")
        await self._init_collector()
        self.logger.info("Collector initialized successfully")

    @abstractmethod
    async def _init_collector(self):
        """Специфичная инициализация коллектора"""
        pass

    async def start(self):
        """Запуск коллектора"""
        self.running = True
        self.logger.info("Starting collector")

        while self.running:
            try:
                await self._collect_data()
                await asyncio.sleep(60)  # Интервал сбора данных
            except Exception as error:
                self.logger.error("Error in collection cycle", error)
                self.stats['failed_requests'] += 1
                await asyncio.sleep(60)

    @abstractmethod
    async def _collect_data(self):
        """Основная логика сбора данных"""
        pass

    async def stop(self):
        """Остановка коллектора"""
        self.running = False
        self.logger.info("Collector stopped")
        self._log_stats()

    def _log_stats(self):
        """Логирование статистики"""
        self.logger.info(f"Final stats: {self.stats}")

    async def save_trades(self, trades: List[Dict[str, Any]], table_name: str):
        """Сохранение торгов в базу данных"""
        if not trades:
            self.logger.info("No trades to save")
            return

        try:
            with db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    saved_count = 0

                    for trade in trades:
                        cursor.execute(
                            f"SELECT 1 FROM {table_name} WHERE trade_id = %s LIMIT 1",
                            (trade['trade_id'],)
                        )

                        if cursor.fetchone():
                            continue

                        self._insert_trade(cursor, trade, table_name)
                        saved_count += 1

                    conn.commit()
                    self.stats['total_trades_saved'] += saved_count
                    self.logger.info(f"Saved {saved_count} new trades to {table_name}")

        except Exception as error:
            self.logger.error("Error saving trades", error)
            raise error

    @abstractmethod
    def _insert_trade(self, cursor, trade: Dict[str, Any], table_name: str):
        """Вставка торга в базу данных (синхронный метод)"""
        pass
