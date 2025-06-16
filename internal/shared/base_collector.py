from abc import ABC, abstractmethod
import asyncio
from typing import Dict, Any, List
from .logger import CollectorLogger
from cmd.db import db_manager

class BaseCollector(ABC):
    def __init__(self, name: str):
        self.name = name
        self.logger = CollectorLogger(name)
        self.running = False
        self.db_pool = None
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'total_trades_saved': 0
        }

    async def init(self):
        self.logger.info("Initializing collector")
        self.db_pool = db_manager.pool
        await self._init_collector()
        self.logger.info("Collector initialized successfully")

    @abstractmethod
    async def _init_collector(self):
        pass

    async def start(self):
        from cmd.config import config
        self.running = True
        self.logger.info("Starting collector")

        while self.running:
            try:
                await self._collect_data()
                await asyncio.sleep(config.collectors.collection_interval)
            except Exception as error:
                self.logger.error("Error in collection cycle", error)
                self.stats['failed_requests'] += 1
                await asyncio.sleep(config.collectors.collection_interval)

    @abstractmethod
    async def _collect_data(self):
        pass

    async def stop(self):
        self.running = False
        self.logger.info("Collector stopped")
        self._log_stats()

    def _log_stats(self):
        self.logger.info(f"Final stats: {self.stats}")

    async def save_trades(self, trades: List[Dict[str, Any]], table_name: str):
        if not trades:
            return

        try:
            async with self.db_pool.acquire() as conn:
                saved_count = 0

                for trade in trades:
                    existing = await conn.fetchval(
                        f"SELECT 1 FROM {table_name} WHERE trade_id = $1 LIMIT 1",
                        trade['trade_id']
                    )

                    if existing:
                        continue

                    await self._insert_trade_async(conn, trade, table_name)
                    saved_count += 1

                self.stats['total_trades_saved'] += saved_count
                if saved_count > 0:
                    self.logger.info(f"Saved {saved_count} new trades to {table_name}")

        except Exception as error:
            self.logger.error("Error saving trades", error)
            raise error

    async def _insert_trade_async(self, conn, trade: Dict[str, Any], table_name: str):
        pass

    @abstractmethod
    def _insert_trade(self, cursor, trade: Dict[str, Any], table_name: str):
        pass
