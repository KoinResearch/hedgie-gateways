from abc import ABC, abstractmethod
import asyncio
from typing import Dict, Any, List
from datetime import datetime, timedelta
from .logger import CollectorLogger
from cmd.db import db_manager
from internal.telegram.notification_service import notification_service

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
        self.last_data_time = None
        self.error_streak = 0
        self.status_update_interval = 60

    async def init(self):
        self.logger.info("Initializing collector")
        self.db_pool = db_manager.pool
        await self._init_collector()
        self.logger.info("Collector initialized successfully")

        await notification_service.notify_collector_started(self.name)

    @abstractmethod
    async def _init_collector(self):
        pass

    async def start(self):
        from cmd.config import config
        self.running = True
        self.logger.info("Starting collector")

        status_task = asyncio.create_task(self._status_updater())

        monitor_task = asyncio.create_task(self._data_monitor())

        try:
            while self.running:
                try:
                    await self._collect_data()
                    self.last_data_time = datetime.utcnow()
                    self.error_streak = 0
                    await asyncio.sleep(config.collectors.collection_interval)

                except Exception as error:
                    self.logger.error("Error in collection cycle", error)
                    self.stats['failed_requests'] += 1
                    self.error_streak += 1

                    await self._handle_error(error)

                    await asyncio.sleep(config.collectors.collection_interval)
        finally:
            status_task.cancel()
            monitor_task.cancel()

            try:
                await status_task
            except asyncio.CancelledError:
                pass

            try:
                await monitor_task
            except asyncio.CancelledError:
                pass

    async def _status_updater(self):
        while self.running:
            try:
                await notification_service.update_collector_status(self.name, self.stats.copy())
                await asyncio.sleep(self.status_update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error updating status: {e}")
                await asyncio.sleep(self.status_update_interval)

    async def _data_monitor(self):
        while self.running:
            try:
                await asyncio.sleep(300)

                if self.last_data_time:
                    time_since_data = datetime.utcnow() - self.last_data_time
                    if time_since_data > timedelta(minutes=10):
                        minutes = int(time_since_data.total_seconds() / 60)
                        await notification_service.notify_no_data(self.name, minutes)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in data monitor: {e}")

    async def _handle_error(self, error: Exception):
        if self.error_streak >= 5:
            await notification_service.notify_connection_error(self.name, str(error))

        if self.stats['total_requests'] > 20:
            error_rate = (self.stats['failed_requests'] / self.stats['total_requests']) * 100
            if error_rate > 50:
                await notification_service.notify_high_error_rate(self.name, error_rate)

    @abstractmethod
    async def _collect_data(self):
        pass

    async def stop(self):
        self.running = False
        self.logger.info("Collector stopped")
        self._log_stats()

        await notification_service.notify_collector_stopped(self.name)

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

                    await self._check_large_trade(trade)

                self.stats['total_trades_saved'] += saved_count
                if saved_count > 0:
                    self.logger.info(f"Saved {saved_count} new trades to {table_name}")

        except Exception as error:
            self.logger.error("Error saving trades", error)
            await notification_service.notify_database_error(self.name, str(error))
            raise error

    async def _check_large_trade(self, trade: Dict[str, Any]):
        amount = trade.get('amount', 0)

        if amount > 100000:
            await notification_service.notify_large_trade(self.name, trade)

    async def _insert_trade_async(self, conn, trade: Dict[str, Any], table_name: str):
        pass

    @abstractmethod
    def _insert_trade(self, cursor, trade: Dict[str, Any], table_name: str):
        pass
