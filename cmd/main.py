import asyncio
import signal
import sys
import logging
from typing import List
from .config import config
from .db import db_manager
from internal.shared.logger import setup_logger
from internal.collectors.deribit_collector import DeribitCollector
from internal.collectors.okx_collector import OKXCollector
from internal.collectors.bybit_collector import BybitCollector
from internal.collectors.binance_collector import BinanceCollector
from internal.telegram.bot_manager import telegram_manager
from internal.collectors.ohlc_collector import OHLCCollector

logger = logging.getLogger(__name__)

class CollectorOrchestrator:
    def __init__(self):
        self.collectors = []
        self.running = False
        self.telegram_task = None

    async def init(self):
        try:
            errors = config.validate()
            if errors:
                raise ValueError(f"Configuration validation failed:\n{chr(10).join(errors)}")

            logger.info("Configuration validated successfully")

            await db_manager.init()

            await telegram_manager.init()

            await self._init_collectors()

            logger.info("Collector orchestrator initialized successfully")

        except Exception as error:
            logger.error(f"Failed to initialize orchestrator: {error}")
            raise error

    async def _init_collectors(self):
        if config.collectors.deribit_enabled:
            deribit_collector = DeribitCollector()
            await deribit_collector.init()
            self.collectors.append(deribit_collector)
            logger.info("Deribit collector initialized")

        if config.collectors.okx_enabled:
            okx_collector = OKXCollector()
            await okx_collector.init()
            self.collectors.append(okx_collector)
            logger.info("OKX collector initialized")

        if config.collectors.bybit_enabled:
            bybit_collector = BybitCollector()
            await bybit_collector.init()
            self.collectors.append(bybit_collector)
            logger.info("Bybit collector initialized")

        if config.collectors.binance_enabled:
            binance_collector = BinanceCollector()
            await binance_collector.init()
            self.collectors.append(binance_collector)
            logger.info("Binance collector initialized")

        if config.collectors.ohlc_enabled:
            ohlc_collector = OHLCCollector()
            await ohlc_collector.init()
            self.collectors.append(ohlc_collector)
            logger.info("OHLC collector initialized")

        logger.info(f"Initialized {len(self.collectors)} collectors")

    async def start(self):
        self.running = True
        logger.info("Starting all collectors...")

        tasks = []

        for collector in self.collectors:
            task = asyncio.create_task(collector.start())
            tasks.append(task)

        if telegram_manager.bot:
            self.telegram_task = asyncio.create_task(telegram_manager.start())
            tasks.append(self.telegram_task)

        try:
            await asyncio.gather(*tasks)
        except Exception as error:
            logger.error(f"Error in collector tasks: {error}")
            await self.stop()

    async def stop(self):
        self.running = False
        logger.info("Stopping all collectors...")

        for collector in self.collectors:
            await collector.stop()

        if self.telegram_task:
            self.telegram_task.cancel()
            try:
                await self.telegram_task
            except asyncio.CancelledError:
                pass

        await telegram_manager.stop()
        await db_manager.close()
        logger.info("All collectors stopped")

async def main():
    setup_logger()

    orchestrator = CollectorOrchestrator()

    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        asyncio.create_task(orchestrator.stop())

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        await orchestrator.init()
        await orchestrator.start()
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, shutting down...")
        await orchestrator.stop()
    except Exception as error:
        logger.error(f"Application error: {error}")
        await orchestrator.stop()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
