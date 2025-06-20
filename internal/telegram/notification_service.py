import asyncio
import logging
from typing import Dict, Any
from datetime import datetime
from .bot_manager import telegram_manager

logger = logging.getLogger(__name__)

class NotificationService:
    def __init__(self):
        self.enabled = True

    async def notify_collector_started(self, collector_name: str):
        await telegram_manager.send_notification(
            f"🚀 *{collector_name}* коллектор запущен",
            level="success",
            collector_name=collector_name
        )

    async def notify_collector_stopped(self, collector_name: str):
        await telegram_manager.send_notification(
            f"🛑 *{collector_name}* коллектор остановлен",
            level="warning",
            collector_name=collector_name
        )

    async def notify_connection_error(self, collector_name: str, error: str):
        await telegram_manager.send_notification(
            f"❌ *{collector_name}* ошибка подключения\n"
            f"```\n{str(error)[:100]}...\n```",
            level="error",
            collector_name=collector_name
        )

    async def update_collector_status(self, collector_name: str, stats: Dict[str, Any]):
        status = {
            **stats,
            'last_activity': datetime.utcnow(),
            'connected': True
        }

        await telegram_manager.update_collector_status(collector_name, status)

notification_service = NotificationService()
