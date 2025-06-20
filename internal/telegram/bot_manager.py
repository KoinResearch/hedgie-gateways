import asyncio
import logging
from typing import Dict, Optional
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from cmd.config import config
from cmd.db import db_manager

logger = logging.getLogger(__name__)

class TelegramBotManager:
    def __init__(self):
        self.bot: Optional[Bot] = None
        self.dp: Optional[Dispatcher] = None
        self.running = False
        self.admin_chat_id = config.telegram.admin_chat_id
        self.alert_cache: Dict[str, datetime] = {}
        self.collector_statuses: Dict[str, Dict] = {}

        self.main_keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [
                    KeyboardButton(text="📊"),
                    KeyboardButton(text="📄"),
                    KeyboardButton(text="📈")
                ]
            ],
            resize_keyboard=True,
            persistent=True
        )

    async def init(self):
        if not config.telegram.notifications_enabled:
            logger.info("Telegram notifications disabled")
            return

        if not config.telegram.bot_token:
            logger.error("Telegram bot token not provided")
            return

        try:
            self.bot = Bot(token=config.telegram.bot_token)
            storage = MemoryStorage()
            self.dp = Dispatcher(storage=storage)

            self._register_handlers()

            logger.info("Telegram bot initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Telegram bot: {e}")
            self.bot = None

    def _register_handlers(self):

        @self.dp.message(Command("start"))
        async def start_handler(message: types.Message):
            await message.answer(
                "🤖 *Hedgie Gateways Monitor Bot*\n\n"
                "Используйте кнопки ниже:\n\n"
                "📊 - Статус всех коллекторов\n"
                "📄 - Логи коллекторов\n"
                "📈 - Статистика по торгам",
                parse_mode="Markdown",
                reply_markup=self.main_keyboard
            )

        @self.dp.message(lambda message: message.text == "📊")
        async def status_button_handler(message: types.Message):
            status_text = await self._get_status_message()
            await message.answer(status_text, parse_mode="Markdown")

        @self.dp.message(lambda message: message.text == "📄")
        async def logs_button_handler(message: types.Message):
            await message.answer(
                "📄 *Выберите коллектор для получения логов:*\n\n"
                "Напишите один из:\n"
                "• `DER` - Deribit\n"
                "• `OKX` - OKX\n"
                "• `BYBIT` - Bybit\n"
                "• `BINANCE` - Binance",
                parse_mode="Markdown"
            )

        @self.dp.message(lambda message: message.text == "📈")
        async def stats_button_handler(message: types.Message):
            stats_text = await self._get_stats_message()
            await message.answer(stats_text, parse_mode="Markdown")

        @self.dp.message(lambda message: message.text and message.text.upper() in ["DER", "OKX", "BYBIT", "BINANCE"])
        async def logs_collector_handler(message: types.Message):
            collector_name = message.text.upper()
            collector_map = {
                "DER": "deribit",
                "OKX": "okx",
                "BYBIT": "bybit",
                "BINANCE": "binance"
            }

            full_name = collector_map.get(collector_name)
            if full_name:
                log_info = await self._get_logs_info(full_name)
                await message.answer(log_info, parse_mode="Markdown")

    async def start(self):
        if not self.bot:
            return

        self.running = True

        try:
            await self.send_notification(
                "🚀 *Hedgie Gateways запущен*\n"
                "Система мониторинга активна",
                level="info"
            )

            await self.dp.start_polling(self.bot)

        except Exception as e:
            logger.error(f"Error starting telegram bot: {e}")

    async def stop(self):
        self.running = False

        if self.bot:
            try:
                await self.send_notification(
                    "🛑 *Hedgie Gateways остановлен*\n"
                    "Система мониторинга отключена",
                    level="warning"
                )
                await self.bot.session.close()
            except Exception as e:
                logger.error(f"Error stopping telegram bot: {e}")

    async def send_notification(self, message: str, level: str = "info", collector_name: str = ""):
        if not self.bot or not self.admin_chat_id:
            return

        alert_key = f"{collector_name}_{level}_{hash(message)}"
        now = datetime.utcnow()

        if alert_key in self.alert_cache:
            last_sent = self.alert_cache[alert_key]
            if now - last_sent < timedelta(seconds=config.telegram.alert_cooldown):
                return

        self.alert_cache[alert_key] = now

        timestamp = now.strftime("%H:%M:%S")
        message = f"🕐 {timestamp}\n{message}"

        try:
            await self.bot.send_message(
                chat_id=self.admin_chat_id,
                text=message,
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Failed to send telegram notification: {e}")

    async def update_collector_status(self, collector_name: str, status: Dict):
        old_status = self.collector_statuses.get(collector_name, {})
        self.collector_statuses[collector_name] = status

        await self._check_status_changes(collector_name, old_status, status)

    async def _check_status_changes(self, collector_name: str, old_status: Dict, new_status: Dict):
        old_connected = old_status.get('connected', False)
        new_connected = new_status.get('connected', False)

        if old_connected and not new_connected:
            await self.send_notification(
                f"❌ *{collector_name}* потерял соединение\n"
                f"Попытка переподключения...",
                level="error",
                collector_name=collector_name
            )
        elif not old_connected and new_connected:
            await self.send_notification(
                f"✅ *{collector_name}* восстановил соединение\n"
                f"Сбор данных возобновлен",
                level="success",
                collector_name=collector_name
            )

    async def _get_status_message(self) -> str:
        if not self.collector_statuses:
            return "📊 *Статус коллекторов*\n\nДанные пока не поступали"

        message = "📊 *Статус коллекторов*\n\n"

        for collector_name, status in self.collector_statuses.items():
            connected = status.get('connected', False)
            total_requests = status.get('total_requests', 0)
            successful_requests = status.get('successful_requests', 0)
            failed_requests = status.get('failed_requests', 0)
            trades_saved = status.get('total_trades_saved', 0)
            last_activity = status.get('last_activity')

            status_icon = "🟢" if connected else "🔴"
            success_rate = (successful_requests / total_requests * 100) if total_requests > 0 else 0

            message += f"{status_icon} *{collector_name.upper()}*\n"
            message += f"   Запросов: {total_requests}\n"
            message += f"   Успешных: {successful_requests} ({success_rate:.1f}%)\n"
            message += f"   Ошибок: {failed_requests}\n"
            message += f"   Торгов сохранено: {trades_saved}\n"

            if last_activity:
                message += f"   Последняя активность: {last_activity.strftime('%H:%M:%S')}\n"

            message += "\n"

        return message

    async def _get_stats_message(self) -> str:
        try:
            async with db_manager.pool.acquire() as conn:
                tables = [
                    ('all_btc_trades', 'Deribit BTC'),
                    ('all_eth_trades', 'Deribit ETH'),
                    ('okx_btc_trades', 'OKX BTC'),
                    ('okx_eth_trades', 'OKX ETH'),
                    ('bybit_btc_trades', 'Bybit BTC'),
                    ('bybit_eth_trades', 'Bybit ETH'),
                    ('binance_btc_trades', 'Binance BTC'),
                    ('binance_eth_trades', 'Binance ETH')
                ]

                message = "📈 *Статистика по торгам*\n\n"
                total_trades = 0

                for table_name, display_name in tables:
                    try:
                        count_query = f"""
                        SELECT COUNT(*) as count
                        FROM information_schema.tables
                        WHERE table_name = '{table_name}'
                        """
                        table_exists = await conn.fetchval(count_query)

                        if table_exists > 0:
                            trades_query = f"SELECT COUNT(*) FROM {table_name}"
                            count = await conn.fetchval(trades_query)

                            if count > 0:
                                message += f"📊 *{display_name}*: {count:,} торгов\n"
                                total_trades += count

                    except Exception as e:
                        logger.error(f"Error getting stats for {table_name}: {e}")
                        continue

                if total_trades == 0:
                    message += "Торги пока не собраны"
                else:
                    message += f"\n📊 *Всего торгов*: {total_trades:,}"

                return message

        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return "❌ Ошибка получения статистики"

    async def _get_logs_info(self, collector_name: str) -> str:
        import os

        try:
            log_file = f"logs/{collector_name}.log"

            if not os.path.exists(log_file):
                return f"📄 Лог файл для {collector_name} не найден"

            file_size = os.path.getsize(log_file)
            size_mb = file_size / (1024 * 1024)

            with open(log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            last_lines = lines[-10:] if len(lines) >= 10 else lines

            message = f"📄 *Логи {collector_name.upper()}*\n\n"
            message += f"📁 Размер файла: {size_mb:.1f} MB\n"
            message += f"📝 Всего строк: {len(lines)}\n\n"
            message += f"*Последние записи:*\n"
            message += "```\n"

            for line in last_lines:
                clean_line = line.strip()
                if len(clean_line) > 100:
                    clean_line = clean_line[:100] + "..."
                message += clean_line + "\n"

            message += "```"

            return message

        except Exception as e:
            logger.error(f"Error reading logs for {collector_name}: {e}")
            return f"❌ Ошибка чтения логов для {collector_name}"

telegram_manager = TelegramBotManager()
