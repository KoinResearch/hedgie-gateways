import asyncio
import websockets
import json
import aiohttp
from datetime import datetime
from typing import Dict, Any, List, Optional
from ..shared.base_collector import BaseCollector

class DeribitCollector(BaseCollector):
    """Коллектор публичных рыночных данных с биржи Deribit"""

    def __init__(self):
        super().__init__("deribit")
        # Используем тестовый сервер для разработки
        self.ws_url = 'wss://test.deribit.com/ws/api/v2'
        # Для продакшена: 'wss://www.deribit.com/ws/api/v2'

        self.currency_pairs = ['BTC', 'ETH']
        self.websocket = None
        self.session = None
        self.heartbeat_task = None
        self.msg_id = 1
        self.is_heartbeat_set = False

    async def _init_collector(self):
        """Инициализация Deribit коллектора"""
        timeout = aiohttp.ClientTimeout(total=30)
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={'User-Agent': 'Hedgie-Deribit-Market-Collector/1.0'}
        )

        self.logger.info("Deribit market data collector initialized successfully")

    async def _collect_data(self):
        """Основной цикл сбора публичных торгов"""
        max_retries = 5
        retry_count = 0

        while self.running and retry_count < max_retries:
            try:
                await self._connect_and_subscribe()
                retry_count = 0  # Сбрасываем счетчик при успешном подключении

            except websockets.exceptions.ConnectionClosed:
                retry_count += 1
                self.logger.warning(f"WebSocket connection closed, retry {retry_count}/{max_retries}")
                await asyncio.sleep(min(retry_count * 2, 30))

            except Exception as error:
                retry_count += 1
                self.logger.error(f"WebSocket error, retry {retry_count}/{max_retries}", error)
                await asyncio.sleep(min(retry_count * 2, 30))

        if retry_count >= max_retries:
            self.logger.error("Max retries reached, stopping collector")
            self.running = False

    async def _connect_and_subscribe(self):
        """Подключение к WebSocket и подписка на публичные торги"""
        self.logger.info("Connecting to Deribit WebSocket for market data...")

        async with websockets.connect(
            self.ws_url,
            ping_interval=30,
            ping_timeout=10,
            close_timeout=10,
            max_size=10**7  # 10MB буфер для больших сообщений
        ) as websocket:
            self.websocket = websocket
            self.logger.info("Connected to Deribit WebSocket")

            # Шаг 1: Настройка heartbeat согласно документации
            await self._setup_heartbeat(websocket)

            # Шаг 2: Подписка на публичные торги для каждой валюты
            for currency in self.currency_pairs:
                await self._subscribe_to_market_trades(websocket, currency)

            # Шаг 3: Запуск задачи мониторинга heartbeat
            self.heartbeat_task = asyncio.create_task(self._heartbeat_monitor(websocket))

            # Основной цикл получения сообщений
            try:
                while self.running:
                    message = await asyncio.wait_for(websocket.recv(), timeout=60)
                    await self._process_message(message)

            except asyncio.TimeoutError:
                self.logger.warning("WebSocket receive timeout")
                raise
            finally:
                if self.heartbeat_task:
                    self.heartbeat_task.cancel()

    async def _setup_heartbeat(self, websocket):
        """Настройка heartbeat согласно официальной документации"""
        heartbeat_msg = {
            "jsonrpc": "2.0",
            "id": self._get_next_id(),
            "method": "public/set_heartbeat",
            "params": {
                "interval": 30  # 30 секунд между heartbeat
            }
        }

        await websocket.send(json.dumps(heartbeat_msg))
        self.logger.info("Heartbeat setup request sent (30s interval)")

    async def _heartbeat_monitor(self, websocket):
        """Мониторинг heartbeat и ответы на test_request"""
        try:
            while self.running and not websocket.closed:
                await asyncio.sleep(5)  # Проверяем каждые 5 секунд

        except asyncio.CancelledError:
            self.logger.debug("Heartbeat monitor task cancelled")
        except Exception as e:
            self.logger.error(f"Heartbeat monitor error: {e}")

    async def _subscribe_to_market_trades(self, websocket, currency: str):
        """Подписка на публичные торги рынка"""
        # Корректный формат для публичных торгов согласно документации
        subscribe_msg = {
            "jsonrpc": "2.0",
            "id": self._get_next_id(),
            "method": "public/subscribe",
            "params": {
                "channels": [f"trades.{currency}.100ms"]
            }
        }

        await websocket.send(json.dumps(subscribe_msg))
        self.logger.info(f"Subscribed to market trades for {currency}")

    async def _process_message(self, message: str):
        """Обработка входящих сообщений"""
        try:
            data = json.loads(message)

            # Обрабатываем подписки (основные данные)
            if data.get("method") == "subscription":
                await self._handle_market_data(data)

            # Обрабатываем heartbeat
            elif data.get("method") == "heartbeat":
                await self._handle_heartbeat(data)

            # Обрабатываем ответы на запросы
            elif "result" in data:
                await self._handle_response(data)

            # Обрабатываем ошибки
            elif "error" in data:
                self.logger.error(f"Deribit API error: {data['error']}")

        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON message: {e}")
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")

    async def _handle_market_data(self, data: Dict[str, Any]):
        """Обработка данных рыночных торгов"""
        try:
            params = data.get("params", {})
            channel = params.get("channel", "")
            trades_data = params.get("data", [])

            if not channel.startswith("trades."):
                return

            # Извлекаем валюту из канала (trades.BTC.100ms -> BTC)
            parts = channel.split('.')
            if len(parts) < 2:
                self.logger.warning(f"Invalid channel format: {channel}")
                return

            currency = parts[1]
            self.logger.debug(f"Received {len(trades_data)} market trades for {currency}")

            if not trades_data:
                return

            # Обрабатываем каждый торг
            processed_trades = []
            options_count = 0

            for trade in trades_data:
                processed_trade = self._process_market_trade(trade, currency)
                if processed_trade:
                    processed_trades.append(processed_trade)

                    # Считаем опционы для статистики
                    if self._is_option_trade(processed_trade):
                        options_count += 1

            if processed_trades:
                # Фильтруем только опционы
                options_trades = self._filter_options(processed_trades)

                if options_trades:
                    # Сохраняем все опционы
                    table_name = f"all_{currency.lower()}_trades"
                    await self.save_trades(options_trades, table_name)

                    # Фильтруем и сохраняем блок-торги
                    block_trades = self._filter_block_trades(options_trades)
                    if block_trades:
                        block_table_name = f"{currency.lower()}_block_trades"
                        await self.save_trades(block_trades, block_table_name)
                        self.logger.info(f"Saved {len(block_trades)} block trades for {currency}")

                    self.stats['successful_requests'] += 1
                    self.logger.info(f"Processed {len(options_trades)} option trades for {currency} "
                                   f"(total trades: {len(processed_trades)})")
                else:
                    self.logger.debug(f"No option trades found in {len(processed_trades)} trades for {currency}")

        except Exception as e:
            self.logger.error(f"Error handling market data: {e}")
            self.stats['failed_requests'] += 1

    async def _handle_heartbeat(self, data: Dict[str, Any]):
        """Обработка heartbeat сообщений согласно документации"""
        try:
            params = data.get("params", {})
            heartbeat_type = params.get("type")

            if heartbeat_type == "test_request":
                # Отвечаем на test_request
                response_msg = {
                    "jsonrpc": "2.0",
                    "id": self._get_next_id(),
                    "method": "public/test",
                    "params": {}
                }
                await self.websocket.send(json.dumps(response_msg))
                self.logger.debug("Heartbeat test_request answered")

            elif heartbeat_type == "heartbeat":
                self.logger.debug(f"Heartbeat received: {params}")

        except Exception as e:
            self.logger.error(f"Error handling heartbeat: {e}")

    async def _handle_response(self, data: Dict[str, Any]):
        """Обработка ответов от сервера"""
        result = data.get("result")
        request_id = data.get("id")

        if result == "ok":
            self.logger.debug(f"Operation confirmed (id: {request_id})")
        elif isinstance(result, dict) and "interval" in result:
            self.logger.info(f"Heartbeat configured: {result}")
            self.is_heartbeat_set = True
        elif result == "pong":
            self.logger.debug("Test response received (pong)")
        else:
            self.logger.debug(f"Server response (id: {request_id}): {result}")

    def _process_market_trade(self, trade: Dict[str, Any], currency: str) -> Optional[Dict[str, Any]]:
        """Обработка и нормализация рыночного торга"""
        try:
            processed_trade = {
                'trade_id': trade.get('trade_id'),
                'block_trade_leg_count': trade.get('block_trade_leg_count'),
                'contracts': self._safe_float(trade.get('contracts')),
                'block_trade_id': trade.get('block_trade_id'),
                'combo_id': trade.get('combo_id'),
                'tick_direction': trade.get('tick_direction'),
                'mark_price': self._safe_float(trade.get('mark_price')),
                'amount': self._safe_float(trade.get('amount')),
                'trade_seq': trade.get('trade_seq'),
                'instrument_name': trade.get('instrument_name'),
                'index_price': self._safe_float(trade.get('index_price')),
                'direction': trade.get('direction'),
                'price': self._safe_float(trade.get('price')),
                'iv': self._safe_float(trade.get('iv')),
                'liquidation': trade.get('liquidation'),
                'combo_trade_id': trade.get('combo_trade_id'),
                'timestamp': trade.get('timestamp')
            }

            return processed_trade

        except Exception as e:
            self.logger.error(f"Error processing market trade: {e}, trade data: {trade}")
            return None

    def _is_option_trade(self, trade: Dict[str, Any]) -> bool:
        """Проверка, является ли торг опционом"""
        instrument_name = trade.get('instrument_name', '')
        if isinstance(instrument_name, str) and '-' in instrument_name:
            parts = instrument_name.split('-')
            # Формат опциона: BTC-25MAR23-420-C или BTC-25MAR23-420-P
            return len(parts) >= 4 and parts[-1] in {'C', 'P'}
        return False

    def _filter_options(self, trades: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Фильтрация только опционов"""
        filtered = []
        for trade in trades:
            if self._is_option_trade(trade):
                filtered.append(trade)

        self.logger.debug(f"Filtered {len(filtered)} options from {len(trades)} trades")
        return filtered

    def _filter_block_trades(self, trades: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Фильтрация блок-торгов"""
        filtered = []
        for trade in trades:
            if (trade.get('block_trade_id') is not None and
                trade.get('block_trade_leg_count') is not None):
                filtered.append(trade)

        self.logger.debug(f"Filtered {len(filtered)} block trades from {len(trades)} trades")
        return filtered

    def _safe_float(self, value) -> Optional[float]:
        """Безопасное преобразование в float"""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _get_next_id(self) -> int:
        """Получить следующий ID для запроса"""
        self.msg_id += 1
        return self.msg_id

    def _insert_trade(self, cursor, trade: Dict[str, Any], table_name: str):
        """Вставка рыночного торга в базу данных"""
        query = f"""
        INSERT INTO {table_name} (
            trade_id, block_trade_leg_count, contracts, block_trade_id, combo_id, tick_direction,
            mark_price, amount, trade_seq, instrument_name, index_price, direction, price, iv,
            liquidation, combo_trade_id, timestamp
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, to_timestamp(%s / 1000))
        ON CONFLICT (trade_id) DO NOTHING
        """

        cursor.execute(query, (
            trade.get('trade_id'),
            trade.get('block_trade_leg_count'),
            trade.get('contracts'),
            trade.get('block_trade_id'),
            trade.get('combo_id'),
            trade.get('tick_direction'),
            trade.get('mark_price'),
            trade.get('amount'),
            trade.get('trade_seq'),
            trade.get('instrument_name'),
            trade.get('index_price'),
            trade.get('direction'),
            trade.get('price'),
            trade.get('iv'),
            trade.get('liquidation'),
            trade.get('combo_trade_id'),
            trade.get('timestamp')
        ))

    async def stop(self):
        """Остановка коллектора"""
        self.logger.info("Stopping Deribit market collector...")

        if self.heartbeat_task:
            self.heartbeat_task.cancel()

        if self.websocket and not self.websocket.closed:
            # Отписываемся от каналов перед закрытием
            try:
                for currency in self.currency_pairs:
                    unsubscribe_msg = {
                        "jsonrpc": "2.0",
                        "id": self._get_next_id(),
                        "method": "public/unsubscribe",
                        "params": {
                            "channels": [f"trades.{currency}.100ms"]
                        }
                    }
                    await self.websocket.send(json.dumps(unsubscribe_msg))

                await asyncio.sleep(0.1)  # Даем время на обработку
            except Exception as e:
                self.logger.warning(f"Error during unsubscribe: {e}")

            await self.websocket.close()

        if self.session and not self.session.closed:
            await self.session.close()

        await super().stop()
