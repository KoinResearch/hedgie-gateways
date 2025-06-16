import asyncio
import json
import websockets
from datetime import datetime
from typing import Dict, Any, List, Set, Optional
from decimal import Decimal

from ..shared.base_collector import BaseCollector

class BinanceCollector(BaseCollector):
    def __init__(self):
        super().__init__("binance")
        self.underlying_assets = ['BTC', 'ETH']
        self._stop_event = asyncio.Event()
        self.ws_tasks = []
        self.stream_stats = {
            'BTC': {'messages': 0, 'trades': 0, 'last_trade': None, 'connected': False},
            'ETH': {'messages': 0, 'trades': 0, 'last_trade': None, 'connected': False}
        }
        self.processed_trade_ids: Dict[str, Set[str]] = {
            'BTC': set(),
            'ETH': set()
        }
        self.max_cache_size = 5000
        self.trade_queue = asyncio.Queue()
        self._trade_processor_task = None
        self._db_lock = asyncio.Lock()

    async def _init_collector(self):
        await self._load_last_trade_ids()
        self._trade_processor_task = asyncio.create_task(self._process_trade_queue())
        await self._start_options_websockets()

    async def _load_last_trade_ids(self):
        try:
            for underlying in self.underlying_assets:
                table_name = f"binance_{underlying.lower()}_trades"
                query = f"SELECT trade_id FROM {table_name} ORDER BY timestamp DESC LIMIT 1000"

                async with self._db_lock:
                    async with self.db_pool.acquire() as connection:
                        try:
                            rows = await connection.fetch(query)
                            trade_ids = {str(row['trade_id']) for row in rows}
                            self.processed_trade_ids[underlying] = trade_ids
                        except Exception:
                            self.processed_trade_ids[underlying] = set()
        except Exception as e:
            self.logger.error(f"Error loading trade IDs: {e}")
            for underlying in self.underlying_assets:
                self.processed_trade_ids[underlying] = set()

    async def _process_trade_queue(self):
        while not self._stop_event.is_set():
            try:
                trade_item = await asyncio.wait_for(self.trade_queue.get(), timeout=1.0)
                trade_data, underlying = trade_item

                await self._process_single_trade(trade_data, underlying)
                self.trade_queue.task_done()

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self.logger.error(f"Error in trade queue processor: {e}")
                await asyncio.sleep(0.1)

    async def _process_single_trade(self, trade_data: Dict[str, Any], underlying: str):
        try:
            symbol = trade_data.get('s', '')
            trade_id_raw = trade_data.get('t', '')
            price_raw = trade_data.get('p', '')
            quantity_raw = trade_data.get('q', '')
            direction_code = trade_data.get('S', '1')
            trade_time = trade_data.get('T', 0)

            try:
                trade_id = int(trade_id_raw) if trade_id_raw else None
            except (ValueError, TypeError):
                return

            try:
                price = float(price_raw) if price_raw else None
            except (ValueError, TypeError):
                return

            try:
                quantity = float(quantity_raw) if quantity_raw else None
            except (ValueError, TypeError):
                return

            if not all([symbol, trade_id is not None, price is not None, quantity is not None]):
                return

            trade_id_str = str(trade_id)
            if trade_id_str and trade_id_str in self.processed_trade_ids[underlying]:
                return

            if isinstance(trade_time, (int, float)) and trade_time > 0:
                if trade_time > 1e12:
                    timestamp = datetime.fromtimestamp(trade_time / 1000.0)
                else:
                    timestamp = datetime.fromtimestamp(trade_time)
            else:
                timestamp = datetime.utcnow()

            if str(direction_code) == "-1":
                direction = "sell"
            else:
                direction = "buy"

            abs_quantity = abs(quantity)
            amount = abs_quantity * price

            trade = {
                'trade_id': trade_id,
                'contracts': abs_quantity,
                'amount': amount,
                'instrument_name': symbol,
                'direction': direction,
                'price': price,
                'timestamp': timestamp
            }

            table_name = f"binance_{underlying.lower()}_trades"

            async with self._db_lock:
                await self.save_trades([trade], table_name)

            if trade_id_str:
                self.processed_trade_ids[underlying].add(trade_id_str)
                self._cleanup_cache(underlying)

            self.stream_stats[underlying]['trades'] += 1
            self.stream_stats[underlying]['last_trade'] = datetime.utcnow()
            self.stats['successful_requests'] += 1

        except Exception as e:
            self.logger.error(f"Error processing trade data for {underlying}: {e}")
            self.stats['failed_requests'] += 1

    async def _start_options_websockets(self):
        for underlying in self.underlying_assets:
            task = asyncio.create_task(self._options_websocket_stream(underlying))
            self.ws_tasks.append(task)

    async def _options_websocket_stream(self, underlying: str):
        stream_name = f"{underlying}@trade"
        uri = f"wss://nbstream.binance.com/eoptions/stream?streams={stream_name}"

        reconnect_delay = 1
        max_reconnect_delay = 60
        attempt = 0

        while not self._stop_event.is_set():
            attempt += 1
            try:
                async with websockets.connect(
                    uri,
                    timeout=30,
                    ping_interval=20,
                    ping_timeout=10,
                    extra_headers={"User-Agent": "hedgie-gateways/1.0"}
                ) as websocket:

                    self.stream_stats[underlying]['connected'] = True
                    reconnect_delay = 1
                    attempt = 0

                    async for message in websocket:
                        if self._stop_event.is_set():
                            break

                        try:
                            await self._handle_options_message(message, underlying)
                        except Exception as e:
                            self.logger.error(f"Error processing Options message for {underlying}: {e}")
                            continue

            except Exception as e:
                self.stream_stats[underlying]['connected'] = False
                if not self._stop_event.is_set():
                    self.logger.error(f"{underlying} Options WebSocket error: {e}")

            if not self._stop_event.is_set():
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)

    async def _handle_options_message(self, message, underlying: str):
        try:
            self.stream_stats[underlying]['messages'] += 1

            if isinstance(message, str):
                data = json.loads(message)
            else:
                data = message

            trade_data = None

            if isinstance(data, dict):
                if 'stream' in data and 'data' in data:
                    if data.get('stream') == f"{underlying}@trade":
                        trade_data = data['data']
                elif data.get('e') == 'trade':
                    trade_data = data
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and item.get('e') == 'trade':
                            await self.trade_queue.put((item, underlying))
                    return

            if trade_data:
                await self.trade_queue.put((trade_data, underlying))

        except Exception as e:
            self.logger.error(f"Error handling Options message for {underlying}: {e}")

    def _cleanup_cache(self, underlying: str):
        cache = self.processed_trade_ids[underlying]
        if len(cache) > self.max_cache_size:
            excess = len(cache) - int(self.max_cache_size * 0.8)
            cache_list = list(cache)
            for _ in range(excess):
                if cache_list:
                    cache.discard(cache_list.pop(0))

    async def _collect_data(self):
        await asyncio.sleep(1)

    async def _insert_trade_async(self, conn, trade: Dict[str, Any], table_name: str):
        query = f"""
        INSERT INTO {table_name} (
            trade_id, contracts, amount, instrument_name,
            direction, price, timestamp
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (trade_id) DO NOTHING
        """

        await conn.execute(query,
            trade.get('trade_id'),
            trade.get('contracts'),
            trade.get('amount'),
            trade.get('instrument_name'),
            trade.get('direction'),
            trade.get('price'),
            trade.get('timestamp')
        )

    def _insert_trade(self, cursor, trade: Dict[str, Any], table_name: str):
        pass

    async def stop(self):
        self._stop_event.set()

        if self._trade_processor_task:
            self._trade_processor_task.cancel()
            try:
                await self._trade_processor_task
            except asyncio.CancelledError:
                pass

        if not self.trade_queue.empty():
            await self.trade_queue.join()

        if self.ws_tasks:
            await asyncio.gather(*self.ws_tasks, return_exceptions=True)

        await super().stop()
