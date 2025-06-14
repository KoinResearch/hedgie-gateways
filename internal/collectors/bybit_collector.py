import asyncio
import aiohttp
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Set
from ..shared.base_collector import BaseCollector

class BybitCollector(BaseCollector):

    def __init__(self):
        super().__init__("bybit")
        self.base_url = "https://api-testnet.bybit.com"
        self.session = None
        self.currency_pairs = ['BTC', 'ETH']
        self.collection_interval = 30

        self.processed_trade_ids: Dict[str, Set[str]] = {
            'BTC': set(),
            'ETH': set()
        }

        self.max_cache_size = 10000

    async def _init_collector(self):
        timeout = aiohttp.ClientTimeout(total=30)
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={'User-Agent': 'Hedgie-Bybit-Options-Collector/1.0'}
        )

        await self._load_last_trade_ids()
        self.logger.info("Successfully connected to Bybit API")

    async def _load_last_trade_ids(self):
        try:
            for currency in self.currency_pairs:
                table_name = f"bybit_{currency.lower()}_trades"

                query = f"""
                SELECT trade_id FROM {table_name}
                ORDER BY timestamp DESC
                LIMIT 1000
                """

                async with self.db_pool.acquire() as connection:
                    try:
                        rows = await connection.fetch(query)
                        trade_ids = {row['trade_id'] for row in rows}
                        self.processed_trade_ids[currency] = trade_ids

                    except Exception as e:
                        self.processed_trade_ids[currency] = set()

        except Exception as e:
            pass

    async def _collect_data(self):
        for currency in self.currency_pairs:
            try:
                self.stats['total_requests'] += 1

                trades = await self._fetch_trades(currency)

                if trades:
                    options_trades = self._filter_options(trades)

                    if options_trades:
                        new_trades = self._filter_new_trades(options_trades, currency)

                        if new_trades:
                            processed_trades = []
                            for trade in new_trades:
                                processed_trade = self._process_single_trade(trade, currency)
                                if processed_trade:
                                    processed_trades.append(processed_trade)

                            if processed_trades:
                                table_name = f"bybit_{currency.lower()}_trades"
                                await self.save_trades(processed_trades, table_name)

                                self._update_cache(processed_trades, currency)

                                self.logger.info(f"Saved {len(processed_trades)} {currency} trades")
                                self.stats['total_trades_saved'] += len(processed_trades)

                self.stats['successful_requests'] += 1
                await asyncio.sleep(2)

            except Exception as e:
                self.logger.error(f"Error collecting {currency} trades: {e}")
                self.stats['failed_requests'] += 1

    def _filter_new_trades(self, trades: List[Dict[str, Any]], currency: str) -> List[Dict[str, Any]]:
        new_trades = []
        cached_ids = self.processed_trade_ids[currency]

        for trade in trades:
            trade_id = trade.get('execId', '')
            if trade_id and trade_id not in cached_ids:
                new_trades.append(trade)

        return new_trades

    def _update_cache(self, processed_trades: List[Dict[str, Any]], currency: str):
        cache = self.processed_trade_ids[currency]

        for trade in processed_trades:
            trade_id = trade.get('trade_id')
            if trade_id:
                cache.add(trade_id)

        if len(cache) > self.max_cache_size:
            excess = len(cache) - self.max_cache_size
            cache_list = list(cache)
            for _ in range(excess):
                cache.discard(cache_list.pop(0))

    async def _fetch_trades(self, currency: str) -> List[Dict[str, Any]]:
        try:
            params = {
                "category": "option",
                "baseCoin": currency,
                "limit": 100
            }

            url = f"{self.base_url}/v5/market/recent-trade"

            async with self.session.get(url, params=params) as response:
                if response.status != 200:
                    response.raise_for_status()

                data = await response.json()

                ret_code = data.get("retCode", -1)
                if ret_code != 0:
                    return []

                result = data.get("result", {})
                trades_list = result.get("list", [])

                return trades_list

        except Exception as e:
            raise

    def _filter_options(self, trades: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        filtered = []
        for trade in trades:
            symbol = trade.get('symbol', '')
            if isinstance(symbol, str) and '-' in symbol:
                parts = symbol.split('-')
                if len(parts) >= 4 and (parts[-1] in {'C', 'P'} or parts[-2] in {'C', 'P'}):
                    filtered.append(trade)

        return filtered

    def _process_single_trade(self, trade: Dict[str, Any], currency: str) -> Optional[Dict[str, Any]]:
        try:
            exec_id = trade.get("execId", "")
            symbol = trade.get("symbol", "")
            price = self._safe_float(trade.get("price"))
            size = self._safe_float(trade.get("size"))
            side = trade.get("side", "")
            timestamp = trade.get("time", "")

            mark_price = self._safe_float(trade.get("mP"))
            index_price = self._safe_float(trade.get("iP"))
            iv = self._safe_float(trade.get("iv"))

            trade_timestamp = None
            if timestamp:
                try:
                    trade_timestamp = datetime.fromtimestamp(int(timestamp) / 1000)
                except (ValueError, TypeError):
                    trade_timestamp = datetime.utcnow()

            processed_trade = {
                "trade_id": exec_id,
                "contracts": size,
                "mark_price": mark_price,
                "amount": price * size if price and size else None,
                "instrument_name": symbol,
                "index_price": index_price,
                "direction": side,
                "price": price,
                "iv": iv,
                "timestamp": trade_timestamp
            }

            return processed_trade

        except Exception as e:
            return None

    def _safe_float(self, value) -> Optional[float]:
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _insert_trade(self, cursor, trade: Dict[str, Any], table_name: str):
        query = f"""
        INSERT INTO {table_name} (
            trade_id, contracts, mark_price, amount, instrument_name,
            index_price, direction, price, iv, timestamp
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (trade_id) DO NOTHING
        """

        cursor.execute(query, (
            trade.get('trade_id'),
            trade.get('contracts'),
            trade.get('mark_price'),
            trade.get('amount'),
            trade.get('instrument_name'),
            trade.get('index_price'),
            trade.get('direction'),
            trade.get('price'),
            trade.get('iv'),
            trade.get('timestamp')
        ))

    async def stop(self):
        if self.session and not self.session.closed:
            await self.session.close()

        await super().stop()
