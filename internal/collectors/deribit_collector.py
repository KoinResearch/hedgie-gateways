import aiohttp
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List
from ..shared.base_collector import BaseCollector

class DeribitCollector(BaseCollector):
    def __init__(self):
        super().__init__("deribit")
        self.base_url = "https://www.deribit.com/api/v2"
        self.currency_pairs = ['BTC', 'ETH']
        self.session = None

    async def _init_collector(self):
        timeout = aiohttp.ClientTimeout(total=30)
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={'User-Agent': 'Hedgie-Collector/1.0'}
        )
        self.logger.info("Successfully connected to Deribit API")

    async def _collect_data(self):
        current_time = datetime.utcnow()
        one_minute_ago = current_time - timedelta(minutes=1)

        start_timestamp = int(one_minute_ago.timestamp() * 1000)
        end_timestamp = int(current_time.timestamp() * 1000)

        for currency in self.currency_pairs:
            try:
                self.stats['total_requests'] += 1

                trades = await self._fetch_trades(currency, start_timestamp, end_timestamp)

                if trades:
                    options_trades = self._filter_options(trades)

                    if options_trades:
                        processed_trades = []
                        for trade in options_trades:
                            processed_trade = self._process_trade(trade)
                            if processed_trade:
                                processed_trades.append(processed_trade)

                        if processed_trades:
                            await self.save_trades(processed_trades, f"all_{currency.lower()}_trades")

                            block_trades = self._filter_block_trades(processed_trades)
                            if block_trades:
                                await self.save_trades(block_trades, f"{currency.lower()}_block_trades")

                self.stats['successful_requests'] += 1

            except Exception as error:
                self.logger.error(f"❌ Error fetching {currency} trades: {error}")
                self.stats['failed_requests'] += 1

    def _process_trade(self, trade: Dict[str, Any]) -> Dict[str, Any]:
        try:
            timestamp_ms = trade.get('timestamp')
            if timestamp_ms:
                timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000.0)
                trade['timestamp'] = timestamp_dt

            return trade
        except Exception as e:
            self.logger.error(f"Error processing trade: {e}")
            return None

    async def _fetch_trades(self, currency: str, start_timestamp: int, end_timestamp: int) -> List[Dict[str, Any]]:
        endpoint = f"{self.base_url}/public/get_last_trades_by_currency_and_time"

        params = {
            'currency': str(currency),
            'start_timestamp': int(start_timestamp),
            'end_timestamp': int(end_timestamp),
            'count': 1000,
            'include_old': 'false'
        }

        try:
            async with self.session.get(endpoint, params=params) as response:
                if response.status != 200:
                    response.raise_for_status()

                data = await response.json()

                if 'error' in data:
                    return []

                if 'result' in data and 'trades' in data['result']:
                    return data['result']['trades']

                return []

        except Exception as e:
            raise

    def _filter_options(self, trades: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        filtered = []
        for trade in trades:
            instrument_name = trade.get('instrument_name', '')
            if isinstance(instrument_name, str) and '-' in instrument_name:
                parts = instrument_name.split('-')
                if len(parts) >= 4 and parts[-1] in {'C', 'P'}:
                    filtered.append(trade)
        return filtered

    def _filter_block_trades(self, trades: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        filtered = []
        for trade in trades:
            block_trade_id = trade.get('block_trade_id')
            if block_trade_id is not None and block_trade_id != '':
                filtered.append(trade)
        return filtered

    async def _insert_trade_async(self, conn, trade: Dict[str, Any], table_name: str):
        query = f"""
        INSERT INTO {table_name} (
            trade_id, block_trade_leg_count, contracts, block_trade_id, combo_id, tick_direction,
            mark_price, amount, trade_seq, instrument_name, index_price, direction, price, iv,
            liquidation, combo_trade_id, timestamp
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
        """

        await conn.execute(query,
            str(trade.get('trade_id', '')),
            str(trade.get('block_trade_leg_count', '')),
            trade.get('contracts'),
            str(trade.get('block_trade_id', '')),
            str(trade.get('combo_id', '')),
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
            str(trade.get('combo_trade_id', '')),
            trade.get('timestamp')
        )

    def _insert_trade(self, cursor, trade: Dict[str, Any], table_name: str):
        pass

    async def stop(self):
        if self.session and not self.session.closed:
            await self.session.close()
        await super().stop()
