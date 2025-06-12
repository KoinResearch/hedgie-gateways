import aiohttp
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List
from ..shared.base_collector import BaseCollector

class DeribitCollector(BaseCollector):
    """Коллектор данных с биржи Deribit"""

    def __init__(self):
        super().__init__("deribit")
        self.base_url = "https://www.deribit.com/api/v2"
        self.currency_pairs = ['BTC', 'ETH']
        self.session = None

    async def _init_collector(self):
        """Инициализация HTTP сессии"""
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(timeout=timeout)

    async def _collect_data(self):
        """Сбор данных с Deribit"""
        current_time = datetime.utcnow()
        one_minute_ago = current_time - timedelta(minutes=1)

        start_timestamp = int(one_minute_ago.timestamp() * 1000)
        end_timestamp = int(current_time.timestamp() * 1000)

        for currency in self.currency_pairs:
            try:
                self.stats['total_requests'] += 1

                trades = await self._fetch_trades(currency, start_timestamp, end_timestamp)

                if trades:
                    self.logger.info(f"Fetched {len(trades)} trades for {currency}")

                    # Фильтруем опционы
                    options_trades = self._filter_options(trades)

                    # Сохраняем все торги
                    await self.save_trades(options_trades, f"all_{currency.lower()}_trades")

                    # Фильтруем и сохраняем блок-торги
                    block_trades = self._filter_block_trades(options_trades)
                    if block_trades:
                        await self.save_trades(block_trades, f"{currency.lower()}_block_trades")

                self.stats['successful_requests'] += 1

            except Exception as error:
                self.logger.error(f"Error fetching {currency} trades", error)
                self.stats['failed_requests'] += 1

    async def _fetch_trades(self, currency: str, start_timestamp: int, end_timestamp: int) -> List[Dict[str, Any]]:
        """Получение торгов с API"""
        url = f"{self.base_url}/public/get_last_trades_by_currency_and_time"
        params = {
            'currency': currency,
            'start_timestamp': start_timestamp,
            'end_timestamp': end_timestamp,
            'count': 1000,
            'include_old': 'false'  # Исправлено: строка вместо bool
        }

        async with self.session.get(url, params=params) as response:
            response.raise_for_status()
            data = await response.json()

            if 'result' in data and 'trades' in data['result']:
                return data['result']['trades']

            return []

    def _filter_options(self, trades: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Фильтрация опционов"""
        filtered = []
        for trade in trades:
            instrument_name = trade.get('instrument_name', '')
            if '-' in instrument_name and instrument_name.split('-')[-1] in {'C', 'P'}:
                filtered.append(trade)

        self.logger.debug(f"Filtered {len(filtered)} options from {len(trades)} trades")
        return filtered

    def _filter_block_trades(self, trades: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Фильтрация блок-торгов"""
        filtered = []
        for trade in trades:
            if 'block_trade_id' in trade and 'block_trade_leg_count' in trade:
                filtered.append(trade)

        self.logger.debug(f"Filtered {len(filtered)} block trades from {len(trades)} trades")
        return filtered

    def _insert_trade(self, cursor, trade: Dict[str, Any], table_name: str):
        """Вставка торга Deribit в базу данных"""
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
            trade['trade_id'],
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
            trade['timestamp']
        ))

    async def stop(self):
        """Остановка коллектора"""
        if self.session:
            await self.session.close()
        await super().stop()
