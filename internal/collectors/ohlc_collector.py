import asyncio
import aiohttp
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from ..shared.base_collector import BaseCollector

class OHLCCollector(BaseCollector):
    def __init__(self):
        super().__init__("ohlc")
        self.base_url = "https://api.binance.com"
        self.session = None
        self.currency_pairs = ['BTC', 'ETH']
        self.interval = "1h"  # 1-часовые свечи

        # Самые ранние данные (август 2017 для BTC, август 2017 для ETH)
        self.earliest_timestamps = {
            'BTC': 1503360000000,  # 22 августа 2017
            'ETH': 1502928000000   # 17 августа 2017 (примерно когда ETH появился на Binance)
        }

        # Статус сбора для каждой валюты
        self.collection_status = {
            'BTC': {'catching_up': True, 'last_timestamp': None},
            'ETH': {'catching_up': True, 'last_timestamp': None}
        }

        self.rate_limit_delay = 0.1  # 100ms между запросами для соблюдения лимитов

    async def _init_collector(self):
        timeout = aiohttp.ClientTimeout(total=30)
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={'User-Agent': 'Hedgie-OHLC-Collector/1.0'}
        )

        # Проверяем последние сохраненные данные для каждой валюты
        await self._check_last_saved_data()
        self.logger.info("Successfully connected to Binance API")

    async def _check_last_saved_data(self):
        """Проверяем последние сохраненные данные для каждой валюты"""
        for currency in self.currency_pairs:
            try:
                table_name = f"ohlc_{currency.lower()}"

                query = f"""
                SELECT MAX(open_time) as last_timestamp
                FROM {table_name}
                WHERE interval_type = $1 AND exchange = 'binance'
                """

                async with self.db_pool.acquire() as connection:
                    result = await connection.fetchval(query, self.interval)

                    if result:
                        self.collection_status[currency]['last_timestamp'] = result
                        # Если у нас есть данные до текущего времени минус 2 часа, считаем что догнали
                        current_time = int(datetime.utcnow().timestamp() * 1000)
                        time_diff = current_time - result
                        if time_diff < 2 * 60 * 60 * 1000:  # меньше 2 часов
                            self.collection_status[currency]['catching_up'] = False
                            self.logger.info(f"{currency}: Found recent data, switching to live mode")
                    else:
                        # Начинаем с самых ранних данных
                        self.collection_status[currency]['last_timestamp'] = self.earliest_timestamps[currency]

                    self.logger.info(f"{currency}: Last timestamp: {self.collection_status[currency]['last_timestamp']}, Catching up: {self.collection_status[currency]['catching_up']}")

            except Exception as e:
                self.logger.error(f"Error checking last saved data for {currency}: {e}")
                # Начинаем с самых ранних данных
                self.collection_status[currency]['last_timestamp'] = self.earliest_timestamps[currency]

    async def _collect_data(self):
        # Собираем данные для обеих валют
        for currency in self.currency_pairs:
            try:
                await self._collect_currency_data(currency)
                # Небольшая задержка между валютами
                await asyncio.sleep(0.1)
            except Exception as e:
                self.logger.error(f"Error collecting data for {currency}: {e}")
                self.stats['failed_requests'] += 1

    async def _collect_currency_data(self, currency: str):
        """Собираем данные для конкретной валюты"""
        symbol = f"{currency}USDT"
        status = self.collection_status[currency]

        # Определяем лимит записей в зависимости от режима
        if status['catching_up']:
            limit = 250  # В режиме догона берем больше данных
        else:
            limit = 10   # В режиме живых данных берем меньше

        try:
            # Определяем временной диапазон
            start_time = status['last_timestamp']

            if status['catching_up']:
                # В режиме догона берем данные от последней метки до текущего времени
                end_time = None
            else:
                # В живом режиме берем только последние данные
                end_time = None

            candles = await self._fetch_klines(symbol, start_time, end_time, limit)

            if candles:
                # Фильтруем новые свечи
                new_candles = await self._filter_new_candles(candles, currency)

                if new_candles:
                    # Сохраняем новые свечи
                    await self._save_candles(new_candles, currency)

                    # Обновляем последнюю метку времени
                    last_candle_time = max(candle['open_time'] for candle in new_candles)
                    status['last_timestamp'] = last_candle_time

                    self.logger.info(f"{currency}: Saved {len(new_candles)} new candles, last timestamp: {last_candle_time}")

                    # Проверяем, догнали ли мы до текущего времени
                    if status['catching_up']:
                        current_time = int(datetime.utcnow().timestamp() * 1000)
                        time_diff = current_time - last_candle_time

                        if time_diff < 2 * 60 * 60 * 1000:  # меньше 2 часов
                            status['catching_up'] = False
                            self.logger.info(f"{currency}: Caught up to current time, switching to live mode")

                else:
                    if not status['catching_up']:
                        self.logger.debug(f"{currency}: No new candles in live mode")

            self.stats['successful_requests'] += 1

        except Exception as e:
            self.logger.error(f"Error collecting {currency} data: {e}")
            self.stats['failed_requests'] += 1
            raise

        # Соблюдаем лимиты API
        await asyncio.sleep(self.rate_limit_delay)

    async def _fetch_klines(self, symbol: str, start_time: Optional[int] = None,
                           end_time: Optional[int] = None, limit: int = 500) -> List[List]:
        """Получаем данные свечей с Binance API"""
        endpoint = f"{self.base_url}/api/v3/klines"

        params = {
            'symbol': symbol,
            'interval': self.interval,
            'limit': min(limit, 1000)  # Максимум 1000 записей за запрос
        }

        if start_time:
            params['startTime'] = start_time + 1  # +1 чтобы не дублировать последнюю свечу

        if end_time:
            params['endTime'] = end_time

        self.stats['total_requests'] += 1

        try:
            async with self.session.get(endpoint, params=params) as response:
                if response.status == 429:  # Rate limit
                    self.logger.warning(f"Rate limit hit for {symbol}, waiting...")
                    await asyncio.sleep(1)
                    raise aiohttp.ClientError("Rate limit exceeded")

                if response.status != 200:
                    response.raise_for_status()

                data = await response.json()

                self.logger.debug(f"Fetched {len(data)} candles for {symbol}")
                return data

        except Exception as e:
            self.logger.error(f"Error fetching klines for {symbol}: {e}")
            raise

    async def _filter_new_candles(self, candles: List[List], currency: str) -> List[Dict[str, Any]]:
        """Фильтруем только новые свечи, которых еще нет в базе"""
        if not candles:
            return []

        # Получаем список уже существующих временных меток
        table_name = f"ohlc_{currency.lower()}"
        open_times = [int(candle[0]) for candle in candles]

        query = f"""
        SELECT open_time FROM {table_name}
        WHERE open_time = ANY($1) AND interval_type = $2 AND exchange = 'binance'
        """

        async with self.db_pool.acquire() as connection:
            existing_times = await connection.fetch(query, open_times, self.interval)
            existing_set = {row['open_time'] for row in existing_times}

        # Фильтруем только новые свечи
        new_candles = []
        for candle in candles:
            open_time = int(candle[0])
            if open_time not in existing_set:
                processed_candle = self._process_candle(candle, currency)
                if processed_candle:
                    new_candles.append(processed_candle)

        return new_candles

    def _process_candle(self, candle_data: List, currency: str) -> Dict[str, Any]:
        """Обрабатываем данные свечи в нужный формат"""
        try:
            open_time = int(candle_data[0])

            processed_candle = {
                'open_time': open_time,
                'open_price': float(candle_data[1]),
                'high_price': float(candle_data[2]),
                'low_price': float(candle_data[3]),
                'close_price': float(candle_data[4]),
                'volume': float(candle_data[5]),
                'close_time': int(candle_data[6]),
                'quote_asset_volume': float(candle_data[7]),
                'number_of_trades': int(candle_data[8]),
                'taker_buy_base_asset_volume': float(candle_data[9]),
                'taker_buy_quote_asset_volume': float(candle_data[10]),
                'interval_type': self.interval,
                'exchange': 'binance',
                'symbol': f"{currency}USDT",
                'timestamp': datetime.fromtimestamp(open_time / 1000.0)
            }

            return processed_candle

        except Exception as e:
            self.logger.error(f"Error processing candle data: {e}")
            return None

    async def _save_candles(self, candles: List[Dict[str, Any]], currency: str):
        """Сохраняем свечи в базу данных"""
        if not candles:
            return

        table_name = f"ohlc_{currency.lower()}"

        try:
            async with self.db_pool.acquire() as conn:
                saved_count = 0

                for candle in candles:
                    try:
                        await self._insert_candle_async(conn, candle, table_name)
                        saved_count += 1
                    except Exception as e:
                        # Вероятно, дублирование - пропускаем
                        self.logger.debug(f"Skipping candle {candle['open_time']}: {e}")
                        continue

                self.stats['total_trades_saved'] += saved_count

                if saved_count > 0:
                    self.logger.info(f"Saved {saved_count} new {currency} candles to {table_name}")

        except Exception as error:
            self.logger.error(f"Error saving {currency} candles: {error}")
            raise error

    async def _insert_candle_async(self, conn, candle: Dict[str, Any], table_name: str):
        """Вставляем свечу в базу данных"""
        query = f"""
        INSERT INTO {table_name} (
            open_time, open_price, high_price, low_price, close_price,
            volume, close_time, quote_asset_volume, number_of_trades,
            taker_buy_base_asset_volume, taker_buy_quote_asset_volume,
            interval_type, exchange, symbol, timestamp
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        ON CONFLICT (open_time, interval_type, exchange, symbol) DO NOTHING
        """

        await conn.execute(query,
            candle['open_time'],
            candle['open_price'],
            candle['high_price'],
            candle['low_price'],
            candle['close_price'],
            candle['volume'],
            candle['close_time'],
            candle['quote_asset_volume'],
            candle['number_of_trades'],
            candle['taker_buy_base_asset_volume'],
            candle['taker_buy_quote_asset_volume'],
            candle['interval_type'],
            candle['exchange'],
            candle['symbol'],
            candle['timestamp']
        )

    async def _insert_trade_async(self, conn, trade: Dict[str, Any], table_name: str):
        """Заглушка для совместимости с базовым классом"""
        pass

    def _insert_trade(self, cursor, trade: Dict[str, Any], table_name: str):
        """Заглушка для совместимости с базовым классом"""
        pass

    async def stop(self):
        if self.session and not self.session.closed:
            await self.session.close()
        await super().stop()
