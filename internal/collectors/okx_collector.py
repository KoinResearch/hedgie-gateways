import asyncio
import websockets
import json
import aiohttp
from datetime import datetime
from typing import Dict, Any, List
from ..shared.base_collector import BaseCollector

class OKXCollector(BaseCollector):
    def __init__(self):
        super().__init__("okx")
        self.ws_url = 'wss://ws.okx.com:8443/ws/v5/public'
        self.currency_pairs = ['BTC-USD', 'ETH-USD']
        self.websocket = None
        self.session = None

    async def _init_collector(self):
        timeout = aiohttp.ClientTimeout(total=30)
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={'User-Agent': 'Hedgie-OKX-Collector/1.0'}
        )
        self.logger.info("Successfully connected to OKX API")

    async def _collect_data(self):
        max_retries = 5
        retry_count = 0

        while self.running and retry_count < max_retries:
            try:
                await self._connect_and_subscribe()
                retry_count = 0

            except websockets.exceptions.ConnectionClosed:
                retry_count += 1
                self.logger.warning(f"WebSocket connection closed, retry {retry_count}/{max_retries}")
                await asyncio.sleep(min(retry_count * 2, 30))

            except Exception as error:
                retry_count += 1
                self.logger.error(f"WebSocket error, retry {retry_count}/{max_retries}: {error}")
                await asyncio.sleep(min(retry_count * 2, 30))

        if retry_count >= max_retries:
            self.logger.error("Max retries reached, stopping collector")
            self.running = False

    async def _connect_and_subscribe(self):
        self.logger.info("Connecting to OKX WebSocket...")

        async with websockets.connect(
            self.ws_url,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=10
        ) as websocket:
            self.websocket = websocket
            self.logger.info("Connected to OKX WebSocket")

            for currency in self.currency_pairs:
                await self._subscribe_to_trades(websocket, currency)

            while self.running:
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=30)
                    await self._process_message(message)

                except asyncio.TimeoutError:
                    self.logger.warning("WebSocket receive timeout")
                    await websocket.ping()

                except websockets.exceptions.ConnectionClosed:
                    self.logger.warning("WebSocket connection lost")
                    break

    async def _subscribe_to_trades(self, websocket, currency: str):
        subscribe_message = {
            "op": "subscribe",
            "args": [{
                "channel": "option-trades",
                "instType": "OPTION",
                "instFamily": currency
            }]
        }

        await websocket.send(json.dumps(subscribe_message))
        self.logger.info(f"Subscribed to option trades for {currency}")

    async def _process_message(self, message: str):
        try:
            data = json.loads(message)

            if "data" in data and data.get("arg", {}).get("channel") == "option-trades":
                trades = data["data"]
                currency = data['arg']['instFamily'].split('-')[0]

                self.logger.debug(f"Received {len(trades)} trades for {currency}")

                processed_trades = []
                for trade in trades:
                    processed_trade = self._process_trade(trade, currency)
                    if processed_trade:
                        processed_trades.append(processed_trade)

                if processed_trades:
                    table_name = f"okx_{currency.lower()}_trades"
                    await self.save_trades(processed_trades, table_name)
                    self.stats['successful_requests'] += 1

            elif "event" in data:
                if data["event"] == "subscribe":
                    self.logger.info(f"Successfully subscribed to {data.get('arg', {})}")
                elif data["event"] == "error":
                    self.logger.error(f"OKX API error: {data}")

        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON message: {e}")
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")

    def _process_trade(self, trade: Dict[str, Any], currency: str) -> Dict[str, Any]:
        try:
            timestamp_ms = int(trade['ts'])
            timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000.0)

            instrument_name = self._convert_instrument_name(trade.get('instId', ''))

            amount = float(trade.get('sz', 0)) * 0.01

            iv = None
            if trade.get('fillVol') is not None:
                iv = float(trade.get('fillVol', 0)) * 100

            processed_trade = {
                'trade_id': trade['tradeId'],
                'mark_price': self._safe_float(trade.get('markPx')),
                'amount': amount,
                'instrument_name': instrument_name,
                'index_price': self._safe_float(trade.get('idxPx')),
                'direction': trade.get('side'),
                'price': self._safe_float(trade.get('px')),
                'iv': iv,
                'timestamp': timestamp_dt
            }

            self.logger.debug(f"Processed trade: {processed_trade['trade_id']} - {instrument_name}")
            return processed_trade

        except Exception as e:
            self.logger.error(f"Error processing trade: {e}, trade data: {trade}")
            return None

    def _convert_instrument_name(self, okx_name: str) -> str:
        try:
            parts = okx_name.split('-')
            if len(parts) != 5:
                self.logger.warning(f"Unexpected instrument name format: {okx_name}")
                return okx_name

            currency = parts[0]
            date_code = parts[2]
            strike_price = parts[3]
            option_type = parts[4]

            date_obj = datetime.strptime(date_code, '%y%m%d')
            formatted_date = date_obj.strftime('%d%b%y').upper()

            deribit_format = f"{currency}-{formatted_date}-{strike_price}-{option_type}"
            return deribit_format

        except ValueError as e:
            self.logger.error(f"Date conversion error for {okx_name}: {e}")
            return okx_name
        except Exception as e:
            self.logger.error(f"Instrument name conversion error: {e}")
            return okx_name

    def _safe_float(self, value) -> float:
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    async def _insert_trade_async(self, conn, trade: Dict[str, Any], table_name: str):
        query = f"""
        INSERT INTO {table_name} (
            trade_id, mark_price, amount, instrument_name, index_price,
            direction, price, iv, timestamp
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """

        await conn.execute(query,
            trade['trade_id'],
            trade['mark_price'],
            trade['amount'],
            trade['instrument_name'],
            trade['index_price'],
            trade['direction'],
            trade['price'],
            trade['iv'],
            trade['timestamp']
        )

    def _insert_trade(self, cursor, trade: Dict[str, Any], table_name: str):
        pass

    async def stop(self):
        self.logger.info("Stopping OKX collector...")

        if self.websocket and not self.websocket.closed:
            await self.websocket.close()

        if self.session and not self.session.closed:
            await self.session.close()

        await super().stop()
