import os
import requests
import psycopg2
from datetime import datetime, timedelta
import time
import pytz
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import sys

print("Script started!", flush=True)
print(f"Python version: {sys.version}", flush=True)

BASE_URL = 'https://www.deribit.com/api/v2'
CURRENCY_PAIRS = ['BTC', 'ETH']
MOSCOW_TZ = pytz.timezone('Europe/Moscow')

print("Constants defined", flush=True)

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_NAME = os.getenv("PG_NAME", "deribit_trades")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")

print(f"DB config: {PG_HOST}:{PG_PORT}/{PG_NAME}", flush=True)

def get_postgres_connection():
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_NAME,
            user=PG_USER,
            password=PG_PASSWORD
        )
        print("PostgreSQL connection successful!", flush=True)
        return conn
    except Exception as e:
        print(f"PostgreSQL connection failed: {e}", flush=True)
        raise

retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retries)
http = requests.Session()
http.mount("https://", adapter)

print("HTTP session configured", flush=True)

def get_last_trades_by_currency(currency, start_timestamp, end_timestamp):
    print(f"Fetching trades for {currency}...", flush=True)
    endpoint = f"{BASE_URL}/public/get_last_trades_by_currency_and_time"
    params = {
        'currency': currency,
        'start_timestamp': start_timestamp,
        'end_timestamp': end_timestamp,
        'count': 1000,
        'include_old': False
    }
    try:
        response = http.get(endpoint, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}", flush=True)
        return {}

def filter_options(trades):
    filtered = [
        trade for trade in trades
        if "-" in trade['instrument_name'] and trade['instrument_name'].split('-')[-1] in {'C', 'P'}
    ]
    print(f"Filtered {len(filtered)} options from {len(trades)} trades", flush=True)
    return filtered

def filter_block_trades(trades):
    filtered = [trade for trade in trades if 'block_trade_id' in trade and 'block_trade_leg_count' in trade]
    print(f"Filtered {len(filtered)} block trades from {len(trades)} trades", flush=True)
    return filtered

def save_trades_to_postgres(trades, currency, table_name):
    if not trades:
        print(f"No trades to save for {currency}", flush=True)
        return

    print(f"Saving {len(trades)} trades to {table_name}...", flush=True)

    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()

        saved_count = 0
        for trade in trades:
            cursor.execute(f"""
            SELECT 1 FROM {table_name} WHERE trade_id = %s LIMIT 1
            """, (trade['trade_id'],))
            exists = cursor.fetchone()

            if exists:
                print(f"Skipping existing trade with trade_id {trade['trade_id']}", flush=True)
                continue

            query = f"""
            INSERT INTO {table_name} (
                trade_id, block_trade_leg_count, contracts, block_trade_id, combo_id, tick_direction,
                mark_price, amount, trade_seq, instrument_name, index_price, direction, price, iv,
                liquidation, combo_trade_id, timestamp
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, to_timestamp(%s / 1000))
            ON CONFLICT DO NOTHING;
            """
            cursor.execute(query, (
                trade['trade_id'],
                trade.get('block_trade_leg_count', None),
                trade.get('contracts', None),
                trade.get('block_trade_id', None),
                trade.get('combo_id', None),
                trade.get('tick_direction', None),
                trade.get('mark_price', None),
                trade.get('amount', None),
                trade.get('trade_seq', None),
                trade.get('instrument_name', None),
                trade.get('index_price', None),
                trade.get('direction', None),
                trade.get('price', None),
                trade.get('iv', None),
                trade.get('liquidation', None),
                trade.get('combo_trade_id', None),
                trade['timestamp']
            ))
            saved_count += 1

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Successfully saved {saved_count} new trades to {table_name} for {currency}", flush=True)

    except Exception as e:
        print(f"Error saving trades: {e}", flush=True)

def fetch_and_save_trades():
    current_time_utc = datetime.now(pytz.utc)
    one_minute_ago = current_time_utc - timedelta(minutes=1)

    start_timestamp = int(one_minute_ago.timestamp() * 1000)
    end_timestamp = int(current_time_utc.timestamp() * 1000)

    for currency in CURRENCY_PAIRS:
        trades_response = get_last_trades_by_currency(currency, start_timestamp, end_timestamp)

        if 'result' in trades_response and 'trades' in trades_response['result']:
            trades = trades_response['result']['trades']
            print(f"Got {len(trades)} total trades for {currency}", flush=True)

            if trades:
                options_trades = filter_options(trades)

                save_trades_to_postgres(options_trades, currency, f"all_{currency.lower()}_trades")

                block_trades = filter_block_trades(options_trades)
                if block_trades:
                    save_trades_to_postgres(block_trades, currency, f"{currency.lower()}_block_trades")
        else:
            print(f"No trades found for {currency}", flush=True)

if __name__ == "__main__":
    print("=== Starting Deribit trades collector ===", flush=True)

    while True:
        try:
            fetch_and_save_trades()
        except Exception as e:
            print(f"Error in main loop: {e}", flush=True)
        time.sleep(60)
