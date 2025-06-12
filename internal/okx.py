import psycopg2
import asyncio
import websockets
import json
from datetime import datetime

# Настройки PostgreSQL
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "deribit_trades"
DB_USER = "admin"
DB_PASSWORD = "admin123"

# WebSocket URL для OKX
WS_URL = 'wss://ws.okx.com:8443/ws/v5/public'
CURRENCY_PAIRS = ['BTC-USD', 'ETH-USD']

# Подключение к PostgreSQL
def get_postgres_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

# Функция для создания таблицы, если она не существует
def create_table_if_not_exists(currency):
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    table_name = f"okx_{currency.lower().split('-')[0]}_trades"
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        trade_id text PRIMARY KEY,
        mark_price numeric,
        amount numeric,
        instrument_name text,
        index_price numeric,
        direction text,
        price numeric,
        iv numeric,
        timestamp timestamp without time zone
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Table {table_name} is ready.")

# Функция для преобразования имени инструмента OKX в формат Deribit
def convert_instrument_name(okx_name):
    parts = okx_name.split('-')
    if len(parts) != 5:
        print(f"Unexpected format for instrument name: {okx_name}")
        return okx_name

    currency = parts[0]
    date_code = parts[2]
    strike_price = parts[3]
    option_type = parts[4]

    try:
        date_obj = datetime.strptime(date_code, '%y%m%d')
        formatted_date = date_obj.strftime('%d%b%y').upper()
    except ValueError as e:
        print(f"Date conversion error for {date_code}: {e}")
        return okx_name

    deribit_format = f"{currency}-{formatted_date}-{strike_price}-{option_type}"
    return deribit_format

# Функция для сохранения сделки в PostgreSQL
def save_trade_to_postgres(trade, currency):
    conn = get_postgres_connection()
    cursor = conn.cursor()

    table_name = f"okx_{currency.lower().split('-')[0]}_trades"
    timestamp_in_seconds = int(trade['ts']) / 1000
    instrument_name = convert_instrument_name(trade.get('instId', None))
    amount = float(trade.get('sz', 0)) * 0.01  # Умножаем на 0.01

    # Преобразование IV (умножаем на 100 для приведения к тем же единицам, что и на Deribit)
    iv = float(trade.get('fillVol', 0)) * 100 if trade.get('fillVol') is not None else None

    # Логирование значений перед вставкой
    print(f"Saving trade - ID: {trade['tradeId']}, Mark Price: {trade.get('markPx', None)}, "
          f"Adjusted Amount: {amount}, Instrument Name: {instrument_name}, "
          f"Index Price: {trade.get('idxPx', None)}, Direction: {trade.get('side', None)}, "
          f"Price: {trade.get('px', None)}, IV: {iv}, Timestamp: {timestamp_in_seconds}")

    query = f"""
    INSERT INTO {table_name} (
        trade_id, mark_price, amount, instrument_name, index_price, direction, price, iv, timestamp
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, to_timestamp(%s))
    ON CONFLICT DO NOTHING;
    """
    cursor.execute(query, (
        trade['tradeId'],
        trade.get('markPx', None),    
        amount,                        # Используем скорректированное значение amount
        instrument_name,              
        trade.get('idxPx', None),     
        trade.get('side', None),      
        trade.get('px', None),        
        iv,                            # Используем скорректированное значение IV
        timestamp_in_seconds          
    ))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"New trade added to {table_name}: {trade}")

# Функция для подписки и получения данных через WebSocket
async def fetch_trades():
    async with websockets.connect(WS_URL) as websocket:
        for currency in CURRENCY_PAIRS:
            create_table_if_not_exists(currency)

        for currency in CURRENCY_PAIRS:
            subscribe_message = {
                "op": "subscribe",
                "args": [{
                    "channel": "option-trades",
                    "instType": "OPTION",
                    "instFamily": currency
                }]
            }
            await websocket.send(json.dumps(subscribe_message))
            print(f"Subscribed to option trades for {currency}")

        while True:
            response = await websocket.recv()
            data = json.loads(response)

            if "data" in data:
                trades = data["data"]
                currency = data['arg']['instFamily'].split('-')[0]
                
                for trade in trades:
                    save_trade_to_postgres(trade, currency)
            else:
                print(f"Received message: {data}")

# Запускаем WebSocket-клиент
if __name__ == "__main__":
    asyncio.run(fetch_trades())
