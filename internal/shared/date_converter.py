from datetime import datetime

def normalize_binance_instrument_name(instrument_name: str) -> str:
    try:
        parts = instrument_name.split('-')
        if len(parts) != 4:
            return instrument_name

        currency, date_part, strike, option_type = parts

        date_obj = datetime.strptime(date_part, '%y%m%d')
        normalized_date = date_obj.strftime('%d%b%y').upper()

        return f"{currency}-{normalized_date}-{strike}-{option_type}"

    except Exception:
        return instrument_name
