import os
import json
import boto3
import yfinance as yf
from datetime import datetime
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

AWS_REGION = os.environ.get("AWS_REGION", "ap-south-2")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY", "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_KEY", "")
MARKET_TABLE_NAME = os.environ.get("MARKET_TABLE_NAME", "markets")

kwargs = {}
if AWS_ACCESS_KEY_ID:
    kwargs['aws_access_key_id'] = AWS_ACCESS_KEY_ID

if AWS_SECRET_ACCESS_KEY:
    kwargs['aws_secret_access_key'] = AWS_SECRET_ACCESS_KEY

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION, **kwargs)
market_table = dynamodb.Table(MARKET_TABLE_NAME)

ALL_TICKERS = [
    'AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA', 'SPY',
    'AMZN', 'META', 'BRK.B', 'AVGO', 'GOOG', 'JPM',
    'LLY', 'V', 'UNH', 'XOM', 'MA', 'PG', 'JNJ', 'HD',
    'COST', 'ORCL', 'AMD', 'BAC', 'NFLX', 'CRM', 'ABT',
    'WMT', 'TMO', 'CVX', 'ACN', 'KO', 'AVY', 'PM', 'DIS',
    'PFE', 'INTU', 'ADBE', 'TXN', 'CSCO', 'NEE', 'WFC',
    'ABBV', 'DHR', 'COP', 'IBM', 'QCOM', 'CAT', 'RTX',
    'AXP', 'GS', 'BLK', 'AMGN', 'BX', 'PLD', 'LIN',
]


def _convert_floats(obj):
    """Convert float values to Decimal for DynamoDB compatibility."""
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: _convert_floats(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_convert_floats(i) for i in obj]
    return obj


def store_ticker_data(ticker, hist, period='2mo'):
    """Store a ticker's dataframe into the markets table."""
    records = []
    for date, row in hist.iterrows():
        records.append({
            "date": date.strftime('%Y-%m-%d'),
            "Open": float(row["Open"]),
            "High": float(row["High"]),
            "Low": float(row["Low"]),
            "Close": float(row["Close"]),
            "Volume": int(row["Volume"]),
        })

    item = {
        "ticker": ticker,
        "period": period,
        "records": _convert_floats(records),
        "record_count": len(records),
        "updated_at": datetime.now().isoformat(),
    }

    market_table.put_item(Item=item)
    print(f"[DB] Cached {ticker} - {len(records)} records")


def _fetch_and_store(ticker, period='2mo'):
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period=period)

        if hist.empty:
            print(f"[SKIP] {ticker} - no data returned")
            return ticker, False

        store_ticker_data(ticker, hist, period)
        print(f"[OK] {ticker} - {len(hist)} records stored")
        return ticker, True

    except Exception as e:
        print(f"[FAIL] {ticker} - {str(e)}")
        return ticker, False


def refresh_all(tickers=None, period='2mo', max_workers=10):
    tickers = tickers or ALL_TICKERS
    success = 0
    failed = []

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_fetch_and_store, t, period): t for t in tickers}
        for future in futures:
            ticker, ok = future.result()
            if ok:
                success += 1
            else:
                failed.append(ticker)

    print(f"\nDone: {success}/{len(tickers)} tickers stored, {len(failed)} failed")
    if failed:
        print(f"Failed: {failed}")

    return {"success": success, "failed": failed}


def get_market_data(ticker):
    """Fetch cached market data for a single ticker from DynamoDB."""
    response = market_table.get_item(Key={"ticker": ticker})
    return response.get("Item")


def lambda_handler(event, context):
    """Can be deployed as a scheduled Lambda via EventBridge."""
    result = refresh_all()
    return result


if __name__ == "__main__":
    refresh_all()
