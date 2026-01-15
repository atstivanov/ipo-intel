from __future__ import annotations

from datetime import date, datetime, timedelta
import os
import requests

from src.db import upsert_daily_prices

BASE = "https://www.alphavantage.co/query"


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _max_loaded_price_date(conn, symbol: str) -> date | None:
    """
    Return the latest price_date we have for this symbol.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(price_date)
            FROM raw.daily_prices
            WHERE source = 'alphavantage' AND symbol = %s;
            """,
            (symbol,),
        )
        return cur.fetchone()[0]


def fetch_alpha_daily(symbol: str) -> dict:
    """
    FREE endpoint: TIME_SERIES_DAILY (compact ~= last ~100 trading days)
    """
    api_key = os.getenv("ALPHAVANTAGE_API_KEY")
    if not api_key:
        raise RuntimeError("Missing ALPHAVANTAGE_API_KEY in environment")

    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "compact",
        "apikey": api_key,
    }
    r = requests.get(BASE, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    if "Note" in data:
        raise RuntimeError(f"Alpha Vantage rate limit hit: {data['Note']}")
    if "Error Message" in data:
        raise RuntimeError(f"Alpha Vantage error for {symbol}: {data['Error Message']}")
    if "Information" in data:
        raise RuntimeError(f"Alpha Vantage info for {symbol}: {data['Information']}")
    if "Time Series (Daily)" not in data:
        raise RuntimeError(f"Alpha Vantage unexpected response for {symbol}: {data}")

    return data


def normalize_alpha_rows(symbol: str, payload: dict, date_from: date, date_to: date) -> list[dict]:
    series = payload.get("Time Series (Daily)", {})
    rows: list[dict] = []

    for ds, ohlc in series.items():
        d = _parse_date(ds)
        if d < date_from or d > date_to:
            continue

        rows.append(
            {
                "source": "alphavantage",
                "symbol": symbol,
                "date": ds,
                "open": float(ohlc["1. open"]) if ohlc.get("1. open") else None,
                "high": float(ohlc["2. high"]) if ohlc.get("2. high") else None,
                "low": float(ohlc["3. low"]) if ohlc.get("3. low") else None,
                "close": float(ohlc["4. close"]) if ohlc.get("4. close") else None,
                "volume": int(float(ohlc["5. volume"])) if ohlc.get("5. volume") else None,
            }
        )

    rows.sort(key=lambda r: r["date"])
    return rows


def ingest_alpha_prices_for_symbol(
    conn,
    symbol: str,
    date_from: date,
    date_to: date,
    incremental: bool = True,
) -> int:
    effective_from = date_from
    if incremental:
        max_dt = _max_loaded_price_date(conn, symbol)
        if max_dt is not None:
            effective_from = max(max_dt + timedelta(days=1), date_from)

    if effective_from > date_to:
        return 0

    payload = fetch_alpha_daily(symbol)
    rows = normalize_alpha_rows(symbol, payload, effective_from, date_to)

    if not rows:
        return 0

    # upsert_daily_prices already commits/rollbacks inside db layer (or caller)
    return upsert_daily_prices(conn, rows)
