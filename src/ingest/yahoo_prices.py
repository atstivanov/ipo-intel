from __future__ import annotations

from datetime import date, timedelta
import time
from typing import Optional

import pandas as pd
import yfinance as yf

from src.db import upsert_daily_prices


def _max_loaded_price_date(conn, symbol: str) -> Optional[date]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(price_date)
            FROM raw.daily_prices
            WHERE source = 'yahoo' AND symbol = %s;
            """,
            (symbol,),
        )
        return cur.fetchone()[0]


def _download_yahoo_daily(symbol: str, date_from: date, date_to: date) -> pd.DataFrame:
    """
    Uses yfinance to download daily prices.
    Note: yfinance end-date is effectively exclusive, so we add +1 day.
    """
    # yfinance expects strings or datetimes; we pass ISO dates
    start = date_from.isoformat()
    end = (date_to + timedelta(days=1)).isoformat()

    df = yf.download(
        tickers=symbol,
        start=start,
        end=end,
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
        group_by="column",
    )

    # Expected columns: Open, High, Low, Close, Adj Close, Volume (index = Datetime)
    if df is None or df.empty:
        return pd.DataFrame()

    # Sometimes returned columns are multiindex if tickers list used. Normalize.
    if isinstance(df.columns, pd.MultiIndex):
        # pick the first level that matches typical OHLCV columns
        df = df.droplevel(0, axis=1)

    return df


def _normalize_yfinance_rows(symbol: str, df: pd.DataFrame, date_from: date, date_to: date) -> list[dict]:
    rows: list[dict] = []

    # Ensure index is datetime-like
    if not isinstance(df.index, pd.DatetimeIndex):
        try:
            df.index = pd.to_datetime(df.index)
        except Exception:
            return []

    # standardize column names
    col_map = {c.lower(): c for c in df.columns}
    open_col = col_map.get("open")
    high_col = col_map.get("high")
    low_col = col_map.get("low")
    close_col = col_map.get("close")
    volume_col = col_map.get("volume")

    for ts, r in df.iterrows():
        d = ts.date()
        if d < date_from or d > date_to:
            continue

        o = r[open_col] if open_col in r else None
        h = r[high_col] if high_col in r else None
        l = r[low_col] if low_col in r else None
        c = r[close_col] if close_col in r else None
        v = r[volume_col] if volume_col in r else None

        # skip fully empty row
        if pd.isna(o) and pd.isna(h) and pd.isna(l) and pd.isna(c):
            continue

        rows.append(
            {
                "source": "yahoo",
                "symbol": symbol,
                "date": d.isoformat(),
                "open": float(o) if o is not None and not pd.isna(o) else None,
                "high": float(h) if h is not None and not pd.isna(h) else None,
                "low": float(l) if l is not None and not pd.isna(l) else None,
                "close": float(c) if c is not None and not pd.isna(c) else None,
                "volume": int(v) if v is not None and not pd.isna(v) else None,
            }
        )

    rows.sort(key=lambda x: x["date"])
    return rows


def ingest_yahoo_prices_for_symbol(
    conn,
    symbol: str,
    date_from: date,
    date_to: date,
    incremental: bool = True,
    sleep_s: float = 0.2,
) -> int:
    """
    Ingest daily prices from Yahoo via yfinance.

    Returns number of rows upserted into raw.daily_prices.
    """
    effective_from = date_from

    if incremental:
        max_dt = _max_loaded_price_date(conn, symbol)
        if max_dt is not None:
            effective_from = max(max_dt + timedelta(days=1), date_from)

    if effective_from > date_to:
        return 0

    try:
        df = _download_yahoo_daily(symbol, effective_from, date_to)
    except Exception as e:
        # Keep error readable for logs
        raise RuntimeError(f"yfinance download failed for {symbol}: {e}") from e

    rows = _normalize_yfinance_rows(symbol, df, effective_from, date_to)
    if not rows:
        time.sleep(sleep_s)
        return 0

    n = upsert_daily_prices(conn, rows)
    time.sleep(sleep_s)
    return n
