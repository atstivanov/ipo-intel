import json
import hashlib
from typing import Any

import psycopg2
from psycopg2.extras import execute_values

from src.config import PG


# -----------------------
# Connection helpers
# -----------------------

def get_conn():
    return psycopg2.connect(**PG)


def start_run(conn, source: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO raw.ingestion_runs(source) VALUES (%s) RETURNING run_id;",
            (source,),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def finish_run(conn, run_id: int, status: str, notes: str | None = None):
    """
    Mark an ingestion run as finished.

    Important: Call conn.rollback() BEFORE this if a previous statement failed,
    otherwise you'll hit 'current transaction is aborted'.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE raw.ingestion_runs
            SET finished_at = now(), status = %s, notes = %s
            WHERE run_id = %s;
            """,
            (status, notes, run_id),
        )
    conn.commit()


# -----------------------
# IPO events
# -----------------------

def _event_id(source: str, ipo_date: Any, company_name: str | None, symbol: str | None) -> str:
    """
    Stable id for an IPO calendar event.
    We keep it deterministic so re-runs upsert the same row.
    """
    base = f"{source}|{ipo_date}|{company_name or ''}|{symbol or ''}"
    return hashlib.md5(base.encode("utf-8")).hexdigest()


def upsert_ipo_events(conn, rows: list[dict]):
    """
    Upsert Finnhub IPO events into raw.ipo_events.
    Deduplicates events within the same batch by event_id.
    """
    if not rows:
        return

    deduped = {}
    for r in rows:
        ipo_date = r.get("date")
        company_name = r.get("name")
        symbol = r.get("symbol")

        event_id = _event_id("finnhub", ipo_date, company_name, symbol)

        # last write wins if Finnhub sends duplicates
        deduped[event_id] = (
            event_id,
            "finnhub",
            ipo_date,
            symbol,
            company_name,
            r.get("exchange"),
            r.get("priceRangeLow"),
            r.get("priceRangeHigh"),
            r.get("numberOfShares"),
            json.dumps(r, default=str),
        )

    values = list(deduped.values())

    sql = """
    INSERT INTO raw.ipo_events (
      event_id, source, ipo_date, symbol, company_name, exchange,
      price_range_low, price_range_high, shares, raw_json
    )
    VALUES %s
    ON CONFLICT (event_id)
    DO UPDATE SET
      exchange = EXCLUDED.exchange,
      price_range_low = EXCLUDED.price_range_low,
      price_range_high = EXCLUDED.price_range_high,
      shares = EXCLUDED.shares,
      raw_json = EXCLUDED.raw_json,
      ingested_at = now();
    """

    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, values, page_size=1000)
        conn.commit()
    except Exception:
        conn.rollback()
        raise



# -----------------------
# Daily prices
# -----------------------

def upsert_symbol_map(conn, vendor: str, rows: list[dict]) -> int:
    """
    rows: [{ipo_symbol, vendor_symbol, is_priceable, notes}]
    """
    if not rows:
        return 0

    values = []
    for r in rows:
        values.append((
            vendor,
            r["ipo_symbol"],
            r["vendor_symbol"],
            bool(r.get("is_priceable", True)),
            r.get("notes"),
        ))

    sql = """
    INSERT INTO raw.symbol_map (vendor, ipo_symbol, vendor_symbol, is_priceable, notes)
    VALUES %s
    ON CONFLICT (vendor, ipo_symbol)
    DO UPDATE SET
      vendor_symbol = EXCLUDED.vendor_symbol,
      is_priceable = EXCLUDED.is_priceable,
      notes = EXCLUDED.notes,
      last_checked_at = now();
    """

    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, values, page_size=5000)
        conn.commit()
        return len(values)
    except Exception:
        conn.rollback()
        raise


def upsert_daily_prices(conn, rows: list[dict]) -> int:
    """
    Generic upsert for daily OHLCV rows from any source.
    Expected keys per row:
      source, symbol, date, open, high, low, close, volume
    """
    if not rows:
        return 0

    values = []
    for r in rows:
        values.append((
            r.get("source", "unknown"),
            r["symbol"],
            r["date"],  # ISO string 'YYYY-MM-DD' is fine for Postgres date
            r.get("open"),
            r.get("high"),
            r.get("low"),
            r.get("close"),
            r.get("volume"),
            json.dumps(r, default=str),
        ))

    sql = """
    INSERT INTO raw.daily_prices (
      source, symbol, price_date, open, high, low, close, volume, raw_row
    )
    VALUES %s
    ON CONFLICT (source, symbol, price_date)
    DO UPDATE SET
      open = EXCLUDED.open,
      high = EXCLUDED.high,
      low = EXCLUDED.low,
      close = EXCLUDED.close,
      volume = EXCLUDED.volume,
      raw_row = EXCLUDED.raw_row,
      ingested_at = now();
    """

    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, values, page_size=5000)
        conn.commit()
        return len(values)
    except Exception:
        conn.rollback()
        raise

def upsert_company_profiles(conn, rows: list[dict]) -> int:
    """
    Upsert Finnhub company profiles into raw.company_profiles,
    but dynamically adapts to the actual table columns.
    """
    if not rows:
        return 0

    import json
    from psycopg2.extras import execute_values

    def _clean_str(v):
        if v is None:
            return None
        s = str(v).strip()
        return None if s == "" or s.lower() in ("null", "none", "n/a") else s

    def _clean_date(v):
        # Accept YYYY-MM-DD strings, turn empty -> NULL
        s = _clean_str(v)
        return s  # let postgres cast if column is date; if text, ok

    def _clean_num(v):
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return v
        s = str(v).strip()
        if s == "" or s.lower() in ("null", "none", "n/a"):
            return None
        try:
            return float(s)
        except Exception:
            return None

    # 1) discover table columns
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='raw' AND table_name='company_profiles'
            ORDER BY ordinal_position;
            """
        )
        cols = [r[0] for r in cur.fetchall()]

    if "symbol" not in cols:
        raise RuntimeError("raw.company_profiles must have a 'symbol' column")

    # 2) Build a normalized record from finnhub payload, then only keep existing cols
    def to_record(p: dict) -> dict:
        # Finnhub fields:
        # ticker, name, exchange, country, ipo, marketCapitalization, finnhubIndustry, weburl, logo
        rec = {
            "symbol": _clean_str(p.get("symbol") or p.get("ticker")),
            "name": _clean_str(p.get("name")),
            "exchange": _clean_str(p.get("exchange")),
            "country": _clean_str(p.get("country")),
            "ipo": _clean_date(p.get("ipo")),
            "market_cap": _clean_num(p.get("marketCapitalization") or p.get("market_cap")),
            "industry": _clean_str(p.get("finnhubIndustry") or p.get("industry")),
            "weburl": _clean_str(p.get("weburl")),
            "logo": _clean_str(p.get("logo")),
            "raw_json": json.dumps(p, default=str),
        }
        # Keep only columns that exist in table
        return {k: v for k, v in rec.items() if k in cols}

    records = [to_record(r) for r in rows if (r.get("ticker") or r.get("symbol"))]
    records = [r for r in records if r.get("symbol")]
    if not records:
        return 0

    # 3) Build SQL dynamically
    insert_cols = list(records[0].keys())

    # Ensure all records have same keys (fill missing as None)
    for r in records:
        for c in insert_cols:
            r.setdefault(c, None)

    values = [[r[c] for c in insert_cols] for r in records]

    set_cols = [c for c in insert_cols if c != "symbol"]
    set_clause = ",\n      ".join([f"{c} = EXCLUDED.{c}" for c in set_cols]) if set_cols else ""

    sql = f"""
    INSERT INTO raw.company_profiles ({", ".join(insert_cols)})
    VALUES %s
    ON CONFLICT (symbol)
    DO UPDATE SET
      {set_clause}
    ;
    """ if set_clause else f"""
    INSERT INTO raw.company_profiles ({", ".join(insert_cols)})
    VALUES %s
    ON CONFLICT (symbol)
    DO NOTHING;
    """

    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, values, page_size=500)
        conn.commit()
        return len(records)
    except Exception:
        conn.rollback()
        raise
