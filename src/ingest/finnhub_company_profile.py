from __future__ import annotations

from datetime import datetime
import time
import requests

from src.config import FINNHUB_API_KEY
from src.db import upsert_company_profiles

BASE = "https://finnhub.io/api/v1/stock/profile2"


def _to_none_if_blank(v):
    if v is None:
        return None
    if isinstance(v, str) and v.strip() == "":
        return None
    return v


def _parse_date_yyyy_mm_dd(v) -> str | None:
    """
    Finnhub often returns:
      - "ipo": "2023-01-01"
      - or "ipo": ""
    We store dates in Postgres; easiest is to pass None for blanks and keep ISO for valid.
    """
    v = _to_none_if_blank(v)
    if v is None:
        return None
    try:
        # Validate format; keep as ISO string so psycopg2 can cast to date
        datetime.strptime(v, "%Y-%m-%d")
        return v
    except Exception:
        return None


def _to_float(v) -> float | None:
    v = _to_none_if_blank(v)
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _normalize_profile(payload: dict, fallback_symbol: str) -> dict | None:
    """
    Map Finnhub payload -> a normalized dict that your upsert can safely insert.
    We keep raw_json too (payload).
    """
    if not payload or not payload.get("ticker"):
        return None

    ticker = (payload.get("ticker") or fallback_symbol or "").strip()
    if not ticker:
        return None

    # IMPORTANT: ensure we never send empty string for date columns
    ipo_date = _parse_date_yyyy_mm_dd(payload.get("ipo"))

    normalized = {
        # core identity
        "symbol": ticker,

        # commonly used fields
        "name": _to_none_if_blank(payload.get("name")),
        "exchange": _to_none_if_blank(payload.get("exchange")),
        "country": _to_none_if_blank(payload.get("country")),
        "ipo": ipo_date,  # keep key "ipo" if your upsert expects it
        "marketCapitalization": _to_float(payload.get("marketCapitalization")),
        "finnhubIndustry": _to_none_if_blank(payload.get("finnhubIndustry")),

        # always keep the raw payload
        "raw_json": payload,
    }

    return normalized


def fetch_profile(symbol: str) -> dict:
    params = {"symbol": symbol, "token": FINNHUB_API_KEY}
    r = requests.get(BASE, params=params, timeout=30)

    # If 429, raise with response attached so caller can backoff/stop
    r.raise_for_status()
    return r.json() or {}


def ingest_finnhub_company_profiles(
    conn,
    symbols: list[str],
    sleep_s: float = 0.8,
    batch_size: int = 50,
    stop_on_429: bool = True,
    max_429_retries: int = 2,
) -> int:
    """
    Fetch Finnhub profiles for a list of symbols and upsert them.

    Key behaviors:
    - Normalizes empty strings (especially ipo="") into None
    - Batches inserts
    - Handles 429 with limited retries/backoff; can stop early to avoid wasting time
    """
    upserted_total = 0
    batch: list[dict] = []

    for i, sym in enumerate(symbols, start=1):
        sym = (sym or "").strip().upper()
        if not sym:
            continue

        retries_429 = 0

        while True:
            try:
                payload = fetch_profile(sym)
                normalized = _normalize_profile(payload, fallback_symbol=sym)

                # Finnhub sometimes returns {} for unknown symbols
                if normalized is None:
                    break

                batch.append(normalized)

                if len(batch) >= batch_size:
                    upserted_total += upsert_company_profiles(conn, batch)
                    batch = []

                if i % 50 == 0:
                    print(f"🏷️ Company profiles progress: {i}/{len(symbols)} symbols, upserted={upserted_total}")

                break  # success -> exit retry loop

            except requests.HTTPError as e:
                status = getattr(e.response, "status_code", None)

                if status == 429:
                    retries_429 += 1
                    print(f"⚠️ Profile 429 for {sym} (retry {retries_429}/{max_429_retries})")

                    # backoff a bit (simple linear backoff)
                    time.sleep(max(2.0, sleep_s) * retries_429)

                    if retries_429 <= max_429_retries:
                        continue

                    if stop_on_429:
                        print("⛔ Finnhub rate limit hit. Stopping early; rerun later to continue.")
                        # flush what we already have before stopping
                        if batch:
                            upserted_total += upsert_company_profiles(conn, batch)
                        return upserted_total

                    break  # keep going to next symbol if not stopping

                print(f"⚠️ Profile failed for {sym}: {e}")
                break

            except Exception as e:
                print(f"⚠️ Profile failed for {sym}: {e}")
                break

        time.sleep(sleep_s)

    if batch:
        upserted_total += upsert_company_profiles(conn, batch)

    return upserted_total
