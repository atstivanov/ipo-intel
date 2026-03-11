from __future__ import annotations

import time
import requests

from src.config import FINNHUB_API_KEY
from src.db import upsert_company_profiles

BASE = "https://finnhub.io/api/v1/stock/profile2"


def fetch_profile(symbol: str) -> dict:
    if not FINNHUB_API_KEY:
        raise RuntimeError("Missing FINNHUB_API_KEY in environment (.env)")

    params = {"symbol": symbol, "token": FINNHUB_API_KEY}
    r = requests.get(BASE, params=params, timeout=30)

    # Explicit 429 handling so you see it clearly
    if r.status_code == 429:
        raise requests.HTTPError("429 Too Many Requests", response=r)

    r.raise_for_status()
    return r.json() or {}


def ingest_finnhub_company_profiles(conn, symbols: list[str], sleep_s: float = 0.8, batch_size: int = 50) -> int:
    """
    Fetch Finnhub profiles for a list of symbols and upsert them.

    Improvements vs your previous version:
    - logs empty payloads ({}), so "upserted=0" becomes explainable
    - stops early on 429
    - writes `source='finnhub'` explicitly (helps precedence later)
    """
    upserted_total = 0
    batch: list[dict] = []

    empty_payloads = 0
    failed = 0

    for i, sym in enumerate(symbols, start=1):
        try:
            payload = fetch_profile(sym)

            # Finnhub sometimes returns {} for unknown / unsupported tickers
            ticker = payload.get("ticker") if isinstance(payload, dict) else None
            if not payload or not ticker:
                empty_payloads += 1
                if i % 10 == 0:
                    print(f"🏷️ Finnhub profiles: empty_payloads={empty_payloads} so far (example symbol={sym})")
                time.sleep(sleep_s)
                continue

            # Normalize into our storage shape
            payload_out = dict(payload)
            payload_out["source"] = "finnhub"
            payload_out["symbol"] = ticker or sym

            batch.append(payload_out)

            if len(batch) >= batch_size:
                upserted_total += upsert_company_profiles(conn, batch)
                batch = []

            if i % 25 == 0:
                print(
                    f"🏷️ Finnhub profiles progress: {i}/{len(symbols)} | "
                    f"upserted={upserted_total} | empty={empty_payloads} | failed={failed}"
                )

        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                print(f"⚠️ Finnhub rate limit (429). Stopping early. Progress: {i}/{len(symbols)}")
                break
            failed += 1
            print(f"⚠️ Finnhub profile failed for {sym}: {e}")
        except Exception as e:
            failed += 1
            print(f"⚠️ Finnhub profile failed for {sym}: {e}")

        time.sleep(sleep_s)

    if batch:
        upserted_total += upsert_company_profiles(conn, batch)

    print(f"🏷️ Finnhub profiles done: upserted={upserted_total} | empty={empty_payloads} | failed={failed}")
    return upserted_total
