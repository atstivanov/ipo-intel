from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from typing import Optional

import yfinance as yf

from src.db import upsert_company_profiles


def _clean_str(v) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() in {"none", "null", "n/a"}:
        return None
    return s


def _clean_num(v) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _fetch_yahoo_info_one(symbol: str) -> dict:
    """
    Pulls company metadata via yfinance.
    This avoids direct calls to Yahoo quoteSummary that often 401.
    """
    t = yf.Ticker(symbol)

    # get_info() is the stable API; .info is sometimes cached/lazy and can behave oddly
    info = t.get_info() or {}

    # If symbol invalid, yfinance often returns {} or missing longName
    # We still return {} so caller can decide.
    return info


def _call_with_timeout(fn, timeout_s: float):
    """
    Hard timeout wrapper so one Yahoo request can't hang the whole run.
    """
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(fn)
        return fut.result(timeout=timeout_s)


def ingest_yahoo_company_profiles(
    conn,
    symbols: list[str],
    sleep_s: float = 0.6,
    timeout_s: float = 10.0,
    batch_size: int = 25,
) -> int:
    """
    Fetch Yahoo profiles via yfinance and upsert them into raw.company_profiles.

    - No Yahoo API key required.
    - Uses a hard per-symbol timeout to prevent long hangs.
    - Upserts in batches to keep DB ops efficient.
    """
    upserted_total = 0
    batch: list[dict] = []

    empty_payloads = 0
    failed = 0
    timed_out = 0

    for i, sym in enumerate(symbols, start=1):
        try:
            info = _call_with_timeout(lambda: _fetch_yahoo_info_one(sym), timeout_s=timeout_s)

            if not info:
                empty_payloads += 1
                time.sleep(sleep_s)
                continue

            # Map yfinance fields -> our profile record
            # Keep fields that your db.py upsert_company_profiles likely supports
            payload = {
                "source": "yahoo",
                "symbol": sym,
                "name": _clean_str(info.get("longName") or info.get("shortName") or info.get("displayName")),
                "exchange": _clean_str(info.get("exchange") or info.get("fullExchangeName")),
                "country": _clean_str(info.get("country")),
                "industry": _clean_str(info.get("industry")),
                "sector": _clean_str(info.get("sector")),
                "market_cap": _clean_num(info.get("marketCap")),
                "currency": _clean_str(info.get("currency")),
                "weburl": _clean_str(info.get("website")),
                "logo": None,  # yfinance info doesn't always provide a logo reliably
                "raw_json": info,  # db.py will json.dumps it
            }

            # If we got neither sector nor industry nor name, treat as useless
            if not payload.get("industry") and not payload.get("sector") and not payload.get("name"):
                empty_payloads += 1
                time.sleep(sleep_s)
                continue

            batch.append(payload)

            if len(batch) >= batch_size:
                upserted_total += upsert_company_profiles(conn, batch)
                batch = []

            if i % 25 == 0:
                print(
                    f"🏷️ Yahoo profiles progress: {i}/{len(symbols)} | "
                    f"upserted={upserted_total} | empty={empty_payloads} | timeouts={timed_out} | failed={failed}"
                )

        except FuturesTimeoutError:
            timed_out += 1
            print(f"⚠️ Yahoo profile timeout for {sym} after {timeout_s}s (skipping)")
            # keep going
        except Exception as e:
            failed += 1
            print(f"⚠️ Yahoo profile failed for {sym}: {e}")
            # keep going

        time.sleep(sleep_s)

    if batch:
        upserted_total += upsert_company_profiles(conn, batch)

    print(
        f"🏷️ Yahoo profiles done: upserted={upserted_total} | empty={empty_payloads} | "
        f"timeouts={timed_out} | failed={failed}"
    )
    return upserted_total
