from __future__ import annotations

import os
import requests
from datetime import datetime

from src.db import upsert_symbol_map

BASE = "https://www.alphavantage.co/query"


def symbol_search(query: str) -> dict:
    api_key = os.getenv("ALPHAVANTAGE_API_KEY")
    if not api_key:
        raise RuntimeError("Missing ALPHAVANTAGE_API_KEY in environment")

    params = {"function": "SYMBOL_SEARCH", "keywords": query, "apikey": api_key}
    r = requests.get(BASE, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    if "Note" in data:
        raise RuntimeError(f"Alpha Vantage rate limit hit: {data['Note']}")

    return data


def resolve_and_upsert(conn, ipo_symbols: list[str], sleep_s: float = 12.0) -> tuple[int, int, int]:
    """
    For each ipo_symbol, attempt to resolve a vendor_symbol for Alpha Vantage.
    Returns (attempted, priceable, not_priceable)
    """
    attempted = 0
    priceable = 0
    not_priceable = 0
    rows = []

    for sym in ipo_symbols:
        attempted += 1
        vendor_symbol = sym
        is_priceable = True
        notes = None

        try:
            data = symbol_search(sym)
            matches = data.get("bestMatches", []) or []

            # Heuristic: pick exact symbol match if present
            exact = None
            for m in matches:
                if (m.get("1. symbol") or "").upper() == sym.upper():
                    exact = m
                    break

            pick = exact or (matches[0] if matches else None)

            if not pick:
                is_priceable = False
                notes = "No matches from SYMBOL_SEARCH"
            else:
                vendor_symbol = (pick.get("1. symbol") or sym).upper()
                # Optional: store some metadata
                notes = f"match={vendor_symbol} name={pick.get('2. name')} region={pick.get('4. region')}"

            rows.append(
                {
                    "ipo_symbol": sym.upper(),
                    "vendor_symbol": vendor_symbol.upper(),
                    "is_priceable": is_priceable,
                    "notes": notes,
                }
            )

            if is_priceable:
                priceable += 1
            else:
                not_priceable += 1

        except Exception as e:
            # Don't kill the run
            rows.append(
                {
                    "ipo_symbol": sym.upper(),
                    "vendor_symbol": sym.upper(),
                    "is_priceable": False,
                    "notes": f"resolve_error: {e}",
                }
            )
            not_priceable += 1

        if len(rows) >= 200:
            upsert_symbol_map(conn, "alphavantage", rows)
            rows.clear()

        # Sleep to respect free tier
        import time
        time.sleep(sleep_s)

    if rows:
        upsert_symbol_map(conn, "alphavantage", rows)

    return attempted, priceable, not_priceable
