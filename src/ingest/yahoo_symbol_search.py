from __future__ import annotations

import time
from typing import Optional, List

import requests

from src.db import upsert_symbol_map

BASE = "https://query2.finance.yahoo.com/v1/finance/search"


def yahoo_search(query: str) -> dict:
    params = {"q": query, "quotesCount": 10, "newsCount": 0, "listsCount": 0}
    r = requests.get(BASE, params=params, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return r.json() or {}


def best_yahoo_symbol_for_ipo_symbol(ipo_symbol: str) -> Optional[str]:
    data = yahoo_search(ipo_symbol)
    quotes = data.get("quotes") or []

    def score(q: dict) -> tuple:
        qt = (q.get("quoteType") or "").upper()
        exch = (q.get("exchange") or "").upper()
        sym = (q.get("symbol") or "").upper()
        exact = 1 if sym == ipo_symbol.upper() else 0
        is_equity = 1 if qt in ("EQUITY", "ETF") else 0
        is_us = 1 if exch in ("NYQ", "NMS", "NGM", "ASE") else 0
        return (exact, is_equity, is_us)

    for q in sorted(quotes, key=score, reverse=True):
        qt = (q.get("quoteType") or "").upper()
        sym = q.get("symbol")
        if sym and qt in ("EQUITY", "ETF"):
            return sym

    return None


def resolve_yahoo_and_upsert(conn, ipo_symbols: List[str], sleep_s: float = 0.8) -> tuple[int, int, int]:
    """
    Uses your db.upsert_symbol_map(conn, vendor, rows).
    rows: [{ipo_symbol, vendor_symbol, is_priceable, notes}]
    """
    attempted = 0
    priceable = 0
    not_priceable = 0
    buffer: list[dict] = []

    def flush():
        nonlocal buffer
        if buffer:
            upsert_symbol_map(conn, "yahoo", buffer)
            buffer = []

    for sym in ipo_symbols:
        attempted += 1
        try:
            yahoo_sym = best_yahoo_symbol_for_ipo_symbol(sym)
            if yahoo_sym:
                buffer.append(
                    {"ipo_symbol": sym, "vendor_symbol": yahoo_sym, "is_priceable": True, "notes": "yahoo_search"}
                )
                priceable += 1
            else:
                buffer.append(
                    {"ipo_symbol": sym, "vendor_symbol": None, "is_priceable": False, "notes": "yahoo_search:no_match"}
                )
                not_priceable += 1

            if len(buffer) >= 200:
                flush()

        except Exception as e:
            buffer.append(
                {"ipo_symbol": sym, "vendor_symbol": None, "is_priceable": False, "notes": f"yahoo_search:error:{e}"}
            )
            not_priceable += 1

        time.sleep(sleep_s)

    flush()
    return attempted, priceable, not_priceable
