from __future__ import annotations

import time
from typing import Optional

import requests

from src.db import upsert_symbol_map

BASE = "https://query2.finance.yahoo.com/v1/finance/search"


def yahoo_search(query: str) -> dict:
    params = {
        "q": query,
        "quotesCount": 10,
        "newsCount": 0,
        "listsCount": 0,
    }
    r = requests.get(
        BASE,
        params=params,
        timeout=30,
        headers={"User-Agent": "Mozilla/5.0"},
    )
    r.raise_for_status()
    return r.json() or {}


def best_yahoo_symbol_for_ipo_symbol(ipo_symbol: str) -> Optional[str]:
    """
    Try to find a likely Yahoo tradable symbol for an IPO symbol.
    Prefer exact-match equities on major exchanges.
    """
    data = yahoo_search(ipo_symbol)
    quotes = data.get("quotes") or []

    def score(q: dict) -> tuple:
        quote_type = (q.get("quoteType") or "").upper()
        exch = (q.get("exchange") or "").upper()
        sym = (q.get("symbol") or "").upper()

        exact = 1 if sym == ipo_symbol.upper() else 0
        is_equity = 1 if quote_type in ("EQUITY", "ETF") else 0
        is_us = 1 if exch in ("NYQ", "NMS", "NGM", "ASE") else 0
        return (exact, is_equity, is_us)

    quotes_sorted = sorted(quotes, key=score, reverse=True)

    for q in quotes_sorted:
        sym = q.get("symbol")
        quote_type = (q.get("quoteType") or "").upper()

        if not sym:
            continue

        # Only accept instruments likely to have daily prices
        if quote_type in ("EQUITY", "ETF"):
            return sym

    return None


def resolve_yahoo_and_upsert(conn, ipo_symbols: list[str], sleep_s: float = 0.8) -> tuple[int, int, int]:
    """
    Resolve Yahoo symbols and upsert into raw.symbol_map.

    IMPORTANT:
    raw.symbol_map.vendor_symbol is NOT NULL in your DB.
    For no-match cases we persist vendor_symbol = ipo_symbol and set is_priceable = false.
    """
    attempted = 0
    priceable = 0
    not_priceable = 0
    buffer: list[dict] = []

    def flush():
        nonlocal buffer
        if not buffer:
            return

        cleaned: list[dict] = []
        for r in buffer:
            ipo_sym = (r.get("ipo_symbol") or "").strip().upper()
            vendor_sym = (r.get("vendor_symbol") or "").strip().upper()

            if not ipo_sym:
                continue

            # satisfy NOT NULL vendor_symbol constraint
            if not vendor_sym:
                vendor_sym = ipo_sym
                r["vendor_symbol"] = vendor_sym
                r["is_priceable"] = False
                if not r.get("notes"):
                    r["notes"] = "yahoo_search:no_match"

            cleaned.append(
                {
                    "ipo_symbol": ipo_sym,
                    "vendor_symbol": vendor_sym,
                    "is_priceable": bool(r.get("is_priceable", False)),
                    "notes": r.get("notes"),
                }
            )

        if cleaned:
            upsert_symbol_map(conn, "yahoo", cleaned)

        buffer = []

    for sym in ipo_symbols:
        attempted += 1

        try:
            yahoo_sym = best_yahoo_symbol_for_ipo_symbol(sym)

            if yahoo_sym:
                buffer.append(
                    {
                        "ipo_symbol": sym,
                        "vendor_symbol": yahoo_sym,
                        "is_priceable": True,
                        "notes": "yahoo_search:match",
                    }
                )
                priceable += 1
            else:
                buffer.append(
                    {
                        "ipo_symbol": sym,
                        "vendor_symbol": sym,   # <- never NULL
                        "is_priceable": False,
                        "notes": "yahoo_search:no_match",
                    }
                )
                not_priceable += 1

        except Exception as e:
            buffer.append(
                {
                    "ipo_symbol": sym,
                    "vendor_symbol": sym,   # <- never NULL
                    "is_priceable": False,
                    "notes": f"yahoo_search:error:{str(e)[:200]}",
                }
            )
            not_priceable += 1

        if len(buffer) >= 200:
            flush()

        time.sleep(sleep_s)

    flush()
    return attempted, priceable, not_priceable