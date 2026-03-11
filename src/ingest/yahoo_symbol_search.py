from __future__ import annotations

import time
from typing import Optional

import requests

from src.db import upsert_symbol_map

BASE = "https://query2.finance.yahoo.com/v1/finance/search"

# Keep resolution conservative for this capstone:
# we only want likely US tradable common-stock style tickers.
US_EXCHANGES = {
    "NYQ",      # NYSE
    "NMS",      # NASDAQ Global Select
    "NGM",      # NASDAQ Global Market
    "NCM",      # NASDAQ Capital Market
    "ASE",      # AMEX / NYSE American
    "NYSE",
    "NASDAQ",
    "AMEX",
}

ALLOWED_QUOTE_TYPES = {"EQUITY", "ETF"}

# Reject obvious bad / foreign / derivative patterns
DISALLOWED_SUBSTRINGS = (
    ".",        # foreign suffixes like .T .MI .L .DE
    "=F",       # futures
    "^",        # indexes
    "-UN",      # units
    "-U",
    "-WT",      # warrants
    "-WS",
    "-RT",      # rights
    "-P",       # preferred / odd structured
)

# Extra hard blocks for suspicious forms frequently returned by Yahoo
DISALLOWED_ENDINGS = (
    "W",
    "R",
)

USER_AGENT = "Mozilla/5.0"


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
        headers={"User-Agent": USER_AGENT},
    )
    r.raise_for_status()
    return r.json() or {}


def _clean_symbol(sym: str | None) -> str:
    return (sym or "").strip().upper()


def _looks_like_us_equity_symbol(sym: str) -> bool:
    """
    Very conservative symbol acceptance for this project.
    We only want simple US-style symbols like ARM, CAST, BOBS, etc.
    """
    sym = _clean_symbol(sym)
    if not sym:
        return False

    for bad in DISALLOWED_SUBSTRINGS:
        if bad in sym:
            return False

    if not sym.isalnum():
        return False

    if len(sym) < 1 or len(sym) > 5:
        return False

    # Reject obvious SPAC-ish / rights / warrant-style endings
    # (your upstream IPO filters already do some of this, but keep it here too)
    if len(sym) >= 2 and sym.endswith(DISALLOWED_ENDINGS):
        return False

    return True


def _is_good_candidate(q: dict, ipo_symbol: str) -> bool:
    sym = _clean_symbol(q.get("symbol"))
    exch = _clean_symbol(q.get("exchange"))
    quote_type = _clean_symbol(q.get("quoteType"))

    if not sym:
        return False
    if quote_type not in ALLOWED_QUOTE_TYPES:
        return False
    if exch not in US_EXCHANGES:
        return False
    if not _looks_like_us_equity_symbol(sym):
        return False

    return True


def _score_candidate(q: dict, ipo_symbol: str) -> tuple:
    """
    Higher is better.
    Prioritize:
      1) exact symbol match
      2) US exchange
      3) EQUITY over ETF
      4) shorter/simple symbol
    """
    sym = _clean_symbol(q.get("symbol"))
    exch = _clean_symbol(q.get("exchange"))
    quote_type = _clean_symbol(q.get("quoteType"))

    exact = 1 if sym == _clean_symbol(ipo_symbol) else 0
    us = 1 if exch in US_EXCHANGES else 0
    equity = 1 if quote_type == "EQUITY" else 0
    short = -len(sym) if sym else -99

    return (exact, us, equity, short)


def best_yahoo_symbol_for_ipo_symbol(ipo_symbol: str) -> Optional[str]:
    """
    Return a conservative Yahoo symbol match or None.
    """
    ipo_symbol = _clean_symbol(ipo_symbol)
    if not _looks_like_us_equity_symbol(ipo_symbol):
        return None

    data = yahoo_search(ipo_symbol)
    quotes = data.get("quotes") or []

    # keep only safe candidates
    candidates = [q for q in quotes if _is_good_candidate(q, ipo_symbol)]

    if not candidates:
        return None

    candidates = sorted(
        candidates,
        key=lambda q: _score_candidate(q, ipo_symbol),
        reverse=True,
    )

    best = _clean_symbol(candidates[0].get("symbol"))

    # Strong rule: accept exact match only.
    # This avoids dangerous mismatches like BAO -> BZUN.
    if best == ipo_symbol:
        return best

    return None


def resolve_yahoo_and_upsert(conn, ipo_symbols: list[str], sleep_s: float = 0.8) -> tuple[int, int, int]:
    """
    Resolve Yahoo symbols and upsert into raw.symbol_map.

    IMPORTANT:
    - raw.symbol_map.vendor_symbol is NOT NULL
    - for no-match rows, persist vendor_symbol = ipo_symbol and is_priceable = false
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
            ipo_sym = _clean_symbol(r.get("ipo_symbol"))
            vendor_sym = _clean_symbol(r.get("vendor_symbol"))

            if not ipo_sym:
                continue

            # satisfy NOT NULL constraint
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
        sym = _clean_symbol(sym)

        try:
            yahoo_sym = best_yahoo_symbol_for_ipo_symbol(sym)

            if yahoo_sym:
                buffer.append(
                    {
                        "ipo_symbol": sym,
                        "vendor_symbol": yahoo_sym,
                        "is_priceable": True,
                        "notes": "yahoo_search:exact_us_match",
                    }
                )
                priceable += 1
            else:
                buffer.append(
                    {
                        "ipo_symbol": sym,
                        "vendor_symbol": sym,   # never NULL
                        "is_priceable": False,
                        "notes": "yahoo_search:no_match",
                    }
                )
                not_priceable += 1

        except Exception as e:
            buffer.append(
                {
                    "ipo_symbol": sym,
                    "vendor_symbol": sym,   # never NULL
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