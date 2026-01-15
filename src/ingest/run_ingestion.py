from __future__ import annotations

from datetime import date, timedelta
import os
import time

from src.db import get_conn, start_run, finish_run
from src.ingest.finnhub_ipo_calendar import ingest_finnhub_ipos
from src.ingest.finnhub_company_profile import ingest_finnhub_company_profiles
from src.ingest.alphavantage_symbol_search import resolve_and_upsert
from src.ingest.alphavantage_prices import ingest_alpha_prices_for_symbol


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v and v.strip() else default


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    return float(v) if v and v.strip() else default


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


# ===== knobs =====
IPO_BACKFILL_START_YEAR = _env_int("IPO_BACKFILL_START_YEAR", 2014)
IPO_BACKFILL_END_YEAR = _env_int("IPO_BACKFILL_END_YEAR", date.today().year + 1)

IPO_RECENT_DAYS = _env_int("IPO_RECENT_DAYS", 120)
PRICE_WINDOW_DAYS = _env_int("IPO_PRICE_WINDOW_DAYS", 100)

# Finnhub profiles
FINNHUB_PROFILE_MAX_PER_RUN = _env_int("FINNHUB_PROFILE_MAX_PER_RUN", 30)
FINNHUB_PROFILE_SLEEP_SECONDS = _env_float("FINNHUB_PROFILE_SLEEP_SECONDS", 0.8)

# Alpha symbol resolving
MAX_RESOLVE_PER_RUN = _env_int("ALPHAVANTAGE_MAX_RESOLVE_PER_RUN", 50)
RESOLVE_SLEEP_SECONDS = _env_float("ALPHAVANTAGE_RESOLVE_SLEEP_SECONDS", 12.0)

# Alpha pricing
MAX_TICKERS_PER_RUN = _env_int("ALPHAVANTAGE_MAX_TICKERS_PER_RUN", 25)
SLEEP_SECONDS_BETWEEN_TICKERS = _env_float("ALPHAVANTAGE_SLEEP_SECONDS", 15.0)
PRICE_INCREMENTAL = _env_bool("PRICE_INCREMENTAL", True)


def backfill_ipos_yearly(conn) -> int:
    total = 0
    for year in range(IPO_BACKFILL_START_YEAR, IPO_BACKFILL_END_YEAR + 1):
        n = ingest_finnhub_ipos(conn, date(year, 1, 1), date(year, 12, 31))
        total += n
        print(f"📥 IPO year {year}: {n} events")
    return total


def fetch_recent_equity_candidate_symbols(conn, recent_days: int) -> list[str]:
    """
    Recent IPO symbols that look like common-stock candidates.
    We explicitly exclude SPAC-like suffixes U/W/R.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT symbol
            FROM raw.ipo_events
            WHERE symbol IS NOT NULL
              AND symbol <> ''
              AND symbol ~ '^[A-Z]{1,5}$'
              AND symbol !~ '.*(U|W|R)$'
              AND ipo_date >= (CURRENT_DATE - (%s || ' days')::interval)
            ORDER BY symbol;
            """,
            (recent_days,),
        )
        return [r[0] for r in cur.fetchall()]


def fetch_symbols_missing_profile(conn, recent_days: int, limit_n: int) -> list[str]:
    """
    Only fetch profiles for symbols that are missing profiles
    OR missing industry/sector (i.e. still not enriched).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.symbol
            FROM (
              SELECT DISTINCT symbol
              FROM raw.ipo_events
              WHERE symbol IS NOT NULL
                AND symbol <> ''
                AND symbol ~ '^[A-Z]{1,5}$'
                AND symbol !~ '.*(U|W|R)$'
                AND ipo_date >= (CURRENT_DATE - (%s || ' days')::interval)
            ) s
            LEFT JOIN raw.company_profiles p
              ON p.symbol = s.symbol
            WHERE p.symbol IS NULL
               OR NULLIF(p.industry,'') IS NULL
            ORDER BY s.symbol
            LIMIT %s;
            """,
            (recent_days, limit_n),
        )
        return [r[0] for r in cur.fetchall()]


def fetch_unresolved_symbols_recent(conn, vendor: str, recent_days: int, limit_n: int) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.symbol
            FROM (
              SELECT DISTINCT symbol
              FROM raw.ipo_events
              WHERE symbol IS NOT NULL
                AND symbol <> ''
                AND symbol ~ '^[A-Z]{1,5}$'
                AND symbol !~ '.*(U|W|R)$'
                AND ipo_date >= (CURRENT_DATE - (%s || ' days')::interval)
            ) s
            LEFT JOIN raw.symbol_map m
              ON m.vendor = %s AND m.ipo_symbol = s.symbol
            WHERE m.ipo_symbol IS NULL
            ORDER BY s.symbol
            LIMIT %s;
            """,
            (recent_days, vendor, limit_n),
        )
        return [r[0] for r in cur.fetchall()]


def fetch_priceable_pairs_recent(conn, vendor: str, recent_days: int, limit_n: int) -> list[tuple[str, str, date]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT e.symbol AS ipo_symbol, m.vendor_symbol, e.ipo_date
            FROM raw.ipo_events e
            JOIN raw.symbol_map m
              ON m.vendor = %s AND m.ipo_symbol = e.symbol
            WHERE m.is_priceable = true
              AND e.ipo_date IS NOT NULL
              AND e.symbol ~ '^[A-Z]{1,5}$'
              AND e.symbol !~ '.*(U|W|R)$'
              AND e.ipo_date >= (CURRENT_DATE - (%s || ' days')::interval)
            GROUP BY e.symbol, m.vendor_symbol, e.ipo_date
            ORDER BY e.ipo_date DESC
            LIMIT %s;
            """,
            (vendor, recent_days, limit_n),
        )
        return [(r[0], r[1], r[2]) for r in cur.fetchall()]


def main():
    conn = get_conn()
    run_id = start_run(conn, "ipo_ingestion")

    try:
        # 1) IPO backfill
        n_ipos = backfill_ipos_yearly(conn)

        # 2) Company profiles (incremental, capped)
        to_profile = fetch_symbols_missing_profile(conn, IPO_RECENT_DAYS, FINNHUB_PROFILE_MAX_PER_RUN)
        n_profiles = 0
        if to_profile:
            n_profiles = ingest_finnhub_company_profiles(conn, to_profile, sleep_s=FINNHUB_PROFILE_SLEEP_SECONDS)
        print(f"🏷️ Company profiles upserted: {n_profiles} (attempted={len(to_profile)})")

        # 3) Resolve Alpha symbols for equity candidates (capped)
        unresolved = fetch_unresolved_symbols_recent(conn, "alphavantage", IPO_RECENT_DAYS, MAX_RESOLVE_PER_RUN)
        if unresolved:
            attempted, priceable, not_priceable = resolve_and_upsert(conn, unresolved, sleep_s=RESOLVE_SLEEP_SECONDS)
            print(f"🧭 Symbol resolve: attempted={attempted}, priceable={priceable}, not_priceable={not_priceable}")
        else:
            print("🧭 Symbol resolve: nothing new to resolve")

        # 4) Prices for mapped+priceable equity candidates (capped)
        pairs = fetch_priceable_pairs_recent(conn, "alphavantage", IPO_RECENT_DAYS, MAX_TICKERS_PER_RUN)

        today = date.today()
        total_rows = 0
        attempted = 0
        with_data = 0
        no_data = 0
        failed = 0

        for ipo_symbol, vendor_symbol, ipo_dt in pairs:
            attempted += 1
            price_from = ipo_dt
            price_to = min(ipo_dt + timedelta(days=PRICE_WINDOW_DAYS), today)

            try:
                inserted = ingest_alpha_prices_for_symbol(
                    conn=conn,
                    symbol=vendor_symbol,
                    date_from=price_from,
                    date_to=price_to,
                    incremental=PRICE_INCREMENTAL,
                )
                total_rows += inserted
                if inserted > 0:
                    with_data += 1
                else:
                    no_data += 1

            except Exception as e:
                failed += 1
                msg = str(e)
                print(f"⚠️ Prices failed for {ipo_symbol}->{vendor_symbol} ({ipo_dt}): {msg}")

                low = msg.lower()
                if "rate limit" in low or "call frequency" in low:
                    print("⛔ Stopping early due to Alpha Vantage rate limit. Tune sleeps/chunks or rerun later.")
                    break

                # IMPORTANT: prevent one failure from poisoning the whole run
                conn.rollback()

            if attempted % 10 == 0:
                print(
                    f"📈 Prices progress: {attempted}/{len(pairs)} tickers | "
                    f"rows={total_rows} | with_data={with_data} | no_data={no_data} | failed={failed}"
                )

            time.sleep(SLEEP_SECONDS_BETWEEN_TICKERS)

        finish_run(
            conn,
            run_id,
            "success",
            notes=(
                f"ipos={n_ipos}, profiles_upserted={n_profiles}, "
                f"prices_tickers={attempted}, prices_rows={total_rows}, "
                f"with_data={with_data}, no_data={no_data}, failed={failed}"
            ),
        )
        print(
            f"✅ Ingestion ok: IPO events={n_ipos}, tickers={attempted}, "
            f"daily prices rows={total_rows}, with_data={with_data}, no_data={no_data}, failed={failed}"
        )

    except Exception as e:
        conn.rollback()
        # finish_run must not be called inside an aborted transaction
        try:
            finish_run(conn, run_id, "failed", notes=str(e))
        except Exception:
            conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
