from __future__ import annotations

from datetime import date, timedelta
import os
import time

from src.db import get_conn, start_run, finish_run

from src.ingest.finnhub_ipo_calendar import ingest_finnhub_ipos
from src.ingest.finnhub_company_profile import ingest_finnhub_company_profiles

from src.ingest.alphavantage_symbol_search import resolve_and_upsert as resolve_alpha_and_upsert
from src.ingest.alphavantage_prices import ingest_alpha_prices_for_symbol

from src.ingest.yahoo_symbol_search import resolve_yahoo_and_upsert
from src.ingest.yahoo_company_profile import ingest_yahoo_company_profiles
from src.ingest.yahoo_prices import ingest_yahoo_prices_for_symbol


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


# -----------------------
# Knobs
# -----------------------

IPO_FULL_BACKFILL = _env_bool("IPO_FULL_BACKFILL", False)

IPO_BACKFILL_START_YEAR = _env_int("IPO_BACKFILL_START_YEAR", 2014)
IPO_BACKFILL_END_YEAR = _env_int("IPO_BACKFILL_END_YEAR", date.today().year + 1)

IPO_CALENDAR_LOOKBACK_DAYS = _env_int("IPO_CALENDAR_LOOKBACK_DAYS", 365)
IPO_CALENDAR_LOOKAHEAD_DAYS = _env_int("IPO_CALENDAR_LOOKAHEAD_DAYS", 14)

IPO_RECENT_DAYS = _env_int("IPO_RECENT_DAYS", 120)
PRICE_WINDOW_DAYS = _env_int("IPO_PRICE_WINDOW_DAYS", 100)

FINNHUB_PROFILE_MAX_PER_RUN = _env_int("FINNHUB_PROFILE_MAX_PER_RUN", 30)
FINNHUB_PROFILE_SLEEP_SECONDS = _env_float("FINNHUB_PROFILE_SLEEP_SECONDS", 0.8)

YAHOO_PROFILE_MAX_PER_RUN = _env_int("YAHOO_PROFILE_MAX_PER_RUN", 50)
YAHOO_PROFILE_SLEEP_SECONDS = _env_float("YAHOO_PROFILE_SLEEP_SECONDS", 0.6)

ALPHA_MAX_RESOLVE_PER_RUN = _env_int("ALPHAVANTAGE_MAX_RESOLVE_PER_RUN", 50)
ALPHA_RESOLVE_SLEEP_SECONDS = _env_float("ALPHAVANTAGE_RESOLVE_SLEEP_SECONDS", 12.0)

YAHOO_MAX_RESOLVE_PER_RUN = _env_int("YAHOO_MAX_RESOLVE_PER_RUN", 80)
YAHOO_RESOLVE_SLEEP_SECONDS = _env_float("YAHOO_RESOLVE_SLEEP_SECONDS", 0.8)

MAX_TICKERS_PER_RUN = _env_int("MAX_TICKERS_PER_RUN", 40)
SLEEP_SECONDS_BETWEEN_TICKERS = _env_float("SLEEP_SECONDS_BETWEEN_TICKERS", 1.0)

PRICE_INCREMENTAL = _env_bool("PRICE_INCREMENTAL", True)

# IMPORTANT:
# For now we want Yahoo to be the primary practical price source.
ENABLE_YAHOO_PRICE_PRIMARY = _env_bool("ENABLE_YAHOO_PRICE_PRIMARY", True)
ENABLE_ALPHA_PRICE_FALLBACK = _env_bool("ENABLE_ALPHA_PRICE_FALLBACK", False)


# -----------------------
# Helpers
# -----------------------

def backfill_ipos_yearly(conn) -> int:
    total = 0
    for year in range(IPO_BACKFILL_START_YEAR, IPO_BACKFILL_END_YEAR + 1):
        n = ingest_finnhub_ipos(conn, date(year, 1, 1), date(year, 12, 31))
        total += n
        print(f"📥 IPO year {year}: {n} events")
    return total


def ingest_ipos_incremental(conn) -> int:
    d_from = date.today() - timedelta(days=IPO_CALENDAR_LOOKBACK_DAYS)
    d_to = date.today() + timedelta(days=IPO_CALENDAR_LOOKAHEAD_DAYS)
    n = ingest_finnhub_ipos(conn, d_from, d_to)
    print(f"📥 IPO incremental window: {d_from} -> {d_to} | events={n}")
    return n


def fetch_recent_equity_symbols(conn, recent_days: int) -> list[str]:
    """
    Recent IPO symbols that look like common-stock candidates.
    Conservative filter:
    - uppercase alnum tickers up to 5 chars
    - exclude common SPAC / warrant / right endings
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT upper(symbol) AS symbol
            FROM raw.ipo_events
            WHERE symbol IS NOT NULL
              AND symbol <> ''
              AND upper(symbol) ~ '^[A-Z0-9]{1,5}$'
              AND upper(symbol) !~ '.*(U|W|R)$'
              AND ipo_date >= (CURRENT_DATE - (%s || ' days')::interval)
            ORDER BY 1;
            """,
            (recent_days,),
        )
        return [r[0] for r in cur.fetchall()]


def fetch_symbols_missing_profile(conn, recent_days: int, limit_n: int) -> list[str]:
    """
    Symbols missing both industry and sector in current profile store.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.symbol
            FROM (
              SELECT DISTINCT upper(symbol) AS symbol
              FROM raw.ipo_events
              WHERE symbol IS NOT NULL
                AND symbol <> ''
                AND upper(symbol) ~ '^[A-Z0-9]{1,5}$'
                AND upper(symbol) !~ '.*(U|W|R)$'
                AND ipo_date >= (CURRENT_DATE - (%s || ' days')::interval)
            ) s
            LEFT JOIN (
              SELECT symbol,
                     MAX(NULLIF(industry, '')) AS industry_any,
                     MAX(NULLIF(sector, '')) AS sector_any
              FROM raw.company_profiles
              WHERE source IN ('finnhub', 'yahoo')
              GROUP BY symbol
            ) p
              ON p.symbol = s.symbol
            WHERE p.symbol IS NULL
               OR (p.industry_any IS NULL AND p.sector_any IS NULL)
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
              SELECT DISTINCT upper(symbol) AS symbol
              FROM raw.ipo_events
              WHERE symbol IS NOT NULL
                AND symbol <> ''
                AND upper(symbol) ~ '^[A-Z0-9]{1,5}$'
                AND upper(symbol) !~ '.*(U|W|R)$'
                AND ipo_date >= (CURRENT_DATE - (%s || ' days')::interval)
            ) s
            LEFT JOIN raw.symbol_map m
              ON m.vendor = %s
             AND m.ipo_symbol = s.symbol
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
            SELECT
                upper(e.symbol) AS ipo_symbol,
                m.vendor_symbol,
                e.ipo_date::date
            FROM raw.ipo_events e
            JOIN raw.symbol_map m
              ON m.vendor = %s
             AND m.ipo_symbol = upper(e.symbol)
            WHERE m.is_priceable = true
              AND m.vendor_symbol IS NOT NULL
              AND e.ipo_date IS NOT NULL
              AND e.ipo_date <= CURRENT_DATE
              AND upper(e.symbol) ~ '^[A-Z0-9]{1,5}$'
              AND upper(e.symbol) !~ '.*(U|W|R)$'
              AND e.ipo_date >= (CURRENT_DATE - (%s || ' days')::interval)
            GROUP BY upper(e.symbol), m.vendor_symbol, e.ipo_date
            ORDER BY e.ipo_date DESC
            LIMIT %s;
            """,
            (vendor, recent_days, limit_n),
        )
        return [(r[0], r[1], r[2]) for r in cur.fetchall()]


def fetch_symbols_with_any_profile(conn, recent_days: int) -> list[str]:
    """
    Optional helper if later you want to prioritize only symbols with profile info.
    Currently not used directly but useful for future tightening.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT upper(e.symbol) AS symbol
            FROM raw.ipo_events e
            JOIN raw.company_profiles p
              ON p.symbol = upper(e.symbol)
            WHERE e.ipo_date >= (CURRENT_DATE - (%s || ' days')::interval)
              AND upper(e.symbol) ~ '^[A-Z0-9]{1,5}$'
              AND upper(e.symbol) !~ '.*(U|W|R)$'
              AND p.source IN ('finnhub', 'yahoo')
            ORDER BY 1;
            """,
            (recent_days,),
        )
        return [r[0] for r in cur.fetchall()]


# -----------------------
# Main
# -----------------------

def main():
    conn = get_conn()
    run_id = start_run(conn, "ipo_ingestion")

    try:
        # 1) IPO calendar
        if IPO_FULL_BACKFILL:
            n_ipos = backfill_ipos_yearly(conn)
        else:
            n_ipos = ingest_ipos_incremental(conn)

        # 2) Profiles
        to_profile_fh = fetch_symbols_missing_profile(conn, IPO_RECENT_DAYS, FINNHUB_PROFILE_MAX_PER_RUN)
        n_fh_profiles = 0
        if to_profile_fh:
            n_fh_profiles = ingest_finnhub_company_profiles(
                conn,
                to_profile_fh,
                sleep_s=FINNHUB_PROFILE_SLEEP_SECONDS,
            )
        print(f"🏷️ Finnhub profiles upserted: {n_fh_profiles} (attempted={len(to_profile_fh)})")

        to_profile_yh = fetch_symbols_missing_profile(conn, IPO_RECENT_DAYS, YAHOO_PROFILE_MAX_PER_RUN)
        n_yh_profiles = 0
        if to_profile_yh:
            n_yh_profiles = ingest_yahoo_company_profiles(
                conn,
                to_profile_yh,
                sleep_s=YAHOO_PROFILE_SLEEP_SECONDS,
            )
        print(f"🏷️ Yahoo profiles upserted: {n_yh_profiles} (attempted={len(to_profile_yh)})")

        # 3) Symbol resolution
        # Alpha stays available, but no longer blocks pricing.
        unresolved_alpha = fetch_unresolved_symbols_recent(
            conn,
            "alphavantage",
            IPO_RECENT_DAYS,
            ALPHA_MAX_RESOLVE_PER_RUN,
        )
        alpha_attempted = alpha_priceable = alpha_not_priceable = 0
        if unresolved_alpha:
            alpha_attempted, alpha_priceable, alpha_not_priceable = resolve_alpha_and_upsert(
                conn,
                unresolved_alpha,
                sleep_s=ALPHA_RESOLVE_SLEEP_SECONDS,
            )
            print(
                f"🧭 Alpha mapping: attempted={alpha_attempted}, "
                f"priceable={alpha_priceable}, not_priceable={alpha_not_priceable}"
            )
        else:
            print("🧭 Alpha mapping: nothing new to resolve")

        unresolved_yahoo = fetch_unresolved_symbols_recent(
            conn,
            "yahoo",
            IPO_RECENT_DAYS,
            YAHOO_MAX_RESOLVE_PER_RUN,
        )
        yh_attempted = yh_priceable = yh_not_priceable = 0
        if unresolved_yahoo:
            yh_attempted, yh_priceable, yh_not_priceable = resolve_yahoo_and_upsert(
                conn,
                unresolved_yahoo,
                sleep_s=YAHOO_RESOLVE_SLEEP_SECONDS,
            )
            print(
                f"🧭 Yahoo mapping: attempted={yh_attempted}, "
                f"priceable={yh_priceable}, not_priceable={yh_not_priceable}"
            )
        else:
            print("🧭 Yahoo mapping: nothing new to resolve")

        # 4) Prices
        # NEW STRATEGY:
        # - Yahoo is primary
        # - Alpha is optional fallback
        yahoo_pairs = fetch_priceable_pairs_recent(conn, "yahoo", IPO_RECENT_DAYS, MAX_TICKERS_PER_RUN)
        alpha_pairs = fetch_priceable_pairs_recent(conn, "alphavantage", IPO_RECENT_DAYS, MAX_TICKERS_PER_RUN)

        alpha_by_ipo: dict[str, tuple[str, date]] = {}
        for ipo_symbol, vendor_symbol, ipo_dt in alpha_pairs:
            alpha_by_ipo.setdefault(ipo_symbol, (vendor_symbol, ipo_dt))

        today = date.today()
        attempted = 0
        total_rows = 0
        with_data = 0
        no_data = 0
        failed = 0
        yahoo_used = 0
        alpha_used = 0

        for ipo_symbol, yahoo_symbol, ipo_dt in yahoo_pairs:
            if attempted >= MAX_TICKERS_PER_RUN:
                break

            attempted += 1
            price_from = ipo_dt
            price_to = min(ipo_dt + timedelta(days=PRICE_WINDOW_DAYS), today)

            #SKIP IPOs that have not happened yet
            if price_from > price_to:
                no_data += 1
                continue

            inserted_total_for_ipo = 0

            # --- Yahoo primary ---
            if ENABLE_YAHOO_PRICE_PRIMARY:
                try:
                    inserted_yh = ingest_yahoo_prices_for_symbol(
                        conn=conn,
                        symbol=yahoo_symbol,
                        date_from=price_from,
                        date_to=price_to,
                        incremental=PRICE_INCREMENTAL,
                    )
                    inserted_total_for_ipo += inserted_yh
                    yahoo_used += 1
                except Exception as e:
                    failed += 1
                    print(f"⚠️ Yahoo prices failed for {ipo_symbol}->{yahoo_symbol} ({ipo_dt}): {e}")
                    conn.rollback()

            # --- Alpha fallback only if Yahoo inserted nothing ---
            if ENABLE_ALPHA_PRICE_FALLBACK and inserted_total_for_ipo == 0:
                alpha_match = alpha_by_ipo.get(ipo_symbol)
                if alpha_match and alpha_match[0]:
                    alpha_symbol = alpha_match[0]
                    try:
                        inserted_alpha = ingest_alpha_prices_for_symbol(
                            conn=conn,
                            symbol=alpha_symbol,
                            date_from=price_from,
                            date_to=price_to,
                            incremental=PRICE_INCREMENTAL,
                        )
                        inserted_total_for_ipo += inserted_alpha
                        alpha_used += 1
                    except Exception as e:
                        failed += 1
                        msg = str(e)
                        print(f"⚠️ Alpha prices failed for {ipo_symbol}->{alpha_symbol} ({ipo_dt}): {msg}")

                        # Do not break the whole run anymore
                        conn.rollback()

            total_rows += inserted_total_for_ipo

            if inserted_total_for_ipo > 0:
                with_data += 1
            else:
                no_data += 1

            if attempted % 10 == 0:
                print(
                    f"📈 Prices progress: {attempted}/{min(len(yahoo_pairs), MAX_TICKERS_PER_RUN)} | "
                    f"rows={total_rows} | with_data={with_data} | no_data={no_data} | failed={failed} | "
                    f"yahoo_used={yahoo_used} | alpha_used={alpha_used}"
                )

            time.sleep(SLEEP_SECONDS_BETWEEN_TICKERS)

        finish_run(
            conn,
            run_id,
            "success",
            notes=(
                f"ipos={n_ipos}, "
                f"fh_profiles={n_fh_profiles}, yh_profiles={n_yh_profiles}, "
                f"alpha_map_attempted={alpha_attempted}, alpha_map_priceable={alpha_priceable}, alpha_map_not_priceable={alpha_not_priceable}, "
                f"yahoo_map_attempted={yh_attempted}, yahoo_map_priceable={yh_priceable}, yahoo_map_not_priceable={yh_not_priceable}, "
                f"prices_tickers={attempted}, prices_rows={total_rows}, "
                f"with_data={with_data}, no_data={no_data}, failed={failed}, "
                f"yahoo_used={yahoo_used}, alpha_used={alpha_used}"
            ),
        )

        print(
            "✅ Ingestion ok: "
            f"ipos={n_ipos}, fh_profiles={n_fh_profiles}, yh_profiles={n_yh_profiles}, "
            f"prices_tickers={attempted}, rows={total_rows}, with_data={with_data}, no_data={no_data}, failed={failed}, "
            f"yahoo_used={yahoo_used}, alpha_used={alpha_used}"
        )

    except Exception as e:
        conn.rollback()
        try:
            finish_run(conn, run_id, "failed", notes=str(e))
        except Exception:
            conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()