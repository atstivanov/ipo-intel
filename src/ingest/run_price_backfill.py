from __future__ import annotations

from datetime import date
import os
import time

from src.db import get_conn, start_run, finish_run
from src.ingest.yahoo_prices import ingest_yahoo_prices_for_symbol
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


BACKFILL_MAX_TICKERS = _env_int("BACKFILL_MAX_TICKERS", 30)
BACKFILL_SLEEP_SECONDS = _env_float("BACKFILL_SLEEP_SECONDS", 0.5)
BACKFILL_PRICE_WINDOW_DAYS = _env_int("IPO_PRICE_WINDOW_DAYS", 100)

# Keep this focused on recent cohort recovery
BACKFILL_RECENT_DAYS = _env_int("IPO_RECENT_DAYS", 120)

# Practical current setup: Yahoo primary, Alpha optional
BACKFILL_USE_YAHOO = _env_bool("BACKFILL_USE_YAHOO", True)
BACKFILL_USE_ALPHA_FALLBACK = _env_bool("BACKFILL_USE_ALPHA_FALLBACK", False)

# Prioritize incomplete but potentially recoverable IPOs
TARGET_STATUSES = (
    "mapped_but_no_prices",
    "very_low_coverage",
    "partial_coverage",
)


def fetch_backfill_candidates(conn, limit_n: int) -> list[dict]:
    """
    Uses analytics_analytics.ipo_coverage as the driver table.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                c.ipo_symbol,
                c.price_symbol,
                c.price_vendor,
                c.ipo_date::date,
                c.coverage_status,
                COALESCE(c.distinct_price_days, 0) AS distinct_price_days
            FROM analytics_analytics.ipo_coverage c
            WHERE c.coverage_status IN %s
              AND c.ipo_date <= CURRENT_DATE
              AND c.ipo_date >= CURRENT_DATE - (%s || ' days')::interval
              AND c.price_symbol IS NOT NULL
            ORDER BY
                CASE c.coverage_status
                    WHEN 'mapped_but_no_prices' THEN 1
                    WHEN 'very_low_coverage' THEN 2
                    WHEN 'partial_coverage' THEN 3
                    ELSE 99
                END,
                c.ipo_date DESC
            LIMIT %s;
            """,
            (TARGET_STATUSES, BACKFILL_RECENT_DAYS, limit_n),
        )
        rows = cur.fetchall()

    return [
        {
            "ipo_symbol": r[0],
            "price_symbol": r[1],
            "price_vendor": r[2],
            "ipo_date": r[3],
            "coverage_status": r[4],
            "distinct_price_days": r[5],
        }
        for r in rows
    ]


def fetch_alpha_symbol(conn, ipo_symbol: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT vendor_symbol
            FROM raw.symbol_map
            WHERE vendor = 'alphavantage'
              AND ipo_symbol = %s
              AND is_priceable = true
            LIMIT 1;
            """,
            (ipo_symbol,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def main():
    conn = get_conn()
    run_id = start_run(conn, "ipo_price_backfill")

    try:
        candidates = fetch_backfill_candidates(conn, BACKFILL_MAX_TICKERS)

        print(f"🔎 Backfill candidates selected: {len(candidates)}")

        attempted = 0
        with_data = 0
        no_data = 0
        failed = 0
        yahoo_used = 0
        alpha_used = 0
        total_rows = 0

        today = date.today()

        for c in candidates:
            ipo_symbol = c["ipo_symbol"]
            price_symbol = c["price_symbol"]
            ipo_dt = c["ipo_date"]

            attempted += 1

            price_from = ipo_dt
            price_to = min(ipo_dt.replace(), ipo_dt)  # placeholder keeps type
            price_to = min(ipo_dt + (today - ipo_dt), today)
            price_to = min(ipo_dt + __import__("datetime").timedelta(days=BACKFILL_PRICE_WINDOW_DAYS), today)

            if price_from > price_to:
                no_data += 1
                continue

            inserted_total = 0

            if BACKFILL_USE_YAHOO:
                try:
                    inserted_yh = ingest_yahoo_prices_for_symbol(
                        conn=conn,
                        symbol=price_symbol,
                        date_from=price_from,
                        date_to=price_to,
                        incremental=True,
                    )
                    inserted_total += inserted_yh
                    yahoo_used += 1
                except Exception as e:
                    failed += 1
                    print(f"⚠️ Yahoo backfill failed for {ipo_symbol}->{price_symbol}: {e}")
                    conn.rollback()

            if BACKFILL_USE_ALPHA_FALLBACK and inserted_total == 0:
                alpha_symbol = fetch_alpha_symbol(conn, ipo_symbol)
                if alpha_symbol:
                    try:
                        inserted_alpha = ingest_alpha_prices_for_symbol(
                            conn=conn,
                            symbol=alpha_symbol,
                            date_from=price_from,
                            date_to=price_to,
                            incremental=True,
                        )
                        inserted_total += inserted_alpha
                        alpha_used += 1
                    except Exception as e:
                        failed += 1
                        print(f"⚠️ Alpha backfill failed for {ipo_symbol}->{alpha_symbol}: {e}")
                        conn.rollback()

            total_rows += inserted_total

            if inserted_total > 0:
                with_data += 1
            else:
                no_data += 1

            if attempted % 10 == 0:
                print(
                    f"📈 Backfill progress: {attempted}/{len(candidates)} | "
                    f"rows={total_rows} | with_data={with_data} | no_data={no_data} | "
                    f"failed={failed} | yahoo_used={yahoo_used} | alpha_used={alpha_used}"
                )

            time.sleep(BACKFILL_SLEEP_SECONDS)

        finish_run(
            conn,
            run_id,
            "success",
            notes=(
                f"attempted={attempted}, rows={total_rows}, with_data={with_data}, "
                f"no_data={no_data}, failed={failed}, yahoo_used={yahoo_used}, alpha_used={alpha_used}"
            ),
        )

        print(
            f"✅ Backfill ok: attempted={attempted}, rows={total_rows}, "
            f"with_data={with_data}, no_data={no_data}, failed={failed}, "
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