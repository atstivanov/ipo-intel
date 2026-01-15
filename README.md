# IPO Intel - Finnhub + Alpha Vantage + Postgres + dbt + Metabase

## Overview
IPO Intel ingests IPO events and daily prices into Postgres, builds analytics marts with dbt, and supports dashboards in Metabase or Streamlit.

Core tables:
- Raw ingestion:
  - `raw.ipo_events` (Finnhub IPO calendar)
  - `raw.daily_prices` (Alpha Vantage daily OHLCV)
- dbt models:
  - `analytics_staging.stg_ipos_recent`
  - `analytics_staging.stg_prices_ipo_window_100d`
  - `analytics_analytics.ipo_metrics_100d`
  - `analytics_analytics.industry_benchmarks_100d`

## Prerequisites
- Docker + Docker Compose (for Postgres + Metabase)
- Python 3.11+ (for ingestion/dbt/Streamlit)

## Quickstart (Docker + local ingestion)
1) Create `.env` from the template and add your API keys:
```bash
cp .env.example .env
```
Set at least:
- `FINNHUB_API_KEY`
- `ALPHAVANTAGE_API_KEY`

If you use Docker Compose for Postgres, set `PG_PORT=5433` in `.env` (host port mapping is `5433 -> 5432`).

2) Start Postgres + Metabase:
```bash
docker compose up -d
```

3) Install Python dependencies:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

4) Run ingestion (IPO events, profiles, symbols, prices):
```bash
python -m src.ingest.run_ingestion
```

5) Build dbt models:
```bash
dbt deps --project-dir dbt
dbt run --project-dir dbt --profiles-dir dbt
```

6) Optional: run the Streamlit dashboard:
```bash
pip install streamlit sqlalchemy psycopg2-binary
streamlit run app.py
```

## Metabase
Open `http://localhost:3000` and add the Postgres database:
- Host: `localhost`
- Port: `5433` (Docker default)
- Database: `ipo`
- Username/password: from `.env`

## Configuration knobs
Ingestion is controlled via environment variables (defaults shown in code):
- `IPO_BACKFILL_START_YEAR`, `IPO_BACKFILL_END_YEAR`
- `IPO_RECENT_DAYS`, `IPO_PRICE_WINDOW_DAYS`
- `FINNHUB_PROFILE_MAX_PER_RUN`, `FINNHUB_PROFILE_SLEEP_SECONDS`
- `ALPHAVANTAGE_MAX_RESOLVE_PER_RUN`, `ALPHAVANTAGE_RESOLVE_SLEEP_SECONDS`
- `ALPHAVANTAGE_MAX_TICKERS_PER_RUN`, `ALPHAVANTAGE_SLEEP_SECONDS`
- `PRICE_INCREMENTAL`

## Notes
- Free API tiers are rate-limited. If ingestion stops early, rerun later or increase sleep settings.
- The Docker Compose database is initialized via `sql/init_db.sql`. If you change schema, recreate the volume or apply migrations manually.
