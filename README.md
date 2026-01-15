# IPO Intel (ELT) — Finnhub + Stooq + Postgres + dbt + Metabase + Airflow

## What it does
- Ingests IPO calendar data from Finnhub into Postgres (`raw.ipo_events`)
- Ingests daily OHLCV prices from Stooq into Postgres (`raw.daily_prices`)
- Builds analytics tables with dbt:
  - `analytics.ipo_metrics`
  - `analytics.industry_benchmarks`
- Visualize in Metabase, orchestrate daily in Airflow

## Prerequisites
- Docker + Docker Compose
- Python 3.11 (optional if you run ingestion locally)

## Quickstart (Docker: Postgres + Metabase)
1) Create `.env`:
```bash
cat > .env << 'EOF'
PG_DB=ipo
PG_USER=postgres
PG_PASSWORD=postgres
PG_PORT=5432
FINNHUB_API_KEY=PASTE_KEY
EOF
