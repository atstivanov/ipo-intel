IPO Intel - End-to-End IPO Analytics Pipeline
Overview

IPO Intel is an end-to-end data pipeline that ingests IPO events and stock prices, transforms them with dbt, and serves insights via dashboards.

The pipeline integrates:

Finnhub (IPO calendar + company profiles)

Yahoo Finance / Alpha Vantage (prices + symbol resolution)

PostgreSQL (data warehouse)

dbt (transformations)

Streamlit + Metabase (analytics layer)

Airflow (orchestration)

🎯 Project Objective

The original goal was to analyze IPO performance over a 100-day post-listing window.

However, due to:

incomplete price coverage

symbol resolution issues

API limitations

👉 the project evolved into a 30-day post-IPO analysis framework with coverage-aware logic.

📊 Final Analytical Approach

IPO coverage is classified based on available trading days in the first 30 days:

Good coverage → ≥ 15 days

Partial coverage → 7–14 days

Low coverage → 1–6 days

No data → 0 days

This ensures:

consistent comparisons

realistic benchmarks

transparent data quality

🏗️ Architecture Overview
APIs (Finnhub, Yahoo, Alpha Vantage)
        ↓
Python Ingestion Layer
        ↓
PostgreSQL (raw layer)
        ↓
dbt (staging + marts)
        ↓
Streamlit / Metabase (analytics)
        ↓
Airflow (orchestration)
🚀 Run the Full Pipeline
1. Setup environment
cd ipo-intel

python3.11 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
2. Load environment variables
set -a
source .env
set +a
export PYTHONPATH="$(pwd)"
3. Start infrastructure
docker compose up -d
4. Run ingestion
python -m src.ingest.run_ingestion
5. Backfill prices
python -m src.ingest.run_price_backfill
6. Run dbt transformations
cd dbt
dbt run
dbt test
cd ..
7. Launch dashboard
streamlit run streamlit_app.py
📊 Open Dashboards

Streamlit → http://localhost:8501

Metabase → http://localhost:3000

🔁 Run with Airflow (Optional)
Start Airflow

Terminal 1

source .venv_airflow/bin/activate
export AIRFLOW_HOME="$(pwd)/airflow"
airflow webserver --port 8080

Terminal 2

source .venv_airflow/bin/activate
export AIRFLOW_HOME="$(pwd)/airflow"
airflow scheduler
Open Airflow UI

http://localhost:8080

Steps:

Enable DAG: ipo_pipeline_daily

Click Trigger DAG

✅ Expected Outcome

After running the pipeline:

PostgreSQL contains updated IPO and price data

dbt models are built and tested

Coverage-aware metrics are available

Dashboard reflects latest data

⚠️ Notes

Free API tiers are rate-limited → rerun ingestion if needed

Backfill significantly improves dataset quality

Airflow is optional but recommended for automation