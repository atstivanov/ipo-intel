# IPO Intel - End-to-End IPO Analytics Pipeline

## Overview
IPO Intel is an end-to-end data pipeline and analytics project that ingests IPO events and stock prices, transforms them with dbt, and serves insights via dashboards.

The pipeline integrates:
- Finnhub (IPO calendar + company profiles)
- Yahoo Finance / Alpha Vantage (prices + symbol resolution)
- PostgreSQL (data warehouse)
- dbt (transformations)
- Streamlit + Metabase (analytics layer)
- Airflow (orchestration)

---

## 🎯 Project Objective

The original goal was to analyze IPO performance over a **100-day post-listing window**.

However, due to:
- incomplete price coverage
- symbol resolution issues
- API limitations

👉 the project evolved to a **30-day post-IPO analysis framework with coverage-aware logic**.

---

## 📊 Final Analytical Approach

IPO coverage is classified based on available trading days in the first 30 days:

- **Good coverage** → ≥ 15 days
- **Partial coverage** → 7–14 days
- **Low coverage** → 1–6 days
- **No data** → 0 days

This ensures:
- consistent comparisons
- realistic benchmarks
- transparent data quality

---

## 🏗️ Architecture Overview

```text
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

# --- 1. Go to project ---
cd ipo-intel

# --- 2. Create environment (first time only) ---
python3.11 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# --- 3. Load environment variables ---
set -a
source .env
set +a
export PYTHONPATH="$(pwd)"

# --- 4. Start infrastructure ---
docker compose up -d

# --- 5. Run ingestion (IPOs + profiles + prices) ---
python -m src.ingest.run_ingestion

# --- 6. Backfill missing prices ---
python -m src.ingest.run_price_backfill

# --- 7. Run dbt transformations ---
cd dbt
dbt run
dbt test
cd ..

# --- 8. Launch dashboard ---
streamlit run streamlit_app.py

📊 Open dashboards

Streamlit → http://localhost:8501

Metabase → http://localhost:3000

📊 Open dashboards

Streamlit → http://localhost:8501

Metabase → http://localhost:3000

# --- 1. Create Airflow env (first time only) ---
python3.11 -m venv .venv_airflow
source .venv_airflow/bin/activate

pip install "apache-airflow==2.10.5" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"

# --- 2. Initialize Airflow ---
export AIRFLOW_HOME="$(pwd)/airflow"
airflow db init

airflow users create \
  --username admin \
  --firstname admin \
  --lastname admin \
  --role Admin \
  --email admin@example.com \
  --password admin

# --- 3. Start Airflow ---
airflow webserver --port 8080 &
airflow scheduler

🌐 Airflow UI

Open in browser: http://localhost:8080

Then:
Enable DAG: ipo_pipeline_daily
Click Trigger DAG