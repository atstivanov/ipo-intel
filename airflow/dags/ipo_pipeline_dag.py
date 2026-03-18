from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIR = "/Users/atanasivanov/ipo-intel/ipo-intel"

default_args = {
    "owner": "atanas",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="ipo_pipeline_daily",
    default_args=default_args,
    description="IPO ingestion, backfill, dbt run, and dbt test pipeline",
    schedule_interval="0 8 * * *",
    start_date=datetime(2026, 3, 18),
    catchup=False,
    tags=["ipo", "dbt", "streamlit"],
) as dag:

    ingest_full = BashOperator(
        task_id="ingest_full",
        bash_command=f"""bash -lc '
        cd {PROJECT_DIR} &&
        source .venv/bin/activate &&
        set -a &&
        source .env &&
        set +a &&
        export PYTHONPATH="{PROJECT_DIR}" &&
        python -m src.ingest.run_ingestion
        '""",
    )

    backfill_prices = BashOperator(
        task_id="backfill_prices",
        bash_command=f"""bash -lc '
        cd {PROJECT_DIR} &&
        source .venv/bin/activate &&
        set -a &&
        source .env &&
        set +a &&
        export PYTHONPATH="{PROJECT_DIR}" &&
        python -m src.ingest.run_price_backfill
        '""",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""bash -lc '
        cd {PROJECT_DIR}/dbt &&
        source {PROJECT_DIR}/.venv/bin/activate &&
        set -a &&
        source {PROJECT_DIR}/.env &&
        set +a &&
        dbt run
        '""",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""bash -lc '
        cd {PROJECT_DIR}/dbt &&
        source {PROJECT_DIR}/.venv/bin/activate &&
        set -a &&
        source {PROJECT_DIR}/.env &&
        set +a &&
        dbt test
        '""",
    )

    ingest_full >> backfill_prices >> dbt_run >> dbt_test