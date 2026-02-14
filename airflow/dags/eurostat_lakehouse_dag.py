from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/project"

default_args = {
    "owner": "mauri",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="eurostat_lakehouse",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["eurostat", "lakehouse"],
) as dag:

    extract_raw = BashOperator(
        task_id="extract_raw",
        bash_command=f"cd {PROJECT_DIR} && python3 src/01_extract_raw.py",
    )

    bronze = BashOperator(
        task_id="bronze_ingest",
        bash_command=f"cd {PROJECT_DIR} && python3 src/02_bronze_ingest.py",
    )

    silver = BashOperator(
        task_id="silver_transform",
        bash_command=f"cd {PROJECT_DIR} && python3 src/03_silver_transform.py",
    )

    gold = BashOperator(
        task_id="gold_analytics",
        bash_command=f"cd {PROJECT_DIR} && python3 src/04_gold_analytics.py",
    )

    quality = BashOperator(
        task_id="quality_checks",
        bash_command=f"cd {PROJECT_DIR} && python3 src/05_quality_checks.py",
    )

    extract_raw >> bronze >> silver >> gold >> quality

