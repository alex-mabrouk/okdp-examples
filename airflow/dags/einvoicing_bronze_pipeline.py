"""
E-invoicing - bronze ingestion
Republishes the received invoices as Parquet, keeping the XML byte for byte, and
fires as soon as the generator publishes a new flow.
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Airflow 3 loads each DAG file through module_from_spec without putting the DAGs
# folder on sys.path, unlike Airflow 2, so the shared modules sitting next to this
# one are not importable unless we say where they are.
sys.path.append(str(Path(__file__).parent))

import spark_submit
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from einvoicing_assets import (
    BRONZE_ASSET,
    BRONZE_PREFIX,
    PIPELINE,
    RAW_ASSET,
    SOURCE_PREFIX,
    s3_path,
)

SCRIPT_PATH = Path(__file__).parent / "spark_jobs" / "einvoicing_bronze_job.py"

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


def ingest(run_id):
    app = spark_submit.submit_and_wait(
        name=f"{PIPELINE}-bronze",
        run_id=run_id,
        script_path=SCRIPT_PATH,
        arguments=[
            "--input", s3_path(f"{SOURCE_PREFIX}/factures"),
            "--output", s3_path(f"{BRONZE_PREFIX}/factures_cii"),
        ],
        driver_memory="4g",
        executors=4,
        executor_cores=2,
        executor_memory="4g",
        timeout_seconds=3600,
        poll_seconds=15,
    )
    return f"Bronze published: {s3_path(f'{BRONZE_PREFIX}/factures_cii')} ({app})"


with DAG(
    dag_id="einvoicing_bronze",
    default_args=default_args,
    description="Lands the received Factur-X documents in bronze, untouched",
    schedule=[RAW_ASSET],
    catchup=False,
    # Two runs writing the same S3 prefixes corrupt each other.
    max_active_runs=1,
    tags=["einvoicing", "bronze", "spark", "etl"],
) as dag:
    PythonOperator(
        task_id="ingest",
        python_callable=ingest,
        op_kwargs={"run_id": "{{ run_id }}"},
        outlets=[BRONZE_ASSET],
    )
