"""
France establishments - silver conformation
Joins the SIRENE bronze tables and publishes the conformed active establishments
as an Iceberg table in the Polaris silver catalog.
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
from france_establishments_assets import (
    BRONZE_BUCKET,
    BRONZE_PREFIX,
    DEPARTEMENTS,
    SILVER_ASSET,
    SILVER_CATALOG,
    SILVER_INPUTS,
    SILVER_NAMESPACE,
    SILVER_TABLE,
)

SCRIPT_PATH = Path(__file__).parent / "spark_jobs" / "france_establishments_silver_job.py"

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


def conform(run_id):
    conf = spark_submit.iceberg_catalog_conf(SILVER_CATALOG)
    conf["spark.sql.shuffle.partitions"] = "400"

    app = spark_submit.submit_and_wait(
        name=f"{BRONZE_PREFIX}-silver",
        run_id=run_id,
        script_path=SCRIPT_PATH,
        arguments=[
            "--bronze", f"s3a://{BRONZE_BUCKET}/{BRONZE_PREFIX}",
            "--catalog", SILVER_CATALOG,
            "--namespace", SILVER_NAMESPACE,
            "--table", SILVER_TABLE,
            "--departments", DEPARTEMENTS,
            "--run-id", spark_submit.slug(run_id),
        ],
        spark_conf=conf,
        polaris=True,
        driver_memory="4g",
        executors=4,
        executor_cores=3,
        executor_memory="8g",
        timeout_seconds=5400,
        poll_seconds=15,
    )
    return f"Silver published: {SILVER_CATALOG}.{SILVER_NAMESPACE}.{SILVER_TABLE} ({app})"


with DAG(
    dag_id="france_establishments_silver",
    default_args=default_args,
    description="Conforms the SIRENE bronze tables into the Iceberg silver catalog",
    # Runs once the three SIRENE bronze tables have all been refreshed.
    schedule=SILVER_INPUTS,
    catchup=False,
    # Two runs writing the same S3 prefixes or the same Iceberg tables corrupt
    # each other; the chain has nothing to gain from overlapping.
    max_active_runs=1,
    tags=["france-establishments", "silver", "iceberg", "polaris", "spark", "etl"],
) as dag:
    PythonOperator(
        task_id="conform_establishments",
        python_callable=conform,
        op_kwargs={"run_id": "{{ run_id }}"},
        outlets=[SILVER_ASSET],
    )
