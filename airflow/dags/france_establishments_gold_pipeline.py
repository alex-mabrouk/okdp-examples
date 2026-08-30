"""
France establishments - gold indicators
Aggregates the silver establishments into the dashboard tables and publishes them
as Iceberg tables in the Polaris gold catalog.
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
    BRONZE_PREFIX,
    GOLD_ASSETS,
    GOLD_CATALOG,
    GOLD_NAMESPACE,
    SILVER_ASSET,
    SILVER_CATALOG,
    SILVER_NAMESPACE,
    SILVER_TABLE,
)

SCRIPT_PATH = Path(__file__).parent / "spark_jobs" / "france_establishments_gold_job.py"

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


def aggregate(run_id):
    # Gold reads one Polaris catalog and writes another, so both are declared.
    conf = spark_submit.iceberg_catalog_conf(SILVER_CATALOG, GOLD_CATALOG)
    conf["spark.sql.shuffle.partitions"] = "200"

    app = spark_submit.submit_and_wait(
        name=f"{BRONZE_PREFIX}-gold",
        run_id=run_id,
        script_path=SCRIPT_PATH,
        arguments=[
            "--source-catalog", SILVER_CATALOG,
            "--source-namespace", SILVER_NAMESPACE,
            "--source-table", SILVER_TABLE,
            "--catalog", GOLD_CATALOG,
            "--namespace", GOLD_NAMESPACE,
            "--run-id", spark_submit.slug(run_id),
        ],
        spark_conf=conf,
        polaris=True,
        executors=2,
        executor_cores=2,
        executor_memory="4g",
        timeout_seconds=3600,
        poll_seconds=15,
    )
    return f"Gold tables published in {GOLD_CATALOG}.{GOLD_NAMESPACE} ({app})"


with DAG(
    dag_id="france_establishments_gold",
    default_args=default_args,
    description="Aggregates the silver establishments into the dashboard tables",
    # Runs as soon as silver publishes a new snapshot.
    schedule=[SILVER_ASSET],
    catchup=False,
    # Two runs writing the same S3 prefixes or the same Iceberg tables corrupt
    # each other; the chain has nothing to gain from overlapping.
    max_active_runs=1,
    tags=["france-establishments", "gold", "iceberg", "polaris", "spark", "etl"],
) as dag:
    PythonOperator(
        task_id="build_indicators",
        python_callable=aggregate,
        op_kwargs={"run_id": "{{ run_id }}"},
        outlets=GOLD_ASSETS,
    )
