"""
Public procurement - gold datamart
Materializes the purchases datamart from silver and the reference tables.
"""
import sys
from datetime import datetime
from pathlib import Path

# Airflow 3 does not put the DAGs folder on sys.path.
sys.path.append(str(Path(__file__).parent))

import spark_submit
from achats_publics_assets import (
    ACHETEURS_ASSET,
    GOLD_ASSETS,
    GOLD_CATALOG,
    GOLD_NAMESPACE,
    REFERENTIELS_NAMESPACE,
    SILVER_ASSETS,
    SILVER_CATALOG,
    SILVER_NAMESPACE,
)
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

SCRIPT_PATH = Path(__file__).parent / "spark_jobs" / "achats_publics_gold_job.py"

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}


def build_datamart(run_id):
    conf = spark_submit.iceberg_catalog_conf(SILVER_CATALOG, GOLD_CATALOG)
    conf["spark.sql.shuffle.partitions"] = "32"
    app = spark_submit.submit_and_wait(
        name="achats-publics-gold",
        run_id=run_id,
        script_path=SCRIPT_PATH,
        arguments=[
            "--silver-catalog", SILVER_CATALOG,
            "--silver-namespace", SILVER_NAMESPACE,
            "--referentiels-namespace", REFERENTIELS_NAMESPACE,
            "--catalog", GOLD_CATALOG,
            "--namespace", GOLD_NAMESPACE,
            "--run-id", spark_submit.slug(run_id),
        ],
        spark_conf=conf,
        polaris=True,
        executors=2,
        executor_cores=2,
        executor_memory="4g",
        timeout_seconds=1800,
        poll_seconds=15,
    )
    return f"Datamart published in {GOLD_CATALOG}.{GOLD_NAMESPACE} ({app})"


with DAG(
    dag_id="achats_publics_gold",
    default_args=default_args,
    description="Materializes the purchases datamart",
    schedule=[*SILVER_ASSETS, ACHETEURS_ASSET],
    catchup=False,
    max_active_runs=1,
    tags=["achats-publics", "gold", "iceberg", "polaris", "spark", "etl"],
) as dag:
    PythonOperator(
        task_id="build_datamart",
        python_callable=build_datamart,
        op_kwargs={"run_id": "{{ run_id }}"},
        outlets=GOLD_ASSETS,
    )
