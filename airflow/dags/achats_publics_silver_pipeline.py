"""
Public procurement - silver conformation
Publishes the geographic reference tables, then applies the business rules to
the DECP contracts and matches them with SIRENE.
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Airflow 3 does not put the DAGs folder on sys.path.
sys.path.append(str(Path(__file__).parent))

import spark_submit
from achats_publics_assets import (
    ACHETEURS_ASSET,
    ANNEES,
    BRONZE_ASSETS,
    BRONZE_BUCKET,
    BRONZE_PREFIX,
    REFERENTIELS_ASSETS,
    REFERENTIELS_NAMESPACE,
    SILVER_ASSETS,
    SILVER_CATALOG,
    SILVER_NAMESPACE,
    SIRENE_BRONZE,
    SIRENE_INPUTS,
)
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

JOBS = Path(__file__).parent / "spark_jobs"

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}


def build_referentiels(run_id):
    app = spark_submit.submit_and_wait(
        name="achats-publics-referentiels",
        run_id=run_id,
        script_path=JOBS / "achats_publics_referentiels_job.py",
        modules=(JOBS / "france_departements.py",),
        arguments=[
            "--cog", f"s3a://{BRONZE_BUCKET}/{BRONZE_PREFIX}/cog",
            "--catalog", SILVER_CATALOG,
            "--namespace", REFERENTIELS_NAMESPACE,
            "--run-id", spark_submit.slug(run_id),
        ],
        spark_conf=spark_submit.iceberg_catalog_conf(SILVER_CATALOG),
        polaris=True,
        executors=1,
        timeout_seconds=1800,
        poll_seconds=15,
    )
    return f"Reference tables published in {SILVER_CATALOG}.{REFERENTIELS_NAMESPACE} ({app})"


def conform(run_id):
    conf = spark_submit.iceberg_catalog_conf(SILVER_CATALOG)
    conf["spark.sql.shuffle.partitions"] = "64"
    app = spark_submit.submit_and_wait(
        name="achats-publics-silver",
        run_id=run_id,
        script_path=JOBS / "achats_publics_silver_job.py",
        arguments=[
            "--decp", f"s3a://{BRONZE_BUCKET}/{BRONZE_PREFIX}/decp",
            "--sirene", SIRENE_BRONZE,
            "--catalog", SILVER_CATALOG,
            "--namespace", SILVER_NAMESPACE,
            "--referentiels-namespace", REFERENTIELS_NAMESPACE,
            "--annees", ANNEES,
            "--run-id", spark_submit.slug(run_id),
        ],
        spark_conf=conf,
        polaris=True,
        driver_memory="4g",
        executors=2,
        executor_cores=2,
        executor_memory="6g",
        timeout_seconds=3600,
        poll_seconds=15,
    )
    return f"Silver published in {SILVER_CATALOG}.{SILVER_NAMESPACE} ({app})"


with DAG(
    dag_id="achats_publics_silver",
    default_args=default_args,
    description="Business rules on DECP, matched with SIRENE, and the reference tables",
    schedule=[BRONZE_ASSETS["decp"], BRONZE_ASSETS["cog"], *SIRENE_INPUTS],
    catchup=False,
    max_active_runs=1,
    tags=["achats-publics", "silver", "iceberg", "polaris", "spark", "etl"],
) as dag:
    PythonOperator(
        task_id="build_referentiels",
        python_callable=build_referentiels,
        op_kwargs={"run_id": "{{ run_id }}"},
        outlets=REFERENTIELS_ASSETS,
    ) >> PythonOperator(
        task_id="conform_marches",
        python_callable=conform,
        op_kwargs={"run_id": "{{ run_id }}"},
        outlets=[*SILVER_ASSETS, ACHETEURS_ASSET],
    )
