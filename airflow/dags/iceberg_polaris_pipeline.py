"""
Iceberg / Polaris reference pipeline - Airflow + Spark Operator
The smallest example of publishing a managed Iceberg table through a Polaris REST
catalog, authenticating as an OAuth2 service account. Reads the NYC taxi bronze
data already used elsewhere in this repo, so the pipeline is the subject rather
than the dataset.
"""
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Airflow 3 loads each DAG file through module_from_spec without putting the DAGs
# folder on sys.path, unlike Airflow 2, so the shared module sitting next to this
# one is not importable unless we say where it is.
sys.path.append(str(Path(__file__).parent))

import spark_submit
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

SCRIPT_PATH = Path(__file__).parent / "spark_jobs" / "iceberg_polaris_etl_job.py"

ICEBERG_CATALOG = os.getenv("ICEBERG_CATALOG", "silver")
ICEBERG_NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "reference")
ICEBERG_TABLE = os.getenv("ICEBERG_TABLE", "nyc_taxi_hourly")

S3_INPUT = os.getenv("ICEBERG_S3_INPUT", "s3a://bronze/mobility/nyc_tlc/yellow/")

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


def publish_iceberg_table(run_id):
    app = spark_submit.submit_and_wait(
        name="iceberg-polaris-etl",
        run_id=run_id,
        script_path=SCRIPT_PATH,
        arguments=[
            "--input", S3_INPUT,
            "--catalog", ICEBERG_CATALOG,
            "--namespace", ICEBERG_NAMESPACE,
            "--table", ICEBERG_TABLE,
            "--run-id", spark_submit.slug(run_id),
        ],
        spark_conf=spark_submit.iceberg_catalog_conf(ICEBERG_CATALOG),
        # Mounts the Polaris credentials and the platform CA truststore: the
        # driver talks to Polaris and Keycloak over TLS the JVM only trusts
        # through that store.
        polaris=True,
        timeout_seconds=1800,
    )
    return (
        f"Iceberg table published: "
        f"{ICEBERG_CATALOG}.{ICEBERG_NAMESPACE}.{ICEBERG_TABLE} ({app})"
    )


with DAG(
    dag_id="iceberg_polaris_pipeline",
    default_args=default_args,
    description="Reference pipeline writing an Iceberg table through Polaris",
    schedule=None,
    catchup=False,
    tags=["iceberg", "polaris", "spark", "etl"],
) as dag:
    PythonOperator(
        task_id="publish_iceberg_table",
        python_callable=publish_iceberg_table,
        op_kwargs={"run_id": "{{ run_id }}"},
    )
