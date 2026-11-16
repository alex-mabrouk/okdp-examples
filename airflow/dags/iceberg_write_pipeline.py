"""
Iceberg write pipeline - Airflow + Spark Operator
Writes a small Iceberg table through a Polaris REST catalog with the workload
identity of the project. No object store key: the catalog vends the credentials
of the table.
"""
import os
import sys
from datetime import datetime
from pathlib import Path

# Airflow 3 does not put the DAGs folder on sys.path.
sys.path.append(str(Path(__file__).parent))

import spark_submit
from airflow import DAG
from airflow.sdk import Param
from airflow.providers.standard.operators.python import PythonOperator

SCRIPT_PATH = Path(__file__).parent / "spark_jobs" / "iceberg_write_job.py"

ICEBERG_CATALOG = os.getenv("ICEBERG_WRITE_CATALOG", "lake")
ICEBERG_NAMESPACE = os.getenv("ICEBERG_WRITE_NAMESPACE", "sales")
ICEBERG_TABLE = os.getenv("ICEBERG_WRITE_TABLE", "orders_dag")


def write_table(run_id, params):
    catalog = params["catalog"]
    app = spark_submit.submit_and_wait(
        name="iceberg-write",
        run_id=run_id,
        script_path=SCRIPT_PATH,
        arguments=[
            "--catalog", catalog,
            "--namespace", ICEBERG_NAMESPACE,
            "--table", ICEBERG_TABLE,
            "--run-id", spark_submit.slug(run_id),
        ],
        spark_conf=spark_submit.iceberg_catalog_conf(catalog),
        polaris=True,
        s3=False,
        executors=1,
        timeout_seconds=900,
    )
    return f"Iceberg table written: {catalog}.{ICEBERG_NAMESPACE}.{ICEBERG_TABLE} ({app})"


with DAG(
    dag_id="iceberg_write_pipeline",
    description="Writes an Iceberg table through Polaris with vended credentials",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["iceberg", "polaris", "spark"],
    # One of the icebergCatalogs of the Airflow instance.
    params={"catalog": Param(ICEBERG_CATALOG, type="string")},
) as dag:
    PythonOperator(
        task_id="write_table",
        python_callable=write_table,
        op_kwargs={"run_id": "{{ run_id }}"},
    )
