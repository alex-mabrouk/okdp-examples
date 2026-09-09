"""
E-invoicing - silver conformation and controls
Parses the invoices, validates them against the official Factur-X artefacts,
checks them against the SIRENE referential the platform already holds, and
publishes the invoices, their lines and their anomalies as Iceberg tables.
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
    EINVOICING_IMAGE,
    PIPELINE,
    REFERENTIEL_INLETS,
    SILVER_ASSETS,
    SILVER_CATALOG,
    SILVER_NAMESPACE,
    s3_path,
)
from france_establishments_assets import BRONZE_BUCKET as SIRENE_BUCKET
from france_establishments_assets import BRONZE_PREFIX as SIRENE_PREFIX
from france_establishments_assets import (
    SILVER_CATALOG as REF_CATALOG,
    SILVER_NAMESPACE as REF_NAMESPACE,
    SILVER_TABLE as REF_TABLE,
)

JOBS = Path(__file__).parent / "spark_jobs"
SCRIPT_PATH = JOBS / "einvoicing_silver_job.py"
MODULES = (
    JOBS / "einvoicing_parse.py",
    JOBS / "einvoicing_rules.py",
    JOBS / "einvoicing_validate.py",
)

REFERENTIEL = f"{REF_CATALOG}.{REF_NAMESPACE}.{REF_TABLE}"

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


def conform(run_id):
    # Silver reads the referential catalog and writes its own, so both are declared.
    conf = spark_submit.iceberg_catalog_conf(REF_CATALOG, SILVER_CATALOG)
    conf["spark.sql.shuffle.partitions"] = "64"

    app = spark_submit.submit_and_wait(
        name=f"{PIPELINE}-silver",
        run_id=run_id,
        script_path=SCRIPT_PATH,
        modules=MODULES,
        # Running the official Schematrons means Saxon, which only this image has.
        image=EINVOICING_IMAGE,
        arguments=[
            "--bronze", s3_path(f"{BRONZE_PREFIX}/factures_cii"),
            "--referentiel", REFERENTIEL,
            "--sirene-bronze", f"s3a://{SIRENE_BUCKET}/{SIRENE_PREFIX}",
            "--catalog", SILVER_CATALOG,
            "--namespace", SILVER_NAMESPACE,
            "--run-id", spark_submit.slug(run_id),
        ],
        spark_conf=conf,
        polaris=True,
        driver_memory="4g",
        executors=4,
        executor_cores=2,
        executor_memory="6g",
        timeout_seconds=5400,
        poll_seconds=15,
    )
    return f"Silver published in {SILVER_CATALOG}.{SILVER_NAMESPACE} ({app})"


with DAG(
    dag_id="einvoicing_silver",
    default_args=default_args,
    description="Validates, checks and enriches the invoices against SIRENE",
    # Only its own bronze asset gates the run. The referential is an inlet: asset
    # scheduling is an AND, so listing it here would stall silver until the SIRENE
    # chain republished.
    schedule=[BRONZE_ASSET],
    catchup=False,
    max_active_runs=1,
    tags=["einvoicing", "silver", "iceberg", "polaris", "spark", "etl"],
) as dag:
    PythonOperator(
        task_id="conform",
        python_callable=conform,
        op_kwargs={"run_id": "{{ run_id }}"},
        inlets=REFERENTIEL_INLETS,
        outlets=list(SILVER_ASSETS.values()),
    )
