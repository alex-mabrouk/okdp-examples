"""
E-invoicing - generation
Draws a cast of real companies out of the establishments referential, then writes
a flow of synthetic Factur-X invoices from it, one XML file each.

This DAG carries no schedule and is triggered by hand. It produces the fixture the
rest of the chain consumes, and it reads the referential rather than waiting on
it: scheduling it on the SIRENE assets would regenerate the whole flow every time
that chain republished, which is not what a fixture is for.
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
from airflow.sdk import Param
from einvoicing_assets import (
    ANOMALY_RATE,
    BRONZE_BUCKET,
    CASTING_BUYERS,
    CASTING_CLOSED,
    CASTING_PREFIX,
    CASTING_SUPPLIERS,
    EINVOICING_IMAGE,
    EXTENDED_CTC_FR_RATE,
    INVOICE_COUNT,
    MONTHS,
    PIPELINE,
    RAW_ASSET,
    REFERENTIEL_INLETS,
    SEED,
    SOURCE_PREFIX,
    TRUTH_PREFIX,
    s3_path,
)
from france_establishments_assets import BRONZE_BUCKET as SIRENE_BUCKET
from france_establishments_assets import BRONZE_PREFIX as SIRENE_PREFIX
from france_establishments_assets import (
    SILVER_CATALOG,
    SILVER_NAMESPACE,
    SILVER_TABLE,
)

JOBS = Path(__file__).parent / "spark_jobs"
CASTING_SCRIPT = JOBS / "einvoicing_casting_job.py"
GENERATE_SCRIPT = JOBS / "einvoicing_generate_job.py"
# The generator imports both; they ride in the job ConfigMap and are declared as
# pyFiles so the executors can import them too.
GENERATE_MODULES = (JOBS / "einvoicing_cii.py", JOBS / "einvoicing_rules.py")

REFERENTIEL = f"{SILVER_CATALOG}.{SILVER_NAMESPACE}.{SILVER_TABLE}"

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


def build_casting(run_id, seed):
    app = spark_submit.submit_and_wait(
        name=f"{PIPELINE}-casting",
        run_id=run_id,
        script_path=CASTING_SCRIPT,
        arguments=[
            "--referentiel", REFERENTIEL,
            "--bronze", f"s3a://{SIRENE_BUCKET}/{SIRENE_PREFIX}",
            "--output", s3_path(CASTING_PREFIX),
            "--suppliers", CASTING_SUPPLIERS,
            "--buyers", CASTING_BUYERS,
            "--closed", CASTING_CLOSED,
            "--seed", seed,
        ],
        spark_conf=spark_submit.iceberg_catalog_conf(SILVER_CATALOG),
        polaris=True,
        driver_memory="4g",
        executors=4,
        executor_cores=2,
        executor_memory="6g",
        timeout_seconds=3600,
        poll_seconds=15,
    )
    return f"Cast drawn into {s3_path(CASTING_PREFIX)} ({app})"


def generate_invoices(run_id, count, seed):
    app = spark_submit.submit_and_wait(
        name=f"{PIPELINE}-generate",
        run_id=run_id,
        script_path=GENERATE_SCRIPT,
        modules=GENERATE_MODULES,
        # Writing Factur-X needs the image that carries a PDF engine and the
        # artefacts of the standard; the platform image has neither.
        image=EINVOICING_IMAGE,
        arguments=[
            "--casting", s3_path(CASTING_PREFIX),
            "--output", s3_path(f"{SOURCE_PREFIX}/factures"),
            "--truth", s3_path(TRUTH_PREFIX),
            "--bucket", BRONZE_BUCKET,
            "--key-prefix", f"{SOURCE_PREFIX}/factures",
            "--count", count,
            "--months", MONTHS,
            "--seed", seed,
            "--anomaly-rate", ANOMALY_RATE,
            "--extended-rate", EXTENDED_CTC_FR_RATE,
        ],
        executors=4,
        executor_cores=2,
        executor_memory="4g",
        timeout_seconds=5400,
        poll_seconds=15,
    )
    return f"{int(count):,} invoices written under {s3_path(SOURCE_PREFIX)} ({app})"


with DAG(
    dag_id="einvoicing_generate",
    default_args=default_args,
    description="Generates the synthetic Factur-X flow from the SIRENE referential",
    schedule=None,
    catchup=False,
    # Rehearse on a thousand invoices, film on a hundred thousand: the count is a
    # trigger-time parameter rather than a Release setting, so changing it does not
    # mean redeploying Airflow. The seed sits next to it because reproducing a run
    # means reproducing both.
    params={
        "count": Param(INVOICE_COUNT, type="integer", minimum=1),
        "seed": Param(SEED, type="integer"),
    },
    # Two runs writing the same S3 prefixes corrupt each other, and the seed makes
    # a second concurrent run pointless anyway.
    max_active_runs=1,
    tags=["einvoicing", "factur-x", "generation", "spark"],
) as dag:
    casting = PythonOperator(
        task_id="build_casting",
        python_callable=build_casting,
        op_kwargs={"run_id": "{{ run_id }}", "seed": "{{ params.seed }}"},
        # Declared, not awaited: this is what draws the two chains into one
        # lineage graph without making the fixture wait on the SIRENE schedule.
        inlets=REFERENTIEL_INLETS,
    )
    generate = PythonOperator(
        task_id="generate_invoices",
        python_callable=generate_invoices,
        op_kwargs={
            "run_id": "{{ run_id }}",
            "count": "{{ params.count }}",
            "seed": "{{ params.seed }}",
        },
        outlets=[RAW_ASSET],
    )
    casting >> generate
