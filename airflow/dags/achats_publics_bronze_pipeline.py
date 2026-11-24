"""
Public procurement - bronze ingestion
Lands the consolidated DECP files and the INSEE geographic code in S3 as
published, then republishes them as Parquet. SIRENE comes from the
france_establishments chain.
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Airflow 3 does not put the DAGs folder on sys.path.
sys.path.append(str(Path(__file__).parent))

import decp_stream
import s3_landing
import spark_submit
from achats_publics_assets import (
    BRONZE_ASSETS,
    BRONZE_BUCKET,
    BRONZE_PREFIX,
    COG_URLS,
    DECP_URLS,
)
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

SCRIPT_PATH = Path(__file__).parent / "spark_jobs" / "achats_publics_bronze_job.py"
JSONL_PREFIX = f"{BRONZE_PREFIX}/_jsonl/decp"

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def land_decp():
    s3 = s3_landing.s3_client()
    return [
        s3_landing.land_url(s3, url, BRONZE_BUCKET, f"{BRONZE_PREFIX}/_source/decp")
        for url in DECP_URLS
    ]


def land_cog():
    s3 = s3_landing.s3_client()
    return [
        s3_landing.land_url(s3, url, BRONZE_BUCKET, f"{BRONZE_PREFIX}/_source/cog")
        for url in COG_URLS.values()
    ]


def split_decp(ti):
    """Cut each landed JSON document into JSON Lines parts Spark can read in parallel."""
    s3 = s3_landing.s3_client()
    s3_landing.delete_prefix(s3, BRONZE_BUCKET, f"{JSONL_PREFIX}/")
    for key in ti.xcom_pull(task_ids="land_decp"):
        stem = Path(key).stem
        body = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)["Body"]
        counts = {}
        records = decp_stream.iter_records(decp_stream.decode_chunks(body))
        for kind, index, data in decp_stream.jsonl_parts(records):
            part_key = f"{JSONL_PREFIX}/{kind}/fichier={stem}/part-{index:05d}.jsonl.gz"
            s3.put_object(Bucket=BRONZE_BUCKET, Key=part_key, Body=data)
            counts[kind] = counts.get(kind, 0) + 1
        print(f"{key}: {counts} parts")


def publish(source, input_prefix, run_id):
    base = f"s3a://{BRONZE_BUCKET}"
    app = spark_submit.submit_and_wait(
        name=f"achats-publics-bronze-{source}",
        run_id=run_id,
        script_path=SCRIPT_PATH,
        arguments=[
            "--source", source,
            "--input", f"{base}/{input_prefix}",
            "--output", f"{base}/{BRONZE_PREFIX}/{source}",
        ],
        driver_memory="2g",
        executors=2,
        executor_cores=2,
        executor_memory="4g",
        timeout_seconds=3600,
        poll_seconds=15,
    )
    return f"Bronze published: {base}/{BRONZE_PREFIX}/{source}/ ({app})"


with DAG(
    dag_id="achats_publics_bronze",
    default_args=default_args,
    description="Lands DECP and the INSEE geographic code in bronze",
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    tags=["achats-publics", "bronze", "spark", "etl"],
) as dag:
    (
        PythonOperator(task_id="land_decp", python_callable=land_decp)
        >> PythonOperator(task_id="split_decp", python_callable=split_decp)
        >> PythonOperator(
            task_id="publish_decp",
            python_callable=publish,
            op_kwargs={"source": "decp", "input_prefix": JSONL_PREFIX, "run_id": "{{ run_id }}"},
            outlets=[BRONZE_ASSETS["decp"]],
        )
    )
    PythonOperator(task_id="land_cog", python_callable=land_cog) >> PythonOperator(
        task_id="publish_cog",
        python_callable=publish,
        op_kwargs={
            "source": "cog",
            "input_prefix": f"{BRONZE_PREFIX}/_source/cog",
            "run_id": "{{ run_id }}",
        },
        outlets=[BRONZE_ASSETS["cog"]],
    )
