"""
France establishments - AI insights
Turns the gold indicators into sentences a dashboard can show, using the model
served inside the cluster. Runs as soon as gold publishes new tables.
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
    AI_ASSETS,
    BRONZE_PREFIX,
    GOLD_ASSETS,
    GOLD_CATALOG,
    GOLD_NAMESPACE,
    OLLAMA_MODEL,
    OLLAMA_URL,
)

SCRIPT_PATH = Path(__file__).parent / "spark_jobs" / "france_establishments_ai_job.py"

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


def write_insights(run_id):
    app = spark_submit.submit_and_wait(
        name=f"{BRONZE_PREFIX}-ai",
        run_id=run_id,
        script_path=SCRIPT_PATH,
        arguments=[
            "--catalog", GOLD_CATALOG,
            "--namespace", GOLD_NAMESPACE,
            "--ollama-url", OLLAMA_URL,
            "--model", OLLAMA_MODEL,
            "--run-id", spark_submit.slug(run_id),
        ],
        spark_conf=spark_submit.iceberg_catalog_conf(GOLD_CATALOG),
        polaris=True,
        executors=1,
        executor_cores=1,
        executor_memory="2g",
        timeout_seconds=1800,
        poll_seconds=15,
    )
    return f"Insights published in {GOLD_CATALOG}.{GOLD_NAMESPACE} ({app})"


with DAG(
    dag_id="france_establishments_ai",
    default_args=default_args,
    description="Writes the gold indicators up as verified sentences, with a local model",
    schedule=GOLD_ASSETS,
    catchup=False,
    # Two runs writing the same S3 prefixes or the same Iceberg tables corrupt
    # each other; the chain has nothing to gain from overlapping.
    max_active_runs=1,
    tags=["france-establishments", "ai", "ollama", "iceberg", "polaris", "spark"],
) as dag:
    PythonOperator(
        task_id="write_insights",
        python_callable=write_insights,
        op_kwargs={"run_id": "{{ run_id }}"},
        outlets=AI_ASSETS,
    )
