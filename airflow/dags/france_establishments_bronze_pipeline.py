"""
France establishments - bronze ingestion
Lands the raw open-data sources in S3, then republishes each one as Parquet,
partitioned by French department where the source carries an address.
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

# Airflow 3 loads each DAG file through module_from_spec without putting the DAGs
# folder on sys.path, unlike Airflow 2, so the shared modules sitting next to this
# one are not importable unless we say where they are.
sys.path.append(str(Path(__file__).parent))

import os

import spark_submit
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from france_establishments_assets import (
    BRONZE_ASSETS,
    BRONZE_BUCKET,
    BRONZE_PREFIX,
    DEPARTEMENTS,
    departments,
)

SCRIPT_PATH = Path(__file__).parent / "spark_jobs" / "france_establishments_bronze_job.py"

# data.gouv resource ids. They are stable across millésimes; the redirect they
# resolve to carries the dated file name.
SOURCE_URLS = {
    "sirene_geoloc": os.getenv(
        "SIRENE_GEOLOC_URL",
        "https://www.data.gouv.fr/api/1/datasets/r/672007af-0146-491f-835c-8314d63fa44e",
    ),
    "sirene_etablissement": os.getenv(
        "SIRENE_ETABLISSEMENT_URL",
        "https://www.data.gouv.fr/api/1/datasets/r/a29c1297-1f92-4e2a-8f6b-8c902ce96c5f",
    ),
    "sirene_unite_legale": os.getenv(
        "SIRENE_UNITE_LEGALE_URL",
        "https://www.data.gouv.fr/api/1/datasets/r/350182c9-148a-46e0-8389-76c2ec1374a3",
    ),
}
BAN_BASE_URL = os.getenv(
    "BAN_BASE_URL", "https://adresse.data.gouv.fr/data/ban/adresses/latest/csv"
)

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _s3_client():
    import boto3

    missing = [v for v in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY") if not os.getenv(v)]
    if missing:
        raise RuntimeError(
            f"{', '.join(missing)} missing: the task pod has no object-store identity"
        )
    return boto3.client(
        "s3",
        endpoint_url=spark_submit.s3_endpoint(),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )


def _already_landed(s3, key, expected_size):
    """A source file is immutable for a given millésime: same size means done."""
    try:
        head = s3.head_object(Bucket=BRONZE_BUCKET, Key=key)
    except Exception:
        return False
    return expected_size is not None and head["ContentLength"] == expected_size


def _land_url(s3, url, source):
    """Stream one remote file straight into S3, without staging it on disk."""
    import requests

    with requests.get(url, stream=True, timeout=(30, 600)) as response:
        response.raise_for_status()
        file_name = Path(urlparse(response.url).path).name
        size = response.headers.get("Content-Length")
        size = int(size) if size else None
        key = f"{BRONZE_PREFIX}/_source/{source}/{file_name}"

        if _already_landed(s3, key, size):
            print(f"already landed, skipping: s3://{BRONZE_BUCKET}/{key}")
            return key

        pretty = f"{size / 1e6:.1f} MB" if size else "unknown size"
        print(f"landing {response.url} ({pretty}) -> s3://{BRONZE_BUCKET}/{key}")
        response.raw.decode_content = True
        s3.upload_fileobj(response.raw, BRONZE_BUCKET, key)

    print(f"landed s3://{BRONZE_BUCKET}/{key}")
    return key


def land_source(source):
    return _land_url(_s3_client(), SOURCE_URLS[source], source)


def land_ban():
    """BAN is the one source published per department, so the dev loop is cheap."""
    s3 = _s3_client()
    targets = departments() or ["france"]
    return [
        _land_url(s3, f"{BAN_BASE_URL}/adresses-{target}.csv.gz", "ban")
        for target in targets
    ]


def partition_source(source, run_id):
    """Republish one landed source as Parquet, partitioned by department."""
    base = f"s3a://{BRONZE_BUCKET}/{BRONZE_PREFIX}"
    app = spark_submit.submit_and_wait(
        name=f"{BRONZE_PREFIX}-bronze-{source.replace('_', '-')}",
        run_id=run_id,
        script_path=SCRIPT_PATH,
        arguments=[
            "--source", source,
            "--input", f"{base}/_source/{source}/",
            "--output", f"{base}/{source}/",
            "--departments", DEPARTEMENTS,
        ],
        driver_memory="4g",
        executors=4,
        executor_cores=2,
        executor_memory="6g",
        timeout_seconds=5400,
        poll_seconds=15,
    )
    return f"Bronze published: {base}/{source}/ ({app})"


with DAG(
    dag_id="france_establishments_bronze",
    default_args=default_args,
    description="Lands SIRENE and BAN in bronze, partitioned by French department",
    # The sources are republished monthly; the DAG starts paused, as all of them do.
    schedule="@monthly",
    catchup=False,
    tags=["france-establishments", "bronze", "spark", "etl"],
) as dag:
    for source in BRONZE_ASSETS:
        land = PythonOperator(
            task_id=f"land_{source}",
            python_callable=land_ban if source == "ban" else land_source,
            op_kwargs={} if source == "ban" else {"source": source},
        )
        partition = PythonOperator(
            task_id=f"partition_{source}",
            python_callable=partition_source,
            op_kwargs={"source": source, "run_id": "{{ run_id }}"},
            outlets=[BRONZE_ASSETS[source]],
        )
        land >> partition
