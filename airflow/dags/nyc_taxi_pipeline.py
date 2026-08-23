"""
NYC Taxi Pipeline - Airflow + Spark Operator
Uses the Kubernetes Python API to submit a SparkApplication.
"""
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException


def _current_namespace() -> str:
    """Namespace of the pod this task runs in, empty outside a cluster."""
    try:
        with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace") as f:
            return f.read().strip()
    except OSError:
        return ""


NAMESPACE = os.getenv("AIRFLOW_NAMESPACE") or _current_namespace() or "default"
SPARK_APP_GROUP = "sparkoperator.k8s.io"
SPARK_APP_VERSION = "v1beta2"
SPARK_APP_PLURAL = "sparkapplications"
SPARK_IMAGE = "quay.io/okdp/spark-py:spark-3.5.6-python-3.11-scala-2.12-java-17"
SPARK_SERVICE_ACCOUNT = "spark"
S3_CREDENTIALS_SECRET = "creds-examples-s3"
S3_ACCESS_KEY_FIELD = "S3_ACCESS_KEY"
S3_SECRET_KEY_FIELD = "S3_SECRET_KEY"
CONFIGMAP_NAME = "nyc-taxi-etl-code"
SCRIPT_MOUNT_DIR = "/opt/spark/app"
SCRIPT_FILE_NAME = "nyc_taxi_etl_job.py"
SCRIPT_FILE_PATH = Path(__file__).parent / "spark_jobs" / SCRIPT_FILE_NAME

# Input/Output S3
# The medallion layout the rest of the examples use: raw parquet lands in
# bronze, this aggregate is a business-ready output and belongs in gold.
S3_INPUT = os.getenv("NYC_TAXI_S3_INPUT", "s3a://bronze/mobility/nyc_tlc/yellow/")
S3_OUTPUT_BASE = os.getenv("NYC_TAXI_S3_OUTPUT", "s3a://gold/mobility/nyc_tlc/yellow")

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


def _slug(value):
    return re.sub(r"[^a-z0-9-]", "-", value.lower()).strip("-")


def _safe_name(prefix, suffix, max_len=63):
    return _slug(f"{prefix}-{suffix}")[:max_len].rstrip("-")


# The sandbox store. Any other platform sets AIRFLOW_ETL_S3_ENDPOINT.
DEFAULT_S3_ENDPOINT = "http://storage-s3.default.svc.cluster.local:8333"


def _s3_endpoint():
    env_endpoint = os.getenv("AIRFLOW_ETL_S3_ENDPOINT", "").strip().rstrip("/")
    return env_endpoint or DEFAULT_S3_ENDPOINT


def _delete_if_exists(custom_api, app_name):
    try:
        custom_api.delete_namespaced_custom_object(
            group=SPARK_APP_GROUP,
            version=SPARK_APP_VERSION,
            namespace=NAMESPACE,
            plural=SPARK_APP_PLURAL,
            name=app_name,
        )
        time.sleep(2)
    except ApiException as exc:
        if exc.status != 404:
            raise


def _ensure_etl_code_configmap(core_api):
    """Publish the job source next to the DAG, so nothing else has to deploy it."""
    if not SCRIPT_FILE_PATH.is_file():
        raise FileNotFoundError(f"Spark ETL script not found: {SCRIPT_FILE_PATH}")
    body = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": CONFIGMAP_NAME, "namespace": NAMESPACE},
        "data": {SCRIPT_FILE_NAME: SCRIPT_FILE_PATH.read_text(encoding="utf-8")},
    }
    try:
        core_api.create_namespaced_config_map(namespace=NAMESPACE, body=body)
    except ApiException as exc:
        if exc.status != 409:
            raise
        core_api.patch_namespaced_config_map(name=CONFIGMAP_NAME, namespace=NAMESPACE, body=body)


def submit_and_wait_nyc_taxi_etl(run_id, timeout_seconds=1200):
    """Submit SparkApplication and wait for completion."""
    config.load_incluster_config()
    core_api = client.CoreV1Api()
    custom_api = client.CustomObjectsApi()

    _ensure_etl_code_configmap(core_api)

    run_key = _slug(run_id)
    app_name = _safe_name("nyc-taxi-etl", run_key)
    s3_endpoint = _s3_endpoint()
    ssl_enabled = str(s3_endpoint.lower().startswith("https://")).lower()
    output_uri = f"{S3_OUTPUT_BASE}/{run_key}"

    _delete_if_exists(custom_api, app_name)

    body = {
        "apiVersion": f"{SPARK_APP_GROUP}/{SPARK_APP_VERSION}",
        "kind": "SparkApplication",
        "metadata": {"name": app_name, "namespace": NAMESPACE},
        "spec": {
            "type": "Python",
            "mode": "cluster",
            "image": SPARK_IMAGE,
            "imagePullPolicy": "IfNotPresent",
            "mainApplicationFile": f"local://{SCRIPT_MOUNT_DIR}/{SCRIPT_FILE_NAME}",
            "arguments": [
                "--input", S3_INPUT,
                "--output", output_uri,
                "--run-id", run_key,
            ],
            "sparkVersion": "3.5.6",
            "restartPolicy": {"type": "Never"},
            "timeToLiveSeconds": 3600,
            "sparkConf": {
                "spark.hadoop.fs.s3a.endpoint": s3_endpoint,
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": ssl_enabled,
                # Fix for SeaweedFS: write to local then upload via script
                "spark.hadoop.fs.s3a.fast.upload": "true",
                "spark.hadoop.fs.s3a.fast.upload.buffer": "bytebuffer",
            },
            "volumes": [
                {"name": "etl-script", "configMap": {"name": CONFIGMAP_NAME}},
            ],
            "driver": {
                "cores": 1,
                "memory": "1g",
                "serviceAccount": SPARK_SERVICE_ACCOUNT,
                "labels": {"workload": "nyc-taxi-etl"},
                "volumeMounts": [{"name": "etl-script", "mountPath": SCRIPT_MOUNT_DIR}],
                "envSecretKeyRefs": {
                    "AWS_ACCESS_KEY_ID": {"name": S3_CREDENTIALS_SECRET, "key": S3_ACCESS_KEY_FIELD},
                    "AWS_SECRET_ACCESS_KEY": {"name": S3_CREDENTIALS_SECRET, "key": S3_SECRET_KEY_FIELD},
                },
                "env": [{"name": "S3_ENDPOINT", "value": s3_endpoint}],
            },
            "executor": {
                "instances": 1,
                "cores": 1,
                "memory": "1g",
                "labels": {"workload": "nyc-taxi-etl"},
                "envSecretKeyRefs": {
                    "AWS_ACCESS_KEY_ID": {"name": S3_CREDENTIALS_SECRET, "key": S3_ACCESS_KEY_FIELD},
                    "AWS_SECRET_ACCESS_KEY": {"name": S3_CREDENTIALS_SECRET, "key": S3_SECRET_KEY_FIELD},
                },
                "env": [{"name": "S3_ENDPOINT", "value": s3_endpoint}],
            },
        },
    }

    custom_api.create_namespaced_custom_object(
        group=SPARK_APP_GROUP,
        version=SPARK_APP_VERSION,
        namespace=NAMESPACE,
        plural=SPARK_APP_PLURAL,
        body=body,
    )

    deadline = time.time() + timeout_seconds
    last_state = "SUBMITTED"
    while time.time() < deadline:
        app = custom_api.get_namespaced_custom_object(
            group=SPARK_APP_GROUP,
            version=SPARK_APP_VERSION,
            namespace=NAMESPACE,
            plural=SPARK_APP_PLURAL,
            name=app_name,
        )
        last_state = (
            app.get("status", {})
            .get("applicationState", {})
            .get("state", "SUBMITTED")
        )
        if last_state == "COMPLETED":
            return f"Spark ETL completed: {app_name}"
        if last_state in {"FAILED", "SUBMISSION_FAILED", "UNKNOWN"}:
            raise RuntimeError(f"Spark ETL failed: {app_name} state={last_state}")
        time.sleep(10)

    raise TimeoutError(f"Spark ETL timeout after {timeout_seconds}s: {app_name} state={last_state}")


with DAG(
    dag_id="nyc_taxi_spark_pipeline",
    default_args=default_args,
    description="NYC Taxi Spark ETL pipeline",
    schedule=None,
    catchup=False,
    tags=["nyc-taxi", "spark", "etl"],
) as dag:
    run_etl = PythonOperator(
        task_id="submit_and_wait_nyc_taxi_etl",
        python_callable=submit_and_wait_nyc_taxi_etl,
        op_kwargs={"run_id": "{{ run_id }}"},
    )
