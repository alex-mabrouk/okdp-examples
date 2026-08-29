"""
Submitting a SparkApplication from an Airflow task
The Kubernetes plumbing every Spark DAG in this folder needs: publish the job
source as a ConfigMap, submit the SparkApplication, wait for it, and fail loudly.
Written once here rather than copied into each DAG.

The line this module holds: it knows about the *platform* -- the object store, the
Polaris catalog, the CA the JVM has to trust -- and nothing about the data. What a
particular source needs in order to be read correctly belongs to the job that
reads it.
"""
import os
import re
import time
from pathlib import Path

from kubernetes import client, config
from kubernetes.client.exceptions import ApiException

SPARK_APP_GROUP = "sparkoperator.k8s.io"
SPARK_APP_VERSION = "v1beta2"
SPARK_APP_PLURAL = "sparkapplications"
SPARK_IMAGE = os.getenv(
    "SPARK_IMAGE",
    "quay.io/okdp/spark-py:spark-3.5.6-python-3.11-scala-2.12-java-17",
)
SPARK_VERSION = "3.5.6"
SPARK_SERVICE_ACCOUNT = os.getenv("SPARK_SERVICE_ACCOUNT", "spark")
SCRIPT_MOUNT_DIR = "/opt/spark/app"

S3_CREDENTIALS_SECRET = os.getenv("S3_CREDENTIALS_SECRET", "creds-examples-s3")
S3_ACCESS_KEY_FIELD = "S3_ACCESS_KEY"
S3_SECRET_KEY_FIELD = "S3_SECRET_KEY"

# Where the Spark History Server of the platform looks for completed runs.
SPARK_EVENT_LOG_DIR = os.getenv("SPARK_EVENT_LOG_DIR", "s3a://spark-events/event-logs")

# The OAuth2 service account jobs authenticate to Polaris with. Its principal
# must hold a role granting writes on the target catalog.
POLARIS_CREDENTIALS_SECRET = os.getenv(
    "POLARIS_CREDENTIALS_SECRET", "creds-polaris-oauth2-etl-okdp-sandbox"
)
POLARIS_CLIENT_ID_FIELD = "client_id"
POLARIS_CLIENT_SECRET_FIELD = "client_secret"
POLARIS_URI = os.getenv("POLARIS_CATALOG_URI", "https://polaris-demo.okdp.sandbox/api/catalog")
POLARIS_OAUTH2_SERVER_URI = os.getenv(
    "POLARIS_OAUTH2_SERVER_URI",
    "https://keycloak.okdp.sandbox/realms/master/protocol/openid-connect/token",
)
POLARIS_REALM = os.getenv("POLARIS_REALM", "sandbox")
# Iceberg defaults to scope=catalog, which Keycloak rejects as an unknown scope.
POLARIS_SCOPE = os.getenv("POLARIS_SCOPE", "profile")

# The driver reaches Polaris and Keycloak over TLS signed by the platform CA,
# which the JVM only trusts through this truststore.
TRUSTSTORE_SECRET = os.getenv("SPARK_TRUSTSTORE_SECRET", "certs-bundle")
TRUSTSTORE_MOUNT_DIR = "/cacerts"
TRUSTSTORE_FILE = "bundle.p12"


def current_namespace() -> str:
    """Namespace of the pod this task runs in, empty outside a cluster."""
    try:
        with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace") as f:
            return f.read().strip()
    except OSError:
        return ""


NAMESPACE = os.getenv("AIRFLOW_NAMESPACE") or current_namespace() or "default"


def slug(value):
    return re.sub(r"[^a-z0-9-]", "-", value.lower()).strip("-")


def safe_name(prefix, suffix, max_len=63):
    return slug(f"{prefix}-{suffix}")[:max_len].rstrip("-")


def s3_endpoint():
    """The object store the platform declared, never a guess.

    Falling back to a default would send writes to whichever store that name
    happens to resolve to, silently, which is worse than not running.
    """
    endpoint = os.getenv("AWS_ENDPOINT_URL_S3", "").strip().rstrip("/")
    if not endpoint:
        raise RuntimeError("AWS_ENDPOINT_URL_S3 is not set: no object store was declared")
    return endpoint


def base_spark_conf():
    endpoint = s3_endpoint()
    return {
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": str(
            endpoint.lower().startswith("https://")
        ).lower(),
        "spark.hadoop.fs.s3a.fast.upload": "true",
        "spark.hadoop.fs.s3a.fast.upload.buffer": "bytebuffer",
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": SPARK_EVENT_LOG_DIR,
    }


def iceberg_catalog_conf(*catalogs):
    """Declare one or more Polaris REST catalogs. The credential is not here.

    Each job assembles it from the mounted Secret so the client secret never
    lands in the SparkApplication manifest.
    """
    conf = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    }
    for catalog in catalogs:
        prefix = f"spark.sql.catalog.{catalog}"
        conf.update(
            {
                prefix: "org.apache.iceberg.spark.SparkCatalog",
                f"{prefix}.type": "rest",
                f"{prefix}.warehouse": catalog,
                f"{prefix}.uri": POLARIS_URI,
                f"{prefix}.rest.auth.type": "oauth2",
                f"{prefix}.oauth2-server-uri": POLARIS_OAUTH2_SERVER_URI,
                f"{prefix}.scope": POLARIS_SCOPE,
                # The client holds the secret, so it re-authenticates on its own
                # rather than carrying a token that expires mid-run.
                f"{prefix}.token-refresh-enabled": "true",
                f"{prefix}.header.Polaris-Realm": POLARIS_REALM,
                f"{prefix}.header.X-Iceberg-Access-Delegation": "vended-credentials",
                f"{prefix}.io-impl": "org.apache.iceberg.io.ResolvingFileIO",
                f"{prefix}.client.region": "us-east-1",
                f"{prefix}.s3.region": "us-east-1",
            }
        )
    return conf


def _configmap_name(script_path):
    return safe_name(script_path.stem.replace("_", "-"), "code")


def _ensure_etl_code_configmap(core_api, script_path):
    """Publish the job source next to the DAG, so nothing else has to deploy it."""
    if not script_path.is_file():
        raise FileNotFoundError(f"Spark job script not found: {script_path}")
    name = _configmap_name(script_path)
    body = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": name, "namespace": NAMESPACE},
        "data": {script_path.name: script_path.read_text(encoding="utf-8")},
    }
    try:
        core_api.create_namespaced_config_map(namespace=NAMESPACE, body=body)
    except ApiException as exc:
        if exc.status != 409:
            raise
        core_api.patch_namespaced_config_map(name=name, namespace=NAMESPACE, body=body)
    return name


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


def _pod_spec(workload, configmap_name, polaris, extra):
    mounts = [{"name": "job-script", "mountPath": SCRIPT_MOUNT_DIR}]
    volumes = [{"name": "job-script", "configMap": {"name": configmap_name}}]
    secrets = {
        "AWS_ACCESS_KEY_ID": {"name": S3_CREDENTIALS_SECRET, "key": S3_ACCESS_KEY_FIELD},
        "AWS_SECRET_ACCESS_KEY": {"name": S3_CREDENTIALS_SECRET, "key": S3_SECRET_KEY_FIELD},
    }
    env = [{"name": "S3_ENDPOINT", "value": s3_endpoint()}]

    if polaris:
        mounts.append({"name": "cacerts", "mountPath": TRUSTSTORE_MOUNT_DIR})
        volumes.append({"name": "cacerts", "secret": {"secretName": TRUSTSTORE_SECRET}})
        secrets["POLARIS_CLIENT_ID"] = {
            "name": POLARIS_CREDENTIALS_SECRET,
            "key": POLARIS_CLIENT_ID_FIELD,
        }
        secrets["POLARIS_CLIENT_SECRET"] = {
            "name": POLARIS_CREDENTIALS_SECRET,
            "key": POLARIS_CLIENT_SECRET_FIELD,
        }
        env.append(
            {
                "name": "JAVA_TOOL_OPTIONS",
                "value": (
                    f"-Djavax.net.ssl.trustStore={TRUSTSTORE_MOUNT_DIR}/{TRUSTSTORE_FILE} "
                    "-Djavax.net.ssl.trustStorePassword="
                ),
            }
        )

    spec = {
        "labels": {"workload": workload},
        "volumeMounts": mounts,
        "envSecretKeyRefs": secrets,
        "env": env,
    }
    spec.update(extra)
    return spec, volumes


def submit_and_wait(
    *,
    name,
    run_id,
    script_path,
    arguments,
    spark_conf=None,
    polaris=False,
    driver_cores=1,
    driver_memory="2g",
    executors=2,
    executor_cores=1,
    executor_memory="2g",
    timeout_seconds=1800,
    poll_seconds=10,
):
    """Run one SparkApplication to completion, or raise.

    Returns the name of the application that ran, which is what the Airflow task
    surfaces in its logs.
    """
    script_path = Path(script_path)
    config.load_incluster_config()
    core_api = client.CoreV1Api()
    custom_api = client.CustomObjectsApi()

    configmap_name = _ensure_etl_code_configmap(core_api, script_path)
    app_name = safe_name(name, slug(run_id))

    conf = base_spark_conf()
    conf.update(spark_conf or {})

    driver, volumes = _pod_spec(
        name,
        configmap_name,
        polaris,
        {
            "cores": driver_cores,
            "memory": driver_memory,
            "serviceAccount": SPARK_SERVICE_ACCOUNT,
        },
    )
    executor, _ = _pod_spec(
        name,
        configmap_name,
        polaris,
        {"instances": executors, "cores": executor_cores, "memory": executor_memory},
    )

    body = {
        "apiVersion": f"{SPARK_APP_GROUP}/{SPARK_APP_VERSION}",
        "kind": "SparkApplication",
        "metadata": {"name": app_name, "namespace": NAMESPACE},
        "spec": {
            "type": "Python",
            "mode": "cluster",
            "image": SPARK_IMAGE,
            "imagePullPolicy": "IfNotPresent",
            "mainApplicationFile": f"local://{SCRIPT_MOUNT_DIR}/{script_path.name}",
            "arguments": [str(a) for a in arguments],
            "sparkVersion": SPARK_VERSION,
            "restartPolicy": {"type": "Never"},
            "timeToLiveSeconds": 3600,
            "sparkConf": conf,
            "volumes": volumes,
            "driver": driver,
            "executor": executor,
        },
    }

    _delete_if_exists(custom_api, app_name)
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
            app.get("status", {}).get("applicationState", {}).get("state", "SUBMITTED")
        )
        if last_state == "COMPLETED":
            return app_name
        if last_state in {"FAILED", "SUBMISSION_FAILED", "UNKNOWN"}:
            raise RuntimeError(f"Spark job failed: {app_name} state={last_state}")
        time.sleep(poll_seconds)

    raise TimeoutError(f"Spark job timeout after {timeout_seconds}s: {app_name} state={last_state}")
