# Airflow Examples

Apache Airflow DAGs and helpers showing how to orchestrate Spark jobs and data
workflows on the OKDP platform.

## Available DAGs

| DAG | Description |
|---|---|
| `hello_world` | Minimal DAG, validates scheduler/worker connectivity |
| `hello_daily` | Same as above, scheduled daily |
| `spark_pi_example` | Submits the canonical Spark Pi job via `SparkApplication` |
| `orders_etl_daily` | Daily Spark ETL with dynamic ConfigMap-based script injection |
| `nyc_taxi_pipeline` | Reads NYC taxi data from S3, transforms with Spark, writes back — [docs](docs/nyc-taxi.md) |
| `iceberg_polaris_pipeline` | Publishes an **Iceberg table** through the Polaris REST catalog — [docs](docs/iceberg-polaris.md) |
| `france_establishments_bronze` | Lands four French open-data sources, partitioned by department — [docs](docs/france-establishments.md) |
| `france_establishments_silver` | Joins them into a conformed Iceberg table — [docs](docs/france-establishments.md) |
| `france_establishments_gold` | Aggregates the five tables the dashboard reads — [docs](docs/france-establishments.md) |

## How a DAG reaches the scheduler

A `gitSync` sidecar clones the repository into the Airflow pods and refreshes it
every 60 seconds. The OKDP sandbox points it at **`main` of the upstream
repository**, so a DAG on a branch or on a local disk is invisible to Airflow
until it is merged:

```bash
kubectl -n demo get deploy demo-airflow-main-dag-processor -o json \
  | jq -r '.spec.template.spec.containers[]|select(.name=="git-sync").env[]
           |select(.name|startswith("GITSYNC"))|"\(.name)=\(.value)"'
```

The repository and branch come from the `dagsGitSync` parameter of the Airflow
package. New DAGs arrive paused.

## Writing a Spark DAG

Every Spark DAG here follows the same shape: the DAG publishes its job source as
a ConfigMap, submits a `SparkApplication`, and waits for it. That plumbing lives
once in `dags/spark_submit.py`, which knows about the platform — object store,
Polaris catalog, the CA the JVM must trust — and nothing about the data. What a
given source needs in order to be read belongs to the job that reads it.

The shape also means any job can be run without Airflow, by submitting its
`SparkApplication` directly — which is how these were tested before merge.

> **Importing a module next to a DAG** needs
> `sys.path.append(str(Path(__file__).parent))` first. Airflow 3 loads DAG files
> through `module_from_spec`, `PYTHONPATH` is empty and `settings.py` never adds
> `DAGS_FOLDER` — unlike Airflow 2. Without the line, the DAG fails at parse time.

## Useful commands

On the sandbox the demo project runs in the `demo` namespace, and its Airflow
instance is named `demo-airflow`. Replace both with your own names.

```bash
# SparkApplication status
kubectl get sparkapplications -n demo

# Spark driver logs
kubectl logs -n demo -l spark-role=driver --tail=50

# List Airflow DAG runs
kubectl exec -n demo deploy/demo-airflow-main-scheduler -c scheduler -- \
  airflow dags list-runs nyc_taxi_spark_pipeline
```

## Repository structure

```
airflow/
├── README.md
├── docs/
│   ├── nyc-taxi.md
│   ├── iceberg-polaris.md
│   └── france-establishments.md
├── dags/
│   ├── hello_world.py
│   ├── hello_daily.py
│   ├── spark_pi_example.py
│   ├── orders_etl_daily.py
│   ├── nyc_taxi_pipeline.py
│   ├── iceberg_polaris_pipeline.py
│   ├── spark_submit.py                      submit a SparkApplication, once
│   ├── france_establishments_assets.py      identifiers and assets of the chain
│   ├── france_establishments_{bronze,silver,gold}_pipeline.py
│   └── spark_jobs/
│       ├── nyc_taxi_etl_job.py
│       ├── orders_etl_job.py
│       ├── iceberg_polaris_etl_job.py
│       └── france_establishments_{bronze,silver,gold}_job.py
└── tests/
    ├── test_dags.py
    └── run_integration_tests.sh
```

## License

Apache 2.0

---

**Built 🚀 for the OKDP Community**
<a href="https://okdp.io">
  <img src="https://okdp.io/logos/okdp-notext.svg" height="20px" style="margin: 0 2px;" />
</a>
