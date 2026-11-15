# NYC Taxi pipeline

`nyc_taxi_pipeline` reads three months of yellow-cab parquet, aggregates it by
hour and day of week, and writes the result back to S3. It is the reference for
a DAG that submits a Spark job and stops at files.

## Running it

The DAG publishes its own Spark job as a ConfigMap in the namespace it runs
in. The raw parquet must be available in the bronze bucket.

The ETL DAGs default to the sandbox object store,
`http://storage-s3.default.svc.cluster.local:8333`. When your store lives
elsewhere, set `AIRFLOW_ETL_S3_ENDPOINT` on the Airflow workers.

```bash
# 1. Open the Airflow UI and trigger the DAG `nyc_taxi_pipeline`
open https://airflow.okdp.sandbox

# 2. Verify the results in SeaweedFS S3
kubectl run --rm -it s3-check --image=amazon/aws-cli:latest --restart=Never \
  --command -- aws --endpoint-url http://storage-s3.default.svc.cluster.local:8333 \
  --no-verify-ssl s3 ls s3://gold/mobility/nyc_tlc/yellow/ --recursive
```

## Architecture

```
Airflow DAG (PythonOperator)
    → SparkApplication (Spark Operator)
        → Spark Driver + Executors
            → Read:  s3a://bronze/mobility/nyc_tlc/yellow/  (11M+ rows)
            → Clean + Aggregate (168 rows: 24h × 7 days)
            → Write: s3a://gold/mobility/nyc_tlc/yellow/<run id>/nyc_taxi_aggregated.csv
```

## Datasets

NYC Yellow Taxi data is already provisioned in SeaweedFS by the
`okdp-examples` Helm chart at deployment time:

```
s3://bronze/mobility/nyc_tlc/yellow/
├── month=2025-01/yellow_tripdata_2025-01.parquet  (59 MB)
├── month=2025-02/yellow_tripdata_2025-02.parquet  (60 MB)
└── month=2025-03/yellow_tripdata_2025-03.parquet  (70 MB)
```

No manual download required.

## Pipeline steps

1. **Read** — 3 months of Parquet data from S3 (11M+ rows)
2. **Clean** — Filter invalid trips (fare ≤ 0, distance ≤ 0, etc.)
3. **Aggregate** — Group by hour and day-of-week (168 rows)
4. **Write** — Upload aggregated CSV to SeaweedFS via the JVM AWS SDK

> **Note**: writes use the JVM S3 SDK (not the Hadoop FileOutputCommitter)
> to work around a SeaweedFS `copyObject` quirk.
