"""
Iceberg write - PySpark job
Replaces a small Iceberg table in a Polaris REST catalog.
"""
import argparse
import os

from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--run-id", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    client_id = os.getenv("POLARIS_CLIENT_ID", "")
    client_secret = os.getenv("POLARIS_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        raise RuntimeError("POLARIS_CLIENT_ID / POLARIS_CLIENT_SECRET are not set")

    spark = (
        SparkSession.builder.appName(f"Iceberg-Write-{args.run_id}")
        .config(f"spark.sql.catalog.{args.catalog}.credential", f"{client_id}:{client_secret}")
        .getOrCreate()
    )
    table = f"{args.catalog}.{args.namespace}.{args.table}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {args.catalog}.{args.namespace}")
    rows = [(1, "FR", args.run_id), (2, "DE", args.run_id), (3, "IT", args.run_id)]
    spark.createDataFrame(rows, "id long, country string, run_id string").writeTo(table).createOrReplace()
    print(f"{table}: {spark.table(table).count()} rows")
    spark.stop()


if __name__ == "__main__":
    main()
