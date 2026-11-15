"""
Iceberg / Polaris reference ETL - PySpark job
Reads Parquet from S3 and publishes the result as an Iceberg table in a
Polaris REST catalog, authenticating as an OAuth2 service account.
"""
import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, dayofweek, hour
from pyspark.sql.functions import round as _round
from pyspark.sql.functions import sum as _sum


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--run-id", required=True)
    return parser.parse_args()


def build_spark(catalog, run_id):
    """Everything but the credential comes from the SparkApplication sparkConf.

    The credential is assembled here from the mounted Secret so the client
    secret never lands in the SparkApplication manifest.
    """
    client_id = os.getenv("POLARIS_CLIENT_ID", "")
    client_secret = os.getenv("POLARIS_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        raise RuntimeError("POLARIS_CLIENT_ID / POLARIS_CLIENT_SECRET are not set")

    return (
        SparkSession.builder
        .appName(f"Iceberg-Polaris-ETL-{run_id}")
        .config(f"spark.sql.catalog.{catalog}.credential", f"{client_id}:{client_secret}")
        .getOrCreate()
    )


def main():
    args = parse_args()
    qualified_table = f"{args.catalog}.{args.namespace}.{args.table}"

    print("=" * 70)
    print("Iceberg / Polaris reference ETL")
    print("=" * 70)
    print(f"Input:  {args.input}")
    print(f"Table:  {qualified_table}")
    print(f"Run:    {args.run_id}")
    print("=" * 70)

    spark = build_spark(args.catalog, args.run_id)
    spark.sparkContext.setLogLevel("WARN")

    print("\nReading data...")
    df = spark.read.parquet(args.input)
    total_rows = df.count()
    print(f"Rows read: {total_rows:,}")

    print("\nCleaning and aggregating...")
    df_agg = (
        df.filter(
            (col("fare_amount") > 0)
            & (col("trip_distance") > 0)
            & (col("passenger_count") > 0)
        )
        .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
        .withColumn("pickup_dayofweek", dayofweek(col("tpep_pickup_datetime")))
        .groupBy("pickup_hour", "pickup_dayofweek")
        .agg(
            count("*").alias("total_trips"),
            _round(avg("fare_amount"), 2).alias("avg_fare"),
            _round(avg("trip_distance"), 2).alias("avg_distance"),
            _round(_sum("fare_amount"), 2).alias("total_revenue"),
        )
        .orderBy("pickup_hour", "pickup_dayofweek")
    )
    df_agg.show(24, truncate=False)

    print(f"\nPublishing {qualified_table}...")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {args.catalog}.{args.namespace}")
    (
        df_agg.writeTo(qualified_table)
        .using("iceberg")
        .partitionedBy(col("pickup_dayofweek"))
        .tableProperty("format-version", "2")
        .createOrReplace()
    )

    print("\nReading the table back through the catalog...")
    print(f"Rows in {qualified_table}: {spark.table(qualified_table).count():,}")

    # The point of writing to Iceberg rather than to S3: every publication is a
    # snapshot the table can be queried at.
    print("\nSnapshot history:")
    spark.sql(
        f"SELECT snapshot_id, committed_at, operation FROM {qualified_table}.snapshots"
    ).show(truncate=False)

    print("\n" + "=" * 70)
    print("ETL completed successfully!")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()
