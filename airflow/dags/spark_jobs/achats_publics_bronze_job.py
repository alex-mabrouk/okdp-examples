"""
Public procurement - bronze PySpark job
Republishes the landed sources as Parquet, unchanged: every value stays a string,
casting and cleaning belong to silver.
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name

# JSON Lines record kind -> bronze table.
DECP_KINDS = {"marche": "marches", "contrat-concession": "concessions"}
COG_TABLES = ("commune", "departement", "region")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, choices=["decp", "cog"])
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def publish(df, output, partition=None):
    df = df.withColumn("_ingested_at", current_timestamp())
    writer = df.write.mode("overwrite")
    if partition:
        writer = writer.partitionBy(partition)
    writer.parquet(output)


def decp(spark, args):
    for kind, table in DECP_KINDS.items():
        # A schema inferred with types would change from one month to the next
        # (offresRecues is "NC" or a number): bronze keeps the text as published.
        df = (
            spark.read.option("primitivesAsString", "true")
            .json(f"{args.input}/{kind}/")
            .withColumn("_source_file", input_file_name())
        )
        publish(df, f"{args.output}/{table}/", "fichier")
        published = spark.read.parquet(f"{args.output}/{table}/")
        print(f"\n{table}: {published.count():,} rows")
        published.groupBy("fichier").count().orderBy("fichier").show(truncate=False)
    spark.read.parquet(f"{args.output}/marches/").printSchema()


def cog(spark, args):
    for table in COG_TABLES:
        df = spark.read.option("header", "true").csv(f"{args.input}/v_{table}_*.csv")
        publish(df, f"{args.output}/{table}/")
        print(f"{table}: {spark.read.parquet(f'{args.output}/{table}/').count():,} rows")


def main():
    args = parse_args()
    print(f"Bronze {args.source}: {args.input} -> {args.output}")
    spark = SparkSession.builder.appName(f"AchatsPublics-Bronze-{args.source}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    {"decp": decp, "cog": cog}[args.source](spark, args)
    spark.stop()


if __name__ == "__main__":
    main()
