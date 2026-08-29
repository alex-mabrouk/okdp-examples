"""
France establishments - bronze ingestion PySpark job
Reads a raw source landed in S3 and republishes it as Parquet, partitioned by
French department when the source carries an address, keeping the source columns
untouched.
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, lit, substring, when

# The column carrying the INSEE commune code, per source. None means the source
# has no address at all: a national reference table, landed flat.
COMMUNE_COLUMN = {
    "sirene_geoloc": "plg_code_commune",
    "sirene_etablissement": "codeCommuneEtablissement",
    "ban": "code_insee",
    "sirene_unite_legale": None,
}

# BAN ships as semicolon-separated gzipped CSV, the other two as Parquet.
CSV_SOURCES = {"ban"}

# SIRENE holds dates outside any plausible range -- year 0004, year 2218 -- which
# Spark 3 refuses to read or write without being told which calendar they use.
# The files are written by DuckDB in proleptic Gregorian, so the stored values are
# kept as-is on both sides: the LEGACY mode the error message suggests assumes a
# Spark 2 writer and would shift them silently. This belongs to the job that reads
# SIRENE, not to whatever submits it.
SIRENE_DATE_CONF = {
    "spark.sql.parquet.datetimeRebaseModeInRead": "CORRECTED",
    "spark.sql.parquet.int96RebaseModeInRead": "CORRECTED",
    "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
    "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED",
}


# Establishments registered abroad carry no commune code.
UNKNOWN_DEPARTMENT = "ZZ"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, choices=sorted(COMMUNE_COLUMN))
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--departments", default="")
    parser.add_argument("--shuffle-partitions", type=int, default=200)
    return parser.parse_args()


def department_of(commune_column):
    """INSEE rule: overseas codes are three digits, everything else is two.

    Corsica needs no special case here: its commune codes start with 2A/2B,
    which the two-character slice already yields.
    """
    code = col(commune_column)
    return (
        when(code.isNull() | (length(code) < 2), lit(UNKNOWN_DEPARTMENT))
        .when(substring(code, 1, 2) == lit("97"), substring(code, 1, 3))
        .otherwise(substring(code, 1, 2))
    )


def read_source(spark, source, path):
    if source in CSV_SOURCES:
        return (
            spark.read.option("header", "true")
            .option("sep", ";")
            .option("inferSchema", "false")
            .csv(path)
        )
    return spark.read.parquet(path)


def main():
    args = parse_args()
    commune_column = COMMUNE_COLUMN[args.source]

    print("=" * 70)
    print(f"Bronze ingestion - {args.source}")
    print("=" * 70)
    print(f"Input:  {args.input}")
    print(f"Output: {args.output}")
    print("=" * 70)

    builder = (
        SparkSession.builder
        .appName(f"FranceEstablishments-Bronze-{args.source}")
        .config("spark.sql.shuffle.partitions", args.shuffle_partitions)
    )
    for key, value in SIRENE_DATE_CONF.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = read_source(spark, args.source, args.input)

    if commune_column is not None:
        if commune_column not in df.columns:
            raise RuntimeError(
                f"{args.source}: expected column {commune_column}, got {df.columns}"
            )
        df = df.withColumn("code_departement", department_of(commune_column))

        wanted = [d.strip() for d in args.departments.split(",") if d.strip()]
        if wanted:
            print(f"Restricting to departments: {', '.join(wanted)}")
            df = df.filter(col("code_departement").isin(wanted))

    print("\nSchema landed in bronze:")
    df.printSchema()

    writer = df.write.mode("overwrite")
    if commune_column is None:
        writer.parquet(args.output)
    else:
        df.repartition("code_departement").write.mode("overwrite").partitionBy(
            "code_departement"
        ).parquet(args.output)

    published = spark.read.parquet(args.output)
    print(f"\nRows written: {published.count():,}")
    if commune_column is not None:
        print("\nTop departments:")
        (
            published.groupBy("code_departement")
            .count()
            .orderBy(col("count").desc())
            .show(10, truncate=False)
        )

    print("\n" + "=" * 70)
    print("Bronze ingestion completed!")
    print("=" * 70)
    spark.stop()


if __name__ == "__main__":
    main()
