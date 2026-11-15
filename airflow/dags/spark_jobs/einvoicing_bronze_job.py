"""
E-invoicing - bronze ingestion PySpark job
Takes the invoice files as they were received and republishes them as Parquet,
one row per document, keeping the XML byte for byte.

Bronze parses nothing. The point of keeping the original payload is that silver
can be rewritten, re-run and corrected without ever asking the sender for the
invoice again -- and that a rejected invoice can still be shown as it arrived,
which is what an auditor asks for first.

The fingerprint is what makes a re-run idempotent and what tells a duplicated
*file* apart from a duplicated *invoice*: the same invoice sent twice under two
names has two rows, one fingerprint.
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    element_at,
    input_file_name,
    length,
    lit,
    regexp_extract,
    sha2,
    split,
)

# The generator writes s3://<bucket>/<prefix>/mois=YYYY-MM/<numero>.xml, and the
# month in the path is the one the invoice was issued in.
MOIS_DANS_LE_CHEMIN = r"mois=(\d{4}-\d{2})"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="s3a:// prefix holding the XML")
    parser.add_argument("--output", required=True)
    parser.add_argument("--shuffle-partitions", type=int, default=64)
    return parser.parse_args()


def main():
    args = parse_args()

    print("=" * 70)
    print("E-invoicing - bronze ingestion")
    print("=" * 70)
    print(f"Input:  {args.input}")
    print(f"Output: {args.output}")
    print("=" * 70)

    spark = (
        SparkSession.builder.appName("EInvoicing-Bronze")
        .config("spark.sql.shuffle.partitions", args.shuffle_partitions)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # binaryFile keeps the payload untouched, which text sources do not: they would
    # strip or re-encode line endings, and the fingerprint has to be the one of the
    # bytes that arrived.
    documents = (
        spark.read.format("binaryFile")
        .option("pathGlobFilter", "*.xml")
        .option("recursiveFileLookup", "true")
        .load(args.input)
    )

    bronze = documents.select(
        col("path").alias("chemin_source"),
        element_at(split(col("path"), "/"), -1).alias("nom_fichier"),
        regexp_extract(col("path"), MOIS_DANS_LE_CHEMIN, 1).alias("mois"),
        col("content").cast("string").alias("contenu_xml"),
        sha2(col("content").cast("string"), 256).alias("empreinte"),
        col("length").alias("taille_octets"),
        col("modificationTime").alias("date_depot"),
        current_timestamp().alias("date_reception"),
    )

    (
        bronze.repartition("mois")
        .write.mode("overwrite")
        .partitionBy("mois")
        .parquet(args.output)
    )

    published = spark.read.parquet(args.output)
    total = published.count()
    print(f"\nDocuments ingested: {total:,}")

    print("\nDistinct fingerprints (a duplicated invoice shares one):")
    published.selectExpr(
        "count(*) as documents",
        "count(distinct empreinte) as empreintes",
        "count(distinct nom_fichier) as fichiers",
    ).show(truncate=False)

    print("\nBy month:")
    published.groupBy("mois").count().orderBy("mois").show(30, truncate=False)

    print("\nPayload kept intact:")
    published.selectExpr(
        "min(taille_octets) as min_octets",
        "cast(avg(taille_octets) as int) as moy_octets",
        "max(taille_octets) as max_octets",
    ).show(truncate=False)

    vides = published.filter(
        col("contenu_xml").isNull() | (length(col("contenu_xml")) == lit(0))
    ).count()
    if vides:
        raise RuntimeError(f"{vides} document(s) landed empty: bronze would lose them")

    print("\n" + "=" * 70)
    print("Bronze ingestion completed!")
    print("=" * 70)
    spark.stop()


if __name__ == "__main__":
    main()
