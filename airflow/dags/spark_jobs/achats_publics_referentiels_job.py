"""
Public procurement - geographic reference tables
Publishes the region > department > commune hierarchy of the INSEE geographic
code (COG) as Iceberg tables shared by every datamart.
"""
import argparse
import os

from france_departements import code_carte
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, create_map, lit

# INSEE region code -> key of Superset's France regions map (6.0).
CARTE_REGIONS = {
    "01": "FR-GUA", "02": "FR-MTQ", "03": "FR-GUF", "04": "FR-LRE", "06": "FR-MAY",
    "11": "FR-IDF", "24": "FR-CVL", "27": "FR-BFC", "28": "FR-NOR", "32": "FR-HDF",
    "44": "FR-GES", "52": "FR-PDL", "53": "FR-BRE", "75": "FR-NAQ", "76": "FR-OCC",
    "84": "FR-ARA", "93": "FR-PAC", "94": "FR-COR",
}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cog", required=True, help="bronze COG path")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--run-id", required=True)
    return parser.parse_args()


def build_spark(catalog, run_id):
    client_id = os.getenv("POLARIS_CLIENT_ID", "")
    client_secret = os.getenv("POLARIS_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        raise RuntimeError("POLARIS_CLIENT_ID / POLARIS_CLIENT_SECRET are not set")
    return (
        SparkSession.builder.appName(f"AchatsPublics-Referentiels-{run_id}")
        .config(f"spark.sql.catalog.{catalog}.credential", f"{client_id}:{client_secret}")
        .getOrCreate()
    )


def departements(cog):
    regions = cog("region").select(
        col("REG").alias("code_region"), col("LIBELLE").alias("libelle_region")
    )
    carte = create_map([lit(x) for pair in CARTE_REGIONS.items() for x in pair])
    return (
        cog("departement")
        .select(
            col("DEP").alias("code_departement"),
            col("LIBELLE").alias("libelle_departement"),
            col("REG").alias("code_region"),
        )
        .join(regions, "code_region")
        .withColumn("code_carte_departement", code_carte())
        .withColumn("code_carte_region", carte[col("code_region")])
        .select(
            "code_region", "libelle_region", "code_carte_region",
            "code_departement", "libelle_departement", "code_carte_departement",
        )
    )


def communes(cog, deps):
    # ARM: the arrondissements of Paris, Lyon and Marseille, which SIRENE uses as
    # the commune of an establishment. Delegated and associated communes carry no
    # department and are not addresses of record.
    return (
        cog("commune")
        .filter(col("TYPECOM").isin("COM", "ARM"))
        .select(
            col("COM").alias("code_commune"),
            col("LIBELLE").alias("libelle_commune"),
            col("TYPECOM").alias("type_commune"),
            col("COMPARENT").alias("code_commune_parente"),
            col("DEP").alias("code_departement"),
        )
        .join(deps, "code_departement")
        .select(
            "code_region", "libelle_region",
            "code_departement", "libelle_departement",
            "code_commune", "libelle_commune", "type_commune", "code_commune_parente",
        )
    )


def publish(spark, df, table):
    df.writeTo(table).using("iceberg").tableProperty("format-version", "2").createOrReplace()
    print(f"{table}: {spark.table(table).count():,} rows")


def main():
    args = parse_args()
    spark = build_spark(args.catalog, args.run_id)
    spark.sparkContext.setLogLevel("WARN")

    def cog(table):
        return spark.read.parquet(f"{args.cog}/{table}/")

    deps = departements(cog)
    missing = deps.filter(col("code_carte_region").isNull()).count()
    if missing:
        raise RuntimeError(f"{missing} departments have a region missing from CARTE_REGIONS")

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {args.catalog}.{args.namespace}")
    prefix = f"{args.catalog}.{args.namespace}"
    publish(spark, deps, f"{prefix}.geo_departements")
    publish(spark, communes(cog, deps), f"{prefix}.geo_communes")
    spark.table(f"{prefix}.geo_departements").orderBy("code_region").show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
