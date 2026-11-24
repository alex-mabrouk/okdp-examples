"""
Public procurement - gold datamart PySpark job
Publishes the gold tables the dashboard reads: attributions, one row per contract
and supplier with the labels of the reference tables, and achats, the datamart
aggregated by month, buyer department, buyer type and supplier profile.
trino/achats_publics/v_achats.sql computes the same rows as achats at query time.
"""
import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, count, lit, sum as _sum, when

CATEGORIES = {"PME": "PME", "ETI": "ETI", "GE": "Grande entreprise"}
NON_RENSEIGNE = "Non renseigné"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-catalog", required=True)
    parser.add_argument("--silver-namespace", required=True)
    parser.add_argument("--referentiels-namespace", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--run-id", required=True)
    return parser.parse_args()


def build_spark(catalogs, run_id):
    client_id = os.getenv("POLARIS_CLIENT_ID", "")
    client_secret = os.getenv("POLARIS_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        raise RuntimeError("POLARIS_CLIENT_ID / POLARIS_CLIENT_SECRET are not set")
    builder = SparkSession.builder.appName(f"AchatsPublics-Gold-{run_id}")
    for catalog in catalogs:
        builder = builder.config(
            f"spark.sql.catalog.{catalog}.credential", f"{client_id}:{client_secret}"
        )
    return builder.getOrCreate()


def categorie(column):
    return coalesce(
        *[when(column == code, lit(label)) for code, label in CATEGORIES.items()],
        lit(NON_RENSEIGNE),
    )


def main():
    args = parse_args()
    spark = build_spark({args.silver_catalog, args.catalog}, args.run_id)
    spark.sparkContext.setLogLevel("WARN")

    silver = f"{args.silver_catalog}.{args.silver_namespace}"
    ref = f"{args.silver_catalog}.{args.referentiels_namespace}"
    marches = spark.table(f"{silver}.marches").select(
        "id_marche", "acheteur_siret", "objet", "procedure", "accord_cadre",
        "clause_sociale", "clause_environnementale",
    )
    acheteurs = spark.table(f"{ref}.acheteurs").select(
        "acheteur_siret", "acheteur_nom", "type_acheteur",
        col("code_departement").alias("acheteur_code_departement"),
    )
    departements = spark.table(f"{ref}.geo_departements")

    attributions = (
        spark.table(f"{silver}.attributions")
        .join(marches, ["id_marche", "acheteur_siret"])
        .join(acheteurs, "acheteur_siret", "left")
        .join(
            departements,
            col("acheteur_code_departement") == col("code_departement"),
            "left",
        )
        .select(
            "annee_notification", "mois_notification", "date_notification",
            "id_marche", "objet", "procedure", "accord_cadre",
            "clause_sociale", "clause_environnementale",
            "acheteur_siret", "acheteur_nom", "type_acheteur",
            "code_region",
            coalesce(col("libelle_region"), lit("Non déterminée")).alias("libelle_region"),
            "code_carte_region",
            col("acheteur_code_departement").alias("code_departement"),
            coalesce(col("libelle_departement"), lit("Non déterminé")).alias("libelle_departement"),
            "code_carte_departement",
            "titulaire_siret", "titulaire_nom",
            categorie(col("titulaire_categorie_entreprise")).alias("categorie_entreprise"),
            when(
                col("titulaire_code_departement").isNotNull()
                & col("acheteur_code_departement").isNotNull(),
                col("titulaire_code_departement") == col("acheteur_code_departement"),
            ).alias("achat_local"),
            "part_marche",
            col("montant_attribue").alias("montant"),
        )
    )

    achats = attributions.groupBy(
        "annee_notification", "mois_notification",
        "code_region", "libelle_region", "code_carte_region",
        "code_departement", "libelle_departement", "code_carte_departement",
        "type_acheteur", "categorie_entreprise", "achat_local",
        "procedure", "accord_cadre", "clause_sociale", "clause_environnementale",
    ).agg(
        _sum("part_marche").alias("nb_marches"),
        count("*").alias("nb_attributions"),
        _sum("montant").alias("montant"),
        _sum(when(col("montant").isNull(), col("part_marche"))).alias("nb_marches_montant_exclu"),
    )

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {args.catalog}.{args.namespace}")
    for df, name in ((attributions, "attributions"), (achats, "achats")):
        table = f"{args.catalog}.{args.namespace}.{name}"
        (
            df.writeTo(table)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .partitionedBy(col("annee_notification"))
            .createOrReplace()
        )
        print(f"{table}: {spark.table(table).count():,} rows")

    spark.table(f"{args.catalog}.{args.namespace}.achats").groupBy("annee_notification").agg(
        _sum("nb_marches").alias("marches"),
        _sum("montant").alias("montant"),
        (_sum(when(col("categorie_entreprise") == "PME", col("montant"))) / _sum("montant")).alias("part_pme"),
        (_sum(when(col("achat_local"), col("montant"))) / _sum("montant")).alias("part_local"),
    ).orderBy("annee_notification").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
