"""
Public procurement - silver conformation PySpark job
Applies the business rules to the DECP contracts and matches buyers and
suppliers with SIRENE. Publishes three Iceberg tables: the contracts, one row
per contract and supplier, and the buyers reference table.
"""
import argparse
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    array_contains,
    coalesce,
    col,
    count,
    current_date,
    exists,
    format_string,
    explode,
    lit,
    month,
    regexp_replace,
    row_number,
    size,
    sum as _sum,
    to_date,
    trim,
    upper,
    when,
    xxhash64,
    year,
)

# Business rules, stated once.
MIN_DATE = "2000-01-01"
# DECP are due from 40 000 EUR: below 100 EUR, above a billion or a run of
# nines (999 999 999 is the most common), the amount is a placeholder.
MIN_MONTANT = 100
MAX_MONTANT = 1e9
PLACEHOLDER_MONTANT = r"^9{5,}$"
NO_SOCIAL = "Pas de considération sociale"
NO_ENVIRONMENTAL = "Pas de considération environnementale"

SIRENE_DATE_CONF = {
    "spark.sql.parquet.datetimeRebaseModeInRead": "CORRECTED",
    "spark.sql.parquet.int96RebaseModeInRead": "CORRECTED",
}

# Buyer type from the INSEE legal category (nomenclature of September 2022),
# first match wins. Exact codes before prefixes.
TYPES_ACHETEUR = [
    ("État", ["71"]),
    ("Régions", ["7230"]),
    ("Départements", ["7220"]),
    ("Communes", ["7210", "7312", "7313"]),
    ("Autres collectivités territoriales", ["72"]),
    ("Intercommunalités", ["734", "7351", "7352", "7353", "7354", "7355", "7356", "7357"]),
    ("Santé et action sociale", ["7361", "7364", "7365", "7366", "7367", "7470"]),
    ("Enseignement et recherche", ["7331", "7383", "7384"]),
    ("Logement social", ["7371", "5546", "5547", "5646", "5647"]),
    ("Autres établissements publics nationaux", ["7382", "7385", "7389", "411", "412", "413", "416"]),
    ("Autres établissements publics locaux", ["73", "414", "415"]),
    ("Autres organismes publics", ["74"]),
]
TYPE_PRIVE = "Organismes de droit privé"
TYPE_INCONNU = "Non identifié dans SIRENE"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--decp", required=True, help="bronze DECP path")
    parser.add_argument("--sirene", required=True, help="bronze SIRENE path")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--referentiels-namespace", required=True)
    parser.add_argument("--annees", required=True, help="notification years, comma separated")
    parser.add_argument("--run-id", required=True)
    return parser.parse_args()


def build_spark(catalog, run_id):
    client_id = os.getenv("POLARIS_CLIENT_ID", "")
    client_secret = os.getenv("POLARIS_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        raise RuntimeError("POLARIS_CLIENT_ID / POLARIS_CLIENT_SECRET are not set")
    builder = SparkSession.builder.appName(f"AchatsPublics-Silver-{run_id}").config(
        f"spark.sql.catalog.{catalog}.credential", f"{client_id}:{client_secret}"
    )
    for key, value in SIRENE_DATE_CONF.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def siret(column):
    digits = regexp_replace(trim(column), r"[\s.]", "")
    return when(digits.rlike(r"^[0-9]{14}$"), digits)


def bounded_date(column):
    value = to_date(trim(column))
    return when(value.between(lit(MIN_DATE).cast("date"), current_date()), value)


def type_acheteur(categorie_juridique):
    rules = [
        (categorie_juridique.startswith(prefix), label)
        for label, prefixes in TYPES_ACHETEUR
        for prefix in prefixes
    ]
    expr = when(categorie_juridique.isNull(), lit(TYPE_INCONNU))
    for test, label in rules:
        expr = expr.when(test, lit(label))
    return expr.otherwise(lit(TYPE_PRIVE))


def conform_marches(raw):
    # Bronze holds 999999999 as "9.99999999E8": the rule reads the number.
    montant = trim(col("montant")).cast("double")
    return raw.select(
        trim(col("id")).alias("id_marche"),
        siret(col("acheteur.id")).alias("acheteur_siret"),
        trim(col("objet")).alias("objet"),
        trim(col("codeCPV")).alias("code_cpv"),
        trim(col("codeCPV")).substr(1, 2).alias("division_cpv"),
        col("nature"),
        col("procedure"),
        array_contains(col("techniques.technique"), "Accord-cadre").alias("accord_cadre"),
        col("formePrix").alias("forme_prix"),
        col("dureeMois").cast("int").alias("duree_mois"),
        col("offresRecues").cast("int").alias("offres_recues"),
        bounded_date(col("dateNotification")).alias("date_notification"),
        bounded_date(col("datePublicationDonnees")).alias("date_publication"),
        montant.alias("montant_declare"),
        (
            montant.isNull()
            | (montant < MIN_MONTANT)
            | (montant >= MAX_MONTANT)
            | format_string("%.0f", montant).rlike(PLACEHOLDER_MONTANT)
        ).alias("montant_aberrant"),
        coalesce(
            exists(col("considerationsSociales.considerationSociale"), lambda x: x != NO_SOCIAL),
            lit(False),
        ).alias("clause_sociale"),
        coalesce(
            exists(
                col("considerationsEnvironnementales.considerationEnvironnementale"),
                lambda x: x != NO_ENVIRONMENTAL,
            ),
            lit(False),
        ).alias("clause_environnementale"),
        col("lieuExecution.code").alias("lieu_execution_code"),
        col("lieuExecution.typeCode").alias("lieu_execution_type"),
        col("titulaires"),
        coalesce(size(col("modifications")), lit(0)).alias("nb_modifications"),
        col("source"),
        col("fichier").alias("fichier_source"),
        # Breaks the last ties between two versions of a contract, the same way at every run.
        xxhash64(col("_source_file"), col("id"), col("montant"), col("datePublicationDonnees")).alias("_version"),
    )


# Same buyer, same object, same amount, same year: the global amount of a
# framework agreement copied on each of its lots. It is split between them.
def meme_montant():
    return Window.partitionBy(
        "acheteur_siret", upper(trim(col("objet"))), "montant_declare", "annee_notification"
    )


def deduplicate(marches):
    """One row per contract: the latest published version wins.

    A contract is identified by its buyer and its id. The consolidated files
    carry it several times: republished after a modification, sent by two
    platforms, or present in two publication years.
    """
    latest = Window.partitionBy("acheteur_siret", "id_marche").orderBy(
        col("date_publication").desc_nulls_last(),
        col("fichier_source").desc(),
        col("nb_modifications").desc(),
        col("_version").desc(),
    )
    return (
        marches.withColumn("_rang", row_number().over(latest))
        .filter(col("_rang") == 1)
        .drop("_rang", "_version")
    )


def main():
    args = parse_args()
    annees = [int(a) for a in args.annees.split(",") if a.strip()]
    spark = build_spark(args.catalog, args.run_id)
    spark.sparkContext.setLogLevel("WARN")

    raw = spark.read.parquet(f"{args.decp}/marches/")
    marches = conform_marches(raw).cache()

    total = marches.count()
    valides = marches.filter(col("id_marche").isNotNull() & col("acheteur_siret").isNotNull())
    nb_valides = valides.count()
    dedoublonnes = deduplicate(valides).cache()
    nb_dedoublonnes = dedoublonnes.count()
    uniques = dedoublonnes.filter(year("date_notification").isin(annees)).cache()

    print("\nBusiness rules, measured")
    print(f"  rows in bronze:                {total:,}")
    print(f"  without buyer SIRET or id:     {total - nb_valides:,}")
    print(f"  duplicates removed:            {nb_valides - nb_dedoublonnes:,}")
    print(f"  contracts notified in {annees}: {uniques.count():,}")

    # SIRENE, restricted to the SIRETs DECP names: buyers and suppliers.
    titulaires = (
        uniques.select("id_marche", "acheteur_siret", explode("titulaires").alias("t"))
        .select(
            "id_marche",
            "acheteur_siret",
            trim(col("t.titulaire.typeIdentifiant")).alias("titulaire_type_identifiant"),
            trim(col("t.titulaire.id")).alias("titulaire_id"),
        )
        .withColumn(
            "titulaire_siret",
            when(col("titulaire_type_identifiant") == "SIRET", siret(col("titulaire_id"))),
        )
        .dropDuplicates(["id_marche", "acheteur_siret", "titulaire_type_identifiant", "titulaire_id"])
    )
    wanted = (
        uniques.select(col("acheteur_siret").alias("siret"))
        .union(titulaires.select(col("titulaire_siret").alias("siret")))
        .filter(col("siret").isNotNull())
        .distinct()
    )
    etablissements = (
        spark.read.parquet(f"{args.sirene}/sirene_etablissement/")
        .select(
            "siret",
            "siren",
            col("codeCommuneEtablissement").alias("code_commune"),
            "code_departement",
        )
        .join(wanted.hint("broadcast"), "siret")
    )
    unites = spark.read.parquet(f"{args.sirene}/sirene_unite_legale/").select(
        "siren",
        col("denominationUniteLegale").alias("denomination"),
        col("categorieJuridiqueUniteLegale").alias("categorie_juridique"),
        col("categorieEntreprise").alias("categorie_entreprise"),
    )
    sirene = etablissements.join(unites, "siren", "left").cache()

    acheteurs = (
        uniques.select("acheteur_siret")
        .distinct()
        .join(sirene, col("acheteur_siret") == col("siret"), "left")
        .select(
            "acheteur_siret",
            col("denomination").alias("acheteur_nom"),
            "categorie_juridique",
            type_acheteur(col("categorie_juridique")).alias("type_acheteur"),
            "code_commune",
            "code_departement",
        )
    )

    nb = (
        titulaires.groupBy("id_marche", "acheteur_siret")
        .agg(count("*").alias("nb_titulaires"))
    )
    marches_out = (
        uniques.drop("titulaires")
        .join(nb, ["id_marche", "acheteur_siret"], "left")
        .withColumn("nb_titulaires", coalesce(col("nb_titulaires"), lit(0)))
        .withColumn("annee_notification", year("date_notification"))
        .withColumn("mois_notification", month("date_notification"))
        .withColumn("nb_lots_meme_montant", count("*").over(meme_montant()))
        .withColumn(
            "montant",
            when(
                ~col("montant_aberrant"),
                col("montant_declare") / col("nb_lots_meme_montant"),
            ),
        )
    )

    attributions = (
        titulaires.join(
            marches_out.select(
                "id_marche", "acheteur_siret", "date_notification", "annee_notification",
                "mois_notification", "montant", "nb_titulaires",
            ),
            ["id_marche", "acheteur_siret"],
        )
        .join(
            sirene.select(
                col("siret").alias("titulaire_siret"),
                col("denomination").alias("titulaire_nom"),
                col("categorie_entreprise").alias("titulaire_categorie_entreprise"),
                col("code_commune").alias("titulaire_code_commune"),
                col("code_departement").alias("titulaire_code_departement"),
            ),
            "titulaire_siret",
            "left",
        )
        # A contract shared by several suppliers is split evenly between them:
        # sums over suppliers give back the contract amounts and counts.
        .withColumn("part_marche", lit(1.0) / col("nb_titulaires"))
        .withColumn("montant_attribue", col("montant") * col("part_marche"))
        .select(
            "id_marche", "acheteur_siret", "date_notification", "annee_notification",
            "mois_notification", "titulaire_type_identifiant", "titulaire_id", "titulaire_siret",
            "titulaire_nom", "titulaire_categorie_entreprise", "titulaire_code_commune",
            "titulaire_code_departement", "part_marche", "montant_attribue",
        )
    )

    prefix = f"{args.catalog}.{args.namespace}"
    ref = f"{args.catalog}.{args.referentiels_namespace}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {prefix}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ref}")
    for df, table, partition in (
        (marches_out, f"{prefix}.marches", "annee_notification"),
        (attributions, f"{prefix}.attributions", "annee_notification"),
        (acheteurs, f"{ref}.acheteurs", None),
    ):
        writer = df.writeTo(table).using("iceberg").tableProperty("format-version", "2")
        if partition:
            writer = writer.partitionedBy(col(partition))
        writer.createOrReplace()
        print(f"{table}: {spark.table(table).count():,} rows")

    print("\nMatching with SIRENE")
    spark.table(f"{prefix}.attributions").selectExpr(
        "count(*) as attributions",
        "count(titulaire_siret) as avec_siret",
        "count(titulaire_categorie_entreprise) as categorie_connue",
        "count(titulaire_code_departement) as localisees",
    ).show()
    spark.table(f"{ref}.acheteurs").groupBy("type_acheteur").count().orderBy(
        col("count").desc()
    ).show(20, truncate=False)
    spark.table(f"{prefix}.marches").groupBy("annee_notification").agg(
        count("*").alias("marches"),
        _sum(col("montant_aberrant").cast("int")).alias("montants_aberrants"),
        _sum((col("nb_lots_meme_montant") > 1).cast("int")).alias("montants_repartis"),
        _sum("montant").alias("montant"),
    ).orderBy("annee_notification").show()
    spark.stop()


if __name__ == "__main__":
    main()
