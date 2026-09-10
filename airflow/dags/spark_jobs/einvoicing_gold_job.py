"""
E-invoicing - gold indicators PySpark job
Aggregates the checked invoices into the six tables the dashboard reads.

Two counting rules run through all of them, and both came out of a measurement:

  anomalies are counted in invoices, never in findings
      one malformed invoice breaks four Schematron rules at once. Counting
      findings would put the anomaly rate at 12 % where it is 7 %, and the higher
      number would be the wrong one

  the unit is the document received, not the invoice issued
      a duplicate is two documents carrying one invoice. Deduplicating here would
      hide exactly what the platform is meant to surface, so the totals count what
      arrived and `qualite_anomalies` carries the amount at stake in the duplicates

`conformite_reforme` is the table the rest exists for: the issuer's size class
comes from SIRENE, and the reform keys its September 2026 and September 2027
deadlines on it, so the flow can be split into what is already mandatory and what
is not yet.
"""
import argparse
import os

from france_departements import code_carte, libelle_departement
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# The reform: reception became mandatory for everyone on 1 September 2026, and
# issuing follows the size of the issuer.
OBLIGATION_EMISSION = {
    "GE": "2026-09-01",
    "ETI": "2026-09-01",
    "PME": "2027-09-01",
}
OBLIGATION_INCONNUE = "indéterminée"

TABLES = (
    "facturation_mensuelle",
    "facturation_par_departement",
    "facturation_par_section_naf",
    "acteurs",
    "qualite_anomalies",
    "conformite_reforme",
)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-catalog", required=True)
    parser.add_argument("--source-namespace", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--run-id", required=True)
    return parser.parse_args()


def build_spark(catalogs, run_id):
    client_id = os.getenv("POLARIS_CLIENT_ID", "")
    client_secret = os.getenv("POLARIS_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        raise RuntimeError("POLARIS_CLIENT_ID / POLARIS_CLIENT_SECRET are not set")

    builder = SparkSession.builder.appName(f"EInvoicing-Gold-{run_id}")
    for catalog in catalogs:
        builder = builder.config(
            f"spark.sql.catalog.{catalog}.credential", f"{client_id}:{client_secret}"
        )
    return builder.getOrCreate()


def _round(column, digits=2):
    return F.round(column, digits)


def _part(numerateur, denominateur):
    """Shares are stored as fractions and rendered as percentages by the chart.

    The unit then sits on the number rather than in a subtitle, and a share is
    never the average of other shares.
    """
    return _round(numerateur / denominateur, 4)


def marquer(factures, anomalies):
    """One row per invoice, with what the controls found on it.

    The join is on the fingerprint rather than the invoice number: two documents
    carrying the same number are two rows here, which is what makes the duplicate
    visible instead of collapsing it.
    """
    par_facture = anomalies.groupBy("empreinte").agg(
        F.count("*").alias("nb_constats"),
        F.countDistinct("regle_id").alias("nb_regles"),
        F.max(F.when(F.col("gravite") == F.lit("bloquante"), 1).otherwise(0)).alias(
            "bloquante"
        ),
        F.concat_ws(", ", F.sort_array(F.collect_set("famille"))).alias("familles"),
    )
    return (
        factures.join(par_facture, "empreinte", "left")
        .withColumn("nb_constats", F.coalesce(F.col("nb_constats"), F.lit(0)))
        .withColumn("bloquante", F.coalesce(F.col("bloquante"), F.lit(0)))
        .withColumn("en_anomalie", (F.col("nb_constats") > 0).cast("int"))
    )


def par_mois(factures):
    return (
        factures.groupBy("mois")
        .agg(
            F.count("*").alias("nb_factures"),
            F.countDistinct("siren_emetteur").alias("nb_emetteurs"),
            F.countDistinct("siret_acheteur").alias("nb_acheteurs"),
            _round(F.sum("montant_ht")).alias("montant_ht"),
            _round(F.sum("montant_tva")).alias("montant_tva"),
            _round(F.sum("montant_ttc")).alias("montant_ttc"),
            F.sum("en_anomalie").alias("nb_factures_anomalie"),
            F.sum("bloquante").alias("nb_factures_bloquantes"),
        )
        .withColumn(
            "taux_anomalie", _part(F.col("nb_factures_anomalie"), F.col("nb_factures"))
        )
        .withColumn("montant_moyen", _round(F.col("montant_ht") / F.col("nb_factures")))
        # Superset needs a temporal column to draw a time axis, and `mois` is the
        # partition key, a string. The first of the month is the month.
        .withColumn("mois_date", F.to_date(F.concat(F.col("mois"), F.lit("-01"))))
        .orderBy("mois")
    )


def par_departement(factures):
    """Keyed on the issuer's department, which is where the activity is."""
    return (
        factures.filter(F.col("code_departement_emetteur").isNotNull())
        .withColumnRenamed("code_departement_emetteur", "code_departement")
        .groupBy("code_departement")
        .agg(
            F.count("*").alias("nb_factures"),
            F.countDistinct("siren_emetteur").alias("nb_emetteurs"),
            _round(F.sum("montant_ht")).alias("montant_ht"),
            _round(F.sum("montant_ttc")).alias("montant_ttc"),
            F.sum("en_anomalie").alias("nb_factures_anomalie"),
        )
        .withColumn(
            "taux_anomalie", _part(F.col("nb_factures_anomalie"), F.col("nb_factures"))
        )
        .withColumn("libelle_departement", libelle_departement())
        # Superset's France map keys on FR-<code>, and on ISO letters overseas.
        .withColumn("code_carte", code_carte())
        .orderBy(F.col("montant_ht").desc())
    )


def par_section_naf(factures):
    return (
        factures.filter(F.col("code_section_naf_emetteur").isNotNull())
        .groupBy(
            F.col("code_section_naf_emetteur").alias("code_section_naf"),
            F.col("libelle_section_naf_emetteur").alias("libelle_section_naf"),
        )
        .agg(
            F.count("*").alias("nb_factures"),
            F.countDistinct("siren_emetteur").alias("nb_emetteurs"),
            _round(F.sum("montant_ht")).alias("montant_ht"),
            _round(F.sum("montant_ttc")).alias("montant_ttc"),
            F.sum("en_anomalie").alias("nb_factures_anomalie"),
        )
        .withColumn("montant_moyen", _round(F.col("montant_ht") / F.col("nb_factures")))
        .withColumn(
            "taux_anomalie", _part(F.col("nb_factures_anomalie"), F.col("nb_factures"))
        )
        .orderBy(F.col("montant_ht").desc())
    )


def acteurs(factures):
    """Issuers and receivers in one table, told apart by a column.

    One dataset and one filter rather than two of everything: the dashboard shows
    the same chart for either side, and a question about a company finds it
    whichever end of the invoice it sits on.
    """
    emetteurs = factures.select(
        F.lit("émetteur").alias("role"),
        F.col("siren_emetteur").alias("siren"),
        F.col("siret_emetteur").alias("siret"),
        F.col("nom_emetteur").alias("nom"),
        F.col("code_departement_emetteur").alias("code_departement"),
        F.col("code_section_naf_emetteur").alias("code_section_naf"),
        F.col("categorie_entreprise_emetteur").alias("categorie_entreprise"),
        "montant_ht",
        "montant_ttc",
        "en_anomalie",
    )
    acheteurs = factures.select(
        F.lit("acheteur").alias("role"),
        F.col("siren_acheteur").alias("siren"),
        F.col("siret_acheteur").alias("siret"),
        F.col("nom_acheteur").alias("nom"),
        F.col("code_departement_acheteur").alias("code_departement"),
        F.lit(None).cast("string").alias("code_section_naf"),
        F.lit(None).cast("string").alias("categorie_entreprise"),
        "montant_ht",
        "montant_ttc",
        "en_anomalie",
    )
    return (
        emetteurs.unionByName(acheteurs)
        .filter(F.col("siren").isNotNull())
        .groupBy("role", "siren", "siret", "nom", "code_departement", "code_section_naf", "categorie_entreprise")
        .agg(
            F.count("*").alias("nb_factures"),
            _round(F.sum("montant_ht")).alias("montant_ht"),
            _round(F.sum("montant_ttc")).alias("montant_ttc"),
            F.sum("en_anomalie").alias("nb_factures_anomalie"),
        )
        .withColumn("libelle_departement", libelle_departement())
        .orderBy(F.col("montant_ht").desc())
    )


def qualite(factures, anomalies):
    """One row per rule: how many invoices it caught, and what they were worth.

    `montant_impacte` is what makes the table readable by someone who does not
    care about rule identifiers: the duplicate line answers "how much would have
    been paid twice", which is the only form of the question that gets an answer
    in a meeting.
    """
    total = factures.count()
    return (
        anomalies.groupBy("regle_id", "famille", "gravite", "libelle")
        .agg(
            F.count("*").alias("nb_constats"),
            F.countDistinct("empreinte").alias("nb_factures"),
            _round(F.sum("montant_ttc")).alias("montant_impacte"),
        )
        .withColumn("part_factures", _part(F.col("nb_factures"), F.lit(total)))
        .orderBy(F.col("nb_factures").desc())
    )


def conformite_reforme(factures):
    """The flow read through the calendar of the reform.

    Reception has been mandatory for every French company subject to VAT since
    1 September 2026, with no tiering. Issuing is tiered on company size, which is
    a SIRENE attribute -- so the split below is the referential speaking, not an
    assumption about the flow.
    """
    obligation = F.create_map(
        *[item for pair in OBLIGATION_EMISSION.items() for item in (F.lit(pair[0]), F.lit(pair[1]))]
    )
    categorie = F.when(
        F.col("categorie_entreprise_emetteur").isin(list(OBLIGATION_EMISSION)),
        F.col("categorie_entreprise_emetteur"),
    ).otherwise(F.lit("NON RENSEIGNÉE"))

    total = factures.count()
    return (
        factures.withColumn("categorie_entreprise", categorie)
        .withColumn(
            "obligation_emission",
            F.coalesce(obligation[F.col("categorie_entreprise")], F.lit(OBLIGATION_INCONNUE)),
        )
        .groupBy("categorie_entreprise", "obligation_emission")
        .agg(
            F.count("*").alias("nb_factures"),
            F.countDistinct("siren_emetteur").alias("nb_emetteurs"),
            _round(F.sum("montant_ht")).alias("montant_ht"),
            F.sum("en_anomalie").alias("nb_factures_anomalie"),
        )
        .withColumn("part_factures", _part(F.col("nb_factures"), F.lit(total)))
        .withColumn(
            "taux_anomalie", _part(F.col("nb_factures_anomalie"), F.col("nb_factures"))
        )
        .orderBy("obligation_emission", F.col("nb_factures").desc())
    )


def main():
    args = parse_args()

    print("=" * 70)
    print("E-invoicing - gold")
    print("=" * 70)
    print(f"Source: {args.source_catalog}.{args.source_namespace}")
    print(f"Target: {args.catalog}.{args.namespace}")
    print("=" * 70)

    spark = build_spark({args.source_catalog, args.catalog}, args.run_id)
    spark.sparkContext.setLogLevel("WARN")

    source = f"{args.source_catalog}.{args.source_namespace}"
    factures = spark.table(f"{source}.factures")
    anomalies = spark.table(f"{source}.anomalies")

    marquees = marquer(factures, anomalies).cache()
    print(f"\nInvoices read: {marquees.count():,}")

    tables = {
        "facturation_mensuelle": par_mois(marquees),
        "facturation_par_departement": par_departement(marquees),
        "facturation_par_section_naf": par_section_naf(marquees),
        "acteurs": acteurs(marquees),
        "qualite_anomalies": qualite(marquees, anomalies),
        "conformite_reforme": conformite_reforme(marquees),
    }

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {args.catalog}.{args.namespace}")
    for nom in TABLES:
        cible = f"{args.catalog}.{args.namespace}.{nom}"
        print(f"\nPublishing {cible}...")
        (
            tables[nom]
            .writeTo(cible)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .createOrReplace()
        )
        print(f"  {spark.table(cible).count():,} row(s)")

    print("\nHeadline figures:")
    marquees.selectExpr(
        "count(*) as documents",
        "count(distinct numero_facture) as factures",
        "round(sum(montant_ht), 2) as montant_ht",
        "round(sum(montant_tva), 2) as montant_tva",
        "round(sum(montant_ttc), 2) as montant_ttc",
        "sum(en_anomalie) as en_anomalie",
        "round(sum(en_anomalie) / count(*), 4) as taux_anomalie",
    ).show(truncate=False)

    print("\nThe reform's calendar, read on this flow:")
    spark.table(f"{args.catalog}.{args.namespace}.conformite_reforme").show(
        truncate=False
    )

    print("\nWhat the controls caught, in invoices:")
    spark.table(f"{args.catalog}.{args.namespace}.qualite_anomalies").select(
        "regle_id", "famille", "gravite", "nb_factures", "nb_constats", "montant_impacte"
    ).show(20, truncate=False)

    print("\n" + "=" * 70)
    print("Gold completed!")
    print("=" * 70)
    spark.stop()


if __name__ == "__main__":
    main()
