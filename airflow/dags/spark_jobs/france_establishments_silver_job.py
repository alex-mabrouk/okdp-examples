"""
France establishments - silver conformation PySpark job
Joins the three SIRENE bronze tables, keeps the active establishments, applies
the conformation rules and publishes the result as an Iceberg table in a Polaris
REST catalog.
"""
import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, concat, concat_ws, current_date
from pyspark.sql.functions import date_trunc, lit, substring, trim, when

ACTIVE = "A"

# INSEE only allows the identity of a unit to be shown when it is fully
# diffusible; the other statuses ship with their name fields already emptied.
FULLY_DIFFUSIBLE = "O"

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


# "Commune sans zonage": a real code in the zoning columns, not a missing value.
NO_ZONING = "CSZ"

# SIRENE ships dates from year 0004 and year 2218. Anything before this is not
# a date, it is data entry.
MIN_PLAUSIBLE_DATE = "1800-01-01"

# NAF rév. 2 divisions to sections. Gold and Superset need a readable label:
# nothing else in the sources carries one.
NAF_SECTIONS = [
    ((1, 3), "A", "Agriculture, sylviculture et pêche"),
    ((5, 9), "B", "Industries extractives"),
    ((10, 33), "C", "Industrie manufacturière"),
    ((35, 35), "D", "Électricité, gaz, vapeur"),
    ((36, 39), "E", "Eau, assainissement, déchets"),
    ((41, 43), "F", "Construction"),
    ((45, 47), "G", "Commerce, réparation d'automobiles"),
    ((49, 53), "H", "Transports et entreposage"),
    ((55, 56), "I", "Hébergement et restauration"),
    ((58, 63), "J", "Information et communication"),
    ((64, 66), "K", "Activités financières et d'assurance"),
    ((68, 68), "L", "Activités immobilières"),
    ((69, 75), "M", "Activités spécialisées, scientifiques et techniques"),
    ((77, 82), "N", "Services administratifs et de soutien"),
    ((84, 84), "O", "Administration publique"),
    ((85, 85), "P", "Enseignement"),
    ((86, 88), "Q", "Santé humaine et action sociale"),
    ((90, 93), "R", "Arts, spectacles et activités récréatives"),
    ((94, 96), "S", "Autres activités de services"),
    ((97, 98), "T", "Activités des ménages en tant qu'employeur"),
    ((99, 99), "U", "Activités extra-territoriales"),
]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze", required=True, help="s3a://<bucket>/<prefix>")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--departments", default="")
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

    builder = (
        SparkSession.builder
        .appName(f"FranceEstablishments-Silver-{run_id}")
        .config(f"spark.sql.catalog.{catalog}.credential", f"{client_id}:{client_secret}")
    )
    for key, value in SIRENE_DATE_CONF.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def table_exists(spark, qualified_table):
    """A first departmental run has no table to overwrite partitions of."""
    try:
        return spark.catalog.tableExists(qualified_table)
    except Exception:
        return False


def bounded_date(column):
    """Null rather than a date no observatory can act on."""
    value = col(column)
    return when(
        value.between(lit(MIN_PLAUSIBLE_DATE).cast("date"), current_date()), value
    )


def naf_section():
    """Only NAF rév. 2 codes: the division ranges differ in older nomenclatures."""
    usable = col("nomenclatureActivitePrincipaleEtablissement") == lit("NAFRev2")
    division = substring(col("activitePrincipaleEtablissement"), 1, 2).cast("int")
    matches = [
        (usable & division.between(low, high), code, label)
        for (low, high), code, label in NAF_SECTIONS
    ]
    return (
        coalesce(*[when(test, lit(code)) for test, code, _ in matches]),
        coalesce(*[when(test, lit(label)) for test, _, label in matches]),
    )


# The legal units are read for the name and for two dimensions gold needs;
# the rest of the 33 columns stays in bronze.
UNITE_LEGALE_COLUMNS = [
    "siren",
    "denominationUniteLegale",
    "sigleUniteLegale",
    "nomUniteLegale",
    "nomUsageUniteLegale",
    "prenomUsuelUniteLegale",
    "prenom1UniteLegale",
    "categorieJuridiqueUniteLegale",
    "categorieEntreprise",
    "economieSocialeSolidaireUniteLegale",
    "statutDiffusionUniteLegale",
]


def nom_etablissement():
    """From the most specific label to the least: the sign on the door first.

    A sole trader has no company name, only a civil identity, which is the last
    resort and the reason this is not a single column lookup.
    """
    personne_physique = trim(
        concat_ws(
            " ",
            coalesce(col("prenomUsuelUniteLegale"), col("prenom1UniteLegale")),
            coalesce(col("nomUsageUniteLegale"), col("nomUniteLegale")),
        )
    )
    name = coalesce(
        col("enseigne1Etablissement"),
        col("denominationUsuelleEtablissement"),
        col("denominationUniteLegale"),
        when(personne_physique != lit(""), personne_physique),
    )
    return when(col("statutDiffusionUniteLegale") == lit(FULLY_DIFFUSIBLE), name)


def conform(etablissements, geoloc, unites_legales):
    section_code, section_label = naf_section()
    qpv = col("plg_qp24")
    zus = col("plg_zus")
    iris = col("plg_iris")

    return (
        etablissements.join(geoloc, on="siret", how="inner")
        # Left: an establishment must not vanish because its legal unit is
        # missing from the stock.
        .join(unites_legales, on="siren", how="left")
        .select(
            col("siret"),
            col("siren"),
            col("nic"),
            col("etablissementSiege").alias("est_siege"),
            col("activitePrincipaleEtablissement").alias("code_naf"),
            col("activitePrincipaleNAF25Etablissement").alias("code_naf25"),
            col("nomenclatureActivitePrincipaleEtablissement").alias("nomenclature_naf"),
            section_code.alias("code_section_naf"),
            section_label.alias("libelle_section_naf"),
            col("enseigne1Etablissement").alias("enseigne"),
            col("denominationUsuelleEtablissement").alias("denomination_usuelle"),
            col("denominationUniteLegale").alias("denomination_unite_legale"),
            col("sigleUniteLegale").alias("sigle"),
            nom_etablissement().alias("nom_etablissement"),
            col("categorieJuridiqueUniteLegale").alias("categorie_juridique"),
            col("categorieEntreprise").alias("categorie_entreprise"),
            (col("economieSocialeSolidaireUniteLegale") == lit("O")).alias("est_ess"),
            col("trancheEffectifsEtablissement").alias("tranche_effectifs"),
            col("anneeEffectifsEtablissement").alias("annee_effectifs"),
            bounded_date("dateCreationEtablissement").alias("date_creation"),
            bounded_date("dateDebut").alias("date_debut"),
            date_trunc("month", bounded_date("dateCreationEtablissement"))
            .cast("date")
            .alias("mois_creation"),
            col("codeCommuneEtablissement").alias("code_commune"),
            col("libelleCommuneEtablissement").alias("libelle_commune"),
            col("codePostalEtablissement").alias("code_postal"),
            col("identifiantAdresseEtablissement").alias("identifiant_adresse"),
            # float in the source: everything downstream reads a double.
            col("y_latitude").cast("double").alias("latitude"),
            col("x_longitude").cast("double").alias("longitude"),
            col("qualite_xy"),
            col("distance_precision"),
            # plg_iris is local to the commune; the national IRIS code is the
            # concatenation.
            when(
                iris.isNotNull() & (iris != lit(NO_ZONING)),
                concat(col("plg_code_commune"), iris),
            ).alias("code_iris"),
            # Most filled plg_qp24 values are HZ, "hors zone": being in a QPV is
            # the Q prefix, not the absence of a null.
            coalesce(qpv.startswith("Q"), lit(False)).alias("en_qpv"),
            when(qpv.startswith("Q"), qpv).alias("code_qpv"),
            when(zus.isNotNull() & (zus != lit(NO_ZONING)), zus).alias("code_zus"),
            # From the administrative commune, not the geocoded one: the two can
            # disagree, and the partition follows the SIRENE address of record.
            col("code_departement"),
        )
    )


def main():
    args = parse_args()
    qualified_table = f"{args.catalog}.{args.namespace}.{args.table}"
    departments = [d.strip() for d in args.departments.split(",") if d.strip()]

    print("=" * 70)
    print("Silver conformation - active geolocated establishments")
    print("=" * 70)
    print(f"Bronze: {args.bronze}")
    print(f"Table:  {qualified_table}")
    print(f"Scope:  {', '.join(departments) if departments else 'national'}")
    print("=" * 70)

    spark = build_spark(args.catalog, args.run_id)
    spark.sparkContext.setLogLevel("WARN")

    etablissements = spark.read.parquet(f"{args.bronze}/sirene_etablissement/").filter(
        col("etatAdministratifEtablissement") == lit(ACTIVE)
    )
    if departments:
        etablissements = etablissements.filter(col("code_departement").isin(departments))

    # Read the geolocation nationally even for a single department: an
    # establishment can be geocoded in a commune of another one, and a
    # department-aligned join would drop it without saying so.
    geoloc = spark.read.parquet(f"{args.bronze}/sirene_geoloc/").drop("code_departement")

    unites_legales = spark.read.parquet(f"{args.bronze}/sirene_unite_legale/").select(
        *UNITE_LEGALE_COLUMNS
    )

    silver = conform(etablissements, geoloc, unites_legales)

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {args.catalog}.{args.namespace}")
    writer = silver.writeTo(qualified_table)

    if departments and table_exists(spark, qualified_table):
        print(f"\nReplacing {len(departments)} partition(s) of {qualified_table}...")
        writer.overwritePartitions()
    else:
        print(f"\nPublishing {qualified_table}...")
        (
            writer.using("iceberg")
            .partitionedBy(col("code_departement"))
            .tableProperty("format-version", "2")
            # One file set per department rather than one per shuffle partition.
            .tableProperty("write.distribution-mode", "hash")
            .createOrReplace()
        )

    published = spark.table(qualified_table)
    print(f"\nRows in {qualified_table}: {published.count():,}")

    print("\nTop departments:")
    (
        published.groupBy("code_departement")
        .count()
        .orderBy(col("count").desc())
        .show(10, truncate=False)
    )

    print("\nConformation rules, measured:")
    published.selectExpr(
        "count(*) as total",
        "sum(cast(en_qpv as int)) as en_qpv",
        "count(code_iris) as avec_iris",
        "count(date_creation) as date_creation_plausible",
        "count(code_section_naf) as avec_section_naf",
        "count(nom_etablissement) as avec_nom",
    ).show(truncate=False)

    print("\nSnapshot history:")
    spark.sql(
        f"SELECT snapshot_id, committed_at, operation FROM {qualified_table}.snapshots"
    ).show(truncate=False)

    print("\n" + "=" * 70)
    print("Silver conformation completed!")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()
