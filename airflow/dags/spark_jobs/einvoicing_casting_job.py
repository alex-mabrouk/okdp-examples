"""
E-invoicing - casting PySpark job
Draws the cast of the synthetic invoice flow from the establishments referential:
real SIRET, real NAF, real communes, real street addresses. Writes it once as a
frozen sample so that generating invoices never re-reads fourteen million rows,
and so that the same seed yields the same cast run after run.

Three roles come out of it:
  fournisseur         active establishments, the issuers
  acheteur            active public bodies, the receivers
  fournisseur_cesse   closed establishments, which silver filters out and the
                      demo needs: an invoice from one of them is an anomaly the
                      platform can only catch against the real referential
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    broadcast,
    coalesce,
    col,
    concat,
    lit,
    lpad,
    rand,
    trim,
)

# A real B2B flow is dominated by large issuers, and the reform's 2026/2027
# tiering only becomes visible if all three sizes are present. The natural SIRENE
# mix would give almost none: large firms are deliberately oversampled here, and
# this is one of the things the documentation has to state plainly.
SUPPLIER_MIX = {"GE": 0.15, "ETI": 0.25, "PME": 0.60}

# INSEE legal categories starting with 7 are public bodies: the State, local
# authorities, public establishments. The AIFE context wants real public buyers,
# not invented ones.
PUBLIC_LEGAL_CATEGORY_PREFIX = "7"

ACTIVE = "A"
CLOSED = "F"

CASTING_COLUMNS = [
    "role",
    "siret",
    "siren",
    "tva_intracom",
    "nom",
    "code_naf",
    "code_section_naf",
    "libelle_section_naf",
    "categorie_entreprise",
    "categorie_juridique",
    "numero_voie",
    "nom_voie",
    "code_postal",
    "code_commune",
    "libelle_commune",
    "code_departement",
    "actif",
]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--referentiel", required=True, help="catalog.namespace.table")
    parser.add_argument("--bronze", required=True, help="s3a://.../france_establishments")
    parser.add_argument("--output", required=True)
    parser.add_argument("--suppliers", type=int, default=2000)
    parser.add_argument("--buyers", type=int, default=300)
    parser.add_argument("--closed", type=int, default=150)
    parser.add_argument("--seed", type=int, required=True)
    return parser.parse_args()


def tva_intracom(siren_column):
    """The French VAT number is FR + a two-digit key + the SIREN.

    Key = (12 + 3 * (SIREN mod 97)) mod 97. Computing it rather than inventing it
    means python-stdnum validates the generated invoices, and means a deliberately
    broken key is a real anomaly rather than a made-up one.
    """
    key = (lit(12) + lit(3) * (siren_column.cast("long") % lit(97))) % lit(97)
    return concat(lit("FR"), lpad(key.cast("string"), 2, "0"), siren_column)


def sample(df, n, seed):
    return df.orderBy(rand(seed)).limit(n)


def suppliers(referentiel, count, seed):
    """One sample per size class, so the reform's tiering has something to show."""
    usable = referentiel.filter(
        col("nom_etablissement").isNotNull()
        & col("code_postal").isNotNull()
        & col("code_section_naf").isNotNull()
    )
    parts = []
    for categorie, share in SUPPLIER_MIX.items():
        target = max(1, int(count * share))
        parts.append(
            sample(usable.filter(col("categorie_entreprise") == lit(categorie)), target, seed)
        )
    drawn = parts[0]
    for part in parts[1:]:
        drawn = drawn.unionByName(part)
    return drawn.withColumn("role", lit("fournisseur"))


def buyers(referentiel, count, seed):
    public = referentiel.filter(
        col("categorie_juridique").startswith(PUBLIC_LEGAL_CATEGORY_PREFIX)
        & col("nom_etablissement").isNotNull()
        & col("code_postal").isNotNull()
    )
    return sample(public, count, seed).withColumn("role", lit("acheteur"))


def closed_suppliers(spark, bronze, count, seed):
    """Closed establishments live in bronze only: silver keeps active ones.

    Sampling before the join to the legal units keeps a thirty-million-row file
    out of the shuffle -- a hundred and fifty rows are looked up, not joined.
    """
    etablissements = (
        spark.read.parquet(f"{bronze}/sirene_etablissement/")
        .filter(col("etatAdministratifEtablissement") == lit(CLOSED))
        .filter(col("codeCommuneEtablissement").isNotNull())
        .select(
            "siret",
            "siren",
            "activitePrincipaleEtablissement",
            "enseigne1Etablissement",
            "denominationUsuelleEtablissement",
            "codeCommuneEtablissement",
            "libelleCommuneEtablissement",
            "codePostalEtablissement",
            "identifiantAdresseEtablissement",
            "code_departement",
        )
    )
    drawn = sample(etablissements, count, seed)

    unites = spark.read.parquet(f"{bronze}/sirene_unite_legale/").select(
        col("siren").alias("ul_siren"),
        "denominationUniteLegale",
        "categorieEntreprise",
        "categorieJuridiqueUniteLegale",
    )

    return (
        broadcast(drawn)
        .join(unites, drawn["siren"] == unites["ul_siren"], "left")
        .select(
            lit("fournisseur_cesse").alias("role"),
            col("siret"),
            col("siren"),
            coalesce(
                col("denominationUniteLegale"),
                col("enseigne1Etablissement"),
                col("denominationUsuelleEtablissement"),
            ).alias("nom_etablissement"),
            col("activitePrincipaleEtablissement").alias("code_naf"),
            lit(None).cast("string").alias("code_section_naf"),
            lit(None).cast("string").alias("libelle_section_naf"),
            col("categorieEntreprise").alias("categorie_entreprise"),
            col("categorieJuridiqueUniteLegale").alias("categorie_juridique"),
            col("codeCommuneEtablissement").alias("code_commune"),
            col("libelleCommuneEtablissement").alias("libelle_commune"),
            col("codePostalEtablissement").alias("code_postal"),
            col("identifiantAdresseEtablissement").alias("identifiant_adresse"),
            col("code_departement"),
        )
    )


def with_street_address(spark, cast, bronze):
    """The invoice needs a street; SIRENE stops at the commune.

    `identifiant_adresse` is the BAN key, and this is the first use that source
    has been put to since it was landed. Establishments that carry no key -- or
    whose key is not in BAN -- keep the commune alone, which stays a valid postal
    address in France.
    """
    ban = spark.read.parquet(f"{bronze}/ban/").select(
        col("id").alias("ban_id"),
        col("numero").alias("numero_voie"),
        col("nom_voie"),
    )
    return (
        broadcast(cast)
        .join(ban, cast["identifiant_adresse"] == ban["ban_id"], "left")
        .drop("ban_id", "identifiant_adresse")
    )


def main():
    args = parse_args()

    print("=" * 70)
    print("E-invoicing - casting")
    print("=" * 70)
    print(f"Referential: {args.referentiel}")
    print(f"Output:      {args.output}")
    print(f"Seed:        {args.seed}")
    print("=" * 70)

    spark = (
        SparkSession.builder.appName("EInvoicing-Casting")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    referentiel = spark.table(args.referentiel)

    active = suppliers(referentiel, args.suppliers, args.seed).unionByName(
        buyers(referentiel, args.buyers, args.seed)
    )
    active = active.select(
        "role",
        "siret",
        "siren",
        "nom_etablissement",
        "code_naf",
        "code_section_naf",
        "libelle_section_naf",
        "categorie_entreprise",
        "categorie_juridique",
        "code_commune",
        "libelle_commune",
        "code_postal",
        "identifiant_adresse",
        "code_departement",
    )

    cast = active.unionByName(closed_suppliers(spark, args.bronze, args.closed, args.seed))
    cast = with_street_address(spark, cast, args.bronze)

    cast = (
        cast.withColumn("tva_intracom", tva_intracom(col("siren")))
        .withColumn("actif", col("role") != lit("fournisseur_cesse"))
        .withColumn("nom", trim(col("nom_etablissement")))
        .select(*CASTING_COLUMNS)
    )

    cast.coalesce(1).write.mode("overwrite").parquet(args.output)

    published = spark.read.parquet(args.output)
    print(f"\nCast drawn: {published.count():,}")
    print("\nBy role:")
    published.groupBy("role").count().orderBy("role").show(truncate=False)
    print("\nSuppliers by size class:")
    (
        published.filter(col("role") == lit("fournisseur"))
        .groupBy("categorie_entreprise")
        .count()
        .orderBy(col("count").desc())
        .show(truncate=False)
    )
    print("\nStreet address found for:")
    published.selectExpr(
        "count(*) as total",
        "count(nom_voie) as avec_voie",
        "count(code_postal) as avec_code_postal",
    ).show(truncate=False)
    print("\nSample:")
    published.select(
        "role", "siret", "tva_intracom", "nom", "numero_voie", "nom_voie", "libelle_commune"
    ).show(8, truncate=False)

    print("\n" + "=" * 70)
    print("Casting completed!")
    print("=" * 70)
    spark.stop()


if __name__ == "__main__":
    main()
