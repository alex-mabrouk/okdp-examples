"""
France establishments - shared identifiers and assets
What the three DAGs of the chain agree on: where bronze lands, which Iceberg
tables are published, and the assets that turn the chain into a lineage graph
rather than three DAGs run by hand.
"""
import os

from airflow.sdk import Asset

PIPELINE = "france_establishments"

BRONZE_BUCKET = os.getenv("FRANCE_ESTABLISHMENTS_BRONZE_BUCKET", "bronze")
BRONZE_PREFIX = os.getenv("FRANCE_ESTABLISHMENTS_BRONZE_PREFIX", PIPELINE)

SILVER_CATALOG = os.getenv("FRANCE_ESTABLISHMENTS_SILVER_CATALOG", "silver")
SILVER_NAMESPACE = os.getenv("FRANCE_ESTABLISHMENTS_SILVER_NAMESPACE", PIPELINE)
SILVER_TABLE = os.getenv("FRANCE_ESTABLISHMENTS_SILVER_TABLE", "etablissements_actifs")

GOLD_CATALOG = os.getenv("FRANCE_ESTABLISHMENTS_GOLD_CATALOG", "gold")
GOLD_NAMESPACE = os.getenv("FRANCE_ESTABLISHMENTS_GOLD_NAMESPACE", PIPELINE)

# Empty means national, and rewrites the whole silver table. Naming departments
# rewrites only their partitions, which is the development loop.
DEPARTEMENTS = os.getenv("FRANCE_ESTABLISHMENTS_DEPARTEMENTS", "").strip()

BRONZE_SOURCES = (
    "sirene_geoloc",
    "sirene_etablissement",
    "sirene_unite_legale",
    "ban",
)

GOLD_TABLES = (
    "etablissements_par_departement",
    "etablissements_par_section_naf",
    "etablissements_par_commune",
    "etablissements_par_categorie",
    "creations_par_mois",
)

BRONZE_ASSETS = {
    source: Asset(
        name=f"bronze_{PIPELINE}_{source}",
        uri=f"s3://{BRONZE_BUCKET}/{BRONZE_PREFIX}/{source}/",
    )
    for source in BRONZE_SOURCES
}

# Silver joins the three SIRENE files and waits for all of them. BAN is landed
# for the address enrichment to come and is deliberately not a dependency: making
# it one would block the chain on a source nothing reads yet.
SILVER_INPUTS = [
    BRONZE_ASSETS[source]
    for source in ("sirene_geoloc", "sirene_etablissement", "sirene_unite_legale")
]

SILVER_ASSET = Asset(
    name=f"silver_{SILVER_NAMESPACE}_{SILVER_TABLE}",
    uri=f"iceberg://{SILVER_CATALOG}/{SILVER_NAMESPACE}/{SILVER_TABLE}",
)

GOLD_ASSETS = [
    Asset(
        name=f"gold_{GOLD_NAMESPACE}_{table}",
        uri=f"iceberg://{GOLD_CATALOG}/{GOLD_NAMESPACE}/{table}",
    )
    for table in GOLD_TABLES
]


def departments():
    return [d.strip() for d in DEPARTEMENTS.split(",") if d.strip()]
