"""
Public procurement - shared identifiers and assets
What the DAGs of the chain agree on: where bronze lands, which Iceberg tables are
published, and the assets that chain the DAGs.
"""
import os

from airflow.sdk import Asset
from france_establishments_assets import BRONZE_ASSETS as SIRENE_BRONZE_ASSETS
from france_establishments_assets import BRONZE_PREFIX as SIRENE_PREFIX

PIPELINE = "achats_publics"

BRONZE_BUCKET = os.getenv("ACHATS_PUBLICS_BRONZE_BUCKET", "bronze")
BRONZE_PREFIX = os.getenv("ACHATS_PUBLICS_BRONZE_PREFIX", PIPELINE)

SILVER_CATALOG = os.getenv("ACHATS_PUBLICS_SILVER_CATALOG", "silver")
SILVER_NAMESPACE = os.getenv("ACHATS_PUBLICS_SILVER_NAMESPACE", PIPELINE)
REFERENTIELS_NAMESPACE = os.getenv("ACHATS_PUBLICS_REFERENTIELS_NAMESPACE", "referentiels")

GOLD_CATALOG = os.getenv("ACHATS_PUBLICS_GOLD_CATALOG", "gold")
GOLD_NAMESPACE = os.getenv("ACHATS_PUBLICS_GOLD_NAMESPACE", PIPELINE)

# Notification years kept in silver. The consolidated files are cut by
# publication year, so a contract notified in 2025 may sit in the 2026 file, and
# the current year stays incomplete for months.
ANNEES = os.getenv("ACHATS_PUBLICS_ANNEES", "2024,2025,2026")

# data.gouv resource ids of the consolidated DECP files (Ministères économiques
# et financiers), one per publication year.
DECP_URLS = [
    u.strip()
    for u in os.getenv(
        "ACHATS_PUBLICS_DECP_URLS",
        "https://www.data.gouv.fr/api/1/datasets/r/4fafdaff-b697-4494-9523-e9f56916fea8,"
        "https://www.data.gouv.fr/api/1/datasets/r/d00a6a5a-beef-442e-8aee-5867f47a87d0,"
        "https://www.data.gouv.fr/api/1/datasets/r/2551ad40-584a-42fd-b3cc-e8906183287e",
    ).split(",")
    if u.strip()
]

# Code officiel géographique 2026 (INSEE): communes, departments, regions.
COG_URLS = {
    "commune": os.getenv(
        "ACHATS_PUBLICS_COG_COMMUNE_URL",
        "https://www.data.gouv.fr/api/1/datasets/r/c5800591-813b-4ce2-9a3f-739a9c5d9558",
    ),
    "departement": os.getenv(
        "ACHATS_PUBLICS_COG_DEPARTEMENT_URL",
        "https://www.data.gouv.fr/api/1/datasets/r/e5f32177-5841-4a46-9c17-f8ff94bd38fc",
    ),
    "region": os.getenv(
        "ACHATS_PUBLICS_COG_REGION_URL",
        "https://www.data.gouv.fr/api/1/datasets/r/f7057d4e-0078-45de-95b7-2a0ce6594807",
    ),
}

BRONZE_SOURCES = ("decp", "cog")

BRONZE_ASSETS = {
    source: Asset(
        name=f"bronze_{PIPELINE}_{source}",
        uri=f"s3://{BRONZE_BUCKET}/{BRONZE_PREFIX}/{source}/",
    )
    for source in BRONZE_SOURCES
}

# SIRENE is landed once, by the france_establishments chain, and read here.
SIRENE_BRONZE = f"s3a://{BRONZE_BUCKET}/{SIRENE_PREFIX}"
SIRENE_INPUTS = [SIRENE_BRONZE_ASSETS[s] for s in ("sirene_etablissement", "sirene_unite_legale")]


def _iceberg(catalog, namespace, table):
    return Asset(
        name=f"{catalog}_{namespace}_{table}",
        uri=f"iceberg://{catalog}/{namespace}/{table}",
    )


REFERENTIELS_TABLES = ("geo_communes", "geo_departements")
REFERENTIELS_ASSETS = [
    _iceberg(SILVER_CATALOG, REFERENTIELS_NAMESPACE, t) for t in REFERENTIELS_TABLES
]

SILVER_TABLES = ("marches", "attributions")
SILVER_ASSETS = [_iceberg(SILVER_CATALOG, SILVER_NAMESPACE, t) for t in SILVER_TABLES]
ACHETEURS_ASSET = _iceberg(SILVER_CATALOG, REFERENTIELS_NAMESPACE, "acheteurs")

GOLD_TABLES = ("attributions", "achats")
GOLD_ASSETS = [_iceberg(GOLD_CATALOG, GOLD_NAMESPACE, t) for t in GOLD_TABLES]
