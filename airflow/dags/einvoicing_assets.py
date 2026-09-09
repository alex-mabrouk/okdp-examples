"""
E-invoicing - shared identifiers and assets
What the five DAGs of the chain agree on: where the invoices land, which Iceberg
tables are published, and the assets that turn the chain into a lineage graph.

It imports the establishments referential rather than restating it: that single
import is what makes the two chains one graph in the Airflow UI. The referential
is read, never awaited -- see the note on scheduling below.
"""
import os

from airflow.sdk import Asset
from france_establishments_assets import BRONZE_ASSETS as SIRENE_BRONZE_ASSETS
from france_establishments_assets import SILVER_ASSET as REFERENTIEL_ASSET

PIPELINE = "einvoicing"

BRONZE_BUCKET = os.getenv("EINVOICING_BRONZE_BUCKET", "bronze")
BRONZE_PREFIX = os.getenv("EINVOICING_BRONZE_PREFIX", PIPELINE)

# The platform provisions hive, bronze, silver, gold and spark-events, and nothing
# else. Landing under bronze/<pipeline>/_source/ is the convention the
# establishments chain already follows, so no new bucket has to be granted.
SOURCE_PREFIX = f"{BRONZE_PREFIX}/_source"
CASTING_PREFIX = f"{BRONZE_PREFIX}/_casting"
TRUTH_PREFIX = f"{BRONZE_PREFIX}/_verite"

SILVER_CATALOG = os.getenv("EINVOICING_SILVER_CATALOG", "silver")
SILVER_NAMESPACE = os.getenv("EINVOICING_SILVER_NAMESPACE", PIPELINE)
GOLD_CATALOG = os.getenv("EINVOICING_GOLD_CATALOG", "gold")
GOLD_NAMESPACE = os.getenv("EINVOICING_GOLD_NAMESPACE", PIPELINE)

# Writing Factur-X needs a PDF engine, and validating it needs the XSD and the
# Schematrons of the standard. Neither is in the platform image; this thin layer
# over it carries both. Only the generate and silver jobs use it.
EINVOICING_IMAGE = os.getenv(
    "EINVOICING_SPARK_IMAGE",
    "ghcr.io/alex-mabrouk/okdp-examples-spark-einvoicing:0.2.0",
)

# --- Generation parameters -------------------------------------------------
# The seed is what makes a run reproducible: same seed, same invoices, same
# injected anomalies, so the demo can be rehearsed and the controls compared to
# the ground truth run after run.
SEED = int(os.getenv("EINVOICING_SEED", "20260909"))
INVOICE_COUNT = int(os.getenv("EINVOICING_INVOICE_COUNT", "20000"))
# The months invoices are spread over, ending with the month before the run.
MONTHS = int(os.getenv("EINVOICING_MONTHS", "24"))
# A readable PDF is what a demo shows; 20 000 of them is what nobody watches.
# The subset carries every anomaly worth putting on screen.
PDF_COUNT = int(os.getenv("EINVOICING_PDF_COUNT", "500"))
# EN 16931 is the socle of the reform. EXTENDED-CTC-FR is optional and richer:
# a minority of the flow uses it, which is what makes it worth showing.
EXTENDED_CTC_FR_RATE = float(os.getenv("EINVOICING_EXTENDED_RATE", "0.05"))
ANOMALY_RATE = float(os.getenv("EINVOICING_ANOMALY_RATE", "0.06"))

CASTING_SUPPLIERS = int(os.getenv("EINVOICING_CASTING_SUPPLIERS", "2000"))
CASTING_BUYERS = int(os.getenv("EINVOICING_CASTING_BUYERS", "300"))
CASTING_CLOSED = int(os.getenv("EINVOICING_CASTING_CLOSED", "150"))

# --- Assets ----------------------------------------------------------------
RAW_ASSET = Asset(
    name=f"raw_{PIPELINE}_factures",
    uri=f"s3://{BRONZE_BUCKET}/{SOURCE_PREFIX}/factures/",
)

BRONZE_ASSET = Asset(
    name=f"bronze_{PIPELINE}_factures_cii",
    uri=f"s3://{BRONZE_BUCKET}/{BRONZE_PREFIX}/factures_cii/",
)

SILVER_TABLES = ("factures", "lignes", "anomalies")

SILVER_ASSETS = {
    table: Asset(
        name=f"silver_{SILVER_NAMESPACE}_{table}",
        uri=f"iceberg://{SILVER_CATALOG}/{SILVER_NAMESPACE}/{table}",
    )
    for table in SILVER_TABLES
}

GOLD_TABLES = (
    "facturation_mensuelle",
    "facturation_par_departement",
    "facturation_par_section_naf",
    "acteurs",
    "qualite_anomalies",
    "conformite_reforme",
)

GOLD_ASSETS = [
    Asset(
        name=f"gold_{GOLD_NAMESPACE}_{table}",
        uri=f"iceberg://{GOLD_CATALOG}/{GOLD_NAMESPACE}/{table}",
    )
    for table in GOLD_TABLES
]

AI_TABLES = ("insights_facts", "insights")

AI_ASSETS = [
    Asset(
        name=f"gold_{GOLD_NAMESPACE}_{table}",
        uri=f"iceberg://{GOLD_CATALOG}/{GOLD_NAMESPACE}/{table}",
    )
    for table in AI_TABLES
]

# --- The referential -------------------------------------------------------
# Invoices are drawn from, and checked against, the establishments chain:
#   REFERENTIEL_ASSET             active establishments -- issuers, buyers, NAF
#   SIRENE_ETABLISSEMENT_ASSET    every state, including closed ones, which
#                                 silver filters out and the demo needs
#   BAN_ASSET                     the street address SIRENE does not carry;
#                                 the first consumer this source has had
#
# These belong in `inlets`, never in `schedule`. Airflow's asset scheduling is an
# AND: a DAG scheduled on [its own input, REFERENTIEL_ASSET] would sit and wait
# for the SIRENE chain to republish before it ever ran again.
SIRENE_ETABLISSEMENT_ASSET = SIRENE_BRONZE_ASSETS["sirene_etablissement"]
BAN_ASSET = SIRENE_BRONZE_ASSETS["ban"]

REFERENTIEL_INLETS = [REFERENTIEL_ASSET, SIRENE_ETABLISSEMENT_ASSET, BAN_ASSET]

OLLAMA_URL = os.getenv(
    "EINVOICING_OLLAMA_URL", "http://demo-ollama-main.demo.svc.cluster.local:11434"
)
OLLAMA_MODEL = os.getenv("EINVOICING_OLLAMA_MODEL", "mistral:7b")


def s3_path(prefix):
    return f"s3a://{BRONZE_BUCKET}/{prefix}"
