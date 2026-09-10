# Build the Trino->gold database and the einvoicing datasets in Superset.
import json

from superset import db
from superset.connectors.sqla.models import SqlaTable, SqlMetric, TableColumn
from superset.models.core import Database

DB_NAME = "trino-gold"
URI = "trino://trino@trino-demo.okdp.sandbox:443/gold"
SCHEMA = "einvoicing"
EXTRA = json.dumps(
    {
        "engine_params": {},
        "metadata_params": {},
        "connect_args": {"http_scheme": "https", "verify": False},
        "schemas_allowed_for_file_upload": [],
    }
)

INT = ",d"
# Shares are stored as fractions by their metric and rendered as percentages, so
# the unit is on the number instead of being explained in a subtitle.
PCT = ".1%"
# 1.4M rather than 1 402 118: an amount that large is read, not audited. The euro
# sign lives in the metric label, since d3 formats carry no currency.
COMPACT = ".3~s"
EURO = ",.0f"


def upsert_db():
    """Reuse the connection the platform provisions; create it only if absent."""
    d = db.session.query(Database).filter_by(database_name=DB_NAME).first()
    if not d:
        d = Database(database_name=DB_NAME)
        d.sqlalchemy_uri = URI
        d.extra = EXTRA
        d.impersonate_user = True
        d.allow_ctas = d.allow_cvas = d.allow_dml = False
        d.expose_in_sqllab = True
        db.session.add(d)
        db.session.commit()
    return d


# table -> (columns[(name, type, is_dttm)], metrics[(name, expr, label, format)])
SPECS = {
    "facturation_mensuelle": (
        [
            ("mois", "VARCHAR", False),
            ("mois_date", "DATE", True),
            ("nb_factures", "BIGINT", False),
            ("nb_emetteurs", "BIGINT", False),
            ("nb_acheteurs", "BIGINT", False),
            ("montant_ht", "DECIMAL", False),
            ("montant_tva", "DECIMAL", False),
            ("montant_ttc", "DECIMAL", False),
            ("nb_factures_anomalie", "BIGINT", False),
            ("nb_factures_bloquantes", "BIGINT", False),
            ("taux_anomalie", "DOUBLE", False),
            ("montant_moyen", "DECIMAL", False),
        ],
        [
            ("nb_factures_sum", "SUM(nb_factures)", "Factures reçues", INT),
            ("montant_ht_sum", "SUM(montant_ht)", "Montant HT (€)", COMPACT),
            ("montant_tva_sum", "SUM(montant_tva)", "Montant TVA (€)", COMPACT),
            ("montant_ttc_sum", "SUM(montant_ttc)", "Montant TTC (€)", COMPACT),
            # Recomputed from the counts, never averaged: a mean of monthly rates
            # would weight a quiet August like a busy March.
            (
                "taux_anomalie_pond",
                "1.0 * SUM(nb_factures_anomalie) / SUM(nb_factures)",
                "Taux d'anomalie",
                PCT,
            ),
            (
                "nb_bloquantes_sum",
                "SUM(nb_factures_bloquantes)",
                "Factures non recevables",
                INT,
            ),
        ],
    ),
    "facturation_par_departement": (
        [
            ("code_departement", "VARCHAR", False),
            ("code_carte", "VARCHAR", False),
            ("libelle_departement", "VARCHAR", False),
            ("nb_factures", "BIGINT", False),
            ("nb_emetteurs", "BIGINT", False),
            ("montant_ht", "DECIMAL", False),
            ("montant_ttc", "DECIMAL", False),
            ("nb_factures_anomalie", "BIGINT", False),
            ("taux_anomalie", "DOUBLE", False),
        ],
        [
            ("nb_factures_sum", "SUM(nb_factures)", "Factures reçues", INT),
            ("montant_ht_sum", "SUM(montant_ht)", "Montant HT (€)", COMPACT),
            ("nb_emetteurs_sum", "SUM(nb_emetteurs)", "Émetteurs", INT),
            (
                "taux_anomalie_pond",
                "1.0 * SUM(nb_factures_anomalie) / SUM(nb_factures)",
                "Taux d'anomalie",
                PCT,
            ),
        ],
    ),
    "facturation_par_section_naf": (
        [
            ("code_section_naf", "VARCHAR", False),
            ("libelle_section_naf", "VARCHAR", False),
            ("nb_factures", "BIGINT", False),
            ("nb_emetteurs", "BIGINT", False),
            ("montant_ht", "DECIMAL", False),
            ("montant_ttc", "DECIMAL", False),
            ("nb_factures_anomalie", "BIGINT", False),
            ("montant_moyen", "DECIMAL", False),
            ("taux_anomalie", "DOUBLE", False),
        ],
        [
            ("nb_factures_sum", "SUM(nb_factures)", "Factures reçues", INT),
            ("montant_ht_sum", "SUM(montant_ht)", "Montant HT (€)", COMPACT),
            (
                "montant_moyen_pond",
                "SUM(montant_ht) / SUM(nb_factures)",
                "Facture moyenne (€)",
                EURO,
            ),
            (
                "taux_anomalie_pond",
                "1.0 * SUM(nb_factures_anomalie) / SUM(nb_factures)",
                "Taux d'anomalie",
                PCT,
            ),
        ],
    ),
    # Issuers and receivers in one dataset, told apart by `role`: one chart serves
    # either side, and a company is found whichever end of the invoice it sits on.
    "acteurs": (
        [
            ("role", "VARCHAR", False),
            ("siren", "VARCHAR", False),
            ("siret", "VARCHAR", False),
            ("nom", "VARCHAR", False),
            ("code_departement", "VARCHAR", False),
            ("libelle_departement", "VARCHAR", False),
            ("code_section_naf", "VARCHAR", False),
            ("categorie_entreprise", "VARCHAR", False),
            ("nb_factures", "BIGINT", False),
            ("montant_ht", "DECIMAL", False),
            ("montant_ttc", "DECIMAL", False),
            ("nb_factures_anomalie", "BIGINT", False),
        ],
        [
            ("nb_factures_sum", "SUM(nb_factures)", "Factures", INT),
            ("montant_ht_sum", "SUM(montant_ht)", "Montant HT (€)", COMPACT),
            ("nb_entreprises", "COUNT(DISTINCT siren)", "Entreprises", INT),
            ("nb_anomalies_sum", "SUM(nb_factures_anomalie)", "Factures en anomalie", INT),
        ],
    ),
    # One row per control. `montant_impacte` is what makes it readable by someone
    # who does not care about rule identifiers: the duplicate line answers "how
    # much would have been paid twice".
    "qualite_anomalies": (
        [
            ("regle_id", "VARCHAR", False),
            ("famille", "VARCHAR", False),
            ("gravite", "VARCHAR", False),
            ("libelle", "VARCHAR", False),
            ("nb_constats", "BIGINT", False),
            ("nb_factures", "BIGINT", False),
            ("montant_impacte", "DECIMAL", False),
            ("part_factures", "DOUBLE", False),
        ],
        [
            ("nb_factures_sum", "SUM(nb_factures)", "Factures concernées", INT),
            ("nb_constats_sum", "SUM(nb_constats)", "Constats", INT),
            ("montant_impacte_sum", "SUM(montant_impacte)", "Montant en jeu (€)", COMPACT),
        ],
    ),
    # The flow read through the calendar of the reform: the size class comes from
    # SIRENE, and the reform keys its deadlines on it.
    "conformite_reforme": (
        [
            ("categorie_entreprise", "VARCHAR", False),
            ("obligation_emission", "VARCHAR", False),
            ("nb_factures", "BIGINT", False),
            ("nb_emetteurs", "BIGINT", False),
            ("montant_ht", "DECIMAL", False),
            ("nb_factures_anomalie", "BIGINT", False),
            ("part_factures", "DOUBLE", False),
            ("taux_anomalie", "DOUBLE", False),
        ],
        [
            ("nb_factures_sum", "SUM(nb_factures)", "Factures reçues", INT),
            ("montant_ht_sum", "SUM(montant_ht)", "Montant HT (€)", COMPACT),
            ("nb_emetteurs_sum", "SUM(nb_emetteurs)", "Émetteurs", INT),
        ],
    ),
}

DTTM_COLUMN = {"facturation_mensuelle": "mois_date"}


def upsert_dataset(dbobj, name, columns, metrics):
    # Looked up by schema as well as by name: the establishments chain publishes
    # its own datasets in the same database, and the two schemas are free to use
    # the same table name.
    table = (
        db.session.query(SqlaTable)
        .filter_by(table_name=name, database_id=dbobj.id, schema=SCHEMA)
        .first()
    )
    if not table:
        table = SqlaTable(table_name=name, database=dbobj, schema=SCHEMA)
        db.session.add(table)
        db.session.flush()
    table.schema = SCHEMA
    table.main_dttm_col = DTTM_COLUMN.get(name)

    known = {c.column_name: c for c in table.columns}
    for column_name, column_type, is_dttm in columns:
        column = known.get(column_name) or TableColumn(
            column_name=column_name, table=table
        )
        column.type = column_type
        column.is_dttm = is_dttm
        column.groupby = True
        column.filterable = True
        db.session.add(column)

    known = {m.metric_name: m for m in table.metrics}
    for metric_name, expression, label, d3format in metrics:
        metric = known.get(metric_name) or SqlMetric(
            metric_name=metric_name, table=table
        )
        metric.expression = expression
        metric.verbose_name = label
        metric.d3format = d3format
        db.session.add(metric)

    db.session.commit()
    return table


database = upsert_db()
print("DB", database.id, database.database_name)
for table_name, (columns, metrics) in SPECS.items():
    dataset = upsert_dataset(database, table_name, columns, metrics)
    print(
        "DATASET",
        table_name,
        "id",
        dataset.id,
        "cols",
        len(dataset.columns),
        "metrics",
        len(dataset.metrics),
    )
print("DONE_BUILD_DATASETS")
