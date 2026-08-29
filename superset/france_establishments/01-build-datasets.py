# Build the Trino->gold database and the five france_establishments datasets in Superset.
import json

from superset import db
from superset.connectors.sqla.models import SqlaTable, SqlMetric, TableColumn
from superset.models.core import Database

DB_NAME = "trino-gold"
URI = "trino://trino@trino-demo.okdp.sandbox:443/gold"
SCHEMA = "france_establishments"
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
PCT2 = ".2%"
# 14.0M rather than 14,025,514: a count that large is read, not audited.
COMPACT = ".3~s"


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
    "etablissements_par_departement": (
        [
            ("code_departement", "VARCHAR", False),
            ("code_carte", "VARCHAR", False),
            ("nb_etablissements", "BIGINT", False),
            ("nb_qpv", "BIGINT", False),
            ("part_qpv", "DOUBLE", False),
            ("nb_ess", "BIGINT", False),
            ("part_ess", "DOUBLE", False),
            ("nb_sieges", "BIGINT", False),
            ("nb_communes", "BIGINT", False),
            ("nb_iris", "BIGINT", False),
        ],
        [
            ("nb_etab_sum", "SUM(nb_etablissements)", "Établissements actifs", COMPACT),
            # Recomputed from the counts, never averaged: a mean of percentages
            # would weight Lozère like Paris.
            (
                "part_qpv_pond",
                "1.0 * SUM(nb_qpv) / SUM(nb_etablissements)",
                "Part en QPV",
                PCT,
            ),
            (
                "part_ess_pond",
                "1.0 * SUM(nb_ess) / SUM(nb_etablissements)",
                "Part ESS",
                PCT,
            ),
            ("nb_communes_sum", "SUM(nb_communes)", "Communes couvertes", INT),
            ("nb_sieges_sum", "SUM(nb_sieges)", "Sièges", INT),
        ],
    ),
    "etablissements_par_section_naf": (
        [
            ("code_departement", "VARCHAR", False),
            ("code_section_naf", "VARCHAR", False),
            ("libelle_section_naf", "VARCHAR", False),
            ("nb_etablissements", "BIGINT", False),
            ("nb_qpv", "BIGINT", False),
        ],
        [
            ("nb_etab_sum", "SUM(nb_etablissements)", "Établissements actifs", INT),
            ("nb_qpv_sum", "SUM(nb_qpv)", "Établissements en QPV", INT),
            (
                "taux_qpv_secteur",
                "1.0 * SUM(nb_qpv) / SUM(nb_etablissements)",
                "Part du secteur en QPV",
                PCT,
            ),
        ],
    ),
    "etablissements_par_commune": (
        [
            ("code_departement", "VARCHAR", False),
            ("code_commune", "VARCHAR", False),
            ("libelle_commune", "VARCHAR", False),
            ("commune_departement", "VARCHAR", False),
            ("nb_etablissements", "BIGINT", False),
            ("nb_qpv", "BIGINT", False),
        ],
        [
            ("nb_etab_sum", "SUM(nb_etablissements)", "Établissements actifs", INT),
            (
                "taux_qpv_commune",
                "1.0 * SUM(nb_qpv) / SUM(nb_etablissements)",
                "Part en QPV",
                PCT,
            ),
        ],
    ),
    "etablissements_par_categorie": (
        [
            ("code_departement", "VARCHAR", False),
            ("categorie", "VARCHAR", False),
            ("nb_etablissements", "BIGINT", False),
        ],
        [("nb_etab_sum", "SUM(nb_etablissements)", "Établissements actifs", COMPACT)],
    ),
    "creations_par_mois": (
        [
            ("mois_creation", "DATE", True),
            ("code_departement", "VARCHAR", False),
            ("nb_creations", "BIGINT", False),
        ],
        [("nb_creations_sum", "SUM(nb_creations)", "Créations", INT)],
    ),
}

DTTM_COLUMN = {"creations_par_mois": "mois_creation"}


def upsert_dataset(dbobj, name, columns, metrics):
    table = (
        db.session.query(SqlaTable)
        .filter_by(table_name=name, database_id=dbobj.id)
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
