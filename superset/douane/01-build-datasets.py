# Build the Trino->gold database and the douane datasets in Superset.
import json

from superset import db
from superset.connectors.sqla.models import SqlaTable, SqlMetric, TableColumn
from superset.models.core import Database

DB_NAME = "trino-gold"
URI = "trino://trino@trino-demo.okdp.sandbox:443/gold"
SCHEMA = "douane"
EXTRA = json.dumps(
    {
        "engine_params": {},
        "metadata_params": {},
        "connect_args": {"http_scheme": "https", "verify": False},
        "schemas_allowed_for_file_upload": [],
    }
)

INT = ",d"
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
    "containers": (
        [
            ("event_id", "VARCHAR", False),
            ("timestamp", "TIMESTAMP", True),
            ("port", "VARCHAR", False),
            ("container_id", "VARCHAR", False),
            ("pays_origine", "VARCHAR", False),
            ("poids_kg", "BIGINT", False),
            ("nb_colis", "BIGINT", False),
        ],
        [
            ("nb_containers", "COUNT(*)", "Conteneurs arrivés", INT),
            ("poids_sum", "SUM(poids_kg)", "Poids total (kg)", COMPACT),
            ("nb_colis_sum", "SUM(nb_colis)", "Colis déclarés", COMPACT),
        ],
    ),
    "scans": (
        [
            ("event_id", "VARCHAR", False),
            ("timestamp", "TIMESTAMP", True),
            ("port", "VARCHAR", False),
            ("colis_id", "VARCHAR", False),
            ("scanner_id", "VARCHAR", False),
            ("poids_kg", "DOUBLE", False),
            ("score_risque", "BIGINT", False),
        ],
        [
            ("nb_scans", "COUNT(*)", "Colis scannés", INT),
            ("score_risque_moyen", "AVG(score_risque)", "Score de risque moyen", ",.1f"),
        ],
    ),
    # sous_type (arme / drogue / contrefacon) tells the fraud families apart;
    # quantite_estimee is never summed across rows, its unit (kg, unité,
    # articles) changes with sous_type.
    "alertes": (
        [
            ("event_id", "VARCHAR", False),
            ("timestamp", "TIMESTAMP", True),
            ("port", "VARCHAR", False),
            ("sous_type", "VARCHAR", False),
            ("colis_id", "VARCHAR", False),
            ("quantite_estimee", "BIGINT", False),
            ("unite", "VARCHAR", False),
        ],
        [("nb_alertes", "COUNT(*)", "Alertes de fraude", INT)],
    ),
    "entreprises_suspectes": (
        [
            ("event_id", "VARCHAR", False),
            ("timestamp", "TIMESTAMP", True),
            ("port", "VARCHAR", False),
            ("siren", "VARCHAR", False),
            ("nom_entreprise", "VARCHAR", False),
            ("motif", "VARCHAR", False),
        ],
        [
            ("nb_signalements", "COUNT(*)", "Signalements", INT),
            ("nb_entreprises", "COUNT(DISTINCT siren)", "Entreprises distinctes", INT),
        ],
    ),
}

DTTM_COLUMN = {name: "timestamp" for name in SPECS}


def upsert_dataset(dbobj, name, columns, metrics):
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
