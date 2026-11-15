# Build the douane charts and dashboard on the four gold datasets.
import json
import uuid

from superset import db
from superset.connectors.sqla.models import SqlaTable
from superset.models.dashboard import Dashboard
from superset.models.slice import Slice

TITLE = "Douane — activité en temps réel"
SLUG = "douane"
SCHEMA = "douane"

# Keyed on the schema too: other chains publish datasets in the same database,
# and nothing stops two schemas from naming a table the same way.
DATASETS = {
    t.table_name: t for t in db.session.query(SqlaTable).all() if t.schema == SCHEMA
}

CONTAINERS = "containers"
SCANS = "scans"
ALERTES = "alertes"
ENTREPRISES = "entreprises_suspectes"
DATASETS_BUILT = {CONTAINERS, SCANS, ALERTES, ENTREPRISES}


def dataset_id(name):
    return DATASETS[name].id


def source(name):
    return "%d__table" % dataset_id(name)


INT = ",d"

CHARTS = {}


def chart(key, name, params):
    CHARTS[key] = (name, params)


def kpi(key, name, dataset, metric, subheader, number_format):
    chart(
        key,
        name,
        {
            "datasource": source(dataset),
            "viz_type": "big_number_total",
            "metric": metric,
            "subheader": subheader,
            "y_axis_format": number_format,
            "header_font_size": 0.4,
            "subheader_font_size": 0.15,
            "adhoc_filters": [],
        },
    )


# One tile per data source: the manager's first read is "how much has come
# through, how much has been checked, how much has been caught".
kpi("kpi_containers", "🚢 Conteneurs", CONTAINERS, "nb_containers",
    "Arrivages enregistrés", INT)
kpi("kpi_scans", "📦 Colis scannés", SCANS, "nb_scans",
    "Scans passés au contrôle", ",d")
kpi("kpi_alertes", "🚨 Alertes de fraude", ALERTES, "nb_alertes",
    "Armes, drogue, contrefaçon détectées", ",d")
kpi("kpi_entreprises", "🏢 Entreprises suspectes", ENTREPRISES, "nb_entreprises",
    "Entreprises distinctes signalées", ",d")

chart("activite_temps", "📈 Colis scannés, par minute", {
    "datasource": source(SCANS),
    "viz_type": "echarts_timeseries_bar",
    "x_axis": "timestamp",
    "time_grain_sqla": "PT1M",
    "metrics": ["nb_scans"],
    "groupby": [],
    "row_limit": 500,
    "adhoc_filters": [],
    "y_axis_format": ",d",
    "x_axis_time_format": "%H:%M",
    "color_scheme": "supersetColors",
    "show_legend": False,
})

chart("alertes_type", "⚠️ Alertes par type de fraude", {
    "datasource": source(ALERTES),
    "viz_type": "pie",
    "groupby": ["sous_type"],
    "metric": "nb_alertes",
    "row_limit": 10,
    "adhoc_filters": [],
    "color_scheme": "supersetColors",
    "show_legend": True,
    "label_type": "key_value",
    "number_format": ",d",
})

chart("containers_port", "🗺️ Conteneurs par port", {
    "datasource": source(CONTAINERS),
    "viz_type": "echarts_timeseries_bar",
    "x_axis": "port",
    "orientation": "horizontal",
    "metrics": ["nb_containers"],
    "groupby": [],
    "row_limit": 10,
    "x_axis_sort": "nb_containers",
    "x_axis_sort_asc": False,
    "adhoc_filters": [],
    "y_axis_format": ",d",
    "color_scheme": "supersetColors",
})

# The live feed: what a customs manager actually watches minute to minute.
# Raw, most recent first, no aggregation — the point is to see each hit land.
chart("dernieres_alertes", "🚨 Dernières alertes", {
    "datasource": source(ALERTES),
    "viz_type": "table",
    "query_mode": "raw",
    "all_columns": ["timestamp", "port", "sous_type", "colis_id", "quantite_estimee", "unite"],
    "order_by_cols": ['["timestamp", false]'],
    "row_limit": 20,
    "adhoc_filters": [],
    "color_scheme": "supersetColors",
})

chart("dernieres_entreprises", "🏢 Dernières entreprises signalées", {
    "datasource": source(ENTREPRISES),
    "viz_type": "table",
    "query_mode": "raw",
    "all_columns": ["timestamp", "nom_entreprise", "siren", "port", "motif"],
    "order_by_cols": ['["timestamp", false]'],
    "row_limit": 20,
    "adhoc_filters": [],
    "color_scheme": "supersetColors",
    "column_config": {"nom_entreprise": {"columnWidth": 260}},
})


def verifier_labels(charts):
    """A column may not appear twice in one query, whatever the role it plays."""
    for key, (name, params) in charts.items():
        etiquettes = []
        axe = params.get("x_axis")
        if isinstance(axe, str):
            etiquettes.append(axe)
        for champ in ("groupby", "all_columns", "metrics"):
            for valeur in params.get(champ) or []:
                if isinstance(valeur, str):
                    etiquettes.append(valeur)
                elif isinstance(valeur, dict):
                    etiquettes.append(valeur.get("label") or valeur.get("column_name"))
        doublons = {e for e in etiquettes if e and etiquettes.count(e) > 1}
        if doublons:
            raise SystemExit(
                f"chart {key} ({name}): label used twice -> {sorted(doublons)}"
            )


verifier_labels(CHARTS)

slice_ids = {}
for key, (name, params) in CHARTS.items():
    existing = db.session.query(Slice).filter_by(slice_name=name).first()
    slice_ = existing or Slice(slice_name=name)
    slice_.viz_type = params["viz_type"]
    slice_.datasource_type = "table"
    slice_.datasource_id = int(params["datasource"].split("__")[0])
    slice_.params = json.dumps(params, ensure_ascii=False)
    slice_.query_context = None
    db.session.add(slice_)
    db.session.flush()
    slice_ids[key] = slice_.id
db.session.commit()
print("CHART_IDS", json.dumps(slice_ids))


def chart_node(key, width, height, row_key):
    node_id = "CHART-%s" % key
    return node_id, {
        "type": "CHART",
        "id": node_id,
        "children": [],
        "meta": {
            "chartId": slice_ids[key],
            "width": width,
            "height": height,
            "sliceName": CHARTS[key][0],
        },
        "parents": ["ROOT_ID", "GRID_ID", row_key],
    }


def row(row_id, cells):
    row_key = "ROW-%s" % row_id
    nodes = {}
    children = []
    for key, width, height in cells:
        node_id, node = chart_node(key, width, height, row_key)
        nodes[node_id] = node
        children.append(node_id)
    nodes[row_key] = {
        "type": "ROW",
        "id": row_key,
        "children": children,
        "meta": {"background": "BACKGROUND_TRANSPARENT"},
        "parents": ["ROOT_ID", "GRID_ID"],
    }
    return row_key, nodes


position = {
    "DASHBOARD_VERSION_KEY": "v2",
    "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
    "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
    "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": TITLE}},
}
# Counters first, then the live feed a manager actually watches, then the
# breakdowns — same "what it weighs, then the detail" reading order as the
# other domains.
ROWS = [
    ("KPI", [("kpi_containers", 3, 30), ("kpi_scans", 3, 30),
             ("kpi_alertes", 3, 30), ("kpi_entreprises", 3, 30)]),
    ("TEMPS", [("activite_temps", 8, 50), ("alertes_type", 4, 50)]),
    ("FEED", [("dernieres_alertes", 6, 60), ("dernieres_entreprises", 6, 60)]),
    ("DETAIL", [("containers_port", 12, 50)]),
]
grid_children = []
for row_id, cells in ROWS:
    row_key, nodes = row(row_id, cells)
    position.update(nodes)
    grid_children.append(row_key)
position["GRID_ID"]["children"] = grid_children


def native_filter(name, dataset, column, multi=True):
    return {
        "id": "NATIVE_FILTER-%s" % uuid.uuid4().hex[:10],
        "name": name,
        "filterType": "filter_select",
        "targets": [{"datasetId": dataset_id(dataset), "column": {"name": column}}],
        "controlValues": {
            "enableEmptyFilter": False,
            "multiSelect": multi,
            "searchAllOptions": False,
            "inverseSelection": False,
        },
        "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
        "type": "NATIVE_FILTER",
        "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
        "cascadeParentIds": [],
    }


native_filters = [
    native_filter("Port", CONTAINERS, "port"),
]

json_metadata = {
    "native_filter_configuration": native_filters,
    "color_scheme": "",
    # The whole point of this dashboard: it refreshes itself every 15s, no
    # click needed — that is what carries the "live" effect for an audience.
    "refresh_frequency": 15,
    "expanded_slices": {},
    "label_colors": {},
    "cross_filters_enabled": True,
    "default_filters": "{}",
    "filter_scopes": {},
    "chart_configuration": {},
}

dashboard = (
    db.session.query(Dashboard).filter_by(slug=SLUG).first() or Dashboard(slug=SLUG)
)
dashboard.dashboard_title = TITLE
dashboard.position_json = json.dumps(position, ensure_ascii=False)
dashboard.json_metadata = json.dumps(json_metadata, ensure_ascii=False)
dashboard.published = True
dashboard.slices = [db.session.get(Slice, i) for i in slice_ids.values()]
db.session.add(dashboard)
db.session.commit()
print("DASHBOARD_ID", dashboard.id, "slug", dashboard.slug, "slices", len(dashboard.slices))

# Charts and datasets are upserted by name, so renaming either leaves the old
# one behind. Sweep the whole douane schema rather than the current names.
schema_datasets = [
    d for d in db.session.query(SqlaTable).all() if d.schema == SCHEMA
]
for dataset in schema_datasets:
    for orphan in dataset.slices:
        if not orphan.dashboards:
            print("removing orphaned chart:", orphan.id, orphan.slice_name)
            db.session.delete(orphan)
db.session.commit()

for dataset in schema_datasets:
    if dataset.table_name not in DATASETS_BUILT and not dataset.slices:
        print("removing stale dataset:", dataset.id, dataset.table_name)
        db.session.delete(dataset)
db.session.commit()
print("DONE_BUILD_DASHBOARD")
