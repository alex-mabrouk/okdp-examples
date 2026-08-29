# Build the France establishments charts and dashboard on the five gold datasets.
import json
import uuid

from superset import db
from superset.connectors.sqla.models import SqlaTable
from superset.models.dashboard import Dashboard
from superset.models.slice import Slice

TITLE = "France — observatoire des établissements actifs"
SLUG = "france-establishments"

DATASETS = {t.table_name: t for t in db.session.query(SqlaTable).all()}
PAR_DEPARTEMENT = "etablissements_par_departement"
PAR_SECTION = "etablissements_par_section_naf"
PAR_MOIS = "creations_par_mois"
PAR_COMMUNE = "etablissements_par_commune"
PAR_CATEGORIE = "etablissements_par_categorie"
DATASETS_BUILT = {
    PAR_DEPARTEMENT,
    "etablissements_par_section_naf",
    "etablissements_par_commune",
    PAR_CATEGORIE,
    "creations_par_mois",
}


def dataset_id(name):
    return DATASETS[name].id


def source(name):
    return "%d__table" % dataset_id(name)


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


# The card header names the theme and the subheader says what the number means,
# so the figure is read rather than decoded. Counts are shown compact: nobody
# audits 14 025 514 from a slide.
kpi("kpi_total", "🏢 Établissements", PAR_DEPARTEMENT,
    "nb_etab_sum", "Établissements actifs géolocalisés", ".3~s")
kpi("kpi_qpv", "🏘️ Quartiers prioritaires", PAR_DEPARTEMENT,
    "part_qpv_pond", "Établissements situés en QPV", ".1%")
kpi("kpi_ess", "🤝 Économie sociale et solidaire", PAR_DEPARTEMENT,
    "part_ess_pond", "Établissements relevant de l'ESS", ".1%")
kpi("kpi_communes", "📍 Communes", PAR_DEPARTEMENT,
    "nb_communes_sum", "Communes couvertes", ",d")


def country_map(key, name, metric, color_scheme, number_format):
    """Country Map is the one viz that renders France offline.

    Two traps. It keys on code_carte, not on code_departement: Superset's France
    map uses FR-<code> and names the overseas departments by ISO letter code.
    And its renderer reads `colorScheme ? categorical(country_id) : linear(metric)`
    — any categorical scheme, including one inherited from the dashboard, colours
    the map by department id and ignores the metric entirely. Hence the explicit
    empty value.
    """
    chart(
        key,
        name,
        {
            "datasource": source(PAR_DEPARTEMENT),
            "viz_type": "country_map",
            "select_country": "France",
            "entity": "code_carte",
            "metric": metric,
            "linear_color_scheme": color_scheme,
            "color_scheme": "",
            "number_format": number_format,
            "adhoc_filters": [],
        },
    )


country_map("carte_densite", "🗺️ Établissements actifs par département",
            "nb_etab_sum", "superset_seq_1", "SMART_NUMBER")
country_map("carte_qpv", "🗺️ Part d'établissements en QPV par département",
            "part_qpv_pond", "schemeYlOrRd", ".1%")

chart("top_departements", "📊 Top 15 des départements", {
    "datasource": source(PAR_DEPARTEMENT),
    "viz_type": "echarts_timeseries_bar",
    "x_axis": "code_departement",
    "orientation": "horizontal",
    "metrics": ["nb_etab_sum"],
    "groupby": [],
    "row_limit": 15,
    "x_axis_sort": "nb_etab_sum",
    "x_axis_sort_asc": False,
    "adhoc_filters": [],
    "y_axis_format": "SMART_NUMBER",
    "color_scheme": "supersetColors",
})

chart("sections_naf", "🍩 Répartition par secteur d'activité", {
    "datasource": source(PAR_SECTION),
    "viz_type": "pie",
    "groupby": ["libelle_section_naf"],
    "metric": "nb_etab_sum",
    "donut": True,
    "innerRadius": 45,
    "outerRadius": 75,
    "show_labels": True,
    "labels_outside": True,
    "label_type": "key_percent",
    "number_format": "SMART_NUMBER",
    "row_limit": 25,
    "adhoc_filters": [],
    "color_scheme": "supersetColors",
    "show_legend": True,
})

chart("sections_qpv", "📊 Secteurs les plus présents en QPV", {
    "datasource": source(PAR_SECTION),
    "viz_type": "echarts_timeseries_bar",
    "x_axis": "libelle_section_naf",
    "orientation": "horizontal",
    "metrics": ["nb_qpv_sum"],
    "groupby": [],
    "row_limit": 12,
    "x_axis_sort": "nb_qpv_sum",
    "x_axis_sort_asc": False,
    "adhoc_filters": [],
    "y_axis_format": "SMART_NUMBER",
    "color_scheme": "supersetColors",
})

# The volume chart above ranks by sector size, so Commerce always wins. This one
# ranks by intensity and surfaces what the volume hides: Transports sits four
# times above the 4.30% national rate.
chart("qpv_taux", "🏘️ Secteurs où le QPV pèse le plus (en % du secteur)", {
    "datasource": source(PAR_SECTION),
    "viz_type": "echarts_timeseries_bar",
    "x_axis": "libelle_section_naf",
    "orientation": "horizontal",
    "metrics": ["taux_qpv_secteur"],
    "groupby": [],
    "row_limit": 12,
    "x_axis_sort": "taux_qpv_secteur",
    "x_axis_sort_asc": False,
    "adhoc_filters": [],
    "y_axis_format": ".1%",
})

chart("top_communes", "🏙️ Top 20 des communes par nombre d'établissements", {
    "datasource": source(PAR_COMMUNE),
    "viz_type": "echarts_timeseries_bar",
    "x_axis": "commune_departement",
    "orientation": "horizontal",
    "metrics": ["nb_etab_sum"],
    "groupby": [],
    "row_limit": 20,
    "x_axis_sort": "nb_etab_sum",
    "x_axis_sort_asc": False,
    "adhoc_filters": [],
    "y_axis_format": "SMART_NUMBER",
})

# Not "size": the enterprise category is an INSEE statistical computation
# (décret 2008-1354) from headcount, turnover and balance sheet. Units with no
# such economic data fall back to their legal form, which names the 43% that
# would otherwise read "unknown".
chart("taille_entreprises", "🏭 Catégorie d'entreprise", {
    "datasource": source(PAR_CATEGORIE),
    "viz_type": "pie",
    "groupby": ["categorie"],
    "metric": "nb_etab_sum",
    "donut": True,
    "innerRadius": 45,
    "outerRadius": 75,
    "show_labels": True,
    "labels_outside": True,
    "label_type": "key_percent",
    # Thirteen slices, six of them under 2%: their outside labels would collide.
    # The legend still names every one.
    "show_labels_threshold": 2,
    "number_format": "SMART_NUMBER",
    "row_limit": 20,
    "adhoc_filters": [],
    "show_legend": True,
})

# SIRENE uses 1900-01-01 as "creation date unknown": 94 338 establishments carry
# that exact day, 89% of everything dated before 1950. The register itself only
# starts in 1973, so the curve is read from 1970 on and the spike stays out.
chart("evolution", "📈 Créations d'établissements par mois (depuis 1970)", {
    "datasource": source(PAR_MOIS),
    "viz_type": "echarts_timeseries_line",
    "x_axis": "mois_creation",
    "time_grain_sqla": "P1M",
    "metrics": ["nb_creations_sum"],
    "groupby": [],
    "row_limit": 10000,
    "adhoc_filters": [
        {
            "clause": "WHERE",
            "subject": "mois_creation",
            "operator": "TEMPORAL_RANGE",
            "comparator": "1970-01-01 : ",
            "expressionType": "SIMPLE",
        }
    ],
    "y_axis_format": ",d",
    "x_axis_time_format": "%Y-%m",
    "color_scheme": "supersetColors",
    "show_legend": False,
    "truncateXAxis": True,
})

chart("detail", "📋 Détail par département", {
    "datasource": source(PAR_DEPARTEMENT),
    "viz_type": "table",
    "query_mode": "aggregate",
    "groupby": ["code_departement"],
    "metrics": [
        "nb_etab_sum",
        "part_qpv_pond",
        "part_ess_pond",
        "nb_communes_sum",
        "nb_sieges_sum",
    ],
    "row_limit": 200,
    "order_desc": True,
    "adhoc_filters": [],
    "color_scheme": "supersetColors",
})

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
ROWS = [
    ("KPI", [("kpi_total", 3, 30), ("kpi_qpv", 3, 30), ("kpi_ess", 3, 30),
             ("kpi_communes", 3, 30)]),
    ("CARTES", [("carte_densite", 6, 70), ("carte_qpv", 6, 70)]),
    ("ACTIVITE", [("sections_naf", 6, 60), ("top_departements", 6, 60)]),
    ("QPV", [("sections_qpv", 6, 60), ("qpv_taux", 6, 60)]),
    ("COMMUNES", [("top_communes", 6, 65), ("taille_entreprises", 6, 65)]),
    ("EVOLUTION", [("evolution", 12, 55)]),
    ("DETAIL", [("detail", 12, 60)]),
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


# The three gold tables all carry code_departement, so this one filter drives
# every chart on the dashboard rather than only its own dataset.
native_filters = [
    native_filter("Département", PAR_DEPARTEMENT, "code_departement"),
    native_filter("Secteur d'activité", PAR_SECTION, "libelle_section_naf"),
    # Scoped to its own dataset: the commune is the only dimension the other
    # gold tables do not carry.
    native_filter("Commune", PAR_COMMUNE, "libelle_commune"),
]

json_metadata = {
    "native_filter_configuration": native_filters,
    # Deliberately empty: a dashboard-level categorical scheme is pushed into
    # every chart, and Country Map then colours by id instead of by metric.
    "color_scheme": "",
    "refresh_frequency": 0,
    "expanded_slices": {},
    "label_colors": {},
    "cross_filters_enabled": True,
    "default_filters": "{}",
    "filter_scopes": {},
    "chart_configuration": {},
}

# Keyed on the slug, not the title: the slug is the stable identity and the URL
# people bookmark, so retitling updates this dashboard instead of forking a
# second one that shares its charts.
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

# Charts and datasets are upserted by name, so renaming either leaves the old one
# behind. Sweep the whole france_establishments schema rather than the current names: a
# rename moves a chart off the very list that would have caught it.
schema_datasets = [
    d for d in db.session.query(SqlaTable).all() if d.schema == "france_establishments"
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
