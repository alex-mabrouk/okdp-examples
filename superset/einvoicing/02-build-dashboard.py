# Build the e-invoicing charts and dashboard on the six gold datasets.
import json
import uuid

from superset import db
from superset.connectors.sqla.models import SqlaTable
from superset.models.dashboard import Dashboard
from superset.models.slice import Slice

TITLE = "Facturation électronique — contrôle du flux Factur-X"
SLUG = "einvoicing"
SCHEMA = "einvoicing"

# Keyed on the schema too: the establishments chain publishes datasets in the same
# database, and nothing stops two schemas from naming a table the same way.
DATASETS = {
    t.table_name: t for t in db.session.query(SqlaTable).all() if t.schema == SCHEMA
}

PAR_MOIS = "facturation_mensuelle"
PAR_DEPARTEMENT = "facturation_par_departement"
PAR_SECTION = "facturation_par_section_naf"
ACTEURS = "acteurs"
QUALITE = "qualite_anomalies"
CONFORMITE = "conformite_reforme"
INSIGHTS = "insights"
CONFORMITE_SECTION = "conformite_par_section_naf"
CONFORMITE_DEPARTEMENT = "conformite_par_departement"
EMETTEURS = "emetteurs_en_anomalie"
DATASETS_BUILT = {
    PAR_MOIS, PAR_DEPARTEMENT, PAR_SECTION, ACTEURS, QUALITE, CONFORMITE, INSIGHTS,
    CONFORMITE_SECTION, CONFORMITE_DEPARTEMENT, EMETTEURS,
}

ECHEANCE_2027 = [
    {
        "clause": "WHERE",
        "subject": "obligation_emission",
        "operator": "==",
        "comparator": "2027-09-01",
        "expressionType": "SIMPLE",
    }
]


def dataset_id(name):
    return DATASETS[name].id


def source(name):
    return "%d__table" % dataset_id(name)


CHARTS = {}


def chart(key, name, params):
    CHARTS[key] = (name, params)


def kpi(key, name, dataset, metric, subheader, number_format, filters=None):
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
            "adhoc_filters": filters or [],
        },
    )


def kpi_dynamique(key, name, metric, subheader, number_format):
    """The latest month, and how it compares to the same month a year earlier.

    Year on year, never month on month: the flow dips by half every August, so a
    month-over-month figure reads -53 % and means "it is August". A twelve-period
    lag compares like with like.
    """
    chart(
        key,
        name,
        {
            "datasource": source(PAR_MOIS),
            "viz_type": "big_number",
            "metric": metric,
            "granularity_sqla": "mois_date",
            "time_grain_sqla": "P1M",
            "compare_lag": 12,
            "compare_suffix": "sur un an",
            "show_trend_line": True,
            "start_y_axis_at_zero": True,
            "subheader": subheader,
            "y_axis_format": number_format,
            "header_font_size": 0.4,
            "subheader_font_size": 0.15,
            "adhoc_filters": [],
        },
    )


def egal(subject, value):
    return {
        "clause": "WHERE",
        "subject": subject,
        "operator": "==",
        "comparator": value,
        "expressionType": "SIMPLE",
    }


# The header names the theme, the subheader says what the number means. Amounts
# are compact: nobody audits 41 235 812 € from a slide.
kpi("kpi_factures", "📄 Factures", PAR_MOIS, "nb_factures_sum",
    "Documents Factur-X reçus", ",d")
kpi("kpi_ht", "💶 Montant HT", PAR_MOIS, "montant_ht_sum",
    "Total hors taxes facturé", ".3~s")
kpi("kpi_tva", "🧾 TVA", PAR_MOIS, "montant_tva_sum",
    "Total de TVA facturée", ".3~s")
kpi("kpi_ttc", "💰 Montant TTC", PAR_MOIS, "montant_ttc_sum",
    "Total toutes taxes comprises", ".3~s")
# Counted on the issuer side only: the same company appears on both ends of the
# flow, and summing the two roles would count it twice.
kpi("kpi_entreprises", "🏢 Entreprises", ACTEURS, "nb_entreprises",
    "Émetteurs distincts", ",d", [egal("role", "émetteur")])
kpi("kpi_anomalies", "⚠️ Taux d'anomalie", PAR_MOIS, "taux_anomalie_pond",
    "Factures portant au moins un constat", ".1%")
# The companies the referential catches, counted rather than counted by hand: the table
# below names them, this says how many there are.
kpi("kpi_cesses", "🚫 Entreprises cessées", EMETTEURS, "nb_entreprises",
    "Facturent depuis un établissement administrativement cessé", ",d",
    [egal("regle_id", "REF-EMETTEUR-CESSE")])

# The two deadlines side by side, each with its own figure. A single "404" tile was a
# number with no scale: what gives it meaning is the 582 still to come, and that one was
# only readable by measuring a bar.
#
# Two tiles rather than one two-bar chart: plotting a column and grouping on the same
# column is rejected by Superset ("duplicate column label"), and every other way of
# colouring one bar differently costs more than the distinction is worth. The wording
# carries it instead -- "déjà" against "à raccorder d'ici".
kpi("kpi_obligation_2026", "📅 Déjà soumises à l'obligation", CONFORMITE,
    "nb_factures_sum", "Émetteurs GE et ETI, depuis le 1er septembre 2026", ",d",
    [egal("obligation_emission", "2026-09-01")])
kpi("kpi_obligation_2027", "🕓 À raccorder d'ici septembre 2027", CONFORMITE,
    "nb_factures_sum", "Émetteurs PME, échéance du 1er septembre 2027 — pas encore en vigueur",
    ",d", [egal("obligation_emission", "2027-09-01")])

# The flow is not flat: the reform makes it climb. These three read the last month
# against the same month a year earlier.
kpi_dynamique("dyn_factures", "📄 Factures du dernier mois", "nb_factures_sum",
              "Dernier mois clos, comparé à un an plus tôt", ",d")
kpi_dynamique("dyn_ht", "💶 Montant HT du dernier mois", "montant_ht_sum",
              "Dernier mois clos, comparé à un an plus tôt", ".3~s")
kpi_dynamique("dyn_anomalies", "⚠️ Taux d'anomalie du dernier mois", "taux_anomalie_pond",
              "Dernier mois clos, comparé à un an plus tôt", ".1%")

chart("volume_mensuel", "📊 Factures reçues par mois", {
    "datasource": source(PAR_MOIS),
    "viz_type": "echarts_timeseries_bar",
    "x_axis": "mois_date",
    "time_grain_sqla": "P1M",
    "metrics": ["nb_factures_sum"],
    "groupby": [],
    "row_limit": 1000,
    "adhoc_filters": [],
    "y_axis_format": ",d",
    "x_axis_time_format": "%Y-%m",
    "color_scheme": "supersetColors",
    "show_legend": False,
})

chart("montant_mensuel", "📈 Montant facturé par mois", {
    "datasource": source(PAR_MOIS),
    "viz_type": "echarts_timeseries_line",
    "x_axis": "mois_date",
    "time_grain_sqla": "P1M",
    "metrics": ["montant_ht_sum", "montant_tva_sum"],
    "groupby": [],
    "row_limit": 1000,
    "adhoc_filters": [],
    "y_axis_format": "SMART_NUMBER",
    "x_axis_time_format": "%Y-%m",
    "color_scheme": "supersetColors",
    "show_legend": True,
    "markerEnabled": True,
})


def country_map(key, name, metric, color_scheme, number_format):
    """Country Map is the one viz that renders France offline.

    Two traps, both inherited from the establishments dashboard. It keys on
    code_carte, not on code_departement: the map uses FR-<code> and names the
    overseas departments by ISO letter code. And its renderer reads
    `colorScheme ? categorical(country_id) : linear(metric)` -- any categorical
    scheme, including one inherited from the dashboard, colours by department id
    and stops reflecting the data while still looking plausible.
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


country_map("carte_montant", "🗺️ Montant facturé par département",
            "montant_ht_sum", "superset_seq_1", "SMART_NUMBER")

chart("secteurs", "🏭 Montant facturé par secteur d'activité", {
    "datasource": source(PAR_SECTION),
    "viz_type": "echarts_timeseries_bar",
    "x_axis": "libelle_section_naf",
    "orientation": "horizontal",
    "metrics": ["montant_ht_sum"],
    "groupby": [],
    "row_limit": 12,
    "x_axis_sort": "montant_ht_sum",
    "x_axis_sort_asc": False,
    "adhoc_filters": [],
    "y_axis_format": "SMART_NUMBER",
    "color_scheme": "supersetColors",
})

chart("top_acteurs", "🏢 Principaux émetteurs et acheteurs", {
    "datasource": source(ACTEURS),
    "viz_type": "table",
    "query_mode": "aggregate",
    "groupby": ["role", "nom", "libelle_departement", "categorie_entreprise"],
    "metrics": ["nb_factures_sum", "montant_ht_sum", "nb_anomalies_sum"],
    "row_limit": 50,
    "order_desc": True,
    "timeseries_limit_metric": "montant_ht_sum",
    "adhoc_filters": [],
    "color_scheme": "supersetColors",
    "column_config": {"nom": {"columnWidth": 320}},
})

# Counted in invoices, not in findings: one malformed invoice breaks four
# Schematron rules at once, and the rate computed on findings would be the wrong
# number. The `nb_constats` column sits beside it so the gap is visible.
chart("anomalies_type", "⚠️ Factures concernées par type d'anomalie", {
    "datasource": source(QUALITE),
    "viz_type": "echarts_timeseries_bar",
    "x_axis": "libelle",
    "orientation": "horizontal",
    "metrics": ["nb_factures_sum"],
    "groupby": ["famille"],
    "row_limit": 20,
    "x_axis_sort": "nb_factures_sum",
    "x_axis_sort_asc": False,
    "adhoc_filters": [],
    "y_axis_format": ",d",
    "color_scheme": "supersetColors",
    "show_legend": True,
    "stack": True,
})

chart("anomalies_montant", "💸 Montant en jeu par contrôle", {
    "datasource": source(QUALITE),
    "viz_type": "table",
    "query_mode": "raw",
    "all_columns": [
        "famille",
        "gravite",
        "libelle",
        "nb_factures",
        "nb_constats",
        "montant_impacte",
    ],
    "order_by_cols": ['["nb_factures", false]'],
    "row_limit": 30,
    "adhoc_filters": [],
    "column_config": {
        "libelle": {"columnWidth": 340},
        "montant_impacte": {"d3NumberFormat": ",.0f"},
    },
    "color_scheme": "supersetColors",
})

chart("anomalies_mensuel", "📉 Taux d'anomalie par mois", {
    "datasource": source(PAR_MOIS),
    "viz_type": "echarts_timeseries_line",
    "x_axis": "mois_date",
    "time_grain_sqla": "P1M",
    "metrics": ["taux_anomalie_pond"],
    "groupby": [],
    "row_limit": 1000,
    "adhoc_filters": [],
    "y_axis_format": ".1%",
    "x_axis_time_format": "%Y-%m",
    "color_scheme": "supersetColors",
    "show_legend": False,
    "markerEnabled": True,
})

# The chart the demo exists for: the size class comes from SIRENE, and the reform
# keys its September 2026 and September 2027 deadlines on it. Reception has been
# mandatory for everyone since September 2026, with no tiering -- only issuing is
# staged, which is what this splits.
chart("conformite", "📅 Obligation d'émission : ce flux face au calendrier", {
    "datasource": source(CONFORMITE),
    "viz_type": "echarts_timeseries_bar",
    "x_axis": "obligation_emission",
    "metrics": ["nb_factures_sum"],
    "groupby": ["categorie_entreprise"],
    "row_limit": 20,
    "adhoc_filters": [],
    "y_axis_format": ",d",
    "color_scheme": "supersetColors",
    "show_legend": True,
    "stack": True,
})

# Where the next deadline lands. conformite_reforme says how much of the flow is
# already mandatory; this says which trades have to be brought along by September 2027,
# which is a question about the reform rather than about the flow.
chart("vague_2027", "Septembre 2027 : les secteurs à accompagner", {
    "datasource": source(CONFORMITE_SECTION),
    "viz_type": "echarts_timeseries_bar",
    "x_axis": "libelle_section_naf",
    "metrics": ["nb_factures_sum", "nb_emetteurs_sum"],
    "groupby": [],
    "orientation": "horizontal",
    "row_limit": 10,
    "order_desc": True,
    "adhoc_filters": ECHEANCE_2027,
    "color_scheme": "supersetColors",
    "show_legend": True,
})

# By name, not by rate: what a room remembers is a list of companies, and this list
# only exists because SIRENE sits next to the flow.
chart("emetteurs_cesses", "Entreprises facturant depuis un établissement cessé", {
    "datasource": source(EMETTEURS),
    "viz_type": "table",
    "query_mode": "raw",
    "all_columns": [
        "nom", "siren", "libelle_departement", "nb_factures", "montant_ttc",
    ],
    "column_config": {"nom": {"columnWidth": 260}, "siren": {"columnWidth": 110}},
    "order_by_cols": ['["montant_ttc", false]'],
    "row_limit": 25,
    "adhoc_filters": [
        {
            "clause": "WHERE",
            "subject": "regle_id",
            "operator": "==",
            "comparator": "REF-EMETTEUR-CESSE",
            "expressionType": "SIMPLE",
        }
    ],
    "color_scheme": "supersetColors",
})

# Only the verified rows are shown. The rejected ones stay in the table on purpose
# -- the check is part of what there is to demonstrate -- but a dashboard is not
# where a sentence the pipeline refused belongs.
#
# The title differs from the establishments panel on purpose: charts are upserted by
# name across the whole instance, so sharing a title would rebind that chart to this
# dataset and put e-invoicing sentences on the other dashboard.
chart("ai_insights", "🤖 Lecture du flux de factures par le modèle local", {
    "datasource": source(INSIGHTS),
    "viz_type": "table",
    "query_mode": "raw",
    "all_columns": ["category", "scope", "insight", "model"],
    "column_config": {
        "category": {"columnWidth": 110},
        "scope": {"columnWidth": 150},
        "model": {"columnWidth": 90},
    },
    "order_by_cols": ['["category", true]'],
    "row_limit": 100,
    "adhoc_filters": [
        {
            "clause": "WHERE",
            "subject": "status",
            "operator": "==",
            "comparator": "verified",
            "expressionType": "SIMPLE",
        }
    ],
    "color_scheme": "supersetColors",
})

def verifier_labels(charts):
    """A column may not appear twice in one query, whatever the role it plays.

    Plotting `obligation_emission` and grouping on it too is rejected at render time
    with "Duplicate column/metric labels", which is a blank chart on the dashboard and
    nothing at build time. Failing here instead costs one second and never ships.
    """
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
# Three levels of reading, in this order: what the flow weighs, what the reform says
# of it, what the controls found in it. The raw detail goes last -- it is evidence to
# drill into, not the opening screen.
ROWS = [
    # 1. What the flow weighs, and how it moves.
    ("VOLUMETRIE", [("kpi_factures", 3, 30), ("kpi_ht", 3, 30),
                    ("kpi_anomalies", 3, 30), ("kpi_cesses", 3, 30)]),
    ("DYNAMIQUE", [("dyn_factures", 4, 32), ("dyn_ht", 4, 32), ("dyn_anomalies", 4, 32)]),
    ("TEMPOREL", [("volume_mensuel", 6, 55), ("montant_mensuel", 6, 55)]),
    # 2. The reform read on this flow: how many today, how many still to come, and who
    #    has to be brought along first.
    ("REFORME", [("kpi_obligation_2026", 3, 45), ("kpi_obligation_2027", 3, 45),
                 ("vague_2027", 6, 45)]),
    # 3. What the controls found, ending on the companies they name.
    ("CONTROLE", [("anomalies_montant", 6, 60), ("emetteurs_cesses", 6, 60)]),
    ("IA", [("ai_insights", 12, 60)]),
    # The evidence, for whoever wants to go down to it.
    ("DETAIL", [("carte_montant", 6, 70), ("secteurs", 6, 70)]),
    ("DETAIL2", [("anomalies_type", 6, 60), ("anomalies_mensuel", 6, 60)]),
    ("DETAIL3", [("conformite", 12, 55)]),
    ("ACTEURS", [("top_acteurs", 12, 65)]),
    ("APPOINT", [("kpi_tva", 4, 30), ("kpi_ttc", 4, 30), ("kpi_entreprises", 4, 30)]),
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


# Department and sector are carried by several gold tables, so those two filters
# drive most of the dashboard. Family and role are scoped to their own dataset,
# which is the only one that carries them.
native_filters = [
    native_filter("Département", PAR_DEPARTEMENT, "code_departement"),
    native_filter("Secteur d'activité", PAR_SECTION, "libelle_section_naf"),
    native_filter("Famille de contrôle", QUALITE, "famille"),
    native_filter("Rôle", ACTEURS, "role", multi=False),
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
# people bookmark, so retitling updates this dashboard instead of forking a second
# one that shares its charts.
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
# behind. Sweep the whole einvoicing schema rather than the current names: a
# rename moves a chart off the very list that would have caught it.
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
