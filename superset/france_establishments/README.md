# Superset dashboard — France establishments

Reproducible build of the demo dashboard on the **gold** catalog (Iceberg via
Polaris, queried through Trino): one database connection, five datasets, 14 charts
and one dashboard. Idempotent — upserts by name, safe to re-run.

> The dashboard lives only in Superset's metadata database, so a fresh cluster has
> to rebuild it. These scripts are the source of truth: `export-dashboards` needs a
> web request context and fails from a shell.

## Apply

`france_establishments_gold` must have run first. Then, from this directory:

```bash
POD=$(kubectl -n demo get pod -l app=superset --field-selector=status.phase=Running -o name \
  | grep -E 'superset-main-[0-9a-f]+-' | grep -vE 'worker|redis|websocket' | head -1 | sed 's@pod/@@')
for f in 01-build-datasets.py 02-build-dashboard.py; do
  kubectl -n demo cp "$f" "demo/$POD:/tmp/$f" -c superset
  echo "exec(open('/tmp/$f').read())" | kubectl -n demo exec -i "$POD" -c superset -- superset shell
done
```

Then open `https://superset-demo.okdp.sandbox/superset/dashboard/france-establishments/`.

## The map has two traps

`Country Map` is the one visualisation that renders France without an external
tile provider, which is why the dashboard uses it rather than a deck.gl layer.

**It keys on `FR-<code>`, and names the overseas departments by ISO letter code:**

| Department | Map key |
|---|---|
| 971 Guadeloupe · 972 Martinique | `FR-GP` · `FR-MQ` |
| 973 Guyane · 974 La Réunion · 976 Mayotte | `FR-GF` · `FR-RE` · `FR-YT` |

Charting `code_departement` renders metropolitan France and drops the rest without
an error. The gold job publishes `code_carte` for this.

**Its renderer is `colorScheme ? categorical(country_id) : linear(metric)`.** Any
categorical scheme — including one inherited from the dashboard — colours by
department id and stops reflecting the data, while still looking plausible. The
dashboard sets no categorical scheme and the maps pass `color_scheme: ""`.

## Conventions worth keeping

- **Shares are fractions**, rendered with a percent format, so the unit sits on the
  number rather than in a subtitle. They are recomputed from the counts
  (`SUM(nb_qpv) / SUM(nb_etablissements)`): averaging per-department percentages
  would weight Lozère like Paris.
- **Charts are upserted by name, the dashboard by slug.** Renaming leaves orphans
  behind, so the build sweeps the whole `france_establishments` schema at the end.
  It never drops an Iceberg table — that is not a dashboard script's job.
- **Rendering authenticates per user.** Superset redirects the viewer to the
  identity provider on the first query, then queries Trino as them. This is why the
  connection cannot be tested from `superset shell`.

## Why "catégorie d'entreprise" has so many legal forms in it

`categorieEntreprise` is an INSEE statistical computation (décret 2008-1354) from
headcount, turnover and balance sheet, not a declared field — 43 % of
establishments have none. Rather than a large "unknown" slice, the gold job falls
back to the legal form, which is populated for all of them and explains the
absence: sole traders, property-holding companies and associations mostly have no
payroll and no accounts to declare.
