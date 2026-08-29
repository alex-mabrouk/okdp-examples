# France establishments

A medallion chain over four French open-data sources, linked by assets so the
lineage graph in the UI is the real dependency graph:

```
france_establishments_bronze   @monthly
  land_* → partition_*  ──→ bronze_france_establishments_{sirene_geoloc,
                              sirene_etablissement, sirene_unite_legale, ban}
france_establishments_silver   ◀── the three SIRENE assets
  conform_establishments  ──→ silver_france_establishments_etablissements_actifs
france_establishments_gold     ◀── the silver asset
  build_indicators        ──→ the five gold tables
```

Only bronze carries a clock. `bronze_..._ban` is produced but consumed by nobody:
BAN is landed for the address enrichment to come, and making silver depend on it
would stall the chain on a source nothing reads.

## Conformation rules

Four of them, none guessable from the source documentation:

| Rule | What it does |
|---|---|
| `CSZ` is not a null | It means *commune sans zonage*. Zoning codes equal to it become null |
| QPV is a prefix | Most filled `plg_qp24` values are `HZ`, *hors zone*: `en_qpv` is `LIKE 'Q%'` |
| IRIS is local to the commune | The national code is `plg_code_commune \|\| plg_iris` |
| Dates need bounding | SIRENE holds year 0004 and year 2218; outside `[1800, today]` becomes null |

Silver keeps only active establishments (`etatAdministratifEtablissement = 'A'`)
and inner-joins the geolocation, so an establishment with no coordinates is
dropped rather than carried unmappable.

Gold publishes a `code_carte` column because Superset's France map does not key on
the department code; the five tables all carry `code_departement` so that one
dashboard filter drives every chart. Both are explained in
[`../superset/france_establishments/`](../superset/france_establishments/).

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `FRANCE_ESTABLISHMENTS_DEPARTEMENTS` | *(empty = national)* | Departments to process, e.g. `31,09`. Silver rewrites only their partitions |
| `FRANCE_ESTABLISHMENTS_BRONZE_BUCKET` / `_PREFIX` | `bronze` / `france_establishments` | Bronze location |
| `FRANCE_ESTABLISHMENTS_{SILVER,GOLD}_CATALOG` / `_NAMESPACE` | `silver`/`gold` / `france_establishments` | Iceberg targets |

```bash
kubectl exec -n demo deploy/demo-trino-main-trino-coordinator -- \
  trino --execute "SELECT code_departement, nb_etablissements, part_qpv \
                   FROM gold.france_establishments.etablissements_par_departement \
                   ORDER BY nb_etablissements DESC LIMIT 10"
```
