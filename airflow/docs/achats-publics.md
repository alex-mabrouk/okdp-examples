# Public procurement (achats publics)

French public contracts (DECP, Ministères économiques et financiers) matched
with SIRENE and the INSEE geographic code (COG), linked by assets:

```
achats_publics_bronze   @monthly
  land_decp → split_decp → publish_decp  ──→ bronze_achats_publics_decp
  land_cog → publish_cog                 ──→ bronze_achats_publics_cog
achats_publics_silver   ◀── decp, cog, SIRENE etablissement and unite_legale
  build_referentiels  ──→ silver.referentiels.geo_{departements,communes}
  conform_marches     ──→ silver.achats_publics.{marches,attributions},
                          silver.referentiels.acheteurs
achats_publics_gold     ◀── the silver tables
  build_datamart      ──→ gold.achats_publics.{attributions,achats}
```

SIRENE is landed by `france_establishments_bronze`, read here from its bronze
prefix: run that DAG first.

The consolidated DECP files are single JSON documents of 550 to 700 MB.
`split_decp` cuts them into JSON Lines in a stream (`decp_stream.py`), so
neither the task pod nor Spark has to hold one in memory. Bronze keeps every
value as text.

## Business rules

| Rule | What it does |
|---|---|
| One row per contract | A contract is its buyer SIRET and its id. The latest published version wins (publication date, then file, then number of modifications) |
| SIRET | Spaces and dots removed; anything but 14 digits is not a SIRET |
| Dates | Outside `[2000-01-01, today]` becomes null |
| Abnormal amounts | Below 100 EUR, from a billion EUR, or a run of nines: flagged, excluded from the sums |
| Amount copied on each lot | Same buyer, object, amount and year: the amount is split between the lots |
| Several suppliers | The contract counts and amounts are split evenly between them (`part_marche`) |
| Buyer type | From the INSEE legal category of the buyer |

Measured on the files of November 2026 (2024 to 2026): 922 539 rows in bronze,
244 440 duplicates removed, 577 377 contracts; 98.3 % of the suppliers found
with SIRENE.

The amount of a framework agreement is its ceiling, not what was spent: keep
`accord_cadre` in view when reading amounts. The current year is incomplete for
months after its end, as contracts are published late: compare complete years.

## Materialized or virtual

`gold.achats_publics.achats` is computed by the DAG.
[`../../trino/achats_publics/v_achats.sql`](../../trino/achats_publics/v_achats.sql)
creates `gold.achats_publics.v_achats`, the same rows computed by Trino from
silver at query time; run it once in Trino.
[`question_4_4.sql`](../../trino/achats_publics/question_4_4.sql) asks the same
question of both.

## Configuration

| Variable | Default |
|---|---|
| `ACHATS_PUBLICS_ANNEES` | `2024,2025,2026` (notification years) |
| `ACHATS_PUBLICS_DECP_URLS` | the 2024, 2025 and 2026 consolidated files |
| `ACHATS_PUBLICS_COG_{COMMUNE,DEPARTEMENT,REGION}_URL` | COG 2026 |
| `ACHATS_PUBLICS_{BRONZE_BUCKET,BRONZE_PREFIX}` | `bronze`, `achats_publics` |
| `ACHATS_PUBLICS_{SILVER,GOLD}_CATALOG` | `silver`, `gold` |
