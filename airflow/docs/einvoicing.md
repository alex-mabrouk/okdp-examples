# E-invoicing

A flow of Factur-X invoices, checked against the SIRENE referential the
[establishments chain](france-establishments.md) publishes. The two are one lineage
graph: this chain reads the other's assets, so a change upstream is visible here.

```
einvoicing_generate            schedule=None, triggered with count / seed / pdf_count
  build_casting → generate_invoices  ──→ raw_einvoicing_factures
    ◀ inlets: silver_..._etablissements_actifs, bronze_..._{sirene_etablissement,ban}
einvoicing_bronze              ◀── the raw asset
  ingest                       ──→ bronze_einvoicing_factures_cii
einvoicing_silver              ◀── the bronze asset
  conform                      ──→ silver_einvoicing_{factures,lignes,anomalies}
einvoicing_gold                ◀── the three silver assets
  build_indicators             ──→ the nine gold tables
einvoicing_ai                  ◀── the nine gold assets
  write_insights               ──→ gold_einvoicing_{insights_facts,insights}
```

The referential belongs in `inlets`, never in `schedule`: asset scheduling is an
AND, so a DAG waiting on both its own input and the SIRENE assets would sit until
the other chain republished.

## Real or synthetic

| Real and public | Made up by this repo |
|---|---|
| The companies: SIRET, SIREN, name, NAF, address, size class, administrative state | Every invoice, amount, date, line and payment term |
| The reform's calendar and its size-class tiering | The anomalies, injected on purpose |

No invoice below was ever issued. The point of drawing the parties from SIRENE is
that the checks then have something real to fail against.

## Format

**Factur-X 1.09.2 / ZUGFeRD 2.5.2**, published 4 August 2026. Two profiles are
produced: `EN 16931`, the socle of the reform, and `EXTENDED-CTC-FR` for about 5 %
of the flow. Every invoice is written as CII XML; a subset is also rendered as a
PDF/A-3b carrying that XML as an attachment, which is what Factur-X is.

OKDP is not an accredited platform (*plateforme agréée*), does not do e-reporting
and does not hold the directory. It receives, checks and aggregates.

## Generating a flow

`einvoicing_generate` has no schedule. Trigger it with a config:

```json
{"count": 1000, "seed": 20260909, "pdf_count": 500}
```

The seed is what makes a run reproducible: same seed, same invoices, same injected
anomalies, so a control can be compared to the ground truth run after run. The
generator purges its prefixes first — the month sits in the S3 key and moves when
the random sequence changes, so without the purge runs accumulate instead of
overwriting.

Bronze, silver, gold and the AI job then follow on their own, through the assets.

## What is checked

Ten controls, in four families. Nine answer yes or no; the tenth reports.

| Family | Controls |
|---|---|
| `FORMAT` | CII schema, EN 16931 Schematron, unknown profile, missing issuer name |
| `REFERENTIEL` | issuer SIREN absent from SIRENE, issuer establishment ceased, buyer SIRET absent |
| `METIER` | totals, VAT rate, duplicate, due date, mandatory statement, VAT number key |
| `STATISTIQUE` | amount outside the distribution of its sector |

Two of them are worth reading twice. `MET-TAUX-TVA` carries an odd rate with a
correctly computed VAT amount, so the official Schematron passes it and only the
business rule catches it. `FMT-SCHEMATRON` drops the seller's VAT number, which
`BR-S-02` requires: caught by the standard and by nothing of ours.

`STA-MONTANT-ABERRANT` is the one control that is not exact. It reports, it never
rejects. The threshold is set from the flow itself: on the measured run no
legitimate invoice exceeded 5.4 times its sector's median and the 99th percentile
sat at 4.1, so the threshold is 8 and the injection 30 times.

Anomalies are counted in invoices, never in findings: one malformed invoice breaks
four Schematron rules at once, and counting findings would put the rate at 12 %
where it is 7.75 %.

## Tables

`silver.einvoicing` holds `factures`, `lignes` and `anomalies` — one row per
document received, per line, per finding. Bronze keeps the XML byte for byte with
its SHA-256 fingerprint, so any figure can be traced back to the file it came from.

`gold.einvoicing` holds the nine the dashboard reads:

| Table | What it answers |
|---|---|
| `facturation_mensuelle` | volume, amounts and anomaly rate over time |
| `facturation_par_departement` | the same by issuer department, with `code_carte` for the map |
| `facturation_par_section_naf` | the same by sector |
| `acteurs` | issuers and receivers in one table, told apart by `role` |
| `qualite_anomalies` | one row per control: invoices caught and amount at stake |
| `conformite_reforme` | the flow split by the reform's deadlines |
| `conformite_par_section_naf` / `_par_departement` | the same calendar by sector and by department: where the September 2027 wave lands |
| `emetteurs_en_anomalie` | the companies a control caught, by name and SIREN |

`conformite_reforme` is the table the rest exists for: the issuer's size class comes
from SIRENE, and the reform keys its September 2026 and September 2027 deadlines on
it, so the flow splits into what is already mandatory and what is not yet. Broken down
by sector, the same calendar answers a different question — which trades still have to
be brought along — and that one needs the registry, not the invoice.

`emetteurs_en_anomalie` names them. A rate convinces nobody in a meeting; a list of
companies does, and this list exists only because SIRENE sits next to the flow.

> ⚠️ The companies are real and public, the invoices are not. Any table naming a company
> next to an anomaly has to say so out loud: no invoice below was ever issued, and no
> real company ever invoiced from a ceased establishment here.

`einvoicing_ai` adds `insights_facts` and `insights`. Spark computes every figure
*and* the French sentence asserting it; the local model only rephrases; each
sentence is checked back against its figure and the rejects are kept, because the
check is part of what there is to show.

## Dashboard

Slug `einvoicing`, built by the scripts in
[`../../superset/einvoicing/`](../../superset/einvoicing/), which are the source of
truth rather than the export.

```bash
kubectl exec -i -n demo deploy/demo-trino-main-trino-coordinator -- trino <<'SQL'
SELECT regle_id, famille, nb_factures, montant_impacte
FROM gold.einvoicing.qualite_anomalies ORDER BY nb_factures DESC;
SQL
```

Pass the query on stdin, not with `--execute`: through `kubectl exec` the latter
mangles accented characters and returns no row without an error.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `EINVOICING_INVOICE_COUNT` / `_SEED` / `_PDF_COUNT` | `20000` / `20260909` / `500` | Defaults of the trigger parameters |
| `EINVOICING_MONTHS` | `24` | Months the flow spreads over, ending last month |
| `EINVOICING_ANOMALY_RATE` / `_EXTENDED_RATE` | `0.06` / `0.05` | Share of anomalous invoices, share on the EXTENDED profile |
| `EINVOICING_CASTING_{SUPPLIERS,BUYERS,CLOSED}` | `2000` / `300` / `150` | Size of the cast drawn from SIRENE |
| `EINVOICING_BRONZE_BUCKET` / `_PREFIX` | `bronze` / `einvoicing` | Bronze location |
| `EINVOICING_{SILVER,GOLD}_CATALOG` / `_NAMESPACE` | `silver`/`gold` / `einvoicing` | Iceberg targets |
| `EINVOICING_SPARK_IMAGE` | `ghcr.io/…-spark-einvoicing:0.2.0` | Carries factur-x, reportlab and Saxon-HE |
| `EINVOICING_OLLAMA_URL` / `_MODEL` | in-cluster ollama / `mistral:7b` | Used by `einvoicing_ai` only |

The image tag must be bumped on every change: `spark_submit` sets
`imagePullPolicy: IfNotPresent`, so pushing the same tag again installs nothing.
