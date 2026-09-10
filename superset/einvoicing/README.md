# Superset dashboard — facturation électronique

Reproducible build of the AIFE demo dashboard on the **gold** catalog (Iceberg via
Polaris, queried through Trino): one database connection, ten datasets, 24 charts and
one dashboard. Idempotent — upserts by name, safe to re-run.

Laid out in three levels of reading, in this order: what the flow weighs and how it
moves, what the reform says of it, what the controls found in it. The raw detail comes
last — it is evidence to drill into, not the opening screen.

> The dashboard lives only in Superset's metadata database, so a fresh cluster has
> to rebuild it. These scripts are the source of truth: `export-dashboards` needs a
> web request context and fails from a shell.

> ⚠️ Close the dashboard tab before re-running, and reload after. The layout is
> rebuilt whole from `ROWS`, but a browser that has the dashboard open holds its own
> copy and can write it back — which restores the layout you just replaced, references
> to since-deleted charts included. Observed: a rebuild that reported 24 charts was
> found minutes later with 22 and a dangling `CHART-obligation_horizon` node, and the
> page showed *There is no chart definition associated with this component*.

## Apply

`einvoicing_gold` must have run first. Then, from this directory:

```bash
POD=$(kubectl -n demo get pod -l app=superset --field-selector=status.phase=Running -o name \
  | grep -E 'superset-main-[0-9a-f]+-' | grep -vE 'worker|redis|websocket' | head -1 | sed 's@pod/@@')
for f in 01-build-datasets.py 02-build-dashboard.py; do
  kubectl -n demo cp "$f" "demo/$POD:/tmp/$f" -c superset
  echo "exec(open('/tmp/$f').read())" | kubectl -n demo exec -i "$POD" -c superset -- superset shell
done
```

Then open `https://superset-demo.okdp.sandbox/superset/dashboard/einvoicing/`.

## Two schemas, one database

The establishments chain publishes its own datasets through the same `trino-gold`
connection, and both chains publish an `insights`. Datasets are therefore looked up
by **schema and name**, and the final sweep only walks the `einvoicing` schema.

Charts have no schema to key on: `Slice` is upserted by title across the whole
instance. So the AI panel here is titled *Lecture du flux de factures par le modèle
local* and the establishments one *Lecture des indicateurs par le modèle local* —
sharing a title would rebind one chart to the other's dataset and leave each
dashboard showing the other's sentences.

The `Country Map` traps are the same ones the establishments dashboard documents —
it keys on `code_carte` (`FR-<code>`, overseas by ISO letters), and any categorical
colour scheme makes it colour by department id instead of by the metric.

## Anomalies are counted in invoices

One malformed invoice breaks four Schematron rules at once. Measured on a run of
1 006 documents: **110 findings for 72 invoices**. Every rate on the dashboard is
computed on invoices, and `nb_constats` sits next to `nb_factures` in the quality
table so the gap is visible rather than hidden.

`montant_impacte` is what makes that table readable without knowing the rule
identifiers: on the duplicates line, it answers "how much would have been paid
twice".

## Two deadlines, not one figure

`404 factures déjà soumises à l'obligation` on its own is a number with no scale: what
gives it meaning is the 582 still to come, and that one was only readable by measuring
a bar. Both now sit side by side as their own figures.

Two tiles rather than one two-bar chart: plotting a column and grouping on the same
column is rejected at render time with *Duplicate column/metric labels* — a blank chart
on the dashboard and nothing at build time. `verifier_labels()` now fails the build on
that instead. Colouring 2027 differently would need a per-bar series, which costs more
than the distinction is worth; the wording carries it — *déjà* against *à raccorder
d'ici*.

## Year on year, never month on month

The three dynamic KPIs read the last closed month against the same month a year
earlier, with a twelve-period lag. August is half a normal month by design, so a
month-over-month reading of it announces -53 % and means "it is August". The lag
compares like with like: +221 % on the measured flow, which is the ramp the reform
produces.

## The AI panel

`einvoicing_ai` publishes `insights`, and the panel shows only `status = 'verified'`.
The rejected sentences stay in the table on purpose — the check is part of what there
is to demonstrate — but a dashboard is not where a sentence the pipeline refused
belongs.

## Why the reform chart is the one that matters

`conformite_reforme` splits the flow by the issuer's size class, which comes from
SIRENE and which the reform keys its deadlines on: issuing has been mandatory for
large companies and ETI since 1 September 2026, and becomes so for SMEs on
1 September 2027. Reception has been mandatory for everyone since September 2026,
with no tiering — only issuing is staged, and that is what the chart splits.

The figures are the referential speaking, not an assumption about the flow.
