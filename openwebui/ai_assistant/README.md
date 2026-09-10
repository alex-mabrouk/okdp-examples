# AI Assistant — a text-to-SQL pipe for Open WebUI

Asks a French question, shows the SQL, runs it on the **gold** catalog, prints the rows.
The SQL is on screen on purpose: it is what makes the answer auditable.

One pipe, one entry per schema. `DOMAINS` holds what each one is called, what it contains
and which columns its literals are confronted with; adding a schema is one entry, not a
second file. The reader picks the domain in the model list, so the assistant never guesses
which one a question is about.

| Entry | Schema |
|---|---|
| *Établissements français* | `gold.france_establishments` |
| *Facturation électronique* | `gold.einvoicing` |

An Open WebUI **Pipe Function**: it runs inside the existing `open-webui` container and
appears in the model picker. Not a Tool (the model would
have to decide to call it) and not a Pipeline (a separate sidecar, disabled in the package).
The flow is ours, so the SQL always renders and a rejection is visible rather than retried
out of sight.

## Install

Open WebUI → *Admin → Functions → +* → paste `ai_assistant.py` → save, then open its
valves and set `CLIENT_SECRET` (Keycloak client `svc-trino-examples-writer`, from
`creds-examples-oauth2-trino`). The other valves default to the in-cluster endpoints of
the `demo` project. Pick a domain in a new chat.

Functions live in Open WebUI's database, not in git: this file is the source of truth and
a fresh cluster needs the paste again.

## Why the model is not trusted with the values

Two failures, both measured, both **silent** — valid SQL, no error, wrong answer:

| The model writes | What happens |
|---|---|
| `code_departement IN ('075', '69', '13')` | Paris is `'75'`; two rows come back instead of three |
| `code_departement = 'Paris'` | zero rows |

A third one is structural: `ZZ` — *Non déterminé*, 52 establishments and **0 in QPV** —
tops any bottom ranking, and is never an answer.

Two more, measured on the e-invoicing schema, are the same shape: the total invoice count
answered from the wrong column, and "how many invoices carry an unknown SIREN" answered
from `acteurs` with a self-referential subquery that returns 0 without failing.

No syntax check sees any of them. So the pipe loads the real values from gold — department
codes and labels, and for e-invoicing the control identifiers, families and severities —
and confronts every literal with them, the way act 1 confronts a generated sentence with
its fact. Where a schema has an unknown bucket that would top a ranking, it refuses a
ranking that does not exclude it. A rejection triggers **one** retry carrying the reason.

Prompt tuning alone does not get there — it is a see-saw. Three reference questions, five
runs each, `mistral:7b`:

| | Q1 | Q2 | Q3 |
|---|---|---|---|
| schema prompt | 0/5 | — | 5/5 |
| + two worked examples | 5/5 | 0/5 | 5/5 |
| + "never a guessed code" | 5/5 | 5/5 | 0/5 |
| + domain check and one retry | **5/5** | **5/5** | **5/5** |

Roughly 1 s per question, 2.5 s when a retry fires. A larger coder model buys nothing here
and costs the VRAM the act 1 DAG needs.

## What the guard is, and what it is not

`svc-trino-examples-writer` is a **writer** account and Trino carries no catalog rules, so
`guard()` — SELECT only, single statement, and only the schema of the selected domain — is
the sole barrier, not a second belt. It is a demo guard, not an authorization layer: that belongs in
Trino, and `../../trino_opa_policy/` is the example of it.

Trino also denies impersonation: `X-Trino-User` must be the JWT principal, here the
`client_id`. Any other value answers `cannot impersonate user`.
