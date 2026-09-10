"""
title: AI Assistant
id: ai_assistant
description: Answers a French question about a gold schema by writing SQL, checking it against the real values, and running it on Trino.
author: OKDP
version: 0.2.0
"""

import json
import re
import time

import requests
from pydantic import BaseModel, Field

CATALOG = "gold"

FRANCE_ESTABLISHMENTS_SCHEMA = """Catalog `gold`, schema `france_establishments`. Tables and columns:

etablissements_par_departement(code_departement varchar, libelle_departement varchar,
  nb_etablissements bigint, nb_qpv bigint, part_qpv double, nb_ess bigint, part_ess double,
  nb_sieges bigint, nb_communes bigint, nb_iris bigint, code_carte varchar)
etablissements_par_section_naf(code_departement varchar, libelle_departement varchar,
  code_section_naf varchar, libelle_section_naf varchar, nb_etablissements bigint, nb_qpv bigint)
etablissements_par_commune(code_departement varchar, libelle_departement varchar,
  code_commune varchar, libelle_commune varchar, commune_departement varchar,
  nb_etablissements bigint, nb_qpv bigint)
etablissements_par_categorie(code_departement varchar, libelle_departement varchar,
  categorie varchar, nb_etablissements bigint)
creations_par_mois(mois_creation varchar, code_departement varchar,
  libelle_departement varchar, nb_creations bigint)

Notes:
- nb_etablissements is the number of active establishments. nb_qpv and nb_ess count
  subsets of it: a question about how many establishments there are means
  nb_etablissements, never nb_qpv nor nb_ess.
- part_qpv and part_ess are percentages already computed (0-100), never recompute them.
- code_departement is a zero-padded string ('01', '2A', '974'); 'ZZ' means unknown and must
  be excluded with WHERE code_departement <> 'ZZ' in any ranking.
- Every table is per-department; there is no national roll-up row, sum over departments."""

EINVOICING_SCHEMA = """Catalog `gold`, schema `einvoicing`. Tables and columns:

facturation_mensuelle(mois varchar, mois_date date, nb_factures bigint, nb_emetteurs bigint,
  nb_acheteurs bigint, montant_ht decimal, montant_tva decimal, montant_ttc decimal,
  nb_factures_anomalie bigint, nb_factures_bloquantes bigint, taux_anomalie double,
  montant_moyen decimal)
facturation_par_departement(code_departement varchar, code_carte varchar,
  libelle_departement varchar, nb_factures bigint, nb_emetteurs bigint, montant_ht decimal,
  montant_ttc decimal, nb_factures_anomalie bigint, taux_anomalie double)
facturation_par_section_naf(code_section_naf varchar, libelle_section_naf varchar,
  nb_factures bigint, nb_emetteurs bigint, montant_ht decimal, montant_ttc decimal,
  nb_factures_anomalie bigint, montant_moyen decimal, taux_anomalie double)
acteurs(role varchar, siren varchar, siret varchar, nom varchar, code_departement varchar,
  libelle_departement varchar, code_section_naf varchar, categorie_entreprise varchar,
  nb_factures bigint, montant_ht decimal, montant_ttc decimal, nb_factures_anomalie bigint)
qualite_anomalies(regle_id varchar, famille varchar, gravite varchar, libelle varchar,
  nb_constats bigint, nb_factures bigint, montant_impacte decimal, part_factures double)
conformite_reforme(categorie_entreprise varchar, obligation_emission varchar,
  nb_factures bigint, nb_emetteurs bigint, montant_ht decimal, nb_factures_anomalie bigint,
  part_factures double, taux_anomalie double)

Notes:
- taux_anomalie and part_factures are FRACTIONS between 0 and 1, not percentages.
- Amounts are in euros. montant_ht excludes VAT, montant_ttc includes it.
- mois is 'YYYY-MM'; use mois_date for anything chronological.
- role is 'émetteur' or 'acheteur', with the accent.
- obligation_emission is '2026-09-01' for large companies and mid-caps, '2027-09-01' for
  SMEs, 'indéterminée' when SIRENE does not give the size.
- Anomalies are counted in invoices: nb_factures on qualite_anomalies is how many invoices
  a control caught, nb_constats how many findings it raised, which is always larger.
- Any question about a control, an anomaly, a rejected invoice, a duplicate or the SIRENE
  referential is answered from qualite_anomalies, one row per control. REF-SIREN-INCONNU is
  the SIREN absent from the referential, REF-EMETTEUR-CESSE the issuer that has ceased
  trading, MET-DOUBLON the invoice number already issued. montant_impacte is the amount at
  stake on a control, and it is the only column that answers "how much is at stake".
- "Facturer le plus" is about montant_ht, never about the number of invoices.
- qualite_anomalies has NO time column: it covers the whole flow, and it is still the
  table for any question that does not name a period. Only a question naming a month or
  a period goes to facturation_mensuelle, which carries nb_factures_anomalie and
  nb_factures_bloquantes per month but no breakdown per control.
- Every table is already aggregated; never divide two of its columns again."""

# Every rule below was added because a measured run failed without it. The two worked
# examples matter more than the prose: they are what took Q1 from 0/5 to 5/5.
COMMON_RULES = """Rules:
- Answer with the SQL query only, no explanation, no markdown fence.
- SELECT only. Never write to any table.
- Only the tables listed above. The catalogs `bronze` and `silver` do not exist for you.
- Always fully qualify tables as {catalog}.{schema}.<table>.
- Always end a ranking with an explicit LIMIT.
- Select the raw columns. No AS aliases, no computed columns, no renaming.
- Filter on nothing the question did not ask for.
- Never invent a code. A value named in the question is matched on the label column
  named below, never on a code you guessed ('075' is not '75')."""

FRANCE_ESTABLISHMENTS_RULES = """- etablissements_par_departement already holds ONE row per department. Never GROUP BY it,
  never SUM it, never divide two of its columns: part_qpv and part_ess are the percentages.
- code_departement exists for ONE purpose: excluding 'ZZ'. A department named in the
  question is matched on libelle_departement.

Example question: Quels sont les 5 departements avec le plus d'etablissements ?
Example answer: SELECT libelle_departement, nb_etablissements FROM gold.france_establishments.etablissements_par_departement WHERE code_departement <> 'ZZ' ORDER BY nb_etablissements DESC LIMIT 5

Example question: Compare la Savoie et l'Isere : nombre d'etablissements et part ESS.
Example answer: SELECT libelle_departement, nb_etablissements, part_ess FROM gold.france_establishments.etablissements_par_departement WHERE libelle_departement IN ('Savoie', 'Isère')"""

EINVOICING_RULES = """- Each table already holds one row per key. Never GROUP BY a table that is already
  grouped the way the question asks.
- A department named in the question is matched on libelle_departement, a control on
  libelle, a size class on categorie_entreprise.

Example question: Quels sont les 5 controles qui rejettent le plus de factures ?
Example answer: SELECT libelle, nb_factures, montant_impacte FROM gold.einvoicing.qualite_anomalies ORDER BY nb_factures DESC LIMIT 5

Example question: Combien de factures viennent d'entreprises deja soumises a l'obligation ?
Example answer: SELECT categorie_entreprise, nb_factures FROM gold.einvoicing.conformite_reforme WHERE obligation_emission = '2026-09-01'

Example question: Combien de factures ont un SIREN emetteur absent du referentiel ?
Example answer: SELECT libelle, nb_factures FROM gold.einvoicing.qualite_anomalies WHERE regle_id = 'REF-SIREN-INCONNU'"""

# A domain is a schema the assistant may answer on: what it is called, what it holds,
# and the columns whose literals are confronted with the real values before the query
# is ever run.
DOMAINS = {
    "france-establishments": {
        "name": "Établissements français",
        "schema": "france_establishments",
        "doc": FRANCE_ESTABLISHMENTS_SCHEMA,
        "rules": FRANCE_ESTABLISHMENTS_RULES,
        "domain_query": (
            "SELECT code_departement, libelle_departement "
            "FROM gold.france_establishments.etablissements_par_departement"
        ),
        "domain_columns": ("code_departement", "libelle_departement"),
        # 'ZZ' (Non determine, 52 establishments, 0 in QPV) tops every bottom ranking
        # and is never an answer. The prompt asks for the exclusion; this makes it hold.
        "ranking_sentinel": "ZZ",
        "ranking_columns": ("libelle_departement", "code_departement"),
        # None: part_qpv and part_ess are already percentages here.
        "fraction_columns": (),
    },
    "einvoicing": {
        "name": "Facturation électronique",
        "schema": "einvoicing",
        "doc": EINVOICING_SCHEMA,
        "rules": EINVOICING_RULES,
        # Departments, controls and size classes are the three things a question names
        # by hand, and all three are spelled in ways a model gets subtly wrong.
        "domain_query": (
            "SELECT code_departement, libelle_departement, NULL, NULL, NULL "
            "FROM gold.einvoicing.facturation_par_departement "
            "UNION ALL SELECT NULL, NULL, regle_id, famille, gravite "
            "FROM gold.einvoicing.qualite_anomalies"
        ),
        "domain_columns": (
            "code_departement", "libelle_departement", "regle_id", "famille", "gravite",
        ),
        "ranking_sentinel": None,
        "ranking_columns": (),
        # Stored between 0 and 1, rendered as a percentage. Declared rather than
        # inferred: the model must never be asked to multiply, and no rule of thumb
        # tells a fraction from a small percentage.
        "fraction_columns": ("taux_anomalie", "part_factures"),
    },
}


def system_prompt(domain):
    return "\n\n".join(
        [
            "You translate a French business question into ONE Trino SQL query.",
            domain["doc"],
            COMMON_RULES.format(catalog=CATALOG, schema=domain["schema"]),
            domain["rules"],
        ]
    )


FORBIDDEN = re.compile(
    r"\b(insert|update|delete|merge|create|drop|alter|truncate|grant|revoke|call|"
    r"commit|rollback|prepare|execute|set\s+session)\b",
    re.I,
)


def guard(sql, schema):
    """Static check. The service account is a writer, so this is the only barrier."""
    s = sql.strip().rstrip(";")
    if ";" in s:
        return False, "several statements in one query"
    if not re.match(r"^\s*(select|with)\b", s, re.I):
        return False, "not a SELECT"
    if FORBIDDEN.search(s):
        return False, "forbidden keyword"
    # CTE names are local aliases, not tables.
    ctes = {n.lower() for n in re.findall(r"(?:\bwith\s+|,\s*)([a-z0-9_]+)\s+as\s*\(", s, re.I)}
    found = re.findall(r"\bfrom\s+([a-z0-9_.\"]+)|\bjoin\s+([a-z0-9_.\"]+)", s, re.I)
    refs = [t for pair in found for t in pair if t and t.lower() not in ctes]
    if not refs:
        return False, "no table referenced"
    for ref in refs:
        parts = ref.replace('"', "").split(".")
        if len(parts) != 3 or parts[0].lower() != CATALOG or parts[1].lower() != schema:
            return False, f"table outside {CATALOG}.{schema}: {ref}"
    return True, ""


def domain_check(sql, known):
    """Confront every literal with the real values, the way an insight is confronted with
    its fact. `code_departement IN ('075')` and `code_departement = 'Paris'` are both valid
    SQL that answer wrong in silence; nothing else catches them."""
    for column, values in known.items():
        blobs = re.findall(
            rf"{column}\s*(?:=|in)\s*\(?([^)]*?)(?:\)|\s+(?:and|or|group|order|limit)\b|$)",
            sql,
            re.I,
        )
        for blob in blobs:
            for literal in re.findall(r"'([^']*)'", blob):
                if literal not in values:
                    return False, f"{column} = '{literal}' is not a known value"
    return True, ""


def ranking_check(sql, sentinel, columns):
    """The unknown bucket tops every bottom ranking and is never an answer."""
    if not sentinel:
        return True, ""
    names = "|".join(columns)
    selected = re.search(r"\bselect\b(.*?)\bfrom\b", sql, re.I | re.S)
    if not selected or not re.search(r"\border\s+by\b", sql, re.I):
        return True, ""
    if not re.search(rf"\b({names})\b", selected.group(1), re.I):
        return True, ""
    # Naming the rows pins the answer; the unknown one cannot turn up.
    if re.search(rf"\b({names})\s*(=|in)\b", sql, re.I):
        return True, ""
    if f"'{sentinel}'" not in sql.upper():
        return False, f"a ranking must exclude the '{sentinel}' bucket"
    return True, ""


def pretty(sql):
    """One clause per line: a query that scrolls sideways cannot be read on screen."""
    out = re.sub(r"\s+", " ", sql.strip())
    for kw in ("FROM", "WHERE", "GROUP BY", "ORDER BY", "HAVING", "LIMIT"):
        out = re.sub(rf"\s+{kw}\s+", f"\n{kw} ", out, flags=re.I)
    return re.sub(r"\s+(AND|OR)\s+", r"\n  \1 ", out, flags=re.I)


def table(columns, rows, fractions=()):
    """Fractions are rendered as percentages here rather than computed in SQL: the
    query stays the raw columns, and the model is never asked to multiply."""
    scaled = {i for i, c in enumerate(columns) if c in fractions}

    def cell(v, i):
        if v is None:
            return ""
        if i in scaled:
            try:
                return f"{float(v) * 100:.2f} %".replace(".", ",")
            except (TypeError, ValueError):
                pass
        return str(v).replace("|", "\\|")

    head = "| " + " | ".join(columns) + " |"
    rule = "| " + " | ".join("---" for _ in columns) + " |"
    body = [
        "| " + " | ".join(cell(v, i) for i, v in enumerate(row)) + " |" for row in rows
    ]
    return "\n".join([head, rule] + body)


class Pipe:
    class Valves(BaseModel):
        OLLAMA_URL: str = Field(default="http://demo-ollama-main.demo.svc.cluster.local:11434")
        MODEL: str = Field(default="mistral:7b")
        TRINO_URL: str = Field(default="http://demo-trino-main-trino.demo.svc.cluster.local:8080")
        TOKEN_URL: str = Field(
            default="https://keycloak.okdp.sandbox/realms/master/protocol/openid-connect/token"
        )
        CLIENT_ID: str = Field(default="svc-trino-examples-writer")
        CLIENT_SECRET: str = Field(default="")
        MAX_ROWS: int = Field(default=50)
        SHOW_SQL: bool = Field(default=True, description="Show the query before its result")

    def __init__(self):
        self.valves = self.Valves()
        self._token = (None, 0.0)
        self._domains = {}

    def pipes(self):
        """One entry per schema: the reader picks the domain in the model list, and the
        assistant never has to guess which one a question is about."""
        return [{"id": key, "name": d["name"]} for key, d in DOMAINS.items()]

    def _domain(self, body):
        # Open WebUI addresses a pipe as "<function id>.<pipe id>".
        wanted = (body.get("model") or "").split(".")[-1]
        return DOMAINS.get(wanted)

    def _bearer(self):
        token, expiry = self._token
        if token and time.time() < expiry:
            return token
        r = requests.post(
            self.valves.TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self.valves.CLIENT_ID,
                "client_secret": self.valves.CLIENT_SECRET,
            },
            timeout=30,
        )
        r.raise_for_status()
        body = r.json()
        self._token = (body["access_token"], time.time() + body.get("expires_in", 300) - 30)
        return self._token[0]

    def _sql(self, sql, max_rows=None):
        # Trino denies impersonation: X-Trino-User must be the JWT principal, the client_id.
        session = requests.Session()
        session.headers.update(
            {
                "Authorization": f"Bearer {self._bearer()}",
                "X-Trino-User": self.valves.CLIENT_ID,
            }
        )
        r = session.post(f"{self.valves.TRINO_URL}/v1/statement", data=sql.encode(), timeout=60)
        r.raise_for_status()
        payload, rows, columns = r.json(), [], None
        limit = self.valves.MAX_ROWS if max_rows is None else max_rows
        while True:
            if "error" in payload:
                raise RuntimeError(payload["error"].get("message", "Trino error"))
            columns = columns or [c["name"] for c in payload.get("columns", [])] or None
            rows += payload.get("data") or []
            nxt = payload.get("nextUri")
            if not nxt:
                return columns or [], rows[:limit]
            time.sleep(0.15)
            payload = session.get(nxt, timeout=60).json()

    def _known_values(self, key, domain):
        cached = self._domains.get(key)
        if cached and time.time() < cached[1]:
            return cached[0]
        _, rows = self._sql(domain["domain_query"], max_rows=100000)
        known = {column: set() for column in domain["domain_columns"]}
        for row in rows:
            for column, value in zip(domain["domain_columns"], row):
                if value is not None:
                    known[column].add(value)
        self._domains[key] = (known, time.time() + 3600)
        return known

    def _write_sql(self, domain, question, rejection=None):
        prompt = question
        if rejection:
            prompt = (
                f"{question}\n\nYour previous query was rejected: {rejection}\n"
                "Write it again, corrected."
            )
        r = requests.post(
            f"{self.valves.OLLAMA_URL}/api/chat",
            timeout=180,
            json={
                "model": self.valves.MODEL,
                "stream": False,
                "options": {"temperature": 0.0},
                "format": {
                    "type": "object",
                    "properties": {"sql": {"type": "string"}},
                    "required": ["sql"],
                },
                "messages": [
                    {"role": "system", "content": system_prompt(domain)},
                    {"role": "user", "content": prompt},
                ],
            },
        )
        r.raise_for_status()
        return json.loads(r.json()["message"]["content"])["sql"].strip().rstrip(";")

    def _attempt(self, domain, question, known, rejection=None):
        sql = self._write_sql(domain, question, rejection)
        checks = (
            lambda: guard(sql, domain["schema"]),
            lambda: ranking_check(sql, domain["ranking_sentinel"], domain["ranking_columns"]),
            lambda: domain_check(sql, known),
        )
        for check in checks:
            ok, reason = check()
            if not ok:
                return sql, None, None, reason
        try:
            columns, rows = self._sql(sql)
        except Exception as e:
            return sql, None, None, f"Trino refused it: {e}"
        if not rows:
            return sql, columns, rows, "the query returned no row"
        return sql, columns, rows, None

    def pipe(self, body: dict):
        key = (body.get("model") or "").split(".")[-1]
        domain = DOMAINS.get(key)
        if not domain:
            return "Choisis un domaine dans la liste des modèles."

        question = ""
        for message in reversed(body.get("messages", [])):
            if message.get("role") == "user":
                question = message.get("content") or ""
                break
        if not question.strip():
            return f"Pose une question sur : {domain['name'].lower()}."
        if not self.valves.CLIENT_SECRET:
            return "CLIENT_SECRET is not set in the AI Assistant valves."

        started = time.time()
        try:
            known = self._known_values(key, domain)
        except Exception as e:
            return f"Le catalogue gold est injoignable : {e}"

        sql, columns, rows, rejection = self._attempt(domain, question, known)
        retried = rejection
        if rejection:
            sql, columns, rows, rejection = self._attempt(domain, question, known, rejection)

        out = []
        if self.valves.SHOW_SQL:
            out.append(f"```sql\n{pretty(sql)}\n```")
        if rejection:
            out.append(
                f"**Requête rejetée** : {rejection}. La reprise a échoué elle aussi, "
                "aucune réponse n'est donnée."
                if retried
                else f"**Requête rejetée** : {rejection}. Aucune réponse n'est donnée."
            )
        else:
            if retried:
                out.append(f"> Première requête rejetée : {retried}. Reprise ci-dessus.")
            out.append(table(columns, rows, domain["fraction_columns"]))
            out.append(
                f"*{len(rows)} ligne(s) · {self.valves.MODEL} · {time.time() - started:.1f} s*"
            )
        return "\n\n".join(out)
