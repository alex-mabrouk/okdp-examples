"""
title: Gold Copilot
id: gold_copilot
description: Answers a French question about the france_establishments gold catalog by writing SQL, checking it, and running it on Trino.
author: OKDP
version: 0.1.0
"""

import re
import time

import requests
from pydantic import BaseModel, Field

CATALOG = "gold"
SCHEMA = "france_establishments"

GOLD_SCHEMA = """Catalog `gold`, schema `france_establishments`. Tables and columns:

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
- part_qpv and part_ess are percentages already computed (0-100), never recompute them.
- code_departement is a zero-padded string ('01', '2A', '974'); 'ZZ' means unknown and must
  be excluded with WHERE code_departement <> 'ZZ' in any ranking.
- Every table is per-department; there is no national roll-up row, sum over departments."""

# Every rule below was added because a measured run failed without it. The two worked
# examples matter more than the prose: they are what took Q1 from 0/5 to 5/5.
SYSTEM_PROMPT = f"""You translate a French business question into ONE Trino SQL query.

{GOLD_SCHEMA}

Rules:
- Answer with the SQL query only, no explanation, no markdown fence.
- SELECT only. Never write to any table.
- Only the tables listed above. The catalogs `bronze` and `silver` do not exist for you.
- Always fully qualify tables as gold.france_establishments.<table>.
- Always end a ranking with an explicit LIMIT.
- etablissements_par_departement already holds ONE row per department. Never GROUP BY it,
  never SUM it, never divide two of its columns: part_qpv and part_ess are the percentages.
- Select the raw columns. No AS aliases, no computed columns, no renaming.
- Filter on nothing the question did not ask for.
- code_departement exists for ONE purpose: excluding 'ZZ'. A department named in the question
  is matched on libelle_departement, never on a code you guessed ('075' is not '75').

Example question: Quels sont les 5 departements avec le plus d'etablissements ?
Example answer: SELECT libelle_departement, nb_etablissements FROM gold.france_establishments.etablissements_par_departement WHERE code_departement <> 'ZZ' ORDER BY nb_etablissements DESC LIMIT 5

Example question: Compare la Savoie et l'Isere : nombre d'etablissements et part ESS.
Example answer: SELECT libelle_departement, nb_etablissements, part_ess FROM gold.france_establishments.etablissements_par_departement WHERE libelle_departement IN ('Savoie', 'Isère')"""

FORBIDDEN = re.compile(
    r"\b(insert|update|delete|merge|create|drop|alter|truncate|grant|revoke|call|"
    r"commit|rollback|prepare|execute|set\s+session)\b",
    re.I,
)


def guard(sql):
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
        if len(parts) != 3 or parts[0].lower() != CATALOG or parts[1].lower() != SCHEMA:
            return False, f"table outside {CATALOG}.{SCHEMA}: {ref}"
    return True, ""


def domain_check(sql, codes, labels):
    """Confront every literal with the real values, the way an insight is confronted with
    its fact. `code_departement IN ('075')` and `code_departement = 'Paris'` are both valid
    SQL that answer wrong in silence; nothing else catches them."""
    for column, known in (("code_departement", codes), ("libelle_departement", labels)):
        blobs = re.findall(
            rf"{column}\s*(?:=|in)\s*\(?([^)]*?)(?:\)|\s+(?:and|or|group|order|limit)\b|$)",
            sql,
            re.I,
        )
        for blob in blobs:
            for literal in re.findall(r"'([^']*)'", blob):
                if literal != "ZZ" and literal not in known:
                    return False, f"{column} = '{literal}' is not a known value"
    return True, ""


def ranking_check(sql):
    """'ZZ' (Non determine, 52 establishments, 0 in QPV) tops every bottom ranking and is
    never an answer. The prompt asks for the exclusion; this makes it hold."""
    selected = re.search(r"\bselect\b(.*?)\bfrom\b", sql, re.I | re.S)
    if not selected or not re.search(r"\border\s+by\b", sql, re.I):
        return True, ""
    if not re.search(r"\b(libelle_departement|code_departement)\b", selected.group(1), re.I):
        return True, ""
    # Naming departments pins the answer; the unknown one cannot turn up.
    if re.search(r"\b(libelle_departement|code_departement)\s*(=|in)\b", sql, re.I):
        return True, ""
    if "'ZZ'" not in sql.upper():
        return False, "a ranking of departments must exclude code_departement <> 'ZZ'"
    return True, ""


def pretty(sql):
    """One clause per line: a query that scrolls sideways cannot be read on screen."""
    out = re.sub(r"\s+", " ", sql.strip())
    for kw in ("FROM", "WHERE", "GROUP BY", "ORDER BY", "HAVING", "LIMIT"):
        out = re.sub(rf"\s+{kw}\s+", f"\n{kw} ", out, flags=re.I)
    return re.sub(r"\s+(AND|OR)\s+", r"\n  \1 ", out, flags=re.I)


def table(columns, rows):
    def cell(v):
        return "" if v is None else str(v).replace("|", "\\|")

    head = "| " + " | ".join(columns) + " |"
    rule = "| " + " | ".join("---" for _ in columns) + " |"
    body = ["| " + " | ".join(cell(v) for v in row) + " |" for row in rows]
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
        self._domain = (None, None, 0.0)

    def pipes(self):
        return [{"id": "gold-copilot", "name": "Gold Copilot"}]

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

    def _known_values(self):
        codes, labels, expiry = self._domain
        if codes and time.time() < expiry:
            return codes, labels
        _, rows = self._sql(
            f"SELECT code_departement, libelle_departement "
            f"FROM {CATALOG}.{SCHEMA}.etablissements_par_departement",
            max_rows=1000,
        )
        codes = {r[0] for r in rows}
        labels = {r[1] for r in rows}
        self._domain = (codes, labels, time.time() + 3600)
        return codes, labels

    def _write_sql(self, question, rejection=None):
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
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
            },
        )
        r.raise_for_status()
        import json

        return json.loads(r.json()["message"]["content"])["sql"].strip().rstrip(";")

    def _attempt(self, question, codes, labels, rejection=None):
        sql = self._write_sql(question, rejection)
        checks = (lambda: guard(sql), lambda: ranking_check(sql),
                  lambda: domain_check(sql, codes, labels))
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
        question = ""
        for message in reversed(body.get("messages", [])):
            if message.get("role") == "user":
                question = message.get("content") or ""
                break
        if not question.strip():
            return "Pose une question sur les établissements français."
        if not self.valves.CLIENT_SECRET:
            return "CLIENT_SECRET is not set in the Gold Copilot valves."

        started = time.time()
        try:
            codes, labels = self._known_values()
        except Exception as e:
            return f"Le catalogue gold est injoignable : {e}"

        sql, columns, rows, rejection = self._attempt(question, codes, labels)
        retried = rejection
        if rejection:
            sql, columns, rows, rejection = self._attempt(question, codes, labels, rejection)

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
            out.append(table(columns, rows))
            out.append(
                f"*{len(rows)} ligne(s) · {self.valves.MODEL} · {time.time() - started:.1f} s*"
            )
        return "\n\n".join(out)
