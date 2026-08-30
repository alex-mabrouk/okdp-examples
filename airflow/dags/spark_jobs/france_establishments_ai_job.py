"""
France establishments - AI insights PySpark job
Spark computes the figures, the local model only puts them into French, and every
sentence is checked back against its figure before it is published.

The line that makes this defensible in front of a data team: no business value is
ever produced by the model. `value` and `comparison_value` come from the gold
tables; `insight` is the only generated column.
"""
import argparse
import http.client
import json
import os
import re
import time
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

UNKNOWN_DEPARTMENT = "ZZ"
MAX_CHARS = 200

FACTS_SCHEMA = StructType(
    [
        StructField("fact_id", StringType(), False),
        StructField("category", StringType(), False),
        StructField("scope", StringType(), False),
        StructField("subject", StringType(), True),
        StructField("metric", StringType(), False),
        StructField("value", DoubleType(), False),
        StructField("unit", StringType(), False),
        StructField("comparison_value", DoubleType(), True),
        StructField("comparison_unit", StringType(), True),
        StructField("gap_value", DoubleType(), True),
        StructField("rank", IntegerType(), True),
        StructField("claim", StringType(), False),
    ]
)

INSIGHTS_SCHEMA = StructType(
    FACTS_SCHEMA.fields
    + [
        StructField("insight", StringType(), True),
        StructField("status", StringType(), False),
        StructField("model", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("generated_at", TimestampType(), False),
    ]
)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--facts-table", default="insights_facts")
    parser.add_argument("--insights-table", default="insights")
    parser.add_argument("--ollama-url", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--run-id", required=True)
    return parser.parse_args()


def build_spark(catalog, run_id):
    client_id = os.getenv("POLARIS_CLIENT_ID", "")
    client_secret = os.getenv("POLARIS_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        raise RuntimeError("POLARIS_CLIENT_ID / POLARIS_CLIENT_SECRET are not set")
    return (
        SparkSession.builder.appName(f"FranceEstablishments-AI-{run_id}")
        .config(f"spark.sql.catalog.{catalog}.credential", f"{client_id}:{client_secret}")
        .getOrCreate()
    )


# ---------------------------------------------------------------- the figures

def fact(fact_id, category, scope, metric, value, unit, claim, **kw):
    """One figure, plus the sentence Spark asserts about it.

    The gap against the comparison is computed here too: leaving the model to
    subtract two numbers is exactly how it invents a third one.
    """
    comparison = kw.get("comparison")
    gap = None if comparison is None else round(abs(float(value) - float(comparison)), 2)
    return {
        "fact_id": fact_id,
        "category": category,
        "scope": scope,
        "subject": kw.get("subject"),
        "metric": metric,
        "value": float(value),
        "unit": unit,
        "comparison_value": None if comparison is None else float(comparison),
        "comparison_unit": kw.get("comparison_unit"),
        "gap_value": gap,
        "rank": kw.get("rank"),
        "claim": claim,
    }


def collect_facts(spark, gold):
    """Every figure the model may mention, and the sentence Spark asserts about it."""
    dept = f"{gold}.etablissements_par_departement"
    naf = f"{gold}.etablissements_par_section_naf"
    commune = f"{gold}.etablissements_par_commune"
    categorie = f"{gold}.etablissements_par_categorie"
    creations = f"{gold}.creations_par_mois"
    known = f"code_departement <> '{UNKNOWN_DEPARTMENT}'"

    def cast(row):
        # Aggregates come back as Decimal while the gold columns are double, and
        # the two do not mix in plain arithmetic.
        return {
            k: float(v) if isinstance(v, Decimal) else v
            for k, v in row.asDict().items()
        }

    def one(sql):
        return cast(spark.sql(sql).first())

    def rows(sql):
        return [cast(r) for r in spark.sql(sql).collect()]

    n = format_number
    nat = one(
        f"""SELECT sum(nb_etablissements) AS total,
                   round(100.0 * sum(nb_qpv) / sum(nb_etablissements), 2) AS part_qpv,
                   round(100.0 * sum(nb_ess) / sum(nb_etablissements), 2) AS part_ess
            FROM {dept} WHERE {known}"""
    )
    facts = [
        fact("national_total", "volumétrie", "France",
             "nombre d'établissements actifs", nat["total"], "établissements",
             f"En France, on compte {n(nat['total'])} établissements actifs."),
        fact("national_part_qpv", "qpv", "France",
             "part des établissements situés en quartier prioritaire", nat["part_qpv"], "%",
             f"En France, {n(nat['part_qpv'])} % des établissements actifs sont situés "
             f"dans un quartier prioritaire."),
        fact("national_part_ess", "ess", "France",
             "part des établissements de l'économie sociale et solidaire", nat["part_ess"], "%",
             f"En France, {n(nat['part_ess'])} % des établissements actifs relèvent de "
             f"l'économie sociale et solidaire."),
    ]

    for i, r in enumerate(rows(
        f"""SELECT libelle_departement, nb_etablissements FROM {dept}
            WHERE {known} ORDER BY nb_etablissements DESC LIMIT 3"""), start=1):
        d = r["libelle_departement"]
        facts.append(fact(f"top_dept_etablissements_{i}", "volumétrie", d,
                          "nombre d'établissements actifs", r["nb_etablissements"],
                          "établissements",
                          f"{d} compte {n(r['nb_etablissements'])} établissements actifs, "
                          f"ce qui en fait le {i}e département de France.",
                          rank=i))

    for i, r in enumerate(rows(
        f"""SELECT libelle_departement, part_qpv FROM {dept}
            WHERE {known} ORDER BY part_qpv DESC LIMIT 3"""), start=1):
        d, v = r["libelle_departement"], r["part_qpv"]
        gap = round(abs(v - nat["part_qpv"]), 2)
        facts.append(fact(f"top_dept_part_qpv_{i}", "qpv", d,
                          "part des établissements situés en quartier prioritaire", v, "%",
                          f"En {d}, {n(v)} % des établissements actifs sont situés dans un "
                          f"quartier prioritaire, contre {n(nat['part_qpv'])} % en France, "
                          f"soit {n(gap)} points de plus.",
                          rank=i, comparison=nat["part_qpv"], comparison_unit="%"))

    r = one(f"SELECT libelle_departement, part_ess FROM {dept} WHERE {known} ORDER BY part_ess DESC LIMIT 1")
    d, v = r["libelle_departement"], r["part_ess"]
    facts.append(fact("top_dept_part_ess", "ess", d,
                      "part des établissements de l'économie sociale et solidaire", v, "%",
                      f"En {d}, {n(v)} % des établissements actifs relèvent de l'économie "
                      f"sociale et solidaire, contre {n(nat['part_ess'])} % en France, soit "
                      f"{n(round(abs(v - nat['part_ess']), 2))} points de plus.",
                      comparison=nat["part_ess"], comparison_unit="%"))

    r = one(
        f"""SELECT libelle_section_naf, sum(nb_etablissements) AS n FROM {naf}
            WHERE {known} GROUP BY libelle_section_naf ORDER BY n DESC LIMIT 1"""
    )
    facts.append(fact("national_top_section", "secteurs", "France",
                      "secteur d'activité le plus représenté", r["n"], "établissements",
                      f"Le secteur « {r['libelle_section_naf']} » est le plus représenté en "
                      f"France, avec {n(r['n'])} établissements actifs.",
                      subject=r["libelle_section_naf"]))

    r = one(
        f"""SELECT libelle_section_naf, sum(nb_qpv) AS n FROM {naf}
            WHERE {known} GROUP BY libelle_section_naf ORDER BY n DESC LIMIT 1"""
    )
    facts.append(fact("national_top_section_qpv", "secteurs", "France",
                      "secteur d'activité le plus représenté en quartier prioritaire",
                      r["n"], "établissements",
                      f"Dans les quartiers prioritaires, le secteur « {r['libelle_section_naf']} » "
                      f"est le plus représenté, avec {n(r['n'])} établissements actifs.",
                      subject=r["libelle_section_naf"]))

    r = one(
        f"""SELECT libelle_commune, nb_etablissements FROM {commune}
            WHERE {known} ORDER BY nb_etablissements DESC LIMIT 1"""
    )
    facts.append(fact("national_top_commune", "volumétrie", "France",
                      "commune comptant le plus d'établissements actifs",
                      r["nb_etablissements"], "établissements",
                      f"{r['libelle_commune'].title()} est la commune française qui compte le plus "
                      f"d'établissements actifs, avec {n(r['nb_etablissements'])}.",
                      subject=r["libelle_commune"]))

    r = one(
        f"""SELECT categorie, sum(nb_etablissements) AS n FROM {categorie}
            WHERE {known} GROUP BY categorie ORDER BY n DESC LIMIT 1"""
    )
    share = round(100.0 * r["n"] / nat["total"], 2)
    facts.append(fact("national_top_categorie", "volumétrie", "France",
                      "catégorie d'établissement la plus fréquente", share, "%",
                      f"La catégorie « {r['categorie']} » regroupe {n(share)} % des "
                      f"établissements actifs français.",
                      subject=r["categorie"]))

    r = one(
        f"""WITH bounds AS (SELECT max(mois_creation) AS last_month FROM {creations})
            SELECT sum(CASE WHEN mois_creation > add_months(last_month, -12)
                            THEN nb_creations ELSE 0 END) AS last12,
                   sum(CASE WHEN mois_creation > add_months(last_month, -24)
                             AND mois_creation <= add_months(last_month, -12)
                            THEN nb_creations ELSE 0 END) AS prev12
            FROM {creations}, bounds WHERE {known}"""
    )
    last12, prev12 = r["last12"], r["prev12"]
    trend = "de plus" if last12 >= prev12 else "de moins"
    facts.append(fact("creations_12_mois", "créations", "France",
                      "créations d'établissements sur les douze derniers mois",
                      last12, "créations",
                      f"Sur les douze derniers mois, {n(last12)} établissements ont été créés "
                      f"en France, contre {n(prev12)} sur les douze mois précédents, soit "
                      f"{n(abs(last12 - prev12))} {trend}.",
                      comparison=prev12, comparison_unit="créations"))

    top5 = one(
        f"""SELECT sum(nb_etablissements) AS n FROM
              (SELECT nb_etablissements FROM {dept} WHERE {known}
               ORDER BY nb_etablissements DESC LIMIT 5)"""
    )
    share = round(100.0 * top5["n"] / nat["total"], 2)
    facts.append(fact("top5_dept_share", "volumétrie", "France",
                      "part des cinq premiers départements dans le total national", share, "%",
                      f"Les cinq premiers départements concentrent {n(share)} % des "
                      f"établissements actifs français."))
    return facts


# ------------------------------------------------------------- the wording

# Kept verbatim across calls: a stable prefix is what drops the prompt cost from
# 13 s to 0.2 s on CPU.
# Kept verbatim across calls: a stable prefix is what drops the prompt cost from
# 13 s to 0.2 s on CPU.
RULES = """Reformule l'affirmation suivante en une seule phrase française naturelle.

Règles absolues :
- ne change aucun nombre et n'en ajoute aucun ;
- ne calcule rien : aucun écart, rapport, multiple ni pourcentage qui ne soit pas déjà
  écrit dans l'affirmation ;
- n'écris aucun nombre en toutes lettres ;
- ne change pas le sens ;
- 200 caractères au maximum.

Réponds en JSON : {"insight": "..."}
"""

FORMAT = {"type": "object", "properties": {"insight": {"type": "string"}}, "required": ["insight"]}

# Thousands may be split by a plain, a non-breaking or a narrow no-break space.
SPACES = " \u00a0\u202f"
NUMBER = re.compile(rf"\d[\d{SPACES}]*(?:,\d+)?")
NUMBER_WORDS = re.compile(
    r"\b(deux|trois|quatre|cinq|six|sept|huit|neuf|dix|onze|douze|treize|quatorze|quinze"
    r"|seize|vingt|trente|quarante|cinquante|soixante|cent|cents|mille|million|millions"
    r"|milliard|milliards|demi|moitié|double|triple|quadruple|fois)\b",
    re.IGNORECASE,
)


def render_fact(f):
    return f"Affirmation : {f['claim']}"


def format_number(value):
    """French rendering, which is also the only spelling the check accepts."""
    if float(value).is_integer():
        return f"{int(value):,}".replace(",", "\u00a0")
    return f"{value:.2f}".replace(".", ",")


def generate(url, model, prompt, timeout=300):
    body = json.dumps(
        {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.2},
            "format": FORMAT,
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        f"{url.rstrip('/')}/api/generate", data=body,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.load(response)
    return json.loads(payload["response"])["insight"].strip()


def numbers_in(text):
    out = []
    for raw in NUMBER.findall(text):
        cleaned = raw.translate({ord(c): None for c in SPACES}).replace(",", ".")
        try:
            out.append(float(cleaned))
        except ValueError:
            continue
    return out


def check(text, f):
    """Reject anything the fact does not support. Returns None, or why it failed."""
    if not text:
        return "empty"
    if len(text) > MAX_CHARS:
        return f"too long ({len(text)} chars)"
    claimed = {w.lower() for w in NUMBER_WORDS.findall(f["claim"])}
    invented = [w for w in NUMBER_WORDS.findall(text) if w.lower() not in claimed]
    if invented:
        return f"number or ratio written in words: {invented[0]}"
    allowed = [f["value"]] + [
        f[k] for k in ("comparison_value", "gap_value") if f[k] is not None
    ]
    found = numbers_in(text)
    for value in found:
        if not any(abs(value - a) < 0.01 for a in allowed):
            return f"unsupported number {value}"
    if not any(abs(f["value"] - v) < 0.01 for v in found):
        return "the measured value is not quoted"
    return None


def write_insight(url, model, f, attempts=3):
    prompt = f"{RULES}\n{render_fact(f)}"
    text = None
    for attempt in range(attempts):
        try:
            text = generate(url, model, prompt)
        except (OSError, http.client.HTTPException, KeyError, ValueError) as exc:
            # A generation saturates the CPU, the readiness probe times out and the
            # pod leaves the Service endpoints mid-call. Wait for it to come back
            # rather than losing the whole run over one dropped connection.
            print(f"    attempt {attempt + 1} call failed: {exc}")
            time.sleep(15)
            continue
        reason = check(text, f)
        if reason is None:
            return text, "verified"
        print(f"    attempt {attempt + 1} rejected: {reason} -> {text}")
    return text, "rejected"


def main():
    args = parse_args()
    gold = f"{args.catalog}.{args.namespace}"
    print("=" * 70)
    print(f"AI insights - source {gold}, model {args.model} at {args.ollama_url}")
    print("=" * 70)

    spark = build_spark(args.catalog, args.run_id)
    spark.sparkContext.setLogLevel("WARN")

    facts = collect_facts(spark, gold)
    print(f"\nFacts computed: {len(facts)}")
    spark.createDataFrame(facts, schema=FACTS_SCHEMA).writeTo(
        f"{gold}.{args.facts_table}"
    ).using("iceberg").tableProperty("format-version", "2").createOrReplace()

    generated_at = datetime.now(timezone.utc)
    rows = []
    for f in facts:
        text, status = write_insight(args.ollama_url, args.model, f)
        print(f"  [{status}] {f['fact_id']}: {text}")
        rows.append(dict(f, insight=text, status=status, model=args.model,
                         run_id=args.run_id, generated_at=generated_at))

    spark.createDataFrame(rows, schema=INSIGHTS_SCHEMA).writeTo(
        f"{gold}.{args.insights_table}"
    ).using("iceberg").tableProperty("format-version", "2").createOrReplace()

    verified = sum(1 for r in rows if r["status"] == "verified")
    print("\n" + "=" * 70)
    print(f"AI insights completed: {verified}/{len(rows)} verified")
    print("=" * 70)
    spark.stop()


if __name__ == "__main__":
    main()
