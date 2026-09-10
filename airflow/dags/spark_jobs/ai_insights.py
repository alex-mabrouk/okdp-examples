"""
Verified insights - the machinery shared by the AI jobs
Spark computes the figures and asserts the sentence; the local model only puts
that sentence into readable French; every sentence is checked back against its
figure before it is published.

A job that uses this writes one function: `collect_facts`. Nothing here knows
anything about establishments or invoices.

The line that makes this defensible in front of a data team: no business value is
ever produced by the model. `value`, `comparison_value` and `gap_value` come from
the gold tables; `insight` is the only generated column.
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

# Comparison facts need the room: the model keeps landing at ~215 characters and
# three retries do not talk it down.
MAX_CHARS = 240

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


def build_spark(app, catalog, run_id):
    client_id = os.getenv("POLARIS_CLIENT_ID", "")
    client_secret = os.getenv("POLARIS_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        raise RuntimeError("POLARIS_CLIENT_ID / POLARIS_CLIENT_SECRET are not set")
    return (
        SparkSession.builder.appName(f"{app}-{run_id}")
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


def cast(row):
    """Aggregates come back as Decimal while the gold columns are double, and the
    two do not mix in plain arithmetic."""
    return {
        k: float(v) if isinstance(v, Decimal) else v for k, v in row.asDict().items()
    }


def queries(spark):
    """`one` and `rows` bound to a session, since every collect_facts wants both."""
    return (
        lambda sql: cast(spark.sql(sql).first()),
        lambda sql: [cast(r) for r in spark.sql(sql).collect()],
    )


def format_number(value):
    """French rendering, which is also the only spelling the check accepts."""
    if float(value).is_integer():
        return f"{int(value):,}".replace(",", " ")
    # Grouped on the integer part too: an amount reads as 44 500,63, never 44500,63.
    return f"{value:,.2f}".replace(",", " ").replace(".", ",")


# ------------------------------------------------------------- the wording

# Kept verbatim across calls: a stable prefix is what drops the prompt cost from
# 13 s to 0.2 s on CPU.
RULES = """Reformule l'affirmation suivante en une seule phrase française naturelle.

Règles absolues :
- ne change aucun nombre et n'en ajoute aucun ;
- ne calcule rien : aucun écart, rapport, multiple ni pourcentage qui ne soit pas déjà
  écrit dans l'affirmation ;
- n'écris aucun nombre en toutes lettres ;
- ne change pas le sens ;
- 240 caractères au maximum.

Réponds en JSON : {"insight": "..."}
"""

FORMAT = {"type": "object", "properties": {"insight": {"type": "string"}}, "required": ["insight"]}

# Thousands may be split by a plain, a non-breaking or a narrow no-break space.
SPACES = "   "
NUMBER = re.compile(rf"\d[\d{SPACES}]*(?:,\d+)?")
# A ratio the model reached for on its own is an invention, and no arithmetic can
# confirm it. These stay rejected unless the claim already used the word.
RATIO_WORDS = re.compile(r"\b(demi|moitié|double|triple|quadruple|fois)\b", re.IGNORECASE)

# Numerals are a different matter: spelled out, they are still a number, so they are
# decoded and confronted with the fact like any other. The model wrote "mille six
# cent six" for 1 006 -- that is 1 606, and nothing but this catches it.
# "un" and "une" are absent on purpose: they are articles far more often than
# numbers, and reading "un montant" as the number 1 rejects every faithful sentence.
NUMERALS = {
    "zéro": 0, "deux": 2, "trois": 3, "quatre": 4, "cinq": 5,
    "six": 6, "sept": 7, "huit": 8, "neuf": 9, "dix": 10, "onze": 11, "douze": 12,
    "treize": 13, "quatorze": 14, "quinze": 15, "seize": 16, "vingt": 20, "vingts": 20,
    "trente": 30, "quarante": 40, "cinquante": 50, "soixante": 60,
    "quatrevingt": 80,
}
MULTIPLIERS = {
    "cent": 100, "cents": 100, "mille": 1000, "milles": 1000,
    "million": 10 ** 6, "millions": 10 ** 6, "milliard": 10 ** 9, "milliards": 10 ** 9,
}
NUMERAL_RUN = re.compile(
    r"\b(?:" + "|".join(sorted(set(NUMERALS) | set(MULTIPLIERS), key=len, reverse=True))
    + r")(?:[-\s]+(?:et[-\s]+)?(?:"
    + "|".join(sorted(set(NUMERALS) | set(MULTIPLIERS), key=len, reverse=True))
    + r"))*\b",
    re.IGNORECASE,
)


def decode_numeral(run):
    """'trente-cinq' -> 35, 'mille six cent six' -> 1606. The usual accumulator."""
    total = current = 0
    # The vigesimal form is one number, not three: quatre-vingt-dix is 80 + 10, and
    # adding 4, 20 and 10 gives 34.
    run = re.sub(r"quatre[-\s]+vingts?", "quatrevingt", run.lower())
    for word in re.split(r"[-\s]+", run):
        if word == "et":
            continue
        if word in NUMERALS:
            current += NUMERALS[word]
        elif word in MULTIPLIERS:
            scale = MULTIPLIERS[word]
            if scale >= 1000:
                total += (current or 1) * scale
                current = 0
            else:
                current = (current or 1) * scale
    return total + current


def render_fact(f):
    return f"Affirmation : {f['claim']}"


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
    # "pour cent" is the unit spelled out, not a number: 7,75 pour cent says exactly
    # what 7,75 % says, and rejecting it rejects a faithful sentence.
    scanned = re.sub(r"\bpour\s+cents?\b", "%", text, flags=re.I)
    claimed = {w.lower() for w in RATIO_WORDS.findall(f["claim"])}
    invented = [w for w in RATIO_WORDS.findall(scanned) if w.lower() not in claimed]
    if invented:
        return f"ratio written in words: {invented[0]}"
    # Whatever the claim already spells out is supported by construction, since
    # Spark wrote it. Rule labels carry their own digits -- "BR-06", "EN 16931",
    # "art. 242 nonies A CGI" -- and quoting one is faithful, not invention.
    allowed = (
        [f["value"]]
        + [f[k] for k in ("comparison_value", "gap_value") if f[k] is not None]
        + numbers_in(f["claim"])
        # Numerals the claim itself spells out -- "les cinq premiers départements",
        # "les douze derniers mois". Spark wrote them, so they are supported.
        + [decode_numeral(run) for run in NUMERAL_RUN.findall(f["claim"])]
    )
    found = numbers_in(text) + [
        decode_numeral(run) for run in NUMERAL_RUN.findall(scanned)
    ]
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


# ------------------------------------------------------------- the run

def run(app, collect_facts):
    """Compute the facts, write them up, publish both tables."""
    args = parse_args()
    gold = f"{args.catalog}.{args.namespace}"
    print("=" * 70)
    print(f"{app} - source {gold}, model {args.model} at {args.ollama_url}")
    print("=" * 70)

    spark = build_spark(app, args.catalog, args.run_id)
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
    print(f"{app} completed: {verified}/{len(rows)} verified")
    print("=" * 70)
    spark.stop()
