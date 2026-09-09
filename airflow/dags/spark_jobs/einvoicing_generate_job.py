"""
E-invoicing - invoice generation PySpark job
Writes a flow of synthetic Factur-X invoices, one XML file each, from the cast of
real companies the casting job drew out of the SIRENE referential.

What is real and what is not, stated once here because the demo turns on it: the
companies, their SIRET, their NAF, their addresses are real and public. The
invoices are not. No such invoice was ever issued, and every amount, date and line
below was made up by this file.

Anomalies are injected on purpose and written down as they are injected, into a
ground-truth dataset the pipeline never reads. Gold compares what was injected to
what was caught, which is the only honest way to claim a control works.

Two of them are worth reading twice:
  MET-TAUX-TVA           the odd rate comes with a correctly computed VAT amount,
                         so the official Schematron passes it and only the
                         business rule catches it
  FMT-SCHEMATRON         drops the seller's VAT number, which BR-S-02 requires:
                         the reverse case, caught by the standard and by nothing
                         of ours
"""
import argparse
import os
import random
from datetime import date, timedelta

import einvoicing_cii as cii
import einvoicing_rules as rules
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# What gets invoiced, per NAF section, with the order of magnitude of a unit price.
# Enough to make a line read as plausible; not a product catalogue.
CATALOGUE = {
    "A": [("Livraison de céréales (tonne)", 240.0), ("Prestation de récolte", 680.0)],
    "C": [("Pièces mécaniques usinées", 85.0), ("Sous-traitance d'assemblage", 1450.0)],
    "F": [("Travaux de gros œuvre", 3200.0), ("Fourniture et pose de menuiseries", 1850.0)],
    "G": [("Fournitures de bureau", 42.0), ("Matériel informatique", 780.0)],
    "H": [("Transport routier de marchandises", 520.0), ("Prestation logistique", 1250.0)],
    "I": [("Prestation de restauration collective", 18.0), ("Nuitées d'hébergement", 95.0)],
    "J": [("Prestation de développement logiciel", 620.0), ("Licence annuelle", 1200.0)],
    "K": [("Commission de courtage", 950.0), ("Prestation d'expertise financière", 1400.0)],
    "L": [("Loyer de locaux professionnels", 2400.0), ("Charges locatives", 380.0)],
    "M": [("Prestation de conseil", 850.0), ("Mission d'audit", 1750.0)],
    "N": [("Prestation de nettoyage", 320.0), ("Mise à disposition de personnel", 480.0)],
    "P": [("Session de formation", 1100.0), ("Ingénierie pédagogique", 720.0)],
    "Q": [("Prestation de soins", 65.0), ("Fourniture de dispositifs médicaux", 340.0)],
    "S": [("Prestation de maintenance", 450.0), ("Abonnement de service", 210.0)],
}
CATALOGUE_DEFAUT = [("Prestation de service", 500.0), ("Fourniture diverse", 180.0)]

# The rate mix of a domestic B2B flow: mostly the standard rate.
TAUX_TVA_PONDERES = ((20.0, 0.82), (10.0, 0.11), (5.5, 0.05), (2.1, 0.02))

DELAIS_PAIEMENT = (30, 45, 60)

# Invoicing slows in August. Nothing else in the year is worth modelling.
CREUX_AOUT = 0.45

TRUTH_SCHEMA = StructType(
    [
        StructField("numero_facture", StringType(), False),
        StructField("fichier", StringType(), False),
        StructField("mois", StringType(), False),
        StructField("siren_emetteur", StringType(), True),
        StructField("profil", StringType(), False),
        StructField("regle_id", StringType(), True),
        StructField("montant_ttc", DoubleType(), True),
    ]
)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--casting", required=True)
    parser.add_argument("--output", required=True, help="s3a:// prefix for the XML files")
    parser.add_argument("--truth", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--key-prefix", required=True, help="key prefix inside the bucket")
    parser.add_argument("--count", type=int, default=20000)
    parser.add_argument("--months", type=int, default=24)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--anomaly-rate", type=float, default=0.06)
    parser.add_argument("--extended-rate", type=float, default=0.05)
    parser.add_argument("--partitions", type=int, default=0)
    return parser.parse_args()


# --- identifiers -----------------------------------------------------------
def luhn_check_digit(digits):
    total = 0
    for position, char in enumerate(reversed(digits)):
        value = int(char) * (2 if position % 2 == 0 else 1)
        total += value - 9 if value > 9 else value
    return str((10 - total % 10) % 10)


def fabriquer_siren(rng):
    """A SIREN that passes its own check digit and exists nowhere.

    The point of REF-SIREN-INCONNU is that syntax cannot catch it: only the
    referential can say this company does not exist.
    """
    body = "".join(str(rng.randint(0, 9)) for _ in range(8))
    return body + luhn_check_digit(body)


def cle_tva(siren):
    return f"FR{(12 + 3 * (int(siren) % 97)) % 97:02d}{siren}"


def iban_fr(rng):
    bban = "".join(str(rng.randint(0, 9)) for _ in range(23))
    rearranged = bban + "1527" + "00"
    check = 98 - int(rearranged) % 97
    return f"FR{check:02d}{bban}"


# --- the invoice -----------------------------------------------------------
def tirer_taux(rng):
    draw = rng.random()
    cumulative = 0.0
    for taux, weight in TAUX_TVA_PONDERES:
        cumulative += weight
        if draw <= cumulative:
            return taux
    return 20.0


def tirer_date(rng, months):
    """Uniform over the window, minus a dip in August."""
    today = date.today().replace(day=1)
    while True:
        back = rng.randint(1, months)
        month = today.month - back
        year = today.year + (month - 1) // 12
        month = (month - 1) % 12 + 1
        if month == 8 and rng.random() > CREUX_AOUT:
            continue
        return date(year, month, rng.randint(1, 28))


def batir_lignes(rng, section_naf):
    catalogue = CATALOGUE.get(section_naf, CATALOGUE_DEFAUT)
    taux = tirer_taux(rng)
    lignes = []
    for index in range(rng.randint(1, 8)):
        designation, prix_median = catalogue[rng.randrange(len(catalogue))]
        prix = round(prix_median * rng.lognormvariate(0.0, 0.45), 2)
        quantite = rng.randint(1, 20)
        lignes.append(
            {
                "line_id": str(index + 1),
                "designation": designation,
                "quantite": quantite,
                "prix_unitaire": prix,
                "montant_ht": round(prix * quantite, 2),
                "taux_tva": taux,
            }
        )
    return lignes


def totaliser(invoice):
    """Recompute the summation and the VAT breakdown from the lines.

    Called before any anomaly is injected, so that an invoice meant to carry
    inconsistent totals is one that was consistent a moment earlier.
    """
    ventilation = {}
    for ligne in invoice["lignes"]:
        entry = ventilation.setdefault(
            ligne["taux_tva"], {"taux_tva": ligne["taux_tva"], "montant_ht": 0.0}
        )
        entry["montant_ht"] = round(entry["montant_ht"] + ligne["montant_ht"], 2)
    for entry in ventilation.values():
        entry["montant_tva"] = round(entry["montant_ht"] * entry["taux_tva"] / 100.0, 2)

    invoice["ventilation_tva"] = sorted(
        ventilation.values(), key=lambda e: e["taux_tva"], reverse=True
    )
    invoice["total_lignes"] = round(sum(l["montant_ht"] for l in invoice["lignes"]), 2)
    invoice["montant_ht"] = invoice["total_lignes"]
    invoice["montant_tva"] = round(
        sum(e["montant_tva"] for e in invoice["ventilation_tva"]), 2
    )
    invoice["montant_ttc"] = round(invoice["montant_ht"] + invoice["montant_tva"], 2)
    invoice["montant_du"] = invoice["montant_ttc"]
    return invoice


def batir_facture(rng, numero, fournisseur, acheteur, months, extended_rate):
    emission = tirer_date(rng, months)
    echeance = emission + timedelta(days=DELAIS_PAIEMENT[rng.randrange(len(DELAIS_PAIEMENT))])
    profil = (
        "EXTENDED-CTC-FR" if rng.random() < extended_rate else "EN16931"
    )
    invoice = {
        "numero": numero,
        "date_emission": emission.strftime("%Y%m%d"),
        "date_livraison": emission.strftime("%Y%m%d"),
        "date_echeance": echeance.strftime("%Y%m%d"),
        "profil": profil,
        "reference_acheteur": f"SERVICE-{rng.randint(1, 40):03d}",
        "bon_de_commande": f"BC-{emission.year}-{rng.randint(1, 9999):04d}",
        "iban": iban_fr(rng),
        "fournisseur": fournisseur,
        "acheteur": acheteur,
        "lignes": batir_lignes(rng, fournisseur.get("code_section_naf")),
    }
    invoice["_mois"] = emission.strftime("%Y-%m")
    return totaliser(invoice)


# --- anomalies -------------------------------------------------------------
def injecter(rng, invoice, regle_id, cesses):
    """Apply one anomaly. Returns a second invoice when the anomaly is a duplicate."""
    if regle_id == "MET-TOTAUX":
        # Off by a plausible amount, not by a digit: an eye should not catch it.
        invoice["montant_ttc"] = round(invoice["montant_ttc"] + rng.uniform(15, 400), 2)
        invoice["montant_du"] = invoice["montant_ttc"]

    elif regle_id == "MET-TAUX-TVA":
        taux = rng.choice([7.5, 19.6, 33.3])
        for ligne in invoice["lignes"]:
            ligne["taux_tva"] = taux
        totaliser(invoice)

    elif regle_id == "MET-ECHEANCE":
        emission = date.fromisoformat(
            f"{invoice['date_emission'][:4]}-{invoice['date_emission'][4:6]}-"
            f"{invoice['date_emission'][6:]}"
        )
        invoice["date_echeance"] = (emission - timedelta(days=rng.randint(5, 60))).strftime(
            "%Y%m%d"
        )

    elif regle_id == "FMT-SCHEMATRON":
        # The seller's VAT number, which BR-CO-26 and BR-52 require. Dropping the
        # due date instead would have been the obvious choice, and was measured not
        # to fail: BR-CO-25 is not a fatal assertion in this Schematron.
        fournisseur = dict(invoice["fournisseur"])
        fournisseur["tva_intracom"] = None
        invoice["fournisseur"] = fournisseur

    elif regle_id == "MET-MENTION":
        invoice["reference_acheteur"] = None

    elif regle_id == "MET-TVA-CLE":
        siren = invoice["fournisseur"].get("siren") or "000000000"
        invoice["fournisseur"] = dict(invoice["fournisseur"])
        invoice["fournisseur"]["tva_intracom"] = f"FR{rng.randint(0, 99):02d}{siren}"

    elif regle_id == "REF-SIREN-INCONNU":
        siren = fabriquer_siren(rng)
        fournisseur = dict(invoice["fournisseur"])
        fournisseur.update(
            {
                "siren": siren,
                "siret": siren + f"{rng.randint(1, 99999):05d}",
                "tva_intracom": cle_tva(siren),
            }
        )
        invoice["fournisseur"] = fournisseur

    elif regle_id == "REF-EMETTEUR-CESSE" and cesses:
        invoice["fournisseur"] = cesses[rng.randrange(len(cesses))]

    elif regle_id == "STA-MONTANT-ABERRANT":
        for ligne in invoice["lignes"]:
            ligne["quantite"] *= 100
            ligne["montant_ht"] = round(ligne["prix_unitaire"] * ligne["quantite"], 2)
        totaliser(invoice)

    elif regle_id == "MET-DOUBLON":
        # The same invoice sent twice, which is what a duplicate actually is: same
        # number, same issuer, same amount, a second file.
        return dict(invoice)

    return None


# --- generation ------------------------------------------------------------
def s3_client():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        config=Config(retries={"max_attempts": 5, "mode": "standard"}),
    )


def generer_partition(index, rows, params, cast):
    """One partition's worth of invoices, written straight to S3.

    The seed is derived from the partition index, so a partition always produces
    the same invoices however the job is scheduled.
    """
    rng = random.Random(params["seed"] * 1_000_003 + index)
    client = s3_client()
    fournisseurs, acheteurs, cesses = cast

    for row in rows:
        numero = f"FA-{row.id:08d}"
        fournisseur = fournisseurs[rng.randrange(len(fournisseurs))]
        acheteur = acheteurs[rng.randrange(len(acheteurs))]
        invoice = batir_facture(
            rng, numero, fournisseur, acheteur, params["months"], params["extended_rate"]
        )

        regle_id = None
        jumelle = None
        if rng.random() < params["anomaly_rate"]:
            regle_id = rules.INJECTABLE[rng.randrange(len(rules.INJECTABLE))]
            jumelle = injecter(rng, invoice, regle_id, cesses)

        for suffix, document in (("", invoice), ("__doublon", jumelle)):
            if document is None:
                continue
            key = (
                f"{params['key_prefix']}/mois={document['_mois']}/"
                f"{document['numero']}{suffix}.xml"
            )
            client.put_object(
                Bucket=params["bucket"],
                Key=key,
                Body=cii.build(document).encode("utf-8"),
                ContentType="application/xml",
            )
            yield (
                document["numero"],
                key,
                document["_mois"],
                document["fournisseur"].get("siren"),
                document["profil"],
                regle_id,
                float(document["montant_ttc"]),
            )


def charger_casting(spark, path):
    rows = [row.asDict() for row in spark.read.parquet(path).collect()]
    fournisseurs = [r for r in rows if r["role"] == "fournisseur"]
    acheteurs = [r for r in rows if r["role"] == "acheteur"]
    cesses = [r for r in rows if r["role"] == "fournisseur_cesse"]
    if not fournisseurs or not acheteurs:
        raise RuntimeError(
            f"casting incomplet: {len(fournisseurs)} fournisseurs, {len(acheteurs)} acheteurs"
        )
    return fournisseurs, acheteurs, cesses


def main():
    args = parse_args()
    partitions = args.partitions or max(1, min(64, args.count // 500))

    print("=" * 70)
    print("E-invoicing - generation")
    print("=" * 70)
    print(f"Invoices: {args.count:,}   partitions: {partitions}   seed: {args.seed}")
    print(f"Output:   s3://{args.bucket}/{args.key_prefix}/")
    print(f"Truth:    {args.truth}")
    print("=" * 70)

    spark = SparkSession.builder.appName("EInvoicing-Generate").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    cast = charger_casting(spark, args.casting)
    print(
        f"Cast: {len(cast[0])} fournisseurs, {len(cast[1])} acheteurs, "
        f"{len(cast[2])} fournisseurs cessés"
    )

    params = {
        "seed": args.seed,
        "months": args.months,
        "anomaly_rate": args.anomaly_rate,
        "extended_rate": args.extended_rate,
        "bucket": args.bucket,
        "key_prefix": args.key_prefix,
    }
    broadcast_cast = spark.sparkContext.broadcast(cast)
    broadcast_params = spark.sparkContext.broadcast(params)

    def run(index, rows):
        return generer_partition(
            index, rows, broadcast_params.value, broadcast_cast.value
        )

    truth_rdd = spark.range(args.count, numPartitions=partitions).rdd.mapPartitionsWithIndex(
        run
    )
    truth = spark.createDataFrame(truth_rdd, TRUTH_SCHEMA)
    truth.write.mode("overwrite").parquet(args.truth)

    published = spark.read.parquet(args.truth)
    total = published.count()
    print(f"\nInvoices written: {total:,}")

    print("\nInjected anomalies:")
    (
        published.filter(published.regle_id.isNotNull())
        .groupBy("regle_id")
        .count()
        .orderBy("regle_id")
        .show(30, truncate=False)
    )
    print("\nBy profile:")
    published.groupBy("profil").count().show(truncate=False)
    print("\nBy month (first ten):")
    published.groupBy("mois").count().orderBy("mois").show(10, truncate=False)

    print("\n" + "=" * 70)
    print("Generation completed!")
    print("=" * 70)
    spark.stop()


if __name__ == "__main__":
    main()
