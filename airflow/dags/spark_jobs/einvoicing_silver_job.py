"""
E-invoicing - silver PySpark job
Reads the invoices bronze kept, and turns them into three tables: the invoices
themselves, their lines, and every anomaly found on them.

The controls come in two passes, and the split is not organisational:

  in Python, per partition   the document is parsed, and validated against the
                             XSD and the official Schematrons. This asks "is this
                             a conformant invoice", and only the standard can
                             answer it
  in Spark, over the set     everything that needs more than one document or more
                             than the document: the SIRENE referential, the
                             duplicates, the sector distribution

The second pass is where the platform earns its place. A document can be perfectly
conformant and still name a company that never existed, repeat an invoice already
paid, or bill a hundred times the going rate for its trade -- and no Schematron in
the world will say a word about it, because none of them holds the referential.

Silver also enriches: the issuer's NAF section, department and size class come
from the referential, which is what lets gold report on the reform's 2026/2027
tiering without a single extra source.
"""
import argparse
import os
from decimal import Decimal

import einvoicing_parse as parseur
import einvoicing_rules as regles
import einvoicing_validate as validateur
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

MONTANT = DecimalType(20, 2)
TAUX = DecimalType(6, 2)
QUANTITE = DecimalType(20, 3)

# One cent of slack: the standard itself tolerates rounding at that scale, and a
# difference below it is arithmetic, not a defect.
TOLERANCE = Decimal("0.01")

ACTIF = "A"

# An invoice billed at more than this multiple of its sector's median is worth a
# look. Set from the flow itself: the largest legitimate invoice sits at 5.4 times
# its sector median and the 99th percentile at 4.1, so eight leaves a clear margin.
#
# This is the one control that is not exact. It reports, it never rejects, and a
# genuinely large invoice can sit above the threshold while a moderately inflated
# one slips under it. The other nine answer yes or no; this one answers "unusual".
FACTEUR_ABERRANT = 8

LIGNE = StructType(
    [
        StructField("numero_ligne", StringType()),
        StructField("designation", StringType()),
        StructField("quantite", QUANTITE),
        StructField("unite", StringType()),
        StructField("prix_unitaire", MONTANT),
        StructField("montant_ht", MONTANT),
        StructField("taux_tva", TAUX),
        StructField("categorie_tva", StringType()),
    ]
)

VENTILATION = StructType(
    [
        StructField("taux_tva", TAUX),
        StructField("montant_ht", MONTANT),
        StructField("montant_tva", MONTANT),
        StructField("categorie_tva", StringType()),
    ]
)

ANOMALIE_FORMAT = StructType(
    [
        StructField("regle_id", StringType()),
        StructField("code", StringType()),
        StructField("message", StringType()),
    ]
)

PARSED = StructType(
    [
        StructField("chemin_source", StringType()),
        StructField("empreinte", StringType()),
        StructField("mois", StringType()),
        StructField("numero_facture", StringType()),
        StructField("type_document", StringType()),
        StructField("profil", StringType()),
        StructField("guideline", StringType()),
        StructField("date_emission", StringType()),
        StructField("date_livraison", StringType()),
        StructField("date_echeance", StringType()),
        StructField("reference_acheteur", StringType()),
        StructField("bon_de_commande", StringType()),
        StructField("devise", StringType()),
        StructField("iban", StringType()),
        StructField("siret_emetteur", StringType()),
        StructField("siren_emetteur", StringType()),
        StructField("nom_emetteur", StringType()),
        StructField("tva_emetteur", StringType()),
        StructField("code_postal_emetteur", StringType()),
        StructField("commune_emetteur", StringType()),
        StructField("siret_acheteur", StringType()),
        StructField("siren_acheteur", StringType()),
        StructField("nom_acheteur", StringType()),
        StructField("code_postal_acheteur", StringType()),
        StructField("commune_acheteur", StringType()),
        StructField("total_lignes", MONTANT),
        StructField("montant_ht", MONTANT),
        StructField("montant_tva", MONTANT),
        StructField("montant_ttc", MONTANT),
        StructField("montant_du", MONTANT),
        StructField("nb_lignes", IntegerType()),
        StructField("lignes", ArrayType(LIGNE)),
        StructField("ventilation_tva", ArrayType(VENTILATION)),
        StructField("anomalies_format", ArrayType(ANOMALIE_FORMAT)),
    ]
)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze", required=True)
    parser.add_argument("--referentiel", required=True, help="catalog.namespace.table")
    parser.add_argument("--sirene-bronze", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--run-id", required=True)
    return parser.parse_args()


def build_spark(catalogs, run_id):
    client_id = os.getenv("POLARIS_CLIENT_ID", "")
    client_secret = os.getenv("POLARIS_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        raise RuntimeError("POLARIS_CLIENT_ID / POLARIS_CLIENT_SECRET are not set")

    builder = SparkSession.builder.appName(f"EInvoicing-Silver-{run_id}")
    for catalog in catalogs:
        builder = builder.config(
            f"spark.sql.catalog.{catalog}.credential", f"{client_id}:{client_secret}"
        )
    return builder.getOrCreate()


# --- pass one: read and validate, one partition at a time -------------------
def _decimal(value, scale=2):
    return None if value is None else Decimal(value).quantize(Decimal(10) ** -scale)


def lire_partition(rows):
    """Parse and validate a whole partition, then yield one row per invoice.

    Parsing comes first because the profile decides which Schematron applies, and
    a document that is not XML at all is recorded rather than dropped: it would
    otherwise vanish between bronze and silver, which is the one thing a chain
    handling other people's documents must never do.
    """
    rows = list(rows)
    lus, illisibles = [], []

    for row in rows:
        try:
            lus.append((row, parseur.parse(row.contenu_xml)))
        except Exception as erreur:  # noqa: BLE001 - the reason is data, not code
            illisibles.append((row, str(erreur)[:400]))

    verdicts = validateur.valider(
        [
            (row.empreinte + row.chemin_source, row.contenu_xml.encode("utf-8"), lu["profil"])
            for row, lu in lus
        ]
    )

    for row, message in illisibles:
        yield _vide(row, message)

    for row, lu in lus:
        echecs = verdicts.get(row.empreinte + row.chemin_source, [])
        anomalies = [
            {"regle_id": "FMT-SCHEMATRON", "code": e["code"], "message": e["message"]}
            for e in echecs
        ]
        if lu["profil"] is None:
            anomalies.append(
                {
                    "regle_id": "FMT-PROFIL-INCONNU",
                    "code": "profil",
                    "message": f"guideline non reconnue: {lu['guideline']}",
                }
            )
        yield _ligne_facture(row, lu, anomalies)


def _vide(row, message):
    valeurs = {champ.name: None for champ in PARSED.fields}
    valeurs.update(
        {
            "chemin_source": row.chemin_source,
            "empreinte": row.empreinte,
            "mois": row.mois,
            "nb_lignes": 0,
            "lignes": [],
            "ventilation_tva": [],
            "anomalies_format": [
                {"regle_id": "FMT-XSD", "code": "parse", "message": message}
            ],
        }
    )
    return tuple(valeurs[champ.name] for champ in PARSED.fields)


def _ligne_facture(row, lu, anomalies):
    fournisseur, acheteur = lu["fournisseur"], lu["acheteur"]
    valeurs = {
        "chemin_source": row.chemin_source,
        "empreinte": row.empreinte,
        "mois": row.mois,
        "numero_facture": lu["numero_facture"],
        "type_document": lu["type_document"],
        "profil": lu["profil"],
        "guideline": lu["guideline"],
        "date_emission": lu["date_emission"],
        "date_livraison": lu["date_livraison"],
        "date_echeance": lu["date_echeance"],
        "reference_acheteur": lu["reference_acheteur"],
        "bon_de_commande": lu["bon_de_commande"],
        "devise": lu["devise"],
        "iban": lu["iban"],
        "siret_emetteur": fournisseur["siret"],
        "siren_emetteur": fournisseur["siren"],
        "nom_emetteur": fournisseur["nom"],
        "tva_emetteur": fournisseur["tva_intracom"],
        "code_postal_emetteur": fournisseur["code_postal"],
        "commune_emetteur": fournisseur["libelle_commune"],
        "siret_acheteur": acheteur["siret"],
        "siren_acheteur": acheteur["siren"],
        "nom_acheteur": acheteur["nom"],
        "code_postal_acheteur": acheteur["code_postal"],
        "commune_acheteur": acheteur["libelle_commune"],
        "total_lignes": _decimal(lu["total_lignes"]),
        "montant_ht": _decimal(lu["montant_ht"]),
        "montant_tva": _decimal(lu["montant_tva"]),
        "montant_ttc": _decimal(lu["montant_ttc"]),
        "montant_du": _decimal(lu["montant_du"]),
        "nb_lignes": len(lu["lignes"]),
        "lignes": [
            {
                "numero_ligne": l["numero_ligne"],
                "designation": l["designation"],
                "quantite": _decimal(l["quantite"], 3),
                "unite": l["unite"],
                "prix_unitaire": _decimal(l["prix_unitaire"]),
                "montant_ht": _decimal(l["montant_ht"]),
                "taux_tva": _decimal(l["taux_tva"]),
                "categorie_tva": l["categorie_tva"],
            }
            for l in lu["lignes"]
        ],
        "ventilation_tva": [
            {
                "taux_tva": _decimal(v["taux_tva"]),
                "montant_ht": _decimal(v["montant_ht"]),
                "montant_tva": _decimal(v["montant_tva"]),
                "categorie_tva": v["categorie_tva"],
            }
            for v in lu["ventilation_tva"]
        ],
        "anomalies_format": anomalies,
    }
    return tuple(valeurs[champ.name] for champ in PARSED.fields)


# --- pass two: what one document cannot tell on its own ---------------------
def cle_tva_attendue(siren):
    return F.concat(
        F.lit("FR"),
        F.lpad(
            (((F.lit(12) + F.lit(3) * (siren.cast("long") % F.lit(97))) % F.lit(97))).cast(
                "string"
            ),
            2,
            "0",
        ),
        siren,
    )


def anomalie(factures, condition, regle_id, constate=None, attendu=None):
    """One anomaly table row per invoice matching `condition`."""
    famille, gravite, libelle = regles.RULES[regle_id]
    return (
        factures.filter(condition)
        .select(
            F.col("empreinte"),
            F.col("chemin_source"),
            F.col("numero_facture"),
            F.col("mois"),
            F.col("siren_emetteur"),
            F.col("siret_acheteur"),
            F.lit(regle_id).alias("regle_id"),
            F.lit(famille).alias("famille"),
            F.lit(gravite).alias("gravite"),
            F.lit(libelle).alias("libelle"),
            F.lit(None).cast("string").alias("code"),
            (constate if constate is not None else F.lit(None).cast("string")).alias(
                "valeur_constatee"
            ),
            (attendu if attendu is not None else F.lit(None).cast("string")).alias(
                "valeur_attendue"
            ),
            F.col("montant_ttc"),
        )
    )


def controles_metier(factures):
    taux_connus = [float(t) for t in regles.TAUX_TVA_FR]

    ecart_totaux = F.abs(
        F.coalesce(F.col("montant_ht"), F.lit(0))
        + F.coalesce(F.col("montant_tva"), F.lit(0))
        - F.coalesce(F.col("montant_ttc"), F.lit(0))
    )

    taux_inconnu = F.size(
        F.filter(
            F.col("ventilation_tva"),
            lambda v: ~v["taux_tva"].cast("double").isin(taux_connus),
        )
    ) > 0

    return [
        anomalie(
            factures,
            (F.col("montant_ttc").isNotNull()) & (ecart_totaux > F.lit(TOLERANCE)),
            "MET-TOTAUX",
            constate=F.col("montant_ttc").cast("string"),
            attendu=(F.col("montant_ht") + F.col("montant_tva")).cast("string"),
        ),
        anomalie(
            factures,
            taux_inconnu,
            "MET-TAUX-TVA",
            constate=F.concat_ws(
                ", ", F.transform(F.col("ventilation_tva"), lambda v: v["taux_tva"].cast("string"))
            ),
            attendu=F.lit(", ".join(str(t) for t in regles.TAUX_TVA_FR)),
        ),
        anomalie(
            factures,
            F.col("date_echeance").isNotNull()
            & F.col("date_emission").isNotNull()
            & (F.col("date_echeance") < F.col("date_emission")),
            "MET-ECHEANCE",
            constate=F.col("date_echeance"),
            attendu=F.concat(F.lit(">= "), F.col("date_emission")),
        ),
        anomalie(
            factures,
            F.col("reference_acheteur").isNull(),
            "MET-MENTION",
            attendu=F.lit("BT-10 référence acheteur"),
        ),
        anomalie(
            factures,
            F.col("tva_emetteur").isNotNull()
            & F.col("siren_emetteur").isNotNull()
            & (F.col("tva_emetteur") != cle_tva_attendue(F.col("siren_emetteur"))),
            "MET-TVA-CLE",
            constate=F.col("tva_emetteur"),
            attendu=cle_tva_attendue(F.col("siren_emetteur")),
        ),
    ]


def controle_doublons(factures):
    """The same number issued twice by the same company.

    Counted over distinct files, so an invoice that bronze ingested once is never
    its own duplicate however many times the job is re-run.
    """
    compte = (
        factures.groupBy("siren_emetteur", "numero_facture")
        .agg(F.countDistinct("chemin_source").alias("occurrences"))
        .filter(F.col("occurrences") > 1)
    )
    marquees = factures.join(compte, ["siren_emetteur", "numero_facture"], "inner")
    return anomalie(
        marquees,
        F.lit(True),
        "MET-DOUBLON",
        constate=F.col("occurrences").cast("string"),
        attendu=F.lit("1"),
    )


def controles_referentiel(factures, referentiel, etats, sirens):
    """Three questions, three sources, and they are not interchangeable.

    "Does this company exist" is asked of the legal-unit registry, not of the
    active establishments: a company whose only establishment has closed is still
    a company. Asking the wrong table made every ceased issuer look invented as
    well -- measured on a run, six of eleven.
    """
    emetteurs = sirens.select(
        F.col("ul_siren").alias("ref_siren"), F.lit(True).alias("siren_connu")
    )

    acheteurs = referentiel.select(
        F.col("siret").alias("ref_siret_acheteur"), F.lit(True).alias("acheteur_connu")
    )

    marquees = (
        factures.join(
            emetteurs, factures["siren_emetteur"] == F.col("ref_siren"), "left"
        )
        .join(
            acheteurs,
            factures["siret_acheteur"] == F.col("ref_siret_acheteur"),
            "left",
        )
        .join(etats, factures["siret_emetteur"] == etats["etat_siret"], "left")
    )

    return [
        anomalie(
            marquees,
            F.col("siren_connu").isNull() & F.col("siren_emetteur").isNotNull(),
            "REF-SIREN-INCONNU",
            constate=F.col("siren_emetteur"),
            attendu=F.lit("SIREN présent dans SIRENE"),
        ),
        anomalie(
            marquees,
            F.col("etat_etablissement").isNotNull()
            & (F.col("etat_etablissement") != F.lit(ACTIF)),
            "REF-EMETTEUR-CESSE",
            constate=F.col("etat_etablissement"),
            attendu=F.lit(ACTIF),
        ),
        anomalie(
            marquees,
            F.col("acheteur_connu").isNull() & F.col("siret_acheteur").isNotNull(),
            "REF-ACHETEUR-INCONNU",
            constate=F.col("siret_acheteur"),
            attendu=F.lit("SIRET présent dans SIRENE"),
        ),
    ]


def controle_montant_aberrant(factures):
    """Abnormal for its trade, not abnormal in absolute terms.

    A hundred thousand euros is ordinary in construction and remarkable for a
    hairdresser, so the reference is the median of the issuer's own NAF section.
    """
    medianes = factures.groupBy("code_section_naf_emetteur").agg(
        F.expr("percentile_approx(montant_ht, 0.5)").alias("mediane_section")
    )
    marquees = factures.join(medianes, "code_section_naf_emetteur", "left")
    return anomalie(
        marquees,
        F.col("mediane_section").isNotNull()
        & (F.col("montant_ht") > F.col("mediane_section") * F.lit(FACTEUR_ABERRANT)),
        "STA-MONTANT-ABERRANT",
        constate=F.col("montant_ht").cast("string"),
        attendu=F.concat(
            F.lit(f"<= {FACTEUR_ABERRANT} x médiane de section ("),
            F.col("mediane_section").cast("string"),
            F.lit(")"),
        ),
    )


def anomalies_format(factures):
    eclatees = factures.select(
        "empreinte",
        "chemin_source",
        "numero_facture",
        "mois",
        "siren_emetteur",
        "siret_acheteur",
        "montant_ttc",
        F.explode("anomalies_format").alias("a"),
    )
    catalogue = F.create_map(
        *[
            item
            for regle_id, (famille, gravite, libelle) in regles.RULES.items()
            for item in (
                F.lit(regle_id),
                F.struct(
                    F.lit(famille).alias("famille"),
                    F.lit(gravite).alias("gravite"),
                    F.lit(libelle).alias("libelle"),
                ),
            )
        ]
    )
    return eclatees.select(
        "empreinte",
        "chemin_source",
        "numero_facture",
        "mois",
        "siren_emetteur",
        "siret_acheteur",
        F.col("a.regle_id").alias("regle_id"),
        catalogue[F.col("a.regle_id")]["famille"].alias("famille"),
        catalogue[F.col("a.regle_id")]["gravite"].alias("gravite"),
        catalogue[F.col("a.regle_id")]["libelle"].alias("libelle"),
        F.col("a.code").alias("code"),
        F.col("a.message").alias("valeur_constatee"),
        F.lit(None).cast("string").alias("valeur_attendue"),
        "montant_ttc",
    )


# --- enrichment -------------------------------------------------------------
def enrichir(factures, referentiel):
    """What the referential knows that the invoice does not say.

    The issuer's size class is the one that matters most downstream: it is what
    the reform's September 2026 and September 2027 deadlines are keyed on.
    """
    emetteur = referentiel.select(
        F.col("siret").alias("e_siret"),
        F.col("code_section_naf").alias("code_section_naf_emetteur"),
        F.col("libelle_section_naf").alias("libelle_section_naf_emetteur"),
        F.col("code_naf").alias("code_naf_emetteur"),
        F.col("code_departement").alias("code_departement_emetteur"),
        F.col("categorie_entreprise").alias("categorie_entreprise_emetteur"),
    )
    acheteur = referentiel.select(
        F.col("siret").alias("a_siret"),
        F.col("code_departement").alias("code_departement_acheteur"),
        F.col("categorie_juridique").alias("categorie_juridique_acheteur"),
    )
    return (
        factures.join(emetteur, factures["siret_emetteur"] == F.col("e_siret"), "left")
        .join(acheteur, factures["siret_acheteur"] == F.col("a_siret"), "left")
        .drop("e_siret", "a_siret")
    )


def main():
    args = parse_args()

    print("=" * 70)
    print("E-invoicing - silver")
    print("=" * 70)
    print(f"Bronze:      {args.bronze}")
    print(f"Referential: {args.referentiel}")
    print(f"Target:      {args.catalog}.{args.namespace}")
    print("=" * 70)

    catalogue_referentiel = args.referentiel.split(".")[0]
    spark = build_spark({catalogue_referentiel, args.catalog}, args.run_id)
    spark.sparkContext.setLogLevel("WARN")
    for module in (
        "einvoicing_parse.py",
        "einvoicing_rules.py",
        "einvoicing_validate.py",
    ):
        spark.sparkContext.addPyFile(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), module)
        )

    bronze = spark.read.parquet(args.bronze)
    print(f"\nDocuments to process: {bronze.count():,}")

    factures = spark.createDataFrame(
        bronze.select("chemin_source", "empreinte", "mois", "contenu_xml").rdd.mapPartitions(
            lire_partition
        ),
        PARSED,
    )

    referentiel = spark.table(args.referentiel).select(
        "siret",
        "siren",
        "code_naf",
        "code_section_naf",
        "libelle_section_naf",
        "code_departement",
        "categorie_entreprise",
        "categorie_juridique",
    )
    etats = spark.read.parquet(f"{args.sirene_bronze}/sirene_etablissement/").select(
        F.col("siret").alias("etat_siret"),
        F.col("etatAdministratifEtablissement").alias("etat_etablissement"),
    )
    # The registry of legal units: every company, whatever the state of its
    # establishments. This is what "the SIREN exists" means.
    sirens = (
        spark.read.parquet(f"{args.sirene_bronze}/sirene_unite_legale/")
        .select(F.col("siren").alias("ul_siren"))
        .distinct()
    )

    factures = enrichir(factures, referentiel).cache()
    anomalies_ref = controles_referentiel(factures, referentiel, etats, sirens)

    toutes = anomalies_format(factures)
    for lot in controles_metier(factures) + anomalies_ref:
        toutes = toutes.unionByName(lot)
    toutes = toutes.unionByName(controle_doublons(factures)).unionByName(
        controle_montant_aberrant(factures)
    )

    lignes = factures.select(
        "empreinte",
        "numero_facture",
        "mois",
        F.posexplode("lignes").alias("position", "l"),
    ).select(
        "empreinte",
        "numero_facture",
        "mois",
        (F.col("position") + F.lit(1)).alias("position"),
        F.col("l.*"),
    )

    entetes = factures.drop("lignes", "anomalies_format")

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {args.catalog}.{args.namespace}")
    for nom, donnees in (
        ("factures", entetes),
        ("lignes", lignes),
        ("anomalies", toutes),
    ):
        cible = f"{args.catalog}.{args.namespace}.{nom}"
        print(f"\nPublishing {cible}...")
        (
            donnees.writeTo(cible)
            .using("iceberg")
            .partitionedBy(F.col("mois"))
            .tableProperty("format-version", "2")
            .createOrReplace()
        )

    publiees = spark.table(f"{args.catalog}.{args.namespace}.factures")
    anomalies = spark.table(f"{args.catalog}.{args.namespace}.anomalies")

    print(f"\nInvoices: {publiees.count():,}   anomalies: {anomalies.count():,}")

    print("\nAnomalies by family and rule:")
    anomalies.groupBy("famille", "gravite", "regle_id").count().orderBy(
        "famille", "regle_id"
    ).show(40, truncate=False)

    print("\nSchematron rules that fired:")
    anomalies.filter(F.col("regle_id") == F.lit("FMT-SCHEMATRON")).groupBy(
        "code"
    ).count().orderBy(F.col("count").desc()).show(20, truncate=False)

    print("\nInvoices carrying at least one anomaly:")
    publiees.select("empreinte").join(
        anomalies.select("empreinte").distinct(), "empreinte", "left_semi"
    ).selectExpr("count(*) as factures_avec_anomalie").show(truncate=False)

    print("\nEnrichment coverage:")
    publiees.selectExpr(
        "count(*) as factures",
        "count(code_section_naf_emetteur) as avec_section_naf",
        "count(code_departement_emetteur) as avec_departement",
        "count(categorie_entreprise_emetteur) as avec_categorie",
    ).show(truncate=False)

    print("\n" + "=" * 70)
    print("Silver completed!")
    print("=" * 70)
    spark.stop()


if __name__ == "__main__":
    main()
