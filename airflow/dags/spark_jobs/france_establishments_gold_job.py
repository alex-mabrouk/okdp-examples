"""
France establishments - gold indicators PySpark job
Aggregates the silver establishments into the three tables the dashboard reads,
published as Iceberg tables in a Polaris REST catalog.
"""
import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, concat, count, countDistinct
from pyspark.sql.functions import create_map, lit
from pyspark.sql.functions import max_by
from pyspark.sql.functions import round as _round
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import when

# Establishments registered abroad: no cell on any map.
UNKNOWN_DEPARTMENT = "ZZ"

# Superset's France map keys its departments FR-<code>, but names the overseas
# ones by ISO letter code rather than by number. Without this the map renders
# metropolitan France and drops the DOM silently.
CARTE_OUTRE_MER = {"971": "GP", "972": "MQ", "973": "GF", "974": "RE", "976": "YT"}


# INSEE department names, keyed by the code the bronze job slices out of the
# commune code. Saint-Pierre-et-Miquelon, Saint-Barthélemy and Saint-Martin are
# collectivities rather than departments, but SIRENE addresses establishments
# there, so they get a name too. A code that is not listed keeps its number:
# a nomenclature gap shows up as a number, never as the wrong name.
DEPARTEMENTS = {
    "01": "Ain",
    "02": "Aisne",
    "03": "Allier",
    "04": "Alpes-de-Haute-Provence",
    "05": "Hautes-Alpes",
    "06": "Alpes-Maritimes",
    "07": "Ardèche",
    "08": "Ardennes",
    "09": "Ariège",
    "10": "Aube",
    "11": "Aude",
    "12": "Aveyron",
    "13": "Bouches-du-Rhône",
    "14": "Calvados",
    "15": "Cantal",
    "16": "Charente",
    "17": "Charente-Maritime",
    "18": "Cher",
    "19": "Corrèze",
    "21": "Côte-d'Or",
    "22": "Côtes-d'Armor",
    "23": "Creuse",
    "24": "Dordogne",
    "25": "Doubs",
    "26": "Drôme",
    "27": "Eure",
    "28": "Eure-et-Loir",
    "29": "Finistère",
    "2A": "Corse-du-Sud",
    "2B": "Haute-Corse",
    "30": "Gard",
    "31": "Haute-Garonne",
    "32": "Gers",
    "33": "Gironde",
    "34": "Hérault",
    "35": "Ille-et-Vilaine",
    "36": "Indre",
    "37": "Indre-et-Loire",
    "38": "Isère",
    "39": "Jura",
    "40": "Landes",
    "41": "Loir-et-Cher",
    "42": "Loire",
    "43": "Haute-Loire",
    "44": "Loire-Atlantique",
    "45": "Loiret",
    "46": "Lot",
    "47": "Lot-et-Garonne",
    "48": "Lozère",
    "49": "Maine-et-Loire",
    "50": "Manche",
    "51": "Marne",
    "52": "Haute-Marne",
    "53": "Mayenne",
    "54": "Meurthe-et-Moselle",
    "55": "Meuse",
    "56": "Morbihan",
    "57": "Moselle",
    "58": "Nièvre",
    "59": "Nord",
    "60": "Oise",
    "61": "Orne",
    "62": "Pas-de-Calais",
    "63": "Puy-de-Dôme",
    "64": "Pyrénées-Atlantiques",
    "65": "Hautes-Pyrénées",
    "66": "Pyrénées-Orientales",
    "67": "Bas-Rhin",
    "68": "Haut-Rhin",
    "69": "Rhône",
    "70": "Haute-Saône",
    "71": "Saône-et-Loire",
    "72": "Sarthe",
    "73": "Savoie",
    "74": "Haute-Savoie",
    "75": "Paris",
    "76": "Seine-Maritime",
    "77": "Seine-et-Marne",
    "78": "Yvelines",
    "79": "Deux-Sèvres",
    "80": "Somme",
    "81": "Tarn",
    "82": "Tarn-et-Garonne",
    "83": "Var",
    "84": "Vaucluse",
    "85": "Vendée",
    "86": "Vienne",
    "87": "Haute-Vienne",
    "88": "Vosges",
    "89": "Yonne",
    "90": "Territoire de Belfort",
    "91": "Essonne",
    "92": "Hauts-de-Seine",
    "93": "Seine-Saint-Denis",
    "94": "Val-de-Marne",
    "95": "Val-d'Oise",
    "971": "Guadeloupe",
    "972": "Martinique",
    "973": "Guyane",
    "974": "La Réunion",
    "975": "Saint-Pierre-et-Miquelon",
    "976": "Mayotte",
    "977": "Saint-Barthélemy",
    "978": "Saint-Martin",
    UNKNOWN_DEPARTMENT: "Non déterminé",
}


def libelle_departement():
    """The name every gold table carries next to its department code.

    Without it a question about "Paris" has to be answered by guessing the code,
    and the codes are strings: '01' is a department, 1 is nothing.
    """
    labels = create_map([lit(x) for pair in DEPARTEMENTS.items() for x in pair])
    code = col("code_departement")
    return coalesce(labels[code], code)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-catalog", required=True)
    parser.add_argument("--source-namespace", required=True)
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--run-id", required=True)
    return parser.parse_args()


def build_spark(catalogs, run_id):
    """Everything but the credentials comes from the SparkApplication sparkConf.

    The credential is assembled here from the mounted Secret so the client
    secret never lands in the SparkApplication manifest.
    """
    client_id = os.getenv("POLARIS_CLIENT_ID", "")
    client_secret = os.getenv("POLARIS_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        raise RuntimeError("POLARIS_CLIENT_ID / POLARIS_CLIENT_SECRET are not set")

    builder = SparkSession.builder.appName(f"FranceEstablishments-Gold-{run_id}")
    for catalog in catalogs:
        builder = builder.config(
            f"spark.sql.catalog.{catalog}.credential", f"{client_id}:{client_secret}"
        )
    return builder.getOrCreate()


# SIRENE addresses Paris, Lyon and Marseille by arrondissement. A ranking of
# communes wants the city: without this Paris appears twenty times and never
# above Toulouse.
ARRONDISSEMENTS = [("751", "75056"), ("6938", "69123"), ("132", "13055")]


def commune_agregee():
    code = col("code_commune")
    return coalesce(
        *[when(code.startswith(prefix), lit(city)) for prefix, city in ARRONDISSEMENTS],
        code,
    )


# Level III of the INSEE legal-form nomenclature (1 September 2022 edition),
# labels verbatim except for the "(sans autre indication)" qualifier, which is a
# registry detail rather than something to print on a chart. These nine codes
# cover 95% of the establishments INSEE leaves unclassified; the nomenclature has
# 260, and the tail is deliberately not enumerated.
FORMES_JURIDIQUES = {
    1000: "Entrepreneur individuel",
    6540: "Société civile immobilière",
    9220: "Association déclarée",
    5710: "SAS, société par actions simplifiée",
    5499: "Société à responsabilité limitée",
    6599: "Autre société civile",
    2110: "Indivision entre personnes physiques",
    6598: "Exploitation agricole à responsabilité limitée",
    9110: "Syndicat de copropriété",
}
AUTRE_FORME = "Autre forme juridique"


def forme_juridique():
    code = col("categorie_juridique")
    return coalesce(
        *[when(code == lit(value), lit(label)) for value, label in FORMES_JURIDIQUES.items()],
        lit(AUTRE_FORME),
    )


def code_carte():
    department = col("code_departement")
    key = coalesce(
        *[when(department == lit(code), lit(iso)) for code, iso in CARTE_OUTRE_MER.items()],
        department,
    )
    return when(department != lit(UNKNOWN_DEPARTMENT), concat(lit("FR-"), key))


def _share(numerator, total):
    return _round(100.0 * numerator / total, 2)


def par_departement(silver):
    """One row per department: what the map and the indicator tiles read."""
    total = count("*")
    qpv = _sum(col("en_qpv").cast("int"))
    ess = _sum(col("est_ess").cast("int"))
    return (
        silver.groupBy("code_departement")
        .agg(
            total.alias("nb_etablissements"),
            qpv.alias("nb_qpv"),
            _share(qpv, total).alias("part_qpv"),
            ess.alias("nb_ess"),
            _share(ess, total).alias("part_ess"),
            _sum(col("est_siege").cast("int")).alias("nb_sieges"),
            countDistinct(commune_agregee()).alias("nb_communes"),
            countDistinct("code_iris").alias("nb_iris"),
        )
        .withColumn("code_carte", code_carte())
        .orderBy(col("nb_etablissements").desc())
    )


def par_section_naf(silver):
    """Activity mix per department.

    No national roll-up row: the three gold tables all carry code_departement,
    so one dashboard filter drives them all, and summing over departments gives
    the national figure anyway.
    """
    return (
        silver.filter(col("code_section_naf").isNotNull())
        .groupBy("code_departement", "code_section_naf", "libelle_section_naf")
        .agg(
            count("*").alias("nb_etablissements"),
            _sum(col("en_qpv").cast("int")).alias("nb_qpv"),
        )
    )


def par_commune(silver):
    """Commune grain: the ranking, and the commune search the dashboard needs.

    The label cannot be part of the grouping key. SIRENE spells the same commune
    several ways across establishments -- apostrophe variants, pre-merger names,
    arrondissement forms -- so 35 036 commune codes carry 41 683 distinct labels
    and a ranking would list the same commune more than once. One label per code
    is elected by majority, after the arrondissements are rolled up.
    """
    communes = silver.filter(col("code_commune").isNotNull()).withColumn(
        "code_commune", commune_agregee()
    )
    labels = (
        communes.groupBy("code_commune", "libelle_commune")
        .agg(count("*").alias("occurrences"))
        .groupBy("code_commune")
        .agg(max_by("libelle_commune", "occurrences").alias("libelle_commune"))
    )
    return (
        communes.groupBy("code_departement", "code_commune")
        .agg(
            count("*").alias("nb_etablissements"),
            _sum(col("en_qpv").cast("int")).alias("nb_qpv"),
        )
        .join(labels, on="code_commune", how="left")
        # A ranking of bare commune names is ambiguous; the department makes it
        # readable at a glance, so the label ships ready to plot.
        .withColumn(
            "commune_departement",
            concat(col("libelle_commune"), lit(" ("), col("code_departement"), lit(")")),
        )
    )


def par_categorie(silver):
    """PME / ETI / GE, falling back to the legal form.

    INSEE computes the enterprise category (décret 2008-1354) from headcount,
    turnover and balance sheet, so units with no such economic data get none --
    43% of the rows. Labelling that slice "unknown" wastes it: the legal form is
    known for all of them, and saying "sole trader", "property company" or
    "association" is the actual answer to why there is no category.
    """
    return (
        silver.withColumn(
            "categorie",
            coalesce(col("categorie_entreprise"), forme_juridique()),
        )
        .groupBy("code_departement", "categorie")
        .agg(count("*").alias("nb_etablissements"))
    )


def creations_par_mois(silver):
    """Monthly creations. No date floor here: the dashboard sets its own range."""
    return (
        silver.filter(col("mois_creation").isNotNull())
        .groupBy("mois_creation", "code_departement")
        .agg(count("*").alias("nb_creations"))
    )


TABLES = {
    "etablissements_par_departement": par_departement,
    "etablissements_par_section_naf": par_section_naf,
    "etablissements_par_commune": par_commune,
    "etablissements_par_categorie": par_categorie,
    "creations_par_mois": creations_par_mois,
}


def main():
    args = parse_args()
    source = f"{args.source_catalog}.{args.source_namespace}.{args.source_table}"

    print("=" * 70)
    print("Gold indicators - France establishments")
    print("=" * 70)
    print(f"Source: {source}")
    print(f"Target: {args.catalog}.{args.namespace}.*")
    print("=" * 70)

    spark = build_spark({args.source_catalog, args.catalog}, args.run_id)
    spark.sparkContext.setLogLevel("WARN")

    silver = spark.table(source)
    print(f"\nRows read: {silver.count():,}")

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {args.catalog}.{args.namespace}")

    for name, build in TABLES.items():
        qualified_table = f"{args.catalog}.{args.namespace}.{name}"
        print(f"\nPublishing {qualified_table}...")
        # Every gold table groups on code_departement, so the label is added in
        # one place rather than in each builder.
        indicators = build(silver).withColumn(
            "libelle_departement", libelle_departement()
        ).cache()
        (
            indicators.writeTo(qualified_table)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .createOrReplace()
        )
        print(f"Rows: {indicators.count():,}")
        indicators.show(5, truncate=False)
        indicators.unpersist()

    print("\n" + "=" * 70)
    print("Gold indicators completed!")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()
