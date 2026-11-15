"""
France establishments - AI insights PySpark job
The figures the model is allowed to talk about, and the sentence Spark asserts
about each of them. Everything else -- the prompt, the checks, the two tables --
is in `ai_insights`, shared with the e-invoicing chain.
"""
from ai_insights import fact, format_number as n, queries, run

UNKNOWN_DEPARTMENT = "ZZ"


def collect_facts(spark, gold):
    """Every figure the model may mention, and the sentence Spark asserts about it."""
    dept = f"{gold}.etablissements_par_departement"
    naf = f"{gold}.etablissements_par_section_naf"
    commune = f"{gold}.etablissements_par_commune"
    categorie = f"{gold}.etablissements_par_categorie"
    creations = f"{gold}.creations_par_mois"
    known = f"code_departement <> '{UNKNOWN_DEPARTMENT}'"

    one, rows = queries(spark)

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
                          f"{d} — {n(v)} % des établissements actifs sont situés dans un "
                          f"quartier prioritaire, contre {n(nat['part_qpv'])} % au niveau national, "
                          f"soit {n(gap)} points de plus.",
                          rank=i, comparison=nat["part_qpv"], comparison_unit="%"))

    r = one(f"SELECT libelle_departement, part_ess FROM {dept} WHERE {known} ORDER BY part_ess DESC LIMIT 1")
    d, v = r["libelle_departement"], r["part_ess"]
    facts.append(fact("top_dept_part_ess", "ess", d,
                      "part des établissements de l'économie sociale et solidaire", v, "%",
                      f"{d} — {n(v)} % des établissements actifs relèvent de l'économie "
                      f"sociale et solidaire, contre {n(nat['part_ess'])} % au niveau national, soit "
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


if __name__ == "__main__":
    run("FranceEstablishments-AI", collect_facts)
