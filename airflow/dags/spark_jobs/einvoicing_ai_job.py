"""
E-invoicing - AI insights PySpark job
The figures the model is allowed to talk about, and the sentence Spark asserts
about each of them. Everything else -- the prompt, the checks, the two tables --
is in `ai_insights`, shared with the establishments chain.

The four facts that carry the demo are the referential ones and the two reform
ones: they are the platform reading the reform's calendar on its own flow, which
no invoice carries and no LLM could infer.
"""
from ai_insights import fact, format_number as n, queries, run

MOIS = (
    "janvier", "février", "mars", "avril", "mai", "juin",
    "juillet", "août", "septembre", "octobre", "novembre", "décembre",
)

# The two dates the reform keys issuing on, as they appear in conformite_reforme.
ECHEANCE_2026 = "2026-09-01"
ECHEANCE_2027 = "2027-09-01"


def en_toutes_lettres(mois):
    """'2026-03' -> 'mars 2026'. A raw partition key is not a sentence."""
    annee, numero = mois.split("-")
    return f"{MOIS[int(numero) - 1]} {annee}"


def collect_facts(spark, gold):
    """Every figure the model may mention, and the sentence Spark asserts about it."""
    mensuel = f"{gold}.facturation_mensuelle"
    departement = f"{gold}.facturation_par_departement"
    naf = f"{gold}.facturation_par_section_naf"
    acteurs = f"{gold}.acteurs"
    qualite = f"{gold}.qualite_anomalies"
    reforme = f"{gold}.conformite_reforme"

    one, rows = queries(spark)

    flux = one(
        f"""SELECT sum(nb_factures) AS documents,
                   sum(montant_ht) AS ht,
                   sum(montant_tva) AS tva,
                   sum(nb_factures_anomalie) AS anomalies,
                   sum(nb_factures_bloquantes) AS bloquantes,
                   count(*) AS mois
            FROM {mensuel}"""
    )
    documents = flux["documents"]
    # Whole euros rather than millions: asked for "44,77 millions", the model writes
    # "44 770 000" -- a multiplication, and a figure the fact does not carry.
    ht = round(flux["ht"])
    taux = round(100.0 * flux["anomalies"] / documents, 2)

    facts = [
        fact("flux_documents", "volumétrie", "flux reçu",
             "nombre de documents de facturation reçus", documents, "documents",
             f"Sur la période, la plateforme a reçu {n(documents)} documents de "
             f"facturation électronique."),
        fact("flux_montant_ht", "volumétrie", "flux reçu",
             "montant hors taxes du flux reçu", ht, "euros",
             f"Sur la période, les factures reçues représentent {n(ht)} euros hors taxes."),
        fact("flux_montant_tva", "volumétrie", "flux reçu",
             "montant de TVA porté par le flux reçu", round(flux["tva"]), "euros",
             f"Sur la période, les factures reçues portent {n(round(flux['tva']))} euros "
             f"de TVA."),
        fact("flux_mois", "volumétrie", "flux reçu",
             "nombre de mois couverts par le flux", flux["mois"], "mois",
             f"Les factures reçues s'échelonnent sur {n(flux['mois'])} mois."),
    ]

    emetteurs = one(
        f"SELECT count(DISTINCT siren) AS n FROM {acteurs} WHERE role = 'émetteur'"
    )
    facts.append(fact("flux_emetteurs", "volumétrie", "flux reçu",
                      "nombre d'entreprises émettrices", emetteurs["n"], "entreprises",
                      f"Les factures reçues proviennent de {n(emetteurs['n'])} entreprises "
                      f"émettrices distinctes."))

    moyenne = round(flux["ht"] / documents)
    facts.append(fact("flux_facture_moyenne", "volumétrie", "flux reçu",
                      "montant hors taxes moyen d'une facture", moyenne, "euros",
                      f"Une facture du flux reçu porte en moyenne {n(moyenne)} euros hors taxes."))

    actif = one(
        f"SELECT mois, nb_factures FROM {mensuel} ORDER BY nb_factures DESC LIMIT 1"
    )
    mois_actif = en_toutes_lettres(actif["mois"])
    facts.append(fact("flux_mois_actif", "volumétrie", mois_actif,
                      "mois le plus chargé du flux reçu", actif["nb_factures"], "factures",
                      f"Le mois le plus chargé du flux est {mois_actif}, avec "
                      f"{n(actif['nb_factures'])} factures reçues.",
                      subject=actif["mois"]))

    # ------------------------------------------------------------- la qualité
    facts.append(fact("qualite_taux_anomalie", "qualité", "flux reçu",
                      "part des factures portant au moins une anomalie", taux, "%",
                      f"Sur le flux reçu, {n(taux)} % des factures portent au moins une "
                      f"anomalie détectée par les contrôles de la plateforme."))

    facts.append(fact("qualite_bloquantes", "qualité", "flux reçu",
                      "nombre de factures portant une anomalie bloquante",
                      flux["bloquantes"], "factures",
                      f"Sur le flux reçu, {n(flux['bloquantes'])} factures portent une anomalie "
                      f"bloquante et ne sont pas recevables en l'état."))

    premiere = one(
        f"""SELECT libelle, nb_factures FROM {qualite}
            ORDER BY nb_factures DESC LIMIT 1"""
    )
    facts.append(fact("qualite_premiere_regle", "qualité", "flux reçu",
                      "contrôle qui rejette le plus de factures",
                      premiere["nb_factures"], "factures",
                      f"Le contrôle « {premiere['libelle']} » est celui qui signale le plus de "
                      f"factures, avec {n(premiere['nb_factures'])} factures concernées.",
                      subject=premiere["libelle"]))

    # ------------------------------------------------- le contrôle par référentiel
    # The point of the whole demo: these three figures exist only because the
    # platform already hosts SIRENE, and no invoice carries the answer.
    par_regle = {
        r["regle_id"]: r
        for r in rows(
            f"""SELECT regle_id, libelle, nb_factures, montant_impacte FROM {qualite}"""
        )
    }

    inconnu = par_regle.get("REF-SIREN-INCONNU")
    if inconnu:
        facts.append(fact("referentiel_siren_inconnu", "référentiel", "flux reçu",
                          "factures dont le SIREN émetteur est absent de SIRENE",
                          inconnu["nb_factures"], "factures",
                          f"Sur le flux reçu, {n(inconnu['nb_factures'])} factures portent un "
                          f"SIREN émetteur absent du référentiel SIRENE hébergé par la plateforme.",
                          subject="REF-SIREN-INCONNU"))

    cesse = par_regle.get("REF-EMETTEUR-CESSE")
    if cesse:
        facts.append(fact("referentiel_emetteur_cesse", "référentiel", "flux reçu",
                          "factures émises par un établissement administrativement cessé",
                          cesse["nb_factures"], "factures",
                          f"Sur le flux reçu, {n(cesse['nb_factures'])} factures ont été émises "
                          f"par un établissement déclaré cessé dans le référentiel SIRENE.",
                          subject="REF-EMETTEUR-CESSE"))

    doublon = par_regle.get("MET-DOUBLON")
    if doublon:
        montant = round(doublon["montant_impacte"])
        facts.append(fact("qualite_doublons", "qualité", "flux reçu",
                          "montant TTC porté par les factures en doublon", montant, "euros",
                          # No "deux fois", no "double", no "seconde fois": offered any of
                          # them, the model asserts the total is twice something, which it is
                          # not. With nothing to embellish it stays on the figure.
                          f"Les factures détectées en doublon portent {n(montant)} euros TTC.",
                          subject="MET-DOUBLON"))

    # ------------------------------------------------------------- la réforme
    calendrier = {
        r["obligation_emission"]: r
        for r in rows(
            f"""SELECT obligation_emission,
                       sum(nb_factures) AS nb_factures,
                       sum(nb_emetteurs) AS nb_emetteurs
                FROM {reforme} GROUP BY obligation_emission"""
        )
    }
    deja = calendrier.get(ECHEANCE_2026, {}).get("nb_factures", 0)
    plus_tard = calendrier.get(ECHEANCE_2027, {}).get("nb_factures", 0)
    if deja or plus_tard:
        facts.append(fact("reforme_obligation_2026", "réforme", "flux reçu",
                          "factures déjà soumises à l'obligation d'émission",
                          deja, "factures",
                          f"Sur le flux reçu, {n(deja)} factures émanent d'entreprises déjà "
                          f"soumises à l'obligation d'émission depuis le 1er septembre 2026, "
                          f"contre {n(plus_tard)} qui ne le seront qu'au 1er septembre 2027.",
                          comparison=plus_tard, comparison_unit="factures"))

        facts.append(fact("reforme_obligation_2027", "réforme", "flux reçu",
                          "factures soumises à l'obligation d'émission en septembre 2027",
                          plus_tard, "factures",
                          f"Sur le flux reçu, {n(plus_tard)} factures émanent de PME, qui "
                          f"n'auront l'obligation d'émettre au format électronique qu'au "
                          f"1er septembre 2027."))

    couverture = one(
        f"""SELECT sum(CASE WHEN categorie_entreprise = 'NON RENSEIGNÉE' THEN nb_factures
                            ELSE 0 END) AS inconnues,
                   sum(nb_factures) AS total
            FROM {reforme}"""
    )
    part_inconnue = round(100.0 * couverture["inconnues"] / couverture["total"], 2)
    facts.append(fact("reforme_categorie_inconnue", "réforme", "flux reçu",
                      "part des factures dont la taille de l'émetteur est inconnue",
                      part_inconnue, "%",
                      f"Sur le flux reçu, {n(part_inconnue)} % des factures proviennent d'un "
                      f"émetteur dont le référentiel ne donne pas la taille, et dont l'échéance "
                      f"reste donc indéterminée."))

    # ----------------------------------------------------- territoire et secteur
    dept = one(
        f"""SELECT libelle_departement, montant_ht, nb_factures FROM {departement}
            ORDER BY montant_ht DESC LIMIT 1"""
    )
    # The dash form, kept only where the preposition is irregular: "dans le Nord",
    # "en Guyane". Never explained in the prompt -- the 7b copies the instruction
    # into its sentence.
    montant_dept = round(dept["montant_ht"])
    facts.append(fact("territoire_premier_departement", "territoire",
                      dept["libelle_departement"],
                      "montant hors taxes facturé par les émetteurs du département",
                      montant_dept, "euros",
                      f"{dept['libelle_departement']} — {n(montant_dept)} euros hors taxes "
                      f"facturés par les émetteurs du département, soit le premier "
                      f"département du flux.",
                      rank=1))

    secteur = one(
        f"""SELECT libelle_section_naf, montant_ht, nb_factures FROM {naf}
            ORDER BY montant_ht DESC LIMIT 1"""
    )
    facts.append(fact("secteur_premier", "secteurs", "flux reçu",
                      "secteur d'activité portant le plus gros montant facturé",
                      round(secteur["montant_ht"]), "euros",
                      f"Le secteur « {secteur['libelle_section_naf']} » porte le plus gros montant "
                      f"du flux, avec {n(round(secteur['montant_ht']))} euros hors taxes.",
                      subject=secteur["libelle_section_naf"]))

    anomalie_secteur = one(
        f"""SELECT libelle_section_naf, round(100.0 * nb_factures_anomalie / nb_factures, 2)
                   AS taux
            FROM {naf} WHERE nb_factures >= 20 ORDER BY taux DESC LIMIT 1"""
    )
    facts.append(fact("secteur_plus_anomalies", "qualité",
                      anomalie_secteur["libelle_section_naf"],
                      "part des factures en anomalie dans le secteur",
                      anomalie_secteur["taux"], "%",
                      f"Le secteur « {anomalie_secteur['libelle_section_naf']} » affiche le taux "
                      f"d'anomalie le plus élevé du flux, avec {n(anomalie_secteur['taux'])} % de "
                      f"ses factures signalées, contre {n(taux)} % sur l'ensemble du flux.",
                      subject=anomalie_secteur["libelle_section_naf"],
                      comparison=taux, comparison_unit="%"))

    return facts


if __name__ == "__main__":
    run("EInvoicing-AI", collect_facts)
