"""
E-invoicing - the control catalogue
One place naming every anomaly, so that the generator that injects one and the
silver job that catches it cannot drift apart: comparing what was injected to what
was detected only means something if both sides spell the rule the same way.

Four families, and the split is not presentational. It came out of a measurement:
an unusual VAT rate whose VAT amount is correctly computed passes the official
Schematron untouched, because the list of French rates is not an EN 16931 rule.
Format conformity and business conformity really are two different questions.

  FORMAT        the document does not conform to the standard. Verdict from the
                XSD and the Schematrons published with Factur-X 1.09
  REFERENTIEL   the document is well formed but names a company that the SIRENE
                referential the platform already holds does not back
  METIER        the document contradicts itself, or French invoicing rules
  STATISTIQUE   the document is plausible on its own and abnormal in its context

Severity is what a receiving platform would do with it:
  bloquante  the invoice cannot be accepted
  majeure    it is accepted and flagged for handling
  mineure    it is accepted and reported
"""

FORMAT = "FORMAT"
REFERENTIEL = "REFERENTIEL"
METIER = "METIER"
STATISTIQUE = "STATISTIQUE"

BLOQUANTE = "bloquante"
MAJEURE = "majeure"
MINEURE = "mineure"

# regle_id -> (famille, gravite, libelle)
RULES = {
    "FMT-XSD": (FORMAT, BLOQUANTE, "XML non conforme au schéma CII D22B"),
    "FMT-SCHEMATRON": (FORMAT, BLOQUANTE, "Règle EN 16931 en échec"),
    "FMT-PROFIL-INCONNU": (FORMAT, BLOQUANTE, "Profil Factur-X non reconnu"),
    "REF-SIREN-INCONNU": (
        REFERENTIEL,
        BLOQUANTE,
        "SIREN émetteur absent du référentiel SIRENE",
    ),
    "REF-EMETTEUR-CESSE": (
        REFERENTIEL,
        MAJEURE,
        "Établissement émetteur administrativement cessé",
    ),
    "REF-ACHETEUR-INCONNU": (
        REFERENTIEL,
        MAJEURE,
        "SIRET acheteur absent du référentiel SIRENE",
    ),
    "MET-TOTAUX": (METIER, BLOQUANTE, "HT + TVA ≠ TTC au-delà de l'arrondi"),
    "MET-TAUX-TVA": (METIER, BLOQUANTE, "Taux de TVA hors référentiel français"),
    "MET-DOUBLON": (
        METIER,
        MAJEURE,
        "Numéro de facture déjà émis par le même émetteur",
    ),
    "MET-ECHEANCE": (METIER, MAJEURE, "Date d'échéance antérieure à l'émission"),
    "MET-MENTION": (METIER, MAJEURE, "Mention obligatoire absente (art. 242 nonies A CGI)"),
    "MET-TVA-CLE": (METIER, MAJEURE, "Clé du numéro de TVA intracommunautaire invalide"),
    "STA-MONTANT-ABERRANT": (
        STATISTIQUE,
        MINEURE,
        "Montant hors de la distribution du secteur d'activité",
    ),
}

# The rates the French VAT applies. Anything else is not illegal in itself -- an
# invoice can carry a foreign rate -- but on a domestic B2B flow it is a defect
# worth surfacing, and the Schematron will not say a word about it.
TAUX_TVA_FR = (20.0, 10.0, 5.5, 2.1, 0.0)

# What the generator is allowed to inject. FMT-XSD is absent on purpose: a document
# that fails the schema cannot be parsed, and the demo needs every invoice to reach
# silver in order to be counted.
INJECTABLE = (
    "FMT-SCHEMATRON",
    "REF-SIREN-INCONNU",
    "REF-EMETTEUR-CESSE",
    "MET-TOTAUX",
    "MET-TAUX-TVA",
    "MET-DOUBLON",
    "MET-ECHEANCE",
    "MET-MENTION",
    "MET-TVA-CLE",
    "STA-MONTANT-ABERRANT",
)


def famille(regle_id):
    return RULES[regle_id][0]


def gravite(regle_id):
    return RULES[regle_id][1]


def libelle(regle_id):
    return RULES[regle_id][2]


def as_rows():
    """The catalogue as rows, so gold can publish it as a dimension table."""
    return [
        {"regle_id": regle_id, "famille": f, "gravite": g, "libelle": lib}
        for regle_id, (f, g, lib) in sorted(RULES.items())
    ]
