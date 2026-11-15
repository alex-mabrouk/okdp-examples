"""
E-invoicing - running the official Schematrons
Validates a batch of CII documents against the artefacts published with Factur-X
1.09, and returns what failed.

Why this drives Saxon itself rather than calling the library's helper: those
Schematrons are XSLT 2.0/3.0, which lxml cannot run, so `facturx` delegates them
to a Saxon server over HTTP -- and its default is to log an unreachable server and
return success. Measured: four deliberately broken invoices all came back valid.
A validator that cannot fail is worse than none, so Saxon runs here, in the JVM
the image already carries.

It runs on batches, not documents. One JVM start costs about a second and one
compiled stylesheet serves the whole partition; per document it would cost more
than the validation itself.
"""
import os
import shutil
import subprocess
import tempfile

from lxml import etree

SVRL = "{http://purl.oclc.org/dsdl/svrl}"

SAXON_HOME = os.getenv("SAXON_HOME", "/opt/saxon")
SAXON_CP = f"{SAXON_HOME}/saxon-he.jar:{SAXON_HOME}/xmlresolver.jar"


def _artefacts_dir():
    import facturx

    return os.path.join(os.path.dirname(facturx.__file__), "xsd_and_schematron")


def schematrons():
    """The stylesheet to apply per profile.

    A document whose profile we do not recognise is checked against EN 16931, the
    socle every conformant profile extends: reporting the rules it breaks is more
    useful than reporting nothing.
    """
    base = _artefacts_dir()
    return {
        "EN16931": f"{base}/facturx-en16931/Factur-X_1.09_EN16931.xsl",
        "BASIC": f"{base}/facturx-basic/Factur-X_1.09_BASIC.xsl",
        "BASIC WL": f"{base}/facturx-basicwl/Factur-X_1.09_BASICWL.xsl",
        "MINIMUM": f"{base}/facturx-minimum/Factur-X_1.09_MINIMUM.xsl",
        "EXTENDED": f"{base}/facturx-extended/Factur-X_1.09_EXTENDED.xsl",
        "EXTENDED-CTC-FR": f"{base}/cii-extended-ctc-fr/EXTENDED-CTC-FR-CII.xslt",
    }


def _code(assertion):
    """The BR-* identifier the message opens with, e.g. "[BR-CO-15]-Invoice ...".

    The `id` attribute carries the Schematron's own numbering (FX-SCH-A-000121),
    which nobody reading a dashboard recognises; the bracketed code is the rule as
    the standard names it.
    """
    texte = "".join(assertion.itertext()).strip()
    if texte.startswith("[") and "]" in texte:
        return texte[1 : texte.index("]")]
    return assertion.get("id") or "?"


def _message(assertion):
    texte = " ".join("".join(assertion.itertext()).split())
    return texte[:400]


def _lire_svrl(chemin):
    rapport = etree.parse(chemin)
    return [
        {"code": _code(a), "message": _message(a)}
        for a in rapport.findall(f".//{SVRL}failed-assert")
    ]


def valider(documents):
    """Validate a batch.

    `documents` is an iterable of (cle, xml_bytes, profil). Returns
    {cle: [{"code": ..., "message": ...}]}, with an entry for every key handed in,
    empty when the document is conformant.
    """
    documents = list(documents)
    resultats = {cle: [] for cle, _, _ in documents}
    if not documents:
        return resultats

    feuilles = schematrons()
    par_profil = {}
    for index, (cle, xml, profil) in enumerate(documents):
        par_profil.setdefault(profil if profil in feuilles else "EN16931", []).append(
            (index, cle, xml)
        )

    racine = tempfile.mkdtemp(prefix="einvoicing-schematron-")
    try:
        for profil, lot in par_profil.items():
            entree = os.path.join(racine, f"in-{index_sain(profil)}")
            sortie = os.path.join(racine, f"out-{index_sain(profil)}")
            os.makedirs(entree, exist_ok=True)
            os.makedirs(sortie, exist_ok=True)

            noms = {}
            for index, cle, xml in lot:
                nom = f"{index}.xml"
                noms[nom] = cle
                with open(os.path.join(entree, nom), "wb") as fichier:
                    fichier.write(xml if isinstance(xml, bytes) else xml.encode("utf-8"))

            # Saxon takes a directory on both sides and keeps the file names, so one
            # JVM validates the whole batch.
            subprocess.run(
                [
                    "java",
                    "-cp",
                    SAXON_CP,
                    "net.sf.saxon.Transform",
                    f"-s:{entree}",
                    f"-xsl:{feuilles[profil]}",
                    f"-o:{sortie}",
                ],
                check=True,
                capture_output=True,
            )

            for nom, cle in noms.items():
                rapport = os.path.join(sortie, nom)
                if os.path.exists(rapport):
                    resultats[cle] = _lire_svrl(rapport)
    finally:
        shutil.rmtree(racine, ignore_errors=True)

    return resultats


def index_sain(profil):
    return profil.replace(" ", "-").lower()
