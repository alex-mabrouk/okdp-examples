"""
E-invoicing - the readable half of a Factur-X document
Renders one invoice as a PDF a human reads, then embeds its CII XML into it, which
is what makes the result a Factur-X file rather than a PDF next to an XML.

A Factur-X document is one file carrying both halves: the PDF is what a person
opens, the embedded XML is what a machine reads, and they are meant to say the
same thing. This module renders the PDF from the very dict the XML was built from,
so they cannot disagree.

The invoices are synthetic, and the diagonal SPÉCIMEN watermark is what says so on
every page -- the convention a French reader already knows, and the only marking
the document carries.
"""
import io
import os

import reportlab
from facturx import generate_from_binary
from PIL import ImageCms
from pypdf import PdfReader, PdfWriter
from pypdf.generic import (
    ArrayObject,
    DecodedStreamObject,
    DictionaryObject,
    NameObject,
    NumberObject,
    create_string_object,
)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

# PDF/A forbids relying on the fourteen standard PDF fonts: every font used has to
# be embedded in the file. Helvetica is not embeddable, so the document is set in
# Bitstream Vera, which ships with reportlab under a permissive licence and needs
# no extra file in the image.
_FONT_DIR = os.path.join(os.path.dirname(reportlab.__file__), "fonts")
POLICE = "Vera"
POLICE_GRASSE = "Vera-Bold"


def _enregistrer_polices():
    if POLICE in pdfmetrics.getRegisteredFontNames():
        return
    pdfmetrics.registerFont(TTFont(POLICE, os.path.join(_FONT_DIR, "Vera.ttf")))
    pdfmetrics.registerFont(TTFont(POLICE_GRASSE, os.path.join(_FONT_DIR, "VeraBd.ttf")))
    pdfmetrics.registerFontFamily(POLICE, normal=POLICE, bold=POLICE_GRASSE)


ARDOISE = colors.HexColor("#1f2933")
GRIS = colors.HexColor("#e4e7eb")
GRIS_TEXTE = colors.HexColor("#616e7c")


def _styles():
    _enregistrer_polices()
    base = getSampleStyleSheet()
    return {
        "titre": ParagraphStyle(
            "titre",
            parent=base["Title"],
            fontName=POLICE_GRASSE,
            fontSize=20,
            textColor=ARDOISE,
            alignment=0,
        ),
        "normal": ParagraphStyle(
            "n", parent=base["Normal"], fontName=POLICE, fontSize=9, leading=12
        ),
        "petit": ParagraphStyle(
            "p",
            parent=base["Normal"],
            fontName=POLICE,
            fontSize=7.5,
            leading=10,
            textColor=GRIS_TEXTE,
        ),
    }


def _date(compact):
    """Dates travel as YYYYMMDD in CII and are read as DD/MM/YYYY in France."""
    if not compact:
        return "—"
    return f"{compact[6:8]}/{compact[4:6]}/{compact[0:4]}"


def _euro(value):
    entier = f"{float(value):,.2f}".replace(",", " ").replace(".", ",")
    return f"{entier} €"


def _bloc_partie(party, role, styles):
    lignes = [f"<b>{role}</b>", party.get("nom") or "—"]
    voie = " ".join(
        str(part) for part in (party.get("numero_voie"), party.get("nom_voie")) if part
    )
    if voie:
        lignes.append(voie)
    lignes.append(
        " ".join(
            str(part)
            for part in (party.get("code_postal"), party.get("libelle_commune"))
            if part
        )
    )
    if party.get("siret"):
        lignes.append(f"SIRET {party['siret']}")
    if party.get("tva_intracom"):
        lignes.append(f"TVA {party['tva_intracom']}")
    return Paragraph("<br/>".join(lignes), styles["normal"])


def _tableau_lignes(invoice, styles):
    data = [["#", "Désignation", "Qté", "P.U. HT", "TVA", "Montant HT"]]
    for index, ligne in enumerate(invoice["lignes"], start=1):
        data.append(
            [
                str(index),
                Paragraph(ligne["designation"], styles["normal"]),
                str(ligne["quantite"]),
                _euro(ligne["prix_unitaire"]),
                f"{float(ligne['taux_tva']):.1f} %".replace(".", ","),
                _euro(ligne["montant_ht"]),
            ]
        )
    table = Table(data, colWidths=[10 * mm, 78 * mm, 14 * mm, 26 * mm, 18 * mm, 28 * mm])
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), ARDOISE),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, -1), POLICE),
                ("FONTNAME", (0, 0), (-1, 0), POLICE_GRASSE),
                ("FONTSIZE", (0, 0), (-1, -1), 8.5),
                ("ALIGN", (2, 0), (-1, -1), "RIGHT"),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f5f7fa")]),
                ("GRID", (0, 0), (-1, -1), 0.4, GRIS),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    return table


def _tableau_totaux(invoice):
    rows = [["Total HT", _euro(invoice["montant_ht"])]]
    for entry in invoice["ventilation_tva"]:
        taux = f"{float(entry['taux_tva']):.1f} %".replace(".", ",")
        rows.append([f"TVA {taux} sur {_euro(entry['montant_ht'])}", _euro(entry["montant_tva"])])
    rows.append(["Total TVA", _euro(invoice["montant_tva"])])
    rows.append(["Total TTC", _euro(invoice["montant_ttc"])])
    rows.append(["Net à payer", _euro(invoice["montant_du"])])

    table = Table(rows, colWidths=[62 * mm, 32 * mm], hAlign="RIGHT")
    table.setStyle(
        TableStyle(
            [
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("ALIGN", (1, 0), (1, -1), "RIGHT"),
                ("LINEABOVE", (0, -2), (-1, -2), 0.8, ARDOISE),
                ("FONTNAME", (0, 0), (-1, -1), POLICE),
                ("FONTNAME", (0, -1), (-1, -1), POLICE_GRASSE),
                ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#f5f7fa")),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ]
        )
    )
    return table


def _filigrane(canvas, doc):
    canvas.saveState()
    _enregistrer_polices()
    canvas.setFont(POLICE_GRASSE, 30)
    canvas.setFillColor(colors.HexColor("#f0d5c4"))
    canvas.translate(A4[0] / 2, A4[1] / 2)
    canvas.rotate(38)
    canvas.drawCentredString(0, 0, "SPÉCIMEN")
    canvas.restoreState()


def _srgb_icc():
    """The sRGB profile PDF/A wants, built rather than shipped.

    Pillow can synthesise one through littlecms, which spares the image an ICC
    file and a package that would only carry it.
    """
    return ImageCms.ImageCmsProfile(ImageCms.createProfile("sRGB")).tobytes()


def _avec_output_intent(pdf_bytes):
    """Declare the colour space the page is rendered in.

    Without it veraPDF rejects the file: PDF/A forbids DeviceRGB unless the
    document says which RGB it means. This is the last of the two things that
    separate a PDF carrying an attachment from a conforming PDF/A-3.
    """
    writer = PdfWriter(clone_from=PdfReader(io.BytesIO(pdf_bytes)))

    profil = DecodedStreamObject()
    profil.set_data(_srgb_icc())
    profil[NameObject("/N")] = NumberObject(3)

    intent = DictionaryObject(
        {
            NameObject("/Type"): NameObject("/OutputIntent"),
            NameObject("/S"): NameObject("/GTS_PDFA1"),
            NameObject("/OutputConditionIdentifier"): create_string_object("sRGB"),
            NameObject("/Info"): create_string_object("sRGB IEC61966-2.1"),
            NameObject("/DestOutputProfile"): writer._add_object(profil),
        }
    )
    writer._root_object[NameObject("/OutputIntents")] = ArrayObject(
        [writer._add_object(intent)]
    )

    sortie = io.BytesIO()
    writer.write(sortie)
    return sortie.getvalue()


def render_pdf(invoice):
    """The PDF alone, without the XML in it yet."""
    styles = _styles()
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=18 * mm,
        rightMargin=18 * mm,
        topMargin=16 * mm,
        bottomMargin=20 * mm,
        title=f"Facture {invoice['numero']}",
    )

    profil = invoice.get("profil", "EN16931")
    entete = Table(
        [
            [
                Paragraph(f"FACTURE<br/><font size=10>{invoice['numero']}</font>", styles["titre"]),
                Paragraph(
                    f"Émise le <b>{_date(invoice['date_emission'])}</b><br/>"
                    f"Échéance {_date(invoice.get('date_echeance'))}<br/>"
                    f"<font size=7.5>Factur-X 1.09 · profil {profil}</font>",
                    styles["normal"],
                ),
            ]
        ],
        colWidths=[100 * mm, 74 * mm],
    )
    entete.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))

    parties = Table(
        [
            [
                _bloc_partie(invoice["fournisseur"], "ÉMETTEUR", styles),
                _bloc_partie(invoice["acheteur"], "DESTINATAIRE", styles),
            ]
        ],
        colWidths=[87 * mm, 87 * mm],
    )
    parties.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f5f7fa")),
                ("BOX", (0, 0), (-1, -1), 0.4, GRIS),
                ("INNERGRID", (0, 0), (-1, -1), 0.4, GRIS),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
            ]
        )
    )

    references = []
    if invoice.get("reference_acheteur"):
        references.append(f"Référence acheteur : {invoice['reference_acheteur']}")
    if invoice.get("bon_de_commande"):
        references.append(f"Bon de commande : {invoice['bon_de_commande']}")
    if invoice.get("iban"):
        references.append(f"IBAN : {invoice['iban']}")

    story = [
        entete,
        Spacer(1, 8),
        parties,
        Spacer(1, 6),
        Paragraph(" &nbsp;·&nbsp; ".join(references), styles["petit"]),
        Spacer(1, 8),
        _tableau_lignes(invoice, styles),
        Spacer(1, 8),
        _tableau_totaux(invoice),
        Spacer(1, 10),
        Paragraph(
            "TVA acquittée sur les débits. En cas de retard de paiement, application "
            "d'intérêts au taux légal et d'une indemnité forfaitaire de recouvrement "
            "de 40 € (art. L441-10 et D441-5 du code de commerce).",
            styles["petit"],
        ),
    ]

    doc.build(story, onFirstPage=_filigrane, onLaterPages=_filigrane)
    return _avec_output_intent(buffer.getvalue())


def _metadata(invoice):
    """The document properties, stated rather than left to be rediscovered.

    Left to itself the library reads them back out of the XML, and takes the first
    seller name it finds -- which raises on an invoice whose issuer name was
    dropped on purpose. We already hold every value it would look for.
    """
    emetteur = invoice["fournisseur"].get("nom") or "Émetteur non renseigné"
    numero = invoice["numero"]
    return {
        "author": emetteur,
        "keywords": "Facture, Factur-X",
        "title": f"{emetteur}: Facture {numero}",
        "subject": (
            f"Facture {numero} du {_date(invoice['date_emission'])} émise par {emetteur}"
        ),
    }


def render_facturx(invoice, xml, check_xsd=False):
    """The PDF with the CII XML embedded: one Factur-X file.

    `check_xsd` is off by default. Validating five hundred documents that one
    generator just produced from one template proves nothing five times over, and
    it reparses a large schema each call; silver validates every invoice of the
    flow against the XSD *and* the official Schematrons, which is the check that
    counts. Turn it on when rendering a document on its own.
    """
    return generate_from_binary(
        render_pdf(invoice),
        xml if isinstance(xml, bytes) else xml.encode("utf-8"),
        check_xsd=check_xsd,
        pdf_metadata=_metadata(invoice),
    )
