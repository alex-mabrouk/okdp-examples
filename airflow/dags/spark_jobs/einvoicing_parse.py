"""
E-invoicing - reading a CII document back
Turns one Cross Industry Invoice into the flat rows silver stores: a header, its
lines, and its VAT breakdown.

Everything here returns None rather than raising when a field is absent. That is
deliberate: this module reads invoices *sent by other people*, and an invoice
missing a mandatory field is the very thing the pipeline is meant to notice and
record. A parser that refused it would turn an anomaly to be reported into a job
that fails, and the anomaly would never reach a table.

Amounts come back as Decimal. Reading them as float would make an inconsistency of
one cent -- exactly what the totals check looks for -- indistinguishable from the
representation error of the check itself.
"""
from decimal import Decimal, InvalidOperation

from lxml import etree

NS = {
    "rsm": "urn:un:unece:uncefact:data:standard:CrossIndustryInvoice:100",
    "ram": "urn:un:unece:uncefact:data:standard:ReusableAggregateBusinessInformationEntity:100",
    "udt": "urn:un:unece:uncefact:data:standard:UnqualifiedDataType:100",
}

# The guideline URN each profile is published under. Anything else is a document
# whose rules we do not know, which is itself worth recording.
PROFILS = {
    "urn:cen.eu:en16931:2017": "EN16931",
    "urn:cen.eu:en16931:2017#compliant#urn:factur-x.eu:1p0:basic": "BASIC",
    "urn:factur-x.eu:1p0:basicwl": "BASIC WL",
    "urn:factur-x.eu:1p0:minimum": "MINIMUM",
    "urn:cen.eu:en16931:2017#conformant#urn:factur-x.eu:1p0:extended": "EXTENDED",
    "urn:cen.eu:en16931:2017#conformant#urn.cpro.gouv.fr:1p0:extended-ctc-fr": (
        "EXTENDED-CTC-FR"
    ),
}

DOCUMENT = "/rsm:CrossIndustryInvoice/rsm:ExchangedDocument"
TRANSACTION = "/rsm:CrossIndustryInvoice/rsm:SupplyChainTradeTransaction"
AGREEMENT = f"{TRANSACTION}/ram:ApplicableHeaderTradeAgreement"
SETTLEMENT = f"{TRANSACTION}/ram:ApplicableHeaderTradeSettlement"
SUMMATION = f"{SETTLEMENT}/ram:SpecifiedTradeSettlementHeaderMonetarySummation"


def _text(node, path):
    found = node.xpath(path, namespaces=NS)
    if not found:
        return None
    value = found[0].text
    return value.strip() if value and value.strip() else None


def _decimal(node, path):
    raw = _text(node, path)
    if raw is None:
        return None
    try:
        return Decimal(raw)
    except InvalidOperation:
        # A number that is not one is a defect to report, not a crash.
        return None


def _date(node, path):
    """CII dates are YYYYMMDD under format 102; anything else is left as it came."""
    raw = _text(node, path)
    if raw and len(raw) == 8 and raw.isdigit():
        return f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]}"
    return raw


def _partie(root, element):
    base = f"{AGREEMENT}/{element}"
    return {
        "siret": _text(root, f"{base}/ram:ID"),
        "siren": _text(root, f"{base}/ram:SpecifiedLegalOrganization/ram:ID"),
        "nom": _text(root, f"{base}/ram:Name"),
        "tva_intracom": _text(root, f"{base}/ram:SpecifiedTaxRegistration/ram:ID"),
        "code_postal": _text(root, f"{base}/ram:PostalTradeAddress/ram:PostcodeCode"),
        "libelle_commune": _text(root, f"{base}/ram:PostalTradeAddress/ram:CityName"),
        "pays": _text(root, f"{base}/ram:PostalTradeAddress/ram:CountryID"),
    }


def _lignes(root):
    lignes = []
    for index, item in enumerate(
        root.xpath(f"{TRANSACTION}/ram:IncludedSupplyChainTradeLineItem", namespaces=NS),
        start=1,
    ):
        quantite = item.xpath("ram:SpecifiedLineTradeDelivery/ram:BilledQuantity", namespaces=NS)
        lignes.append(
            {
                "numero_ligne": _text(item, "ram:AssociatedDocumentLineDocument/ram:LineID")
                or str(index),
                "designation": _text(item, "ram:SpecifiedTradeProduct/ram:Name"),
                "quantite": _decimal(
                    item, "ram:SpecifiedLineTradeDelivery/ram:BilledQuantity"
                ),
                "unite": quantite[0].get("unitCode") if quantite else None,
                "prix_unitaire": _decimal(
                    item,
                    "ram:SpecifiedLineTradeAgreement/ram:NetPriceProductTradePrice"
                    "/ram:ChargeAmount",
                ),
                "montant_ht": _decimal(
                    item,
                    "ram:SpecifiedLineTradeSettlement"
                    "/ram:SpecifiedTradeSettlementLineMonetarySummation"
                    "/ram:LineTotalAmount",
                ),
                "taux_tva": _decimal(
                    item,
                    "ram:SpecifiedLineTradeSettlement/ram:ApplicableTradeTax"
                    "/ram:RateApplicablePercent",
                ),
                "categorie_tva": _text(
                    item,
                    "ram:SpecifiedLineTradeSettlement/ram:ApplicableTradeTax"
                    "/ram:CategoryCode",
                ),
            }
        )
    return lignes


def _ventilation(root):
    entries = []
    for tax in root.xpath(f"{SETTLEMENT}/ram:ApplicableTradeTax", namespaces=NS):
        entries.append(
            {
                "taux_tva": _decimal(tax, "ram:RateApplicablePercent"),
                "montant_ht": _decimal(tax, "ram:BasisAmount"),
                "montant_tva": _decimal(tax, "ram:CalculatedAmount"),
                "categorie_tva": _text(tax, "ram:CategoryCode"),
            }
        )
    return entries


def parse(xml):
    """Read one CII document. Raises only when the bytes are not XML at all."""
    if isinstance(xml, str):
        xml = xml.encode("utf-8")
    root = etree.fromstring(xml)
    tree = root.getroottree()

    guideline = _text(
        tree,
        "/rsm:CrossIndustryInvoice/rsm:ExchangedDocumentContext"
        "/ram:GuidelineSpecifiedDocumentContextParameter/ram:ID",
    )

    return {
        "numero_facture": _text(tree, f"{DOCUMENT}/ram:ID"),
        "type_document": _text(tree, f"{DOCUMENT}/ram:TypeCode"),
        "guideline": guideline,
        # An unknown URN leaves the profile null, which is a reportable state and
        # not a reason to stop reading the rest of the document.
        "profil": PROFILS.get(guideline),
        "note": _text(tree, f"{DOCUMENT}/ram:IncludedNote/ram:Content"),
        "date_emission": _date(
            tree, f"{DOCUMENT}/ram:IssueDateTime/udt:DateTimeString"
        ),
        "date_livraison": _date(
            tree,
            f"{TRANSACTION}/ram:ApplicableHeaderTradeDelivery"
            "/ram:ActualDeliverySupplyChainEvent/ram:OccurrenceDateTime"
            "/udt:DateTimeString",
        ),
        "date_echeance": _date(
            tree,
            f"{SETTLEMENT}/ram:SpecifiedTradePaymentTerms/ram:DueDateDateTime"
            "/udt:DateTimeString",
        ),
        "reference_acheteur": _text(tree, f"{AGREEMENT}/ram:BuyerReference"),
        "bon_de_commande": _text(
            tree, f"{AGREEMENT}/ram:BuyerOrderReferencedDocument/ram:IssuerAssignedID"
        ),
        "devise": _text(tree, f"{SETTLEMENT}/ram:InvoiceCurrencyCode"),
        "iban": _text(
            tree,
            f"{SETTLEMENT}/ram:SpecifiedTradeSettlementPaymentMeans"
            "/ram:PayeePartyCreditorFinancialAccount/ram:IBANID",
        ),
        "fournisseur": _partie(tree, "ram:SellerTradeParty"),
        "acheteur": _partie(tree, "ram:BuyerTradeParty"),
        "total_lignes": _decimal(tree, f"{SUMMATION}/ram:LineTotalAmount"),
        "montant_ht": _decimal(tree, f"{SUMMATION}/ram:TaxBasisTotalAmount"),
        "montant_tva": _decimal(tree, f"{SUMMATION}/ram:TaxTotalAmount"),
        "montant_ttc": _decimal(tree, f"{SUMMATION}/ram:GrandTotalAmount"),
        "montant_du": _decimal(tree, f"{SUMMATION}/ram:DuePayableAmount"),
        "ventilation_tva": _ventilation(tree),
        "lignes": _lignes(tree),
    }
