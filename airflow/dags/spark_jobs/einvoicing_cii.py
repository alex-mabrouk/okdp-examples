"""
E-invoicing - Factur-X CII document builder
Writes one invoice as UN/CEFACT Cross Industry Invoice D22B, the XML half of a
Factur-X 1.09 document. Pure Python and no Spark, so it can be validated against
the XSD and the Schematrons of the standard outside a cluster.

Element order is not cosmetic here: the CII schema is a sequence, and a field in
the wrong place fails the XSD rather than the Schematron, with a message that
names the neighbour instead of the culprit. The order below is the schema's.

Profiles produced:
  EN16931           the socle of the French reform, and the bulk of the flow
  EXTENDED-CTC-FR   the optional French extension, on a minority of invoices;
                    it is what carries an invoicing agent and sub-lines
"""
from decimal import ROUND_HALF_UP, Decimal
from xml.sax.saxutils import escape

NS = {
    "rsm": "urn:un:unece:uncefact:data:standard:CrossIndustryInvoice:100",
    "ram": "urn:un:unece:uncefact:data:standard:ReusableAggregateBusinessInformationEntity:100",
    "udt": "urn:un:unece:uncefact:data:standard:UnqualifiedDataType:100",
    "qdt": "urn:un:unece:uncefact:data:standard:QualifiedDataType:100",
}

GUIDELINE = {
    "EN16931": "urn:cen.eu:en16931:2017",
    # The French extension is published under the Chorus Pro namespace, not the
    # Factur-X one: an EXTENDED document and an EXTENDED-CTC-FR document differ by
    # this URN before they differ by anything else.
    "EXTENDED-CTC-FR": (
        "urn:cen.eu:en16931:2017#conformant#urn.cpro.gouv.fr:1p0:extended-ctc-fr"
    ),
}

# 380 is a commercial invoice. The reform's other codes -- 381 credit note, 384
# corrected invoice -- are out of scope for a flow that only issues invoices.
INVOICE_TYPE_CODE = "380"
CURRENCY = "EUR"
COUNTRY = "FR"

# SIRET and SIREN as ISO 6523 scheme identifiers, which is how the French reform
# carries them, and VA for the intra-community VAT number.
SCHEME_SIRET = "0009"
SCHEME_SIREN = "0002"

CENT = Decimal("0.01")

# BT-22, the invoice note. The PDF says SPÉCIMEN across every page, but the XML is
# what a machine reads and what survives being extracted from the document: it
# states the same thing in the field the standard provides for it. AAI is the
# UNTDID 4451 code for general information.
NOTE_SYNTHETIQUE = (
    "Document synthétique généré pour une démonstration de la plateforme OKDP. "
    "Facture fictive, jamais émise. Les entreprises citées proviennent de la base "
    "SIRENE ouverte."
)
NOTE_SUBJECT_CODE = "AAI"


def money(value):
    return str(Decimal(value).quantize(CENT, rounding=ROUND_HALF_UP))


def rate(value):
    return str(Decimal(value).quantize(CENT, rounding=ROUND_HALF_UP))


def _tag(name, value, **attrs):
    rendered = "".join(f' {key}="{escape(str(val))}"' for key, val in attrs.items() if val)
    return f"<{name}{rendered}>{escape(str(value))}</{name}>"


def _address(party):
    """BR-09 makes the country code mandatory; the rest is optional but expected.

    An establishment with no BAN match keeps its commune alone, which stays a
    deliverable postal address in France.
    """
    lines = ["<ram:PostalTradeAddress>"]
    if party.get("code_postal"):
        lines.append(_tag("ram:PostcodeCode", party["code_postal"]))
    street = " ".join(
        str(part) for part in (party.get("numero_voie"), party.get("nom_voie")) if part
    )
    if street:
        lines.append(_tag("ram:LineOne", street))
    if party.get("libelle_commune"):
        lines.append(_tag("ram:CityName", party["libelle_commune"]))
    lines.append(_tag("ram:CountryID", COUNTRY))
    lines.append("</ram:PostalTradeAddress>")
    return lines


def _party(element, party, with_vat=True):
    """SellerTradeParty and BuyerTradeParty share their whole structure.

    BR-CO-26 wants at least one of the seller identifier, its legal registration
    or its VAT number: all three are present here, which is what a real French
    invoice carries anyway.
    """
    # BR-06 and BR-07 make the name mandatory, so its absence is either a defect of
    # ours or an anomaly somebody meant to inject, and the two must not look alike:
    #   None or missing  -- nobody decided this. Raise, rather than escape it into
    #                       the literal text "None" and ship a bogus invoice
    #   ""               -- deliberately dropped. Omit the element, which is what a
    #                       real defective invoice looks like and what BR-06 flags
    nom = party.get("nom")
    if nom is None:
        raise ValueError(f"{element}: no name for SIRET {party.get('siret')}")

    lines = [f"<{element}>"]
    if party.get("siret"):
        lines.append(_tag("ram:ID", party["siret"], schemeID=SCHEME_SIRET))
    if nom != "":
        lines.append(_tag("ram:Name", nom))
    if party.get("siren"):
        lines.append("<ram:SpecifiedLegalOrganization>")
        lines.append(_tag("ram:ID", party["siren"], schemeID=SCHEME_SIREN))
        lines.append("</ram:SpecifiedLegalOrganization>")
    lines.extend(_address(party))
    if with_vat and party.get("tva_intracom"):
        lines.append("<ram:SpecifiedTaxRegistration>")
        lines.append(_tag("ram:ID", party["tva_intracom"], schemeID="VA"))
        lines.append("</ram:SpecifiedTaxRegistration>")
    lines.append(f"</{element}>")
    return lines


def _line_item(index, line):
    """One invoice line, in schema order.

    A sub-line -- a kit component, priced inside its parent -- is what release
    1.09 added and what EXTENDED-CTC-FR uses; it is rendered as an ordinary line
    whose line id is the parent's, suffixed.
    """
    return [
        "<ram:IncludedSupplyChainTradeLineItem>",
        "<ram:AssociatedDocumentLineDocument>",
        _tag("ram:LineID", line.get("line_id", str(index))),
        "</ram:AssociatedDocumentLineDocument>",
        "<ram:SpecifiedTradeProduct>",
        _tag("ram:Name", line["designation"]),
        "</ram:SpecifiedTradeProduct>",
        "<ram:SpecifiedLineTradeAgreement>",
        "<ram:NetPriceProductTradePrice>",
        _tag("ram:ChargeAmount", money(line["prix_unitaire"])),
        "</ram:NetPriceProductTradePrice>",
        "</ram:SpecifiedLineTradeAgreement>",
        "<ram:SpecifiedLineTradeDelivery>",
        _tag("ram:BilledQuantity", line["quantite"], unitCode=line.get("unite", "C62")),
        "</ram:SpecifiedLineTradeDelivery>",
        "<ram:SpecifiedLineTradeSettlement>",
        "<ram:ApplicableTradeTax>",
        _tag("ram:TypeCode", "VAT"),
        _tag("ram:CategoryCode", line.get("categorie_tva", "S")),
        _tag("ram:RateApplicablePercent", rate(line["taux_tva"])),
        "</ram:ApplicableTradeTax>",
        "<ram:SpecifiedTradeSettlementLineMonetarySummation>",
        _tag("ram:LineTotalAmount", money(line["montant_ht"])),
        "</ram:SpecifiedTradeSettlementLineMonetarySummation>",
        "</ram:SpecifiedLineTradeSettlement>",
        "</ram:IncludedSupplyChainTradeLineItem>",
    ]


def _trade_tax(ventilation):
    lines = []
    for entry in ventilation:
        lines.extend(
            [
                "<ram:ApplicableTradeTax>",
                _tag("ram:CalculatedAmount", money(entry["montant_tva"])),
                _tag("ram:TypeCode", "VAT"),
                _tag("ram:BasisAmount", money(entry["montant_ht"])),
                _tag("ram:CategoryCode", entry.get("categorie_tva", "S")),
                _tag("ram:RateApplicablePercent", rate(entry["taux_tva"])),
                "</ram:ApplicableTradeTax>",
            ]
        )
    return lines


def build(invoice):
    """Render one invoice. `invoice` is the plain dict the generator builds.

    Nothing here validates: an invoice carrying an injected anomaly must render
    exactly as asked, or the anomaly would never reach the pipeline that is meant
    to catch it.
    """
    profile = invoice.get("profil", "EN16931")
    seller = invoice["fournisseur"]
    buyer = invoice["acheteur"]

    xml = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        "<rsm:CrossIndustryInvoice "
        + " ".join(f'xmlns:{prefix}="{uri}"' for prefix, uri in NS.items())
        + ">",
        "<rsm:ExchangedDocumentContext>",
        "<ram:GuidelineSpecifiedDocumentContextParameter>",
        _tag("ram:ID", GUIDELINE[profile]),
        "</ram:GuidelineSpecifiedDocumentContextParameter>",
        "</rsm:ExchangedDocumentContext>",
        "<rsm:ExchangedDocument>",
        _tag("ram:ID", invoice["numero"]),
        _tag("ram:TypeCode", INVOICE_TYPE_CODE),
        "<ram:IssueDateTime>",
        _tag("udt:DateTimeString", invoice["date_emission"], format="102"),
        "</ram:IssueDateTime>",
        # The schema sequence puts the note after the issue date, not before it.
        "<ram:IncludedNote>",
        _tag("ram:Content", invoice.get("note", NOTE_SYNTHETIQUE)),
        _tag("ram:SubjectCode", NOTE_SUBJECT_CODE),
        "</ram:IncludedNote>",
        "</rsm:ExchangedDocument>",
        "<rsm:SupplyChainTradeTransaction>",
    ]

    for index, line in enumerate(invoice["lignes"], start=1):
        xml.extend(_line_item(index, line))

    xml.append("<ram:ApplicableHeaderTradeAgreement>")
    if invoice.get("reference_acheteur"):
        xml.append(_tag("ram:BuyerReference", invoice["reference_acheteur"]))
    xml.extend(_party("ram:SellerTradeParty", seller))
    xml.extend(_party("ram:BuyerTradeParty", buyer))
    if invoice.get("mandataire"):
        # BT-FR-* : the invoicing agent is one of the actors EXTENDED-CTC-FR adds.
        xml.extend(_party("ram:SellerTaxRepresentativeTradeParty", invoice["mandataire"]))
    if invoice.get("bon_de_commande"):
        xml.append("<ram:BuyerOrderReferencedDocument>")
        xml.append(_tag("ram:IssuerAssignedID", invoice["bon_de_commande"]))
        xml.append("</ram:BuyerOrderReferencedDocument>")
    xml.append("</ram:ApplicableHeaderTradeAgreement>")

    # PEPPOL-EN16931-R008 rejects empty elements, and the schema still wants this
    # one, so it carries the delivery date (BT-72) a real invoice states anyway.
    xml.extend(
        [
            "<ram:ApplicableHeaderTradeDelivery>",
            "<ram:ActualDeliverySupplyChainEvent>",
            "<ram:OccurrenceDateTime>",
            _tag(
                "udt:DateTimeString",
                invoice.get("date_livraison", invoice["date_emission"]),
                format="102",
            ),
            "</ram:OccurrenceDateTime>",
            "</ram:ActualDeliverySupplyChainEvent>",
            "</ram:ApplicableHeaderTradeDelivery>",
        ]
    )

    xml.append("<ram:ApplicableHeaderTradeSettlement>")
    xml.append(_tag("ram:InvoiceCurrencyCode", CURRENCY))
    if invoice.get("iban"):
        xml.extend(
            [
                "<ram:SpecifiedTradeSettlementPaymentMeans>",
                _tag("ram:TypeCode", "30"),
                "<ram:PayeePartyCreditorFinancialAccount>",
                _tag("ram:IBANID", invoice["iban"]),
                "</ram:PayeePartyCreditorFinancialAccount>",
                "</ram:SpecifiedTradeSettlementPaymentMeans>",
            ]
        )
    xml.extend(_trade_tax(invoice["ventilation_tva"]))
    if invoice.get("date_echeance"):
        xml.extend(
            [
                "<ram:SpecifiedTradePaymentTerms>",
                "<ram:DueDateDateTime>",
                _tag("udt:DateTimeString", invoice["date_echeance"], format="102"),
                "</ram:DueDateDateTime>",
                "</ram:SpecifiedTradePaymentTerms>",
            ]
        )
    xml.extend(
        [
            "<ram:SpecifiedTradeSettlementHeaderMonetarySummation>",
            _tag("ram:LineTotalAmount", money(invoice["total_lignes"])),
            _tag("ram:TaxBasisTotalAmount", money(invoice["montant_ht"])),
            _tag("ram:TaxTotalAmount", money(invoice["montant_tva"]), currencyID=CURRENCY),
            _tag("ram:GrandTotalAmount", money(invoice["montant_ttc"])),
            _tag("ram:DuePayableAmount", money(invoice["montant_du"])),
            "</ram:SpecifiedTradeSettlementHeaderMonetarySummation>",
            "</ram:ApplicableHeaderTradeSettlement>",
            "</rsm:SupplyChainTradeTransaction>",
            "</rsm:CrossIndustryInvoice>",
        ]
    )
    return "\n".join(xml)
