"""Reviewed domain references whose complete lookup keys live in main rows.

Declarations preserve source observations and scope. They do not choose a first
or latest target, establish current records, or move keys into receipt lookups.
"""
from .explorer_navigation import array, guard, key, part, route

HASH = r"sha256:[a-f0-9]{64}"
ORDINAL = r"0|[1-9][0-9]*"
FILING = r"urn:fec:filing:official-fec:openfec-file-number:-?[1-9][0-9]*"


def _row(identifier, source, table, columns, fields, *, meaning, guards=(), patterns=None):
    patterns = patterns or {}
    return array(identifier, source, (), (
        route(table, columns, tuple(key(part(field, row=True), pattern=patterns.get(field, ".+"))
                                    for field in fields), *guards),
    ), mode="row", meaning=meaning)


def declarations():
    """Return explicit source-built references; unsupported candidate keys stay out."""
    specs = [
        _row("fec_audit_finding_matter", "fec_audit_findings", "fec_legal_matters",
             ("record_id", "matter_id"), ("matter_record_id", "matter_id"),
             patterns={"matter_record_id": HASH},
             meaning="The exact legal-matter observation supplying this audit category. An explicit no-findings row remains inspectable."),
        _row("fec_audit_finding_document", "fec_audit_findings", "fec_legal_documents",
             ("record_id", "matter_record_id"), ("supporting_document_record_id", "matter_record_id"),
             patterns={"supporting_document_record_id": HASH, "matter_record_id": HASH},
             meaning="The source-linked audit report in the same matter observation. Missing document references create no link; a document record does not prove a captured body."),
        _row("fec_recommendation_report", "fec_oversight_recommendations", "fec_agency_reports",
             ("report_id",), ("report_id",), patterns={"report_id": HASH},
             meaning="The exact report edition supplying this recommendation observation. Repeated status observations do not select a current recommendation."),
    ]
    for source, field, identifier in (
        ("fec_legal_documents", "parent_document_record_id", "fec_document_parent"),
        ("fec_legal_events", "related_document_record_id", "fec_event_document"),
        ("fec_legal_parties", "document_record_id", "fec_party_document"),
    ):
        specs.append(_row(identifier, source, "fec_legal_documents",
                          ("record_id", "matter_record_id"), (field, "matter_record_id"),
                          patterns={field: HASH, "matter_record_id": HASH},
                          meaning="The source-stated document observation within the exact retained matter observation. Optional absent references produce no link; repeated targets stay visible."))

    filing_guards = (
        guard("source_authority", row=True, values=("official-fec",)),
        guard("source_namespace", row=True, values=("fec-openfec-file-number",)),
    )
    specs.append(_row("fec_filing_reference_source", "fec_filing_links", "fec_filings",
                      ("record_id", "filing_key", "source_authority", "source_namespace"),
                      ("source_filing_record_id", "filing_key", "source_authority", "source_namespace"),
                      patterns={"source_filing_record_id": HASH, "filing_key": FILING},
                      guards=(*filing_guards, guard("filing_link_status", row=True, values=("native_file_number",))),
                      meaning="The exact source filing observation with its official native filing key and namespace. Lookup preserves duplicate observations and makes no amendment/current-record selection."))
    specs.append(_row("fec_filing_reference_target", "fec_filing_links", "fec_filings",
                      ("filing_key", "source_authority", "source_namespace"),
                      ("target_filing_key", "source_authority", "source_namespace"),
                      patterns={"target_filing_key": FILING},
                      guards=(*filing_guards, guard("target_resolution_status", row=True,
                                                    values=("resolved_native_filing_key",))),
                      meaning="All filing observations under the conflict-qualified official reference key. The main resolution decision must come from the maintained filing-reference policy; unresolved, missing and conflicting decisions create no link. No first or latest filing is chosen."))

    # Same native printing/element endpoint as legislative_diffs.endpoints,
    # with body agreement enforced directly in the target tuple. Unlike an
    # arbitrary min(body_version_id), exact lookup keeps all repeated matches.
    for side in ("from", "to"):
        specs.append(_row("section_diff_" + side + "_section", "section_diff_items", "bill_sections",
                          ("bill_id", "version_code", "printing_id", "element_id", "body_version_id"),
                          ("bill_id", side + "_version_code", side + "_printing_id",
                           side + "_element_id", side + "_body_version_id"),
                          meaning="This comparison endpoint's exact bill printing, element and derived body. Missing or different bodies do not match; every duplicate target remains visible as ambiguity."))
    specs.append(_row("law_section_parent", "law_sections", "law_sections", ("law_id", "seq"),
                      ("law_id", "parent_seq"), patterns={"parent_seq": ORDINAL},
                      meaning="The publisher's stated parent section position in the same law. Root/absent parents create no link. Quoted sections retain their source hierarchy and do not become enacted text."))
    action_guards = (guard("document_kind", row=True, values=("govinfo_package",)),)
    specs.append(_row("printed_committee_action_document", "bill_committee_actions", "house_activity_reports",
                      ("package_id", "body_version_id"), ("document_key", "body_version_id"),
                      guards=action_guards,
                      meaning="The committee activity report and exact derived body where this action wording was read. This source link does not certify the action-to-bill interpretation."))
    # bill_action_tables documents mention_span_start as the citation's own
    # span_start. Include its end, source kind, bill kind/key and body as well.
    literal_kind = {**part("", row=True), "literal": "bill_number"}
    specs.append(array("printed_committee_action_mention", "bill_committee_actions", (), (
        route("document_citations",
              ("document_kind", "document_key", "body_version_id", "cite_kind", "target_key", "span_start", "span_end"),
              (key(part("document_kind", row=True)), key(part("document_key", row=True)),
               key(part("body_version_id", row=True)), key(literal_kind), key(part("bill_id", row=True)),
               key(part("mention_span_start", row=True), pattern=ORDINAL),
               key(part("mention_span_end", row=True), pattern=ORDINAL)), *action_guards),
    ), mode="row", meaning="The exact bill-designator citation attached to the printed action, in the same source kind, document and body at its stated span. Multiple citations remain visible; this does not establish that the interpretation is correct."))
    # member_party_affiliations already has the complete bioguide_id/term_index
    # relationship in table_joins.JOINS; keep that single maintained definition.
    return specs
