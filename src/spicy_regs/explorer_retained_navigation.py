"""Explicit main-row relationships in retained publisher collections.

The canonical explorer registry consumes these declarations. Content-bound
observation IDs keep source versions separate; incomplete native keys return
all selected observations rather than claiming an exact capture parent.
"""

from spicy_regs.explorer_navigation import array, guard, key, part, route


DIGEST = r"sha256:[a-f0-9]{64}"
POSITIVE = r"[1-9][0-9]*"
POSITION = r"(?:0|[1-9][0-9]*)"
ACCESSION = r"[0-9]{8}-[0-9]{3,5}"
FILE_GUID = r"[0-9A-Fa-f]{8}(?:-[0-9A-Fa-f]{4}){3}-[0-9A-Fa-f]{12}"


def _row(identifier, source, target, components, meaning):
    """Declare every source/target key component; never infer a column match."""
    return array(
        identifier, source, (),
        (route(target, tuple(column for column, _, _ in components),
               tuple(key(part(field, row=True), pattern=pattern) for _, field, pattern in components)),),
        mode="row", meaning=meaning,
    )


def declarations():
    """Portable main-key routes, with no receipt-derived navigation inputs."""
    specs = [
        _row("administration_policy_statement", "administration_policy_bill_links",
             "administration_policy_statements", (("statement_id", "statement_id", DIGEST),),
             "The exact retained statement that lists this bill, including repeated bill mentions."),
        _row("administration_policy_bill", "administration_policy_bill_links", "congress_bills",
             (("bill_id", "bill_id", r"[1-9][0-9]*-(?:hr|s|hres|sres|hjres|sjres|hconres|sconres)-[1-9][0-9]*"),),
             "The bill explicitly listed in the archived metadata; no support or opposition is inferred."),
        _row("sec_file_rulemaking", "sec_comment_files", "sec_rulemakings",
             (("rulemaking_id", "rulemaking_id", DIGEST),),
             "The exact retained rulemaking observation that offered this comment file."),
    ]
    for source, identifier in (("cftc_comments", "cftc_comment_rule"),
                               ("cftc_comment_files", "cftc_file_rule")):
        specs.append(_row(identifier, source, "cftc_rule_observations",
                          (("observation_id", "rule_observation_id", DIGEST),
                           ("rule_id", "rule_id", POSITIVE)),
                          "The exact retained rule listing and its native rule ID."))
    specs.append(_row("cftc_file_comment", "cftc_comment_files", "cftc_comments",
                      (("rule_observation_id", "rule_observation_id", DIGEST),
                       ("rule_id", "rule_id", POSITIVE), ("comment_id", "comment_id", POSITIVE)),
                      "The declaring comment within this retained rule listing; other captures stay separate."))
    for source, identifier in (("edis_attachments", "edis_attachment_document"),
                               ("edis_attachment_files", "edis_file_document")):
        specs.append(_row(identifier, source, "edis_documents",
                          (("observation_id", "document_observation_id", DIGEST),
                           ("document_id", "document_id", POSITIVE)),
                          "The exact retained document observation and its native EDIS document ID."))
    specs.append(_row("edis_file_attachment", "edis_attachment_files", "edis_attachments",
                      (("document_observation_id", "document_observation_id", DIGEST),
                       ("document_id", "document_id", POSITIVE), ("attachment_id", "attachment_id", POSITIVE)),
                      "Retained attachment metadata for this document observation and attachment ID. "
                      "Multiple metadata captures remain separate; no exact metadata version is selected."))
    # Main rows do not name a search capture or a metadata-capture digest.
    # These are literal native-identity collections, not capture-parent claims.
    specs += [
        _row("ferc_file_search_observations", "ferc_accession_files", "ferc_search_documents",
             (("accession", "accession", ACCESSION),),
             "All selected search observations stating this accession; no exact search capture is implied."),
        _row("ferc_original_file_observations", "ferc_original_files", "ferc_accession_files",
             (("accession", "accession", ACCESSION), ("file_guid", "file_guid", FILE_GUID)),
             "All retained metadata observations for this accession and file GUID; capture versions stay separate."),
        _row("supreme_court_opinion_index", "supreme_court_opinion_observations",
             "supreme_court_index_observations", (("observation_id", "index_observation_id", DIGEST),),
             "The exact retained term-index render that states this opinion occurrence."),
    ]
    specs.append(array(
        "supreme_court_pdf_declarations", "supreme_court_pdf_observations", ("declaring_occurrences_json",),
        (route("supreme_court_opinion_observations", ("index_observation_id", "opinion_index"),
               (key(part("index_observation_id"), pattern=DIGEST),
                key(part("opinion_index"), pattern=POSITION)),
               guard("role", values=("opinion", "revision"))),),
        meaning="Each opinion or revision occurrence offering this retained PDF. "
                "Repeated declarations and different index renders stay separate.",
    ))
    specs.append(_row("ecfr_content_snapshot", "ecfr_snapshot_sections", "ecfr_snapshots",
                      (("snapshot_id", "snapshot_id", DIGEST),),
                      "The exact dated eCFR selection containing this source node."))
    for source, identifier in (("uscode_edition_sections", "uscode_content_edition_member"),
                               ("uscode_edition_structure", "uscode_structure_edition_member")):
        specs.append(_row(identifier, source, "uscode_editions", (("edition_id", "edition_id", DIGEST),),
                          "The exact retained archive member and edition observation, including carried appendices."))
    return specs
