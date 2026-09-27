"""Literal regulatory and FCC memberships over retained arrays."""

from .core import ArrayRelationship
from .congress import SCALAR, scalar_valid, value


def strings(name: str, table: str, keys: tuple[str, ...], field: str, kind: str, meaning: str,
            pattern: str = ".+") -> ArrayRelationship:
    return ArrayRelationship(name, table, keys, field, kind, SCALAR, scalar_valid(pattern), meaning)


REGULATORY_RELATIONSHIPS = (
    ArrayRelationship(
        'house_communication_rins', 'house_communications', ('congress','communication_type','number'),
        'rin_occurrences_json', 'rin', value('rin'),
        f"e.type = 'OBJECT' AND regexp_full_match({value('rin')}, '[0-9]{{4}}-[A-Z0-9]{{4}}')",
        'Every qualified RIN finding retained on the communication, with original field digest and text spans. '
        'The communication source route and Record locators remain explicit; this view does not rerun extraction.',
        context_columns=('source_route','record_package_id','record_granule_id','update_date'),
        details=(('matched_text',value('matched_text')),('span_start',value('span_start')),
                 ('span_end',value('span_end')),('field_sha256',value('field_sha256')),
                 ('extraction_rule',value('rule')),('extraction_rule_version',value('rule_version')),
                 ('interpretation_ordinal',value('ordinal'))),
    ),
    strings("fcc_filing_proceedings", "fcc_filings", ("id_submission",), "proceeding_names_json",
            "fcc_proceeding", "FCC proceeding-name membership only. Native numeric proceeding IDs and participant "
            "roles discarded by an earlier shaper cannot be recovered from this held field."),
    strings("federal_register_rins", "federal_register", ("document_number", "publication_date"),
            "regulation_id_numbers_json", "rin", "Every native RIN, retaining the dated Federal Register key.",
            "[0-9]{4}-[A-Z0-9]{4}"),
    strings("federal_register_dockets", "federal_register", ("document_number", "publication_date"),
            "docket_ids_json", "docket_spelling", "Literal publisher docket strings, including historical mentions "
            "and SEC file numbers. No new normalizer runs here; target_key is a spelling, not a resolved docket ID."),
    strings("document_additional_rins", "documents", ("document_id",), "additional_rins", "rin",
            "Every additional RIN retained on a Regulations.gov document.", "[0-9]{4}-[A-Z0-9]{4}"),
    strings("proceeding_dockets", "proceedings", ("proceeding_id",), "docket_ids_json", "regulations_docket",
            "Dockets asserted by the selected proceeding builder output."),
    strings("proceeding_rins", "proceedings", ("proceeding_id",), "rins_json", "rin",
            "Every RIN retained by the proceeding builder; a shared RIN does not establish action identity.",
            "[0-9]{4}-[A-Z0-9]{4}"),
    strings("proceeding_federal_register", "proceedings", ("proceeding_id",), "fr_document_ids_json",
            "dated_federal_register", "Dated Register identities retained by the builder; no undated-number fallback."),
    strings("comment_period_proceedings", "comment_periods", ("comment_period_id",), "proceeding_ids_json",
            "proceeding", "Every proceeding associated with the derived comment period."),
    strings("comment_period_dockets", "comment_periods", ("comment_period_id",), "docket_ids_json",
            "regulations_docket", "Every docket anchor retained even when a period's proceeding is unresolved."),
    strings("comment_period_rins", "comment_periods", ("comment_period_id",), "rins_json", "rin",
            "Every RIN retained by the comment-period builder; no pairwise zipping with dockets or proceedings.",
            "[0-9]{4}-[A-Z0-9]{4}"),
)
