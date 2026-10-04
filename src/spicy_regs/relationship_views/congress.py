"""Publisher-listed congressional relationships, without inferred list pairing."""

from dataclasses import replace

from .core import ArrayRelationship


def value(path: str) -> str:
    return f"json_extract_string(e.value, '$.{path}')"


def scalar_valid(pattern: str = ".+") -> str:
    return f"e.type = 'VARCHAR' AND regexp_full_match(json_extract_string(e.value, '$'), '{pattern}')"


SCALAR = "json_extract_string(e.value, '$')"
BILL_TYPES = "('hr','s','hres','sres','hjres','sjres','hconres','sconres')"
BILL_PATTERN = "[0-9]+-(hr|s|hres|sres|hjres|sjres|hconres|sconres)-[0-9]+"
related_key = f"{value('congress')} || '-' || lower({value('bill_type')}) || '-' || {value('number')}"
related_valid = (
    f"e.type = 'OBJECT' AND regexp_full_match({value('congress')}, '[0-9]+') "
    f"AND lower({value('bill_type')}) IN {BILL_TYPES} AND regexp_full_match({value('number')}, '[0-9]+')"
)
MEETING_KEYS = ("congress", "chamber", "event_id")


def meeting(name: str, field: str, kind: str, expr: str, valid: str, meaning: str) -> ArrayRelationship:
    return ArrayRelationship(
        name, "committee_meetings", MEETING_KEYS, field, kind, expr, valid, meaning,
        context_columns=("meeting_status",), detail_read_column="detail_read",
    )


# Source-native positive fixtures qualify PN, bill, Treaty Doc. N-N and
# Senate S.Amdt. N shapes. The treaty number states its Congress independently
# of the vote context; amendment blocks borrow only their own vote's Congress.
VOTE_TYPE = value("type")
VOTE_CONGRESS = value("congress")
VOTE_NUMBER = value("number")
VOTE_BILL_TYPE = f"lower(replace({VOTE_TYPE}, '.', ''))"
TREATY_VALID = f"{VOTE_TYPE} = 'Treaty Doc.' AND regexp_full_match({VOTE_NUMBER}, '[0-9]+-[0-9]+')"
VOTE_VALID = (
    f"e.type = 'OBJECT' AND (({TREATY_VALID}) OR (regexp_full_match({VOTE_CONGRESS}, '[0-9]+') AND "
    f"(({VOTE_TYPE} = 'PN' AND regexp_full_match({VOTE_NUMBER}, '[0-9]+(-[0-9]+)?')) OR "
    f"({VOTE_BILL_TYPE} IN {BILL_TYPES} AND regexp_full_match({VOTE_NUMBER}, '[0-9]+')))))"
)
VOTE_KIND = f"CASE WHEN {VOTE_TYPE} = 'PN' THEN 'nomination' WHEN {TREATY_VALID} THEN 'treaty' ELSE 'bill' END"
VOTE_KEY = (
    f"CASE WHEN {VOTE_TYPE} = 'PN' THEN {VOTE_CONGRESS} || ':PN' || {VOTE_NUMBER} "
    f"WHEN {TREATY_VALID} THEN {VOTE_NUMBER} "
    f"ELSE {VOTE_CONGRESS} || '-' || {VOTE_BILL_TYPE} || '-' || {VOTE_NUMBER} END"
)
AMENDMENT_VALID = (
    f"e.type = 'OBJECT' AND s.chamber='senate' AND regexp_full_match(s.congress,'[0-9]+') "
    f"AND regexp_full_match(s.vote_id,s.congress||'-senate-[12]-[0-9]+') "
    f"AND regexp_full_match({value('number')}, 'S\\.Amdt\\. [0-9]+')"
)
AMENDMENT_KEY = f"CASE WHEN {AMENDMENT_VALID} THEN s.congress||'-samdt-'||split_part({value('number')},' ',2) END"

CONGRESS_RELATIONSHIPS = (
    ArrayRelationship(
        "bill_related_bills", "congress_bills", ("bill_id",), "related_bills_json", "bill",
        related_key, related_valid,
        "Directed publisher-listed related bills. Complete relationship details stay with each occurrence; "
        "neither reciprocal links nor text equivalence is inferred.",
        details=(("relationship_details_json", "CAST(json_extract(e.value, '$.relationship_details') AS VARCHAR)",
                  "JSON list of the publisher's relationship details (type, identifiedBy) for this related bill."),),
    ),
    ArrayRelationship(
        "member_fec_ids", "members", ("bioguide_id",), "fec_ids_json", "fec_candidate", SCALAR,
        scalar_valid("[HSP][0-9][A-Z0-9]{7}"),
        "Candidate IDs asserted by the captured community legislator crosswalk. No cycle, committee authorization, "
        "donation or official person adjudication is inferred.",
        context_columns=("observed_at", "roster"),
    ),
    meeting("meeting_bills", "bill_ids_json", "bill", SCALAR, scalar_valid(BILL_PATTERN),
            "Meeting-related bills, independently expanded under the complete meeting key."),
    meeting("meeting_hearing_jackets", "hearing_jackets_json", "hearing_jacket",
            f"s.congress || ':' || s.chamber || ':' || {SCALAR}", scalar_valid("[0-9]+"),
            "Hearing jacket references scoped by Congress and chamber; not paired with meeting bills."),
    meeting("meeting_committees", "committees_json", "committee",
            f"s.congress || ':' || {value('systemCode')}",
            f"e.type = 'OBJECT' AND regexp_full_match({value('systemCode')}, '[A-Za-z0-9]+')",
            "Publisher committee objects; target key retains Congress and native systemCode."),
    meeting("meeting_documents", "document_urls_json", "offered_url", SCALAR,
            scalar_valid("https?://.+"),
            "Offered document URLs in the held source order. URLs do not prove acquisition; no witness pairing."),
    meeting("meeting_witness_documents", "witness_documents_json", "offered_url", value("url"),
            f"e.type = 'OBJECT' AND regexp_full_match({value('url')}, 'https?://.+')",
            "Native offered witness-document objects with every role/format field retained; no witness pairing."),
    meeting("meeting_publications", "meeting_documents_json", "offered_url", value("url"),
            f"e.type = 'OBJECT' AND regexp_full_match({value('url')}, 'https?://.+')",
            "Native offered meeting-document objects with every role/format field retained; no body acquisition claim."),
    meeting("meeting_witnesses", "witnesses_json", "witness_observation", "NULL::VARCHAR", "e.type = 'OBJECT'",
            "Native witness objects with ordinals. Names alone create no person identity or document edge."),
    ArrayRelationship(
        "vote_documents", "roll_call_votes", ("vote_id",), "documents_json", "vote_document",
        VOTE_KEY, VOTE_VALID,
        "Independent native Senate document references. PN suffixes and native Congress are retained. "
        "Treaty Doc. N-N retains its own received Congress, independently of the vote Congress. "
        "No positional relationship to amendment blocks, default Congress, or motion interpretation is inferred.",
        rule_version="vote-native-routing/2",
        context_columns=("question", "source_url"),
        details=(("native_congress", VOTE_CONGRESS, "The Congress stated inside the document block, as held."),
                 ("native_type", VOTE_TYPE, "The document type as held: PN, Treaty Doc., or a bill type."),
                 ("native_number", VOTE_NUMBER, "The document number as held, including a PN suffix."),
                 ("native_name", value("name"), "The document name as held.")),
        target_kind_expression=f"CASE WHEN {VOTE_VALID} THEN {VOTE_KIND} ELSE 'unsupported_vote_document' END",
    ),
    ArrayRelationship(
        "vote_amendments", "roll_call_votes", ("vote_id",), "amendments_json", "amendment_observation",
        AMENDMENT_KEY, "e.type = 'OBJECT'",
        "Independent native amendment blocks, including empty identifiers. Qualified Senate S.Amdt. N "
        "uses the source vote Congress and chamber; it never borrows a document block or current Congress.",
        context_columns=("congress", "chamber", "question", "source_url"),
        target_kind_expression=f"CASE WHEN {AMENDMENT_VALID} THEN 'amendment' ELSE 'amendment_observation' END",
        rule_version="vote-native-routing/2",
    ),
)


# Explicit migration selection for the integration registry. The shared view
# implementation converts a native list to JSON only while expanding it; the
# stored source remains a declared Arrow list/struct. Processing context is read
# from receipts by qualified internal callers, never invented on these views.
NATIVE_CONGRESS_RELATIONSHIPS = tuple(
    replace(
        spec,
        source_field=spec.source_field.removesuffix("_json"),
        detail_read_column=None,
        context_columns=tuple(c for c in spec.context_columns if c not in {"observed_at", "roster", "source_url"}),
        details=(("relationship_details", "s.related_bills[CAST(e.key AS BIGINT)+1].relationship_details",
                  "Publisher relationship details in native list order; types and identifying source remain as stated."),)
                if spec.name == "bill_related_bills" else spec.details,
    )
    for spec in CONGRESS_RELATIONSHIPS
)

NATIVE_COMMUNICATION_RINS = ArrayRelationship(
    'house_communication_rins', 'house_communications', ('congress', 'communication_type', 'number'),
    'rins', 'rin', SCALAR, scalar_valid('[0-9]{4}-[A-Z0-9]{4}'),
    'Ordered RIN findings on the communication. Repeats remain distinct; original field spans, '
    'digests and extraction rules are retained in the matching ETL receipt.',
    context_columns=('update_date',),
)
