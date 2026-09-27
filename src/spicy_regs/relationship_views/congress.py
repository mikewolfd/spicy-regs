"""Publisher-listed congressional relationships, without inferred list pairing."""

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
        context_columns=("meeting_status",),
    )


# Source-native types have spelling conventions that differ from bill-table keys.
# Only PN and the bill shapes evidenced by the held parser are routed. Treaty and
# amendment observations remain raw until positive source cases qualify a route.
VOTE_TYPE = value("type")
VOTE_CONGRESS = value("congress")
VOTE_NUMBER = value("number")
VOTE_BILL_TYPE = f"lower(replace({VOTE_TYPE}, '.', ''))"
VOTE_VALID = (
    f"e.type = 'OBJECT' AND regexp_full_match({VOTE_CONGRESS}, '[0-9]+') AND "
    f"(({VOTE_TYPE} = 'PN' AND regexp_full_match({VOTE_NUMBER}, '[0-9]+(-[0-9]+)?')) OR "
    f"({VOTE_BILL_TYPE} IN {BILL_TYPES} AND regexp_full_match({VOTE_NUMBER}, '[0-9]+')))"
)
VOTE_KIND = f"CASE WHEN {VOTE_TYPE} = 'PN' THEN 'nomination' ELSE 'bill' END"
VOTE_KEY = (
    f"CASE WHEN {VOTE_TYPE} = 'PN' THEN {VOTE_CONGRESS} || ':PN' || {VOTE_NUMBER} "
    f"ELSE {VOTE_CONGRESS} || '-' || {VOTE_BILL_TYPE} || '-' || {VOTE_NUMBER} END"
)

CONGRESS_RELATIONSHIPS = (
    ArrayRelationship(
        "bill_related_bills", "congress_bills", ("bill_id",), "related_bills_json", "bill",
        related_key, related_valid,
        "Directed publisher-listed related bills. Complete relationship details stay with each occurrence; "
        "neither reciprocal links nor text equivalence is inferred.",
        details=(("relationship_details_json", "CAST(json_extract(e.value, '$.relationship_details') AS VARCHAR)"),),
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
        "No positional relationship to amendment blocks, default Congress, or motion interpretation is inferred.",
        context_columns=("question", "source_url"),
        details=(("native_congress", VOTE_CONGRESS), ("native_type", VOTE_TYPE),
                 ("native_number", VOTE_NUMBER), ("native_name", value("name"))),
        target_kind_expression=f"CASE WHEN {VOTE_VALID} THEN {VOTE_KIND} ELSE 'unsupported_vote_document' END",
    ),
    ArrayRelationship(
        "vote_amendments", "roll_call_votes", ("vote_id",), "amendments_json", "amendment_observation",
        "NULL::VARCHAR", "e.type = 'OBJECT'",
        "Independent native amendment blocks, including empty identifier blocks. Target routing is unsupported "
        "until positive native amendment specimens qualify it; no pairing with document arrays.",
        context_columns=("question", "source_url"),
    ),
)
