"""Publisher-listed congressional relationships, without inferred list pairing."""

from dataclasses import replace

from spicy_regs.explorer_navigation import array_sql_relationships

from .core import ArrayRelationship
from .sql_views import SQLView


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
    'Ordered RIN findings on the communication. Repeats remain distinct.',
    context_columns=('update_date',),
)


# New relationship key rules have one owner, shared with public browser navigation.
NATIVE_CONGRESS_RELATIONSHIPS += array_sql_relationships()


ARCHIVE_RELATIONSHIP_VIEWS = (
    SQLView(
        name="archive_meeting_documents",
        required={
            "archive_list": (
                "list_row",
                "meeting_keys",
                "filename",
                "title",
                "print_number",
                "document_kind",
                "congress",
                "format",
                "document_date",
                "hidden",
            ),
            "archive_detail": (
                "list_row",
                "link_key",
                "package_id",
                "saved_suffix",
                "saved_media_type",
                "saved_bytes",
            ),
            "archive_meetings": ("event_id", "chamber", "title", "meeting_date", "congress", "outcome"),
        },
        query=lambda _publication: """
            WITH pairs AS (
                SELECT DISTINCT
                    list_row,
                    TRY_CAST(u.meeting_key AS BIGINT) AS event_id
                FROM (
                    SELECT list_row, UNNEST(meeting_keys) AS meeting_key
                    FROM archive_list
                ) AS u
                WHERE TRY_CAST(u.meeting_key AS BIGINT) IS NOT NULL
            )
            SELECT
                p.event_id,
                l.list_row,
                d.link_key,
                d.package_id,
                l.filename,
                l.title,
                l.print_number,
                l.document_kind,
                l.congress,
                l.format,
                l.document_date,
                l.hidden,
                d.saved_suffix,
                d.saved_media_type,
                d.saved_bytes,
                m.chamber AS meeting_chamber,
                m.title AS meeting_title,
                m.meeting_date,
                m.congress AS meeting_congress,
                m.outcome AS meeting_outcome
            FROM pairs AS p
            JOIN archive_list AS l ON l.list_row = p.list_row
            JOIN archive_detail AS d ON d.list_row = p.list_row
            LEFT JOIN archive_meetings AS m ON m.event_id = p.event_id
        """,
        meaning=(
            "Document editions paired cleanly with congressional meetings in the bound archive release via a single "
            "UNNEST(meeting_keys) and hash joins; distinct (event_id, list_row) pairs preserve published attribution "
            "without inferring witness ownership or complete hearing coverage."
        ),
        identity_columns=("event_id", "list_row"),
        rule_version="archive-navigation-v1",
        column_descriptions={
            "event_id": "Decimal congressional meeting event identifier extracted from archive_list.meeting_keys.",
            "meeting_chamber": "Chamber stated for the linked event in archive_meetings (House, Senate, or joint).",
            "meeting_title": "Meeting title from archive_meetings when known, distinct from document title.",
            "meeting_congress": "Congress stated in archive_meetings for the linked event, when known.",
            "meeting_outcome": "Date-specific hearing outcome in archive_meetings (held, canceled, or unknown).",
        },
    ),
    SQLView(
        name="archive_witness_documents",
        required={
            "archive_list": (
                "list_row",
                "witness_keys",
                "filename",
                "title",
                "document_kind",
                "congress",
                "format",
                "document_date",
                "hidden",
            ),
            "archive_detail": (
                "list_row",
                "link_key",
                "package_id",
                "saved_suffix",
                "saved_media_type",
                "saved_bytes",
            ),
            "archive_witnesses": (
                "witness_key",
                "person_key",
                "event_id",
                "source",
                "name",
                "rank",
                "credentials",
                "position",
                "organization",
                "witness_type",
                "superseded_by",
                "bioguide_id",
                "matched_bioguide_id",
                "listing_status",
            ),
            "archive_people": ("person_key", "name", "bioguide_id", "grouping_basis"),
            "archive_meetings": ("event_id", "chamber", "title", "meeting_date", "congress"),
        },
        query=lambda _publication: """
            WITH pairs AS (
                SELECT DISTINCT
                    list_row,
                    u.witness_key
                FROM (
                    SELECT list_row, UNNEST(witness_keys) AS witness_key
                    FROM archive_list
                ) AS u
                WHERE u.witness_key IS NOT NULL
            )
            SELECT
                p.witness_key,
                l.list_row,
                d.link_key,
                d.package_id,
                l.filename,
                l.title,
                l.document_kind,
                l.congress,
                l.format,
                l.document_date,
                l.hidden,
                d.saved_suffix,
                d.saved_media_type,
                d.saved_bytes,
                w.event_id,
                w.source AS witness_source,
                w.name AS witness_name,
                w.rank AS witness_rank,
                w.credentials AS witness_credentials,
                w.position AS witness_position,
                w.organization AS witness_organization,
                w.witness_type,
                w.superseded_by,
                w.bioguide_id AS witness_bioguide_id,
                w.matched_bioguide_id,
                w.listing_status,
                w.person_key,
                pe.name AS person_name,
                pe.bioguide_id AS person_bioguide_id,
                pe.grouping_basis AS person_grouping_basis,
                m.chamber AS meeting_chamber,
                m.title AS meeting_title,
                m.meeting_date,
                m.congress AS meeting_congress
            FROM pairs AS p
            JOIN archive_list AS l ON l.list_row = p.list_row
            JOIN archive_detail AS d ON d.list_row = p.list_row
            LEFT JOIN archive_witnesses AS w ON w.witness_key = p.witness_key
            LEFT JOIN archive_people AS pe ON pe.person_key = w.person_key
            LEFT JOIN archive_meetings AS m ON m.event_id = w.event_id
        """,
        meaning=(
            "Documents attached to witness appearances in the bound archive release via a single UNNEST(witness_keys) "
            "and hash joins to archive_witnesses, archive_detail, archive_people, and archive_meetings. Covers both "
            "House and Senate appearances (including senate_page and MODS occasions) without claiming completeness."
        ),
        identity_columns=("witness_key", "list_row"),
        rule_version="archive-navigation-v1",
        column_descriptions={
            "witness_key": "Published witness appearance key extracted from archive_list.witness_keys.",
            "witness_source": "Observation source family in archive_witnesses (house_xml, congress_gov, senate_page, or mods).",
            "witness_name": "Display name recorded on this witness appearance in archive_witnesses.",
            "witness_rank": "Military or uniformed-service rank stated on this witness appearance, when extracted.",
            "witness_credentials": "Post-nominal credentials stated on this witness appearance, when extracted.",
            "witness_position": "Title or position stated on this witness appearance.",
            "witness_organization": "Organization or affiliation stated on this witness appearance.",
            "witness_bioguide_id": "Publisher-stated Bioguide identifier on this witness appearance (never inferred).",
            "person_name": "Primary display name from the resolved archive_people record for this witness.",
            "person_bioguide_id": "Member Bioguide ID on the resolved archive_people record when grouping_basis is bioguide.",
            "person_grouping_basis": "How archive_people grouped this person (bioguide, name_organization, singleton, or reviewed).",
            "meeting_chamber": "Chamber stated in archive_meetings when event_id resolves to a meeting row.",
            "meeting_title": "Meeting title from archive_meetings when event_id resolves to a meeting row.",
            "meeting_congress": "Congress stated in archive_meetings when event_id resolves to a meeting row.",
        },
    ),
    SQLView(
        name="archive_witness_people",
        required={
            "archive_witnesses": (
                "witness_key",
                "occasion",
                "source",
                "event_id",
                "package_id",
                "page_url",
                "name",
                "rank",
                "credentials",
                "position",
                "organization",
                "on_behalf_of",
                "location",
                "panel",
                "panel_order",
                "display_order",
                "witness_type",
                "testified",
                "bioguide_id",
                "superseded_by",
                "matched_bioguide_id",
                "match_basis",
                "person_key",
                "listing_status",
            ),
            "archive_people": (
                "person_key",
                "bioguide_id",
                "name",
                "rank",
                "credentials",
                "organization",
                "grouping_basis",
                "appearance_count",
                "hearing_count",
                "first_seen_date",
                "last_seen_date",
            ),
            "archive_meetings": ("event_id", "chamber", "title", "meeting_date", "congress", "outcome"),
        },
        query=lambda _publication: """
            SELECT
                w.witness_key,
                w.occasion,
                w.source,
                w.event_id,
                w.package_id,
                w.page_url,
                w.name AS witness_name,
                w.rank AS witness_rank,
                w.credentials AS witness_credentials,
                w.position,
                w.organization AS witness_organization,
                w.on_behalf_of,
                w.location,
                w.panel,
                w.panel_order,
                w.display_order,
                w.witness_type,
                w.testified,
                w.bioguide_id AS witness_bioguide_id,
                w.superseded_by,
                w.matched_bioguide_id,
                w.match_basis,
                w.person_key,
                w.listing_status,
                pe.name AS person_name,
                pe.bioguide_id AS person_bioguide_id,
                pe.rank AS person_rank,
                pe.credentials AS person_credentials,
                pe.organization AS person_organization,
                pe.grouping_basis,
                pe.appearance_count,
                pe.hearing_count,
                pe.first_seen_date,
                pe.last_seen_date,
                m.chamber AS meeting_chamber,
                m.title AS meeting_title,
                m.meeting_date,
                m.congress AS meeting_congress,
                m.outcome AS meeting_outcome
            FROM archive_witnesses AS w
            LEFT JOIN archive_people AS pe ON pe.person_key = w.person_key
            LEFT JOIN archive_meetings AS m ON m.event_id = w.event_id
        """,
        meaning=(
            "Witness appearances hash-joined to resolved archive_people records and archive_meetings context. "
            "Retains both canonical (superseded_by IS NULL) and superseded rows across House and Senate sources."
        ),
        identity_columns=("witness_key",),
        rule_version="archive-navigation-v1",
        column_descriptions={
            "witness_name": "Display name recorded on this witness appearance in archive_witnesses.",
            "witness_rank": "Military or uniformed-service rank recorded on this witness appearance.",
            "witness_credentials": "Post-nominal credentials recorded on this witness appearance.",
            "witness_organization": "Organization recorded on this witness appearance.",
            "witness_bioguide_id": "Publisher-stated Bioguide identifier on this witness appearance (never inferred).",
            "person_name": "Primary display name from the resolved archive_people record.",
            "person_bioguide_id": "Member Bioguide identifier on the resolved archive_people record when grouped by Bioguide.",
            "person_rank": "Primary military/uniformed-service rank on the resolved archive_people record.",
            "person_credentials": "Primary credentials on the resolved archive_people record.",
            "person_organization": "Primary stated organization on the resolved archive_people record.",
            "meeting_chamber": "Chamber stated in archive_meetings for event_id when known.",
            "meeting_title": "Meeting title stated in archive_meetings for event_id when known.",
            "meeting_congress": "Congress stated in archive_meetings for event_id when known.",
            "meeting_outcome": "Date-specific hearing outcome stated in archive_meetings for event_id.",
        },
    ),
    SQLView(
        name="archive_witness_key_resolutions",
        required={
            "archive_witness_key_map": (
                "old_key",
                "witness_key",
                "status",
                "basis",
                "candidate_keys",
                "occasion",
                "source",
            ),
            "archive_witnesses": ("witness_key", "person_key", "event_id", "name", "organization", "superseded_by"),
            "archive_people": ("person_key", "name", "bioguide_id", "grouping_basis"),
        },
        query=lambda _publication: """
            SELECT
                m.old_key,
                m.witness_key,
                m.status,
                m.basis,
                m.candidate_keys,
                m.occasion,
                m.source,
                w.event_id,
                w.name AS witness_name,
                w.organization AS witness_organization,
                w.superseded_by,
                w.person_key,
                pe.name AS person_name,
                pe.bioguide_id AS person_bioguide_id,
                pe.grouping_basis AS person_grouping_basis
            FROM archive_witness_key_map AS m
            LEFT JOIN archive_witnesses AS w ON w.witness_key = m.witness_key
            LEFT JOIN archive_people AS pe ON pe.person_key = w.person_key
        """,
        meaning=(
            "Retired witness keys hash-joined to their unambiguous successor appearance and resolved archive_people "
            "record; retains ambiguous candidate_keys and unresolved statuses explicitly."
        ),
        identity_columns=("old_key",),
        rule_version="archive-navigation-v1",
        column_descriptions={
            "witness_name": "Display name on the successor archive_witnesses row when status is unambiguous.",
            "witness_organization": "Organization on the successor archive_witnesses row when status is unambiguous.",
            "person_name": "Primary display name on the resolved archive_people record for the successor witness.",
            "person_bioguide_id": "Member Bioguide ID on archive_people when grouping_basis is bioguide.",
            "person_grouping_basis": "Person grouping basis on archive_people for the successor witness.",
        },
    ),
    SQLView(
        name="archive_person_resolutions",
        required={
            "archive_person_aliases": (
                "old_person_key",
                "person_key",
                "status",
                "kind",
                "decision_id",
                "last_release_id",
                "candidate_keys",
            ),
            "archive_people": (
                "person_key",
                "bioguide_id",
                "name",
                "rank",
                "credentials",
                "organization",
                "grouping_basis",
                "appearance_count",
                "hearing_count",
            ),
        },
        query=lambda _publication: """
            SELECT
                a.old_person_key,
                a.person_key,
                a.status,
                a.kind,
                a.decision_id,
                a.last_release_id,
                a.candidate_keys,
                pe.bioguide_id,
                pe.name,
                pe.rank,
                pe.credentials,
                pe.organization,
                pe.grouping_basis,
                pe.appearance_count,
                pe.hearing_count
            FROM archive_person_aliases AS a
            LEFT JOIN archive_people AS pe ON pe.person_key = a.person_key
        """,
        meaning=(
            "Retired archive person keys hash-joined to their unambiguous successor record in archive_people; retains "
            "regrouped, merged, split, and hidden transitions alongside candidate_keys for ambiguous splits."
        ),
        identity_columns=("old_person_key",),
        rule_version="archive-navigation-v1",
    ),
)
