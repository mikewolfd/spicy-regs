"""Round-5 chaos repairs at the server boundary: citation order and paging, read statuses, kind scopes, published_at.

Evidence: corpora/mcp-chaos-2026-10-02/round5/phase2-server.md (S5-1..S5-4, freshness) and phase3-review.md (the
recorded decisions: three read statuses from existing parent columns; meaning text moved into the artifacts).
"""

from __future__ import annotations

import asyncio
import hashlib
import json

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from spicy_regs import mcp_server as server, output_ledger, table_joins
from spicy_regs.citation_resolution import SOURCE_TABLES
from spicy_regs.sources import publication as pub
from spicy_regs.sources.publication import read_pinned_root  # bound before conftest keeps roots off the network
from tests.test_mcp_relationships import citation_connection, merged_occurrences
from tests.test_mcp_server import _tool_data


def _resolve(con, monkeypatch, **arguments):
    monkeypatch.setattr(server, "_get_connection", lambda: con)
    return _tool_data(server.build_server(), "resolve_document_citations",
                      {"document_kind": "govinfo_package", "document_key": "CRPT-example", **arguments})


def _cite(con, *rows, key="CRPT-example", digest="digest"):
    """Add (cite_kind, target_key, span_start) occurrences to a held report."""
    con.executemany("INSERT INTO document_citations VALUES "
                    f"('govinfo_package', '{key}', '{digest}', ?, ?, 'true', ?, '003')", rows)


def _report(con, key, digest="other", pages="9", rules="rules", rows="0"):
    con.execute("INSERT INTO house_activity_reports VALUES (?, ?, ?, ?, ?)", [key, digest, pages, rules, rows])


# S5-1: offsets order numerically; one kind and a page are selectable; every kind's count is stated.

def test_occurrences_follow_text_offsets_within_a_kind(monkeypatch):
    with citation_connection() as con:
        _cite(con, ("public_law", "114-public-254", "18732"), ("public_law", "114-public-254", "111876"))
        result = _resolve(con, monkeypatch)
    assert [row["span_start"] for row in result["occurrences"]] == ["1", "2", "18732", "111876"]


def test_cite_kind_selects_one_kind_and_counts_every_held_kind(monkeypatch):
    with citation_connection() as con:
        _cite(con, ("bill_number", "114-hr-1", "5"), ("bill_number", "114-hr-2", "7"))
        result = _resolve(con, monkeypatch, cite_kind="Public_Law", max_occurrences=1)
    assert [row["cite_kind"] for row in result["occurrences"]] == ["public_law"]
    selection = result["coverage"]["occurrence_selection"]
    assert selection["cite_kind"] == "public_law" and selection["status"] == "capped"
    assert result["coverage"]["cite_kind_counts"] == {"bill_number": 2, "public_law": 2}


def test_offset_pages_through_a_kind_without_overlap(monkeypatch):
    with citation_connection() as con:
        _cite(con, ("public_law", "114-public-254", "30"))
        pages = [_resolve(con, monkeypatch, cite_kind="public_law", max_occurrences=2, offset=offset)
                 for offset in (0, 2)]
    assert [[row["span_start"] for row in page["occurrences"]] for page in pages] == [["1", "2"], ["30"]]
    assert [page["truncated"] for page in pages] == [True, False]
    assert [page["coverage"]["occurrence_selection"]["status"] for page in pages] == ["capped", "last_page"]
    # The rows before this page were not looked up; no occurrence reason records that.
    assert pages[1]["coverage"]["partial"] is True and pages[1]["coverage"]["reason_counts"] == {}


def test_a_routed_kind_the_document_does_not_hold_is_a_complete_empty_selection(monkeypatch):
    with citation_connection() as con:
        result = _resolve(con, monkeypatch, cite_kind="crs_report_id")
    assert result["occurrences"] == []
    assert result["coverage"]["occurrence_selection"]["status"] == "complete_held_selection"
    assert result["coverage"]["cite_kind_counts"] == {"public_law": 2}
    assert result["coverage"]["partial"] is False


def test_a_kind_no_writer_emits_is_refused_naming_the_held_kinds(monkeypatch):
    with citation_connection() as con, pytest.raises(ToolError) as refused:
        _resolve(con, monkeypatch, cite_kind="public_laws")
    assert "'public_laws'" in str(refused.value) and "holds: public_law" in str(refused.value)


@pytest.mark.parametrize("kind", ["bioguide_id", "federal_register_number", "federal_register_document"])
def test_a_route_no_writer_emits_is_refused_not_answered_empty(monkeypatch, kind):
    """DRY scout S1: three routes no writer emits answered an empty, complete selection."""
    with citation_connection() as con, pytest.raises(ToolError, match="not a citation kind"):
        _resolve(con, monkeypatch, cite_kind=kind)


def test_a_held_kind_without_a_route_is_a_complete_empty_selection(monkeypatch):
    """case_docket_number is a kind writers emit (11 held rows) with no route; this document holds none."""
    with citation_connection() as con:
        result = _resolve(con, monkeypatch, cite_kind="Case_Docket_Number")
    assert result["occurrences"] == [] and result["coverage"]["occurrence_selection"]["cite_kind"] == "case_docket_number"
    assert result["coverage"]["occurrence_selection"]["status"] == "complete_held_selection"


def test_every_route_is_a_kind_a_writer_emits():
    from spicy_regs.citation_resolution import CITE_KINDS, ROUTES

    assert set(ROUTES) <= set(CITE_KINDS)


# S5-1 EXPAND: the page boundary is stable, the end is stated, and nothing is guessed.

def test_rows_tied_on_kind_span_and_key_page_apart_by_their_text(monkeypatch):
    """Rows of two held texts of one document share kind, span and key; the identity's digest orders them."""
    with citation_connection() as con:
        _cite(con, ("public_law", "114-public-254", "1"), digest="digest-0")
        pages = [merged_occurrences(_resolve(con, monkeypatch, max_occurrences=1, offset=offset))
                 for offset in (0, 1, 2)]
    assert [(row["span_start"], row["text_sha256"]) for [row] in pages] == [
        ("1", "digest"), ("1", "digest-0"), ("2", "digest")]


def test_an_offset_past_the_end_is_an_empty_last_page_that_is_partial(monkeypatch):
    with citation_connection() as con:
        result = _resolve(con, monkeypatch, offset=50)
    assert result["occurrences"] == [] and result["truncated"] is False
    assert result["coverage"]["occurrence_selection"] | {"meaning": None} == {
        "status": "last_page", "cite_kind": None, "offset": 50, "max_occurrences": 25, "meaning": None}
    assert result["coverage"]["partial"] is True


def test_a_span_that_is_not_an_integer_is_refused_rather_than_ordered_as_text(monkeypatch):
    with citation_connection() as con, pytest.raises(ToolError, match="span"):
        _cite(con, ("public_law", "114-public-254", "12a"))
        _resolve(con, monkeypatch)


def test_cite_kind_is_a_parameter_not_sql(monkeypatch):
    with citation_connection() as con, pytest.raises(ToolError, match="not a citation kind"):
        _resolve(con, monkeypatch, cite_kind="public_law' OR 1=1 --")


def test_an_unknown_key_is_refused_for_the_key_before_any_cite_kind(monkeypatch):
    with citation_connection() as con, pytest.raises(ToolError, match="case-sensitive"):
        _resolve(con, monkeypatch, document_key="CRPT-missing", cite_kind="public_laws")


# S5-2: not held, not read and read with none found are three answers, and a key nothing holds is refused.

def test_a_key_nothing_holds_is_refused_naming_the_held_spelling(monkeypatch):
    with citation_connection() as con, pytest.raises(ToolError) as refused:
        _resolve(con, monkeypatch, document_key="crpt-example")
    message = str(refused.value)
    assert "'crpt-example'" in message and "house_activity_reports" in message and "'CRPT-example'" in message
    assert "case-sensitive" in message


def test_a_key_held_under_another_kind_is_refused_naming_that_kind(monkeypatch):
    with citation_connection() as con, pytest.raises(ToolError) as refused:
        con.execute("CREATE TABLE budget_volumes AS SELECT * FROM house_activity_reports LIMIT 0")
        _resolve(con, monkeypatch, document_kind="budget_volume")
    message = str(refused.value)
    assert "budget_volumes" in message and "govinfo_package 'CRPT-example'" in message


def test_citations_of_a_document_no_longer_held_read_not_held(monkeypatch):
    with citation_connection() as con:
        con.execute("DELETE FROM house_activity_reports")
        result = _resolve(con, monkeypatch)
    assert result["source_read"]["status"] == "not_held"
    assert {row["source_status"] for row in merged_occurrences(result)} == {"unread_source"}


def test_a_held_document_without_a_digest_stays_missing_digest(monkeypatch):
    with citation_connection() as con:
        con.execute("UPDATE house_activity_reports SET text_sha256 = NULL")
        assert _resolve(con, monkeypatch)["source_read"]["status"] == "missing_digest"


def test_a_report_read_with_no_findings_is_read_none_found(monkeypatch):
    with citation_connection() as con:
        _report(con, "CRPT-quiet")
        result = _resolve(con, monkeypatch, document_key="CRPT-quiet")
    assert result["occurrences"] == [] and result["source_read"]["status"] == "read_none_found"
    assert result["coverage"]["occurrence_selection"]["status"] == "complete_held_selection"
    assert result["coverage"]["partial"] is False


@pytest.mark.parametrize("unrecorded", ["digest", "pages", "rules", "rows"])
def test_a_report_without_a_read_record_is_not_read_and_partial(monkeypatch, unrecorded):
    with citation_connection() as con:
        _report(con, "CRPT-quiet", **{unrecorded: None})
        result = _resolve(con, monkeypatch, document_key="CRPT-quiet")
    assert result["occurrences"] == [] and result["source_read"]["status"] == "not_read"
    assert result["coverage"]["partial"] is True


def test_a_report_whose_read_states_rows_that_are_not_held_is_refused(monkeypatch):
    with citation_connection() as con, pytest.raises(ToolError, match="states 5 citation rows"):
        _report(con, "CRPT-quiet", rows="5")
        _resolve(con, monkeypatch, document_key="CRPT-quiet")


@pytest.mark.parametrize("body", ["Pursuant to 5 U.S.C. 553.", None])
def test_a_held_field_with_no_rows_is_not_read_because_its_table_records_no_read(monkeypatch, body):
    """Held fields are read only when an operator selects them; the source table records no such read."""
    with citation_connection() as con:
        con.execute("CREATE TABLE comments (comment_id VARCHAR, comment VARCHAR)")
        con.execute("INSERT INTO comments VALUES ('EPA-HQ-0001', ?)", [body])
        result = _resolve(con, monkeypatch, document_kind="comment_inline", document_key="EPA-HQ-0001")
    assert result["source_read"] == {"table": "comments", "status": "not_read"}
    assert result["coverage"]["partial"] is True


# The held-citations pipeline's read table (implementer C): where a held field's read is recorded once it is published.

BODY = "Pursuant to 5 U.S.C. 553."


def _held_field_reads(con, *reads):
    """A held comment, and document_citation_reads published in print-citations with ``reads`` as its rows."""
    con.execute("CREATE TABLE comments (comment_id VARCHAR, comment VARCHAR)")
    con.execute("INSERT INTO comments VALUES ('EPA-HQ-0001', ?)", [BODY])
    con.execute("CREATE TABLE document_citation_reads (document_kind VARCHAR, document_key VARCHAR, "
                "text_sha256 VARCHAR, rule_set_version VARCHAR, read_at VARCHAR, citation_rows BIGINT)")
    for read in reads:
        con.execute("INSERT INTO document_citation_reads VALUES ('comment_inline', 'EPA-HQ-0001', ?, ?, ?, ?)", read)
    index = _index()
    index["families"]["print-citations"] = {**index["families"]["laws"], "tables": {
        "document_citation_reads.parquet": index["families"]["laws"]["tables"]["laws.parquet"]}}
    con.execute("UPDATE _spicy_publication SET snapshot = ?", [json.dumps(index)])


HELD = "sha256:" + hashlib.sha256(BODY.encode()).hexdigest()


def _resolve_held(con, monkeypatch):
    return _resolve(con, monkeypatch, document_kind="comment_inline", document_key="EPA-HQ-0001")


def test_a_held_field_read_that_found_nothing_is_read_none_found(monkeypatch):
    with citation_connection() as con:
        _held_field_reads(con, (HELD, "rules", "2026-10-03T12:00:00Z", 0))
        result = _resolve_held(con, monkeypatch)
    assert result["source_read"] == {"table": "comments", "status": "read_none_found"}
    assert result["coverage"]["partial"] is False and "document_citation_reads" in result["publication"]


@pytest.mark.parametrize("reads", [
    [],  # never read
    [("sha256:" + "0" * 64, "rules", "2026-09-01T00:00:00Z", 0)],  # read, but an earlier text of the field
    # Round 6 dropped "a read row stating no rule set is no read record": every published read states none, which
    # the dictionary documents as normal, so the filter turned every recorded read into not_read
    # (test_chaos_r6_server.test_a_zero_row_read_recorded_without_a_rule_set_or_time_is_read_none_found).
])
def test_a_held_field_with_no_read_of_its_current_text_is_not_read(monkeypatch, reads):
    with citation_connection() as con:
        _held_field_reads(con, *reads)
        result = _resolve_held(con, monkeypatch)
    assert result["source_read"]["status"] == "not_read" and result["coverage"]["partial"] is True


def test_the_latest_read_of_the_current_text_decides(monkeypatch):
    with citation_connection() as con:
        _held_field_reads(con, (HELD, "old", "2026-09-01T00:00:00Z", 3), (HELD, "new", "2026-10-03T12:00:00Z", 0))
        assert _resolve_held(con, monkeypatch)["source_read"]["status"] == "read_none_found"


def test_a_held_field_read_stating_rows_that_are_not_held_is_refused(monkeypatch):
    with citation_connection() as con, pytest.raises(ToolError, match="states 2 citation rows"):
        _held_field_reads(con, (HELD, "rules", "2026-10-03T12:00:00Z", 2))
        _resolve_held(con, monkeypatch)


# S5-3 and S5-4: what the client is told.

def _citation_tool():
    [tool] = [t for t in asyncio.run(server.build_server().list_tools()) if t.name == "resolve_document_citations"]
    return tool


def test_the_schema_names_each_supported_kind_with_its_table_once():
    """The field description lists the kinds the round-4 enum listed, each with its table, and nothing repeats them."""
    document_kind = _citation_tool().input_schema["properties"]["document_kind"]
    assert all(f"{kind}: {table}" in document_kind["description"] for kind, table in SOURCE_TABLES.items())
    assert "enum" not in document_kind


def test_the_description_defines_partial_the_key_rule_and_the_read_statuses():
    description = _citation_tool().description or ""
    assert "coverage.partial" in description and "case-sensitive" in description
    assert all(status in description for status in ("read_none_found", "not_read", "not_held"))
    assert "govinfo_package covers only" in description


# Reply compaction (owner decision 2026-10-03): fields that cannot vary are stated once; pages are 25 to 100 rows.

def test_the_default_page_is_25_and_a_page_above_100_is_refused_with_how_to_page(monkeypatch):
    schema = _citation_tool().input_schema["properties"]["max_occurrences"]
    assert (schema["default"], schema["maximum"]) == (25, 100)
    with citation_connection() as con:
        assert _resolve(con, monkeypatch, max_occurrences=100)["max_occurrences"] == 100
        with pytest.raises(ToolError, match="at most 100.*offset"):
            _resolve(con, monkeypatch, max_occurrences=101)


def test_fields_that_cannot_vary_are_stated_once_and_merge_back(monkeypatch):
    with citation_connection() as con:
        _cite(con, ("bill_number", "114-hr-1", "5"), ("bill_number", "114-hr-2", "7"))
        result = _resolve(con, monkeypatch)
        laws = server._publication_status(con)["publication"]["laws"]
    fields = result["occurrence_fields"]
    assert {"document_kind", "document_key", "text_sha256", "resolution_rule", "source_status"} <= set(fields["shared"])
    # Route fields are stated per kind, even where two kinds share a value (target_grain here).
    assert {"target_snapshot", "target_table_selected", "target_grain"} <= set(fields["by_cite_kind"]["public_law"])
    hoisted = set(fields["shared"]) | {"target_kind", "normalized_key", "target_snapshot", "target_grain"}
    assert all(not hoisted & set(row) and "cite_kind" in row for row in result["occurrences"])
    merged = merged_occurrences(result)
    assert [(row["target_kind"], row["normalized_key"]) for row in merged] == [
        (row["cite_kind"], row["target_key"]) for row in merged]
    assert [row["target_snapshot"] for row in merged if row["cite_kind"] == "public_law"] == [laws] * 2


def test_the_occurrence_shape_is_the_same_on_every_page(monkeypatch):
    """Coordinator answer 6: a fixed list is hoisted, so a page whose rows happen to agree (all found) keeps its shape."""
    with citation_connection() as con:
        _cite(con, ("public_law", "119-public-999", "30"), ("public_law", "119-public-998", "40"))
        pages = [_resolve(con, monkeypatch, max_occurrences=2, offset=offset) for offset in (0, 2)]
    assert [[row["target_status"] for row in merged_occurrences(page)] for page in pages] == [
        ["found", "found"], ["missing", "missing"]]
    assert {tuple(sorted(row)) for page in pages for row in page["occurrences"]} == {
        tuple(sorted(pages[0]["occurrences"][0]))}
    assert pages[0]["occurrence_fields"]["hoisted"] == pages[1]["occurrence_fields"]["hoisted"] == {
        "shared": list(server.OCCURRENCE_DOCUMENT_FIELDS), "by_cite_kind": list(server.OCCURRENCE_KIND_FIELDS)}


def test_the_occurrence_projection_is_lossless():
    snapshot = {"status": "managed_generation", "family": "laws"}
    row = {"document_key": "D", "text_sha256": "t1", "cite_kind": "public_law", "target_kind": "public_law",
           "target_key": "1-public-2", "normalized_key": "1-public-2", "target_snapshot": snapshot,
           "target_grain": "one target record", "span_start": "4", "target_status": "found", "reason": None}
    rows = [row,
            {**row, "text_sha256": "t2", "target_key": "1-public-3", "normalized_key": "1-public-3", "span_start": "9"},
            {**row, "cite_kind": "bill_number", "target_kind": "bill_number", "target_key": None, "normalized_key": "",
             "target_snapshot": None, "target_grain": "one target record", "span_start": "12",
             "target_status": "not_checked", "reason": "unsettled_key", "error_type": "x"}]
    compact, fields = server._compact_occurrences(rows)
    assert merged_occurrences({"occurrences": compact, "occurrence_fields": fields}) == rows
    # Two texts on one page: text_sha256 is stated on each occurrence and named, never merged into one value.
    assert fields["shared"] == {"document_key": "D"} and fields["not_hoisted"] == ["text_sha256"]
    assert fields["by_cite_kind"]["public_law"] == {"target_snapshot": snapshot, "target_grain": "one target record"}
    assert "target_status" in compact[0] and "reason" in compact[0]  # per-occurrence fields are never hoisted
    assert compact[2]["normalized_key"] == ""  # kept where it does not repeat target_key
    single, single_fields = server._compact_occurrences(rows[:1])
    assert set(single[0]) == {"cite_kind", "target_key", "span_start", "target_status", "reason"}
    assert single_fields["shared"] == {"document_key": "D", "text_sha256": "t1"}


def test_the_acquisition_queue_states_its_constant_fields_once(monkeypatch):
    with citation_connection() as con:
        _cite(con, ("public_law", "93-public-344", "40"), ("public_law", "93-public-344", "50"),
              ("public_law", "94-public-1", "60"))
        # The queue plans only from a pinned source, so the report's table joins a published family.
        index = _index()
        index["families"]["print-citations"] = {**index["families"]["laws"], "tables": {
            "house_activity_reports.parquet": index["families"]["laws"]["tables"]["laws.parquet"]}}
        con.execute("UPDATE _spicy_publication SET snapshot = ?", [json.dumps(index)])
        queue = _resolve(con, monkeypatch)["acquisition_queue"]
    shared = queue["shared_fields"]
    # Round 6 (owner decision: shrink replies) names each requesting occurrence by its span on the page, which states
    # every field a request restated; test_chaos_r6_server rebuilds the whole queue from the reply.
    assert shared["input_snapshot"]["family"] == "print-citations"
    assert {"intended_query", "queue_rule", "acquisition_outcome"} <= set(shared["item"])
    assert {"target_snapshot", "status"} <= set(shared["by_target_kind"]["public_law"])
    assert [(item["normalized_key"], item["requesting_spans"]) for item in queue["items"]] == [
        ("93-public-344", ["40", "50"]), ("94-public-1", ["60"])]


# Freshness: the publisher states when it moved a family's pointer; every reply that pins a generation says so.

def _index(**family):
    return {"format": "spicy-regs-publication", "version": 2, "families": {"laws": {
        "prefix": "generations/laws/" + "a" * 64, "logicalId": "urn:example", "artifactDigest": "sha256:" + "a" * 64,
        "tables": {"laws.parquet": {"sha256": "sha256:" + "b" * 64, "byteSize": 1, "rows": 1,
                                    "columns": [["law_id", "VARCHAR"]]}}, **family}}}


def test_the_index_admits_a_publication_instant_that_version_1_omits():
    index = pub.parse_index(json.dumps(_index(publishedAt="2026-10-01T03:12:26Z")).encode())
    assert index["families"]["laws"]["publishedAt"] == "2026-10-01T03:12:26Z"
    assert "publishedAt" not in pub.derive_v1(index)["families"]["laws"]
    assert pub.parse_index(json.dumps(_index(publishedAt="2026-10-01T03:12:26.123456Z")).encode())
    for invalid in ("yesterday", "2026-10-01 03:12:26", "2026-10-01T03:12:26+00:00", "2026-13-01T03:12:26Z",
                    1759288346, None):
        with pytest.raises(pub.PublicationError):
            pub.parse_index(json.dumps(_index(publishedAt=invalid)).encode())
    with pytest.raises(pub.PublicationError):
        pub.parse_index(json.dumps(_index(publishedBy="someone")).encode())


@pytest.mark.parametrize("published_at", ["2026-10-01T03:12:26Z", None])
def test_replies_state_when_the_publisher_moved_the_pointer(monkeypatch, published_at):
    with citation_connection() as con:
        family = {"publishedAt": published_at} if published_at else {}
        con.execute("UPDATE _spicy_publication SET snapshot = ?", [json.dumps(_index(**family))])
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        mcp = server.build_server()
        described = _tool_data(mcp, "describe_table", {"table": "laws"})["publication"]
        queried = _tool_data(mcp, "query_sql", {"sql": "SELECT count(*) AS n FROM laws"})["publication"]["laws"]
        cited = _tool_data(mcp, "resolve_document_citations",
                           {"document_kind": "govinfo_package", "document_key": "CRPT-example"})
        listed = {row["table"]: row for row in _tool_data(mcp, "list_sources", {})["tables"]}
        status = server._publication_status(con)["publication"]["laws"]
    assert described["published_at"] == queried["published_at"] == published_at
    assert "published_at" in described and "published_at" in queried
    # The citation reply's target pins come from the same projection; derived views' embedded pins do not move.
    assert merged_occurrences(cited)[0]["target_snapshot"] == status
    assert "published_at" not in status and "published_at" not in listed["laws"]


# Parent lag (owner decision 2026-10-03): a derived family's root records the parent generations it was built from.

def _family(name, digest, tables, **extra):
    return {"prefix": f"generations/{name}/{digest[7:]}", "logicalId": f"urn:{name}", "artifactDigest": digest,
            "tables": {f"{table}.parquet": {"sha256": sha, "byteSize": 1, "rows": 1, "columns": [["id", "VARCHAR"]]}
                       for table, sha in tables.items()}, **extra}


DOCS_LIVE, DOCS_OLD, SIGNALS = ("sha256:" + c * 64 for c in "abc")


def _lineage(monkeypatch, parents, documents=DOCS_LIVE, documents_sha="sha256:" + "d" * 64, roots=None):
    """A connection whose index pins documents and discovery_signals, and a root reader counting its reads."""
    import duckdb

    index = {"format": "spicy-regs-publication", "version": 2, "families": {
        "documents": _family("documents", documents, {"documents": documents_sha}),
        "discovery-signals": _family("discovery-signals", SIGNALS, {"discovery_signals": "sha256:" + "e" * 64}),
    }}
    con = duckdb.connect()
    for table in ("documents", "discovery_signals"):
        con.execute(f"CREATE TABLE {table} (id VARCHAR)")
    con.execute("CREATE TABLE _spicy_publication (snapshot VARCHAR)")
    con.execute("INSERT INTO _spicy_publication VALUES (?)", [json.dumps(index)])
    reads = []
    stored = roots if roots is not None else {SIGNALS: {"spec": {"parents": parents}}, DOCS_LIVE: {"spec": {}},
                                              DOCS_OLD: {"spec": {}}}

    def read_root(base_url, entry):
        reads.append(entry["artifactDigest"])
        root = stored[entry["artifactDigest"]]
        if isinstance(root, Exception):
            raise root
        return {"artifactDigest": entry["artifactDigest"], "logicalId": entry["logicalId"], **root}

    monkeypatch.setattr(pub, "read_pinned_root", read_root)
    monkeypatch.setattr(server, "_ROOT_PARENTS", {})
    monkeypatch.setattr(server, "_ROOT_FAILED_AT", {})
    monkeypatch.setattr(server, "_get_connection", lambda: con)
    return con, reads


def _documents_parent(digest=DOCS_OLD, sha="sha256:" + "f" * 64):
    return {"documents.parquet": {"family": "documents", "artifactDigest": digest, "sha256": sha, "byteSize": 1}}


def test_a_derived_table_names_the_parent_generation_it_was_built_from(monkeypatch):
    con, _ = _lineage(monkeypatch, _documents_parent())
    with con:
        mcp = server.build_server()
        described = _tool_data(mcp, "describe_table", {"table": "discovery_signals"})["publication"]
        queried = _tool_data(mcp, "query_sql", {"sql": "SELECT * FROM discovery_signals"})["publication"]
    expected = [{"table": "documents", "family": "documents", "built_from": DOCS_OLD, "live": DOCS_LIVE,
                 "input_table_current": False}]
    assert described["inputs"] == queried["discovery_signals"]["inputs"] == expected
    assert described["inputs_current"] is False


def test_a_derived_table_built_from_the_live_parent_is_current(monkeypatch):
    con, _ = _lineage(monkeypatch, _documents_parent(DOCS_LIVE, "sha256:" + "d" * 64))
    with con:
        described = _tool_data(server.build_server(), "describe_table", {"table": "discovery_signals"})["publication"]
    assert described["inputs"][0]["input_table_current"] is True and described["inputs_current"] is True


def test_a_parent_family_that_moved_for_another_table_still_counts_as_current(monkeypatch):
    """The family digest moved, the parent table's bytes did not: the derived table read what is live."""
    con, _ = _lineage(monkeypatch, _documents_parent(DOCS_OLD, "sha256:" + "d" * 64))
    with con:
        described = _tool_data(server.build_server(), "describe_table", {"table": "discovery_signals"})["publication"]
    assert (described["inputs"][0]["built_from"], described["inputs"][0]["live"]) == (DOCS_OLD, DOCS_LIVE)
    assert described["inputs_current"] is True


def test_the_description_says_input_table_current_compares_the_parent_tables_bytes():
    [tool] = [t for t in asyncio.run(server.build_server().list_tools()) if t.name == "describe_table"]
    assert "input_table_current" in (tool.description or "") and "another table" in (tool.description or "")


def test_a_family_without_parents_states_no_inputs(monkeypatch):
    con, _ = _lineage(monkeypatch, _documents_parent())
    with con:
        described = _tool_data(server.build_server(), "describe_table", {"table": "documents"})["publication"]
    assert "inputs" not in described and "inputs_current" not in described and "inputs_status" not in described


def test_a_storage_version_parent_is_reported_without_a_lag_claim(monkeypatch):
    parents = {**_documents_parent(), "comments.parquet": {"etag": '"7a81"', "byteSize": 9}}
    con, _ = _lineage(monkeypatch, parents)
    with con:
        described = _tool_data(server.build_server(), "describe_table", {"table": "discovery_signals"})["publication"]
    assert described["inputs"][0] == {"table": "comments", "family": None, "built_from": '"7a81"', "live": None,
                                      "input_table_current": None}
    assert described["inputs_current"] is False  # the managed parent still lags
    con, _ = _lineage(monkeypatch, {"comments.parquet": {"etag": '"7a81"', "byteSize": 9}})
    with con:
        alone = _tool_data(server.build_server(), "describe_table", {"table": "discovery_signals"})["publication"]
    assert alone["inputs_current"] is None


def test_an_unreadable_root_is_stated_never_guessed_and_retried_after_60_seconds(monkeypatch):
    roots = {SIGNALS: pub.PublicationError("Pinned root differs from its captured pin"), DOCS_LIVE: {"spec": {}}}
    con, reads = _lineage(monkeypatch, {}, roots=roots)
    clock = [1000.0]
    monkeypatch.setattr(server, "_monotonic", lambda: clock[0])
    with con:
        mcp = server.build_server()
        for now in (1000.0, 1059.0, 1061.0):
            clock[0] = now
            described = _tool_data(mcp, "describe_table", {"table": "discovery_signals"})["publication"]
            assert described["inputs"] is None and described["inputs_status"] == "root_unavailable"
    assert reads == [SIGNALS, SIGNALS]  # the read at 1059 s waited out the failure at 1000 s


def test_roots_are_read_lazily_once_per_generation_and_never_for_discovery(monkeypatch):
    con, reads = _lineage(monkeypatch, _documents_parent())
    with con:
        mcp = server.build_server()
        server._publication_status(con)
        _tool_data(mcp, "list_sources", {})
        assert reads == []
        for _ in range(3):
            _tool_data(mcp, "describe_table", {"table": "discovery_signals"})
        listed = {row["table"]: row for row in _tool_data(mcp, "list_sources", {})["tables"]}
    assert reads == [SIGNALS] and "inputs" not in listed["discovery_signals"]


def test_the_server_reads_a_pinned_root_without_rulespec_and_refuses_another_pin(monkeypatch):
    entry = _family("documents", DOCS_LIVE, {"documents": "sha256:" + "d" * 64})
    root = {"artifactDigest": DOCS_LIVE, "logicalId": "urn:documents", "spec": {"parents": {}}}
    urls = []
    monkeypatch.setattr(pub, "_bounded_get", lambda url, **_: urls.append(url) or json.dumps(root).encode())
    assert read_pinned_root("https://pub.example/", entry) == root
    assert urls == [f"https://pub.example/generations/documents/{'a' * 64}/artifact.json"]
    with pytest.raises(pub.PublicationError):
        read_pinned_root("https://pub.example", {**entry, "artifactDigest": DOCS_OLD})


# Meaning text moved into the artifacts the server reads (pattern 9).

def test_the_basis_texts_live_in_the_bundled_records(monkeypatch):
    assert not hasattr(server, "JOINS_BASIS") and not hasattr(server, "QUALIFICATION_BASIS")
    joins = table_joins.joins_record()["basis"]
    qualification = output_ledger.qualification_record(output_ledger.LEDGER.read_text(encoding="utf-8"))["basis"]
    assert "it does not check that the publisher paired them correctly" in joins
    assert "the maintainer's retained evidence, not public files" in qualification
    assert server._joins()["basis"] == joins and server._ledger()[0]["basis"] == qualification
    with citation_connection() as con:
        con.execute("UPDATE _spicy_publication SET snapshot = ?", [json.dumps(_index())])
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        described = _tool_data(server.build_server(), "describe_table", {"table": "laws"})
    assert described["joins"]["basis"] == joins and described["qualification"]["basis"] == qualification
