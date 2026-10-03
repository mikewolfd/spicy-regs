"""Normalization is not existence; target selection and source text govern lookup."""
import duckdb

from spicy_regs.citation_resolution import resolve_citations


def occurrence(kind="public_law", key="114-public-254", **extra):
    return {"document_kind": "budget_volume", "document_key": "BUDGET-2027", "text_sha256": "held",
            "cite_kind": kind, "target_key": key, "target_resolved": "true", "span_start": "10",
            "matched_text": "Public Law 114–254", "target_rule": "public_law", "rule_version": "003", **extra}


DIGESTS = {("budget_volume", "BUDGET-2027"): "held"}
PINS = {"laws": {"artifact_digest": "sha256:one"}}


def resolve(con, items, pins=PINS, **kwargs):
    return resolve_citations(con, items, pins, source_digests=DIGESTS, **kwargs)


def laws():
    con = duckdb.connect()
    con.execute("CREATE TABLE laws(law_id VARCHAR, congress VARCHAR, law_type VARCHAR, number VARCHAR)")
    con.execute("INSERT INTO laws VALUES ('114-public-254','114','public','254')")
    return con


def test_normalized_missing_and_new_target_without_reextracting_source():
    with laws() as con:
        item = occurrence(key="119-public-999")
        missing = resolve(con, [item])["occurrences"][0]
        assert missing["target_resolved"] == "true" and missing["target_status"] == "missing"
        con.execute("INSERT INTO laws VALUES ('119-public-999','119','public','999')")
        found = resolve(con, [item], {"laws": {"artifact_digest": "sha256:two"}})["occurrences"][0]
        assert found["target_status"] == "found"
        assert found["occurrence_key"] == missing["occurrence_key"]
        assert found["target_snapshot"] != missing["target_snapshot"]
        assert found["matched_text"] == item["matched_text"]


def test_ambiguous_unsupported_unread_and_partial_keys_are_not_missing():
    with laws() as con:
        con.execute("INSERT INTO laws VALUES ('114-public-254','115','public','254')")
        items = [occurrence(), occurrence("case_docket_number", "20-1"), occurrence("bill_number", "119-hr-1"),
                 occurrence(target_resolved="false"), occurrence(text_sha256="old"), occurrence(text_sha256=None)]
        rows = resolve(con, items, {**PINS, "congress_bills": {"artifact_digest": "sha256:b"}})["occurrences"]
        assert [r["target_status"] for r in rows] == ["ambiguous","unsupported","not_checked","not_checked","not_checked","not_checked"]
        assert len(rows[0]["candidate_keys"]) == 2
        assert rows[2]["reason"] == "target_read_failure"
        assert rows[3]["reason"] == "unsettled_key"
        assert rows[4]["source_status"] == "stale_source"
        assert rows[5]["source_status"] == "unread_source"


def test_unique_target_keys_are_batched_and_occurrences_are_preserved():
    with laws() as con:
        result = resolve(con, [occurrence(span_start=str(i)) for i in range(4)])
        assert len(result["occurrences"]) == 4
        assert len({r["occurrence_key"] for r in result["occurrences"]}) == 4
        assert result["coverage"]["distinct_target_keys_read"] == 1
        assert result["coverage"]["target_batches"] == 1
        limited = resolve(con, [occurrence(), occurrence(key="1-public-1")], max_target_keys=1)
        assert limited["coverage"]["partial"]
        assert limited["occurrences"][1]["reason"] == "target_key_limit"


def test_fr_dates_alternate_system_and_set_valued_rin():
    with duckdb.connect() as con:
        con.execute("CREATE TABLE federal_register(document_number VARCHAR, publication_date VARCHAR, volume VARCHAR, start_page VARCHAR)")
        con.execute("INSERT INTO federal_register VALUES ('2026-1','2026-01-01','91','1'),('2026-1','2026-02-01','91','20')")
        con.execute("CREATE TABLE unified_agenda(rin VARCHAR, agenda_edition VARCHAR)")
        con.execute("INSERT INTO unified_agenda VALUES ('1234-AB12','202410'),('1234-AB12','202504')")
        pins = {name: {"artifact_digest": name} for name in ["federal_register", "unified_agenda"]}
        items = [occurrence("federal_register_cite","91-20"),occurrence("rin","1234-AB12")]
        rows = resolve(con,items,pins)["occurrences"]
        assert [r["target_status"] for r in rows] == ["found","found"]
        assert rows[0]["candidate_keys"] == [{"document_number":"2026-1","publication_date":"2026-02-01"}]
        assert rows[1]["expected_cardinality"] == "many" and len(rows[1]["candidate_keys"]) == 2
        limited = resolve(con,[items[1]],pins,max_candidates=1)["occurrences"][0]
        assert limited["target_status"] == "not_checked" and limited["reason"] == "candidate_limit"


def test_unpinned_selection_and_source_digest_are_required_for_text():
    with laws() as con:
        rows = resolve(con,[occurrence()],{"laws":{"status":"legacy_unversioned"}})["occurrences"]
        assert rows[0]["reason"] == "target_snapshot_unavailable"
        assert resolve_citations(con,[occurrence()],PINS)["occurrences"][0]["source_status"] == "unread_source"
        native = {"cite_kind":"public_law","target_key":"114-public-254","document_key":"native"}
        assert resolve_citations(con,[native],PINS)["occurrences"][0]["target_status"] == "found"


def test_target_timeout_is_explicit_and_preserves_other_results():
    class TimedOut:
        def execute(self, *args):
            raise TimeoutError("details must not leak")
    row = resolve(TimedOut(),[occurrence()])["occurrences"][0]
    assert row["target_status"] == "not_checked" and row["reason"] == "target_timeout"
    assert "details" not in str(row)


def test_timeout_does_not_start_another_batch_after_request_timer_expires():
    class TimedOut:
        calls = 0

        def execute(self, *args):
            self.calls += 1
            raise TimeoutError("request timer expired")
    con = TimedOut()
    result = resolve(con, [occurrence(), occurrence("bill_number", "119-hr-1")],
                     {**PINS, "congress_bills": {"artifact_digest": "sha256:b"}})
    assert con.calls == 1
    assert all(row["reason"] == "target_timeout" for row in result["occurrences"])
    assert result["coverage"]["distinct_target_keys_read"] == 0
