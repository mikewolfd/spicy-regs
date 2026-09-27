"""Population reconciliation failures are meaningful only for the same pinned scope."""

import duckdb
import pytest

from spicy_regs.aggregate_checks import BY_NAME, measure, qualify


def lineage(check):
    pins = {
        t: {"artifactDigest": "sha256:" + str(i) * 64, "sha256": "sha256:" + str(i + 1) * 64}
        for i, t in enumerate((check.output, *check.inputs))
    }
    root = {
        "artifactDigest": pins[check.output]["artifactDigest"],
        "spec": {
            "publicationStatus": "complete-family",
            "parents": {t + ".parquet": {"sha256": pins[t]["sha256"]} for t in check.inputs},
        },
    }
    return {"pins": pins, "root": root}


def test_equal_totals_wrong_agency_are_mismatches_and_independent_generations_abstain():
    check = BY_NAME["agency-document"]
    args = lineage(check)
    with duckdb.connect() as con:
        con.execute("CREATE TABLE documents AS SELECT * FROM (VALUES ('A'),('B'),(NULL)) t(agency_code)")
        con.execute("CREATE TABLE agency_stats AS SELECT 'A' agency_code,2 document_count")
        result = measure(con, check, **args)
        assert result["expected_total"] == result["observed_total"] == 2
        assert result["status"] == "MISMATCH" and result["mismatched_cells"] == 2
        args["pins"]["documents"]["sha256"] = "sha256:" + "f" * 64
        con.execute("DROP TABLE documents")
        assert measure(con, check, **args)["status"] == "INCOMPARABLE"  # no accidental scan


def test_comments_cohorts_null_keys_compare_without_fanout():
    check = BY_NAME["comments-index"]
    with duckdb.connect() as con:
        con.execute("""CREATE TABLE comments AS SELECT * FROM (VALUES
            ('A',NULL,'2025-01-01'),('A',NULL,'2025-01-02'),(NULL,'"D"',NULL))
            t(agency_code,docket_id,posted_date)""")
        con.execute("""CREATE TABLE comments_index AS SELECT * FROM (VALUES
            ('A',NULL,2025,1,2),(NULL,'D',NULL,NULL,1)) t(agency_code,docket_id,year,month,row_count)""")
        result = measure(con, check, **lineage(check))
        assert result["status"] == "OK" and result["expected_total"] == 3
        assert result["expected_cells"] == result["observed_cells"] == 2


def test_monthly_omits_invalid_year_zero_and_missing_dates_not_nullable_agency():
    check = BY_NAME["monthly-documents"]
    with duckdb.connect() as con:
        con.execute("""CREATE TABLE documents AS SELECT * FROM (VALUES
            (NULL,'2025-01-02',NULL),('A','0000-01-01','Rule'),('A','bad','Rule'),('A',NULL,'Rule'))
            t(agency_code,posted_date,document_type)""")
        con.execute("""CREATE TABLE agency_monthly_volume AS SELECT NULL::VARCHAR agency_code,2025 AS year,1 AS month,
            NULL::VARCHAR document_type,1 document_count""")
        assert measure(con, check, **lineage(check))["status"] == "OK"


def test_lifecycle_global_not_sum_of_overlapping_strata():
    check = BY_NAME["lifecycle-outcomes"]
    with duckdb.connect() as con:
        con.execute("""CREATE TABLE rulemaking_lifecycles AS SELECT * FROM (VALUES
            ('A','2025-01-01','final'),(NULL,'2025-01-01','censored'),('A','2025-01-01',NULL))
            t(agency_code,censor_date,outcome)""")
        con.execute("""CREATE TABLE agency_lifecycle_stats AS SELECT * FROM (VALUES
            ('A','2025-01-01','all',1,0,0),(NULL,'2025-01-01','all',1,0,1),
            ('A','2025-01-01','routine',1,0,0)) t(agency_code,censor_date,stratum,finals,withdrawals,censored)""")
        result = measure(con, check, **lineage(check))
        assert result["status"] == "OK" and result["expected_total"] == 3


def test_feed_comment_scope_excludes_orphans_and_null_dockets():
    check = BY_NAME["feed-comments"]
    with duckdb.connect() as con:
        con.execute("CREATE TABLE dockets AS SELECT * FROM (VALUES ('\"D\"'),(NULL)) t(docket_id)")
        con.execute(
            "CREATE TABLE comments_index AS SELECT * FROM (VALUES ('D',2),('orphan',4),(NULL,7)) t(docket_id,row_count)"
        )
        con.execute("CREATE TABLE feed_summary AS SELECT * FROM (VALUES ('D',2),(NULL,0)) t(docket_id,comment_count)")
        result = measure(con, check, **lineage(check))
        assert result["status"] == "OK" and result["expected_total"] == 2


@pytest.mark.parametrize("status", ["failed", "capped", "acquired", "parsed"])
def test_incomplete_read_cannot_be_empty_or_published(status):
    check = BY_NAME["agency-document"]
    with duckdb.connect() as con:
        assert measure(con, check, read_status=status, **lineage(check))["status"] == "NOT_MEASURED"


def test_valid_empty_missing_schema_and_unpinned_are_distinct():
    check = BY_NAME["agency-document"]
    with duckdb.connect() as con:
        assert measure(con, check, **lineage(check))["status"] == "READ_FAILURE"
        con.execute("CREATE TABLE documents(agency_code VARCHAR)")
        con.execute("CREATE TABLE agency_stats(agency_code VARCHAR,document_count BIGINT)")
        assert measure(con, check, **lineage(check))["status"] == "EMPTY"
        assert measure(con, check, pins={}, root={})["status"] == "UNPINNED"


def test_carried_forward_same_family_is_not_proof_of_co_build():
    check = BY_NAME["comments-index"]
    args = lineage(check)
    args["pins"]["comments"]["artifactDigest"] = args["pins"]["comments_index"]["artifactDigest"]
    args["root"]["spec"]["parents"] = {}
    assert qualify(check, **args)[0] == "COMPARABLE"
    args["root"]["spec"]["carriedForward"] = {"comments.parquet": "old"}
    assert qualify(check, **args)[0] == "INCOMPARABLE"


def test_timeout_never_becomes_a_valid_empty():
    from dataclasses import replace

    check = replace(
        BY_NAME["agency-document"],
        expected="SELECT sum(i) AS value, NULL::VARCHAR agency_code FROM range(1000000000000) t(i)",
    )
    with duckdb.connect() as con:
        con.execute("CREATE TABLE agency_stats(agency_code VARCHAR,document_count BIGINT)")
        result = measure(con, check, timeout_seconds=0.01, **lineage(check))
        assert result["status"] == "TIMEOUT" and result["read_status"] == "failed"
        assert "expected_total" not in result


def test_local_partial_publication_does_not_prove_published_scope():
    check = BY_NAME["agency-document"]
    args = lineage(check)
    args["root"]["spec"]["publicationStatus"] = "local-partial"
    assert qualify(check, **args)[0] == "INCOMPARABLE"
