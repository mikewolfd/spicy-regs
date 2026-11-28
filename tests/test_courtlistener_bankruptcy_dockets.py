"""CourtListener 2022 bankruptcy dockets: typed schema, literal fidelity, fail-closed guards, and receipts-free publish."""

from __future__ import annotations

import bz2
from datetime import UTC, date, datetime
import hashlib
import json
from pathlib import Path
import shutil
from types import SimpleNamespace

import duckdb
import pyarrow.parquet as pq
import pytest
from spicy_docs.storage.raw_archive import CentralRawArchiveStore

from scripts import check_rollup_freshness as freshness
from spicy_regs import data_dictionary as dd
from spicy_regs import local_publication, mcp_server, odcs_contracts
from spicy_regs.contract_types import described_schema
from spicy_regs.explorer_metadata import build_bundle
from spicy_regs.pipelines import courtlistener_bankruptcy_dockets as pipeline
from spicy_regs.sources import publication
from spicy_regs.transforms import build_courtlistener_bankruptcy_dockets as module
from tests.generation_fakes import Store

REPO_ROOT = Path(__file__).resolve().parents[1]
REAL_FIXTURE_CSV = REPO_ROOT / "tests" / "fixtures" / "courtlistener" / "bankruptcy-dockets-2022-09-19-lines.csv"
REAL_FIXTURE_SHA256 = "b8af9f437dd522587821deb10f97dfcd74751f0855727f8429faff7f699cb6dc"

HEADER = ",".join(module.SOURCE_COLUMNS)


def _write_bz2(path: Path, text: str) -> tuple[Path, str, int]:
    payload = bz2.compress(text.encode("utf-8"))
    path.write_bytes(payload)
    digest = "sha256:" + hashlib.sha256(payload).hexdigest()
    return path, digest, len(payload)


def _edge_fixture_csv() -> str:
    """Synthetic CSV covering NULL vs '', sentinel strings, leading zeros, pre-1900 dates, and non-FB courts."""
    rows = [
        # 1. NULL docket_number (mspb spillover), blocked='f', NULL date_blocked, literal "null" pacer_case_id,
        #    pre-1900 date_filed (1871-10-12), pre-1900 date_terminated (0205-02-25), 0 fractional digits timestamp
        '1001,2014-10-30 05:45:51+00,2015-01-02 03:04:05+00,"","",1871-10-12,0205-02-25,NULL,In re Null Docket,"",,02015759,null,,f,,mspb,',
        # 2. Empty-string "" docket_number (neb spillover), blocked='f' with non-null date_blocked,
        #    literal "undefined" pacer_case_id, future dates, 4-digit fractional timestamp
        '1002,2022-01-01 00:00:00.1234+00,2022-09-19 22:37:44.365037+00,"","",2079-11-23,2097-02-13,N/A,Nebraska Spillover,"","",00012345,undefined,2021-01-01,f,,neb,',
        # 3. Regular federal bankruptcy row with leading-zero docket_number_core, NULL pacer_case_id,
        #    NULL date_filed/date_terminated, assigned_to_id present, backslash and doubled quotes
        '1003,2020-12-29 23:35:50.06631+00,2021-01-03 04:28:21.386803+00,"Judge ""A"" Smith","",,,NA,"Acme\\Corp, Inc.","Full Caption",6:04-bk-09593,0409593,,2020-12-29,t,8925,flmb,',
        # 4. Remaining non-FB spillover courts + empty string vs NULL checks
        '1004,2022-02-02 10:20:30.1+00,2022-02-02 10:20:30.123456+00,"","",2020-05-21,,"",PA Common Pleas,"","",01000001,999,2022-02-02,f,,pactcomplcolumb,',
        '1005,2022-02-03 10:20:30+00,2022-02-03 10:20:30+00,"","",2020-05-22,,"",PA Orphans,"",20-001,"",1000,2022-02-03,f,,paorphctcolumb,',
        '1006,2022-02-04 10:20:30+00,2022-02-04 10:20:30+00,"","",2020-05-23,,"",NY Oyer,"","","",1001,2022-02-04,f,,nyoytermctalb,',
    ]
    return "\n".join([HEADER, *rows]) + "\n"


def test_real_31_line_fixture_matches_verified_review_bytes_and_decodes_cleanly(tmp_path):
    assert REAL_FIXTURE_CSV.is_file()
    raw_csv_bytes = REAL_FIXTURE_CSV.read_bytes()
    assert hashlib.sha256(raw_csv_bytes).hexdigest() == REAL_FIXTURE_SHA256

    bz2_path = tmp_path / "bankruptcy-dockets-2022-09-19-lines.csv.bz2"
    bz2_bytes = bz2.compress(raw_csv_bytes)
    bz2_path.write_bytes(bz2_bytes)
    expected_sha = "sha256:" + hashlib.sha256(bz2_bytes).hexdigest()

    out_path, stats = module.build_courtlistener_bankruptcy_dockets(
        bz2_path,
        tmp_path / "out",
        source_sha256=expected_sha,
        piece_bytes=4096,
        parse_workers=2,
        decompression_threads=1,
    )
    assert out_path.name == module.OUTPUT
    assert pq.read_schema(out_path).equals(module.SCHEMA)
    assert stats["rows"] == 31
    assert stats["canary_rows"] == 31
    assert stats["docket_number_null"] == 0
    assert stats["docket_number_empty"] == 0
    assert stats["blocked_true"] == 31
    assert stats["blocked_false"] == 0
    assert stats["non_fb_rows"] == 0
    assert stats["dump_date"] == module.DUMP_DATE
    assert stats["source_sha256"] == expected_sha

    rows = pq.read_table(out_path).to_pylist()
    assert len(rows) == 31
    by_id = {row["cl_docket_id"]: row for row in rows}

    # Spot-check dialect-stress rows from ss-olaa raw visual review:
    # 1. Doubled quotes inside case_name + truncated HTML entity
    r_arlene = by_id["26619976"]
    assert r_arlene["case_name"] == 'Freida "Arlene&qu Smith'
    assert r_arlene["case_name_short"] == 'Freida "Arlene&qu Smith'
    assert r_arlene["date_filed"] == date(2004, 7, 2)
    assert r_arlene["date_terminated"] == date(2004, 12, 22)
    assert r_arlene["blocked"] is True
    assert r_arlene["assigned_to_id"] is None
    assert r_arlene["referred_to_id"] is None
    assert r_arlene["referred_to_str"] == ""

    # 2. PACER HTML markup + doubled quotes
    r_html = by_id["59981746"]
    assert r_html["case_name"] == (
        'C.C. Westland Joint Venture <b><font color="red">Jointly Administered under 21-31948.</font></b>'
    )
    assert r_html["docket_number_core"] == "21031973"

    # 3. Literal backslashes (including double backslash & trailing backslash before comma)
    assert by_id["42623512"]["case_name"] == r"Elouis ]\\ Gardner"
    assert by_id["23966168"]["case_name"] == "Anna M. N. Anna Maria Nicholson McNamara\\"
    assert by_id["28384475"]["case_name"] == r"Oakes v. C\O Thomas Barnett"

    # 4. Leading-zero docket_number_core & NULL date_terminated & UTC microsecond timestamps
    assert by_id["55046985"]["docket_number_core"] == "0409593"
    r_open = by_id["17184572"]
    assert r_open["date_terminated"] is None
    assert r_open["date_created"] == datetime(2020, 5, 21, 20, 32, 53, 687951, tzinfo=UTC)
    assert r_open["date_modified"] == datetime(2022, 8, 23, 7, 31, 34, 719100, tzinfo=UTC)


def test_edge_values_null_vs_empty_sentinels_leading_zeros_and_pre1900_dates(tmp_path):
    bz2_path, expected_sha, _ = _write_bz2(tmp_path / "edge.csv.bz2", _edge_fixture_csv())
    out_path, stats = module.build_courtlistener_bankruptcy_dockets(
        bz2_path,
        tmp_path / "out",
        piece_bytes=4096,
        parse_workers=2,
        decompression_threads=1,
    )
    assert pq.read_schema(out_path).equals(module.SCHEMA)
    assert stats["rows"] == 6
    assert stats["docket_number_null"] == 1
    assert stats["docket_number_empty"] == 3
    assert stats["blocked_true"] == 1
    assert stats["blocked_false"] == 5
    assert stats["non_fb_rows"] == 5
    assert stats["source_sha256"] == expected_sha

    # Verify via DuckDB DESCRIBE + SQL reconciliation queries matching blueprint §11.
    with duckdb.connect() as con:
        con.execute("SET TimeZone = 'UTC'")
        described = [(row[0], row[1]) for row in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(out_path)]).fetchall()]
        assert described == described_schema(module.SCHEMA)

        row = con.execute(
            """
            SELECT
                COUNT(*),
                COUNT(DISTINCT cl_docket_id),
                COUNT(*) FILTER (WHERE docket_number IS NULL),
                COUNT(*) FILTER (WHERE docket_number = ''),
                COUNT(*) FILTER (WHERE blocked),
                COUNT(*) FILTER (WHERE NOT blocked),
                COUNT(*) FILTER (WHERE pacer_case_id = 'null'),
                COUNT(*) FILTER (WHERE pacer_case_id = 'undefined'),
                COUNT(*) FILTER (WHERE pacer_case_id IS NULL),
                COUNT(*) FILTER (WHERE docket_number_core LIKE '0%'),
                MIN(date_filed),
                MAX(date_filed),
                MIN(date_terminated),
                MAX(date_terminated),
                COUNT(*) FILTER (WHERE case_name_short = 'NULL'),
                COUNT(*) FILTER (WHERE case_name_short = 'N/A')
            FROM read_parquet(?)
            """,
            [str(out_path)],
        ).fetchone()
        assert row == (
            6,
            6,
            1,
            3,
            1,
            5,
            1,
            1,
            1,
            4,
            date(1871, 10, 12),
            date(2079, 11, 23),
            date(205, 2, 25),
            date(2097, 2, 13),
            1,
            1,
        )


@pytest.mark.parametrize(
    ("mutation", "pattern"),
    [
        ("extra_header_col", "header differs"),
        ("reordered_header", "header differs"),
        ("duplicate_id", "Duplicate cl_docket_id"),
        ("zero_id", "bad_id"),
        ("leading_zero_id", "bad_id"),
        ("null_id", "bad_id"),
        ("empty_court_id", "bad_court_id"),
        ("null_court_id", "bad_court_id"),
        ("bad_blocked_word", "bad_blocked"),
        ("null_blocked", "bad_blocked"),
        ("invalid_calendar_date_filed", "bad_date_filed"),
        ("empty_string_date_filed", "bad_date_filed"),
        ("invalid_date_terminated", "bad_date_terminated"),
        ("invalid_date_blocked", "bad_date_blocked"),
        ("non_utc_date_created", "bad_date_created"),
        ("iso_t_date_modified", "bad_date_modified"),
        ("bad_assigned_to_id", "bad_assigned_to_id"),
        ("bad_referred_to_id", "bad_referred_to_id"),
        ("null_case_name", "null_case_name"),
        ("null_assigned_to_str", "null_assigned_to_str"),
        ("null_docket_number_core", "null_docket_number_core"),
        ("zero_rows", "zero data rows"),
        ("bad_sha", "differs from expected source_sha256"),
    ],
)
def test_fail_closed_guards_refuse_and_preserve_prior_output(tmp_path, mutation, pattern):
    base_row = (
        '2001,2021-01-03 04:28:21.336035+00,2021-01-03 04:28:21.386803+00,'
        'Judge Name,"",2002-10-31,2003-03-07,"",Case Name,"",02-05494,"",73771,2021-01-02,t,8925,oknb,'
    )
    header = HEADER
    rows = [base_row]
    override_sha: str | None = None

    if mutation == "extra_header_col":
        header = HEADER + ",extra"
        rows = [base_row + ","]
    elif mutation == "reordered_header":
        cols = list(module.SOURCE_COLUMNS)
        cols[0], cols[1] = cols[1], cols[0]
        header = ",".join(cols)
        parts = base_row.split(",")
        parts[0], parts[1] = parts[1], parts[0]
        rows = [",".join(parts)]
    elif mutation == "duplicate_id":
        rows = [base_row, base_row]
    elif mutation == "zero_id":
        rows = [base_row.replace("2001,", "0,", 1)]
    elif mutation == "leading_zero_id":
        rows = [base_row.replace("2001,", "02001,", 1)]
    elif mutation == "null_id":
        rows = [base_row.replace("2001,", ",", 1)]
    elif mutation == "empty_court_id":
        rows = [base_row.replace(",oknb,", ',"",', 1)]
    elif mutation == "null_court_id":
        rows = [base_row.replace(",oknb,", ",,", 1)]
    elif mutation == "bad_blocked_word":
        rows = [base_row.replace(",t,8925,", ",true,8925,", 1)]
    elif mutation == "null_blocked":
        rows = [base_row.replace(",t,8925,", ",,8925,", 1)]
    elif mutation == "invalid_calendar_date_filed":
        rows = [base_row.replace("2002-10-31", "2002-02-30", 1)]
    elif mutation == "empty_string_date_filed":
        rows = [base_row.replace("2002-10-31", '""', 1)]
    elif mutation == "invalid_date_terminated":
        rows = [base_row.replace("2003-03-07", "2003/03/07", 1)]
    elif mutation == "invalid_date_blocked":
        rows = [base_row.replace("2021-01-02", "2021-13-02", 1)]
    elif mutation == "non_utc_date_created":
        rows = [base_row.replace("2021-01-03 04:28:21.336035+00", "2021-01-03 04:28:21.336035-05", 1)]
    elif mutation == "iso_t_date_modified":
        rows = [base_row.replace("2021-01-03 04:28:21.386803+00", "2021-01-03T04:28:21.386803Z", 1)]
    elif mutation == "bad_assigned_to_id":
        rows = [base_row.replace(",8925,oknb,", ",08925,oknb,", 1)]
    elif mutation == "bad_referred_to_id":
        rows = [base_row + "abc"]
    elif mutation == "null_case_name":
        rows = [base_row.replace(",Case Name,", ",,", 1)]
    elif mutation == "null_assigned_to_str":
        rows = [base_row.replace(",Judge Name,", ",,", 1)]
    elif mutation == "null_docket_number_core":
        rows = [base_row.replace(',02-05494,"",', ",02-05494,,", 1)]
    elif mutation == "zero_rows":
        rows = []
    elif mutation == "bad_sha":
        override_sha = "sha256:" + "0" * 64

    csv_text = "\n".join([header, *rows]) + "\n"
    bz2_path, _, _ = _write_bz2(tmp_path / f"{mutation}.csv.bz2", csv_text)

    out_dir = tmp_path / "out"
    out_dir.mkdir()
    prior = out_dir / module.OUTPUT
    prior.write_bytes(b"prior-parquet-sentinel")

    with pytest.raises(ValueError, match=pattern):
        module.build_courtlistener_bankruptcy_dockets(
            bz2_path,
            out_dir,
            source_sha256=override_sha,
            piece_bytes=4096,
            parse_workers=2,
            decompression_threads=1,
        )
    assert prior.read_bytes() == b"prior-parquet-sentinel"
    assert not list(out_dir.glob(".*.tmp"))


def test_pipeline_build_publish_dry_run_mcp_and_failure_sealing(tmp_path, monkeypatch):
    monkeypatch.setattr(
        shutil,
        "disk_usage",
        lambda _: SimpleNamespace(free=local_publication.DRY_RUN_FREE_FLOOR_BYTES + 10 * 1024**3),
    )

    raw_root = tmp_path / "isolated-raw"
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(raw_root))
    store = CentralRawArchiveStore(raw_root, create=True)
    bz2_bytes = bz2.compress(_edge_fixture_csv().encode("utf-8"))
    sha = "sha256:" + hashlib.sha256(bz2_bytes).hexdigest()
    store.put_blob(sha, len(bz2_bytes), [bz2_bytes])

    build_dir = tmp_path / "candidate"
    artifact, evidence_dir, stats = pipeline.build(
        build_dir,
        archive_root=raw_root,
        sha256=sha,
        byte_size=len(bz2_bytes),
        piece_bytes=4096,
        parse_workers=2,
        decompression_threads=1,
    )
    assert artifact.pin.artifact_digest.startswith("sha256:")
    assert stats["rows"] == 6
    assert stats["docket_number_null"] == 1
    assert stats["docket_number_empty"] == 3
    assert stats["non_fb_rows"] == 5

    # Zero-copy adoption moved tables/courtlistener_bankruptcy_dockets.parquet into generation/.
    assert not (build_dir / "tables" / module.OUTPUT).exists()
    gen_parquet = build_dir / "generation" / module.OUTPUT
    assert gen_parquet.is_file()
    assert pq.read_schema(gen_parquet).equals(module.SCHEMA)

    # Journal records retained-file in central-raw + courtlistener-bankruptcy-build event, with no local paths.
    journal_events = [json.loads(line) for line in (evidence_dir / "journal.jsonl").read_text().splitlines()]
    retained_events = [e for e in journal_events if e["event"] == "retained-file"]
    assert len(retained_events) == 1
    assert retained_events[0]["stage"] == pipeline.STAGE_EXPORT
    assert retained_events[0]["blob_store"] == "central-raw"
    assert retained_events[0]["source_url"] == module.PUBLISHER_URL
    assert retained_events[0]["original_location"] == module.PUBLISHER_URL
    assert retained_events[0]["etag"] == module.EXPECTED_ETAG
    assert retained_events[0]["dataset_id"] == module.FAMILY
    assert retained_events[0]["source_version"] == module.DUMP_DATE
    build_events = [e for e in journal_events if e["event"] == pipeline.BUILD_EVENT]
    assert len(build_events) == 1
    assert build_events[0]["rows"] == 6
    assert build_events[0]["docket_number_null"] == 1
    assert build_events[0]["docket_number_empty"] == 3

    # Receipts-free publication into an in-memory Store.
    index = publication.publish_generation(
        build_dir / "generation",
        client=Store(),
        bucket="fixture",
        prior_index=publication.empty_index(),
        evidence_directories=(evidence_dir,),
    )
    entry = index["families"][module.FAMILY]
    assert entry["artifactDigest"] == artifact.pin.artifact_digest
    assert entry.get("etlReceipts") is None
    assert set(entry["tables"]) == {module.OUTPUT}
    assert entry["tables"][module.OUTPUT]["rows"] == 6
    assert entry["tables"][module.OUTPUT]["columns"] == [list(col) for col in described_schema(module.SCHEMA)]

    # Explorer metadata bundle documents the table cleanly.
    bundle = build_bundle(index)
    meta = bundle["tables"][module.TABLE]
    assert meta["metadataStatus"] == meta["sourceStatus"] == "documented"
    assert meta["recordIdentity"]["columns"] == ["cl_docket_id"]
    assert meta["family"] == module.FAMILY
    assert [src["id"] for src in meta["sources"]] == ["courts"]

    # publish-local-generation --dry-run --scratch-raw accepts the candidate receipts-free.
    live = tmp_path / "live-publication.v2.json"
    live.write_text(json.dumps(publication.empty_index()))
    dry_out = tmp_path / "dry-run-out"
    local_publication.main(
        [
            "--generation",
            str(build_dir / "generation"),
            "--family",
            module.FAMILY,
            "--evidence",
            str(evidence_dir),
            "--output",
            str(dry_out),
            "--dry-run",
            "--scratch-raw",
            "--live-index",
            str(live),
        ]
    )
    receipt = json.loads((dry_out / "dry-run.json").read_text())
    assert receipt["status"] == "dry_run_accepted"
    assert receipt["family"] == module.FAMILY
    assert receipt["etlReceipts"] is None
    assert receipt["tables"][module.OUTPUT]["rows"] == 6

    # Local MCP connection queries the typed table cleanly.
    monkeypatch.setattr(mcp_server, "DATA_DIR", build_dir / "generation")
    with mcp_server._build_connection() as con:
        res = con.execute(
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE docket_number IS NULL),
                   COUNT(*) FILTER (WHERE docket_number = ''),
                   COUNT(*) FILTER (WHERE blocked)
            FROM courtlistener_bankruptcy_dockets
            """
        ).fetchone()
        assert res == (6, 1, 3, 1)

    # Fresh output directory guard refuses reuse.
    with pytest.raises(ValueError, match="fresh output directory"):
        pipeline.build(build_dir, archive_root=raw_root, sha256=sha, byte_size=len(bz2_bytes))

    # Wrong byte_size fails closed and seals failure outcome.
    failed_dir = tmp_path / "failed"
    with pytest.raises(Exception):
        pipeline.build(failed_dir, archive_root=raw_root, sha256=sha, byte_size=len(bz2_bytes) + 1)
    outcomes = list(failed_dir.glob("source-evidence/*/run-outcome.json"))
    assert len(outcomes) == 1
    assert json.loads(outcomes[0].read_text())["outcome"] != "build-complete"


def test_registration_parity_and_receipts_free_invariants():
    from spicy_regs import etl_policy_registry

    # No etl_policies JSON for this receipts-free local family.
    assert not (REPO_ROOT / "src" / "spicy_regs" / "etl_policies" / f"{module.TABLE}.json").exists()
    assert module.TABLE not in etl_policy_registry.installed_policies()

    # local_publication registration
    assert module.FAMILY in local_publication.LOCAL_FAMILIES
    schemas = local_publication._family_schemas(module.FAMILY)
    assert schemas is not None and schemas == {module.TABLE: module.SCHEMA}

    # data_dictionary + mcp_server parity
    assert module.TABLE in dd.TABLES
    assert module.TABLE in dd.MCP_QUERYABLE
    assert module.TABLE in mcp_server.TABLES
    assert dd.expected_schemas()[module.TABLE] == described_schema(module.SCHEMA)

    descriptions = dd.load_descriptions()
    entry = descriptions[module.TABLE]
    assert entry["subject"] == "courts"
    assert entry["identity_columns"] == ["cl_docket_id"]
    assert entry["coverage"].startswith("Not a range.")
    assert dd.coverage_prose_errors(module.TABLE, entry["coverage"], str(entry["measured_on"])) == []
    assert dd.data_quality_prose_errors(module.TABLE, entry["data_quality"]) == []
    assert dd.receipt_pointers(entry) == []
    assert "neb" in entry["data_quality"] and "mspb" in entry["data_quality"]

    # explorer_sources.json, odcs_contracts, and check_rollup_freshness
    explorer_sources = json.loads((REPO_ROOT / "src/spicy_regs/explorer_sources.json").read_text())
    assert explorer_sources["families"][module.FAMILY]["sources"] == ["courts"]
    assert (
        explorer_sources["families"][module.FAMILY]["evidence"]
        == "src/spicy_regs/transforms/build_courtlistener_bankruptcy_dockets.py"
    )
    assert odcs_contracts.FAMILY_TABLES[module.FAMILY] == (module.TABLE,)
    assert odcs_contracts.FAMILY_DOMAIN_ASSIGNMENTS[module.FAMILY] == ("spicygov", ())
    cadence = freshness.FAMILY_CADENCE[module.FAMILY]
    assert cadence.months is None and cadence.input_pin is None

    # managed_only prevents bare legacy fallback when not in publication index
    with pytest.MonkeyPatch.context() as mp:
        from spicy_regs.sources import publication as pub
        from tests.test_generation_mcp import connection_fixture

        mp.setattr(mcp_server, "DATA_DIR", None)
        built = connection_fixture(mp, pub.empty_index(), {}, tables=(module.TABLE,))
        mcp_server._build_connection()
        (con,) = built
        try:
            assert con.seen == []
            assert module.TABLE not in mcp_server._available_tables(con.cursor())
        finally:
            con.close()
