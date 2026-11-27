"""NPPES V2 lifecycle, sidecar replacement, atomic refusal, and MCP serving tests."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import zipfile
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest

from spicy_docs.sources.nppes import NppesSourceError, read_csv_cohort
from spicy_regs import explorer_metadata, local_publication, mcp_server
from spicy_regs.pipelines import nppes_providers as pipeline
from spicy_regs.sources import publication
from spicy_regs.transforms.build_nppes_providers import (
    FAMILY,
    OUTPUT,
    SCHEMA,
    TABLE,
    MainRelease,
    apply_main_release,
    apply_v2_release,
    build_observations,
    build_observations_multi,
    classify_v2_main_state,
    read_v2_csv_cohort,
    read_v2_zip_member,
    snapshot_rows,
    v2_snapshot_sidecar_rows,
)
from tests.generation_fakes import Store
from tests.test_mcp_server import _records, _tool_data

NPIS = ("1851806699", "1699600866", "1669740403")
HEADER = (
    "NPI",
    "Entity Type Code",
    "Provider Organization Name (Legal Business Name)",
    "Provider Last Name (Legal Name)",
    "Provider First Name",
    "NPI Deactivation Reason Code",
    "NPI Deactivation Date",
    "NPI Reactivation Date",
)
LEGACY_HEADER = (
    "NPI",
    "Entity Type Code",
    "Provider Organization Name (Legal Business Name)",
    "Provider Last Name (Legal Name)",
    "Provider First Name",
    "NPI Deactivation Date",
    "NPI Reactivation Date",
)
OTHERNAME_HEADER = (
    "NPI",
    "Provider Other Organization Name",
    "Provider Other Organization Name Type Code",
    "Created Date",
)
PL_HEADER = (
    "NPI",
    "Provider Secondary Practice Location Address- Address Line 1",
    "Provider Secondary Practice Location Address - City Name",
    "Provider Secondary Practice Location Address - State Name",
)
ENDPOINT_HEADER = (
    "NPI",
    "Endpoint Type",
    "Endpoint",
)


def _validate_synthetic_npi(value: str) -> str:
    if value not in NPIS:
        raise ValueError("NPI is outside the synthetic test set")
    return value


def _csv_bytes(columns: tuple[str, ...], rows: list[list[str]]) -> bytes:
    text = io.StringIO(newline="")
    csv.writer(text).writerows([columns, *rows])
    return text.getvalue().encode("utf-8")


def parsed(rows, *, scope="member", columns=LEGACY_HEADER, kind="main"):
    payload = _csv_bytes(columns, rows)
    return read_csv_cohort(
        io.BytesIO(payload),
        kind=kind,
        scope=scope,
        source_url="https://example.test/synthetic.zip",
        member_name="synthetic.csv",
        expected_sha256="sha256:" + hashlib.sha256(payload).hexdigest(),
        expected_bytes=len(payload),
        columns=columns,
        cohort=NPIS,
        validate_npi=_validate_synthetic_npi,
        max_scanned_rows=20,
        max_selected_rows=20,
    )


def parsed_v2(
    rows,
    *,
    scope="member",
    columns=HEADER,
    kind="main",
    source_url="https://download.cms.gov/nppes/synthetic_v2.zip",
    member_name="npidata_pfile_20261109-20261115.csv",
):
    payload = _csv_bytes(columns, rows)
    return read_v2_csv_cohort(
        io.BytesIO(payload),
        kind=kind,
        scope=scope,
        source_url=source_url,
        member_name=member_name,
        expected_sha256="sha256:" + hashlib.sha256(payload).hexdigest(),
        expected_bytes=len(payload),
        columns=columns,
        cohort=NPIS,
        validate_npi=_validate_synthetic_npi,
        max_scanned_rows=20,
        max_selected_rows=20,
    )


def release(rows, *, kind="monthly", start=1, end=11, predecessor=None, scope="member"):
    return MainRelease(
        f"synthetic-{start}-{end}-{kind}",
        kind,
        date(2026, 10, start),
        date(2026, 10, end),
        predecessor,
        parsed(rows, scope=scope),
    )


def release_v2(rows, *, kind="monthly", start=9, end=15, predecessor=None, scope="member", member_name=None):
    member = member_name or f"npidata_pfile_202611{start:02d}-202611{end:02d}.csv"
    return MainRelease(
        f"v2-{start}-{end}-{kind}",
        kind,
        date(2026, 11, start),
        date(2026, 11, end),
        predecessor,
        parsed_v2(rows, scope=scope, member_name=member),
    )


def test_replacement_sparse_deactivation_reactivation_and_monthly_reset():
    baseline = apply_main_release(
        None,
        release(
            [
                [NPIS[0], "1", "", "OLD", "  Literal  ", "", ""],
                [NPIS[1], "2", "000 ORGANIZATION", "", "", "", ""],
            ]
        ),
    )
    week1 = release(
        [[NPIS[0], "", "", "", "", "10/12/2026", ""]],
        kind="weekly",
        start=12,
        end=18,
        predecessor=baseline.release.digest,
    )
    inactive = apply_main_release(baseline, week1)
    rows = list(snapshot_rows(inactive))
    assert rows[0]["fields"]["Provider Last Name (Legal Name)"] == ""
    assert rows[0]["fields"]["Entity Type Code"] == ""
    assert rows[0]["lifecycle_state"] == "deactivated"
    assert rows[1]["source_sha256"] == baseline.release.csv_input.sha256
    assert list(snapshot_rows(baseline))[0]["fields"]["Provider Last Name (Legal Name)"] == "OLD"
    week2 = release(
        [[NPIS[0], "1", "", "NEW", "", "", "10/19/2026"]],
        kind="weekly",
        start=19,
        end=25,
        predecessor=inactive.release.digest,
    )
    active = apply_main_release(inactive, week2)
    assert list(snapshot_rows(active))[0]["fields"]["Provider First Name"] == ""
    assert list(snapshot_rows(active))[0]["fields"]["Provider Last Name (Legal Name)"] == "NEW"
    assert list(snapshot_rows(active))[0]["lifecycle_state"] == "reactivated"
    reset = apply_main_release(active, release([[NPIS[2], "2", "NEW ORG", "", "", "", ""]], end=31))
    assert [r["npi"] for r in snapshot_rows(reset)] == [NPIS[2]]


@pytest.mark.parametrize(
    "failure", ["missing-predecessor", "changed-predecessor", "gap", "overlap", "excerpt", "sidecar"]
)
def test_incomplete_sequence_and_unknown_sidecar_rules_refuse_atomically(failure):
    baseline = apply_main_release(None, release([[NPIS[0], "1", "", "OLD", "", "", ""]]))
    before = tuple(snapshot_rows(baseline))
    update = release(
        [[NPIS[0], "1", "", "NEW", "", "", ""]],
        kind="weekly",
        start=13 if failure == "gap" else 11 if failure == "overlap" else 12,
        end=18,
        predecessor="sha256:changed" if failure == "changed-predecessor" else baseline.release.digest,
        scope="excerpt" if failure == "excerpt" else "member",
    )
    with pytest.raises(ValueError):
        apply_main_release(
            None if failure == "missing-predecessor" else baseline,
            update,
            sidecars=(object(),) if failure == "sidecar" else (),
        )
    assert tuple(snapshot_rows(baseline)) == before


def test_observation_schema_preserves_source_empty_vs_missing(tmp_path):
    data = parsed([[NPIS[0], "1", "", "LITERAL", "", "", ""]], scope="excerpt")
    path = build_observations(data, tmp_path / "nppes.parquet")
    row = pq.read_table(path).to_pylist()[0]
    assert row["scope"] == "excerpt" and row["organization_name"] == "" and row["first_name"] == ""
    sidecar = parsed([[NPIS[0], ""], [NPIS[0], ""]], kind="othername", columns=("NPI", "Other name"))
    sidecar_path = build_observations(sidecar, tmp_path / "sidecars.parquet")
    rows = pq.read_table(sidecar_path).to_pylist()
    assert len(rows) == 2 and all(r["entity_type_code"] is None for r in rows)
    assert [r["record_number"] for r in rows] == [2, 3]


def test_v2_combined_deactivation_reactivation_and_refusals():
    # Live V2 reactivations keep historical NPI Deactivation Date alongside NPI Reactivation Date.
    reactivated = {
        "NPI": NPIS[0],
        "Entity Type Code": "2",
        "Provider Organization Name (Legal Business Name)": "CHIROS ON 54 LLC",
        "Provider Last Name (Legal Name)": "",
        "Provider First Name": "",
        "NPI Deactivation Reason Code": "",
        "NPI Deactivation Date": "11/10/2026",
        "NPI Reactivation Date": "11/17/2026",
    }
    assert classify_v2_main_state(reactivated) == "reactivated"
    same_day = {**reactivated, "NPI Reactivation Date": "11/10/2026"}
    assert classify_v2_main_state(same_day) == "reactivated"

    # Impossible chronology (reactivation before deactivation) refuses.
    with pytest.raises(ValueError, match="precedes"):
        classify_v2_main_state({**reactivated, "NPI Reactivation Date": "11/09/2026"})

    # Sparse deactivated row disclosing any extra field (including non-public reason code) refuses.
    deact_extra = {
        "NPI": NPIS[0],
        "Entity Type Code": "",
        "Provider Organization Name (Legal Business Name)": "",
        "Provider Last Name (Legal Name)": "",
        "Provider First Name": "",
        "NPI Deactivation Reason Code": "DT",
        "NPI Deactivation Date": "11/10/2026",
        "NPI Reactivation Date": "",
    }
    with pytest.raises(ValueError, match="discloses fields beyond"):
        classify_v2_main_state(deact_extra)


def test_v2_sidecar_replacement_removal_carry_forward_and_deactivated_tagging():
    m1 = release_v2(
        [
            [NPIS[0], "2", "ORG ONE", "", "", "", "", ""],
            [NPIS[1], "1", "", "POTTER", "NICHOLAS", "", "", ""],
            [NPIS[2], "", "", "", "", "", "11/10/2026", ""],
        ],
        kind="monthly",
        start=9,
        end=15,
    )
    on1 = parsed_v2(
        [
            [NPIS[0], "DBA ALPHA", "3", "06/01/2017"],
            [NPIS[0], "DBA BETA", "3", "03/05/2019"],
            # Deactivated NPI still emitted in CMS sidecar export!
            [NPIS[2], "HISTORICAL DBA", "3", "02/24/2020"],
        ],
        kind="othername",
        columns=OTHERNAME_HEADER,
        member_name="othername_pfile_20261109-20261115.csv",
    )
    pl1 = parsed_v2(
        [
            [NPIS[0], "100 MAIN ST", "BILLINGS", "MT"],
            [NPIS[1], "200 OAK AVE", "LOUISVILLE", "KY"],
        ],
        kind="practice_location",
        columns=PL_HEADER,
        member_name="pl_pfile_20261109-20261115.csv",
    )
    snap1 = apply_v2_release(None, m1, sidecars=(on1, pl1))

    # Week 2 touches NPIS[0] (updates othername, removes all pl) and reactivates NPIS[2] with BOTH dates!
    # NPIS[1] is untouched in week 2, so its pl row must carry forward untouched.
    m2 = release_v2(
        [
            [NPIS[0], "2", "ORG ONE RENAMED", "", "", "", "", ""],
            [NPIS[2], "2", "RESTORED CLINIC", "", "", "", "11/10/2026", "11/17/2026"],
        ],
        kind="weekly",
        start=16,
        end=22,
        predecessor=m1.digest,
    )
    on2 = parsed_v2(
        [
            [NPIS[0], "DBA ALPHA", "3", "06/01/2017"],
            [NPIS[0], "DBA GAMMA", "3", "11/20/2026"],
            [NPIS[2], "RESTORED DBA", "3", "11/17/2026"],
        ],
        kind="othername",
        columns=OTHERNAME_HEADER,
        member_name="othername_pfile_20261116-20261122.csv",
    )
    # Empty pl2 removes NPIS[0]'s prior practice location while carrying forward untouched NPIS[1]!
    pl2 = parsed_v2(
        [],
        kind="practice_location",
        columns=PL_HEADER,
        member_name="pl_pfile_20261116-20261122.csv",
    )
    snap2 = apply_v2_release(snap1, m2, sidecars=(on2, pl2))

    # Snap1 remained immutable and tagged deactivated NPIS[2]'s sidecar explicitly.
    snap1_sc = {(r["kind"], r["npi"]): r["main_lifecycle_state"] for r in v2_snapshot_sidecar_rows(snap1)}
    assert snap1_sc[("othername", NPIS[2])] == "deactivated"

    # Snap2 reflects whole-set replacement, removal, untouched carry-forward, and combined-date reactivation.
    main2 = {r["npi"]: r for r in snapshot_rows(snap2)}
    assert main2[NPIS[2]]["lifecycle_state"] == "reactivated"
    assert main2[NPIS[2]]["fields"]["NPI Deactivation Date"] == "11/10/2026"
    assert main2[NPIS[2]]["fields"]["NPI Reactivation Date"] == "11/17/2026"

    sc2 = list(v2_snapshot_sidecar_rows(snap2))
    on_names = [(r["npi"], r["fields"]["Provider Other Organization Name"], r["main_lifecycle_state"]) for r in sc2 if r["kind"] == "othername"]
    assert on_names == [
        (NPIS[0], "DBA ALPHA", "active"),
        (NPIS[0], "DBA GAMMA", "active"),
        (NPIS[2], "RESTORED DBA", "reactivated"),
    ]
    pl_npis = [r["npi"] for r in sc2 if r["kind"] == "practice_location"]
    assert pl_npis == [NPIS[1]]  # NPIS[0] removed; untouched NPIS[1] carried forward!

    # Orphan weekly sidecar referencing an NPI absent from that week's main CSV refuses atomically!
    before = tuple(v2_snapshot_sidecar_rows(snap2))
    m3 = release_v2(
        [[NPIS[0], "2", "ORG ONE", "", "", "", "", ""]],
        kind="weekly",
        start=23,
        end=29,
        predecessor=m2.digest,
    )
    orphan_ep = parsed_v2(
        [[NPIS[1], "DIRECT", "doc@example.org"]],
        kind="endpoint",
        columns=ENDPOINT_HEADER,
        member_name="endpoint_pfile_20261123-20261129.csv",
    )
    with pytest.raises(ValueError, match="absent from week's main CSV"):
        apply_v2_release(snap2, m3, sidecars=(orphan_ep,))
    assert tuple(v2_snapshot_sidecar_rows(snap2)) == before


@pytest.mark.parametrize(
    "fault",
    ["malformed-header", "duplicate-npi", "bad-width", "changed-zip-hash", "missing-member", "corrupt-zip"],
)
def test_atomic_refusal_on_malformed_csv_and_changed_archives(tmp_path: Path, fault: str):
    valid_rows = [
        [NPIS[0], "1", "", "POTTER", "NICHOLAS", "", "", ""],
        [NPIS[1], "2", "CLINIC LLC", "", "", "", "", ""],
    ]
    if fault == "malformed-header":
        csv_bytes = _csv_bytes(("WRONG_COL", *HEADER[1:]), valid_rows)
    elif fault == "duplicate-npi":
        csv_bytes = _csv_bytes(HEADER, [valid_rows[0], valid_rows[0]])
    elif fault == "bad-width":
        csv_bytes = _csv_bytes(HEADER, [valid_rows[0][:-1]])
    else:
        csv_bytes = _csv_bytes(HEADER, valid_rows)

    member_name = "npidata_pfile_20261109-20261115.csv"
    zip_path = tmp_path / "weekly.zip"
    if fault == "corrupt-zip":
        zip_path.write_bytes(b"not-a-valid-zip-archive")
    else:
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("other.csv" if fault == "missing-member" else member_name, csv_bytes)

    raw_zip = zip_path.read_bytes()
    zip_sha = "sha256:" + ("0" * 64 if fault == "changed-zip-hash" else hashlib.sha256(raw_zip).hexdigest())
    member_sha = "sha256:" + hashlib.sha256(csv_bytes).hexdigest()

    prior = tmp_path / OUTPUT
    prior.write_bytes(b"prior-parquet-bytes")

    with pytest.raises(NppesSourceError):
        data = read_v2_zip_member(
            zip_path,
            archive_sha256=zip_sha,
            archive_bytes=len(raw_zip),
            kind="main",
            scope="member",
            source_url="https://download.cms.gov/nppes/weekly.zip",
            member_name=member_name,
            member_sha256=member_sha,
            member_bytes=len(csv_bytes),
            columns=HEADER,
            cohort=NPIS,
            validate_npi=_validate_synthetic_npi,
            max_scanned_rows=20,
            max_selected_rows=20,
        )
        build_observations(data, prior)

    assert prior.read_bytes() == b"prior-parquet-bytes"


def test_serving_registration_pipeline_and_mcp_duckdb_readback(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "scratch-raw"))
    assert FAMILY in local_publication.LOCAL_FAMILIES
    assert local_publication._family_schemas(FAMILY) == {TABLE: SCHEMA}
    assert TABLE in mcp_server.TABLES

    main_csv = parsed_v2(
        [
            [NPIS[0], "1", "", "POTTER", "NICHOLAS", "", "", ""],
            [NPIS[1], "2", "CHIROS ON 54 LLC", "", "", "", "11/10/2026", "11/17/2026"],
            [NPIS[2], "", "", "", "", "", "11/16/2026", ""],
        ],
        member_name="npidata_pfile_20261116-20261122.csv",
    )
    sidecar_csv = parsed_v2(
        [[NPIS[1], "CHIROS DBA", "3", "11/17/2026"], [NPIS[2], "OLD DBA", "3", "02/24/2020"]],
        kind="othername",
        columns=OTHERNAME_HEADER,
        member_name="othername_pfile_20261116-20261122.csv",
    )
    raw_zip = tmp_path / "weekly_v2.zip"
    with zipfile.ZipFile(raw_zip, "w") as zf:
        zf.writestr("marker.txt", b"fixture-archive")

    artifact, evidence = pipeline.build(
        (main_csv, sidecar_csv),
        tmp_path / "out",
        raw_files=[(raw_zip, "https://download.cms.gov/nppes/NPPES_Data_Dissemination_111626_112226_Weekly_V2.zip", "weekly-zip")],
        release_metadata={"period": "2026-11-16..2026-11-22"},
    )
    assert artifact.pin.artifact_digest and evidence.exists()
    # No local machine paths leak into published source evidence.
    assert str(tmp_path) not in (evidence / "journal.jsonl").read_text()

    gen_dir = tmp_path / "out" / "generation"
    root = local_publication._candidate(gen_dir, FAMILY, (evidence,))
    assert root["spec"]["family"] == FAMILY

    # Verify dry-run acceptance under mocked headroom + explorer metadata resolution.
    live_index = tmp_path / "publication.v2.json"
    live_index.write_text(json.dumps(publication.empty_index()))
    dry_out = tmp_path / "dry-run"
    args = SimpleNamespace(
        generation=gen_dir,
        family=FAMILY,
        evidence=[evidence],
        added_table=[],
        expect_prior=None,
        output=dry_out,
        live_index=live_index,
        scratch_raw=True,
    )
    receipt = local_publication.dry_run(
        args,
        root,
        disk_usage=lambda _: SimpleNamespace(free=local_publication.DRY_RUN_FREE_FLOOR_BYTES + 10 * 1024**3),
    )
    assert receipt["status"] == "dry_run_accepted"
    assert receipt["tables"][OUTPUT]["rows"] == 5

    index = publication.publish_generation(
        gen_dir,
        client=Store(),
        bucket="fixture",
        prior_index=publication.empty_index(),
        evidence_directories=(evidence,),
    )
    meta = explorer_metadata.build_bundle(index)["tables"][TABLE]
    assert meta["metadataStatus"] == meta["sourceStatus"] == "documented"
    assert meta["sources"][0]["id"] == "cms-nppes"
    assert meta["recordIdentity"]["columns"] == ["source_sha256", "source_member", "record_number"]

    # Verify MCP describe_table and DuckDB query_sql readback keeping NPI strictly distinct from CCN
    # and preserving unmatched rows!
    monkeypatch.setattr(mcp_server, "DATA_DIR", gen_dir)
    con = mcp_server._build_connection()
    monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
    try:
        server = mcp_server.build_server()
        described = _tool_data(server, "describe_table", {"table": TABLE})
        assert described["table"] == TABLE and len(described["columns"]) == len(SCHEMA)

        joined = _tool_data(
            server,
            "query_sql",
            {
                "sql": f"""
                WITH requests(request_id, identifier_kind, identifier) AS (
                    VALUES
                        ('1-npi-active', 'NPI', '{NPIS[0]}'),
                        ('2-npi-reactivated', 'NPI', '{NPIS[1]}'),
                        ('3-npi-deactivated', 'NPI', '{NPIS[2]}'),
                        ('4-ccn-same-digits', 'CCN', '{NPIS[0]}'),
                        ('5-ccn-leading-zero', 'CCN', '010001'),
                        ('6-npi-unobserved', 'NPI', '1234567893')
                )
                SELECT r.request_id, r.identifier_kind, r.identifier,
                       p.npi IS NOT NULL AS matched,
                       p.entity_type_code, p.deactivation_date, p.reactivation_date
                FROM requests r
                LEFT JOIN nppes_provider_observations p
                  ON p.kind = 'main' AND r.identifier_kind = 'NPI' AND r.identifier = p.npi
                ORDER BY r.request_id
                """
            },
        )
        rows = _records(joined)
    finally:
        con.close()
    assert len(rows) == 6
    by_id = {row["request_id"]: row for row in rows}
    assert by_id["1-npi-active"]["matched"] is True and by_id["1-npi-active"]["entity_type_code"] == "1"
    assert by_id["2-npi-reactivated"]["matched"] is True
    assert by_id["2-npi-reactivated"]["deactivation_date"] == "11/10/2026"
    assert by_id["2-npi-reactivated"]["reactivation_date"] == "11/17/2026"
    assert by_id["3-npi-deactivated"]["matched"] is True and by_id["3-npi-deactivated"]["entity_type_code"] == ""
    assert by_id["4-ccn-same-digits"]["matched"] is False
    assert by_id["5-ccn-leading-zero"]["matched"] is False
    assert by_id["6-npi-unobserved"]["matched"] is False

    # Duplicate observation identity refuses atomically without corrupting destination.
    dup_target = tmp_path / "dup.parquet"
    dup_target.write_bytes(b"intact")
    with pytest.raises(ValueError, match="Duplicate NPPES observation identity"):
        build_observations_multi((main_csv, main_csv), dup_target)
    assert dup_target.read_bytes() == b"intact"
