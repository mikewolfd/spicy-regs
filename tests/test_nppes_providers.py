"""NPPES V2 lifecycle, sidecar replacement, atomic refusal, and MCP serving tests."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import zipfile
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import LocalMemberSource, admit_artifact

from spicy_docs.sources.nppes import NppesSourceError, read_csv_cohort
from spicy_regs import explorer_metadata, local_publication, mcp_server
from spicy_regs.pipelines import nppes_providers as pipeline
from spicy_regs.source_catalog.input_proof import input_proof
from spicy_regs.source_evidence import EvidenceMemberSource
from spicy_regs.sources import publication
from spicy_regs.transforms.build_nppes_providers import (
    FAMILY,
    OUTPUT,
    SCHEMA,
    TABLE,
    V2_FILEHEADER_PINS,
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
    verify_zip_pin,
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


def _luhn_validate_npi(value: str) -> str:
    """45 CFR 162.406 Luhn NPI validator for standalone unit tests."""
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{10}", value):
        raise ValueError("NPI must be 10 ASCII digits")
    digits = [int(ch) for ch in ("80840" + value[:9])]
    total = 0
    for pos, digit in enumerate(reversed(digits)):
        if pos % 2 == 0:
            digit *= 2
            if digit > 9:
                digit -= 9
        total += digit
    check = str((10 - (total % 10)) % 10)
    if value[9] != check:
        raise ValueError("NPI check digit fails Luhn")
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
    cohort=NPIS,
    validate_npi=_validate_synthetic_npi,
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
        cohort=cohort,
        validate_npi=validate_npi,
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
    assert rows[0]["product"] == "main-records-only"
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


def test_monthly_boundary_day_overlap_requires_explicit_opt_in_and_never_applies_weekly_to_weekly():
    monthly = apply_main_release(
        None,
        release_v2([[NPIS[0], "1", "", "OLD", "A", "", "", ""]], kind="monthly", start=1, end=2),
    )
    overlap_w1 = release_v2(
        [[NPIS[0], "1", "", "UPDATED", "A", "", "", ""]],
        kind="weekly",
        start=2,
        end=8,
        predecessor=monthly.release.digest,
    )
    # Default refuses 1-day monthly->weekly overlap.
    with pytest.raises(ValueError, match="gap or overlap"):
        apply_main_release(monthly, overlap_w1)

    # Explicit opt-in permits monthly->weekly boundary-day overlap (e.g. Nov 2 monthly -> Nov 2-8 weekly).
    after_w1 = apply_main_release(monthly, overlap_w1, allow_monthly_boundary_day_overlap=True)
    assert list(snapshot_rows(after_w1))[0]["fields"]["Provider Last Name (Legal Name)"] == "UPDATED"

    # Weekly->weekly overlap STILL refuses even when allow_monthly_boundary_day_overlap=True!
    overlap_w2 = release_v2(
        [[NPIS[0], "1", "", "BAD", "A", "", "", ""]],
        kind="weekly",
        start=8,
        end=14,
        predecessor=after_w1.release.digest,
    )
    with pytest.raises(ValueError, match="gap or overlap"):
        apply_main_release(after_w1, overlap_w2, allow_monthly_boundary_day_overlap=True)

    # Same contract holds for apply_v2_release with sidecars.
    v2_m = apply_v2_release(
        None,
        release_v2([[NPIS[0], "2", "ORG", "", "", "", "", ""]], kind="monthly", start=1, end=2),
        sidecars=(parsed_v2([[NPIS[0], "DBA", "3", "11/01/2026"]], kind="othername", columns=OTHERNAME_HEADER),),
    )
    v2_w1 = release_v2(
        [[NPIS[0], "2", "ORG2", "", "", "", "", ""]],
        kind="weekly",
        start=2,
        end=8,
        predecessor=v2_m.digest,
    )
    sc_w1 = (parsed_v2([[NPIS[0], "DBA2", "3", "11/03/2026"]], kind="othername", columns=OTHERNAME_HEADER),)
    with pytest.raises(ValueError, match="gap or overlap"):
        apply_v2_release(v2_m, v2_w1, sidecars=sc_w1)
    v2_after_w1 = apply_v2_release(v2_m, v2_w1, sidecars=sc_w1, allow_monthly_boundary_day_overlap=True)
    assert list(snapshot_rows(v2_after_w1))[0]["fields"]["Provider Organization Name (Legal Business Name)"] == "ORG2"
    v2_w2_overlap = release_v2(
        [[NPIS[0], "2", "ORG3", "", "", "", "", ""]],
        kind="weekly",
        start=8,
        end=14,
        predecessor=v2_after_w1.digest,
    )
    with pytest.raises(ValueError, match="gap or overlap"):
        apply_v2_release(v2_after_w1, v2_w2_overlap, sidecars=sc_w1, allow_monthly_boundary_day_overlap=True)


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


def test_v2_fileheader_pins_are_well_formed():
    assert set(V2_FILEHEADER_PINS) == {"main", "othername", "practice_location", "endpoint"}
    assert V2_FILEHEADER_PINS["main"]["column_count"] == 330
    assert V2_FILEHEADER_PINS["othername"]["column_count"] == 4
    assert V2_FILEHEADER_PINS["practice_location"]["column_count"] == 10
    assert V2_FILEHEADER_PINS["endpoint"]["column_count"] == 19
    for pin in V2_FILEHEADER_PINS.values():
        assert isinstance(pin["byte_size"], int) and pin["byte_size"] > 0
        assert re.fullmatch(r"sha256:[0-9a-f]{64}", pin["sha256"])


def test_v2_combined_deactivation_reactivation_and_refusals():
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
    assert classify_v2_main_state({**reactivated, "NPI Reactivation Date": "11/10/2026"}) == "reactivated"
    # Cross-year reactivation (12/20/2025 -> 01/05/2026) is accepted.
    assert (
        classify_v2_main_state(
            {**reactivated, "NPI Deactivation Date": "12/20/2025", "NPI Reactivation Date": "01/05/2026"}
        )
        == "reactivated"
    )

    with pytest.raises(ValueError, match="precedes"):
        classify_v2_main_state({**reactivated, "NPI Reactivation Date": "11/09/2026"})

    with pytest.raises(ValueError, match="discloses fields beyond"):
        classify_v2_main_state(
            {
                "NPI": NPIS[0],
                "Entity Type Code": "",
                "Provider Organization Name (Legal Business Name)": "",
                "Provider Last Name (Legal Name)": "",
                "Provider First Name": "",
                "NPI Deactivation Reason Code": "DT",
                "NPI Deactivation Date": "11/10/2026",
                "NPI Reactivation Date": "",
            }
        )


@pytest.mark.parametrize(
    ("overrides", "match"),
    [
        ({"NPI Deactivation Date": "2026-11-10", "NPI Reactivation Date": ""}, "literal MM/DD/YYYY"),
        ({"NPI Deactivation Date": "02/30/2026", "NPI Reactivation Date": ""}, "literal MM/DD/YYYY"),
        ({"NPI Deactivation Date": "13/01/2026", "NPI Reactivation Date": ""}, "literal MM/DD/YYYY"),
        ({"NPI Reactivation Date": "2026-11-17"}, "literal MM/DD/YYYY"),
        ({"Entity Type Code": "", "NPI Deactivation Date": "", "NPI Reactivation Date": ""}, "Entity Type Code"),
        ({"Entity Type Code": "3", "NPI Deactivation Date": "", "NPI Reactivation Date": ""}, "Entity Type Code"),
        (
            {
                "Entity Type Code": "1",
                "Provider Organization Name (Legal Business Name)": "",
                "NPI Deactivation Date": "11/10/2026",
                "NPI Reactivation Date": "",
            },
            "discloses fields beyond",
        ),
    ],
)
def test_classify_v2_main_state_date_and_entity_edge_refusals(overrides, match):
    base = {
        "NPI": NPIS[0],
        "Entity Type Code": "2",
        "Provider Organization Name (Legal Business Name)": "ORG",
        "Provider Last Name (Legal Name)": "",
        "Provider First Name": "",
        "NPI Deactivation Reason Code": "",
        "NPI Deactivation Date": "",
        "NPI Reactivation Date": "",
    }
    with pytest.raises(ValueError, match=match):
        classify_v2_main_state({**base, **overrides})


@pytest.mark.parametrize(
    "bad_npi",
    [
        "1851806690",  # 10 ASCII digits, invalid Luhn check digit
        "185180669",  # wrong length (9 digits)
        "18518066991",  # wrong length (11 digits)
        " 1851806699",  # leading whitespace
        "1851806699 ",  # trailing whitespace
        "１８５１８０６６９９",  # fullwidth Unicode digits
        "010001",  # 6-char CCN
    ],
)
def test_read_v2_csv_cohort_refuses_invalid_npis_with_nppes_source_error(bad_npi: str):
    payload = _csv_bytes(HEADER, [[bad_npi, "1", "", "DOE", "JANE", "", "", ""]])
    with pytest.raises(NppesSourceError):
        read_v2_csv_cohort(
            io.BytesIO(payload),
            kind="main",
            scope="member",
            source_url="https://download.cms.gov/nppes/weekly.zip",
            member_name="npidata_pfile_20261109-20261115.csv",
            expected_sha256="sha256:" + hashlib.sha256(payload).hexdigest(),
            expected_bytes=len(payload),
            columns=HEADER,
            cohort=NPIS,
            validate_npi=_luhn_validate_npi,
            max_scanned_rows=10,
            max_selected_rows=10,
        )


def test_read_v2_csv_cohort_stream_and_bound_refusals_raise_nppes_source_error():
    rows = [
        [NPIS[0], "1", "", "POTTER", "NICHOLAS", "", "", ""],
        [NPIS[1], "2", "CLINIC LLC", "", "", "", "", ""],
    ]
    payload = _csv_bytes(HEADER, rows)
    sha = "sha256:" + hashlib.sha256(payload).hexdigest()

    def _call(
        stream: Any,
        *,
        expected_bytes: int = len(payload),
        max_scanned_rows: int = 10,
        max_selected_rows: int = 10,
        max_record_bytes: int = 1024 * 1024,
    ):
        return read_v2_csv_cohort(
            stream,
            kind="main",
            scope="member",
            source_url="https://download.cms.gov/nppes/weekly.zip",
            member_name="npidata_pfile_20261109-20261115.csv",
            expected_sha256=sha,
            expected_bytes=expected_bytes,
            columns=HEADER,
            cohort=NPIS,
            validate_npi=_luhn_validate_npi,
            max_scanned_rows=max_scanned_rows,
            max_selected_rows=max_selected_rows,
            max_record_bytes=max_record_bytes,
        )

    # Non-binary stream raises NppesSourceError (not plain ValueError).
    with pytest.raises(NppesSourceError, match="binary stream"):
        _call(io.StringIO(payload.decode("utf-8")))

    # Oversized stream exceeding expected_bytes raises NppesSourceError.
    with pytest.raises(NppesSourceError, match="exceeds its pinned byte size"):
        _call(io.BytesIO(payload), expected_bytes=len(payload) - 5)

    # max_scanned_rows overflow raises NppesSourceError.
    with pytest.raises(NppesSourceError, match="max_scanned_rows"):
        _call(io.BytesIO(payload), max_scanned_rows=1)

    # max_selected_rows overflow raises NppesSourceError.
    with pytest.raises(NppesSourceError, match="max_selected_rows"):
        _call(io.BytesIO(payload), max_selected_rows=1)

    # max_record_bytes overflow raises NppesSourceError (mapped from delimited_rows ValueError).
    with pytest.raises(NppesSourceError):
        _call(io.BytesIO(payload), max_record_bytes=16)


def test_v2_sidecar_replacement_removal_carry_forward_reset_and_refusals():
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
            [NPIS[0], "DBA DELTA", "3", "04/01/2020"],
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
    assert list(snapshot_rows(snap1))[0]["product"] == "v2-main-and-sidecars"

    # Composite predecessor digest refusal: main-only digest refuses when predecessor tracks sidecars!
    m2_main_only_pred = release_v2(
        [[NPIS[0], "2", "ORG ONE RENAMED", "", "", "", "", ""]],
        kind="weekly",
        start=16,
        end=22,
        predecessor=m1.digest,
    )
    on2 = parsed_v2(
        [
            [NPIS[0], "DBA ALPHA", "3", "06/01/2017"],  # partial reduction: 3 -> 1 for NPIS[0]
            [NPIS[2], "RESTORED DBA", "3", "11/17/2026"],
        ],
        kind="othername",
        columns=OTHERNAME_HEADER,
        member_name="othername_pfile_20261116-20261122.csv",
    )
    pl2 = parsed_v2(
        [],
        kind="practice_location",
        columns=PL_HEADER,
        member_name="pl_pfile_20261116-20261122.csv",
    )
    with pytest.raises(ValueError, match="authenticate previous.digest"):
        apply_v2_release(snap1, m2_main_only_pred, sidecars=(on2, pl2))

    # Weekly update authenticating composite snap1.digest succeeds.
    m2 = release_v2(
        [
            [NPIS[0], "2", "ORG ONE RENAMED", "", "", "", "", ""],
            [NPIS[2], "2", "RESTORED CLINIC", "", "", "", "11/10/2026", "11/17/2026"],
        ],
        kind="weekly",
        start=16,
        end=22,
        predecessor=snap1.digest,
    )

    # Omitting a previously tracked sidecar kind (pl) refuses atomically!
    with pytest.raises(ValueError, match="omits previously tracked sidecar kind"):
        apply_v2_release(snap1, m2, sidecars=(on2,))

    # Changed sidecar column layout refuses atomically!
    bad_cols_pl = parsed_v2(
        [],
        kind="practice_location",
        columns=(*PL_HEADER, "Extra Col"),
        member_name="pl_pfile_20261116-20261122.csv",
    )
    with pytest.raises(ValueError, match="sidecar layout changed"):
        apply_v2_release(snap1, m2, sidecars=(on2, bad_cols_pl))

    # Duplicate sidecar kind, excerpt sidecar, and non-NppesCsv sidecar all refuse atomically!
    with pytest.raises(ValueError, match="Duplicate NPPES V2 sidecar kind"):
        apply_v2_release(snap1, m2, sidecars=(on2, on2, pl2))
    excerpt_pl = parsed_v2(
        [],
        kind="practice_location",
        scope="excerpt",
        columns=PL_HEADER,
        member_name="pl_pfile_20261116-20261122.csv",
    )
    with pytest.raises(ValueError, match="complete member"):
        apply_v2_release(snap1, m2, sidecars=(on2, excerpt_pl))
    with pytest.raises(ValueError, match="pinned NppesCsv"):
        apply_v2_release(snap1, m2, sidecars=cast(Any, (on2, object())))

    snap2 = apply_v2_release(snap1, m2, sidecars=(on2, pl2))

    # Snap1 remained immutable and tagged deactivated NPIS[2]'s sidecar explicitly.
    snap1_sc = {(r["kind"], r["npi"]): r["main_lifecycle_state"] for r in v2_snapshot_sidecar_rows(snap1)}
    assert snap1_sc[("othername", NPIS[2])] == "deactivated"

    # Snap2 reflects partial sidecar reduction (3 -> 1), full removal (NPIS[0] pl), untouched carry-forward (NPIS[1] pl),
    # and combined-date reactivation (NPIS[2]).
    main2 = {r["npi"]: r for r in snapshot_rows(snap2)}
    assert main2[NPIS[2]]["lifecycle_state"] == "reactivated"
    assert main2[NPIS[2]]["fields"]["NPI Deactivation Date"] == "11/10/2026"
    assert main2[NPIS[2]]["fields"]["NPI Reactivation Date"] == "11/17/2026"

    sc2 = list(v2_snapshot_sidecar_rows(snap2))
    on_names = [
        (r["npi"], r["fields"]["Provider Other Organization Name"], r["main_lifecycle_state"])
        for r in sc2
        if r["kind"] == "othername"
    ]
    assert on_names == [
        (NPIS[0], "DBA ALPHA", "active"),
        (NPIS[2], "RESTORED DBA", "reactivated"),
    ]
    assert [r["npi"] for r in sc2 if r["kind"] == "practice_location"] == [NPIS[1]]

    # Orphan weekly sidecar referencing an NPI absent from week's main CSV refuses atomically!
    before = tuple(v2_snapshot_sidecar_rows(snap2))
    m3 = release_v2(
        [[NPIS[0], "2", "ORG ONE", "", "", "", "", ""]],
        kind="weekly",
        start=23,
        end=29,
        predecessor=snap2.digest,
    )
    orphan_ep = parsed_v2(
        [[NPIS[1], "DIRECT", "doc@example.org"]],
        kind="endpoint",
        columns=ENDPOINT_HEADER,
        member_name="endpoint_pfile_20261123-20261129.csv",
    )
    with pytest.raises(ValueError, match="absent from release's main CSV"):
        apply_v2_release(snap2, m3, sidecars=(on2, pl2, orphan_ep))
    assert tuple(v2_snapshot_sidecar_rows(snap2)) == before

    # Monthly orphan sidecar also fails closed!
    monthly_orphan_m = release_v2([[NPIS[0], "2", "ONLY ZERO", "", "", "", "", ""]], kind="monthly", start=24, end=30)
    monthly_orphan_on = parsed_v2(
        [[NPIS[1], "ORPHAN DBA", "3", "11/25/2026"]],
        kind="othername",
        columns=OTHERNAME_HEADER,
        member_name="othername_pfile_20261124-20261130.csv",
    )
    with pytest.raises(ValueError, match="absent from release's main CSV"):
        apply_v2_release(snap2, monthly_orphan_m, sidecars=(monthly_orphan_on,))

    # Monthly reset clears all prior sidecars cleanly.
    reset_m = release_v2([[NPIS[2], "2", "FRESH ORG", "", "", "", "", ""]], kind="monthly", start=24, end=30)
    reset_snap = apply_v2_release(snap2, reset_m)
    assert [r["npi"] for r in snapshot_rows(reset_snap)] == [NPIS[2]]
    assert list(snapshot_rows(reset_snap))[0]["product"] == "main-records-only"
    assert list(v2_snapshot_sidecar_rows(reset_snap)) == []


@pytest.mark.parametrize(
    "fault",
    [
        "malformed-header",
        "duplicate-npi",
        "bad-width",
        "changed-zip-hash",
        "missing-member",
        "corrupt-zip",
        "duplicate-zip-member",
        "directory-member",
        "unsafe-traversal",
        "unsafe-leading-slash",
        "unsafe-backslash",
        "unsafe-nul",
        "symlink-zip",
    ],
)
def test_atomic_refusal_on_malformed_csv_and_hardened_zip_checks(tmp_path: Path, fault: str):
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
    if fault == "unsafe-traversal":
        member_name = "../npidata.csv"
    elif fault == "unsafe-leading-slash":
        member_name = "/npidata.csv"
    elif fault == "unsafe-backslash":
        member_name = r"sub\npidata.csv"
    elif fault == "unsafe-nul":
        member_name = "npidata\x00.csv"

    zip_path = tmp_path / "weekly.zip"
    if fault == "corrupt-zip":
        zip_path.write_bytes(b"not-a-valid-zip-archive")
    elif fault == "duplicate-zip-member":
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(member_name, csv_bytes)
            zf.writestr(member_name, csv_bytes)
    elif fault == "directory-member":
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            info = zipfile.ZipInfo(member_name + "/")
            zf.writestr(info, b"")
        member_name = member_name + "/"
    else:
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("other.csv" if fault == "missing-member" else "npidata_pfile_20261109-20261115.csv", csv_bytes)

    target_zip = zip_path
    if fault == "symlink-zip":
        target_zip = tmp_path / "link.zip"
        target_zip.symlink_to(zip_path)

    raw_zip = zip_path.read_bytes()
    zip_sha = "sha256:" + ("0" * 64 if fault == "changed-zip-hash" else hashlib.sha256(raw_zip).hexdigest())
    member_sha = "sha256:" + hashlib.sha256(csv_bytes).hexdigest()

    prior = tmp_path / OUTPUT
    prior.write_bytes(b"prior-parquet-bytes")

    if fault == "symlink-zip":
        with pytest.raises(NppesSourceError, match="symlink"):
            verify_zip_pin(target_zip, expected_sha256=zip_sha, expected_bytes=len(raw_zip))

    with pytest.raises(NppesSourceError):
        data = read_v2_zip_member(
            target_zip,
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


def test_pipeline_error_paths_and_provenance_guards(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "scratch-raw"))
    url = "https://download.cms.gov/nppes/NPPES_Data_Dissemination_111626_112226_Weekly_V2.zip"
    main_csv = parsed_v2(
        [[NPIS[0], "1", "", "POTTER", "NICHOLAS", "", "", ""]],
        source_url=url,
        member_name="npidata_pfile_20261116-20261122.csv",
    )
    raw_zip = tmp_path / "weekly_v2.zip"
    with zipfile.ZipFile(raw_zip, "w") as zf:
        zf.writestr("marker.txt", b"fixture-archive")

    # Empty csv_inputs refuses.
    with pytest.raises(ValueError, match="at least one pinned NppesCsv"):
        pipeline.build((), tmp_path / "empty-inputs", raw_files=[(raw_zip, url, "weekly-zip")])

    # Empty raw_files refuses.
    with pytest.raises(ValueError, match="non-empty raw_files"):
        pipeline.build((main_csv,), tmp_path / "empty-raw", raw_files=())

    # Unretained source_url refuses.
    with pytest.raises(ValueError, match="not retained in raw_files"):
        pipeline.build(
            (main_csv,),
            tmp_path / "mismatch-url",
            raw_files=[(raw_zip, "https://download.cms.gov/nppes/other.zip", "weekly-zip")],
        )

    # Pre-existing generation directory refuses.
    existing = tmp_path / "existing-out"
    (existing / "generation").mkdir(parents=True)
    with pytest.raises(ValueError, match="fresh NPPES provider generation directory"):
        pipeline.build((main_csv,), existing, raw_files=[(raw_zip, url, "weekly-zip")])


def test_serving_registration_pipeline_and_mcp_duckdb_readback(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "scratch-raw"))
    assert FAMILY in local_publication.LOCAL_FAMILIES
    assert local_publication._family_schemas(FAMILY) == {TABLE: SCHEMA}
    assert TABLE in mcp_server.TABLES

    archive_url = "https://download.cms.gov/nppes/NPPES_Data_Dissemination_111626_112226_Weekly_V2.zip"
    main_csv = parsed_v2(
        [
            [NPIS[0], "1", "", "POTTER", "NICHOLAS", "", "", ""],
            [NPIS[1], "2", "CHIROS ON 54 LLC", "", "", "", "11/10/2026", "11/17/2026"],
            [NPIS[2], "", "", "", "", "", "11/16/2026", ""],
        ],
        source_url=archive_url,
        member_name="npidata_pfile_20261116-20261122.csv",
    )
    sidecar_csv = parsed_v2(
        [[NPIS[1], "CHIROS DBA", "3", "11/17/2026"], [NPIS[2], "OLD DBA", "3", "02/24/2020"]],
        kind="othername",
        columns=OTHERNAME_HEADER,
        source_url=archive_url,
        member_name="othername_pfile_20261116-20261122.csv",
    )
    raw_zip = tmp_path / "weekly_v2.zip"
    with zipfile.ZipFile(raw_zip, "w") as zf:
        zf.writestr("marker.txt", b"fixture-archive")

    artifact, evidence = pipeline.build(
        (main_csv, sidecar_csv),
        tmp_path / "out",
        raw_files=[(raw_zip, archive_url, "weekly-zip")],
        release_metadata={"period": "2026-11-16..2026-11-22"},
    )
    assert artifact.pin.artifact_digest and evidence.exists()
    journal_text = (evidence / "journal.jsonl").read_text()
    # No local machine paths leak into published source evidence.
    assert str(tmp_path) not in journal_text
    # Used-input declaration resolves cleanly through input_proof.
    gen_dir = tmp_path / "out" / "generation"
    ev_source = EvidenceMemberSource(evidence)
    ev_artifact = admit_artifact(ev_source)
    proof, _ = input_proof(artifact, LocalMemberSource(gen_dir), ((ev_source, ev_artifact),))
    assert [src["location"] for src in proof["generations"][0]["sources"]] == [archive_url]

    root = local_publication._candidate(gen_dir, FAMILY, (evidence,))
    assert root["spec"]["family"] == FAMILY

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

    dup_target = tmp_path / "dup.parquet"
    dup_target.write_bytes(b"intact")
    with pytest.raises(ValueError, match="Duplicate NPPES observation identity"):
        build_observations_multi((main_csv, main_csv), dup_target)
    assert dup_target.read_bytes() == b"intact"
