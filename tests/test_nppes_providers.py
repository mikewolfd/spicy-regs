"""Main-only lifecycle and atomic candidate controls; fixtures are synthetic."""

import csv
import hashlib
import io
from datetime import date

import pyarrow.parquet as pq
import pytest

from spicy_docs.sources.nppes import read_csv_cohort
from spicy_regs.transforms.build_nppes_providers import (
    MainRelease,
    apply_main_release,
    build_observations,
    snapshot_rows,
)

NPIS = ("1851806699", "1699600866", "1669740403")
HEADER = (
    "NPI",
    "Entity Type Code",
    "Provider Organization Name (Legal Business Name)",
    "Provider Last Name (Legal Name)",
    "Provider First Name",
    "NPI Deactivation Date",
    "NPI Reactivation Date",
)


def parsed(rows, *, scope="member", columns=HEADER, kind="main"):
    text = io.StringIO(newline="")
    csv.writer(text).writerows([columns, *rows])
    payload = text.getvalue().encode()
    def validate_npi(value):
        if value not in NPIS:
            raise ValueError("NPI is outside the synthetic test set")
        return value

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
        validate_npi=validate_npi,
        max_scanned_rows=10,
        max_selected_rows=10,
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
