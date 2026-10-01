"""Equivalent original bytes and selected source membership must both agree."""

from copy import deepcopy
import hashlib
import json

import duckdb
import pyarrow as pa
import pytest

from spicy_regs.relationship_views.fec_typed import individual_snapshot_inclusion
from spicy_regs.transforms.fec_bulk_selection import SELECTION_SCHEMA, individual_snapshot_selection

PIN = "sha256:" + "a" * 64
GEN = "sha256:" + "b" * 64


def inputs():
    proof = dict(
        status="passed-exact-complete-member-sequence-equivalence",
        source_generation_pin=GEN,
        archive_sha256=PIN,
        main_collection_id="main",
        main_rows=3,
        main_bytes=30,
        date_members=2,
        segments=[
            dict(
                member_ordinal=2,
                member_name="by_date/late.txt",
                main_offset=0,
                bytes=20,
                linefeeds=2,
                ends_with_linefeed=True,
                sha256=PIN,
            ),
            dict(
                member_ordinal=1,
                member_name="by_date/early.txt",
                main_offset=20,
                bytes=10,
                linefeeds=1,
                ends_with_linefeed=True,
                sha256=GEN,
            ),
        ],
    )
    collections = [
        dict(
            collection_id=cid,
            profile="positional",
            scope=dict(
                capture=dict(responseSha256=PIN),
                member=dict(ordinal=ordinal, name=name),
                format="delimited",
                delimiter="|",
                quoting="literal",
                encoding="utf-8",
            ),
            field_mapping=dict(
                header_collection_id="bulk-2026-indiv-header", header_row_ordinal=0, data_has_header=False
            ),
        )
        for ordinal, cid, name in [
            (0, "main", "itcont.txt"),
            (1, "early", "by_date/early.txt"),
            (2, "late", "by_date/late.txt"),
        ]
    ]
    return proof, collections, dict(main=3, early=1, late=2)


def build(proof, collections, counts):
    body = json.dumps(proof).encode()
    return individual_snapshot_selection(
        body,
        proof_sha256="sha256:" + hashlib.sha256(body).hexdigest(),
        source_generation_pin=GEN,
        collections=collections,
        source_counts=counts,
    )


def test_complete_membership_selects_main_once_and_preserves_duplicate_representations():
    rows = build(*inputs())
    assert {r["collection_id"]: r["selection_status"] for r in rows} == dict(
        main="included", early="duplicate", late="duplicate"
    )
    assert sum(r["source_observations"] for r in rows if r["selection_status"] == "included") == 3
    assert all(r["current_record_status"] == "unqualified" for r in rows)
    assert pa.Table.from_pylist(rows, schema=SELECTION_SCHEMA).to_pylist() == rows


@pytest.mark.parametrize("change", ["missing", "duplicate", "offset", "count", "decoding", "generation", "name"])
def test_partial_conflicting_or_differently_decoded_representations_refuse(change):
    p, c, n = inputs()
    if change == "missing":
        c.pop()
    elif change == "duplicate":
        c.append(deepcopy(c[1]))
    elif change == "offset":
        p["segments"][1]["main_offset"] += 1
    elif change == "count":
        n["early"] += 1
    elif change == "decoding":
        c[1]["scope"]["quoting"] = "csv"
    elif change == "generation":
        p["source_generation_pin"] = PIN
    else:
        c[1]["scope"]["member"]["name"] = "other.txt"
    with pytest.raises(ValueError):
        build(p, c, n)


def test_receipt_pin_is_verified_before_using_any_claim():
    p, c, n = inputs()
    with pytest.raises(ValueError, match="digest differs"):
        individual_snapshot_selection(
            json.dumps(p).encode(), proof_sha256=PIN, source_generation_pin=GEN, collections=c, source_counts=n
        )


def test_inclusion_view_preserves_multiplicity_and_refuses_unqualified_corrections_and_policy_drift():
    policies = build(*inputs())
    native = [
        dict(
            record_id=str(i), collection_id=cid, source_sha256=PIN, source_namespace="fec-bulk-individual-contributions"
        )
        for i, cid in enumerate(["main", "main", "early", "late", "delete", "insert"])
    ]
    with duckdb.connect() as db:
        db.register("fec_receipts", pa.Table.from_pylist(native))
        db.register("fec_collection_selection", pa.Table.from_pylist(policies, schema=SELECTION_SCHEMA))
        rows = db.sql(individual_snapshot_inclusion(GEN)).to_arrow_table().to_pylist()
        assert len(rows) == len(native)
        assert {r["target_record_id"]: r["selection_status"] for r in rows} == {
            "0": "included",
            "1": "included",
            "2": "duplicate",
            "3": "duplicate",
            "4": "unresolved",
            "5": "unresolved",
        }
        db.register("fec_collection_selection", pa.Table.from_pylist(policies + [policies[0]], schema=SELECTION_SCHEMA))
        duplicate = db.sql(individual_snapshot_inclusion(GEN)).to_arrow_table().to_pylist()
        assert len(duplicate) == len(native)
        assert all(r["selection_status"] == "unresolved" for r in duplicate if r["collection_id"] == "main")
        drifted = db.sql(individual_snapshot_inclusion(PIN)).to_arrow_table().to_pylist()
        assert len(drifted) == len(native) and all(r["selection_status"] == "unresolved" for r in drifted)


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("policy_version", "unsupported/99", "unsupported-policy-version"),
        ("policy_version", None, "unsupported-policy-version"),
        ("selection_status", "current", "unsupported-policy-status"),
        ("selection_status", None, "unsupported-policy-status"),
    ],
)
def test_inclusion_view_refuses_unknown_semantics_and_exposes_the_observed_version(field, value, reason):
    policies = build(*inputs())
    policies[0][field] = value
    native = dict(
        record_id="observation",
        collection_id="main",
        source_sha256=PIN,
        source_namespace="fec-bulk-individual-contributions",
    )
    with duckdb.connect() as db:
        db.register("fec_receipts", pa.Table.from_pylist([native]))
        db.register("fec_collection_selection", pa.Table.from_pylist(policies, schema=SELECTION_SCHEMA))
        (row,) = db.sql(individual_snapshot_inclusion(GEN)).to_arrow_table().to_pylist()
    assert row["selection_status"] == "unresolved"
    assert row["selection_reason"] == reason
    assert row["policy_version"] == policies[0]["policy_version"]
    assert row["selected_collection_id"] is None
    assert row["current_record_status"] == "unqualified"
