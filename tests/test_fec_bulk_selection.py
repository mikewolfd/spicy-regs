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


def test_cross_source_deduplication_covers_api_cm24_weball_and_foia_without_dropping_occurrences():
    import io
    import zipfile
    from spicy_regs.relationship_views.fec_typed import cross_source_collection_inclusion, deduplicated_subject_sql
    from spicy_regs.transforms.fec_bulk_selection import (
        API_DEDUP_PURPOSE,
        ARCHIVE_INNER_DEDUP_PURPOSE,
        FOIA_EDITION_DEDUP_PURPOSE,
        WEBALL_WEBL_DEDUP_PURPOSE,
        build_archive_inner_member_proof,
        build_foia_dual_edition_proof,
        build_weball_webl_subset_proof,
        cross_source_collection_selection,
    )

    api_sha = "sha256:" + "1" * 64
    weball_sha = "sha256:" + "2" * 64
    webl_sha = "sha256:" + "3" * 64
    foia_fec_sha = "sha256:" + "4" * 64
    foia_gov_sha = "sha256:" + "5" * 64

    def make_zip(content: bytes, comment: bytes) -> tuple[bytes, str]:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.comment = comment
            zf.writestr("cm.txt", content)
        raw = buf.getvalue()
        return raw, "sha256:" + hashlib.sha256(raw).hexdigest()

    cm_payload = b"C00000001|FIRST COMMITTEE\nC00000002|SECOND COMMITTEE\n"
    zip1, cm1_sha = make_zip(cm_payload, b"bulk-cm24")
    zip2, cm2_sha = make_zip(cm_payload, b"retained-cm24")
    assert cm1_sha != cm2_sha

    collections = [
        dict(
            collection_id="filings-f13-selected",
            profile="filing",
            record_count=31,
            scope=dict(captures=[dict(responseSha256=api_sha, requestUrl="https://api.open.fec.gov/v1/filings/")]),
        ),
        dict(
            collection_id="retained-api-003bf112d9dfbfa667ff",
            profile="document",
            record_count=34,
            scope=dict(capture=dict(responseSha256=api_sha, requestUrl="https://api.open.fec.gov/v1/filings/")),
        ),
        dict(
            collection_id="bulk-cm24-zip",
            profile="positional",
            record_count=2,
            scope=dict(
                capture=dict(responseSha256=cm1_sha),
                member=dict(ordinal=0, name="cm.txt"),
                format="delimited",
                delimiter="|",
                quoting="literal",
                encoding="utf-8",
            ),
        ),
        dict(
            collection_id="retained-committee-master-2024-0131eb48",
            profile="positional",
            record_count=2,
            scope=dict(
                capture=dict(responseSha256=cm2_sha),
                member=dict(ordinal=0, name="cm.txt"),
                format="delimited",
                delimiter="|",
                quoting="literal",
                encoding="utf-8",
            ),
        ),
        dict(
            collection_id="bulk-weball24-zip",
            profile="positional",
            record_count=2,
            scope=dict(capture=dict(responseSha256=weball_sha), member=dict(ordinal=0, name="weball24.txt")),
        ),
        dict(
            collection_id="bulk-webl24-zip",
            profile="positional",
            record_count=1,
            scope=dict(capture=dict(responseSha256=webl_sha), member=dict(ordinal=0, name="webl24.txt")),
        ),
        dict(
            collection_id="agency-foia-92157276f61a251a",
            profile="agency",
            record_count=10,
            scope=dict(capture=dict(responseSha256=foia_fec_sha)),
        ),
        dict(
            collection_id="agency-foia-archive-971d4a41312c0718",
            profile="agency",
            record_count=10,
            scope=dict(capture=dict(responseSha256=foia_gov_sha), member=dict(ordinal=0, name="fec-2025.xml")),
        ),
    ]
    cm_proof = build_archive_inner_member_proof(
        {"bulk-cm24-zip": (cm1_sha, zip1), "retained-committee-master-2024-0131eb48": (cm2_sha, zip2)},
        member_name="cm.txt",
        selected_collection_id="bulk-cm24-zip",
        source_generation_pin=GEN,
    )
    shared_row = dict(
        source_cycle=2024,
        candidate_native_id="H4MI05180",
        candidate_name="CAND A",
        period_end=None,
        measures=[dict(native_field="TTL_RECEIPTS", value="100", raw_value="100", value_status="exact", unit="USD")],
        attributes_json="{}",
    )
    extra_row = {**shared_row, "candidate_native_id": "P80001571", "candidate_name": "PRES B"}
    web_proof = build_weball_webl_subset_proof(
        [
            dict(
                superset_collection_id="bulk-weball24-zip",
                superset_sha256=weball_sha,
                superset_rows=[shared_row, extra_row],
                subset_collection_id="bulk-webl24-zip",
                subset_sha256=webl_sha,
                subset_rows=[shared_row],
            )
        ],
        source_generation_pin=GEN,
    )
    metric = dict(
        native_field="ProcessingStatisticsReceivedQuantity",
        metric_definition_id="def-1",
        fiscal_year=2025,
        raw_value="306",
        value="306",
        value_status="exact",
        value_operator="=",
        bound_value=None,
        unit="count",
    )
    foia_proof = build_foia_dual_edition_proof(
        [
            dict(collection_id="agency-foia-92157276f61a251a", source_sha256=foia_fec_sha, metrics=[metric]),
            dict(collection_id="agency-foia-archive-971d4a41312c0718", source_sha256=foia_gov_sha, metrics=[metric]),
        ],
        selected_collection_id="agency-foia-92157276f61a251a",
        source_generation_pin=GEN,
    )

    selection_rows = cross_source_collection_selection(
        collections,
        source_generation_pin=GEN,
        archive_inner_proofs=[cm_proof],
        weball_webl_proofs=[web_proof],
        foia_edition_proofs=[foia_proof],
    )
    assert pa.Table.from_pylist(selection_rows, schema=SELECTION_SCHEMA).to_pylist() == selection_rows
    by_cid = {r["collection_id"]: (r["purpose"], r["selection_status"], r["selected_collection_id"]) for r in selection_rows}
    assert by_cid == {
        "filings-f13-selected": (API_DEDUP_PURPOSE, "included", "filings-f13-selected"),
        "retained-api-003bf112d9dfbfa667ff": (API_DEDUP_PURPOSE, "duplicate", "filings-f13-selected"),
        "bulk-cm24-zip": (ARCHIVE_INNER_DEDUP_PURPOSE, "included", "bulk-cm24-zip"),
        "retained-committee-master-2024-0131eb48": (ARCHIVE_INNER_DEDUP_PURPOSE, "duplicate", "bulk-cm24-zip"),
        "bulk-weball24-zip": (WEBALL_WEBL_DEDUP_PURPOSE, "included", "bulk-weball24-zip"),
        "bulk-webl24-zip": (WEBALL_WEBL_DEDUP_PURPOSE, "duplicate", "bulk-weball24-zip"),
        "agency-foia-92157276f61a251a": (FOIA_EDITION_DEDUP_PURPOSE, "included", "agency-foia-92157276f61a251a"),
        "agency-foia-archive-971d4a41312c0718": (FOIA_EDITION_DEDUP_PURPOSE, "duplicate", "agency-foia-92157276f61a251a"),
    }

    # Query consumer keeps every occurrence in base table and deduplicates cleanly in view
    filings = [
        dict(record_id="f-domain", collection_id="filings-f13-selected", source_sha256=api_sha, filing_key="k1"),
        dict(record_id="f-dup", collection_id="retained-api-003bf112d9dfbfa667ff", source_sha256=api_sha, filing_key="k1"),
        dict(record_id="f-unique", collection_id="other-filings", source_sha256=PIN, filing_key="k2"),
    ]
    with duckdb.connect() as db:
        db.register("fec_filings", pa.Table.from_pylist(filings))
        db.register("fec_collection_selection", pa.Table.from_pylist(selection_rows, schema=SELECTION_SCHEMA))
        inc = db.sql(cross_source_collection_inclusion("fec_filings", GEN)).to_arrow_table().to_pylist()
        assert {r["target_record_id"]: r["selection_status"] for r in inc} == {
            "f-domain": "included",
            "f-dup": "duplicate",
            "f-unique": "single_representation",
        }
        dedup = db.sql(deduplicated_subject_sql("fec_filings", GEN)).to_arrow_table().to_pylist()
        assert [r["record_id"] for r in dedup] == ["f-domain", "f-unique"]
