"""Native filing references resolve without expanding money-bearing joins."""

from copy import deepcopy

import duckdb
import pyarrow as pa
import pytest

from spicy_regs.relationship_views.fec_typed import filing_reference_resolution
from spicy_regs.transforms.fec_identity_observations import FILINGS, LINKS, SCHEMAS, filing_key


def filing(rid, number):
    return dict(
        record_id=rid,
        filing_key=filing_key(number),
        report_number=str(number),
        source_authority="official-fec",
        source_namespace="fec-openfec-file-number",
        native_filer_id="C00000001",
        form_type="F3",
    )


def inputs():
    filings = [filing("original", 10), filing("amendment-observation-1", 20), filing("amendment-observation-2", 20)]
    links = [
        dict(
            record_id="reference",
            source_filing_record_id="original",
            filing_key=filing_key(10),
            target_filing_key=filing_key(20),
            relation_type="amendment_chain_member",
            target_resolution_status="native_reference_unresolved",
            replacement_mode="unknown",
            membership_completeness="unqualified",
        )
    ]
    return filings, links


def query(filings, links):
    with duckdb.connect() as db:
        db.execute("SET memory_limit='64MB'")
        db.register(FILINGS, pa.Table.from_pylist(filings, schema=SCHEMAS[FILINGS]))
        db.register(LINKS, pa.Table.from_pylist(links, schema=SCHEMAS[LINKS]))
        return db.sql(filing_reference_resolution()).to_arrow_table().to_pylist()


def test_multiple_observations_of_same_filing_do_not_multiply_reference():
    filings, links = inputs()
    (row,) = query(filings, links)
    assert row["record_id"] == "reference" and row["target_observation_count"] == 2
    assert row["target_resolution_status"] == "resolved_native_filing_key"
    assert row["relation_type"] == "amendment_chain_member"
    assert row["replacement_mode"] == "unknown" and row["membership_completeness"] == "unqualified"


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ("absent-source", "unresolved_source_observation_absent"),
        ("duplicate-source", "unresolved_source_observation_ambiguous"),
        ("source-key", "unresolved_source_identity_mismatch"),
        ("absent-target", "unresolved_target_not_retained"),
        ("null-target", "unresolved_no_file_number"),
        ("duplicate-target", "unresolved_target_observation_ambiguous"),
        ("filer-conflict", "unresolved_target_identity_conflict"),
        ("authority-conflict", "unresolved_target_identity_conflict"),
        ("number-conflict", "unresolved_target_identity_conflict"),
    ],
)
def test_missing_ambiguous_and_conflicting_endpoints_remain_one_unresolved_reference(change, reason):
    filings, links = inputs()
    if change == "absent-source":
        filings.pop(0)
    elif change == "duplicate-source":
        filings.append(deepcopy(filings[0]))
    elif change == "source-key":
        links[0]["filing_key"] = filing_key(999)
    elif change == "absent-target":
        links[0]["target_filing_key"] = filing_key(999)
    elif change == "null-target":
        links[0]["target_filing_key"] = None
    elif change == "duplicate-target":
        filings.append(deepcopy(filings[1]))
    else:
        field = {
            "filer-conflict": "native_filer_id",
            "authority-conflict": "source_authority",
            "number-conflict": "report_number",
        }[change]
        filings[1][field] = "conflicting-value"
    (row,) = query(filings, links)
    assert row["target_resolution_status"] == reason
    assert row["record_id"] == "reference" and row["replacement_mode"] == "unknown"


def test_self_reference_and_unknown_source_filing_identity_are_not_invented_amendments():
    filings, links = inputs()
    links[0]["target_filing_key"] = filings[0]["filing_key"]
    (row,) = query(filings, links)
    assert row["target_resolution_status"] == "resolved_native_filing_key"
    assert row["replacement_mode"] == "unknown"
    filings[0]["filing_key"] = links[0]["filing_key"] = None
    (row,) = query(filings, links)
    assert row["filing_key"] is None and row["target_resolution_status"] == "unresolved_target_not_retained"
