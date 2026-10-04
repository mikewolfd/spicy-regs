"""Bounded Python and SQL filing associations agree without multiplying rows."""

from copy import deepcopy
import hashlib
import json
import subprocess
import sys

import duckdb
import pyarrow as pa
import pytest

from spicy_regs.relationship_views.fec_filing_associations import (
    FILE_NUMBER_MAPPINGS,
    financial_number_association_sql,
)
from spicy_regs.transforms.fec_filing_associations import associate_filing_numbers


def pin(value):
    return "sha256:" + hashlib.sha256(value.encode()).hexdigest()


GEN, PROOF = pin("source-generation"), pin("definition")
NS = "fec-bulk-individual-contributions"
KEY = "urn:fec:filing:official-fec:openfec-file-number:123"


def source(**changes):
    return dict(
        record_id=pin("source"),
        collection_id="selection",
        source_record_id="original-row",
        source_sha256=pin("original"),
        source_authority="official-fec",
        source_locator_json=json.dumps(
            dict(collection_id="selection", source_record_id="original-row"), sort_keys=True, separators=(",", ":")
        ),
        selection_evidence_sha256=PROOF,
        source_namespace=NS,
        mapping_version=FILE_NUMBER_MAPPINGS[NS],
        report_number="123",
        reporting_committee_id="C00000001",
        **changes,
    )


def metadata():
    return dict(
        record_id=pin("metadata"),
        identity_version="fec-typed-observation/1",
        mapping_version="fec-identity-observations/2",
        filing_key=KEY,
        report_number="123",
        source_authority="official-fec",
        source_namespace="fec-openfec-file-number",
        native_filer_id="C00000001",
        form_type="F3",
    )


def check(rows=None, filings=None, proofs=None, *, table="financial"):
    rows = rows if rows is not None else [source()]
    filings = filings if filings is not None else [metadata()]
    proofs = proofs if proofs is not None else {n: PROOF for n in FILE_NUMBER_MAPPINGS}
    names = list(dict.fromkeys(k for row in rows for k in row))
    data = pa.Table.from_pylist(rows, schema=pa.schema([(name, pa.string()) for name in names]))
    f = pa.Table.from_pylist(filings, schema=pa.schema([(k, pa.string()) for k in metadata()]))
    con = duckdb.connect(config={"threads": 1, "memory_limit": "128MB"})
    try:
        con.register(table, data)
        con.register("fec_filings", f)
        sql = financial_number_association_sql(table, GEN, proofs, columns=names)
        actual = con.execute(sql).to_arrow_table().to_pylist()
    finally:
        con.close()
    expected = associate_filing_numbers(
        rows, table=table, filings=filings, source_generation_pin=GEN, namespace_evidence=proofs
    )
    expected = [{k: v for k, v in r.items() if k != "record_id"} for r in expected]
    assert len(actual) == len(rows)
    assert sorted(actual, key=lambda r: r["target_record_id"]) == sorted(expected, key=lambda r: r["target_record_id"])
    return actual


def test_multiple_metadata_observations_resolve_once_per_source_row():
    fs = [metadata(), {**metadata(), "record_id": pin("second")}, {**metadata(), "record_id": pin("third")}]
    result = check(rows=[source(), source()], filings=fs)
    assert len(result) == 2 and all(r["filing_observation_count"] == 3 for r in result)
    assert all(r["association_status"] == "resolved_native_filing_key" for r in result)


@pytest.mark.parametrize("value", [None, "", "0", "-0", "00123", "+123", " 123", "123.0", "FEC-123", "１２３", "124"])
def test_native_absence_spelling_and_missing_targets_match(value):
    check(rows=[{**source(), "report_number": value}])


def test_negative_native_number_and_empty_metadata_are_explicit():
    f = {**metadata(), "report_number": "-123", "filing_key": KEY.replace(":123", ":-123")}
    check(rows=[{**source(), "report_number": "-123"}], filings=[f])
    check(filings=[])


@pytest.mark.parametrize(
    "change",
    [
        "duplicate",
        "authority",
        "namespace",
        "number",
        "filer",
        "form",
        "null-authority",
        "invalid-number",
        "mapper",
        "identity",
        "null-mapper",
    ],
)
def test_every_conflicting_target_stays_in_group_and_refuses(change):
    fs = [metadata(), {**metadata(), "record_id": pin("second")}]
    if change == "duplicate":
        fs[1] = deepcopy(fs[0])
    else:
        key, value = {
            "authority": ("source_authority", "unofficial"),
            "namespace": ("source_namespace", "other"),
            "number": ("report_number", "124"),
            "filer": ("native_filer_id", "C00000002"),
            "form": ("form_type", "F3X"),
            "null-authority": ("source_authority", None),
            "invalid-number": ("report_number", "00123"),
            "mapper": ("mapping_version", "unknown/999"),
            "identity": ("identity_version", "unknown/999"),
            "null-mapper": ("mapping_version", None),
        }[change]
        fs[1][key] = value
    result = check(filings=fs)[0]
    assert result["association_status"].startswith("unresolved_target_")
    assert result["filing_observation_count"] == 2


@pytest.mark.parametrize(
    "change",
    [
        {"source_authority": "unofficial"},
        {"source_namespace": "unknown"},
        {"mapping_version": "unknown/99"},
        {"reporting_committee_id": "C00000002"},
    ],
)
def test_source_authority_mapper_and_filer_are_exact(change):
    check(rows=[{**source(), **change}])
    check(proofs={})


@pytest.mark.parametrize(
    ("family", "field"),
    [("independent-expenditure-csv", "spender_native_id"), ("communication-cost-csv", "reporting_entity_id")],
)
def test_union_null_committee_column_does_not_mask_correct_native_filer(family, field):
    namespace = "fec-bulk-" + family
    row = {
        **source(),
        "source_namespace": namespace,
        "mapping_version": FILE_NUMBER_MAPPINGS[namespace],
        "reporting_committee_id": None,
        field: "C00000002",
    }
    result = check(rows=[row])[0]
    assert result["association_status"] == "unresolved_filer_identity_conflict"


def test_unknown_namespace_with_absent_optional_columns_stays_unresolved():
    row = source()
    del row["report_number"]
    del row["reporting_committee_id"]
    row["source_namespace"] = "fec-bulk-electioneering-candidate-disbursement-csv"
    check(rows=[row])


def test_blank_metadata_attributes_do_not_conflict_with_reported_identity():
    check(filings=[metadata(), {**metadata(), "record_id": pin("second"), "native_filer_id": "", "form_type": None}])


def test_serving_module_has_no_transform_arrow_or_source_reader_imports():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from spicy_regs.relationship_views import fec_filing_associations; import sys; assert not any(n == 'pyarrow' or n.startswith(('spicy_regs.transforms', 'spicy_docs')) for n in sys.modules)",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_pins_required_columns_and_quoted_table_names():
    check(table='odd"table')
    with pytest.raises(ValueError):
        financial_number_association_sql("x", "latest", {}, columns=source())
    with pytest.raises(ValueError):
        financial_number_association_sql("x", GEN, {NS: "bad"}, columns=source())
    with pytest.raises(ValueError):
        financial_number_association_sql("x", GEN, {}, columns=[])


@pytest.mark.parametrize("version", ["fec-identity-observations/1", "fec-identity-observations/3", "unknown"])
def test_sql_and_python_refuse_non_v2_target_metadata(version):
    result = check(filings=[{**metadata(), "mapping_version": version}])
    assert result[0]["association_status"] == "unresolved_target_identity_conflict"
    assert result[0]["filing_key"] is None
