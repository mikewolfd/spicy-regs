"""Financial decisions and source witnesses survive the physical receipt split."""

from pathlib import Path
from decimal import Decimal
import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import ReceiptContext, validate_receipt_bundle
from spicy_regs.transforms.fec_subject_receipts import (
    write_fec_subjects,
    read_fec_with_receipts,
    read_fec_processing,
    register_internal_fec_table,
)
from spicy_regs.transforms.fec_financial_policy import bulk_observation_eligibility
from spicy_regs.relationship_views.fec_financial_meaning import financial_rule_sql


def context(row, ordinal):
    witness = dict(source_id="source", source_uri=None, sha256="sha256:" + "a" * 64, locator=None, body_version=None)
    return ReceiptContext("generation-a", str(ordinal), "test-fec", [witness, witness])


def source_row():
    return dict(
        record_id="sha256:" + "b" * 64,
        identity_version="fec-typed-observation/1",
        mapping_version="fec-bulk-individual-receipt/1",
        value_mapping_version="fec-exact-financial-values/2",
        mapping_status="mapped",
        source_authority="official-fec",
        source_namespace="fec-bulk-individual-contributions",
        source_representation_role="snapshot",
        correction_operation="none",
        current_record_status="unqualified",
        amount=Decimal("123456789.123456789"),
        amount_status="exact",
        currency="USD",
        memo_indicator="",
        transaction_type="15",
    )


def write(tmp_path, rows=None):
    rows = rows or [source_row()]
    schema = pa.Table.from_pylist(rows).schema
    return write_fec_subjects(
        rows,
        tmp_path / "bundle",
        table="fec_receipts",
        input_schema=schema,
        generation_id="generation-a",
        context_for=context,
    ), schema


def test_financial_python_and_sql_match_prior_eligibility(tmp_path):
    ((subject, receipt), policy), schema = write(tmp_path)
    rows = list(read_fec_with_receipts([subject], [receipt], policy, generation_id="generation-a"))
    assert rows == [source_row()]
    assert bulk_observation_eligibility(rows[0]) == bulk_observation_eligibility(source_row())
    assert "amount_status" not in pq.ParquetFile(subject).schema_arrow.names
    witnesses = pq.ParquetFile(receipt).read(columns=["witnesses"]).to_pylist()[0]["witnesses"]
    assert len(witnesses) == 2 and witnesses[0] == witnesses[1]
    with duckdb.connect() as con:
        register_internal_fec_table(
            con,
            table="fec_receipts",
            subject_paths=[subject],
            shared_receipt_paths=[receipt],
            policy=policy,
            input_schema=schema,
            generation_id="generation-a",
        )
        sql = financial_rule_sql("fec_receipts", "bulk_source_analysis", columns=schema.names)
        actual = con.execute(sql).fetchall()
        con.execute("DROP TABLE fec_receipts")
        con.register("fec_receipts", pa.Table.from_pylist([source_row()], schema=schema))
        assert con.execute(sql).fetchall() == actual


@pytest.mark.parametrize("fault", ["generation", "missing", "modified", "duplicate"])
def test_receipt_read_refuses_faults_before_yielding(tmp_path, fault):
    ((subject, receipt), policy), schema = write(tmp_path)
    receipts = [receipt]
    generation = "generation-a"
    if fault == "generation":
        generation = "generation-b"
    if fault == "missing":
        empty = tmp_path / "empty.parquet"
        pq.write_table(pq.ParquetFile(receipt).read().slice(0, 0), empty)
        receipts = [empty]
    if fault == "duplicate":
        receipts = [receipt, receipt]
    if fault == "modified":
        row = pq.ParquetFile(subject).read().to_pylist()[0]
        row["amount"] = Decimal("99")
        pq.write_table(pa.Table.from_pylist([row], schema=policy.subject_schema), subject)
    with pytest.raises(ValueError):
        list(read_fec_with_receipts([subject], receipts, policy, generation_id=generation))


def test_technical_table_has_only_observed_receipts(tmp_path):
    rows = [
        dict(
            record_id="definition",
            family="electronic",
            version="8.5",
            form="F3",
            workbook_sha256="sha256:" + "a" * 64,
            layout_json='{"fields":[]}',
        )
    ]
    schema = pa.Table.from_pylist(rows).schema
    (subject, receipt), policy = write_fec_subjects(
        rows,
        tmp_path / "bundle",
        table="fec_filing_definitions",
        input_schema=schema,
        generation_id="generation-a",
        context_for=context,
    )
    assert subject is None
    assert list(read_fec_processing([receipt], policy, generation_id="generation-a")) == rows
    validate_receipt_bundle({"fec_filing_definitions": []}, [receipt], [policy], generation_id="generation-a")


def test_invalid_native_values_preserve_failed_inputs_without_subject(tmp_path):
    rows = [dict(record_id="legal", native_facts_json='{"citations":{"unknown":[]}}')]
    schema = pa.Table.from_pylist(rows).schema
    (subject, receipt), policy = write_fec_subjects(
        rows,
        tmp_path / "bundle",
        table="fec_legal_matters",
        input_schema=schema,
        generation_id="generation-a",
        context_for=context,
    )
    assert pq.ParquetFile(subject).metadata.num_rows == 0
    r = pq.ParquetFile(receipt).read().to_pylist()[0]
    assert r["outcome"] == "refused"
    assert r["subject_version"] is None
    assert "unknown" in r["processing_json"]


def test_subject_assembly_checks_input_partition_and_receipt_readback(tmp_path):
    from spicy_regs.transforms.assemble_fec_query import TypedInput, assemble_subject_table, _sha

    row = source_row()
    schema = pa.Table.from_pylist([row]).schema
    source = tmp_path / "source.parquet"
    pq.write_table(pa.Table.from_pylist([row], schema=schema), source)
    item = TypedInput(source, _sha(source), 1, row["source_namespace"])
    result = assemble_subject_table(
        [item],
        table="fec_receipts",
        schema=schema,
        output=tmp_path / "assembled",
        generation_id="generation-a",
        context_for=context,
        check_resources=lambda: None,
        source_partitioned=True,
    )
    assert result["exact_mapper_rows_compared"] == 1
    assert result["subject_partition_columns"] == []
    assert result["source_partition_columns"] == ["source_namespace"]
    assert "source_namespace" not in pq.ParquetFile(result["subject_path"]).schema_arrow.names
    wrong = TypedInput(source, _sha(source), 1, "wrong-namespace")
    with pytest.raises(ValueError, match="source namespace"):
        assemble_subject_table(
            [wrong],
            table="fec_receipts",
            schema=schema,
            output=tmp_path / "wrong",
            generation_id="generation-a",
            context_for=context,
            check_resources=lambda: None,
            source_partitioned=True,
        )


def test_clean_subject_dictionary_cannot_trigger_legacy_evidence_fallback(tmp_path):
    from spicy_regs.relationship_views.fec_query_views import fec_query_views

    root = tmp_path / "package"
    root.mkdir()
    installed = Path(__file__).parents[1] / "src/spicy_regs/table_metadata.json"
    metadata = json.loads(installed.read_text())
    metadata["fec_receipts"]["columns"] = [
        c
        for c in metadata["fec_receipts"]["columns"]
        if c["column_name"] not in {"source_record_id", "source_sha256", "collection_id", "source_locator_json"}
    ]
    (root / "table_metadata.json").write_text(json.dumps(metadata))
    with pytest.raises(ValueError, match="receipt-validated processing"):
        fec_query_views(
            source_generation_pin="sha256:" + "a" * 64,
            population="test",
            as_of="test",
            namespace_evidence={},
            package_root=root,
        )
