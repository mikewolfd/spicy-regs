"""Selected query interpretation and exact legacy reconstruction stay distinct."""

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.fec_receipt_adapter import ReceiptAdapter, qualified_views
from spicy_regs.transforms.fec_identity_context_fields import normalize_record, subject_schema
from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter
from spicy_regs.transforms.fec_subject_receipts import write_fec_subjects
from spicy_regs.transforms.fec_filing_associations import ASSOCIATION_SCHEMA, associate_filing_headers
from tests.test_fec_filing_associations import financial, filing, header_inputs, headers, pin, GEN, EVIDENCE, NAMESPACE
from tests.test_fec_identity_receipts import WITNESS
from tests.test_fec_receipt_adapter import _native
from tests.test_fec_subject_receipts import context


STAGED = pin("prepared-observations")


def view(name):
    specs = qualified_views(dict(source_generation_pin=STAGED, population="fixture", as_of="fixture",
                                 namespace_evidence={NAMESPACE: EVIDENCE}))
    return next(spec.view for spec in specs if spec.view.name == name)


def filing_pair(tmp_path, *, current_version="fec-identity-observations/2"):
    current_row = {**filing(), "mapping_version": current_version, "pages": 171}
    native = normalize_record("fec_filings", current_row)
    adopted = {name: native[name] for name in subject_schema("fec_filings").names}
    original = {**filing(), "mapping_version": "fec-identity-observations/1", "pages": "0171",
                "mapping_reason_json": '{"legacy":"retained"}'}
    emitted = {**original, "mapping_reason_json": '{"current":"qualified"}'}
    directory = tmp_path / "filings"
    with IdentityReceiptWriter(directory, generation_id="query", tables=["fec_filings"]) as writer:
        writer.emit("fec_filings", emitted, input_witness=WITNESS,
                    source_input={"fec_retained_overrides": {"mapping_version": original["mapping_version"],
                                                             "pages": original["pages"],
                                                             "mapping_reason_json": original["mapping_reason_json"]}},
                    native_filing_subject=adopted)
    return original, {"subjects": [directory / "fec_filings.parquet"],
                      "receipts": directory / "etl_receipts.parquet", "generation_id": "query"}


def test_current_nonempty_number_view_reads_v2_while_original_restores_v1(tmp_path):
    original, selected = filing_pair(tmp_path)
    financial_input = _native(tmp_path, "fec_receipts", [financial()])
    with duckdb.connect() as con:
        con.register("fec_filings", pq.read_table(selected["subjects"][0]))
        adapter = ReceiptAdapter(con, {"families": {}}, "unused",
                                 local_native={"fec_filings": selected, "fec_receipts": financial_input})
        source_schema = pa.Table.from_pylist([original]).schema
        exact = adapter.restore("fec_filings", source_schema=source_schema)
        assert con.execute(f'SELECT * FROM "{exact}"').to_arrow_table().to_pylist() == [original]
        assert con.execute(f'SELECT mapping_version,pages FROM "{exact}"').fetchall() == [
            ("fec-identity-observations/1", "0171")]
        queried = view("fec_receipts_native_filing_associations")
        prepared = adapter.prepare(queried, queried.query({}))
        assert con.execute(prepared.query({})).fetchone() is not None
        result = con.execute(prepared.query({})).to_arrow_table().to_pylist()
        assert len(result) == 1
        assert result[0]["association_status"] == "resolved_native_filing_key"
        assert result[0]["source_generation_pin"] == STAGED
        current = adapter.restore("fec_filings", current=True)
        assert con.execute(f'SELECT mapping_version,pages FROM "{current}"').fetchall() == [
            ("fec-identity-observations/2", 171)]
        assert con.execute(f'SELECT mapping_reason_json FROM "{current}"').fetchone() == (
            '{"current":"qualified"}',)
        assert con.execute(f'SELECT mapping_reason_json FROM "{exact}"').fetchone() == (
            '{"legacy":"retained"}',)
        # Neither private relation replaces the ordinary selected public main.
        assert con.execute("SELECT mapping_version,pages FROM fec_filings").fetchall() == [
            ("fec-identity-observations/2", 171)]
        assert con.execute(f'SELECT mapping_version,pages FROM "{exact}"').fetchall() == [
            ("fec-identity-observations/1", "0171")]


def test_current_nonempty_header_view_reads_rebuilt_policy_and_staged_pin(tmp_path):
    hs, scopes = header_inputs()
    original = {**headers()[0], "policy_version": "fec-retained-filing-association/1",
                "record_id": pin("old-header")}
    [current] = associate_filing_headers(hs, scopes=scopes, authorities={"c": "official-fec"},
                                        filings=[filing()], source_generation_pin=STAGED,
                                        selection_evidence_sha256=pin("staged-header-selection"))
    (subject, receipts), _ = write_fec_subjects(
        [original], tmp_path / "headers", table="fec_filing_header_associations",
        input_schema=ASSOCIATION_SCHEMA, generation_id="generation-a", context_for=context,
        current_header_for=lambda row, ordinal: current)
    money = financial(filing_header_record_id=hs[0]["source_record_id"],
                      filing_header_locator_json=hs[0]["source_locator_json"])
    selected = {"fec_receipts": _native(tmp_path, "fec_receipts", [money]),
                "fec_filing_header_associations": {"subjects": [subject], "receipts": receipts,
                                                    "generation_id": "generation-a"}}
    with duckdb.connect() as con:
        adapter = ReceiptAdapter(con, {"families": {}}, "unused", local_native=selected)
        exact = adapter.restore("fec_filing_header_associations")
        assert con.execute(f'SELECT * FROM "{exact}"').to_arrow_table().to_pylist() == [original]
        assert con.execute(f'SELECT policy_version,source_generation_pin,record_id FROM "{exact}"').fetchall() == [
            (original["policy_version"], GEN, original["record_id"])]
        queried = view("fec_receipts_filing_associations")
        prepared = adapter.prepare(queried, queried.query({}))
        [decision] = con.execute(prepared.query({})).to_arrow_table().to_pylist()
        assert decision["association_status"] == "resolved_native_filing_key"
        assert decision["association_record_id"] == current["record_id"]
        assert decision["header_association_policy_version"] == "fec-retained-filing-association/2"
        relation = adapter.restore("fec_filing_header_associations", current=True)
        assert con.execute(f'SELECT source_generation_pin FROM "{relation}"').fetchone() == (STAGED,)
        assert con.execute(f'SELECT source_generation_pin FROM "{exact}"').fetchone() == (GEN,)


def test_current_interpretation_never_relabels_unknown_filing_version(tmp_path):
    _, selected = filing_pair(tmp_path, current_version="unsupported/9")
    financial_input = _native(tmp_path, "fec_receipts", [financial()])
    with duckdb.connect() as con:
        adapter = ReceiptAdapter(con, {"families": {}}, "unused",
                                 local_native={"fec_filings": selected, "fec_receipts": financial_input})
        queried = view("fec_receipts_native_filing_associations")
        prepared = adapter.prepare(queried, queried.query({}))
        [decision] = con.execute(prepared.query({})).to_arrow_table().to_pylist()
        assert decision["filing_key"] is None
        assert decision["association_status"] == "unresolved_target_identity_conflict"


def test_current_query_refuses_arbitrary_source_schema(tmp_path):
    _, selected = filing_pair(tmp_path)
    with duckdb.connect() as con:
        adapter = ReceiptAdapter(con, {"families": {}}, "unused", local_native={"fec_filings": selected})
        with pytest.raises(ValueError, match="maintained processing schema"):
            adapter.restore("fec_filings", source_schema=pa.schema([]), current=True)


@pytest.mark.parametrize("source_input", [[], {"fec_retained_overrides": []}])
def test_invalid_original_override_representation_never_creates_current_relation(tmp_path, source_input):
    directory = tmp_path / "bad"
    with IdentityReceiptWriter(directory, generation_id="query", tables=["fec_filings"]) as writer:
        writer.emit("fec_filings", filing(), input_witness=WITNESS, source_input=source_input)
    selected = {"subjects": [directory / "fec_filings.parquet"],
                "receipts": directory / "etl_receipts.parquet", "generation_id": "query"}
    with duckdb.connect() as con:
        adapter = ReceiptAdapter(con, {"families": {}}, "unused", local_native={"fec_filings": selected})
        with pytest.raises(ValueError, match="must be a mapping|must be a named mapping"):
            adapter.restore("fec_filings", current=True)
        assert con.execute("SHOW TABLES").fetchall() == []
