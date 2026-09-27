"""The Regulations.gov attribute tables: one mirror read, two rows, typed as the contract states (decisions 65-67)."""

from datetime import UTC, datetime

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.schemas import COLUMN_TYPES, TABLE_CONTRACTS

from spicy_regs.contract_types import DESCRIBED, arrow_schema, arrow_type
from spicy_regs.pipelines import attributes_sweep
from spicy_regs.pipelines.regulations import RegulationsPipeline
from spicy_regs.pipelines.rollups.regulatory_base import DocketAttributesFamily, DocumentAttributesFamily
from spicy_regs.transforms.regulations_attributes import TeeAttributes, merge_attribute_parts


def _document(document_id: str, **attributes) -> dict:
    return {"data": {"id": document_id, "type": "documents", "attributes": attributes}}


def _parts(tmp_path, table, payloads, *, batch_size=2):
    tee = TeeAttributes(table, tmp_path, batch_size=batch_size)
    assert list(tee.apply(payloads)) == payloads, "every payload passes on unchanged"
    return tee


@pytest.mark.parametrize("name", COLUMN_TYPES)
def test_each_contract_type_is_written_as_duckdb_describes_it(tmp_path, name):
    """The spelling publication descriptors carry, read back from a written file rather than assumed."""
    path = tmp_path / "one.parquet"
    pq.write_table(pa.table({"c": pa.array([None], arrow_type(name))}), path)
    assert duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()[0][1] == DESCRIBED[name]


def test_the_tee_writes_each_records_typed_attribute_row_in_batches(tmp_path):
    payloads = [_document(f"EPA-1-{n:04d}", pageCount=n, openForComment=n % 2 == 0,
                          receiveDate="2021-01-01T05:00:00Z", topics=["Air"], firstName="Ada") for n in range(5)]
    tee = _parts(tmp_path, "document_attributes", payloads)
    (part,) = (tmp_path / "document_attributes").glob("*.parquet")
    table = pq.read_table(part)
    assert tee.rows_written == 5 and table.num_rows == 5
    assert table.schema.remove(table.schema.get_field_index("_attributes_sha256")).remove(
        table.schema.get_field_index("_modify_date")) == arrow_schema(TABLE_CONTRACTS["document_attributes"])
    row = table.to_pylist()[3]
    assert (row["document_id"], row["page_count"], row["open_for_comment"], row["topics"], row["first_name"]) == (
        "EPA-1-0003", 3, False, ["Air"], "Ada")
    assert row["receive_date"] == datetime(2021, 1, 1, 5, tzinfo=UTC)


def test_merging_parts_over_a_prior_keeps_the_fresh_row_and_the_types(tmp_path):
    _parts(tmp_path / "first", "document_attributes", [_document("A", pageCount=1), _document("B", pageCount=2)])
    prior = tmp_path / "document_attributes.parquet"
    assert merge_attribute_parts("document_attributes", tmp_path / "first" / "document_attributes", None, prior) == 2
    _parts(tmp_path / "second", "document_attributes", [_document("B", pageCount=20), _document("C", pageCount=3)])
    assert merge_attribute_parts("document_attributes", tmp_path / "second" / "document_attributes", prior, prior) == 3
    table = pq.read_table(prior)
    assert {r["document_id"]: r["page_count"] for r in table.to_pylist()} == {"A": 1, "B": 20, "C": 3}
    assert table.schema.field("page_count").type == pa.int32()
    assert table.schema.field("receive_date").type == pa.timestamp("us", tz="UTC")


def test_the_daily_etl_drops_attribute_rows_until_the_sweep_seeds_the_table(tmp_path):
    pipeline = RegulationsPipeline(output_dir=tmp_path)
    staging = tmp_path / "staging"
    _parts(staging, "docket_attributes", [{"data": {"id": "EPA-1", "attributes": {"keywords": ["a"]}}}])
    assert pipeline._merge_attributes(staging, tmp_path, "docket_attributes") is False
    assert not (tmp_path / "docket_attributes.parquet").exists()

    _parts(tmp_path / "seed", "docket_attributes", [{"data": {"id": "EPA-0", "attributes": {}}}])
    merge_attribute_parts("docket_attributes", tmp_path / "seed" / "docket_attributes", None,
                          tmp_path / "docket_attributes.parquet")
    assert pipeline._merge_attributes(staging, tmp_path, "docket_attributes") is True
    assert pq.read_table(tmp_path / "docket_attributes.parquet").column("docket_id").to_pylist() == ["EPA-0", "EPA-1"]


class _Reader:
    def __init__(self, payloads, failed=()):
        self.payloads, self.last_keys, self.failed_keys = payloads, [], list(failed)

    def iter_records(self):
        yield from self.payloads


def test_the_sweep_writes_both_tables_whole(tmp_path):
    served = {
        ("EPA", "dockets"): [{"data": {"id": "EPA-1", "attributes": {"keywords": ["air"]}}}],
        ("EPA", "documents"): [_document("EPA-1-0001", pageCount=4)],
        ("FDA", "dockets"): [{"data": {"id": "FDA-1", "attributes": {}}}],
        ("FDA", "documents"): [_document("FDA-1-0001"), _document("FDA-1-0002")],
    }
    rows = attributes_sweep.sweep(tmp_path, agencies=["EPA", "FDA"],
                                  read_factory=lambda consumed: lambda agency, kind: _Reader(served[(agency, kind.name)]))
    assert rows == {"document_attributes": 3, "docket_attributes": 2}
    assert pq.read_table(tmp_path / "docket_attributes.parquet").column("keywords").to_pylist() == [["air"], None]
    assert not (tmp_path / "attributes-staging").exists()


def test_a_key_that_keeps_failing_in_transport_writes_nothing(tmp_path):
    with pytest.raises(RuntimeError, match="unread after 3 passes"):
        attributes_sweep.sweep(tmp_path, agencies=["EPA"],
                               read_factory=lambda consumed: lambda agency, kind: _Reader(
                                   [], failed=["raw-data/EPA/x.json"]))
    assert not (tmp_path / "document_attributes.parquet").exists()


def test_a_transport_failure_is_re_read_skipping_what_was_read(tmp_path):
    """A connection drop costs a re-read of the failed agency's unread keys, not the sweep."""
    passes = []

    def factory(consumed):
        passes.append(consumed)
        first = len(passes) == 1

        def read(agency, kind):
            if kind.name != "documents":
                return _Reader([])
            reader = _Reader([_document("EPA-1-0001")] if first else [_document("EPA-1-0002")],
                             failed=["raw-data/EPA/EPA-1/documents/EPA-1-0002.json"] if first else [])
            reader.last_keys = ["raw-data/EPA/EPA-1/documents/EPA-1-0001.json"] if first else [
                "raw-data/EPA/EPA-1/documents/EPA-1-0002.json"]
            return reader

        return read

    rows = attributes_sweep.sweep(tmp_path, agencies=["EPA"], read_factory=factory)
    assert rows["document_attributes"] == 2
    assert passes == [frozenset(), frozenset({"raw-data/EPA/EPA-1/documents/EPA-1-0001.json"})]


def test_the_families_publish_on_the_contracts_key():
    assert DocumentAttributesFamily().key() == "document_id"
    assert DocketAttributesFamily().key() == "docket_id"


def test_a_duplicated_id_keeps_the_newest_modify_date_as_the_thin_table_does(tmp_path):
    """The mirror can hold one id in two files; the pick must match ``merge_staging_files`` and repeat every run."""
    older = _document("BIS-1-0001", pageCount=1, modifyDate="2024-01-01T00:00:00Z")
    newer = _document("BIS-1-0001", pageCount=2, modifyDate="2025-01-01T00:00:00Z")
    for order, name in (([older, newer], "a"), ([newer, older], "b")):
        _parts(tmp_path / name, "document_attributes", order, batch_size=1)
        out = tmp_path / f"{name}.parquet"
        merge_attribute_parts("document_attributes", tmp_path / name / "document_attributes", None, out)
        table = pq.read_table(out)
        assert table.column("page_count").to_pylist() == [2]
        assert "_modify_date" not in table.column_names and "_attributes_sha256" not in table.column_names


def test_a_refused_record_loses_only_its_attributes_row(tmp_path):
    """One malformed record must not stop the base ETL: the payload still reaches the thin extract."""
    bad = _document("", pageCount=1)
    good = _document("EPA-1-0001", pageCount=2)
    tee = _parts(tmp_path, "document_attributes", [bad, good])
    assert tee.rows_written == 1 and [key for key, _ in tee.refused] == [""]


def test_the_sweep_reports_refusals_beside_the_tables(tmp_path):
    served = {("EPA", "dockets"): [{"data": {"id": "", "attributes": {}}}], ("EPA", "documents"): []}
    rows = attributes_sweep.sweep(tmp_path, agencies=["EPA"],
                                  read_factory=lambda consumed: lambda agency, kind: _Reader(served[(agency, kind.name)]))
    import json

    assert rows["docket_attributes"] == 0
    receipt = json.loads((tmp_path / "attribute_refusals.json").read_text())
    assert [(r["table"], r["id"]) for r in receipt["refused"]] == [("docket_attributes", "")]
