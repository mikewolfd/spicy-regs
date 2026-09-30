"""The Regulations.gov attribute tables: one mirror read, two rows, typed as the contract states (decisions 65-67)."""

import random
from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.releases.observations import volatile_tie_choice
from spicy_docs.schemas import COLUMN_TYPES, TABLE_CONTRACTS
from spicy_docs.source_native.regulations_gov import (
    VOLATILE_TIE_MARGIN_SECONDS as MARGIN,
    docket_source_record_digest,
    document_source_record_digest,
)
from spicy_docs.sources.mirrulations import KeyedPayload

from spicy_regs.contract_types import DESCRIBED, arrow_schema, arrow_type
from spicy_regs.pipelines import attributes_sweep
from spicy_regs.pipelines.regulations import RegulationsPipeline
from spicy_regs.pipelines.rollups.regulatory_base import DocketAttributesFamily, DocumentAttributesFamily
from spicy_regs.pipelines.staging import stage_agencies
from spicy_regs.schemas import RECORD_TYPES
from spicy_regs.transforms.regulations_attributes import (
    ORDER_COLUMNS,
    TeeAttributes,
    _with_order_columns,
    merge_attribute_parts,
    newest_copy_sql,
)

#: A bulk-era write instant (2025-04-14) and one modifyDate the tied copies share.
WRITTEN = datetime(2025, 4, 14, 5, 29, 57, tzinfo=UTC)
MODIFIED = "2023-05-10T01:00:44Z"


def _document(document_id: str, **attributes) -> dict:
    return {"data": {"id": document_id, "type": "documents", "attributes": attributes}}


def _keyed(payload: dict, seconds: int | None = None) -> KeyedPayload:
    """``payload`` as the keyed reader yields it, written ``seconds`` after WRITTEN (None: S3 omitted it)."""
    written = None if seconds is None else WRITTEN + timedelta(seconds=seconds)
    return KeyedPayload(f"raw-data/{payload['data']['id']}.json", written, payload)


def _parts(tmp_path, table, records, *, batch_size=2):
    keyed = [record if isinstance(record, KeyedPayload) else _keyed(record) for record in records]
    tee = TeeAttributes(table, tmp_path, batch_size=batch_size)
    assert list(tee.apply(keyed)) == [record.payload for record in keyed], "every bare payload passes on"
    return tee


def _published(path, table):
    """The table at ``path``, asserting it carries exactly the contract's columns, no ordering column."""
    written = pq.read_table(path)
    assert written.column_names == list(TABLE_CONTRACTS[table].columns)
    return written


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
    assert table.drop_columns(list(ORDER_COLUMNS)).schema == arrow_schema(TABLE_CONTRACTS["document_attributes"])
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
    table = _published(prior, "document_attributes")
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
    """Serves ``records`` (payloads, or KeyedPayloads for a stated write) on both reader streams."""

    def __init__(self, records, failed=()):
        self.records, self.last_keys, self.failed_keys = records, [], list(failed)

    def iter_records(self):
        yield from (record.payload if isinstance(record, KeyedPayload) else record for record in self.records)

    def iter_keyed_records(self):
        yield from (record if isinstance(record, KeyedPayload) else _keyed(record) for record in self.records)


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
    assert _published(tmp_path / "docket_attributes.parquet", "docket_attributes").column(
        "keywords").to_pylist() == [["air"], None]
    _published(tmp_path / "document_attributes.parquet", "document_attributes")
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


def _pick(tmp_path, records) -> list[dict]:
    """The published row(s) the merge keeps from ``records``, read in either order."""
    picks = []
    for name, order in (("forward", records), ("reverse", records[::-1])):
        _parts(tmp_path / name, "document_attributes", order, batch_size=1)
        out = tmp_path / f"{name}.parquet"
        merge_attribute_parts("document_attributes", tmp_path / name / "document_attributes", None, out)
        picks.append(_published(out, "document_attributes").to_pylist())
    assert picks[0] == picks[1], "the pick does not depend on read order"
    return picks[0]


def test_a_newer_modify_date_beats_any_write_time(tmp_path):
    """The mirror can hold one id in two files; the newer version wins however long before the other it was written."""
    older = _document("BIS-1-0001", pageCount=1, modifyDate="2024-01-01T00:00:00Z")
    newer = _document("BIS-1-0001", pageCount=2, modifyDate="2025-01-01T00:00:00Z")
    assert [row["page_count"] for row in _pick(tmp_path, [_keyed(older, 10 * MARGIN), _keyed(newer, 0)])] == [2]


@pytest.mark.parametrize("apart", [MARGIN, MARGIN + 1])
def test_a_tie_goes_to_the_newest_write_only_beyond_the_margin(tmp_path, apart):
    """Copies tied on modifyDate that differ only in openForComment: at the margin (3600 s) the smaller record
    digest wins; one second beyond it, the newest write wins. The smaller digest is the older write, so only the
    margin decides."""
    smaller, larger = sorted((_document("USA-2022-HQ-0007-0002", modifyDate=MODIFIED, openForComment=flag)
                              for flag in (True, False)), key=document_source_record_digest)
    (row,) = _pick(tmp_path, [_keyed(smaller, 0), _keyed(larger, apart)])
    expected = larger if apart > MARGIN else smaller
    assert row["open_for_comment"] is expected["data"]["attributes"]["openForComment"]


def test_the_rule_chooses_as_spicy_docs_volatile_tie_choice_does(tmp_path):
    """Random tied groups, including unstated writes, against SpicyDocs' own chooser (policy 1.3)."""
    rng = random.Random(20260927)
    offsets = [0, 1, 12, MARGIN - 1, MARGIN, MARGIN + 1, 2 * MARGIN, 2 * MARGIN + 1, 86_400]
    rows, expected = [], {}
    for group in range(3_000):
        base = rng.choice([0, 1_744_641_653])
        copies = [(ordinal, None if rng.random() < 0.08 else base + rng.choice(offsets), f"sha256:{rng.getrandbits(64):016x}")
                  for ordinal in range(rng.randint(1, 5))]
        expected[f"G-{group}"] = volatile_tie_choice(copies, margin_seconds=MARGIN)
        rows += [{"document_id": f"G-{group}", "page_count": ordinal, "_modify_date": MODIFIED, "_written_at": written,
                  "_record_digest": digest} for ordinal, written, digest in copies]
    path = tmp_path / "copies.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=_with_order_columns(
        arrow_schema(TABLE_CONTRACTS["document_attributes"]))), path)
    chosen = duckdb.sql(newest_copy_sql(f"read_parquet('{path}')", "document_attributes")).fetchall()
    columns = TABLE_CONTRACTS["document_attributes"].columns
    identity, marker = columns.index("document_id"), columns.index("page_count")
    assert {row[identity]: row[marker] for row in chosen} == expected


def test_the_staged_order_is_the_write_time_and_spicy_docs_record_digest(tmp_path):
    document = _document("EPA-1-0001", modifyDate=MODIFIED, openForComment=True)
    docket = {"data": {"id": "EPA-1", "type": "dockets", "attributes": {"modifyDate": MODIFIED}}}
    for table, payload, digest in (("document_attributes", document, document_source_record_digest),
                                   ("docket_attributes", docket, docket_source_record_digest)):
        _parts(tmp_path, table, [_keyed(payload, 5)])
        (part,) = (tmp_path / table).glob("*.parquet")
        row = pq.read_table(part).to_pylist()[0]
        assert (row["_modify_date"], row["_written_at"], row["_record_digest"]) == (
            MODIFIED, int(WRITTEN.timestamp()) + 5, digest(payload))


def test_every_base_type_reads_keyed_for_its_attribute_tee_and_the_chunked_comment_path_reads_bare(tmp_path):
    """The ETL's one factory serves every type; the attribute tee asks for the keyed stream, comments included."""
    calls = []

    class Reader:
        last_keys: list[str] = []
        failed_keys: list[str] = []

        def __init__(self, name):
            self.name = name

        def iter_records(self):
            calls.append((self.name, "bare"))
            return iter(())

        def iter_keyed_records(self):
            calls.append((self.name, "keyed"))
            return iter(())

    pipeline = RegulationsPipeline(output_dir=tmp_path)
    pipeline._staging_dir = tmp_path / "staging"
    stage_agencies(["EPA"], list(RECORD_TYPES.values()), tmp_path / "staging", lambda agency, kind: Reader(kind.name),
                   transform_for=pipeline._transform_for)
    assert sorted(calls) == [("comments", "keyed"), ("dockets", "keyed"), ("documents", "keyed")]
    assert not pipeline._transform_for(RECORD_TYPES["comments"], attributes=False).keyed


def test_the_etl_factory_gives_an_excluded_comment_no_row_in_either_table(tmp_path, monkeypatch):
    """Through RegulationsPipeline._transform_for, the factory the ETL stages with, not a hand-built chain."""
    import hashlib
    import json

    from spicy_regs.transforms import reviewed_comments

    kept = {"data": {"id": "EPA-1-0001", "attributes": {"trackingNbr": "t", "withdrawn": False}}}
    excluded = {"data": {"id": "EPA-1-0002", "attributes": {"trackingNbr": "x", "withdrawn": False}}}
    digest = hashlib.sha256(json.dumps(excluded, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    real_init = reviewed_comments.ExcludeReviewedComments.__init__

    def init(self, *, keyed=False):
        real_init(self, keyed=keyed)
        self.decisions = {"EPA-1-0002": {"canonical_sha256": digest, "reason": "reviewed"}}

    monkeypatch.setattr(reviewed_comments.ExcludeReviewedComments, "__init__", init)
    pipeline = RegulationsPipeline(output_dir=tmp_path)
    pipeline._staging_dir = tmp_path / "staging"
    transform = pipeline._transform_for(RECORD_TYPES["comments"])
    rows = list(transform.apply([KeyedPayload("k1", None, kept), KeyedPayload("k2", None, excluded)]))
    assert [row["comment_id"] for row in rows] == ["EPA-1-0001"]
    (part,) = (tmp_path / "staging" / "comment_attributes").glob("*.parquet")
    assert [r["comment_id"] for r in pq.read_table(part).to_pylist()] == ["EPA-1-0001"]


def test_a_reviewed_exclusion_gets_no_comment_attributes_row(tmp_path):
    """The exclusion runs ahead of the tee, so an excluded comment is in neither the thin table nor its attributes."""
    import hashlib
    import json

    from spicy_regs.transforms import Chain, ExtractRecords
    from spicy_regs.transforms.reviewed_comments import ExcludeReviewedComments

    kept = {"data": {"id": "EPA-1-0001", "attributes": {"trackingNbr": "t", "withdrawn": False}}}
    excluded = {"data": {"id": "EPA-1-0002", "attributes": {"trackingNbr": "x", "withdrawn": False}}}
    exclusion = ExcludeReviewedComments(keyed=True)
    digest = hashlib.sha256(json.dumps(excluded, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    exclusion.decisions = {"EPA-1-0002": {"canonical_sha256": digest, "reason": "reviewed"}}
    chain = Chain(exclusion, TeeAttributes("comment_attributes", tmp_path), ExtractRecords(RECORD_TYPES["comments"]))
    rows = list(chain.apply([KeyedPayload("k1", None, kept), KeyedPayload("k2", None, excluded)]))
    assert [row["comment_id"] for row in rows] == ["EPA-1-0001"]
    (part,) = (tmp_path / "comment_attributes").glob("*.parquet")
    assert [r["comment_id"] for r in pq.read_table(part).to_pylist()] == ["EPA-1-0001"]


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


def test_shards_deal_agencies_round_robin_in_name_order():
    agencies = ["FDA", "EPA", "BIS", "CMS", "DOT"]
    shards = [attributes_sweep.shard_agencies(agencies, n, 2) for n in range(2)]
    assert shards == [["BIS", "DOT", "FDA"], ["CMS", "EPA"]]
    with pytest.raises(ValueError, match="outside"):
        attributes_sweep.shard_agencies(agencies, 2, 2)


def _shard(tmp_path, name, served):
    out = tmp_path / name
    attributes_sweep.sweep(out, agencies=sorted({agency for agency, _ in served}), keep_order=True,
                           read_factory=lambda consumed: lambda agency, kind: _Reader(served.get((agency, kind.name), [])))
    return out


def test_combine_keeps_the_newest_version_of_an_id_two_shards_both_wrote(tmp_path):
    """The mirror files a few documents under two agencies; combine keeps one row as a single sweep would."""
    older = _document("DOT-1-0001", pageCount=1, modifyDate="2024-01-01T00:00:00Z")
    newer = _document("DOT-1-0001", pageCount=2, modifyDate="2025-01-01T00:00:00Z")
    first = _shard(tmp_path, "a", {("DOT", "documents"): [older, _document("DOT-1-0002")],
                                   ("DOT", "dockets"): [{"data": {"id": "", "attributes": {}}}]})
    second = _shard(tmp_path, "b", {("FAA", "documents"): [newer]})
    rows = attributes_sweep.combine([first, second], tmp_path / "out")
    assert rows == {"document_attributes": 2, "docket_attributes": 0}
    table = _published(tmp_path / "out" / "document_attributes.parquet", "document_attributes")
    assert {r["document_id"]: r["page_count"] for r in table.to_pylist()} == {"DOT-1-0001": 2, "DOT-1-0002": None}
    import json

    assert [r["id"] for r in json.loads((tmp_path / "out" / "attribute_refusals.json").read_text())["refused"]] == [""]


def test_combine_chooses_among_every_shards_copies_at_once(tmp_path):
    """The margin runs from the newest copy, which can be in another shard. Over all three copies b1 is newest, a1
    falls outside the margin and a2 beats b1 on digest. Choosing within shard a first would keep a1, which b1 then
    beats by being written more than the margin after it."""
    a1, a2, b1 = sorted((_document("DOT-1-0001", modifyDate=MODIFIED, pageCount=n) for n in range(3)),
                        key=document_source_record_digest)
    writes = {"a1": 0, "a2": MARGIN - 600, "b1": MARGIN + 1}
    assert volatile_tie_choice([(0, writes["a1"], document_source_record_digest(a1)),
                                (1, writes["a2"], document_source_record_digest(a2)),
                                (2, writes["b1"], document_source_record_digest(b1))], margin_seconds=MARGIN) == 1
    first = _shard(tmp_path, "a", {("DOT", "documents"): [_keyed(a1, writes["a1"]), _keyed(a2, writes["a2"])]})
    second = _shard(tmp_path, "b", {("FAA", "documents"): [_keyed(b1, writes["b1"])]})
    attributes_sweep.combine([first, second], tmp_path / "out")
    table = _published(tmp_path / "out" / "document_attributes.parquet", "document_attributes")
    assert table.column("page_count").to_pylist() == [a2["data"]["attributes"]["pageCount"]]


def test_combine_refuses_shards_written_without_the_ordering_columns(tmp_path):
    served = {("EPA", "documents"): [_document("EPA-1-0001")]}
    plain = tmp_path / "plain"
    attributes_sweep.sweep(plain, agencies=["EPA"],
                           read_factory=lambda consumed: lambda agency, kind: _Reader(served.get((agency, kind.name), [])))
    with pytest.raises(RuntimeError, match="lack"):
        attributes_sweep.combine([plain], tmp_path / "out")
