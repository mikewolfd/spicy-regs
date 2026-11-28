"""A table whose receipts are retired, through every reader that took "installed policy" to mean "has receipts".

The marker (``subject_catalog.RECEIPTS_RETIRED``) sits beside the table's descriptor in its installed policy file, so
retiring moves no policy version and no ledger entry. These tests mark (or unmark) a table by patching the one
function every reader asks (``subject_catalog.retired_receipt_datasets``).
Which tables the installed files mark is held in one place: tests/test_receipt_archive_pins.py requires the marked
tables to equal the pinned ones, each pin checked against its generation's own root, so a table marked by accident
(no pin) fails there and a family's switch edits no test here.
"""

import copy
import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import subject_catalog
from spicy_regs.policy_migrations import descriptor_digest, ledger, unreadable_shipped_policies

REGISTER = Path(__file__).parent / "fixtures/federal_register_switch/expected/night-1.parquet"


@pytest.fixture
def retire(monkeypatch):
    """Start from no table retired, then mark the ones a test names, as their installed files would: a family's
    switch changes the installed marks and none of these tests."""
    def mark(*names):
        monkeypatch.setattr(subject_catalog, "retired_receipt_datasets", lambda: frozenset(names))
    mark()
    return mark


def test_the_marker_is_a_datasets_and_never_part_of_its_policy():
    declared = {**subject_catalog.descriptors()["federal_register"], subject_catalog.RECEIPTS_RETIRED: True}
    assert subject_catalog.is_receipts_retired(declared)
    # Stripped before digesting: the policy, its version and its ledger entry are what they were.
    assert subject_catalog.descriptor_of(declared) == subject_catalog.descriptors()["federal_register"]
    assert descriptor_digest(subject_catalog.policies()["federal_register"]) == ledger()["federal_register"]["installed"]
    # Owned receipt-only state datasets (without shared_log) may retire their receipts too (ss-vse3 F1).
    receipt_only = {**subject_catalog.descriptors()["bill_family_archives"], subject_catalog.RECEIPTS_RETIRED: True}
    assert subject_catalog.is_receipts_retired(receipt_only)
    assert subject_catalog.descriptor_of(receipt_only) == subject_catalog.descriptors()["bill_family_archives"]
    # Shared logs and non-boolean markers are still refused.
    shared_log = {**subject_catalog.descriptors()["congress_acquisition"], subject_catalog.SHARED_LOG: True}
    for refused in ({**declared, subject_catalog.RECEIPTS_RETIRED: "yes"},
                    {**declared, subject_catalog.RECEIPTS_RETIRED: False},
                    {**declared, "receipt_only": "no"},
                    {**shared_log, subject_catalog.RECEIPTS_RETIRED: True},
                    {**declared, subject_catalog.SHARED_LOG: True}):
        with pytest.raises(ValueError):
            subject_catalog.descriptor_of(refused)


def test_admission_needs_no_receipts_for_a_retired_table_and_refuses_them(retire):
    from spicy_regs.etl_policy_registry import require_registered_receipts

    tables = {"federal_register.parquet": {}}
    with pytest.raises(ValueError, match="Registered datasets require ETL receipts"):
        require_registered_receipts(tables, None)
    retire("federal_register")
    require_registered_receipts(tables, None)
    stated = {"policies": [subject_catalog.descriptors()["federal_register"]]}
    with pytest.raises(ValueError, match=r"Receipts are retired for \['federal_register'\]"):
        require_registered_receipts(tables, stated)
    # Every other registered table still needs its receipts.
    with pytest.raises(ValueError, match="Registered datasets require ETL receipts"):
        require_registered_receipts({**tables, "fr_docket_links.parquet": {}}, None)


def test_the_ledger_guard_checks_a_retired_table_like_any_other(retire):
    """A marked table's policy may still change in the commit that marks it; an unrecorded change still fails."""
    from spicy_regs.etl_policy_registry import installed_policies

    installed = installed_policies()
    current = installed["federal_register"]
    installed["federal_register"] = replace(current, subject_schema=current.subject_schema.append(
        pa.field("added_later", pa.string())), policy_version="regulations-native-v9")
    migrations = copy.deepcopy(dict(ledger()))
    retire("federal_register")
    assert [p.split(":")[0] for p in unreadable_shipped_policies(installed, migrations)] == ["federal_register"]


def test_the_generator_keeps_the_marker_its_installed_file_carries(retire):
    """``scripts/update_etl_policy_declarations.py`` regenerates every declaration; the marker survives it."""
    from scripts.update_etl_policy_declarations import declarations

    retire("federal_register")
    declared = declarations(frozenset({"federal_register", "fr_docket_links"}))
    assert declared["federal_register"][subject_catalog.RECEIPTS_RETIRED] is True
    assert subject_catalog.RECEIPTS_RETIRED not in declared["fr_docket_links"]
    assert subject_catalog.descriptor_of(declared["federal_register"]) == subject_catalog.descriptors()["federal_register"]


def test_the_dictionary_lists_no_receipts_for_a_retired_table_and_checks_it_as_native(retire):
    from spicy_regs import data_dictionary

    assert "federal_register" in {d["dataset"] for d in data_dictionary.receipt_metadata()["datasets"]}
    retire("federal_register")
    assert "federal_register" not in {d["dataset"] for d in data_dictionary.receipt_metadata()["datasets"]}
    # A family entry with no receipt member is a legacy (pre-receipts) table, except a retired one: that one is the
    # native table by design and is checked against the native declaration.
    index = {"families": {"federal-register": {"tables": {"federal_register.parquet": {}}}}}
    _, schemas = data_dictionary.selected_dictionary_definitions(index, ["federal_register"],
                                                                 data_dictionary.DEFAULT_DESCRIPTIONS)
    assert schemas["federal_register"] == data_dictionary.expected_schemas()["federal_register"]


def test_the_receipt_adapter_reads_a_retired_table_as_published(tmp_path, retire):
    import duckdb

    from spicy_regs.fec_receipt_adapter import ReceiptAdapter, published_without_receipts

    key = "federal_register.parquet"
    (tmp_path / key).write_bytes(REGISTER.read_bytes())  # a local mirror holds a member by its key
    digest = "sha256:" + hashlib.sha256(REGISTER.read_bytes()).hexdigest()
    rows = pq.ParquetFile(REGISTER).metadata.num_rows
    index = {"families": {"federal-register": {"prefix": "generations/federal-register/" + "a" * 64, "tables": {
        key: {"sha256": digest, "byteSize": REGISTER.stat().st_size, "rows": rows}}}}}
    adapter = ReceiptAdapter(duckdb.connect(), index, "", local_directory=tmp_path)
    assert not published_without_receipts(index, "federal_register")
    with pytest.raises(ValueError, match="require selected native receipts"):
        list(adapter.selected_rows("federal_register"))
    retire("federal_register")
    assert published_without_receipts(index, "federal_register")
    assert list(adapter.selected_rows("federal_register")) == pq.read_table(REGISTER).to_pylist()


def test_readers_of_schema_and_identity_keep_a_retired_table_as_it_was(retire):
    """explorer_metadata, relationship_views, MCP schemas and the CLI read the declaration's schema and identity."""
    before = json.dumps(subject_catalog.descriptors()["federal_register"], sort_keys=True)
    retire("federal_register")
    assert json.dumps(subject_catalog.descriptors()["federal_register"], sort_keys=True) == before
    assert not subject_catalog.descriptors()["federal_register"]["receipt_only"]


def test_a_retired_parent_is_selected_as_its_pinned_public_table(tmp_path, monkeypatch, retire):
    """``SelectedInputs.select`` (remote): no receipt owner and the marker set give the table alone; unmarked refuses."""
    from spicy_regs.selected_generations import SelectedInputs
    from spicy_regs.sources import publication

    key = "federal_register.parquet"
    digest = "sha256:" + hashlib.sha256(REGISTER.read_bytes()).hexdigest()
    index = {"families": {"federal-register": {"prefix": "generations/federal-register/" + "a" * 64, "tables": {
        key: {"sha256": digest, "byteSize": REGISTER.stat().st_size, "rows": pq.read_metadata(REGISTER).num_rows}}}}}

    def fetch(base, member, target, path):
        target.write_bytes(REGISTER.read_bytes())
        return True

    monkeypatch.setattr(publication, "fetch_member", fetch)
    with pytest.raises(ValueError, match="requires one ETL receipt generation"):
        SelectedInputs(tmp_path / "root", tmp_path / "unmarked", index=index, public_url="https://x.invalid").select(
            "federal_register")
    retire("federal_register")
    selected = SelectedInputs(tmp_path / "root", tmp_path / "marked", index=index,
                              public_url="https://x.invalid").select("federal_register")
    assert selected is not None and selected.receipts is None and selected.generation_id is None
    [subject] = selected.subjects
    assert subject.read_bytes() == REGISTER.read_bytes()


def test_a_receipt_route_reader_gets_a_retired_parent_as_its_mapper_reads_it(tmp_path, monkeypatch, retire):
    """Consumers and parents switch in either order: a reader still on receipts (``SelectedPriors``, which every
    receipt rollup's inputs and soft inputs go through) gets a retired Register table as the regulations builders read
    their input, bound to the table alone. (Until step 3 the docket links were such a consumer, and published from it
    exactly the links the receipt route published; they are a plain rollup now.)"""
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors
    from spicy_regs.receipt_switch import table_differences
    from spicy_regs.transforms.regulations_shape import published_as_source

    root = tmp_path / "output"
    root.mkdir()
    (root / "federal_register.parquet").write_bytes(REGISTER.read_bytes())  # the plain rollup's local table
    retire("federal_register")
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    prior = SelectedPriors(tmp_path / "probe", root=root, public_url="")
    restored = prior.get("federal_register")
    assert prior.selections["federal_register"][1:] == (None, None)
    assert "docket_ids_json" in pq.read_schema(restored).names  # the builder's text shape, not the native lists
    expected = published_as_source("federal_register", REGISTER, tmp_path / "expected.parquet")
    assert table_differences(restored, expected, key=("document_number", "publication_date")) == []
    binding = prior.input_provenance("federal_register")
    assert binding["receipts"] is None and binding["subjects"] == [{"sha256": "sha256:" + hashlib.sha256(
        REGISTER.read_bytes()).hexdigest(), "byteSize": REGISTER.stat().st_size}]


def test_a_retired_parent_without_a_reader_for_its_mapper_refuses_by_name(tmp_path, retire):
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors

    root = tmp_path / "output"
    root.mkdir()
    (root / "bill_cosponsors.parquet").write_bytes(REGISTER.read_bytes())
    retire("bill_cosponsors")
    with pytest.raises(ValueError, match="bill_cosponsors: its receipts are retired and its mapper has no reader"):
        SelectedPriors(tmp_path / "probe", root=root, public_url="").get("bill_cosponsors")


def pin_archive(monkeypatch, tmp_path, archives):
    """Serve ``archives`` as docs/pins/receipt-archive.json to retention's reader, for one test."""
    from spicy_regs import generation_retention

    path = tmp_path / "receipt-archive.json"
    path.write_text(json.dumps({"format": generation_retention.RECEIPT_ARCHIVE_FORMAT, "version": 1,
                                "archives": archives}))
    monkeypatch.setattr(generation_retention, "RECEIPT_ARCHIVE", path)


def _pin(family, digest):
    hexd = digest.removeprefix("sha256:")
    return {"family": family, "artifactDigest": digest, "publishedAt": "2026-10-10T15:45:23Z",
            "receipts": f"generations/{family}/{hexd}/etl_receipts.parquet"}


@pytest.mark.parametrize("stale", [False, True], ids=["live-generation-pinned", "stale-pin"])
def test_the_first_receipts_free_publish_needs_the_live_generation_pinned(tmp_path, monkeypatch, retire, stale):
    """Shaped like comments on 2026-11-10: one family, two tables, both retired together, both pinned to the family's
    live generation (the last with receipts). A pin of any other generation is refused, naming table and both
    digests, and nothing is published."""
    from spicy_regs import etl_policy_registry
    from spicy_regs.etl_receipts import DatasetPolicy, ReceiptContext, combine_receipts, write_dataset
    from spicy_regs.generations import build_generation
    from spicy_regs.sources.publication import PublicationError, empty_index, publish_generation
    from tests.generation_fakes import Store

    store, schema = Store(), pa.schema([("id", pa.string())])
    policies = [DatasetPolicy(name, schema, ("id",), ("held",)) for name in ("comments", "comments_index")]
    monkeypatch.setattr(etl_policy_registry, "installed_policies", lambda: {p.dataset: p for p in policies})
    held = ReceiptContext("build-1", "row", "test", [{"source_id": "s", "body_version": "1"}])
    pairs = [write_dataset([({"id": "1", "held": "x"}, held)], tmp_path / p.dataset, p) for p in policies]
    receipts = combine_receipts([receipt for _, receipt in pairs], tmp_path / "etl_receipts.parquet")
    tables = [subject for subject, _ in pairs if subject is not None]
    keys = [p.dataset + ".parquet" for p in policies]
    build_generation(tmp_path / "with-receipts", family="comments", files=tables, expected_keys=keys,
                     receipt_path=receipts, receipt_policies=policies, receipt_generation_id="build-1")
    index = publish_generation(tmp_path / "with-receipts", client=store, bucket="b", prior_index=empty_index())
    live = index["families"]["comments"]["artifactDigest"]
    pinned = "sha256:" + "ab" * 32 if stale else live
    pin_archive(monkeypatch, tmp_path, {"comments": _pin("comments", live), "comments_index": _pin("comments", pinned)})
    retire("comments", "comments_index")
    build_generation(tmp_path / "receipts-free", family="comments", files=tables, expected_keys=keys)
    if stale:
        with pytest.raises(PublicationError, match=f"comments_index leaves its receipts in comments generation {live}, "
                                                   f"but docs/pins/receipt-archive.json pins comments {pinned}"):
            publish_generation(tmp_path / "receipts-free", client=store, bucket="b", prior_index=index)
        assert "etlReceipts" in store_index(store)["families"]["comments"]
    else:
        published = publish_generation(tmp_path / "receipts-free", client=store, bucket="b", prior_index=index)
        assert "etlReceipts" not in published["families"]["comments"]


def store_index(store):
    from spicy_regs.sources import publication

    return publication.parse_index(store.objects[publication.INDEX_V2_KEY])


def test_a_retirement_cannot_be_checked_without_the_pin_file(tmp_path, monkeypatch):
    from spicy_regs import generation_retention
    from spicy_regs.sources.publication import PublicationError, _require_receipt_archive_pins

    monkeypatch.setattr(generation_retention, "RECEIPT_ARCHIVE", tmp_path / "missing.json")
    with pytest.raises(PublicationError, match="publish a retirement from a spicy-regs checkout"):
        _require_receipt_archive_pins("comments", "sha256:" + "ab" * 32, {"comments"})


def test_the_mcp_server_serves_no_legacy_meanings_for_a_retired_table(monkeypatch, retire):
    """A family entry without receipts read as a pre-native generation; a retired table's entry has none by design."""
    from spicy_regs import mcp_server

    index = {"families": {"federal-register": {"tables": {"federal_register.parquet": {}}}}}
    monkeypatch.setattr(mcp_server, "_connection_local_selection", lambda cursor: None)
    monkeypatch.setattr(mcp_server, "_connection_index", lambda cursor: index)
    entry = {"legacy_column_descriptions": {"docket_ids_json": "The pre-native spelling."}}
    import duckdb

    with duckdb.connect() as cursor:
        retire()
        assert mcp_server._legacy_column_meanings(cursor, "federal_register", entry) == entry[
            "legacy_column_descriptions"]
        retire("federal_register")
        assert mcp_server._legacy_column_meanings(cursor, "federal_register", entry) == {}


@pytest.mark.parametrize("mode", ["pinned", "unpinned", "unretired"])
def test_receipt_only_dataset_retires_without_a_public_table_when_pinned(tmp_path, monkeypatch, retire, mode):
    """A receipt_only dataset can retire alongside its family's tables without becoming a public table,
    provided its last receipted generation is pinned; unpinned or unretired drops still refuse (ss-vse3 F1)."""
    from spicy_regs import etl_policy_registry
    from spicy_regs.etl_receipts import DatasetPolicy, ReceiptContext, combine_receipts, write_dataset
    from spicy_regs.generations import build_generation
    from spicy_regs.sources.publication import (
        PublicationError,
        empty_index,
        publish_generation,
        require_live_receipt_pins,
    )
    from tests.generation_fakes import Store

    store, schema = Store(), pa.schema([("id", pa.string())])
    table_policy = DatasetPolicy("sample_table", schema, ("id",), ("held",))
    state_policy = DatasetPolicy("sample_state", pa.schema([]), (), ("held",), receipt_only=True)
    policies = [table_policy, state_policy]
    monkeypatch.setattr(etl_policy_registry, "installed_policies", lambda: {p.dataset: p for p in policies})

    held = ReceiptContext("build-1", "row", "test", [{"source_id": "s", "body_version": "1"}])
    table_subject, table_receipts = write_dataset([({"id": "1", "held": "x"}, held)], tmp_path / "table", table_policy)
    _, state_receipts = write_dataset([({"held": "state-1"}, held)], tmp_path / "state", state_policy)
    assert table_subject is not None
    receipts = combine_receipts([table_receipts, state_receipts], tmp_path / "etl_receipts.parquet")
    build_generation(
        tmp_path / "with-receipts",
        family="sample-family",
        files=[table_subject],
        expected_keys=["sample_table.parquet"],
        receipt_path=receipts,
        receipt_policies=policies,
        receipt_generation_id="build-1",
    )
    index = publish_generation(tmp_path / "with-receipts", client=store, bucket="b", prior_index=empty_index())
    live = index["families"]["sample-family"]["artifactDigest"]
    assert set(index["families"]["sample-family"]["etlReceipts"]["datasets"]) == {"sample_table", "sample_state"}

    if mode == "unretired":
        pin_archive(monkeypatch, tmp_path, {"sample_table": _pin("sample-family", live)})
        retire("sample_table")
        build_generation(
            tmp_path / "receipts-free",
            family="sample-family",
            files=[table_subject],
            expected_keys=["sample_table.parquet"],
        )
        with pytest.raises(PublicationError, match="drops required ETL receipts|drops receipt dataset ownership"):
            publish_generation(tmp_path / "receipts-free", client=store, bucket="b", prior_index=index)
        return

    pinned = live if mode == "pinned" else "sha256:" + "ab" * 32
    pin_archive(
        monkeypatch,
        tmp_path,
        {"sample_table": _pin("sample-family", live), "sample_state": _pin("sample-family", pinned)},
    )
    retire("sample_table", "sample_state")
    build_generation(
        tmp_path / "receipts-free",
        family="sample-family",
        files=[table_subject],
        expected_keys=["sample_table.parquet"],
    )
    if mode == "unpinned":
        with pytest.raises(PublicationError, match=f"sample_state leaves its receipts in sample-family generation {live}"):
            require_live_receipt_pins(index, ["sample_state"])
        with pytest.raises(PublicationError, match=f"sample_state leaves its receipts in sample-family generation {live}"):
            publish_generation(tmp_path / "receipts-free", client=store, bucket="b", prior_index=index)
        assert "etlReceipts" in store_index(store)["families"]["sample-family"]
    else:
        require_live_receipt_pins(index, ["sample_table", "sample_state"])
        published = publish_generation(tmp_path / "receipts-free", client=store, bucket="b", prior_index=index)
        assert "etlReceipts" not in published["families"]["sample-family"]
        assert list(published["families"]["sample-family"]["tables"]) == ["sample_table.parquet"]


def test_retiring_table_dataset_still_refuses_dropping_its_table(tmp_path, monkeypatch, retire):
    """Relaxing the table requirement for retired receipt_only datasets must NOT allow dropping a real table."""
    from spicy_regs import etl_policy_registry
    from spicy_regs.etl_receipts import DatasetPolicy, ReceiptContext, combine_receipts, write_dataset
    from spicy_regs.generations import build_generation
    from spicy_regs.sources.publication import PublicationError, empty_index, publish_generation
    from tests.generation_fakes import Store

    store, schema = Store(), pa.schema([("id", pa.string())])
    p1 = DatasetPolicy("table_one", schema, ("id",), ("held",))
    p2 = DatasetPolicy("table_two", schema, ("id",), ("held",))
    monkeypatch.setattr(etl_policy_registry, "installed_policies", lambda: {p.dataset: p for p in (p1, p2)})
    held = ReceiptContext("build-1", "row", "test", [{"source_id": "s", "body_version": "1"}])
    s1, r1 = write_dataset([({"id": "1", "held": "x"}, held)], tmp_path / "t1", p1)
    s2, r2 = write_dataset([({"id": "2", "held": "y"}, held)], tmp_path / "t2", p2)
    assert s1 is not None and s2 is not None
    receipts = combine_receipts([r1, r2], tmp_path / "etl_receipts.parquet")
    build_generation(
        tmp_path / "with-receipts",
        family="two-tables",
        files=[s1, s2],
        expected_keys=["table_one.parquet", "table_two.parquet"],
        receipt_path=receipts,
        receipt_policies=[p1, p2],
        receipt_generation_id="build-1",
    )
    index = publish_generation(tmp_path / "with-receipts", client=store, bucket="b", prior_index=empty_index())
    live = index["families"]["two-tables"]["artifactDigest"]
    pin_archive(monkeypatch, tmp_path, {"table_one": _pin("two-tables", live), "table_two": _pin("two-tables", live)})
    retire("table_one", "table_two")
    # Build a generation that omits table_two.parquet (even if old_family.tables didn't guard it, the retired check refuses).
    from spicy_regs.sources import publication as pub

    index["families"]["two-tables"]["tables"].pop("table_two.parquet")
    store.objects[pub.INDEX_V2_KEY] = json.dumps(index).encode()
    build_generation(
        tmp_path / "dropped-table",
        family="two-tables",
        files=[s1],
        expected_keys=["table_one.parquet"],
    )
    with pytest.raises(PublicationError, match="A retired receipt dataset keeps its table and publishes no receipts"):
        publish_generation(tmp_path / "dropped-table", client=store, bucket="b", prior_index=index)


def test_documents_family_refuses_when_documents_is_not_marked_retired(tmp_path, monkeypatch, retire):
    """DocumentsFamily only publishes receiptlessly when documents is marked receipts_retired."""
    from spicy_regs.pipelines.rollups.regulatory_base import DocumentsFamily

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    retire()  # unmark documents
    with pytest.raises(ValueError, match="documents still publishes receipts"):
        DocumentsFamily(output_dir=tmp_path).build(tmp_path)


def test_a_selection_says_whose_receipts_are_retired():
    from spicy_regs.selected_generations import SelectedDataset

    held = SelectedDataset("documents", (), Path("r.parquet"), "g")
    assert held.required_receipts("a reader") == Path("r.parquet")
    with pytest.raises(ValueError, match="comments has retired receipts; a reader still reads them"):
        SelectedDataset("comments", (), None, None).required_receipts("a reader")
