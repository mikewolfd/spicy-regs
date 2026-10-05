"""A shared log is written by many families and claimed by none; every other receipt dataset keeps one owner."""

import collections
import importlib
import json
import pkgutil

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import subject_catalog
from spicy_regs.congress_receipts import ACQUISITION_POLICY
from spicy_regs.etl_receipts import DatasetPolicy, ReceiptContext, combine_receipts, write_dataset
from spicy_regs.generations import build_generation
from spicy_regs.sources.publication import (
    INDEX_V2_KEY, PublicationError, empty_index, parse_index, publish_generation, receipt_members)
from tests.generation_fakes import Store

SHARED = {"congress_acquisition", "legislative_document_file_states"}
LOG = ACQUISITION_POLICY.dataset


def declared_writers():
    """Each receipt dataset's publishing families, from every pipeline class that declares receipt policies."""
    import spicy_regs.pipelines as pipelines

    writers = collections.defaultdict(set)
    for module in pkgutil.walk_packages(pipelines.__path__, pipelines.__name__ + "."):
        for value in vars(importlib.import_module(module.name)).values():
            if isinstance(value, type) and getattr(value, "receipt_policies", ()):
                family = getattr(value, "publication_family", None) or value.name
                assert isinstance(family, str) and family
                for policy in value.receipt_policies:
                    writers[policy.dataset].add(family)
    return writers


def test_the_shared_logs_are_exactly_the_datasets_several_families_declare():
    """The marker and the declared writers must agree in both directions.

    A dataset that gains a second family without the marker would have its second family refused at publication.
    Decide which it is: a log each family keeps for itself (list it in ``shared_logs`` in
    ``scripts/update_etl_policy_declarations.py`` and regenerate), or a checkpoint that one family owns. A marked
    dataset with one family is a dataset its readers could no longer find by name.

    This sees pipeline classes only. The court, scorecard and regulations catalog writers pass their policies in a
    call; for those, publication's one-owner check is the only guard.
    """
    writers = declared_writers()
    several = {dataset: sorted(families) for dataset, families in writers.items() if len(families) > 1}
    assert set(several) == subject_catalog.shared_receipt_logs() == SHARED, several
    # Two rollups publishing into one family are one writer: the citation reads have one owner.
    assert writers["document_citation_reads"] == {"print-citations"}
    # Some source-owner checkpoints have policies before their rollup declares them.
    # This guard tests actual writers, without assigning undeclared checkpoints an owner.
    for dataset, families in writers.items():
        if dataset.endswith("_reads"):
            assert len(families) == 1 and dataset not in SHARED


def test_the_marker_is_only_in_the_two_generated_declarations():
    from scripts.update_etl_policy_declarations import ROOT, declarations

    generated = declarations()
    installed = {path.stem: json.loads(path.read_text()) for path in (ROOT / "src/spicy_regs/etl_policies").glob("*.json")}
    assert generated == installed
    assert {name for name, declared in installed.items() if subject_catalog.SHARED_LOG in declared} == SHARED
    # What a generation states and a receipt is checked against is the policy alone, without the marker.
    assert subject_catalog.descriptors()[LOG] == ACQUISITION_POLICY.descriptor() == subject_catalog.policies()[LOG].descriptor()


def test_the_marker_is_refused_on_a_subject_table_or_with_another_value():
    subject = subject_catalog.descriptors()["cfr_sections"]
    with pytest.raises(ValueError, match="Only a receipt-only dataset can be declared a shared log: cfr_sections"):
        subject_catalog.is_shared_log({**subject, subject_catalog.SHARED_LOG: True})
    with pytest.raises(ValueError, match="Only a receipt-only dataset"):
        subject_catalog.is_shared_log({**ACQUISITION_POLICY.descriptor(), subject_catalog.SHARED_LOG: "yes"})
    assert not subject_catalog.is_shared_log(subject)
    # A receipt-only policy cannot have subject columns or an identity, so the marker cannot reach one either.
    with pytest.raises(ValueError, match="Invalid explicit field policy"):
        DatasetPolicy(LOG, pa.schema([("id", pa.string())]), ("id",), ("capture_event",), receipt_only=True)


@pytest.fixture
def families(tmp_path):
    """Publish a family's subject table with its own rows of the acquisition log, as every Congress.gov family does."""
    store = Store()
    witness = {"source_id": "listing", "source_uri": "https://example.test/list", "sha256": "a" * 64,
               "locator": "row:1", "body_version": None}

    def publish(family, build, prior):
        subject_policy = DatasetPolicy(family + "_rows", pa.schema([("id", pa.string())]), ("id",), ("note",))
        context = ReceiptContext(build, "attempt-1", "fixture:1", [witness])
        work = tmp_path / build
        subject, subject_receipts = write_dataset([({"id": "1", "note": family}, context)], work / "subject", subject_policy)
        _, log_receipts = write_dataset([({"capture_event": {"listing": "read"}, "build_event": None}, context)],
                                        work / "log", ACQUISITION_POLICY)
        assert subject is not None
        build_generation(work / "generation", family=family, files=[subject], expected_keys=[subject.name],
                         receipt_path=combine_receipts([subject_receipts, log_receipts], work / "etl_receipts.parquet"),
                         receipt_policies=[subject_policy, ACQUISITION_POLICY], receipt_generation_id=build)
        return publish_generation(work / "generation", client=store, bucket="b", prior_index=prior)

    return store, publish


def log_rows(store, index, family):
    entry = index["families"][family]
    table = pq.read_table(pa.BufferReader(store.objects[f"{entry['prefix']}/etl_receipts.parquet"]))
    return [row for row in table.to_pylist() if row["dataset"] == LOG]


def test_two_families_publish_their_own_rows_of_one_log(families):
    store, publish = families
    index = publish("amendments", "build-a", empty_index())
    index = publish("nominations", "build-n", index)

    for family, build in (("amendments", "build-a"), ("nominations", "build-n")):
        receipts = index["families"][family]["etlReceipts"]
        # The entry claims the family's own dataset only; the member still holds the family's log rows.
        assert receipts["datasets"] == [family + "_rows"] and receipts["rows"] == 2
        (row,) = log_rows(store, index, family)
        assert row["generation_id"] == build == receipts["generationId"] and row["record_id"] is None
    assert parse_index(store.objects[INDEX_V2_KEY]) == index
    assert len(receipt_members(index)) == 2
    assert len(receipt_members(index, dataset="amendments_rows")) == 1
    with pytest.raises(PublicationError, match="congress_acquisition is a shared log with no owning family"):
        receipt_members(index, dataset=LOG)
    # Two families that log the same event share no receipt: its generation is part of what a receipt identifies.
    first, second = (log_rows(store, index, family)[0] for family in ("amendments", "nominations"))
    assert first["processing_json"] == second["processing_json"] and first["receipt_id"] != second["receipt_id"]


def test_an_entry_that_lists_a_shared_log_blocks_no_family_and_stops_listing_it(families):
    """The state a family converted before logs were shared would have left: its entry claims the log."""
    store, publish = families
    index = publish("amendments", "build-a", empty_index())
    index["families"]["amendments"]["etlReceipts"]["datasets"].append(LOG)
    store.objects[INDEX_V2_KEY] = json.dumps(index).encode()
    index = parse_index(store.objects[INDEX_V2_KEY])

    index = publish("nominations", "build-n", index)
    assert index["families"]["amendments"]["etlReceipts"]["datasets"] == ["amendments_rows", LOG]
    with pytest.raises(PublicationError, match="shared log with no owning family"):
        receipt_members(index, dataset=LOG)
    index = publish("amendments", "build-a2", index)
    assert [index["families"][name]["etlReceipts"]["datasets"] for name in ("amendments", "nominations")] == [
        ["amendments_rows"], ["nominations_rows"]]


def test_no_reader_accepts_two_families_listing_one_dataset(families):
    """Every index reader refuses a second owner, a shared log included; publication never writes one."""
    store, publish = families
    index = publish("nominations", "build-n", publish("amendments", "build-a", empty_index()))
    for dataset in (LOG, "amendments_rows"):
        doubled = json.loads(json.dumps(index))
        for family in ("amendments", "nominations"):
            listed = doubled["families"][family]["etlReceipts"]["datasets"]
            listed.extend(name for name in (dataset,) if name not in listed)
        with pytest.raises(PublicationError, match="Invalid publication index"):
            parse_index(json.dumps(doubled).encode())
