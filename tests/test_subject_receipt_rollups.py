"""Scheduled producer integration keeps selected processing priors out of subjects."""

import hashlib
import shutil

import pyarrow.parquet as pq
import pytest

from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors, SubjectReceiptRollup
from spicy_regs.sources import publication
from tests.test_congress_receipts import shaped
from tests.test_legislative_receipts import retained, row


class MixedRollup(SubjectReceiptRollup):
    name = "mixed-test"
    outputs = ("members.parquet", "laws.parquet", "bill_family_archives.parquet")

    def build(self, output_dir):
        raise NotImplementedError


def mixed_builder(directory, *, download_prior):
    # A retained empty technical read still has file metadata and must resume.
    archive = directory / "bill_family_archives.parquet"
    if not download_prior(archive.name, archive):
        shaped(archive, [{"congress": "119", "bill_type": "hr", "size": "50"}], {b"completed": b"yes"})
    member = directory / "members.parquet"
    if not download_prior(member.name, member):
        shaped(member, [{"bioguide_id": "X", "fec_ids_json": '[ "A", "A" ]', "observed_at": "then"}])
    law = directory / "laws.parquet"
    if not download_prior(law.name, law):
        retained(
            directory,
            "laws",
            [
                row(
                    "laws",
                    law_id="119-public-1",
                    congress="119",
                    law_type="public",
                    number="1",
                    uslm_outcome="request_failed",
                )
            ],
        )
    return member, law, archive


def select_bundle(monkeypatch, directory, pipeline):
    generation = pipeline.receipt_generation_id
    receipt = directory / "etl_receipts.parquet"
    keys = list(pipeline.outputs)
    index = publication.empty_index()

    def entry(path):
        return {
            "sha256": "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest(),
            "byteSize": path.stat().st_size,
            "rows": pq.read_metadata(path).num_rows,
        }

    tables = {}
    for key in keys:
        path = directory / key
        if path.exists():
            tables[key] = entry(path)
        else:
            root = directory / key.removesuffix(".parquet")
            members = [{"key": str(p.relative_to(directory)), **entry(p)} for p in sorted(root.rglob("*.parquet"))]
            tables[key] = {"members": members, "rows": sum(m["rows"] for m in members)}
    index["families"][pipeline.name] = {
        "prefix": "generations/mixed/" + "a" * 64,
        "tables": tables,
        "etlReceipts": {
            "key": receipt.name,
            **entry(receipt),
            "generationId": generation,
            "datasets": [p.dataset for p in pipeline.receipt_policies],
        },
    }
    monkeypatch.setenv("R2_PUBLIC_URL", "https://selected.invalid")
    monkeypatch.setattr(publication, "current_index", lambda _: index)

    def fetch(base, member, target, label):
        source = directory / member.key
        assert member.sha256 == entry(source)["sha256"]
        shutil.copyfile(source, target)
        return True

    monkeypatch.setattr(publication, "fetch_member", fetch)
    return index


def test_mixed_family_restores_both_owners_and_receipt_only_archive(tmp_path, monkeypatch):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    first_dir = tmp_path / "first"
    first_dir.mkdir()
    first = MixedRollup()
    outputs = first.build_receipts(first_dir, mixed_builder)
    assert {p.name for p in outputs} == {"members.parquet", "laws.parquet"}
    assert "bill_family_archives.parquet" in first.receipt_only_tables
    assert "observed_at" not in pq.read_schema(first_dir / "members.parquet").names
    assert pq.read_table(first_dir / "members.parquet")["fec_ids"].to_pylist() == [["A", "A"]]
    select_bundle(monkeypatch, first_dir, first)
    second_dir = tmp_path / "second"
    second_dir.mkdir()
    second = MixedRollup()
    again = second.build_receipts(second_dir, mixed_builder)
    assert {p.name for p in again} == {p.name for p in outputs}
    for path in outputs:
        assert pq.read_table(path).equals(pq.read_table(second_dir / path.name))
    restored = next(second_dir.glob(".builds/*/source-output/bill_family_archives.parquet"))
    assert pq.read_schema(restored).metadata[b"completed"] == b"yes"
    assert pq.read_table(next(second_dir.glob(".builds/*/source-output/laws.parquet")))["uslm_outcome"].to_pylist() == [
        "request_failed"
    ]


def test_missing_receipts_refuse_before_processing_read(tmp_path, monkeypatch):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    directory = tmp_path / "first"
    directory.mkdir()
    first = MixedRollup()
    first.build_receipts(directory, mixed_builder)
    index = select_bundle(monkeypatch, directory, first)
    del index["families"][first.name]["etlReceipts"]
    with pytest.raises(ValueError, match="conversion refused"):
        SelectedPriors(tmp_path / "bad").get("members")


def test_bad_native_value_keeps_refusal_and_cannot_admit_complete_family(tmp_path, monkeypatch):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)

    class Members(SubjectReceiptRollup):
        name = "members-test"
        output = "members.parquet"

        def build(self, output_dir):
            raise NotImplementedError

    def bad(directory, *, download_prior):
        return shaped(directory / "members.parquet", [{"bioguide_id": "X", "fec_ids_json": "bad"}])

    with pytest.raises(ValueError, match="conversion refused"):
        Members().build_receipts(tmp_path, bad)
    assert "refused" in pq.read_table(tmp_path / "etl_receipts.parquet")["outcome"].to_pylist()


def test_held_citations_carries_exact_sibling_receipts_and_zero_result_checkpoint(tmp_path, monkeypatch):
    import json
    import pyarrow as pa
    from spicy_regs.legislative_receipts import migrate_outputs, restore_prior
    from spicy_regs.pipelines.rollups.held_citations import HeldCitationsRollup
    from spicy_regs.etl_receipts import validate_receipt_bundle
    from spicy_regs.transforms.held_citations import NAMESPACE
    from spicy_regs.transforms.read_checkpoints import read_checkpoints

    raws = [
        retained(tmp_path / "raw", name, [row(name, package_id="BUDGET-2027-BUD")] if name == "budget_volumes" else [])
        for name in ("house_activity_reports", "budget_volumes", "bill_committee_actions", "document_citations")
    ]
    bundle = tmp_path / "bundle"
    migrate_outputs(raws, bundle, generation_id="before")
    monkeypatch.setenv("LEGISLATIVE_PRIOR_BUNDLE", str(bundle))
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    manifest = tmp_path / "selection.json"
    manifest.write_text(
        json.dumps(
            {
                "selections": [{"kind": "comment_inline", "keys": ["C-1"]}],
                "input_generations": {"comments": "sha256:" + "a" * 64},
            }
        )
    )
    source = tmp_path / "comments.parquet"
    pq.write_table(pa.Table.from_pylist([{"comment_id": "C-1", "comment": "No legal citations here."}]), source)
    pipeline = HeldCitationsRollup(selection=manifest)
    pipeline.source_members = {"comments": str(source)}
    pipeline.input_pins = {"comments": {"artifactDigest": "sha256:" + "a" * 64}}
    monkeypatch.setattr("spicy_regs.pipelines.rollups.held_citations.load_public_http", lambda *_: None)
    output = tmp_path / "output"
    output.mkdir()
    paths = pipeline.build(output)
    assert {p.name for p in paths} == set(pipeline.outputs)
    validate_receipt_bundle(
        {p.stem: [p] for p in paths} | {"legislative_document_file_states": []},
        [output / "etl_receipts.parquet"],
        pipeline.receipt_policies,
        generation_id=pipeline.receipt_generation_id,
    )
    before = pq.read_table(bundle / "etl_receipts.parquet").to_pylist()
    after = pq.read_table(output / "etl_receipts.parquet").to_pylist()
    for old in before:
        if old["dataset"] == "budget_volumes":
            [new] = [r for r in after if r["dataset"] == old["dataset"] and r["attempt_id"] == old["attempt_id"]]
            assert new["witnesses"] == old["witnesses"]
    restored = restore_prior(output / ".held-native", tmp_path / "restored")
    checkpoint = read_checkpoints(restored["document_citations"], NAMESPACE)
    assert len(checkpoint) == 1 and checkpoint[0]["findings"] == 0


def test_scheduled_fec_catalog_emits_only_receipts(tmp_path, monkeypatch):
    from spicy_regs.pipelines.rollups.fec_source_catalog import FecSourceCatalogRollup

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    rollup = FecSourceCatalogRollup()
    assert rollup.build(tmp_path) == ()
    assert not (tmp_path / "fec_source_catalog.parquet").exists()
    receipts = pq.read_table(tmp_path / "etl_receipts.parquet")
    assert receipts.num_rows > 0
    assert set(receipts["dataset"].to_pylist()) == {"fec_source_catalog"}
    assert set(receipts["outcome"].to_pylist()) == {"observed"}


def test_scheduled_fec_committee_cold_increment_refuses(tmp_path, monkeypatch):
    from spicy_regs.pipelines.rollups.fec_committees import FecCommitteesRollup

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    monkeypatch.delenv("FEC_COMMITTEES_FULL_WALK", raising=False)
    with pytest.raises(ValueError, match="first build requires full_walk"):
        FecCommitteesRollup().build(tmp_path)


def test_actual_bill_family_resumes_both_owners_without_rereading_bodies(tmp_path, monkeypatch):
    from spicy_regs.pipelines.rollups.bill_family import BillFamilyRollup
    from spicy_regs.transforms.build_bill_family import build_bill_family
    from tests.test_bill_family import StubBulkAcquirer, StubBodyAcquirer

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr")
    for key in ("GEMINI_API_KEY", "GOOGLE_API_KEY", "R2_PUBLIC_URL"):
        monkeypatch.delenv(key, raising=False)
    first_dir = tmp_path / "first"
    first_dir.mkdir()
    first = BillFamilyRollup()
    first.build_receipts(
        first_dir, build_bill_family, bulk_acquirer=StubBulkAcquirer(), body_acquirer=StubBodyAcquirer()
    )
    select_bundle(monkeypatch, first_dir, first)
    bulk, body = StubBulkAcquirer(), StubBodyAcquirer()
    second_dir = tmp_path / "second"
    second_dir.mkdir()
    BillFamilyRollup().build_receipts(second_dir, build_bill_family, bulk_acquirer=bulk, body_acquirer=body)
    assert bulk.zip_downloads == []
    assert body.requested == []
    assert list((second_dir / "bill_sections").rglob("*.parquet"))
    assert not (second_dir / "bill_family_archives.parquet").exists()


def test_local_mixed_rollup_can_build_twice_in_one_output_directory(tmp_path, monkeypatch):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    pipeline = MixedRollup()
    first = pipeline.build_receipts(tmp_path, mixed_builder)
    before = {path.name: pq.read_table(path).to_pylist() for path in first}
    second = pipeline.build_receipts(tmp_path, mixed_builder)
    assert {path.name: pq.read_table(path).to_pylist() for path in second} == before
    assert len(list((tmp_path / ".builds").iterdir())) == 2


def test_selected_legacy_generation_is_migrated_before_resuming(tmp_path, monkeypatch):
    from types import SimpleNamespace
    import pyarrow as pa
    from spicy_regs.etl_receipts import read_attempts, select_receipts
    from spicy_regs.pipelines.rollups.subject_receipts import dataset_policy

    directory = tmp_path / "legacy"
    directory.mkdir()
    outputs = mixed_builder(directory, download_prior=lambda *_: False)
    pq.write_table(pa.table({"unused": []}), directory / "etl_receipts.parquet")
    pipeline = SimpleNamespace(
        name="legacy-mixed",
        receipt_generation_id="old-unused",
        outputs=tuple(path.name for path in outputs),
        receipt_policies=MixedRollup.receipt_policies,
    )
    index = select_bundle(monkeypatch, directory, pipeline)
    del index["families"][pipeline.name]["etlReceipts"]
    reader = SelectedPriors(tmp_path / "migration")
    for dataset in ("members", "laws", "bill_family_archives"):
        restored = reader.get(dataset)
        assert pq.read_table(restored).to_pylist() == pq.read_table(directory / (dataset + ".parquet")).to_pylist()
        subjects, receipt, generation = reader.selections[dataset]
        scoped = select_receipts(receipt, tmp_path / (dataset + "-receipts.parquet"), dataset=dataset)
        assert list(read_attempts([scoped], dataset_policy(dataset), generation_id=generation))
    assert pq.read_schema(reader.get("bill_family_archives")).metadata[b"completed"] == b"yes"


def test_fec_committee_legacy_prior_migrates_and_preserves_exact_processing_values(tmp_path, monkeypatch):
    from types import SimpleNamespace
    import pyarrow as pa
    from spicy_regs.transforms.build_fec_committees import COLUMNS, _SCHEMA
    from spicy_regs.pipelines.rollups.subject_receipts import dataset_policy

    directory = tmp_path / "legacy-fec"
    directory.mkdir()
    row = dict.fromkeys(COLUMNS)
    row.update(
        committee_id="C00000001",
        name="Source committee",
        last_file_date="2026-09-30",
        cycles_json="[ 2024, null, 2026, 2024 ]",
        candidate_ids_json='["H0XX00001",null,"H0XX00001"]',
    )
    source = directory / "fec_committees.parquet"
    pq.write_table(pa.Table.from_pylist([row], schema=_SCHEMA), source)
    pq.write_table(pa.table({"unused": []}), directory / "etl_receipts.parquet")
    pipeline = SimpleNamespace(
        name="fec-committees",
        receipt_generation_id="unused",
        outputs=(source.name,),
        receipt_policies=(dataset_policy("fec_committees"),),
    )
    index = select_bundle(monkeypatch, directory, pipeline)
    owner = index["families"][pipeline.name]
    del owner["etlReceipts"]
    owner["artifactDigest"] = "sha256:" + "9" * 64
    reader = SelectedPriors(tmp_path / "migration")
    restored = reader.get("fec_committees")
    assert pq.read_table(restored).to_pylist() == [row]
    subjects, receipt, generation = reader.selections["fec_committees"]
    assert pq.read_table(subjects[0])["cycles"].to_pylist() == [[2024, None, 2026, 2024]]
    accepted = pq.read_table(receipt).to_pylist()[0]
    assert accepted["generation_id"] == generation
    assert accepted["witnesses"][0]["body_version"] == owner["artifactDigest"]
    assert accepted["witnesses"][0]["sha256"] == "sha256:" + hashlib.sha256(source.read_bytes()).hexdigest()
    # Removing receipts from an already-native selected file cannot masquerade as a source-shaped prior.
    native = tmp_path / "native-without-receipts"
    native.mkdir()
    shutil.copyfile(subjects[0], native / source.name)
    shutil.copyfile(receipt, native / "etl_receipts.parquet")
    broken = select_bundle(monkeypatch, native, pipeline)
    del broken["families"][pipeline.name]["etlReceipts"]
    with pytest.raises(ValueError):
        SelectedPriors(tmp_path / "missing-native-evidence").get("fec_committees")
