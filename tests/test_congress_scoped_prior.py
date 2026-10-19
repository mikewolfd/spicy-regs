"""Reuse only the owner's pinned dataset receipt selection, with unchanged carry."""

import hashlib

import pyarrow.parquet as pq
import pytest

from spicy_regs import congress_receipts, etl_bulk, selected_generations, votes_batch_receipts
from spicy_regs.congress_receipts import CongressInput
from spicy_regs.etl_receipts import combine_receipts
from spicy_regs.pipelines.rollups import subject_receipts
from tests.test_congress_receipts import shaped
from tests.test_subject_receipt_rollups import select_bundle
from tests.test_votes_carry import VotesRollup, _accepted, _builder, _night, _rows


@pytest.mark.parametrize("writer", [congress_receipts, votes_batch_receipts], ids=["public", "batch"])
@pytest.mark.parametrize("bulk", [False, True], ids=["row", "bulk"])
def test_scoped_carry_equals_default_mixed_family_selection(tmp_path, monkeypatch, writer, bulk):
    _, subject, receipts = _night(tmp_path, "member_votes", "old", _rows("member_votes", 3))
    foreign_source = shaped(tmp_path / "foreign.parquet", [{"bioguide_id": "X", "fec_ids_json": "invalid"}])
    _, foreign = congress_receipts.write_congress_dataset(
        foreign_source, tmp_path / "foreign", dataset="members", generation_id="old"
    )
    mixed = combine_receipts([receipts, foreign], tmp_path / "family.parquet")
    source = shaped(tmp_path / "new.parquet", _rows("member_votes", 4, {1}))
    selected = []
    admitted = []
    real_select = congress_receipts.select_receipts
    real_validate = etl_bulk.validate_bundle

    def select(*args, **kwargs):
        selected.append(args[0])
        return real_select(*args, **kwargs)

    def validate(*args, **kwargs):
        admitted.append(kwargs.get("generation_id"))
        return real_validate(*args, **kwargs)

    monkeypatch.setattr(congress_receipts, "select_receipts", select)
    monkeypatch.setattr(votes_batch_receipts, "select_receipts", select)
    monkeypatch.setattr(etl_bulk, "validate_bundle", validate)
    default = writer.write_congress_dataset(
        source,
        tmp_path / "default",
        dataset="member_votes",
        generation_id="new",
        prior=CongressInput(subject, mixed, "old"),
        bulk=bulk,
    )
    assert selected == [mixed]
    assert admitted == ["old", "new"]
    selected.clear()
    admitted.clear()
    scoped = writer.write_congress_dataset(
        source,
        tmp_path / "scoped",
        dataset="member_votes",
        generation_id="new",
        prior=CongressInput(subject, receipts, "old"),
        prior_scoped=True,
        bulk=bulk,
    )
    assert selected == []
    assert admitted == ["old", "new"]
    for baseline, reused in zip(default, scoped, strict=True):
        assert pq.read_table(baseline).equals(pq.read_table(reused), check_metadata=True)
    assert set(pq.read_table(scoped[1])["dataset"].to_pylist()) == {"member_votes"}
    before, after = _accepted(receipts), _accepted(scoped[1])
    assert sum(after[key] == row for key, row in before.items()) == 2


def _selected_votes(tmp_path, monkeypatch):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    first_dir = tmp_path / "first"
    first_dir.mkdir()
    first = VotesRollup()
    first.build_receipts(first_dir, _builder(_rows("member_votes", 60)))
    index = select_bundle(monkeypatch, first_dir, first)
    index["families"][first.name]["artifactDigest"] = "sha256:" + "a" * 64
    second_dir = tmp_path / "second"
    second_dir.mkdir()
    return first, first_dir, second_dir


def test_owned_rollup_selects_once_and_keeps_original_provenance(tmp_path, monkeypatch):
    first, first_dir, second_dir = _selected_votes(tmp_path, monkeypatch)
    selections, writes, hashes, admissions = [], [], [], []
    real_select = subject_receipts.select_receipts
    real_write = subject_receipts.write_congress_dataset
    real_pin = selected_generations._pin
    real_validate = etl_bulk.validate_bundle

    def select(*args, **kwargs):
        selections.append(args[0])
        return real_select(*args, **kwargs)

    def write(*args, **kwargs):
        writes.append(kwargs)
        assert kwargs["prior_scoped"] is True
        assert kwargs["prior"].receipts.name == "scoped.parquet"
        assert kwargs["validate"] is False
        return real_write(*args, **kwargs)

    def pin(path):
        hashes.append(path)
        return real_pin(path)

    def validate(*args, **kwargs):
        admissions.append(kwargs.get("generation_id"))
        return real_validate(*args, **kwargs)

    def unexpected_selection(*args, **kwargs):
        pytest.fail("writer filtered an already selected receipt file")

    monkeypatch.setattr(subject_receipts, "select_receipts", select)
    monkeypatch.setattr(subject_receipts, "write_congress_dataset", write)
    monkeypatch.setattr(congress_receipts, "select_receipts", unexpected_selection)
    monkeypatch.setattr(votes_batch_receipts, "select_receipts", unexpected_selection)
    monkeypatch.setattr(selected_generations, "_pin", pin)
    monkeypatch.setattr(etl_bulk, "validate_bundle", validate)
    second = VotesRollup()
    private, prior = second._new_receipt_work(second_dir)
    second._primed_receipt_work = second_dir, private, prior
    second.build_receipts(second_dir, _builder(_rows("member_votes", 60, {5})))
    assert len(selections) == len(writes) == 1
    scoped = writes[0]["prior"].receipts
    assert sum(path == scoped.absolute() for path in hashes) == 1
    assert prior.scoped_receipts["member_votes"] == scoped
    original = prior.selections["member_votes"][1]
    assert original != scoped and original == selections[0]
    assert prior.input_provenance("member_votes")["receipts"]["sha256"] == (
        "sha256:" + hashlib.sha256((first_dir / "etl_receipts.parquet").read_bytes()).hexdigest()
    )
    assert admissions == [first.receipt_generation_id, second.receipt_generation_id]
    before, after = _accepted(first_dir / "etl_receipts.parquet"), _accepted(second_dir / "etl_receipts.parquet")
    assert sum(after[key] == row for key, row in before.items()) == 59


@pytest.mark.parametrize("mode", ["default", "input-only"])
def test_unused_scoped_receipts_get_no_additional_hash(tmp_path, monkeypatch, mode):
    _, _, second_dir = _selected_votes(tmp_path, monkeypatch)
    hashes = []
    real_pin = selected_generations._pin

    def pin(path):
        hashes.append(path)
        return real_pin(path)

    monkeypatch.setattr(selected_generations, "_pin", pin)
    if mode == "default":
        prior = subject_receipts.SelectedPriors(second_dir)
        assert prior.reuse_scoped_datasets == frozenset()
    else:

        class MembersWithVotesInput(subject_receipts.SubjectReceiptRollup):
            name = "members-with-votes-input"
            inputs = ("member_votes.parquet",)
            output = "members.parquet"

            def build(self, output_dir):
                raise NotImplementedError

        _, prior = MembersWithVotesInput()._new_receipt_work(second_dir)
        assert prior.reuse_scoped_datasets == {"members"}
    restored = prior.get("member_votes")
    assert pq.read_table(restored).num_rows == 60
    assert prior.scoped_receipts == {}
    assert not any(path.name == "scoped.parquet" for path in hashes)
    assert prior.get("member_votes") == restored


@pytest.mark.parametrize("when", ["admission", "cached_get", "before_writer", "after_writer"])
def test_owned_rollup_refuses_mutated_scoped_receipts_before_visibility(tmp_path, monkeypatch, when):
    _, _, second_dir = _selected_votes(tmp_path, monkeypatch)
    real_validate = subject_receipts.validate_receipt_bundle
    real_write = subject_receipts.write_congress_dataset
    build = _builder(_rows("member_votes", 60))
    writes = []

    def mutate(path):
        table = pq.read_table(path)
        pq.write_table(table.replace_schema_metadata({b"changed": b"after binding"}), path)

    def validate(*args, **kwargs):
        result = real_validate(*args, **kwargs)
        if when == "admission":
            mutate(args[1][0])
        return result

    def builder(directory, *, download_prior):
        result = build(directory, download_prior=download_prior)
        if when in {"before_writer", "cached_get"}:
            mutate(directory.parent / "selected-priors" / "member_votes" / "scoped.parquet")
        if when == "cached_get":
            download_prior(result.name, result)
        return result

    def write(*args, **kwargs):
        writes.append(True)
        result = real_write(*args, **kwargs)
        if when == "after_writer":
            mutate(kwargs["prior"].receipts)
        return result

    monkeypatch.setattr(subject_receipts, "validate_receipt_bundle", validate)
    monkeypatch.setattr(subject_receipts, "write_congress_dataset", write)
    with pytest.raises(ValueError, match="Selected native member changed"):
        VotesRollup().build_receipts(second_dir, builder)
    assert writes == ([True] if when == "after_writer" else [])
    assert not (second_dir / "member_votes.parquet").exists()
    assert not (second_dir / "etl_receipts.parquet").exists()


@pytest.mark.parametrize("bulk", [False, True])
def test_scoped_prior_preserves_fresh_failure_reporting(tmp_path, bulk):
    rows = [{"vote_id": None, "member_key": "k"}]
    _, subject, receipts = _night(tmp_path, "member_votes", "old", rows, bulk=bulk)
    failures = []
    _, _, current = _night(
        tmp_path,
        "member_votes",
        "new",
        rows,
        bulk=bulk,
        prior=CongressInput(subject, receipts, "old"),
        prior_scoped=True,
        on_fresh_failure=lambda: failures.append("refused"),
    )
    assert failures == ["refused"]
    refused = next(row for row in pq.read_table(receipts).to_pylist() if row["outcome"] == "refused")
    assert refused in pq.read_table(current).to_pylist()
