"""Persisted scorecard receipt joins, exact source replay, and incremental refresh."""

from copy import deepcopy
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import RECEIPT_SCHEMA
from spicy_regs.generations import build_generation, verify_generation
from spicy_regs.scorecards.etl import SOURCE_NAMES, generation_options, read_family, write_family
from spicy_regs.sources import publication as pub
from tests.generation_fakes import Store
from tests.test_scorecard_refresh import edition, tables, provider, evidence, run, combine


def test_native_generation_keeps_exact_provider_rows_and_only_domain_subjects(tmp_path):
    before = tables(edition("2025"))
    directory = tmp_path / "candidate"
    files = write_family(directory, before)
    assert read_family(directory, SOURCE_NAMES) == before
    assert not (directory / "scorecard_snapshots.parquet").exists()
    for path in files:
        names = pq.read_schema(path).names
        assert not {"snapshot_id", "capture_id", "source_url", "source_path", "observed_at"} & set(names)
    target = tmp_path / "generation"
    artifact = build_generation(
        target,
        family="scorecards",
        files=files,
        expected_keys=tuple(p.name for p in files),
        **generation_options(directory, SOURCE_NAMES),
    )
    admitted = verify_generation(target)
    assert admitted.pin == artifact.pin
    assert admitted.root["spec"]["etlReceipts"]["rows"] == sum(map(len, before.values()))


@pytest.mark.parametrize("corruption", ["subject", "receipt", "generation", "duplicate", "missing"])
def test_inconsistent_native_evidence_refuses_before_internal_read(tmp_path, corruption):
    directory = tmp_path / "candidate"
    write_family(directory, tables(edition("2025")))
    kwargs = {}
    if corruption == "subject":
        path = directory / "scorecards.parquet"
        table = pq.read_table(path)
        rows = table.to_pylist()
        rows[0]["title"] = "changed"
        pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), path)
    elif corruption in {"receipt", "duplicate"}:
        path = directory / "etl_receipts.parquet"
        rows = pq.read_table(path).to_pylist()
        if corruption == "receipt":
            rows[0]["processor"] = "changed"
        else:
            rows.append(deepcopy(rows[0]))
        pq.write_table(pa.Table.from_pylist(rows, schema=RECEIPT_SCHEMA), path)
    elif corruption == "generation":
        kwargs["generation_id"] = "wrong-build"
    else:
        (directory / "etl_receipts.parquet").unlink()
    with pytest.raises((ValueError, FileNotFoundError)):
        read_family(directory, SOURCE_NAMES, generation_id=kwargs.get("generation_id"))


@pytest.mark.parametrize("local_receipts", [False, True])
def test_native_prior_reconstructs_validation_and_preserves_unselected_scope(tmp_path, monkeypatch, local_receipts):
    old = combine(tables(edition("2025")), tables(edition("2024")))
    first = tmp_path / "first"
    files = write_family(first, old)
    generation = tmp_path / "generation"
    build_generation(
        generation,
        family="scorecards",
        files=files,
        expected_keys=tuple(p.name for p in files),
        **generation_options(first, SOURCE_NAMES),
    )
    store = Store()
    index = pub.publish_generation(generation, client=store, bucket="test", prior_index=pub.empty_index())

    def fetch(url, member, target, key=None):
        target.write_bytes(store.objects[member.path])
        return True

    monkeypatch.setattr(pub, "fetch_member", fetch)
    monkeypatch.setenv("R2_PUBLIC_URL", "https://data.example")
    second = tmp_path / "second"
    second.mkdir()
    e = evidence(second)
    e.read_snapshot = index

    def download(key, target):
        return fetch(None, pub.single_member(index, key), target)

    selected = edition("2025")
    options = {}
    if local_receipts:
        monkeypatch.delenv("R2_PUBLIC_URL")
        options["download_prior_receipts"] = lambda target: fetch(
            None, pub.receipt_members(index, dataset="scorecards")[0], target
        )
    run(second, provider([selected], {selected.scorecard_id: tables}), e, download_prior=download, **options)
    actual = read_family(second / "build", SOURCE_NAMES)
    from spicy_regs.etl_receipts import subject_identity
    from spicy_regs.scorecards.etl import POLICIES

    key = subject_identity(POLICIES["scorecards"], {"scorecard_id": "lcv:2024"})[0]
    before_receipt = next(r for r in pq.read_table(first / "etl_receipts.parquet").to_pylist()
                          if r["dataset"] == "scorecards" and r["record_id"] == key)
    after_receipt = next(r for r in pq.read_table(second / "build/etl_receipts.parquet").to_pylist()
                         if r["dataset"] == "scorecards" and r["record_id"] == key)
    assert after_receipt["generation_id"] != before_receipt["generation_id"]
    for field in ("processor", "witnesses", "attempt_id", "processing_json", "subject_version"):
        assert after_receipt[field] == before_receipt[field]
    assert before_receipt["receipt_id"] in after_receipt["diagnostic_json"]
    for name, rows in actual.items():
        if name != "scorecard_publishers":
            assert [r for r in rows if r["scorecard_id"] == "lcv:2024"] == [
                r for r in old[name] if r["scorecard_id"] == "lcv:2024"
            ]


def test_conversion_failure_retains_raw_input_without_subject_generation(tmp_path):
    raw = tables(edition("2025"))
    raw["scorecard_member_ratings"][0]["value_number"] = "0.0000000000000000001"
    with pytest.raises(ValueError, match="conversion refused"):
        write_family(tmp_path / "candidate", raw)
    assert not list((tmp_path / "candidate").glob("*.parquet"))
    receipts = list((tmp_path / "candidate").glob(".scorecard-etl-*/etl_receipts.parquet"))
    assert len(receipts) == 1
    from spicy_regs.etl_receipts import _unpack

    failed = [r for r in pq.read_table(receipts[0]).to_pylist() if r["outcome"] == "error"]
    assert len(failed) == 1
    assert _unpack(json.loads(failed[0]["processing_json"]))["raw_source"]["value_number"] == "0.0000000000000000001"


def test_installed_scorecard_policy_refuses_receiptless_new_generation(tmp_path):
    from spicy_regs.etl_policy_registry import installed_policies
    from spicy_regs.scorecards.etl import POLICIES

    installed = installed_policies()
    assert all(installed[name].descriptor() == policy.descriptor() for name, policy in POLICIES.items())
    files = write_family(tmp_path / "candidate", tables(edition("2025")))
    with pytest.raises(ValueError, match="require ETL receipts"):
        build_generation(
            tmp_path / "refused", family="scorecards", files=files, expected_keys=[p.name for p in files]
        )
