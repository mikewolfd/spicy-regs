"""Persisted scorecard receipt joins, exact source replay, and incremental refresh."""

from copy import deepcopy
import json
from dataclasses import replace

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
        kwargs["generation_id"] = ""
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
    assert after_receipt == before_receipt
    for name, rows in actual.items():
        if name != "scorecard_publishers":
            assert [r for r in rows if r["scorecard_id"] == "lcv:2024"] == [
                r for r in old[name] if r["scorecard_id"] == "lcv:2024"
            ]


def test_conversion_failure_retains_raw_input_without_subject_generation(tmp_path):
    raw = tables(edition("2025"))
    raw["scorecard_member_ratings"][0]["value_number"] = "0.00000000000000000001"
    with pytest.raises(ValueError, match="conversion refused"):
        write_family(tmp_path / "candidate", raw)
    assert not list((tmp_path / "candidate").glob("*.parquet"))
    receipts = list((tmp_path / "candidate").glob(".scorecard-etl-*/etl_receipts.parquet"))
    assert len(receipts) == 1
    from spicy_regs.etl_receipts import _unpack

    failed = [r for r in pq.read_table(receipts[0]).to_pylist() if r["outcome"] == "error"]
    assert len(failed) == 1
    assert _unpack(json.loads(failed[0]["processing_json"]))["raw_source"]["value_number"] == "0.00000000000000000001"


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


@pytest.mark.parametrize("numeric", [None, "+080.125"])
def test_historical_rating_policy_is_validated_before_exact_current_rewrite(tmp_path, monkeypatch, numeric):
    from spicy_regs.scorecards import etl
    from spicy_regs.contract_types import described_schema
    from spicy_regs import etl_policy_registry

    before = tables(edition("2024"))
    before["scorecard_member_ratings"][0]["value_number"] = numeric
    first = tmp_path / "historical"
    historical = dict(etl.POLICIES, scorecard_member_ratings=etl.LEGACY_RATING_POLICY)
    with monkeypatch.context() as old_runtime:
        old_runtime.setattr(etl, "POLICIES", historical)
        old_runtime.setattr(etl_policy_registry, "installed_policies", lambda: historical)
        files = etl.write_family(first, before)
        artifact = build_generation(
            tmp_path / "old-generation", family="scorecards", files=files,
            expected_keys=[p.name for p in files], **etl.generation_options(first, SOURCE_NAMES),
        )
    selected = etl.admitted_read_policies(SOURCE_NAMES, descriptors=artifact.root["spec"]["etlReceipts"]["policies"])
    assert selected["scorecard_member_ratings"].descriptor() == etl.LEGACY_RATING_POLICY.descriptor()
    published_columns = {name: described_schema(p.subject_schema) for name, p in historical.items() if name in SOURCE_NAMES and not p.receipt_only}
    assert etl.admitted_read_policies(SOURCE_NAMES, columns=published_columns) == selected
    indexed = {
        "tables": {name + ".parquet": {"columns": columns} for name, columns in published_columns.items()},
        "etlReceipts": {"generationId": artifact.root["spec"]["etlReceipts"]["generationId"]},
    }
    assert etl.read_indexed_family(first, SOURCE_NAMES, indexed) == before
    assert verify_generation(tmp_path / "old-generation").pin == artifact.pin
    assert etl.read_source_generation(tmp_path / "old-generation") == before
    with pytest.raises(ValueError, match="Subject schema differs"):
        etl.read_family(first, SOURCE_NAMES)
    restored = etl.read_family(first, SOURCE_NAMES, policies=selected)
    assert restored == before
    second = tmp_path / "current"
    etl.write_family(second, restored, prior_receipts=first / "etl_receipts.parquet")
    assert etl.read_family(second, SOURCE_NAMES) == before
    indexed["tables"] = {
        name + ".parquet": {"columns": described_schema(etl.POLICIES[name].subject_schema)}
        for name in SOURCE_NAMES if not etl.POLICIES[name].receipt_only
    }
    indexed["etlReceipts"]["generationId"] = etl.generation_options(second, SOURCE_NAMES)["receipt_generation_id"]
    assert etl.read_indexed_family(second, SOURCE_NAMES, indexed) == before
    old = next(r for r in pq.read_table(first / "etl_receipts.parquet").to_pylist() if r["dataset"] == "scorecard_member_ratings")
    new = next(r for r in pq.read_table(second / "etl_receipts.parquet").to_pylist() if r["dataset"] == "scorecard_member_ratings")
    assert old["policy_version"] == "scorecards-etl-v1"
    assert old["record_id"] == new["record_id"]
    assert old["processing_json"] == new["processing_json"]
    from spicy_regs.etl_receipts import decode_exact_json
    if numeric is None:
        assert new == old
    else:
        assert new["policy_version"] == "scorecards-etl-ratings-v2"
        assert new["subject_version"] != old["subject_version"]
        assert decode_exact_json(new["diagnostic_json"])["prior_receipt"]["receipt_id"] == old["receipt_id"]
    assert "retained_processing" not in decode_exact_json(new["diagnostic_json"])


@pytest.mark.parametrize("change", ["version", "schema", "classification", "duplicate"])
def test_historical_policy_selection_refuses_unknown_declarations(change):
    from spicy_regs.scorecards import etl

    descriptors = [p.descriptor() for name, p in etl.POLICIES.items() if name in SOURCE_NAMES]
    rating = next(p for p in descriptors if p["dataset"] == "scorecard_member_ratings")
    if change == "version":
        rating["policy_version"] = "unreviewed"
    elif change == "schema":
        schema = etl.POLICIES["scorecard_member_ratings"].subject_schema
        schema = schema.set(schema.get_field_index("value_number"), schema.field("value_number").with_type(pa.decimal128(38, 20)))
        rating.update(replace(etl.POLICIES["scorecard_member_ratings"], subject_schema=schema).descriptor())
    elif change == "classification":
        rating["receipt_fields"].remove("raw_source")
    else:
        descriptors[-1] = descriptors[0]
    with pytest.raises(ValueError, match="supported exact"):
        etl.admitted_read_policies(SOURCE_NAMES, descriptors=descriptors)


def test_exact_decimal_cast_cannot_reinterpret_historical_receipt_hashes(tmp_path, monkeypatch):
    from spicy_regs.scorecards import etl
    from spicy_regs.etl_receipts import validate_receipt_bundle

    before = tables(edition("2024"))
    before["scorecard_member_ratings"][0]["value_number"] = "80.125"
    with monkeypatch.context() as old_runtime:
        old_runtime.setattr(etl, "POLICIES", dict(etl.POLICIES, scorecard_member_ratings=etl.LEGACY_RATING_POLICY))
        etl.write_family(tmp_path / "historical", before)
    directory = tmp_path / "historical"
    path = directory / "scorecard_member_ratings.parquet"
    table = pq.read_table(path)
    pq.write_table(table.cast(etl.POLICIES["scorecard_member_ratings"].subject_schema, safe=True), path)
    subjects = {name: [] if etl.POLICIES[name].receipt_only else [directory / (name + ".parquet")] for name in SOURCE_NAMES}
    # Keep the old receipt version, while demonstrating that even an exact
    # numerical cast does not preserve the old scale-specific subject hash.
    rating = replace(etl.POLICIES["scorecard_member_ratings"], policy_version="scorecards-etl-v1")
    policies = [rating if name == "scorecard_member_ratings" else etl.POLICIES[name] for name in SOURCE_NAMES]
    with pytest.raises(ValueError, match="Missing, ambiguous or reused subject receipt"):
        validate_receipt_bundle(subjects, [directory / "etl_receipts.parquet"], policies)


def test_malformed_provider_rating_retains_error_attempt_before_refusal(tmp_path):
    before = tables(edition("2025"))
    before["scorecard_member_ratings"][0]["value_number"] = "not-a-number"
    with pytest.raises(ValueError, match="Scorecard conversion refused"):
        write_family(tmp_path, before, generation_id="bad-rating")
    [stage] = list(tmp_path.glob(".scorecard-etl-*"))
    errors = [r for r in pq.read_table(stage / "etl_receipts.parquet").to_pylist() if r["outcome"] == "error"]
    assert len(errors) == 1
    from spicy_regs.etl_receipts import decode_exact_json
    assert errors[0]["dataset"] == "scorecard_member_ratings"
    assert decode_exact_json(errors[0]["processing_json"])["raw_source"]["value_number"] == "not-a-number"
    assert decode_exact_json(errors[0]["diagnostic_json"])["reason_code"] == "native_conversion_refused"
    assert not (tmp_path / "etl_receipts.parquet").exists()
