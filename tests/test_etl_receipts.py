"""Receipt joins fail closed while domain values and exact evidence survive."""

from dataclasses import replace
from decimal import Decimal
import json

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import (
    DatasetPolicy,
    ReceiptContext,
    RECEIPT_KEY,
    RECEIPT_SCHEMA,
    split_record,
    failure_receipt,
    write_dataset,
    read_with_receipts,
    validate_receipt_bundle,
    combine_receipts,
    subject_identity,
    exact_json,
    read_receipt_bundle,
)
from spicy_regs.generations import build_generation, verify_generation
from spicy_regs.sources.publication import empty_index, publish_generation, receipt_members, PublicationError


@pytest.fixture
def policy():
    return DatasetPolicy(
        "payments",
        pa.schema(
            [
                ("id", pa.string()),
                ("body_id", pa.string()),
                ("status", pa.string()),
                ("amount", pa.decimal128(20, 2)),
                ("cycles", pa.list_(pa.int32())),
                ("people", pa.list_(pa.struct([("name", pa.string()), ("roles", pa.list_(pa.string()))]))),
            ]
        ),
        ("id", "body_id"),
        ("parser_status", "source_url", "amount_raw"),
    )


@pytest.fixture
def context():
    witness = {
        "source_id": "retained-response",
        "source_uri": "https://example.test/source",
        "sha256": "a" * 64,
        "locator": "row:2",
        "body_version": None,
    }
    return ReceiptContext("build-1", "attempt-1", "fixture-parser:1", [witness, witness])


@pytest.fixture
def row():
    return {
        "id": "1",
        "body_id": "v1",
        "status": "withdrawn",
        "amount": Decimal("9999999999999999.99"),
        "cycles": [2024, None, 2024],
        "people": [{"name": "A", "roles": ["member", None]}, None],
        "parser_status": "parsed",
        "source_url": "https://example.test/source",
        "amount_raw": "$9,999,999,999,999,999.99",
    }


def test_native_roundtrip_and_internal_read(tmp_path, policy, context, row):
    subject, receipt = write_dataset([(row, context)], tmp_path / "bundle", policy)
    assert subject is not None
    held = pq.read_table(subject).to_pylist()[0]
    assert set(held) == set(policy.subject_schema.names)
    assert held["amount"] == row["amount"]
    assert held["cycles"] == row["cycles"]
    assert held["people"] == row["people"]
    assert held["status"] == "withdrawn"
    receipt_row = pq.read_table(receipt).to_pylist()[0]
    assert receipt_row["witnesses"] == list(context.witnesses)
    assert list(read_with_receipts([subject], [receipt], policy, generation_id="build-1")) == [row]


def test_complete_bundle_read_preserves_exact_values_and_explicit_attempts(tmp_path, policy, context, row):
    reads = DatasetPolicy("parser_reads", pa.schema([]), (), ("checkpoint",), receipt_only=True)
    failed = failure_receipt(policy, replace(context, attempt_id="refused"), outcome="refused", raw_fields=row)
    subject, receipts = write_dataset([(row, context)], tmp_path / "payments", policy, failures=[failed])
    _, observed = write_dataset([({"checkpoint": [None, "", 0]}, context)], tmp_path / "reads", reads)
    shared = combine_receipts([receipts, observed], tmp_path / "shared.parquet")
    subjects, policies = {policy.dataset: [subject], reads.dataset: []}, [policy, reads]
    assert read_receipt_bundle(subjects, [shared], policies, generation_id="build-1") == {
        "payments": [row], "parser_reads": []
    }
    result = read_receipt_bundle(
        subjects, [shared], policies, generation_id="build-1",
        processing_outcomes={"payments": frozenset({"refused"}), "parser_reads": frozenset({"observed"})},
    )
    assert result == {"payments": [row, row], "parser_reads": [{"checkpoint": [None, "", 0]}]}


@pytest.mark.parametrize("fault", ["late-subject", "orphan", "duplicate", "generation", "receipt", "schema"])
def test_complete_bundle_read_refuses_inconsistent_later_dataset(tmp_path, policy, context, row, fault):
    other = replace(policy, dataset="refunds")
    first, first_receipt = write_dataset([(row, context)], tmp_path / "payments", policy)
    second, second_receipt = write_dataset([(row, context)], tmp_path / "refunds", other)
    shared = combine_receipts([first_receipt, second_receipt], tmp_path / "shared.parquet")
    subjects = {"payments": [first], "refunds": [second]}
    selected_generation = "build-1"
    if fault == "late-subject":
        changed = split_record(other, row | {"amount": Decimal("1.00")}, context)[0]
        pq.write_table(pa.Table.from_pylist([changed], schema=other.subject_schema), second)
    elif fault == "orphan":
        subjects["refunds"] = []
    elif fault in {"duplicate", "receipt"}:
        receipts = pq.read_table(shared).to_pylist()
        if fault == "duplicate":
            receipts.append(receipts[-1])
        else:
            receipts[-1]["processor"] = "altered"
        pq.write_table(pa.Table.from_pylist(receipts, schema=RECEIPT_SCHEMA), shared)
    elif fault == "generation":
        selected_generation = "other-generation"
    else:
        pq.write_table(pq.read_table(second).append_column("extra", pa.array(["x"])), second)
    with pytest.raises(ValueError):
        read_receipt_bundle(subjects, [shared], [policy, other], generation_id=selected_generation)


@pytest.mark.parametrize("outcomes", [{"missing": frozenset({"observed"})}, {"payments": frozenset({"accepted"})}, {"payments": frozenset({"unknown"})}])
def test_complete_bundle_read_requires_explicit_nonaccepted_outcome_selection(tmp_path, policy, context, row, outcomes):
    subject, receipt = write_dataset([(row, context)], tmp_path / "payments", policy)
    with pytest.raises(ValueError, match="nonaccepted"):
        read_receipt_bundle({"payments": [subject]}, [receipt], [policy], generation_id="build-1", processing_outcomes=outcomes)


def test_complete_bundle_read_checks_unselected_failed_receipts(tmp_path, policy, context, row):
    failed = failure_receipt(policy, replace(context, attempt_id="refused"), outcome="refused", raw_fields=row)
    subject, receipt = write_dataset([(row, context)], tmp_path / "payments", policy, failures=[failed])
    receipts = pq.read_table(receipt).to_pylist()
    receipts[-1]["processor"] = "altered"
    pq.write_table(pa.Table.from_pylist(receipts, schema=RECEIPT_SCHEMA), receipt)
    with pytest.raises(ValueError, match="digest"):
        read_receipt_bundle({"payments": [subject]}, [receipt], [policy], generation_id="build-1")


@pytest.mark.parametrize(
    "change",
    [
        {"unclassified": 1},
        {"people": [{"name": "A", "extra": 3}]},
        {"cycles": "[2024]"},
        {"cycles": [True]},
        {"amount": 0.1},
    ],
)
def test_no_silent_loss_or_coercion(policy, context, row, change):
    with pytest.raises(ValueError):
        split_record(policy, row | change, context)


def test_null_empty_and_order_are_distinct(policy, context, row):
    identities = [
        split_record(policy, row | {"cycles": values}, context)[1]["subject_version"]
        for values in (None, [], [None], [2024, 2026], [2026, 2024], [2024, 2024])
    ]
    assert len(set(identities)) == len(identities)


def test_decimal_raw_receipt_encoding_does_not_pass_through_float():
    assert exact_json(Decimal("9999999999999999.99")) == '["decimal","9999999999999999.99"]'
    assert exact_json({"__type__": "decimal", "value": "1.00"}) != exact_json(Decimal("1.00"))


def test_failed_attempts_have_no_subject(tmp_path, policy, context, row):
    failed = failure_receipt(
        policy, replace(context, attempt_id="bad"), outcome="refused", raw_fields=row | {"amount": "not money"}
    )
    subject, receipts = write_dataset([(row, context)], tmp_path / "bundle", policy, failures=[failed])
    assert subject is not None
    assert pq.read_table(subject).num_rows == 1
    assert pq.read_table(receipts).num_rows == 2
    assert failed["record_id"] is None and failed["subject_version"] is None


def test_processing_only(tmp_path, context):
    policy = DatasetPolicy("parser_reads", pa.schema([]), (), ("body_sha", "error"), receipt_only=True)
    subject, receipts = write_dataset([({"body_sha": "x", "error": None}, context)], tmp_path / "bundle", policy)
    assert subject is None
    assert pq.read_table(receipts).to_pylist()[0]["outcome"] == "observed"


def test_duplicate_identity_different_attempts_refused_atomically(tmp_path, policy, context, row):
    with pytest.raises(ValueError, match="ambiguous"):
        write_dataset([(row, context), (row, replace(context, attempt_id="attempt-2"))], tmp_path / "bundle", policy)
    assert not (tmp_path / "bundle").exists()


def test_late_failure_leaves_no_partial_bundle(tmp_path, policy, context, row):
    def rows():
        yield row, context
        raise RuntimeError("later source refused")

    with pytest.raises(RuntimeError):
        write_dataset(rows(), tmp_path / "bundle", policy, batch_size=1)
    assert not (tmp_path / "bundle").exists()


def test_generation_and_content_join(tmp_path, policy, context, row):
    subject, receipts = write_dataset([(row, context)], tmp_path / "bundle", policy)
    assert subject is not None
    with pytest.raises(ValueError, match="selected generation"):
        list(read_with_receipts([subject], [receipts], policy, generation_id="other"))
    pq.write_table(
        pa.Table.from_pylist(
            [split_record(policy, row | {"amount": Decimal("1.00")}, context)[0]], schema=policy.subject_schema
        ),
        subject,
    )
    with pytest.raises(ValueError, match="Missing"):
        validate_receipt_bundle({"payments": [subject]}, [receipts], [policy], generation_id="build-1")


def test_versioned_body_keys_do_not_collapse(policy, row):
    assert subject_identity(policy, row)[0] != subject_identity(policy, row | {"body_id": "v2"})[0]


def test_direct_writer_extra_subject_column_refuses(tmp_path, policy, context, row):
    subject, receipt = write_dataset([(row, context)], tmp_path / "bundle", policy)
    assert subject is not None
    table = pq.read_table(subject).append_column("parser_status", pa.array(["ok"]))
    pq.write_table(table, subject)
    with pytest.raises(ValueError, match="schema"):
        validate_receipt_bundle({"payments": [subject]}, [receipt], [policy])


def test_polars_native_input(tmp_path, policy, context, row):
    frame = pl.from_arrow(pa.Table.from_pylist([split_record(policy, row, context)[0]], schema=policy.subject_schema))
    assert isinstance(frame, pl.DataFrame)
    records = ((r | {k: row[k] for k in policy.receipt_fields}, context) for r in frame.iter_rows(named=True))
    subject, _ = write_dataset(records, tmp_path / "polars", policy)
    assert subject is not None
    assert pq.read_table(subject).to_pylist()[0]["amount"] == row["amount"]


def test_generation_admission_and_receipt_tampering(tmp_path, policy, context, row):
    subject, receipt = write_dataset([(row, context)], tmp_path / "bundle", policy)
    assert subject is not None
    artifact = build_generation(
        tmp_path / "generation",
        family="payments",
        files=[subject],
        expected_keys=[subject.name],
        receipt_path=receipt,
        receipt_policies=[policy],
        receipt_generation_id="build-1",
    )
    assert artifact.root["spec"]["etlReceipts"]["generationId"] == "build-1"
    assert set(artifact.root["spec"]["tables"]) == {"payments.parquet"}
    verify_generation(tmp_path / "generation", expected_pin=artifact.pin)
    pq.write_table(RECEIPT_SCHEMA.empty_table(), tmp_path / "generation" / RECEIPT_KEY)
    with pytest.raises(Exception):
        verify_generation(tmp_path / "generation")


def test_generation_rejects_unclassified_output(tmp_path, policy, context, row):
    subject, receipt = write_dataset([(row, context)], tmp_path / "bundle", policy)
    assert subject is not None
    other = tmp_path / "other.parquet"
    pq.write_table(pa.table({"id": ["1"]}), other)
    with pytest.raises(ValueError, match="classify every subject"):
        build_generation(
            tmp_path / "generation",
            family="payments",
            files=[subject, other],
            expected_keys=[subject.name, other.name],
            receipt_path=receipt,
            receipt_policies=[policy],
            receipt_generation_id="build-1",
        )


def test_publication_shared_receipt_members_do_not_collide(tmp_path, policy, context, row):
    from tests.generation_fakes import Store

    client = Store()
    index = empty_index()
    for dataset in ("payments", "other_payments"):
        p = replace(policy, dataset=dataset)
        subject, receipt = write_dataset([(row, context)], tmp_path / dataset, p)
        assert subject is not None
        generation = tmp_path / (dataset + "-generation")
        build_generation(
            generation,
            family=dataset,
            files=[subject],
            expected_keys=[subject.name],
            receipt_path=receipt,
            receipt_policies=[p],
            receipt_generation_id="build-1",
        )
        index = publish_generation(generation, client=client, bucket="b", prior_index=index)
    assert len(receipt_members(index)) == 2
    assert len(receipt_members(index, dataset="payments")) == 1
    with pytest.raises(PublicationError):
        receipt_members(index, dataset="unknown")


def test_no_witness_or_unpinned_witness_refuses(context):
    with pytest.raises(ValueError):
        replace(context, witnesses=[])
    with pytest.raises(ValueError):
        replace(context, witnesses=[{"source_id": "url-only", "source_uri": "https://example.test"}])


def test_policy_descriptor_exact_native_roundtrip(policy):
    restored = DatasetPolicy.from_descriptor(json.loads(json.dumps(policy.descriptor())))
    assert restored == policy


def test_empty_subject_and_failures_are_valid(tmp_path, policy, context, row):
    failure = failure_receipt(policy, context, outcome="error", raw_fields=row)
    subject, receipt = write_dataset([], tmp_path / "empty", policy, failures=[failure])
    assert subject is not None
    assert pq.read_table(subject).num_rows == 0
    assert pq.read_table(receipt).num_rows == 1


def test_missing_and_orphan_receipts(tmp_path, policy, context, row):
    subject, receipt = write_dataset([(row, context)], tmp_path / "bundle", policy)
    assert subject is not None
    empty = tmp_path / "empty.parquet"
    pq.write_table(RECEIPT_SCHEMA.empty_table(), empty)
    with pytest.raises(ValueError, match="Missing"):
        validate_receipt_bundle({"payments": [subject]}, [empty], [policy])
    pq.write_table(policy.subject_schema.empty_table(), subject)
    with pytest.raises(ValueError, match="no matching subject"):
        validate_receipt_bundle({"payments": [subject]}, [receipt], [policy])


def test_duplicate_receipts_survive_combine_and_refuse(tmp_path, policy, context, row):
    subject, receipt = write_dataset([(row, context)], tmp_path / "bundle", policy)
    assert subject is not None
    combined = combine_receipts([receipt, receipt], tmp_path / "combined.parquet")
    with pytest.raises(ValueError, match="Duplicate"):
        validate_receipt_bundle({"payments": [subject]}, [combined], [policy])


def test_remote_receipt_admission_has_no_local_subject_copy(tmp_path, policy, context, row):
    from hashlib import sha256
    from spicy_regs.native_types import described_schema
    from spicy_regs.remote_generations import prepare_remote_generation, publish_remote_generation
    from spicy_regs.sources.remote_parquet import StoredParquet
    from tests.test_remote_generations import RemoteStore

    subject, receipts = write_dataset([(row, context)], tmp_path / "bundle", policy)
    assert subject is not None
    store, members = RemoteStore(), []
    for path in (subject, receipts):
        raw, key = path.read_bytes(), "stage/one/" + path.name
        store.objects[key] = raw
        members.append(
            StoredParquet(
                key, len(raw), "sha256:" + sha256(raw).hexdigest(), store.get_object(Bucket="b", Key=key)["ETag"], 1
            )
        )
    directory = tmp_path / "remote"
    artifact = prepare_remote_generation(
        directory,
        family="payments",
        client=store,
        bucket="b",
        staging_prefix="stage/one",
        members=members,
        expected_keys=[subject.name, receipts.name],
        schemas={"payments": described_schema(policy.subject_schema), "etl_receipts": described_schema(RECEIPT_SCHEMA)},
        receipt_policies=[policy],
        receipt_generation_id="build-1",
    )
    assert not list(directory.glob("*.parquet"))
    index = publish_remote_generation(
        directory, client=store, bucket="b", staging_prefix="stage/one", members=members, prior_index=empty_index()
    )
    assert receipt_members(index, dataset="payments")[0].sha256 == members[1].sha256
    assert index["families"]["payments"]["artifactDigest"] == artifact.pin.artifact_digest


def test_carry_rebind_preserves_original_evidence(tmp_path, policy, context, row):
    from spicy_regs.etl_receipts import rebind_receipt

    subject, receipts = write_dataset([(row, context)], tmp_path / "original", policy)
    assert subject is not None
    original = pq.read_table(receipts).to_pylist()[0]
    carried = rebind_receipt(original, generation_id="build-2")
    for name in ("witnesses", "processor", "attempt_id", "record_id", "subject_version", "processing_json"):
        assert carried[name] == original[name]
    assert carried["receipt_id"] != original["receipt_id"]
    pq.write_table(pa.Table.from_pylist([carried], schema=RECEIPT_SCHEMA), receipts)
    assert list(read_with_receipts([subject], [receipts], policy, generation_id="build-2")) == [row]


def test_registered_dataset_cannot_bypass_receipts(tmp_path, policy, context, row, monkeypatch):
    from spicy_regs import etl_policy_registry

    monkeypatch.setattr(etl_policy_registry, "installed_policies", lambda: {policy.dataset: policy})
    subject, _ = write_dataset([(row, context)], tmp_path / "bundle", policy)
    assert subject is not None
    with pytest.raises(ValueError, match="require ETL receipts"):
        build_generation(tmp_path / "invalid", family="payments", files=[subject], expected_keys=[subject.name])


def test_receipt_only_generation(tmp_path, context):
    from tests.generation_fakes import Store

    policy = DatasetPolicy("failed_fetches", pa.schema([]), (), ("reason",), receipt_only=True)
    _, receipt = write_dataset([({"reason": "no response"}, context)], tmp_path / "bundle", policy)
    directory = tmp_path / "generation"
    build_generation(
        directory,
        family="reads",
        files=[],
        expected_keys=[],
        receipt_path=receipt,
        receipt_policies=[policy],
        receipt_generation_id="build-1",
    )
    index = publish_generation(directory, client=Store(), bucket="b", prior_index=empty_index())
    assert index["families"]["reads"]["tables"] == {}
    assert len(receipt_members(index, dataset="failed_fetches")) == 1


def test_successful_empty_read_checkpoint_is_not_a_subject_or_failure(tmp_path, policy, context):
    from spicy_regs.etl_receipts import observation_receipt, read_attempts

    receipt = observation_receipt(policy, context, processing_fields={"parser_status": "requested-empty"})
    subject, receipts = write_dataset([], tmp_path / "bundle", policy, failures=[receipt])
    assert subject is not None
    assert pq.read_table(subject).num_rows == 0
    held = list(read_attempts([receipts], policy, generation_id="build-1"))
    assert held[0]["outcome"] == "observed"
    assert held[0]["processing_fields"] == {"parser_status": "requested-empty"}
    assert held[0]["record_id"] is None


def test_processing_table_moves_only_by_explicit_receipt_policy(tmp_path, context):
    from tests.generation_fakes import Store

    store = Store()
    legacy = tmp_path / "reads.parquet"
    pq.write_table(pa.table({"reason": ["requested-empty"]}), legacy)
    build_generation(tmp_path / "old", family="reads", files=[legacy], expected_keys=[legacy.name])
    index = publish_generation(tmp_path / "old", client=store, bucket="b", prior_index=empty_index())
    policy = DatasetPolicy("reads", pa.schema([]), (), ("reason",), receipt_only=True)
    _, receipt = write_dataset([({"reason": "requested-empty"}, context)], tmp_path / "split", policy)
    build_generation(
        tmp_path / "new",
        family="reads",
        files=[],
        expected_keys=[],
        receipt_path=receipt,
        receipt_policies=[policy],
        receipt_generation_id="build-1",
    )
    with pytest.raises(PublicationError, match="membership changed"):
        publish_generation(tmp_path / "new", client=store, bucket="b", prior_index=index)
    updated = publish_generation(
        tmp_path / "new", client=store, bucket="b", prior_index=index, receipt_only_tables=frozenset({"reads.parquet"})
    )
    assert not updated["families"]["reads"]["tables"]
    assert receipt_members(updated, dataset="reads")
    assert any(key.endswith("/reads.parquet") for key in store.objects)


def test_receipt_only_checkpoints_preserve_order_attempts_and_refusals(tmp_path, context):
    from spicy_regs.etl_receipts import read_attempts

    policy = DatasetPolicy("resume_checks", pa.schema([]), (), ("cursor",), receipt_only=True)
    records = [({"cursor": "same"}, replace(context, attempt_id=f"attempt-{i}")) for i in range(2)]
    failed = failure_receipt(
        policy, replace(context, attempt_id="failed"), outcome="error", raw_fields={"cursor": "unavailable"}
    )
    _, receipt = write_dataset(records, tmp_path / "bundle", policy, failures=[failed])
    attempts = list(read_attempts([receipt], policy, generation_id="build-1"))
    assert [r["attempt_id"] for r in attempts] == ["attempt-0", "attempt-1", "failed"]
    checkpoints = list(read_attempts([receipt], policy, generation_id="build-1", outcomes=frozenset({"observed"})))
    assert [r["processing_fields"] for r in checkpoints] == [{"cursor": "same"}, {"cursor": "same"}]
    with pytest.raises(ValueError, match="generation"):
        list(read_attempts([receipt], policy, generation_id="another-build"))


def test_installed_receipt_only_policy_cannot_be_replaced_or_duplicated(monkeypatch):
    from spicy_regs import etl_policy_registry

    policy = DatasetPolicy("resume_checks", pa.schema([]), (), ("cursor",), receipt_only=True)
    monkeypatch.setattr(etl_policy_registry, "installed_policies", lambda: {policy.dataset: policy})
    etl_policy_registry.require_registered_receipts({}, {"policies": [policy.descriptor()]})
    wrong = replace(policy, policy_version="other").descriptor()
    with pytest.raises(ValueError, match="installed field policy"):
        etl_policy_registry.require_registered_receipts({}, {"policies": [wrong]})
    with pytest.raises(ValueError, match="duplicate datasets"):
        etl_policy_registry.require_registered_receipts({}, {"policies": [policy.descriptor()] * 2})


def test_receipt_dataset_has_one_family_owner_and_cannot_disappear(tmp_path, context):
    from tests.generation_fakes import Store
    from spicy_regs.sources.publication import parse_index

    store = Store()
    policy = DatasetPolicy("resume_checks", pa.schema([]), (), ("cursor",), receipt_only=True)
    _, receipt = write_dataset([({"cursor": "one"}, context)], tmp_path / "bundle", policy)

    def generation(name, family, policies, receipt_path):
        path = tmp_path / name
        build_generation(
            path,
            family=family,
            files=[],
            expected_keys=[],
            receipt_path=receipt_path,
            receipt_policies=policies,
            receipt_generation_id="build-1",
        )
        return path

    first = generation("first", "checks", [policy], receipt)
    index = publish_generation(first, client=store, bucket="b", prior_index=empty_index())
    second = generation("second", "other", [policy], receipt)
    with pytest.raises(PublicationError, match="already belongs"):
        publish_generation(second, client=store, bucket="b", prior_index=index)
    cloned = json.loads(json.dumps(index))
    other = cloned["families"]["other"] = json.loads(json.dumps(cloned["families"]["checks"]))
    other["prefix"] = other["prefix"].replace("/checks/", "/other/")
    with pytest.raises(PublicationError, match="Invalid publication index"):
        parse_index(json.dumps(cloned).encode())
    other_policy = replace(policy, dataset="other_checks")
    _, other_receipt = write_dataset([({"cursor": "two"}, context)], tmp_path / "other-bundle", other_policy)
    replacement = generation("replacement", "checks", [other_policy], other_receipt)
    with pytest.raises(PublicationError, match="drops receipt dataset ownership"):
        publish_generation(replacement, client=store, bucket="b", prior_index=index)


def test_receipt_only_rollup_has_no_subject_output(tmp_path, monkeypatch, context):
    from spicy_regs.pipelines.rollups.base import RollupPipeline

    policy = DatasetPolicy("resume_checks", pa.schema([]), (), ("cursor",), receipt_only=True)

    class Checks(RollupPipeline):
        name = "checks"
        output = "resume_checks.parquet"
        receipt_only_tables = ("resume_checks.parquet",)
        receipt_policies = (policy,)

        def build(self, output_dir):
            build_context = replace(context, generation_id=self.receipt_generation_id)
            _, receipt = write_dataset([({"cursor": "complete"}, build_context)], output_dir / "checks", policy)
            combine_receipts([receipt], output_dir / RECEIPT_KEY)
            return ()

        def generation_schemas(self):
            return {}

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    Checks(output_dir=tmp_path, skip_upload=True).run()
    (directory,) = (tmp_path / "generations").iterdir()
    artifact = verify_generation(directory)
    assert artifact.root["spec"]["tables"] == {}
    assert artifact.root["spec"]["etlReceipts"]["policies"] == [policy.descriptor()]


def test_receipt_member_descriptor_count_must_match_bytes(tmp_path, policy, context, row, monkeypatch):
    import rulespec_artifacts

    subject, receipt = write_dataset([(row, context)], tmp_path / "bundle", policy)
    assert subject is not None
    directory = tmp_path / "generation"
    build_generation(
        directory,
        family="payments",
        files=[subject],
        expected_keys=[subject.name],
        receipt_path=receipt,
        receipt_policies=[policy],
        receipt_generation_id="build-1",
    )
    original = rulespec_artifacts.iter_member_descriptors

    def altered(*args):
        for member in original(*args):
            yield replace(member, record_count=99) if member.object_key == RECEIPT_KEY else member

    monkeypatch.setattr(rulespec_artifacts, "iter_member_descriptors", altered)
    with pytest.raises(ValueError, match="receipt member count"):
        verify_generation(directory)


@pytest.mark.parametrize("mutation", ["policy", "generation", "missing_receipt"])
def test_materialized_publication_checks_declared_receipt_policy_before_upload(
    tmp_path, policy, context, row, monkeypatch, mutation
):
    from spicy_regs.pipelines.materialized import MaterializedDatasetPipeline
    from spicy_regs.ontology.common import RunContext

    class Example(MaterializedDatasetPipeline):
        name = "receipt-example"
        dataset_name = "receipt-example"
        published_outputs = ("payments.parquet",)
        receipt_policies = (policy,)

        def stages(self):
            return ()

    output = tmp_path / "bundle"
    write_dataset([(row, context)], output, policy)
    runner = Example(output_dir=output)
    manifest, pointer, files = runner._write_publication_files(
        output, context=RunContext("build-1", "2026-10-03T00:00:00Z"), stages=(), input_snapshot={}
    )
    data = json.loads(manifest.read_text())
    if mutation == "policy":
        data["etlReceipts"]["policies"][0]["policy_version"] = "unknown-policy"
    elif mutation == "generation":
        data["etlReceipts"]["generationId"] = "wrong-generation"
    else:
        del files["etl_receipts.parquet"]
    manifest.write_text(json.dumps(data))
    monkeypatch.setattr("spicy_regs.sources.r2.upload_file", lambda *a, **kw: pytest.fail("uploaded before admission"))
    with pytest.raises(ValueError, match="receipt policy|membership"):
        runner._publish(manifest_path=manifest, pointer_path=pointer, artifact_paths=files)


def test_retained_receipt_lineage_resolves_original_values_after_repeated_updates(policy, row, context):
    from spicy_regs.etl_receipts import inherit_receipt, resolve_receipt_witness, retire_receipt, _unpack
    import hashlib

    original = dict(row)
    original["amount_raw"] = "00012.30"
    raw_witness = {
        "source_id": "amount-literal",
        "source_uri": None,
        "sha256": hashlib.sha256(exact_json(original["amount_raw"]).encode()).hexdigest(),
        "locator": "receipt.values.amount_raw (canonical exact_json)",
        "body_version": "build-1",
    }
    first_context = replace(context, witnesses=[raw_witness, raw_witness])
    _, first = split_record(policy, original, first_context)
    second_context = inherit_receipt(replace(context, generation_id="build-2"), first)
    _, second = split_record(policy, row, second_context)
    _, third = split_record(policy, row, inherit_receipt(replace(context, generation_id="build-3"), second))
    assert third["witnesses"][:2] == first["witnesses"]
    assert resolve_receipt_witness(third, raw_witness) == exact_json("00012.30").encode()
    prior_ref = next(w for w in third["witnesses"] if w["body_version"] == first["receipt_id"])
    assert _unpack(json.loads(resolve_receipt_witness(third, prior_ref))) == _unpack(
        json.loads(first["processing_json"])
    )
    identities = _unpack(json.loads(third["diagnostic_json"]))["prior_receipts"]
    assert first["receipt_id"] in {r["receipt_id"] for r in identities}
    retired = retire_receipt(third, generation_id="build-4", reason="explicit deletion")
    assert retired["outcome"] == "observed" and retired["subject_version"] is None
    assert resolve_receipt_witness(retired, raw_witness) == exact_json("00012.30").encode()
    with pytest.raises(ValueError, match="digest differs"):
        inherit_receipt(context, dict(first, processing_json=exact_json({})))


def test_daily_receipt_carries_have_linear_size_and_resolvable_source_values(policy, row, context):
    from spicy_regs.etl_receipts import inherit_receipt, resolve_receipt_witness, _unpack

    original = dict(row, amount_raw="0" * 8000 + "12.30")
    _, current = split_record(policy, original, context)
    first = current
    sizes = []
    for number in range(25):
        _, current = split_record(
            policy,
            original,
            inherit_receipt(replace(context, generation_id=f"day-{number}", attempt_id=f"day-{number}"), current),
        )
        sizes.append(len(exact_json(current).encode()))
    diagnostics = _unpack(json.loads(current["diagnostic_json"]))
    assert len(diagnostics["retained_processing"]) == 1
    assert len(diagnostics["prior_receipts"]) == 25
    assert sizes[24] - sizes[14] < 1.1 * (sizes[14] - sizes[4])
    witness = next(w for w in current["witnesses"] if w["body_version"] == first["receipt_id"])
    assert resolve_receipt_witness(current, witness) == first["processing_json"].encode()


def test_update_and_rebind_preserve_prior_processing_context(policy, row, context):
    from spicy_regs.etl_receipts import inherit_receipt, rebind_receipt, _unpack

    evidence = {"coverage": "partial", "refusal_kind": "source-declared", "counts": {"seen": 2}}
    _, first = split_record(policy, row, replace(context, diagnostics=evidence))
    _, second = split_record(policy, row, inherit_receipt(replace(context, generation_id="update"), first))
    [prior] = _unpack(json.loads(second["diagnostic_json"]))["prior_receipts"]
    assert prior["diagnostics"] == evidence
    assert (prior["processor"], prior["attempt_id"], prior["outcome"]) == (
        context.processor,
        context.attempt_id,
        "accepted",
    )
    carried = rebind_receipt(first, generation_id="carry")
    diagnostics = _unpack(json.loads(carried["diagnostic_json"]))
    assert {key: diagnostics[key] for key in evidence} == evidence
    assert diagnostics["prior_receipts"][0]["diagnostics"] == evidence
