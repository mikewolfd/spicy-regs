"""A resource executor preserves the original producer and all checkpoint values."""

import importlib.util
import json
from pathlib import Path
from uuid import UUID
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_regs import fec_native_conversion as native
from spicy_regs import fec_observation_segments as segments
from tests.test_fec_observation_segments import (
    fixture_inputs as fixture_inputs,
    bucket_fixture as bucket_fixture,
    PIN,
    build,
)

spec = importlib.util.spec_from_file_location(
    "checkpoint_worker", Path(__file__).parents[1] / "scripts/prepare_fec_checkpoint.py"
)
if spec is None or spec.loader is None:
    raise RuntimeError("Checkpoint worker source unavailable")
worker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(worker)
BASELINE = Path(__file__).parent / "fixtures/fec_checkpoint/fec_observation_segments-871.py.txt"


def mapping(inputs):
    return {m.path: str(inputs[1][key]) for key in inputs[1] for m in native.publication.table_members(inputs[0], key)}


def segment_rows(checkpoint, name):
    rows = []
    for p in sorted((checkpoint / "closed").glob("*.json")):
        entry = json.loads(p.read_text())
        path = checkpoint / entry["directory"] / name
        if path.exists():
            rows.extend(pq.read_table(path).to_pylist())
    return rows


def test_verified_preparation_matches_every_one_pass_value_without_new_alias_or_consolidation(
    tmp_path, inputs, monkeypatch
):
    def fixed():
        return UUID("12345678-1234-1234-1234-123456789abc")

    monkeypatch.setattr(native, "uuid4", fixed)
    monkeypatch.setattr(segments, "uuid4", fixed)
    baseline = tmp_path / "baseline"
    build(tmp_path, inputs, baseline, "one-pass")
    checkpoint = tmp_path / "operation/output/checkpoint"
    import os

    operation = checkpoint.parent.parent
    reference_work = operation / "invocations" / "origin" / "output" / "conversion"
    aliases = reference_work / "retained"
    aliases.mkdir(parents=True)
    for original in inputs[1].values():
        os.link(original, aliases / original.name)
    failed = operation / "invocations" / "failed" / "output" / "partial.bin"
    failed.parent.mkdir(parents=True)
    failed.write_bytes(b"old failed bytes remain charged")
    original_states = {p: p.stat() for p in inputs[1].values()}

    def no_assembly(*args, **kwargs):
        pytest.fail("Resource preparation must not consolidate")

    monkeypatch.setattr(segments, "coalesced_tables", no_assembly)

    def interrupted():
        closed = sorted((checkpoint / "closed").glob("*.json"))
        if any(json.loads(p.read_text())["spec"]["table"] == "fec_source_records" for p in closed):
            raise TimeoutError("fixture boundary")

    with pytest.raises(TimeoutError, match="fixture boundary"):
        worker.prepare_checkpoint(
            checkpoint=checkpoint,
            captured=inputs[0],
            source_pin=PIN,
            retained_members=mapping(inputs),
            original_reference_work=reference_work,
            check_resources=interrupted,
        )
    old = {p: p.read_bytes() for p in checkpoint.rglob("*.json")}
    result = worker.prepare_checkpoint(
        checkpoint=checkpoint,
        captured=inputs[0],
        source_pin=PIN,
        retained_members=mapping(inputs),
        original_reference_work=reference_work,
    )
    assert result["table_consolidations"] == result["original_aliases_created"] == 0
    assert all(p.read_bytes() == raw for p, raw in old.items())
    for name in [
        "fec_collections.parquet",
        "fec_relationships.parquet",
        "fec_source_records.parquet",
        "etl_receipts.parquet",
    ]:
        assert segment_rows(checkpoint, name) == segment_rows(baseline, name)
    for p, state in original_states.items():
        assert (p.stat().st_ino, p.stat().st_nlink, p.stat().st_size) == (state.st_ino, state.st_nlink, state.st_size)
    orphan = checkpoint / "attempts" / ("99999999-" + "b" * 32) / "open.bin"
    orphan.parent.mkdir(parents=True)
    orphan.write_bytes(b"old interrupted attempt remains charged")

    def footprint():
        return sum(max(p.stat().st_size, p.stat().st_blocks * 512) for p in operation.rglob("*") if p.is_file())

    charged_before = footprint()
    files = {p: p.read_bytes() for p in operation.rglob("*") if p.is_file()}
    worker.prepare_checkpoint(
        checkpoint=checkpoint,
        captured=inputs[0],
        source_pin=PIN,
        retained_members=mapping(inputs),
        original_reference_work=reference_work,
    )
    assert {p: p.read_bytes() for p in operation.rglob("*") if p.is_file()} == files
    assert footprint() == charged_before
    assert failed.exists() and orphan.exists()
    # No table assembly files exist outside the closed segment directories.
    assert all("/attempts/" in str(p) for p in checkpoint.rglob("*.parquet"))


@pytest.mark.parametrize("damage", ["missing", "reordered", "corrupted", "range", "input", "identity"])
def test_progress_refuses_damaged_prefix_and_identities(tmp_path, inputs, damage):
    checkpoint = tmp_path / "checkpoint"
    members = mapping(inputs)
    pin = dict(PIN)
    worker.prepare_checkpoint(checkpoint=checkpoint, captured=inputs[0], source_pin=pin, retained_members=members)
    paths = sorted((checkpoint / "closed").glob("*.json"))
    if damage == "missing":
        paths[0].unlink()
    elif damage == "reordered":
        first, second = paths[:2]
        a, b = first.read_bytes(), second.read_bytes()
        first.write_bytes(b)
        second.write_bytes(a)
    elif damage == "corrupted":
        entry = json.loads(paths[0].read_text())
        target = checkpoint / entry["directory"] / "etl_receipts.parquet"
        with target.open("ab") as stream:
            stream.write(b"changed")
    elif damage == "range":
        entry = json.loads(paths[-1].read_text())
        entry["spec"]["attempt_offset"] += 1
        paths[-1].write_text(json.dumps(entry))
    elif damage == "input":
        p = inputs[1]["fec_collections.parquet"]
        rows = pq.read_table(p).to_pylist()
        rows[0]["collection_id"] = "changed"
        pq.write_table(pa.Table.from_pylist(rows, schema=pq.read_schema(p)), p)
    else:
        pin["installed_sdk"] = "changed"
    with pytest.raises(
        ValueError, match="gap|range|bytes changed|input bytes|source, input, policy or version changed"
    ):
        worker.prepare_checkpoint(checkpoint=checkpoint, captured=inputs[0], source_pin=pin, retained_members=members)


def test_exact_resource_refactor_rejects_changed_verification_or_mapping():
    old = BASELINE.read_text()
    new = Path(segments.__file__).read_text()
    segments._check_resource_refactor(old, new)
    for changed in [
        new.replace("self.attempt_offset += count", "self.attempt_offset += count + 1"),
        new.replace("entry['files'][name]", "'different-hash'", 1),
        new.replace("dataset_policy(table)", 'dataset_policy("fec_collections")'),
    ]:
        with pytest.raises(ValueError):
            segments._check_resource_refactor(old, changed)


@pytest.mark.parametrize("damage", [None, "header", "producer", "executor", "policy-file", "kernel", "wheel"])
def test_explicit_original_and_executor_admission_fail_closed(tmp_path, inputs, monkeypatch, damage):
    import types
    from spicy_regs import native_conversion as conversion
    from tests.test_native_conversion import STATE

    pin = {k: v for k, v in STATE.items() if k != "main"}
    checkpoint = tmp_path / "old-producer"
    with segments.observation_segments(
        checkpoint, captured_family=inputs[0]["families"]["fec-observations"], source_pin=pin
    ):
        pass
    original_identity = "urn:spicy-regs:implementation:sha256:" + "b" * 64
    header = checkpoint / "checkpoint.json"
    value = json.loads(header.read_text())
    value["identity"]["implementation"] = original_identity
    header.write_text(json.dumps(value))
    executor_commit = "e1" * 20
    now = {**STATE, "checkout": executor_commit, "main": executor_commit}
    origin = {
        "commit": STATE["checkout"],
        "executor_commit": executor_commit,
        "header_sha256": segments._sha(header),
        "remote": "fixture",
    }
    original_calls = []

    def historical(root, producer, executor):
        original_calls.append((producer, executor))
        return (original_identity if producer == STATE["checkout"] else segments.implementation_id()), {
            "version": STATE["pinned_spicy_docs"],
            "wheels": [{"hash": STATE["spicy_docs_wheel"]["locked_sha256"]}],
        }

    def executor(state, expected, sdk):
        if state["checkout"] != expected:
            raise ValueError("Wrong executor identity")
        return {
            "commit": expected,
            "implementationId": segments.implementation_id(),
            "lockedSpicyDocs": {
                "version": STATE["pinned_spicy_docs"],
                "wheels": [{"hash": STATE["spicy_docs_wheel"]["locked_sha256"]}],
            },
        }

    monkeypatch.setattr(conversion, "source_state", lambda _: now)
    monkeypatch.setattr(conversion, "_checkpoint_producer", executor)
    monkeypatch.setattr(conversion, "_historical_producer", historical)

    def git(command, **kwargs):
        if "merge-base" in command:
            return types.SimpleNamespace(returncode=0, stdout="")
        if "diff" in command:
            return types.SimpleNamespace(
                stdout=(
                    "src/spicy_regs/etl_receipts.py\n"
                    if damage == "policy-file"
                    else "src/spicy_regs/fec_observation_segments.py\n"
                ).encode()
            )
        old = BASELINE.read_text()
        if damage == "kernel":
            old = old.replace("self.attempt_offset += count", "self.attempt_offset += count + 1")
        return types.SimpleNamespace(stdout=old.encode())

    monkeypatch.setattr("subprocess.run", git)
    if damage == "header":
        header.write_text(header.read_text() + " ")
    elif damage == "producer":
        origin["commit"] = "f1" * 20
    elif damage == "executor":
        origin["executor_commit"] = "f2" * 20
    elif damage == "wheel":
        now = {**now, "spicy_docs_wheel": {"changed": True}}
    before = header.read_bytes()
    if damage:
        with pytest.raises(ValueError):
            segments.verify_checkpoint_origin(checkpoint, inputs[0]["families"]["fec-observations"], pin, origin)
    else:
        proof = segments.verify_checkpoint_origin(checkpoint, inputs[0]["families"]["fec-observations"], pin, origin)
        assert proof["originalProducer"]["implementationId"] == original_identity
        assert proof["executor"]["implementationId"] != original_identity
        assert original_calls == [(origin["commit"], executor_commit)]
        worker.prepare_checkpoint(
            checkpoint=checkpoint, captured=inputs[0], source_pin=pin, retained_members=mapping(inputs), origin=origin
        )
        closed_before = {p: p.read_bytes() for p in (checkpoint / "closed").glob("*.json")}
        with segments.checkpoint_executor(origin):
            assembled = conversion.convert(
                "fec-observations",
                allowed=["fec-observations"],
                work=tmp_path / "final-maintained",
                expected_main=executor_commit,
                expected_spicy_docs=STATE["spicy_docs"],
                remote="fixture",
                state=lambda _: dict(now),
                observation_checkpoint=checkpoint,
                retained_members=mapping(inputs),
            )
        assert assembled["prepared"] and assembled["published"] is None
        assert assembled["producer"]["implementationId"] == proof["executor"]["implementationId"]
        assert assembled["producer"]["commit"] == executor_commit
        assert all(p.read_bytes() == raw for p, raw in closed_before.items())
        # The maintained final path may supply the actual executor source pin;
        # the exact admitted original header remains unchanged in this scope.
        with segments.checkpoint_executor(origin):
            with segments.observation_segments(
                checkpoint,
                captured_family=inputs[0]["families"]["fec-observations"],
                source_pin={k: v for k, v in now.items() if k != "main"},
            ):
                pass
        with pytest.raises(ValueError, match="source, input, policy or version changed"):
            with segments.observation_segments(
                checkpoint, captured_family=inputs[0]["families"]["fec-observations"], source_pin=pin
            ):
                pytest.fail("Original implementation accepted outside the explicit executor scope")
    assert header.read_bytes() == before


def test_original_reference_work_cannot_escape_cumulative_root_through_dotdot(tmp_path, inputs):
    import os

    operation = tmp_path / "operation"
    checkpoint = operation / "output" / "checkpoint"
    worker.prepare_checkpoint(
        checkpoint=checkpoint, captured=inputs[0], source_pin=PIN, retained_members=mapping(inputs)
    )
    (operation / "invocations").mkdir()
    outside = tmp_path / "outside"
    (outside / "retained").mkdir(parents=True)
    for original in inputs[1].values():
        os.link(original, outside / "retained" / original.name)
    escaping = operation / "invocations" / ".." / ".." / "outside"
    with pytest.raises(ValueError, match="stay inside this cumulative operation"):
        worker.operation_original_references(
            checkpoint, inputs[0]["families"]["fec-observations"], mapping(inputs), escaping, create=True
        )
    assert not (operation / "output" / "original-references.json").exists()
