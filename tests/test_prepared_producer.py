"""Retained producer bytes remain immutable when current main publishes them."""

import hashlib
import json
import subprocess
from pathlib import Path

import pytest

from spicy_regs import generations
from spicy_regs import native_conversion as conversion
from tests.test_native_conversion import (
    BUCKET, CFR, MAIN, STATE, WHEEL,
    bucket as _bucket,
    convert, publish_old, stored,
)

bucket = _bucket
PUBLISHER = "be" * 20


def _git(root, *arguments):
    return subprocess.run(["git", "-C", str(root), *arguments], check=True, capture_output=True,
                          text=True).stdout.strip()


@pytest.fixture
def history(tmp_path):
    root = tmp_path / "history"
    root.mkdir()
    _git(root, "init")
    _git(root, "config", "user.email", "test@example.invalid")
    _git(root, "config", "user.name", "Test")
    package = root / "src/spicy_regs"
    package.mkdir(parents=True)
    # Path component sorting differs from simple string sorting for a.py versus a/nested.py.
    (package / "a").mkdir()
    (package / "a/nested.py").write_bytes(b"retained = 'producer'\n")
    (package / "a.py").write_bytes(b"value = 1\n")
    (package / "__pycache__").mkdir()
    (package / "__pycache__/ignore.py").write_bytes(b"not source\n")
    (root / "vendor").mkdir()
    wheel = b"retained locked wheel bytes"
    (root / "vendor/spicy_docs.whl").write_bytes(wheel)
    lock = {'package': [{'name': 'spicy-docs', 'version': WHEEL,
                         'source': {'path': 'vendor/spicy_docs.whl'},
                         'wheels': [{'hash': 'sha256:' + hashlib.sha256(wheel).hexdigest()}]}]}
    (root / "uv.lock").write_text(
        f'[[package]]\nname = "spicy-docs"\nversion = "{WHEEL}"\n'
        'source = {path = "vendor/spicy_docs.whl"}\n'
        f'wheels = [{{hash = "{lock["package"][0]["wheels"][0]["hash"]}"}}]\n'
    )
    _git(root, "add", ".")
    _git(root, "commit", "-m", "Retained producer")
    producer = _git(root, "rev-parse", "HEAD")
    identity = "urn:spicy-regs:implementation:sha256:" + generations.source_digest(package)
    (package / "a.py").write_bytes(b"value = 2\n")
    _git(root, "commit", "-am", "Current publisher")
    publisher = _git(root, "rev-parse", "HEAD")
    return root, producer, publisher, identity, lock["package"][0]


def test_historical_identity_uses_exact_git_blobs_and_generation_path_order(history):
    root, producer, publisher, identity, locked = history
    # Neither working files nor archive export transformations may change historical source hashing.
    (root / "src/spicy_regs/a.py").write_bytes(b"different working bytes\n")
    assert conversion._historical_producer(root, producer, publisher) == (identity, locked)


@pytest.mark.parametrize("damage", ["short", "unavailable", "not-ancestor", "locked-runtime", "wheel"])
def test_historical_pin_runtime_and_wheel_refuse(history, damage):
    root, producer, publisher, _, _ = history
    if damage == "short":
        producer = producer[:12]
    elif damage == "unavailable":
        producer = "aa" * 20
    elif damage == "not-ancestor":
        producer, publisher = publisher, producer
    elif damage == "locked-runtime":
        with (root / "uv.lock").open("a") as stream:
            stream.write('\n# changed locked runtime\n')
        _git(root, "commit", "-am", "Changed runtime")
        publisher = _git(root, "rev-parse", "HEAD")
    else:
        (root / "vendor/spicy_docs.whl").write_bytes(b"wrong historical wheel")
        _git(root, "commit", "-am", "Wrong wheel bytes")
        producer = publisher = _git(root, "rev-parse", "HEAD")
    with pytest.raises(conversion.ConversionRefused):
        conversion._historical_producer(root, producer, publisher)


def _historical_fixture(monkeypatch, identity=None):
    calls = []

    def historical(root, producer, publisher):
        calls.append((producer, publisher))
        assert producer == MAIN and publisher == PUBLISHER
        return identity or generations.implementation_id(), {
            "version": WHEEL, "wheels": [{"hash": STATE["spicy_docs_wheel"]["locked_sha256"]}],
        }

    monkeypatch.setattr(conversion, "_historical_producer", historical)
    return calls


def _publish(path, **overrides):
    arguments: dict = dict(allowed=["cfr-sections"], expected_main=PUBLISHER, expected_spicy_docs=WHEEL,
                     expect_bucket=BUCKET, expected_producer=MAIN,
                     state=lambda _: STATE | {"checkout": PUBLISHER, "main": PUBLISHER}) | overrides
    return conversion.publish_prepared(path, **arguments)


def test_distinct_publisher_preserves_producer_seal_generation_and_rollback(tmp_path, monkeypatch, bucket):
    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    work = tmp_path / "work"
    dry = convert("cfr-sections", work)
    generation = Path(dry["generation"]["directory"])
    before = {p.relative_to(generation): p.read_bytes() for p in generation.rglob("*") if p.is_file()}
    calls = _historical_fixture(monkeypatch)

    def no_writer(*args, **kwargs):
        raise AssertionError("Prepared publication must not run a writer")

    monkeypatch.setattr(conversion, "_convert_rollup", no_writer)
    done = _publish(work / conversion.RECEIPT)
    assert calls == [(MAIN, PUBLISHER)]
    assert done["source"] == dry["source"]
    assert done["prepared"] == dry["prepared"]
    assert done["generation"] == dry["generation"]
    assert {p.relative_to(generation): p.read_bytes() for p in generation.rglob("*") if p.is_file()} == before
    publisher = done["publish_attempt"]["publisher"]
    assert publisher == {"source": STATE | {"checkout": PUBLISHER, "main": PUBLISHER},
                         "implementationId": generations.implementation_id()}
    conversion.rollback(work / conversion.RECEIPT, expect_bucket=BUCKET)
    assert stored(bucket) == old


@pytest.mark.parametrize("damage", ["missing-pin", "wrong-pin", "identity", "history", "runtime", "dirty", "moved"])
def test_distinct_producer_refusals_do_not_write_or_modify_the_sealed_receipt(tmp_path, monkeypatch, bucket, damage):
    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    work = tmp_path / "work"
    convert("cfr-sections", work)
    path = work / conversion.RECEIPT
    _historical_fixture(monkeypatch, identity="wrong" if damage == "identity" else None)
    overrides = {}
    if damage in {"missing-pin", "wrong-pin"}:
        overrides["expected_producer"] = None if damage == "missing-pin" else "af" * 20
    elif damage == "history":
        def unavailable(*args):
            raise conversion.ConversionRefused("Unavailable retained history")
        monkeypatch.setattr(conversion, "_historical_producer", unavailable)
    elif damage in {"runtime", "dirty"}:
        state = STATE | {"checkout": PUBLISHER, "main": PUBLISHER}
        if damage == "runtime":
            state["spicy_docs_wheel"] = STATE["spicy_docs_wheel"] | {
                "locked_sha256": "sha256:" + "ca" * 32, "file_sha256": "sha256:" + "ca" * 32,
            }
        else:
            state["uncommitted"] = [" M src/changed.py"]
        overrides["state"] = lambda _: state
    elif damage == "moved":
        states = iter([STATE | {"checkout": PUBLISHER, "main": PUBLISHER}, STATE])
        overrides["state"] = lambda _: next(states)
    before = list(bucket.writes), path.read_bytes()
    with pytest.raises(conversion.ConversionRefused):
        _publish(path, **overrides)
    assert (bucket.writes, path.read_bytes()) == before
    assert stored(bucket) == old
    assert "publish_attempt" not in json.loads(path.read_bytes())
