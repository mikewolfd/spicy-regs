"""Tests for the incremental-processing Manifest (spicy_regs.manifest.Manifest).

Pins additive key recording (``new_keys`` is a copy), the save/load round-trip,
the refusal to start empty unless a fresh start is allowed, and that saving an
empty manifest writes nothing.
"""

from pathlib import Path

import pytest

from spicy_regs import manifest as manifest_module
from spicy_regs.manifest import Manifest, MissingManifestError


def test_empty_manifest_contains_nothing() -> None:
    manifest = Manifest.empty()
    assert "any-key" not in manifest
    assert manifest.new_keys == set()


def test_record_is_additive_and_returns_a_copy() -> None:
    manifest = Manifest.empty()
    manifest.record(["a", "b"])
    manifest.record(["b", "c"])
    assert manifest.new_keys == {"a", "b", "c"}

    # new_keys returns a copy — mutating it must not affect the manifest.
    snapshot = manifest.new_keys
    snapshot.add("zzz")
    assert "zzz" not in manifest.new_keys


def test_save_then_load_roundtrips_keys(tmp_output: Path) -> None:
    manifest = Manifest.empty()
    keys = {"raw-data/EPA/a.json", "raw-data/EPA/b.json"}
    manifest.record(keys)
    manifest.save(tmp_output)
    assert (tmp_output / "manifest.parquet").exists()

    reloaded = Manifest.load(tmp_output)
    for key in keys:
        assert key in reloaded
    assert "raw-data/EPA/never.json" not in reloaded


def test_load_refuses_when_r2_has_no_manifest(tmp_output: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # The fork's failure: R2 answered 404 and every batch silently re-read its
    # agencies' whole history. A missing manifest is an error, not a fresh start.
    asked: list[str] = []
    monkeypatch.setattr(manifest_module, "download_from_r2", lambda key, path: asked.append(key) or False)
    with pytest.raises(MissingManifestError, match="allow-fresh-start"):
        Manifest.load(tmp_output)
    assert asked == ["manifest.parquet"]


def test_load_refuses_when_r2_is_unconfigured(tmp_output: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    with pytest.raises(MissingManifestError):
        Manifest.load(tmp_output)


def test_load_allowing_a_fresh_start_is_empty(tmp_output: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    assert "anything" not in Manifest.load(tmp_output, allow_fresh_start=True)


def test_empty_save_is_noop(tmp_output: Path) -> None:
    Manifest.empty().save(tmp_output)
    assert not (tmp_output / "manifest.parquet").exists()


def test_one_loaded_manifest_can_commit_many_batches_without_reappending(tmp_output: Path) -> None:
    import pyarrow.parquet as pq

    manifest = Manifest.empty()
    for keys in (["a", "b"], ["b", "c"], ["a", "c"]):
        manifest.record(keys)
        assert all(key in manifest for key in keys)
        manifest.save(tmp_output)
        assert not manifest.new_keys
        assert all(key in manifest for key in keys)
    assert set(
        pq.read_table(tmp_output / "manifest.parquet")["key"].to_pylist()
    ) == {"a", "b", "c"}
    assert pq.read_metadata(tmp_output / "manifest.parquet").num_rows == 3


def test_failed_manifest_commit_keeps_old_checkpoint_and_pending_keys(tmp_output, monkeypatch):
    manifest = Manifest.empty()
    manifest.record(["a"])
    manifest.save(tmp_output)
    before = (tmp_output / "manifest.parquet").read_bytes()
    manifest.record(["b"])

    def refuse(*args):
        raise OSError("disk full")

    monkeypatch.setattr(manifest_module, "save_manifest", refuse)
    with pytest.raises(OSError, match="disk full"):
        manifest.save(tmp_output)
    assert manifest.new_keys == {"b"}
    assert (tmp_output / "manifest.parquet").read_bytes() == before
