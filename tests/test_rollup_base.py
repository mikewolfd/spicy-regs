"""Tests for the decoupled rollup base class (pipelines/rollups/base.py).

Covers the ``outputs``/``output`` contract added for multi-artifact rollups
(the bill family produces eleven tables from one pass, so re-running the whole
pipeline per table would re-run its acquisition and model calls eleven times)
and proves the existing single-output contract is unchanged.
"""

from pathlib import Path
from typing import ClassVar
from unittest.mock import MagicMock

import pytest

from spicy_regs.pipelines.rollups.base import RollupPipeline
from spicy_regs.sources import r2 as upload_r2


def _mock_r2_client(monkeypatch: pytest.MonkeyPatch, remote_sizes: dict[str, int | None]) -> list[tuple[Path, str]]:
    """Install a fake R2 client whose HEAD response depends on the object key.

    Mirrors ``tests/test_load.py``'s ``TestUploadShrinkGuard`` fixture, extended
    to answer per-key so a multi-file upload can exercise the shrink guard
    independently for each file rather than once for the whole rollup.
    """
    from botocore.exceptions import ClientError

    uploads: list[tuple[Path, str]] = []
    fake = MagicMock()

    def fake_head_object(Bucket, Key):  # noqa: N803 — matches boto3's call signature
        size = remote_sizes.get(Key)
        if size is None:
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")
        return {"ContentLength": size}

    fake.head_object.side_effect = fake_head_object
    fake.upload_file.side_effect = lambda local, bucket, key, ExtraArgs=None: uploads.append((local, key))

    monkeypatch.setattr(upload_r2, "get_r2_client", lambda: fake)
    return uploads


def _setup_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "fake")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "fake")
    monkeypatch.setenv("R2_BUCKET_NAME", "spicy-regs")


class _SingleOutputRollup(RollupPipeline):
    """A rollup shaped exactly like every existing one: a plain ``output``."""

    name: ClassVar[str] = "single"
    output: ClassVar[str] = "single.parquet"

    def build(self, output_dir: Path) -> Path:
        out = output_dir / self.output
        out.write_bytes(b"x" * 1000)
        return out


class _MultiOutputRollup(RollupPipeline):
    """A synthetic multi-output rollup, standing in for the bill family."""

    name: ClassVar[str] = "multi"
    outputs: ClassVar[tuple[str, ...]] = ("a.parquet", "b.parquet")

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        a, b = output_dir / "a.parquet", output_dir / "b.parquet"
        a.write_bytes(b"a" * 1000)
        b.write_bytes(b"b" * 1000)
        return (a, b)


def test_single_output_rollup_uploads_its_one_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A single-output rollup keeps working unchanged: one file, one upload."""
    _setup_env(monkeypatch)
    uploads = _mock_r2_client(monkeypatch, {"single.parquet": None})

    rollup = _SingleOutputRollup(output_dir=tmp_path, skip_upload=False)
    rollup.run()

    assert [key for _, key in uploads] == ["single.parquet"]


def test_single_output_output_attribute_is_unchanged(tmp_path: Path) -> None:
    """``output`` still reads as the plain declared string, not via the property."""
    rollup = _SingleOutputRollup(output_dir=tmp_path)
    assert rollup.output == "single.parquet"
    assert rollup.outputs == ()  # inherited default; unused by a single-output rollup


def test_multi_output_rollup_uploads_every_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A multi-output rollup uploads every declared artifact, not just outputs[0]."""
    _setup_env(monkeypatch)
    uploads = _mock_r2_client(monkeypatch, {"a.parquet": None, "b.parquet": None})

    rollup = _MultiOutputRollup(output_dir=tmp_path, skip_upload=False)
    rollup.run()

    assert sorted(key for _, key in uploads) == ["a.parquet", "b.parquet"]


def test_multi_output_output_property_reads_first_declared_key(tmp_path: Path) -> None:
    rollup = _MultiOutputRollup(output_dir=tmp_path)
    assert rollup.output == "a.parquet"


def test_multi_output_shrink_guard_applies_per_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """One file catastrophically shrinking must not be masked by its siblings.

    ``a.parquet`` has no prior (guard is a no-op) and uploads; ``b.parquet``'s
    prior is far larger than the freshly built file, so its own guard refuses
    it — proving the guard runs independently per output rather than once for
    the rollup as a whole.
    """
    _setup_env(monkeypatch)
    uploads = _mock_r2_client(monkeypatch, {"a.parquet": None, "b.parquet": 10_000_000})

    rollup = _MultiOutputRollup(output_dir=tmp_path, skip_upload=False)
    with pytest.raises(RuntimeError, match="shrink"):
        rollup.run()

    # a.parquet uploaded before the loop hit the refusal on b.parquet.
    assert [key for _, key in uploads] == ["a.parquet"]


def test_skip_upload_leaves_every_file_local_and_uploads_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _setup_env(monkeypatch)
    uploads = _mock_r2_client(monkeypatch, {})

    rollup = _MultiOutputRollup(output_dir=tmp_path, skip_upload=True)
    rollup.run()

    assert uploads == []
    assert (tmp_path / "a.parquet").exists()
    assert (tmp_path / "b.parquet").exists()


def test_neither_output_nor_outputs_raises(tmp_path: Path) -> None:
    class _Unconfigured(RollupPipeline):
        name: ClassVar[str] = "unconfigured"

        def build(self, output_dir: Path) -> Path:
            return output_dir / "x.parquet"

    with pytest.raises(AttributeError, match="output"):
        _ = _Unconfigured(output_dir=tmp_path).output
