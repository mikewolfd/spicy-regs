"""Tests for the decoupled rollup base class (pipelines/rollups/base.py).

Covers the ``outputs``/``output`` contract added for multi-artifact rollups
(the bill family produces eleven tables from one pass, so re-running the whole
pipeline per table would re-run its acquisition and model calls eleven times)
and proves the existing single-output contract is unchanged.
"""

from pathlib import Path
from typing import ClassVar
import pyarrow as pa
import pyarrow.parquet as pq

from tests.generation_fakes import Store
from spicy_regs.sources import publication as pub

import pytest

from spicy_regs.pipelines.rollups.base import RollupPipeline
from spicy_regs.sources import r2 as upload_r2


def _mock_r2_client(monkeypatch: pytest.MonkeyPatch, remote_sizes: dict[str, int | None]) -> Store:
    fake = Store()
    for key, size in remote_sizes.items():
        if size is not None:
            fake.objects[key] = b"x" * size
    monkeypatch.setattr(upload_r2, "get_r2_client", lambda: fake)
    monkeypatch.setattr(pub, "load_index", lambda url: pub.empty_index())
    return fake


def _setup_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("R2_PUBLIC_URL", "https://example.test")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "fake")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "fake")
    monkeypatch.setenv("R2_BUCKET_NAME", "spicy-regs")


class _SingleOutputRollup(RollupPipeline):
    """A rollup shaped exactly like every existing one: a plain ``output``."""

    name: ClassVar[str] = "single"
    output: ClassVar[str] = "single.parquet"

    def build(self, output_dir: Path) -> Path:
        out = output_dir / self.output
        pq.write_table(pa.table({"id": ["single"]}), out)
        return out


class _MultiOutputRollup(RollupPipeline):
    """A synthetic multi-output rollup, standing in for the bill family."""

    name: ClassVar[str] = "multi"
    outputs: ClassVar[tuple[str, ...]] = ("a.parquet", "b.parquet")

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        a, b = output_dir / "a.parquet", output_dir / "b.parquet"
        pq.write_table(pa.table({"id": ["a"]}), a)
        pq.write_table(pa.table({"id": ["b"]}), b)
        return (a, b)


def test_single_output_rollup_uploads_its_one_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A single-output rollup keeps working unchanged: one file, one upload."""
    _setup_env(monkeypatch)
    uploads = _mock_r2_client(monkeypatch, {"single.parquet": None})

    rollup = _SingleOutputRollup(output_dir=tmp_path, skip_upload=False)
    rollup.run()

    assert uploads.writes[-1] == pub.INDEX_KEY
    assert list(pub.parse_index(uploads.objects[pub.INDEX_KEY])["families"]["single"]["tables"]) == ["single.parquet"]


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

    assert uploads.writes[-1] == pub.INDEX_KEY
    assert set(pub.parse_index(uploads.objects[pub.INDEX_KEY])["families"]["multi"]["tables"]) == {"a.parquet", "b.parquet"}


def test_multi_output_output_property_reads_first_declared_key(tmp_path: Path) -> None:
    rollup = _MultiOutputRollup(output_dir=tmp_path)
    assert rollup.output == "a.parquet"


def test_multi_output_shrink_guard_applies_per_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """One file catastrophically shrinking must not be masked by its siblings.

    A late sibling fails the guard before the first member is uploaded.
    """
    _setup_env(monkeypatch)
    uploads = _mock_r2_client(monkeypatch, {"a.parquet": None, "b.parquet": 10_000_000})

    rollup = _MultiOutputRollup(output_dir=tmp_path, skip_upload=False)
    with pytest.raises(RuntimeError, match="shrink"):
        rollup.run()

    # All shrink checks run before any member uploads.
    assert uploads.writes == []


def test_skip_upload_leaves_every_file_local_and_uploads_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _setup_env(monkeypatch)
    uploads = _mock_r2_client(monkeypatch, {})

    rollup = _MultiOutputRollup(output_dir=tmp_path, skip_upload=True)
    rollup.run()

    assert uploads.writes == []
    [generation] = list((tmp_path / "generations").iterdir())
    assert (generation / "a.parquet").exists()
    assert (generation / "b.parquet").exists()


def test_neither_output_nor_outputs_raises(tmp_path: Path) -> None:
    class _Unconfigured(RollupPipeline):
        name: ClassVar[str] = "unconfigured"

        def build(self, output_dir: Path) -> Path:
            return output_dir / "x.parquet"

    with pytest.raises(AttributeError, match="output"):
        _ = _Unconfigured(output_dir=tmp_path).output


@pytest.mark.parametrize("skip_upload", [True, False])
def test_docket_search_keeps_its_legacy_non_table_object(tmp_path, monkeypatch, skip_upload):
    import gzip
    import json
    from spicy_regs.pipelines.rollups.docket_search import DocketSearchRollup

    pq.write_table(pa.table({
        "docket_id": ["ACF-2015-0001"], "agency_code": ["ACF"],
        "title": ["Review title"], "docket_type": ["Rulemaking"],
        "modify_date": ["2026-09-21"], "abstract": ["Retained abstract"],
    }), tmp_path / "dockets.parquet")
    uploaded = []
    monkeypatch.setattr(upload_r2, "upload_file", lambda path, remote_key: uploaded.append((path, remote_key)))
    DocketSearchRollup(output_dir=tmp_path, skip_upload=skip_upload).run()
    output = tmp_path / "docket_search.json.gz"
    payload = json.loads(gzip.decompress(output.read_bytes()))
    assert payload["docs"][0]["id"] == "ACF-2015-0001"
    assert payload["count"] == 1
    assert uploaded == ([] if skip_upload else [(output, output.name)])
    assert not (tmp_path / "generations").exists()
