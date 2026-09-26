"""A family's table set changes only by an explicit migration: ``added_tables`` may join, none may leave."""

from pathlib import Path
from typing import ClassVar

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.pipelines.rollups.base import RollupPipeline
from spicy_regs.sources import publication as pub


def _table(path: Path) -> Path:
    pq.write_table(pa.table({"id": [path.stem]}), path, store_schema=False)
    return path


class _One(RollupPipeline):
    name: ClassVar[str] = "family"
    outputs: ClassVar[tuple[str, ...]] = ("a.parquet",)

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        return tuple(_table(output_dir / key) for key in self.outputs)


class _Grown(_One):
    outputs: ClassVar[tuple[str, ...]] = ("a.parquet", "b.parquet")


class _Declared(_Grown):
    added_tables: ClassVar[tuple[str, ...]] = ("b.parquet",)


def _tables(store) -> set[str]:
    return set(pub.parse_index(store.objects[pub.INDEX_KEY])["families"]["family"]["tables"])


def test_a_family_that_declares_nothing_republishes_as_before(tmp_path, remote):
    """Every existing rollup (bill family, roll-call votes) declares no added tables and must publish unchanged."""
    _One(output_dir=tmp_path / "first", skip_upload=False).run()
    _One(output_dir=tmp_path / "second", skip_upload=False).run()
    assert _tables(remote) == {"a.parquet"}


def test_an_undeclared_table_refuses_and_a_declared_one_joins_then_stays_inert(tmp_path, remote):
    _One(output_dir=tmp_path / "first", skip_upload=False).run()
    with pytest.raises(pub.PublicationError, match="explicit migration"):
        _Grown(output_dir=tmp_path / "undeclared", skip_upload=False).run()
    assert _tables(remote) == {"a.parquet"}

    _Declared(output_dir=tmp_path / "declared", skip_upload=False).run()
    assert _tables(remote) == {"a.parquet", "b.parquet"}
    # Left in place after the migration, the declaration changes nothing: the union already holds it.
    _Declared(output_dir=tmp_path / "again", skip_upload=False).run()
    _Grown(output_dir=tmp_path / "undeclared-now", skip_upload=False).run()
    assert _tables(remote) == {"a.parquet", "b.parquet"}


def test_a_declaration_never_lets_a_table_leave(tmp_path, remote):
    _Grown(output_dir=tmp_path / "first", skip_upload=False).run()

    class _Dropped(_One):
        added_tables: ClassVar[tuple[str, ...]] = ("b.parquet",)

    with pytest.raises(pub.PublicationError, match="explicit migration"):
        _Dropped(output_dir=tmp_path / "dropped", skip_upload=False).run()
    assert _tables(remote) == {"a.parquet", "b.parquet"}
