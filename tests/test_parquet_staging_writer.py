"""Tests for StagingWriter (Writer connector over write_staging).

Pins the staging layout (``dockets/{agency}.parquet``), that a generator is
accepted and that an empty write creates no file.
"""

from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest

from spicy_regs.schemas import DOCKET
from spicy_regs.sources import StagingWriter


def test_write_creates_staging_parquet(tmp_output: Path, sample_dockets: list[dict]) -> None:
    staging_dir = tmp_output / "staging"
    writer = StagingWriter("EPA", DOCKET, staging_dir)

    writer.write(sample_dockets)

    staging_file = staging_dir / "dockets" / "EPA.parquet"
    assert staging_file.exists()
    assert writer.rows_written == len(sample_dockets)

    df = pl.read_parquet(staging_file)
    assert df.height == len(sample_dockets)
    assert set(df.columns) == set(DOCKET.schema.keys())


def test_write_accepts_a_generator(tmp_output: Path, sample_dockets: list[dict]) -> None:
    staging_dir = tmp_output / "staging"
    writer = StagingWriter("EPA", DOCKET, staging_dir)

    writer.write(r for r in sample_dockets)  # generator, not a list

    assert writer.rows_written == len(sample_dockets)
    assert (staging_dir / "dockets" / "EPA.parquet").exists()


def test_write_empty_records_writes_nothing(tmp_output: Path) -> None:
    staging_dir = tmp_output / "staging"
    writer = StagingWriter("EPA", DOCKET, staging_dir)

    writer.write([])

    assert writer.rows_written == 0
    assert not (staging_dir / "dockets" / "EPA.parquet").exists()


def test_large_text_stream_flushes_before_source_exhaustion(tmp_path, sample_dockets):
    from spicy_regs.transforms.write_staging import MAX_BATCH_CHARACTERS

    writer = StagingWriter("EPA", DOCKET, tmp_path)
    temporary = tmp_path / "dockets/EPA.parquet.tmp"
    row = {**sample_dockets[0], "title": "x" * (MAX_BATCH_CHARACTERS + 1)}

    def source():
        yield row
        assert temporary.stat().st_size > 4  # data reached disk before the next source row
        assert not (tmp_path / "dockets/EPA.parquet").exists()
        yield {**row, "docket_id": "EPA-second"}

    writer.write(source())
    path = tmp_path / "dockets/EPA.parquet"
    assert writer.rows_written == 2
    assert pq.read_metadata(path).num_row_groups == 2
    assert pl.read_parquet(path)["docket_id"].to_list() == [row["docket_id"], "EPA-second"]


def test_incomplete_stream_preserves_previous_file_then_empty_run_removes_stale_staging(tmp_path, sample_dockets):
    from spicy_regs.transforms.write_staging import MAX_BATCH_CHARACTERS

    writer = StagingWriter("EPA", DOCKET, tmp_path)
    writer.write(sample_dockets)
    path = tmp_path / "dockets/EPA.parquet"
    before = path.read_bytes()

    def fails_after_flushing():
        yield {**sample_dockets[0], "title": "x" * (MAX_BATCH_CHARACTERS + 1)}
        raise RuntimeError("source interrupted")

    with pytest.raises(RuntimeError, match="source interrupted"):
        writer.write(fails_after_flushing())
    assert path.read_bytes() == before
    assert list(path.parent.glob("*.parquet")) == [path]
    assert not list(path.parent.glob("*.tmp"))
    writer.write(iter(()))
    assert not path.exists()
    assert writer.rows_written == 0
