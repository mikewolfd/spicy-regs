"""Hermetic tests for the shared incremental-merge helper (transforms/table_merge.py).

No network: where a test needs a "prior" table, it's seeded on disk at
``table_merge``'s own scratch path (:func:`prior_scratch_path`), which the
merge then treats as an already-downloaded prior and never asks
``download_prior`` to fetch — enforced here by a ``download_prior`` stub that
raises if called.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.transforms.table_merge import merge_table, prior_scratch_path

COLUMNS = ("id", "name", "version")
NAME = "widgets"
REMOTE_KEY = "widgets.parquet"


def _write_prior(output_dir: Path, rows: list[dict]) -> None:
    schema = pa.schema([(c, pa.string()) for c in COLUMNS])
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, prior_scratch_path(output_dir, NAME))


def _read(path: Path) -> list[dict]:
    return pq.read_table(path).to_pylist()


def _never_called(remote_key: str, local_path: Path) -> bool:
    raise AssertionError("download_prior must not be called when the prior is already on disk")


def _merge(tmp_path: Path, rows: list[dict], *, download_prior=_never_called, prior_present=None) -> Path:
    return merge_table(
        tmp_path,
        name=NAME,
        columns=COLUMNS,
        identity=("id",),
        version_column="version",
        rows=rows,
        remote_key=REMOTE_KEY,
        download_prior=download_prior,
        prior_present=prior_present,
    )


_PRIOR_ROWS = [
    {"id": "1", "name": "old", "version": "2024-01-01"},
    {"id": "2", "name": "prior-only", "version": "2024-01-02"},
]


def test_merge_prefers_fresh_row_on_repeated_identity(tmp_path):
    _write_prior(tmp_path, _PRIOR_ROWS)
    fresh = [{"id": "1", "name": "new", "version": "2024-02-01"}]

    out = _merge(tmp_path, fresh)

    by_id = {row["id"]: row for row in _read(out)}
    assert by_id["1"]["name"] == "new"
    assert by_id["1"]["version"] == "2024-02-01"


def test_merge_keeps_prior_only_rows(tmp_path):
    _write_prior(tmp_path, _PRIOR_ROWS)
    fresh = [{"id": "1", "name": "new", "version": "2024-02-01"}]

    out = _merge(tmp_path, fresh)

    by_id = {row["id"]: row for row in _read(out)}
    assert by_id["2"] == {"id": "2", "name": "prior-only", "version": "2024-01-02"}


def test_merge_orders_by_version_then_identity(tmp_path):
    fresh = [
        {"id": "3", "name": "c", "version": "2024-01-01"},
        {"id": "1", "name": "a", "version": "2024-03-01"},
        {"id": "2", "name": "b", "version": "2024-03-01"},
    ]

    out = _merge(tmp_path, fresh, download_prior=lambda remote_key, local_path: False)

    # version DESC first, id ascending breaks the tie between "1" and "2".
    assert [row["id"] for row in _read(out)] == ["1", "2", "3"]


def test_merge_tolerates_a_missing_prior(tmp_path):
    fresh = [{"id": "1", "name": "a", "version": "2024-01-01"}]

    out = _merge(tmp_path, fresh, download_prior=lambda remote_key, local_path: False)

    assert _read(out) == fresh


def test_merge_leaves_no_scratch_files_behind(tmp_path):
    _write_prior(tmp_path, _PRIOR_ROWS)
    _merge(tmp_path, [{"id": "1", "name": "new", "version": "2024-02-01"}])

    leftovers = sorted(p.name for p in tmp_path.glob(f"_{NAME}_*.parquet"))
    assert leftovers == []


def test_merge_refuses_null_identity_rows(tmp_path):
    fresh = [
        {"id": None, "name": "orphan", "version": "2024-01-01"},
        {"id": "1", "name": "a", "version": "2024-01-01"},
    ]

    out = _merge(tmp_path, fresh, download_prior=lambda remote_key, local_path: False)

    assert [row["id"] for row in _read(out)] == ["1"]


class _CountingDownload:
    """Counts calls; returns False (no prior found) without writing a file."""

    def __init__(self) -> None:
        self.calls = 0

    def __call__(self, remote_key: str, local_path: Path) -> bool:
        self.calls += 1
        return False


def test_merge_skips_download_prior_on_a_known_cold_start(tmp_path):
    """``prior_present=False`` is the caller saying "I already tried, and there's
    nothing there" — a cold-start regression: without it, a caller that already
    downloaded (and got nothing) pays for the same failed request twice, once for
    itself and once inside merge_table.
    """
    download_prior = _CountingDownload()
    fresh = [{"id": "1", "name": "a", "version": "2024-01-01"}]

    out = _merge(tmp_path, fresh, download_prior=download_prior, prior_present=False)

    assert download_prior.calls == 0
    assert _read(out) == fresh


def test_merge_still_asks_download_prior_when_presence_is_unknown(tmp_path):
    """The default (``prior_present`` omitted) is unchanged: no file on disk and
    no claim from the caller means merge_table asks ``download_prior`` itself.
    """
    download_prior = _CountingDownload()
    fresh = [{"id": "1", "name": "a", "version": "2024-01-01"}]

    out = _merge(tmp_path, fresh, download_prior=download_prior)

    assert download_prior.calls == 1
    assert _read(out) == fresh


def test_merge_null_fills_columns_the_prior_table_lacks(tmp_path):
    """A contract that gained columns merges onto a prior published without them.

    This is ``congress_bills``' live shape: ten frozen columns already
    published, thirty-eight appended by the bill family. The prior rows must
    survive with NULLs in the new columns rather than failing the binder, and a
    fresh row must still win on a repeated identity.
    """
    narrow = ("id", "name", "version")
    schema = pa.schema([(c, pa.string()) for c in narrow])
    pq.write_table(
        pa.Table.from_pylist(
            [
                {"id": "1", "name": "old", "version": "2024-01-01"},
                {"id": "2", "name": "prior-only", "version": "2024-01-02"},
            ],
            schema=schema,
        ),
        prior_scratch_path(tmp_path, NAME),
    )

    wide = ("id", "name", "version", "stage", "stage_rule")
    out = merge_table(
        tmp_path,
        name=NAME,
        columns=wide,
        identity=("id",),
        version_column="version",
        rows=[{"id": "1", "name": "new", "version": "2024-02-01", "stage": "enacted", "stage_rule": "law_line"}],
        remote_key=REMOTE_KEY,
        download_prior=_never_called,
    )

    rows = {r["id"]: r for r in _read(out)}
    assert list(pq.read_table(out).schema.names) == list(wide)
    # The fresh row wins and carries the appended columns.
    assert rows["1"] == {
        "id": "1",
        "name": "new",
        "version": "2024-02-01",
        "stage": "enacted",
        "stage_rule": "law_line",
    }
    # The prior-only row survives, NULL-filled in the columns it predates.
    assert rows["2"] == {
        "id": "2",
        "name": "prior-only",
        "version": "2024-01-02",
        "stage": None,
        "stage_rule": None,
    }
