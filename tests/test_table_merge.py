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
import pytest
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


# --------------------------------------------------------------------------- #
# A8/A9 (laws and rosters): scope replacement, and the statutes join on congress_bills.
# --------------------------------------------------------------------------- #
def _contract_rows(name: str, rows: list[dict]) -> list[dict]:
    from spicy_docs.schemas import TABLE_CONTRACTS

    columns = TABLE_CONTRACTS[name].columns
    return [{c: None for c in columns} | row for row in rows]


def _write(path: Path, name: str, rows: list[dict]) -> None:
    from spicy_docs.schemas import TABLE_CONTRACTS

    columns = TABLE_CONTRACTS[name].columns
    pq.write_table(
        pa.Table.from_pylist(_contract_rows(name, rows), schema=pa.schema([(c, pa.string()) for c in columns])), path
    )


def test_retire_prior_rows_drops_exactly_the_scope(tmp_path):
    from spicy_regs.transforms.table_merge import prior_scratch_path, retire_prior_rows

    prior = prior_scratch_path(tmp_path, "committee_assignments")
    _write(
        prior,
        "committee_assignments",
        [
            {"congress": "119", "chamber": "house", "system_code": "a", "bioguide_id": "1"},
            {"congress": "119", "chamber": "house", "system_code": "b", "bioguide_id": "2"},
            {"congress": "119", "chamber": "senate", "system_code": "c", "bioguide_id": "3"},
            {"congress": "118", "chamber": "house", "system_code": "a", "bioguide_id": "1"},
            {"congress": None, "chamber": "house", "system_code": "z", "bioguide_id": "9"},
        ],
    )
    assert retire_prior_rows(prior, congress="119", chamber="house") == 2
    kept = {(r["congress"], r["chamber"]) for r in pq.read_table(prior).to_pylist()}
    assert kept == {("119", "senate"), ("118", "house"), (None, "house")}
    with pytest.raises(ValueError, match="at least one scope column"):
        retire_prior_rows(prior)
    with pytest.raises(ValueError, match="not a snake_case identifier"):
        retire_prior_rows(prior, **{"congress; DROP": "1"})


def _seed_laws(tmp_path: Path, rows: list[dict]) -> None:
    from spicy_regs.transforms.table_merge import prior_scratch_path

    _write(prior_scratch_path(tmp_path, "laws"), "laws", rows)


def _no_download(remote_key: str, local_path: Path) -> bool:
    return False


def test_congress_bills_takes_its_citation_from_the_published_laws_table(tmp_path):
    from spicy_docs.schemas import TABLE_CONTRACTS

    from spicy_regs.transforms.table_merge import merge_contract_table

    _seed_laws(
        tmp_path,
        [
            {
                "law_id": "119-public-1",
                "congress": "119",
                "law_type": "public",
                "number": "1",
                "bill_id": "119-s-5",
                "statutes_at_large_cite": "139 Stat. 3",
                "update_date": "2026-07-30",
            },
            # A law whose PLAW lagged states no citation and fills nothing.
            {
                "law_id": "119-public-110",
                "congress": "119",
                "law_type": "public",
                "number": "110",
                "bill_id": "119-s-307",
                "update_date": "2026-09-18",
            },
        ],
    )
    fresh = _contract_rows(
        "congress_bills",
        [
            {
                "bill_id": "119-s-5",
                "congress": "119",
                "bill_type": "s",
                "bill_number": "5",
                "update_date": "2026-09-01",
            },
            {
                "bill_id": "119-s-307",
                "congress": "119",
                "bill_type": "s",
                "bill_number": "307",
                "update_date": "2026-09-01",
            },
            {
                "bill_id": "119-hr-1",
                "congress": "119",
                "bill_type": "hr",
                "bill_number": "1",
                "update_date": "2026-09-02",
            },
        ],
    )
    out = merge_contract_table(tmp_path, "congress_bills", fresh, download_prior=_no_download)
    table = pq.read_table(out)
    assert table.schema.names == list(TABLE_CONTRACTS["congress_bills"].columns)
    rows = {r["bill_id"]: r for r in table.to_pylist()}
    assert rows["119-s-5"]["statutes_at_large_cite"] == "139 Stat. 3"
    assert rows["119-s-307"]["statutes_at_large_cite"] is None
    assert rows["119-hr-1"]["statutes_at_large_cite"] is None
    assert [r["bill_id"] for r in table.to_pylist()] == ["119-hr-1", "119-s-307", "119-s-5"], (
        "the merge's order is kept"
    )
    assert not (tmp_path / "_laws_prior.parquet").exists(), "the soft input's scratch copy is not left behind"


def test_a_writers_null_never_erases_a_citation_and_no_laws_table_leaves_it_as_it_was(tmp_path):
    from spicy_regs.transforms.table_merge import merge_contract_table, prior_scratch_path

    _write(
        prior_scratch_path(tmp_path, "congress_bills"),
        "congress_bills",
        [
            {
                "bill_id": "119-s-5",
                "congress": "119",
                "update_date": "2026-09-01",
                "statutes_at_large_cite": "139 Stat. 3",
                "stage": "law",
            }
        ],
    )
    # The family's fresh row states no citation (the one-pass rule) and no laws table is published.
    fresh = _contract_rows(
        "congress_bills", [{"bill_id": "119-s-5", "congress": "119", "update_date": "2026-09-02", "title": "fresh"}]
    )
    out = merge_contract_table(tmp_path, "congress_bills", fresh, download_prior=_no_download, prior_present=True)
    row = pq.read_table(out).to_pylist()[0]
    assert (row["statutes_at_large_cite"], row["stage"], row["title"]) == ("139 Stat. 3", "law", "fresh")


def test_a_recaptured_citation_in_laws_wins_over_the_one_already_here(tmp_path):
    from spicy_regs.transforms.table_merge import merge_contract_table, prior_scratch_path

    _write(
        prior_scratch_path(tmp_path, "congress_bills"),
        "congress_bills",
        [
            {
                "bill_id": "119-s-5",
                "congress": "119",
                "update_date": "2026-09-01",
                "statutes_at_large_cite": "139 Stat. 2",
            }
        ],
    )
    _seed_laws(
        tmp_path,
        [
            {
                "law_id": "119-public-1",
                "congress": "119",
                "law_type": "public",
                "number": "1",
                "bill_id": "119-s-5",
                "statutes_at_large_cite": "139 Stat. 3",
                "update_date": "2026-07-30",
            }
        ],
    )
    out = merge_contract_table(tmp_path, "congress_bills", [], download_prior=_no_download, prior_present=True)
    assert pq.read_table(out).to_pylist()[0]["statutes_at_large_cite"] == "139 Stat. 3"
