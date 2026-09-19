"""Shared merge helper for incremental, deduplicated Parquet tables.

Every rollup that ingests an external source and republishes an incremental
table follows the same three steps: best-effort download the prior published
table from R2, dedup the union of prior + freshly fetched rows on an identity
key (preferring the fresh row), and order the result by a version column.
Lifted out of ``build_congress_bills.py`` (its original prior-download + DuckDB
merge, ~lines 135-210) and parameterised over the column tuple, identity, and
version column, so a table with a different key shape than a single
``bill_id`` does not have to copy the SQL. ``build_congress_bills`` now calls
this directly; the behavior-preservation proof is
``tests/test_congress_bills.py::test_build_congress_bills_merges_prior_and_fresh_rows``,
which seeds a prior Parquet, stubs the fetch and the R2 download, and asserts
on the merged output — the other tests in that file cover ``_shape``,
``_bill_id`` and windowing only, never the merge itself.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2


def prior_scratch_path(output_dir: Path, name: str) -> Path:
    """The local path :func:`merge_table` caches/reuses the prior table under.

    Public so a caller that must inspect the prior table *before* merging —
    e.g. computing an incremental fetch window from its max version column —
    can download to this exact path first. When that download *succeeds*,
    :func:`merge_table` finds the file already present and does not re-download
    it. When it fails (no prior table exists yet — a cold start), no file is
    written, and ``merge_table`` cannot tell "not tried" from "tried and
    absent" from the path alone; pass its own result as ``prior_present`` to
    ``merge_table`` so a known-absent prior isn't downloaded a second time.
    """
    return output_dir / f"_{name}_prior.parquet"


def published_table(
    output_dir: Path, name: str, download_prior: Callable[[str, Path], bool] = r2.download
) -> Path | None:
    """The published ``name`` table, downloaded to :func:`prior_scratch_path` unless already there.

    ``None`` when nothing is published yet, which every caller treats as a cold
    start rather than an error. This is the one idiom behind every "read a
    published table before merging" — a transform's own prior, for a watermark
    or a held-set, and the two merge-time joins against another rollup's
    published output (``press_releases`` against ``congress_bills``,
    ``roll_call_votes`` against ``bill_vote_references``). Pass the result's
    presence on to :func:`merge_table` as ``prior_present`` when the table is
    the caller's own, so a known-absent prior is not asked for twice.
    """
    path = prior_scratch_path(output_dir, name)
    return path if (path.exists() or download_prior(f"{name}.parquet", path)) else None


def merge_table(
    output_dir: Path,
    *,
    name: str,
    columns: tuple[str, ...],
    identity: tuple[str, ...],
    version_column: str | None,
    rows: Iterable[Mapping[str, object]],
    remote_key: str,
    download_prior: Callable[[str, Path], bool] = r2.download,
    prior_present: bool | None = None,
    coalesce_prior: bool = False,
) -> Path:
    """Merge freshly fetched ``rows`` against the prior ``remote_key`` table.

    Writes ``output_dir / remote_key``: the union of the prior table (best-effort
    downloaded via ``download_prior``, defaulting to :func:`spicy_regs.sources.r2.download`)
    and ``rows``, deduplicated on ``identity`` (all columns must be non-null;
    the fresh row wins over the prior one on a repeated identity), ordered by
    ``version_column`` descending then ``identity`` — or by ``identity`` alone
    when ``version_column`` is ``None``, which is how the two contracts with no
    freshness axis (``section_diff_items``, ``financial_changes``, both ordered
    within their parent by a sequence number) publish.

    ``columns`` becomes an all-VARCHAR Arrow schema — every published table in
    this pipeline is string-typed, so callers coerce before calling this. An
    absent prior table (first run, or ``download_prior`` returning ``False``)
    degrades to publishing ``rows`` alone, which is a full backfill, not an
    error.

    A prior table missing some of ``columns`` is also not an error: each absent
    column is selected as a VARCHAR NULL, so appending a column to a contract
    is a NULL backfill on the rows already published rather than a migration.
    ``congress_bills`` is the live case — its first ten columns are frozen
    because other repositories pin that prefix by digest, and the bill family
    appends thirty-eight more, so the first family run merges 48-column rows
    onto a 10-column published table.

    ``coalesce_prior``: merge column-wise instead of row-wise. The default
    (``False``) is row replacement — a fresh row wins whole, and a NULL in it is
    a real value that overwrites. Set it when a table has **two writers that own
    different column subsets**, where a fresh NULL means "I do not populate this
    column" rather than "this is now empty": the merge becomes a FULL OUTER JOIN
    on the identity emitting ``COALESCE(fresh, prior)`` per column, so the narrow
    writer updates its own columns and leaves the others standing.
    ``congress_bills`` is the only such table — ``congress-bills`` walks the whole
    archive for the frozen ten while ``bill-family`` fills all forty-eight — and
    :func:`merge_contract_table` sets the flag for it alone. Without it, a
    narrow run does not merely NULL the appended columns, it *drops* them: the
    projection onto ``columns`` rewrites the file at the narrow writer's width,
    and at realistic scale the result is 96.6% of the prior bytes, which the R2
    shrink guard (0.5) does not notice.

    ``prior_present``: pass ``False`` when the caller already tried
    :func:`prior_scratch_path` and knows the prior is absent (e.g. it read the
    prior table's max version to size an incremental fetch window first, and
    that download itself returned ``False``) — this skips calling
    ``download_prior`` a second time for the same known-absent object.
    ``True`` or the default ``None`` leave the existing
    "already on disk, else ask ``download_prior``" behavior unchanged.
    """
    import duckdb

    out_file = output_dir / remote_key
    prior_file = prior_scratch_path(output_dir, name)
    new_file = output_dir / f"_{name}_new.parquet"

    schema = pa.schema([(c, pa.string()) for c in columns])

    # 1. Pull the prior table (best effort — absence just means full backfill).
    # ``prior_present is False`` is the caller telling us it already tried and
    # found nothing; anything else falls back to disk-then-download.
    if prior_present is False:
        have_prior = False
    else:
        have_prior = prior_file.exists() or download_prior(remote_key, prior_file)
    if have_prior:
        logger.info("{}: merging against prior table {}", name, prior_file)
    else:
        logger.info("{}: no prior table found — full backfill", name)

    # 2. Shape the freshly fetched rows into a "new rows" parquet.
    row_list = list(rows)
    table = pa.Table.from_pylist(row_list, schema=schema) if row_list else schema.empty_table()
    pq.write_table(table, new_file, compression="zstd")
    logger.info("{}: {:,} fresh rows this run", name, len(row_list))

    # 3. Merge prior + new, dedup on identity preferring the new row.
    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    cols = ", ".join(columns)
    key_cols = ", ".join(identity)
    order_by = f"{version_column} DESC, {key_cols}" if version_column else key_cols
    not_null = " AND ".join(f"{c} IS NOT NULL" for c in identity)
    # Two rows from the same source sharing an identity are resolved by the
    # version column, not by whichever the scan happened to reach first, so a
    # re-run over the same input produces the same file.
    newest = f"_src DESC, {version_column} DESC" if version_column else "_src DESC"

    prior_select = ""
    if have_prior:
        # The prior table may predate columns this contract has since gained —
        # ``congress_bills`` is the live case: its first ten columns are frozen
        # and the bill family appends to them, so a prior published before the
        # family ran has ten of forty-eight. Select each missing column as a
        # typed NULL rather than letting the binder fail on it, which makes a
        # column addition a NULL backfill instead of a migration.
        prior_cols = {
            str(row[0]) for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{prior_file}')").fetchall()
        }
        absent = [c for c in columns if c not in prior_cols]
        if absent:
            logger.info(
                "{}: prior table lacks {} of {} columns ({}) — NULL-filling them",
                name,
                len(absent),
                len(columns),
                ", ".join(absent),
            )
        extra = sorted(prior_cols - set(columns))
        if extra:
            # Not fatal — the contract is the published shape — but a column
            # the prior has and the contract does not is either a rename or a
            # removal, and both are worth seeing before the bytes go.
            logger.warning(
                "{}: prior table has {} column(s) the contract does not ({}) — dropping them",
                name,
                len(extra),
                ", ".join(extra),
            )
        prior_select = ", ".join(c if c in prior_cols else f"CAST(NULL AS VARCHAR) AS {c}" for c in columns)

    if have_prior and coalesce_prior:
        # Column-wise: the narrow writer's NULL must not erase the wide
        # writer's value, so each column takes the fresh value only where the
        # fresh row states one.
        fresh_rank = f"ROW_NUMBER() OVER (PARTITION BY {key_cols} ORDER BY {newest})"
        fresh_q = (
            f"SELECT {cols} FROM ("
            f"SELECT {cols}, 1 AS _src, {fresh_rank} AS _rn FROM read_parquet('{new_file}') WHERE {not_null}"
            f") WHERE _rn = 1"
        )
        prior_q = f"SELECT {prior_select} FROM read_parquet('{prior_file}') WHERE {not_null}"
        merged = ", ".join(c if c in identity else f"COALESCE(f.{c}, p.{c}) AS {c}" for c in columns)
        selection = f"SELECT {merged} FROM ({fresh_q}) f FULL OUTER JOIN ({prior_q}) p USING ({key_cols})"
    else:
        if have_prior:
            union = (
                f"SELECT {prior_select}, 0 AS _src FROM read_parquet('{prior_file}') "
                f"UNION ALL BY NAME "
                f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_file}')"
            )
        else:
            union = f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_file}')"
        selection = f"""
            SELECT {cols} FROM (
                SELECT {cols}, ROW_NUMBER() OVER (
                    PARTITION BY {key_cols} ORDER BY {newest}
                ) AS _rn
                FROM ({union})
                WHERE {not_null}
            )
            WHERE _rn = 1
        """

    con.execute(
        f"""
        COPY (
            SELECT {cols} FROM ({selection})
            ORDER BY {order_by}
        ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
        """
    )
    con.close()

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("{}: {:,} rows", name, total)
    return out_file


#: Tables merged column-wise rather than row-wise, because more than one
#: rollup writes them and each owns a different subset of the columns. Only
#: ``congress_bills`` qualifies: ``congress-bills`` walks the whole archive for
#: the frozen ten, ``bill-family`` fills all forty-eight for the Congresses it
#: is scoped to, and neither may erase the other's columns by publishing NULLs
#: where it simply has nothing to say. Adding a table here is a claim that a
#: NULL from one of its writers means "not mine", never "now empty".
COALESCED_TABLES: frozenset[str] = frozenset({"congress_bills"})


def merge_contract_table(
    output_dir: Path,
    contract_name: str,
    rows: Iterable[Mapping[str, object]],
    *,
    download_prior: Callable[[str, Path], bool] = r2.download,
    prior_present: bool | None = None,
) -> Path:
    """Merge ``rows`` for one ``spicy_docs.schemas`` table contract.

    The single door the twenty-two hosted tables go through. The contract owns
    the column tuple, the identity and the version column, so this repository
    never restates a published shape: a column appended in spicy-docs appears
    here by re-reading ``TABLE_CONTRACTS``, and the NULL-fill in
    :func:`merge_table` makes that a backfill rather than a migration.

    The remote key is ``{contract_name}.parquet``, matching the R2 key = MCP
    view = dictionary key convention every other published table follows.
    """
    from spicy_docs.schemas import TABLE_CONTRACTS

    contract = TABLE_CONTRACTS[contract_name]
    coalesce = contract_name in COALESCED_TABLES
    return merge_table(
        output_dir,
        name=contract.name,
        columns=contract.columns,
        identity=contract.identity,
        version_column=contract.version_column,
        rows=rows,
        remote_key=f"{contract.name}.parquet",
        download_prior=download_prior,
        prior_present=prior_present,
        coalesce_prior=coalesce,
    )
