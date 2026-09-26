"""Shared merge helper for incremental, deduplicated Parquet tables.

Every rollup that ingests an external source and republishes an incremental
table follows the same three steps: best-effort download the prior published
table from R2, dedup the union of prior + freshly fetched rows on an identity
key (preferring the fresh row), and order the result by a version column. This
is that step, parameterised over the column tuple, identity and version column
so a table with any key shape can use it.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterable, Mapping
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2

ReplacementScope = tuple[str, Collection[str]] | tuple[tuple[str, ...], Collection[tuple[str, ...]]]


def merge_local_prior(
    con,
    *,
    columns: tuple[str, ...],
    identity: str | tuple[str, ...],
    order_by: str,
    prior_file: Path | None,
    new_file: Path,
    out_file: Path,
    row_group_size: int = 50_000,
    kv_metadata: Mapping[str, str] | None = None,
    retire: str | None = None,
    derived: Mapping[str, str] | None = None,
) -> None:
    """Merge the fresh ``new_file`` over a local prior on one identity column.

    The shared idiom behind the ingest rollups: union the prior file (when it
    exists — the caller already downloaded it where it downloads one) with the
    fresh rows, keep the fresh row on a repeated identity, refuse a NULL
    identity, and publish ordered by ``order_by`` with small row groups for
    scoped reads. One scan per side, deduplicated once, instead of one
    hand-rolled copy per rollup. A composite ``identity`` names its columns in
    order; only the first must be non-NULL (a SAM registration's EFT indicator
    is usually NULL, and NULLs partition together).

    ``retire`` is a SQL predicate over prior rows, for a caller whose fresh rows
    are the complete population of a slice: a prior row it holds TRUE for is
    dropped before the union, so what the slice no longer contains is retired.
    A row it holds NULL for is kept.

    ``derived`` maps an output column to a SQL expression over ``columns``,
    appended after them and recomputed from every merged row each run, so a
    prior row gains it and it never drifts from the columns it reads.
    """
    keys = (identity,) if isinstance(identity, str) else tuple(identity)
    cols = ", ".join(f'"{c}"' for c in columns)
    new_path = str(new_file).replace("'", "''")
    if prior_file is not None and prior_file.exists():
        prior_path = str(prior_file).replace("'", "''")
        union = (
            f"SELECT {cols}, 0 AS _src FROM read_parquet('{prior_path}') "
            + (f"WHERE ({retire}) IS NOT TRUE " if retire else "")
            + f"UNION ALL BY NAME SELECT {cols}, 1 AS _src FROM read_parquet('{new_path}')"
        )
    else:
        union = f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_path}')"
    out_path = str(out_file).replace("'", "''")
    extra = "".join(f', ({expression}) AS "{name}"' for name, expression in (derived or {}).items())
    metadata_clause = ""
    if kv_metadata is not None:
        metadata_clause = ", KV_METADATA ?"
    con.execute(
        f"""
        COPY (
            SELECT {cols}{extra} FROM (
                SELECT {cols}, ROW_NUMBER() OVER (
                    PARTITION BY {", ".join(f'"{k}"' for k in keys)} ORDER BY _src DESC
                ) AS _rn
                FROM ({union})
                WHERE "{keys[0]}" IS NOT NULL
            )
            WHERE _rn = 1
            ORDER BY {order_by}
        ) TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {row_group_size}{metadata_clause});
        """,
        ([kv_metadata] if kv_metadata is not None else None),
    )


def retired_rows(con, *, identity: tuple[str, ...], prior_file: Path, new_file: Path, retire: str) -> list[list]:
    """The identities of prior rows ``retire`` drops that ``new_file`` does not supply again, in key order.

    What :func:`merge_local_prior` removes for the same ``retire``: a caller journals it as a
    ``rows-retired`` event, which qualification reconciles against the rows actually removed.
    """
    prior, new = (str(path).replace("'", "''") for path in (prior_file, new_file))
    keys = ", ".join(f'"{k}"' for k in identity)
    same = " AND ".join(f'n."{k}" IS NOT DISTINCT FROM p."{k}"' for k in identity)
    rows = con.execute(
        f"SELECT {keys} FROM read_parquet('{prior}') p WHERE ({retire}) "
        f"AND NOT EXISTS (SELECT 1 FROM read_parquet('{new}') n WHERE {same}) ORDER BY {keys}"
    ).fetchall()
    return [list(row) for row in rows]


def prior_scratch_path(output_dir: Path, name: str) -> Path:
    """The local path :func:`merge_table` caches/reuses the prior table under.

    Public so a caller that must inspect the prior table *before* merging —
    e.g. to size an incremental fetch window from its max version column — can
    download to this exact path first; a successful download means
    :func:`merge_table` finds the file present and does not re-download it. A
    failed download (a cold start) writes nothing, so pass the caller's own
    result as ``prior_present`` so a known-absent prior isn't asked for twice.
    """
    return output_dir / f"_{name}_prior.parquet"


def published_table(
    output_dir: Path, name: str, download_prior: Callable[[str, Path], bool] = r2.download
) -> Path | None:
    """The published ``name`` table, downloaded to :func:`prior_scratch_path` unless already there.

    ``None`` when nothing is published yet, which every caller treats as a cold
    start rather than an error. This is the one idiom behind every "read a
    published table before merging": a transform's own prior, for a watermark
    or a held-set, and the merge-time joins against another rollup's published
    output. Pass the result's presence on to :func:`merge_table` as
    ``prior_present`` when the table is the caller's own, so a known-absent
    prior is not asked for twice.
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
    replace_parents: ReplacementScope | None = None,
    backfill_prior: Mapping[str, str] | None = None,
    parquet_metadata: Mapping[str, str] | None = None,
) -> Path:
    """Merge freshly fetched ``rows`` against the prior ``remote_key`` table.

    Writes ``output_dir / remote_key``: the union of the prior table
    (best-effort downloaded via ``download_prior``, defaulting to
    :func:`spicy_regs.sources.r2.download`) and ``rows``, deduplicated on
    ``identity`` (every identity column must be non-null; the fresh row wins
    over the prior one on a repeated identity), ordered by ``version_column``
    descending then ``identity`` — or by ``identity`` alone when
    ``version_column`` is ``None``, which is how the two contracts with no
    freshness axis (``section_diff_items``, ``financial_changes``, both ordered
    by a sequence number within their parent) publish.
    ``columns`` becomes an all-VARCHAR Arrow schema, so callers coerce before
    calling. An absent prior table (first run, or ``download_prior`` returning
    ``False``) degrades to publishing ``rows`` alone, which is a full backfill,
    not an error.

    A prior table missing some of ``columns`` is also not an error: each absent
    column is selected as a VARCHAR NULL, so appending a column to a contract
    is a NULL backfill on the rows already published rather than a migration.
    ``congress_bills`` is the live case — its first ten columns are frozen
    because other repositories pin that prefix by digest, and the bill family
    appends the rest.

    ``coalesce_prior``: merge column-wise instead of row-wise. The default
    (``False``) is row replacement — a fresh row wins whole, and a NULL in it
    is a real value that overwrites. Set it when a table holds **rows from two
    sources that state different column subsets**, where a fresh NULL means "I
    do not populate this column" rather than "this is now empty": the merge
    becomes a FULL OUTER JOIN on the identity emitting ``COALESCE(fresh,
    prior)`` per column, except where :data:`COALESCE_RULES` names a column's
    own rule. Without it, a narrower row does not merely NULL the other
    source's columns, it *drops* them, and the loss is small enough that the R2
    shrink guard (0.5) does not notice. :func:`merge_contract_table` sets the
    flag for ``congress_bills`` alone.

    ``prior_present``: pass ``False`` when the caller already tried
    :func:`prior_scratch_path` and knows the prior is absent, skipping a second
    ``download_prior`` call for the same known-absent object; ``True`` or the
    default ``None`` leave the disk-then-download behavior unchanged.

    ``replace_parents`` names a parent column and the successfully evaluated
    parent IDs: remove their prior relationship rows before merging, including
    when their fresh result is empty, while unread or failed parents keep their
    rows. A tuple of column names and a collection of matching value tuples
    replaces composite scopes, such as one file within one package, without
    modifying the retained prior file before the merge succeeds.

    ``backfill_prior`` maps a column to another column of the same table whose
    value a prior row takes where its own is NULL or absent. It exists for a
    column appended to the identity: the NULL-fill above would leave that part
    of every prior row's identity NULL, and the identity test would then drop
    every prior row without a word. The committee-report tables are the case:
    ``{"part_id": "package_id"}`` spells a row published before
    ``(package_id, part_id)`` as the package's one part (decision 29). It is
    applied in the prior's select, before the identity test and alongside
    ``replace_parents``.

    ``parquet_metadata`` writes processing checkpoints in the same artifact as
    the rows, including a successful read producing zero rows; callers supply
    the complete metadata to retain, it is not inferred from row presence.
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
    prior_filter = ""
    if replace_parents is not None:
        scope_columns, scope_values = replace_parents
        if isinstance(scope_columns, str):
            scope_columns = (scope_columns,)
            value_rows = [(value,) for value in scope_values]
        else:
            value_rows = list(scope_values)
        if not scope_columns or len(set(scope_columns)) != len(scope_columns) or any(
            column not in columns or not column.isidentifier() for column in scope_columns
        ):
            raise ValueError("replace_parents must name distinct table columns")
        if any(
            not isinstance(row, tuple) or len(row) != len(scope_columns)
            or any(not isinstance(value, str) for value in row)
            for row in value_rows
        ):
            raise ValueError("replace_parents values must match its columns")
        con.register("replaced_parents", pa.table({
            column: pa.array([row[i] for row in value_rows], type=pa.string())
            for i, column in enumerate(scope_columns)
        }))
        scope_match = " AND ".join(f"r.{column} = p.{column}" for column in scope_columns)
        prior_filter = f"WHERE NOT EXISTS (SELECT 1 FROM replaced_parents r WHERE {scope_match})"
    backfill = dict(backfill_prior or {})
    if any(column not in columns or source not in columns for column, source in backfill.items()):
        raise ValueError("backfill_prior must map table columns to table columns")
    if have_prior:
        # The prior table may predate columns this contract has since gained —
        # ``congress_bills`` is the live case: its first ten columns are frozen
        # and the bill family appends to them, so a prior published before the
        # family ran has only the ten. Select each missing column as a
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

        def prior_column(column: str) -> str:
            source = backfill.get(column)
            source = source if source in prior_cols else None
            if column in prior_cols:
                return f"COALESCE({column}, {source}) AS {column}" if source else column
            return f"{source} AS {column}" if source else f"CAST(NULL AS VARCHAR) AS {column}"

        prior_select = ", ".join(prior_column(c) for c in columns)

    if have_prior and coalesce_prior:
        # Column-wise: a fresh row's NULL must not erase a value the prior
        # row's source stated, so each column takes the fresh value only where
        # the fresh row states one.
        fresh_rank = f"ROW_NUMBER() OVER (PARTITION BY {key_cols} ORDER BY {newest})"
        fresh_q = (
            f"SELECT {cols} FROM ("
            f"SELECT {cols}, 1 AS _src, {fresh_rank} AS _rn FROM read_parquet('{new_file}') WHERE {not_null}"
            f") WHERE _rn = 1"
        )
        prior_q = (f"SELECT * FROM (SELECT {prior_select} FROM read_parquet('{prior_file}') p {prior_filter}) "
                   f"WHERE {not_null}")
        rules = COALESCE_RULES.get(name, {})
        merged = ", ".join(
            c if c in identity else f"{rules[c]} AS {c}" if c in rules else f"COALESCE(f.{c}, p.{c}) AS {c}"
            for c in columns
        )
        selection = f"SELECT {merged} FROM ({fresh_q}) f FULL OUTER JOIN ({prior_q}) p USING ({key_cols})"
    else:
        if have_prior:
            union = (
                f"SELECT {prior_select}, 0 AS _src FROM read_parquet('{prior_file}') p {prior_filter} "
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

    metadata_option = ", KV_METADATA ?" if parquet_metadata else ""
    copy_parameters: list[object] = [str(out_file)]
    if parquet_metadata:
        copy_parameters.append(dict(parquet_metadata))
    con.execute(
        f"""
        COPY (
            SELECT {cols} FROM ({selection})
            ORDER BY {order_by}
        ) TO ? (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000{metadata_option});
        """,
        copy_parameters,
    )
    con.close()

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("{}: {:,} rows", name, total)
    return out_file


#: Tables merged column-wise rather than row-wise, because their rows come
#: from sources that state different subsets of the columns. Only
#: ``congress_bills`` qualifies: the retired list writer (plan A1, decision 31)
#: left the frozen ten on every bill of the archive, ``bill-family`` — now its
#: one writer — fills the whole contract for the Congresses it is scoped to,
#: and a family row that states no ``url`` must not erase the list-era one
#: (decision 1 keeps it, labelled ``inherited``). Adding a table here is a
#: claim that a NULL from one of its sources means "not mine", never "now
#: empty".
COALESCED_TABLES: frozenset[str] = frozenset({"congress_bills"})

#: The two ``update_date`` shapes ``congress_bills`` holds: the list route's date and
#: BILLSTATUS's UTC instant. Measured on the live table of 2026-09-23 (drift audit
#: ``drift-audit-2026-09-23/``): 387,946 dates and 31,920 instants, nothing else, the earliest
#: 2016-10-26 — so a 19xx/20xx year with valid month, day and clock fields is the whole range.
UPDATE_DATE_SHAPE = (
    r"(19|20)\d\d-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])(T([01]\d|2[0-3]):[0-5]\d:[0-5]\dZ)?"
)

#: Columns a column-wise merge derives instead of coalescing. ``url_source`` names who stated
#: ``url``, so it follows the url the merge keeps: the fresh writer's label when it stated one,
#: ``inherited`` when a fresh row stated none and the prior value survives, and the prior label
#: on a row this run did not touch. Delivery decision 1 (2026-09-22) accepts inherited URLs only
#: with this label. ``update_date`` keeps the larger value, as the contract's column sentence
#: says: a fresh read that states an older stamp than the one published never moves the row
#: backwards, and a same-day date-only value (the list route's) loses to BILLSTATUS's instant,
#: which sorts after it. Only when both sides have one of :data:`UPDATE_DATE_SHAPE`'s two shapes
#: is VARCHAR order time order, so only then is ``GREATEST`` taken; any other value on either
#: side — a ``9999-…`` sentinel, a spaced instant — could never be displaced by it, so the
#: fresh value wins as it does in every other column (a NULL side fails the match too).
COALESCE_RULES: dict[str, dict[str, str]] = {
    "congress_bills": {
        "update_date": (
            f"CASE WHEN regexp_full_match(f.update_date, '{UPDATE_DATE_SHAPE}') "
            f"AND regexp_full_match(p.update_date, '{UPDATE_DATE_SHAPE}') "
            "THEN GREATEST(f.update_date, p.update_date) ELSE COALESCE(f.update_date, p.update_date) END"
        ),
        "url_source": (
            "CASE WHEN f.url IS NOT NULL THEN f.url_source WHEN p.url IS NULL THEN NULL "
            "WHEN f.bill_id IS NULL THEN p.url_source ELSE 'inherited' END"
        ),
    },
}

#: ``congress_bills.statutes_at_large_cite`` is filled from ``laws`` at this
#: table's merge — the contract's own sentence for the column — and from
#: nowhere else: the bill family sees one BILLSTATUS document, and the
#: citation lives in the PLAW USLM file the laws rollup captures once per law.
STATUTES_JOIN_TARGET = "congress_bills"
STATUTES_JOIN_SOURCE = "laws"
STATUTES_JOIN_COLUMN = "statutes_at_large_cite"
STATUTES_JOIN_KEY = "bill_id"


def _duckdb_session(output_dir: Path):
    """A connection with the bounds every merge here runs under: 4 GB, two threads, spilling beside the outputs."""
    import duckdb

    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")
    return con


def fill_statutes_at_large_cite(
    output_dir: Path,
    out_file: Path,
    version_column: str | None,
    identity: tuple[str, ...],
    download_prior: Callable[[str, Path], bool] = r2.download,
) -> int:
    """Fill ``statutes_at_large_cite`` on a merged ``congress_bills`` from the published ``laws`` table.

    A merge-time join of the kind ``pipelines/rollups/base.py`` permits:
    ``laws`` is an ingest rollup's output, read best-effort, and
    ``bill-family``, the writer of ``congress_bills``, declares it in
    ``soft_inputs``. Runs *after* the column-wise merge so the coalesce
    semantics are untouched, and keeps a citation already on the row where
    ``laws`` states none (``laws`` never publishes a captured citation as NULL,
    so nothing is ever cleared). One law per bill: where the route lists two
    law entries for one bill, the larger ``update_date`` wins, then the key.

    Best-effort end to end, which is the ``soft_inputs`` promise: an absent
    ``laws`` table, and equally a corrupt, truncated or column-short one,
    leaves ``out_file`` exactly as :func:`merge_table` wrote it, with the
    failure logged, rather than failing the run after its own merge has
    succeeded. Returns how many rows now carry a citation.
    """
    import duckdb
    from spicy_docs.transport.credentials import scrub_credential

    laws = published_table(output_dir, STATUTES_JOIN_SOURCE, download_prior)
    if laws is None:
        logger.info(
            "{}: no published {} table — {} left as merged",
            STATUTES_JOIN_TARGET,
            STATUTES_JOIN_SOURCE,
            STATUTES_JOIN_COLUMN,
        )
        return 0
    filled_file = output_dir / f"_{STATUTES_JOIN_TARGET}_cited.parquet"
    order_by = f"{version_column} DESC, {', '.join(identity)}" if version_column else ", ".join(identity)
    con = None
    try:
        con = _duckdb_session(output_dir)
        con.execute(
            f"""
            COPY (
                SELECT b.* REPLACE (COALESCE(l.{STATUTES_JOIN_COLUMN}, b.{STATUTES_JOIN_COLUMN}) AS {STATUTES_JOIN_COLUMN})
                FROM read_parquet('{out_file}') b
                LEFT JOIN (
                    SELECT {STATUTES_JOIN_KEY}, {STATUTES_JOIN_COLUMN} FROM read_parquet('{laws}')
                    WHERE {STATUTES_JOIN_KEY} IS NOT NULL AND {STATUTES_JOIN_COLUMN} IS NOT NULL
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY {STATUTES_JOIN_KEY} ORDER BY update_date DESC, law_id) = 1
                ) l USING ({STATUTES_JOIN_KEY})
                ORDER BY {order_by}
            ) TO '{filled_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
            """
        )
        cited = con.execute(
            f"SELECT count(*) FROM read_parquet('{filled_file}') WHERE {STATUTES_JOIN_COLUMN} IS NOT NULL"
        ).fetchone()
        count = int(cited[0]) if cited else 0
        filled_file.replace(out_file)
    except (duckdb.Error, OSError) as error:
        logger.warning(
            "{}: joining {} failed — {} left as merged: {}",
            STATUTES_JOIN_TARGET,
            STATUTES_JOIN_SOURCE,
            STATUTES_JOIN_COLUMN,
            scrub_credential(str(error), ""),
        )
        filled_file.unlink(missing_ok=True)
        return 0
    finally:
        if con is not None:
            con.close()
        laws.unlink(missing_ok=True)
    logger.info(
        "{}: {:,} rows carry a {} after joining {}",
        STATUTES_JOIN_TARGET,
        count,
        STATUTES_JOIN_COLUMN,
        STATUTES_JOIN_SOURCE,
    )
    return count


def merge_contract_table(
    output_dir: Path,
    contract_name: str,
    rows: Iterable[Mapping[str, object]],
    *,
    download_prior: Callable[[str, Path], bool] = r2.download,
    prior_present: bool | None = None,
    replace_parents: ReplacementScope | None = None,
    backfill_prior: Mapping[str, str] | None = None,
    parquet_metadata: Mapping[str, str] | None = None,
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
    out_file = merge_table(
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
        replace_parents=replace_parents,
        backfill_prior=backfill_prior,
        parquet_metadata=parquet_metadata,
    )
    if contract_name == STATUTES_JOIN_TARGET:
        fill_statutes_at_large_cite(output_dir, out_file, contract.version_column, contract.identity, download_prior)
    return out_file


def retire_prior_rows(prior_file: Path, **scope: str) -> int:
    """Drop from a downloaded prior table every row matching all of ``scope``, returning how many went.

    For a table that is a *snapshot* rather than an accumulation — today's
    committee seats, one session's classification page — a row the publisher
    no longer lists must not linger from an earlier capture, and
    :func:`merge_table` alone keeps every prior row. Rewriting the prior
    scratch file without the scope's rows before the merge makes that merge
    "replace the scope", and leaves every other scope's rows standing (an
    earlier Congress keeps its last capture). ``scope`` names columns of the
    prior table; values are bound as parameters, and an empty scope or a
    non-snake_case column refuses.
    """
    import duckdb

    if not scope:
        raise ValueError("retire_prior_rows needs at least one scope column")
    for column in scope:
        if not column.isidentifier() or column != column.lower():
            raise ValueError(f"scope column {column!r} is not a snake_case identifier")
    where = " AND ".join(f"{column} = ?" for column in scope)
    values = list(scope.values())
    kept_file = prior_file.with_name(prior_file.stem + "_kept.parquet")
    con = duckdb.connect()
    retired = con.execute(f"SELECT count(*) FROM read_parquet('{prior_file}') WHERE {where}", values).fetchone()
    con.execute(
        f"COPY (SELECT * FROM read_parquet('{prior_file}') WHERE NOT ({where}) OR ({' OR '.join(f'{c} IS NULL' for c in scope)}))"
        f" TO '{kept_file}' (FORMAT PARQUET, COMPRESSION ZSTD)",
        values,
    )
    con.close()
    kept_file.replace(prior_file)
    count = int(retired[0]) if retired else 0
    logger.info("{}: retired {:,} prior row(s) for {}", prior_file.name, count, scope)
    return count
