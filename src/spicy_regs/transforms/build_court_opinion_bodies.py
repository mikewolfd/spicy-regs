"""Transform: build ``court_opinion_bodies.parquet`` — the actual opinion text.

Every CourtListener source text variant is retained under its own field name
(HTML and XML stay markup, not extracted plain text), ``plain_text`` and
``html_with_citations`` keep their existing positions for named consumers, and
version 2 preserves source empty strings separately from NULL in all eight text
fields. The Parquet metadata records the version; a version-1 prior requires an
explicit rebuild because its discarded variants cannot be recovered by merging.

**One opinion per row, not one decision per row.** A cluster (see
``build_court_opinion_clusters``) is the decision; an opinion is one voice
within it — majority, concurrence, dissent — with its own author and text.
``cluster_id`` joins the two, and through the cluster's ``cl_docket_id`` to
``court_dockets``.

**The ingest is bounded because the dump is 50.8 GiB compressed (~8.3x, about
422 GiB decompressed) and one connection is served at ~1.7-2.0 MiB/s.** Remote
input is streamed without keeping a compressed copy and a retained local dump is
streamed through decompression, but both routes check estimated output space
before an unbounded pass and refuse to cross the 100 GiB free-space floor (see
``check_headroom``). A bounded run instead takes a *recorded slice*
(``max_records`` and/or ``max_compressed_bytes``) and logs exactly what the
bound produced — rows, id range, date range, bytes read — so coverage is a
stated number rather than an impression; ``cluster_ids`` narrows a pass to
specific decisions, which is how the documented full backfill targets the APA
docket set without keeping the other ~10 million opinions.
"""

from __future__ import annotations

import shutil
from collections.abc import Container, Generator, Iterable, Iterator, Sized
from contextlib import closing
from datetime import date
import hashlib
from pathlib import Path
import re
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.transforms._courtlistener_writer import CourtListenerTableWriter


OUTPUT = "court_opinion_bodies.parquet"
DATASET = "opinions"
SCHEMA_VERSION = "2"
SCHEMA_VERSION_KEY = "spicy-regs:court-opinion-bodies-schema-version"

#: Free space this project refuses to eat into, in bytes. A bulk ingest that
#: would cross it is stopped and recorded rather than run.
DISK_HEADROOM_FLOOR = 100 * 2**30

#: Planning estimate, not a per-row bound. The version-2 native replay retained
#: all eight variants: 3,626,326 bytes / 284 rows = 12,769 B/row, rounded up.
#: The old two-variant 8 KiB estimate no longer covers even this bounded sample.
BYTES_PER_OPINION_ROW = 16 * 2**10

#: Opinions per cluster, used as a ceiling when sizing a ``cluster_ids`` pass.
#: The most any single cluster carried in that same 250,000-row build was 8
#: (majority, concurrences, dissents, and the odd combined rendering); doubling
#: it keeps the estimate an over-estimate, which is the only direction a disk
#: guard may err in.
OPINIONS_PER_CLUSTER_CEILING = 16

#: Rows buffered before each parquet batch write. Opinion bodies are large, so
#: this batch is much smaller than the cluster builder's.
BATCH_ROWS = 2_000

#: Bound encoded values as well as row count for remote batches. A single
#: larger source-bounded record is emitted alone, preserving complete rows.
REMOTE_BATCH_TEXT_BYTES = 64 * 2**20

#: The raw dump columns that can hold a body. CourtListener populates whichever
#: one the upstream source provided, so "has text" is a question about the set,
#: not about ``plain_text`` alone.
_TEXT_FIELDS = (
    "plain_text",
    "html",
    "html_lawbox",
    "html_columbia",
    "html_anon_2020",
    "html_with_citations",
    "xml_harvard",
    "xml_scan",
)

COLUMNS = (
    "opinion_id",
    "cluster_id",
    "opinion_type",
    "author_str",
    "author_id",
    "joined_by_str",
    "per_curiam",
    "sha1",
    "page_count",
    "download_url",
    "local_path",
    "extracted_by_ocr",
    "plain_text",
    "html_with_citations",
    "available_text_fields",
    "text_char_count",
    "date_created",
    "date_modified",
    "dump_date",
    "html",
    "html_lawbox",
    "html_columbia",
    "html_anon_2020",
    "xml_harvard",
    "xml_scan",
)
_SCHEMA = pa.schema(
    [(c, pa.string()) for c in COLUMNS],
    metadata={SCHEMA_VERSION_KEY.encode(): SCHEMA_VERSION.encode()},
)


def _s(value: object) -> str | None:
    """Metadata policy: empty strings and missing values are both NULL."""
    if value is None:
        return None
    text = str(value)
    return text if text != "" else None


def check_headroom(needed_bytes: int, *, path: Path | None = None) -> None:
    """Refuse an ingest that would take free space below the project floor.

    Raises rather than warning: a run that silently fills the disk is worse than
    a run that did not happen, and the whole point of recording sizes first is to
    be able to make this call before the bytes arrive.
    """
    usage = shutil.disk_usage(path or Path.home())
    remaining = usage.free - needed_bytes
    if remaining < DISK_HEADROOM_FLOOR:
        raise RuntimeError(
            f"CourtListener bulk: refusing to ingest {needed_bytes / 2**30:.1f} GiB — "
            f"would leave {remaining / 2**30:.1f} GiB free, below the "
            f"{DISK_HEADROOM_FLOOR / 2**30:.0f} GiB floor "
            f"(currently {usage.free / 2**30:.1f} GiB free)"
        )


def estimate_output_bytes(dump_size: int, cluster_ids: Container[str] | None) -> int:
    """Estimate output bytes for planning an unbounded pass.

    The dump is *streamed* — decompressed inline, never landed — so what the
    volume pays for is the parquet this run writes, not the 50.8 GiB it reads.
    For an unfiltered pass, use twice the compressed dump size: the version-2
    native prefix produced 3.63 MB of Parquet from a 2 MiB compressed capture.
    This is a planning estimate, not a full-population size guarantee. For a
    ``cluster_ids`` pass they are nothing alike: the same 8.6 hours of reading
    produces a table sized by the *targets*, and charging it 50.8 GiB refuses a
    run that costs megabytes.

    An unsized filter uses that same unfiltered estimate.
    """
    if cluster_ids is None or not isinstance(cluster_ids, Sized):
        return 2 * dump_size
    return len(cluster_ids) * OPINIONS_PER_CLUSTER_CEILING * BYTES_PER_OPINION_ROW


def _shape(row: dict, *, dump_date: date | None) -> dict:
    """Map one bulk ``opinions`` CSV row onto the published columns."""
    text: dict[str, str | None] = {}
    for name in _TEXT_FIELDS:
        value = row.get(name)
        if value is not None and not isinstance(value, str):
            raise TypeError(f"CourtListener opinion {name} must be a string or NULL")
        text[name] = value
    present = [name for name, value in text.items() if value]
    longest = max((len(value) for value in text.values() if value), default=0)
    return {
        "opinion_id": _s(row.get("id")),
        "cluster_id": _s(row.get("cluster_id")),
        "opinion_type": _s(row.get("type")),
        "author_str": _s(row.get("author_str")),
        "author_id": _s(row.get("author_id")),
        "joined_by_str": _s(row.get("joined_by_str")),
        "per_curiam": _s(row.get("per_curiam")),
        "sha1": _s(row.get("sha1")),
        "page_count": _s(row.get("page_count")),
        "download_url": _s(row.get("download_url")),
        "local_path": _s(row.get("local_path")),
        "extracted_by_ocr": _s(row.get("extracted_by_ocr")),
        **text,
        "available_text_fields": ",".join(present) if present else None,
        "text_char_count": str(longest),
        "date_created": _s(row.get("date_created")),
        "date_modified": _s(row.get("date_modified")),
        "dump_date": dump_date.isoformat() if dump_date else None,
    }


def _remote_opinion_batches(rows: Iterable[dict], *, dump_date: date) -> Iterator[pa.Table]:
    """Keep row groups useful without accumulating unbounded opinion text."""
    pending: list[dict] = []
    text_bytes = 0
    for raw in rows:
        row = _shape(raw, dump_date=dump_date)
        row_bytes = sum(len(value.encode("utf-8")) for value in row.values() if value is not None)
        if pending and (len(pending) >= BATCH_ROWS or text_bytes + row_bytes > REMOTE_BATCH_TEXT_BYTES):
            table = pa.Table.from_pylist(pending, schema=_SCHEMA)
            pending = []
            text_bytes = 0
            yield table
            del table
        pending.append(row)
        text_bytes += row_bytes
    if pending:
        yield pa.Table.from_pylist(pending, schema=_SCHEMA)


def stage_court_opinion_bodies_remote(
    *, client, bucket: str, key: str, local_file: Path, source_sha256: str,
    dump_date: date, max_output_bytes: int,
):
    """Stream a complete pinned local original to an unpublished remote table.

    This path uses the same source reader, mapping and version-2 schema as the
    local builder. It has no source prefix/row cap and creates no local output
    copy. Generation admission, full native audits, prior-population comparison
    and publication remain separate required steps.
    """
    from spicy_docs.sources.courtlistener.bulk import CourtListenerBulkReader
    from spicy_regs.sources.remote_parquet import write_remote_parquet

    if not re.fullmatch(r"sha256:[0-9a-f]{64}", source_sha256):
        raise ValueError("A complete source SHA-256 pin is required")
    if local_file.is_symlink() or not local_file.is_file():
        raise ValueError("Retained opinion input must be a regular file")

    def identity():
        stat = local_file.stat()
        return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns

    before = identity()
    with local_file.open("rb") as stream:
        actual = "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()
    if actual != source_sha256 or identity() != before:
        raise ValueError("Retained opinion source differs from its pin")
    reader = CourtListenerBulkReader(DATASET, dump_date=dump_date, local_file=local_file)

    def batches():
        with closing(cast(Generator[dict, None, None], reader.iter_records())) as source_rows:
            yield from _remote_opinion_batches(source_rows, dump_date=dump_date)
        if (identity() != before or reader.compressed_bytes != before[2]
                or reader.rows_scanned != reader.rows_yielded):
            raise ValueError("Retained opinion source changed or was not fully consumed")

    return write_remote_parquet(
        client=client, bucket=bucket, key=key, schema=_SCHEMA, batches=batches(), max_bytes=max_output_bytes,
    )


def build_court_opinion_bodies(
    output_dir: Path,
    *,
    dump_date: date | None = None,
    local_file: Path | None = None,
    max_records: int | None = None,
    max_compressed_bytes: int | None = None,
    cluster_ids: Container[str] | None = None,
    rebuild: bool = False,
) -> Path:
    """Build ``court_opinion_bodies.parquet`` from a bounded slice of the dump.

    Returns the written path. The exact bound reached is logged and is the number
    that belongs in the coverage record — this builder never claims completeness
    it did not achieve. ``rebuild=True`` skips prior download/merge, including a
    legacy prior, and replaces this output with only the explicitly selected
    source scope. Use a fresh output directory to retain the old generation.
    """
    import duckdb

    from spicy_docs.sources.courtlistener.bulk import (
        CourtListenerBulkReader,
        find_dump,
        latest_dump_date,
        list_bulk_dumps,
    )

    out_file = output_dir / OUTPUT
    prior_file = output_dir / "_bodies_prior.parquet"
    new_file = output_dir / "_bodies_new.parquet"

    have_prior = not rebuild and (prior_file.exists() or r2.download(OUTPUT, prior_file))
    if have_prior:
        prior_schema = pq.read_schema(prior_file)
        if (prior_schema.metadata or {}).get(
            SCHEMA_VERSION_KEY.encode()
        ) != SCHEMA_VERSION.encode() or prior_schema.remove_metadata() != _SCHEMA.remove_metadata():
            raise ValueError(
                "CourtListener opinion bodies require a version-2 prior with all eight text fields; "
                "retain the old artifact and pass rebuild=True for the selected source scope "
                "in a fresh output directory"
            )
    logger.info(
        "Opinion bodies: {}",
        f"merging against prior table {prior_file}" if have_prior else "no prior table — first build",
    )

    if local_file is None:
        objects = list_bulk_dumps()
        resolved = dump_date or latest_dump_date(objects, DATASET)
        if resolved is None:
            raise RuntimeError(f"CourtListener bulk: no published {DATASET} dump found")
        published = find_dump(objects, DATASET, resolved)
        if published is None:
            raise RuntimeError(f"CourtListener bulk: no {DATASET} dump for {resolved}")
        logger.info(
            "Opinion bodies: dump {} is {:.3f} GiB compressed; bound = {} rows / {} bytes / {} cluster ids",
            published.filename,
            published.size / 2**30,
            max_records or "unbounded",
            max_compressed_bytes or "unbounded",
            len(cluster_ids) if isinstance(cluster_ids, Sized) else "unbounded",
        )
        dump_size = published.size
    else:
        resolved = dump_date
        logger.info("Opinion bodies: reading local dump {}", local_file)
        dump_size = local_file.stat().st_size

    # Local originals consume space already, but their output still needs the
    # same admission check as a remote stream. Check the destination volume
    # before opening the bulk reader or writing a staged table.
    if max_records is None and max_compressed_bytes is None:
        needed = estimate_output_bytes(dump_size, cluster_ids)
        logger.info(
            "Opinion bodies: unbounded pass, estimated output {:.3f} GiB",
            needed / 2**30,
        )
        check_headroom(needed, path=output_dir)

    row_filter = None
    if cluster_ids is not None:

        def row_filter(row: dict) -> bool:  # noqa: F811
            return (row.get("cluster_id") or "") in cluster_ids

    writer = CourtListenerTableWriter(new_file, schema=_SCHEMA, batch_size=BATCH_ROWS)
    reader = CourtListenerBulkReader(
        DATASET,
        dump_date=resolved,
        local_file=local_file,
        max_records=max_records,
        max_compressed_bytes=max_compressed_bytes,
        row_filter=row_filter,
    )
    try:
        with closing(cast(Generator[dict, None, None], reader.iter_records())) as source_rows:
            for row in source_rows:
                writer.add(_shape(row, dump_date=resolved))

        writer.close()
    except BaseException:
        try:
            writer.abort()
        except Exception:
            logger.exception("Could not finish cleaning up failed CourtListener staging output")
        raise

    logger.info(
        "Opinion bodies: bound reached — {:,} rows scanned, {:,} kept, {:.3f} GiB compressed read",
        reader.rows_scanned,
        reader.rows_yielded,
        reader.compressed_bytes / 2**30,
    )

    # A first build has no prior table to merge against and one input, whose
    # opinion_id is the publisher's primary key — so the dedup cannot remove a
    # row and the whole COPY is a sort. It is an expensive sort: ranking 250,000
    # rows that each carry kilobytes of opinion text ran the 4 GiB duckdb budget
    # out of memory, and holding the staged and merged copies at once is 3.5 GiB
    # of disk against a 100 GiB floor. So promote the staged file instead, and
    # pay for it in row order.
    if not have_prior:
        new_file.replace(out_file)
        if not rebuild:
            prior_file.unlink(missing_ok=True)
        total = pq.ParquetFile(out_file).metadata.num_rows
        logger.info("Court opinion bodies: {:,} rows (first build, dump order)", total)
        return out_file

    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    cols = ", ".join(COLUMNS)
    if have_prior:
        union = (
            f"SELECT {cols}, 0 AS _src FROM read_parquet('{prior_file}') "
            f"UNION ALL BY NAME "
            f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_file}')"
        )
    else:
        union = f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_file}')"

    con.execute(
        f"""
        COPY (
            SELECT {cols} FROM (
                SELECT {cols}, ROW_NUMBER() OVER (
                    PARTITION BY opinion_id ORDER BY _src DESC
                ) AS _rn
                FROM ({union})
                WHERE opinion_id IS NOT NULL
            )
            WHERE _rn = 1
            ORDER BY CAST(opinion_id AS BIGINT) DESC
        ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 2000, KV_METADATA ?);
        """,
        [{SCHEMA_VERSION_KEY: SCHEMA_VERSION}],
    )
    con.close()

    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("Court opinion bodies: {:,} rows", total)
    return out_file
