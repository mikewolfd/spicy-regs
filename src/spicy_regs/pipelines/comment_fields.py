"""Fill the comment fields a catalog row predates by re-reading every Mirrulations comment (owner decision 2026-09-28).

The catalog replaces a row only when its ``modify_date`` moves, and the manifest skips keys already read, so rows
ingested before a field was retained keep it NULL: the four comment-reference columns and ``subtype`` and
``duplicate_comments`` (``iceberg._COMMENT_ADDED_COLUMNS``). This re-reads each comment object once and fills them.

Two phases, so the long read holds no lock and the catalog write holds it briefly:

* ``plan`` + ``read`` touch no catalog. ``plan`` splits the ETL manifest's comment keys into fixed chunks per agency;
  ``read`` fetches each chunk through the ETL's Mirrulations reader and writes one Parquet part per chunk, keeping
  only the object key, ``comment_id``, ``agency_code``, ``modify_date`` and the fill columns. A part is written whole
  or not at all, so a rerun reads only the chunks that have no part, and a chunk with transport failures stays
  unwritten for the next run. Answers that are not transport failures (unreadable or empty objects) are kept in the
  chunk's journal line.
* ``write`` (see :func:`fill_catalog`) runs under the catalog lock.
"""

from __future__ import annotations

import hashlib
import json
import resource as rusage
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import duckdb
import polars as pl
from cyclopts import App
from loguru import logger

from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg

#: The columns a fill writes: exactly the nullable columns added after the catalog table was created.
FILL_COLUMNS = iceberg._COMMENT_ADDED_COLUMNS
#: Keys per chunk: the resume unit, about 17 s of reads at the measured rate.
CHUNK_KEYS = 20_000
PART_SCHEMA = {"key": pl.Utf8, "comment_id": pl.Utf8, "agency_code": pl.Utf8, "modify_date": pl.Utf8,
               **{column: COMMENT.schema[column] for column in FILL_COLUMNS}}


def _sql(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def plan(workdir: Path, manifest: Path, *, chunk_keys: int = CHUNK_KEYS) -> Path:
    """Add the manifest's comment keys that no earlier plan holds, as new fixed chunks; return the plan file.

    A plan is immutable and named by the manifest's digest, so a rerun on the same manifest is a no-op and a newer
    manifest adds only the keys ingested since. Chunks are contiguous in key order within an agency directory.
    """
    digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
    plans = workdir / "plan"
    target = plans / f"{digest[:16]}.parquet"
    if target.exists():
        return target
    plans.mkdir(parents=True, exist_ok=True)
    earlier = sorted(plans.glob("*.parquet"))
    temporary = target.with_suffix(".tmp")
    with duckdb.connect() as con:
        con.execute("SET preserve_insertion_order=false")
        known = f"SELECT key FROM read_parquet([{', '.join(map(_sql, earlier))}])" if earlier else "SELECT NULL::VARCHAR AS key WHERE false"
        con.execute(f"""
            COPY (
                SELECT key, split_part(key, '/', 2) AS agency,
                       (row_number() OVER (PARTITION BY split_part(key, '/', 2) ORDER BY key) - 1) // {int(chunk_keys)} AS chunk
                FROM read_parquet({_sql(manifest)})
                WHERE key LIKE '%/comments/%' AND key LIKE '%.json' AND key NOT IN ({known})
                ORDER BY agency, chunk, key
            ) TO {_sql(temporary)} (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {int(chunk_keys)})
        """)
        keys, chunks = con.execute(
            f"SELECT count(*), count(DISTINCT (agency, chunk)) FROM read_parquet({_sql(temporary)})").fetchall()[0]
    temporary.replace(target)
    (plans / f"{digest[:16]}.json").write_text(json.dumps(
        {"manifest": str(manifest), "manifest_sha256": digest, "keys": keys, "chunks": chunks, "chunk_keys": chunk_keys,
         "earlier_plans": [p.name for p in earlier]}, indent=2) + "\n")
    logger.info("plan {}: {:,} new comment keys in {:,} chunks", target.name, keys, chunks)
    return target


def _chunks(workdir: Path, agencies: set[str] | None) -> Iterator[tuple[str, str, int, list[str]]]:
    """Every planned chunk as ``(plan id, agency, chunk, keys)``, in plan, agency and chunk order."""
    for plan_file in sorted((workdir / "plan").glob("*.parquet")):
        with duckdb.connect() as con:
            index = con.execute(f"SELECT DISTINCT agency, chunk FROM {_sql(plan_file)} ORDER BY 1, 2").fetchall()
            for agency, chunk in index:
                if agencies is not None and agency not in agencies:
                    continue
                keys = [row[0] for row in con.execute(
                    f"SELECT key FROM {_sql(plan_file)} WHERE agency = ? AND chunk = ? ORDER BY key", [agency, chunk]
                ).fetchall()]
                yield plan_file.stem, agency, chunk, keys


def part_path(workdir: Path, plan_id: str, agency: str, chunk: int) -> Path:
    return workdir / "parts" / f"agency={agency}" / f"{plan_id}-{chunk:05d}.parquet"


def _peak_rss_mb() -> float:
    peak = rusage.getrusage(rusage.RUSAGE_SELF).ru_maxrss
    return peak / 2**20 if sys.platform == "darwin" else peak / 2**10


def read(
    workdir: Path, *, workers: int = 64, agencies: list[str] | None = None, max_chunks: int | None = None,
    max_rss_mb: float = 2048, resource=None, progress_name: str = "progress.json",
) -> dict:
    """Read every planned chunk that has no part yet; return this run's counts.

    ``agencies`` restricts the run (a shard); ``resource`` replaces the anonymous S3 resource in tests. The process
    stops cleanly after the chunk that takes its peak RSS past ``max_rss_mb``; a rerun resumes after it.
    """
    from spicy_docs.sources import mirrulations

    from spicy_regs.pipelines.regulations_state import source_record_type

    resource = resource or mirrulations.s3_resource(max_pool_connections=workers)
    source = source_record_type(COMMENT)
    journal = workdir / "journal.jsonl"
    progress = workdir / progress_name
    totals: dict = {"chunks": 0, "keys": 0, "rows": 0, "unresolved": 0, "deferred_chunks": 0, "stopped": None}
    started = time.monotonic()
    wanted = set(agencies) if agencies is not None else None
    for plan_id, agency, chunk, keys in _chunks(workdir, wanted):
        target = part_path(workdir, plan_id, agency, chunk)
        if target.exists():
            continue
        if max_chunks is not None and totals["chunks"] + totals["deferred_chunks"] >= max_chunks:
            break
        begun = time.monotonic()
        reader = mirrulations.MirrulationsReader(
            resource, mirrulations.BUCKET, mirrulations.PREFIX, agency, source,
            key_lister=lambda keys=keys: keys, download_workers=workers,
        )
        rows = []
        for keyed in reader.iter_keyed_records():
            row = COMMENT.extract(keyed.payload)
            rows.append({"key": keyed.key, **{column: row[column] for column in PART_SCHEMA if column != "key"}})
        transport = [o for o in reader.unresolved if o.status == mirrulations.STATUS_TRANSPORT]
        record = {"plan": plan_id, "agency": agency, "chunk": chunk, "keys": len(keys), "rows": len(rows),
                  "seconds": round(time.monotonic() - begun, 2),
                  "unresolved": [{"key": o.key, "status": o.status} for o in reader.unresolved]}
        if transport:
            # Unwritten, so the next run reads the whole chunk again; nothing it holds is lost or doubled.
            totals["deferred_chunks"] += 1
            logger.warning("{} chunk {}: {} keys failed in transport; deferred", agency, chunk, len(transport))
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(target.name + ".tmp")
        pl.DataFrame(rows, schema=PART_SCHEMA, orient="row").write_parquet(temporary, compression="zstd")
        temporary.replace(target)
        with journal.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record) + "\n")
        totals["chunks"] += 1
        totals["keys"] += len(keys)
        totals["rows"] += len(rows)
        totals["unresolved"] += len(reader.unresolved)
        elapsed = time.monotonic() - started
        progress.write_text(json.dumps({**totals, "seconds": round(elapsed, 1),
                                        "keys_per_second": round(totals["keys"] / elapsed, 1) if elapsed else None,
                                        "last": {k: v for k, v in record.items() if k != "unresolved"},
                                        "peak_rss_mb": round(_peak_rss_mb()),
                                        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())},
                                       indent=2) + "\n")
        if _peak_rss_mb() > max_rss_mb:
            totals["stopped"] = f"peak RSS {_peak_rss_mb():.0f} MB above {max_rss_mb:.0f} MB"
            logger.warning("stopping after {} chunk {}: {}", agency, chunk, totals["stopped"])
            break
    return totals


def status(workdir: Path) -> dict:
    """Planned and read chunks and keys, from the plans and the parts on disk."""
    with duckdb.connect() as con:
        plans = sorted((workdir / "plan").glob("*.parquet"))
        if not plans:
            return {"planned_keys": 0, "planned_chunks": 0, "read_chunks": 0, "read_keys": 0}
        planned = con.execute(f"""
            SELECT agency, chunk, split_part(filename, '/', -1) AS plan_file, count(*) AS n
            FROM read_parquet([{', '.join(map(_sql, plans))}], filename=true) GROUP BY ALL""").fetchall()
    done = {(path.parent.name.removeprefix("agency="), path.stem) for path in (workdir / "parts").glob("agency=*/*.parquet")}
    read_keys = sum(n for agency, chunk, plan_file, n in planned
                    if (agency, f"{plan_file.removesuffix('.parquet')}-{chunk:05d}") in done)
    return {"planned_keys": sum(row[3] for row in planned), "planned_chunks": len(planned),
            "read_chunks": len(done), "read_keys": read_keys}


def planned_agencies(workdir: Path) -> list[str]:
    plans = sorted((workdir / "plan").glob("*.parquet"))
    with duckdb.connect() as con:
        return [row[0] for row in con.execute(
            f"SELECT DISTINCT agency FROM read_parquet([{', '.join(map(_sql, plans))}]) ORDER BY 1").fetchall()]


app = App(name="fill-comment-fields", help=__doc__)


@app.command(name="plan")
def plan_command(*, workdir: Path, manifest: Path, chunk_keys: int = CHUNK_KEYS) -> None:
    """Add a manifest's unplanned comment keys as new chunks."""
    plan(workdir, manifest, chunk_keys=chunk_keys)


@app.command(name="read")
def read_command(
    *, workdir: Path, workers: int = 64, shard: int | None = None, shards: int | None = None,
    max_rss_mb: float = 2048,
) -> None:
    """Read the unread chunks, or one ``--shard`` of ``--shards`` (agencies dealt round-robin)."""
    from spicy_regs.pipelines.attributes_sweep import shard_agencies

    if (shard is None) != (shards is None):
        raise ValueError("--shard and --shards go together")
    agencies = shard_agencies(planned_agencies(workdir), shard, shards) if shard is not None and shards else None
    name = f"progress-{shard}.json" if shard is not None else "progress.json"
    totals = read(workdir, workers=workers, agencies=agencies, max_rss_mb=max_rss_mb, progress_name=name)
    print(json.dumps(totals))
    if totals["deferred_chunks"] or totals["stopped"]:
        raise SystemExit(3)


@app.command(name="status")
def status_command(*, workdir: Path) -> None:
    """Planned and read chunks and keys."""
    print(json.dumps(status(workdir), indent=2))


if __name__ == "__main__":
    app()
