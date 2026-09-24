#!/usr/bin/env python3
"""Seed the ETL's ``manifest.parquet`` from the ids the published base tables hold.

The fork's scheduled ETL has never completed: with no manifest on R2 each batch
treated its agencies' whole Mirrulations history as new and was cancelled at 60
minutes. A copy of upstream's manifest would be wrong the other way — it lists
keys upstream consumed but never published (VA, USCIS), which the fork would then
skip forever. So the seed names exactly what the fork's own published
``dockets.parquet``, ``documents.parquet`` and ``comments.parquet`` hold: one
Mirrulations key per id, in the manifest's schema (:data:`MANIFEST_SCHEMA`).

Key layout, checked 2026-09-24 against upstream's 29.07M-key manifest (every
comment, all but one docket and one document matched)::

    raw-data/{agency}/{docket}/text-{docket}/{docket|documents|comments}/{id}.json

``docket`` is the record's ``docket_id`` — for a document or comment without one
(146,229 documents), its id minus the last ``-`` segment — and ``agency`` is
``agency_code``, else the docket's leading run before ``-`` or ``_``. A wrong
derivation is fail-safe: a key the mirror does not list never matches, and its
object is read again. The mirror's re-fetch copies (``{id}(1).json`` …) are not
derivable from ids; the first sweep reads them and keeps only strictly newer rows.

Three modes, one per step of ``docs/etl-catalog-seed.md``:

* build (default) — local Parquet in, ``manifest.parquet`` + ``manifest-seed.json``
  out. Writes nothing remote.
* ``--check`` — against the live bucket and catalog, write nothing: the local
  manifest matches its receipt, every published input still has the ETag and
  size it was built from, ``manifest.parquet`` is absent on R2, and every docket
  and comment id the seed marks is in the catalog table the ETL merges into.
* ``--publish`` — ``--check``, then upload ``manifest.parquet`` and read it back
  over the public URL. Needs R2 S3 keys, ``R2_PUBLIC_URL`` and ``R2_CATALOG_*``.

Usage:
    uv run python scripts/seed_manifest_from_published.py --output-dir DIR \\
        --dockets dockets.parquet --documents documents.parquet --comments comments.parquet
    uv run python scripts/seed_manifest_from_published.py --output-dir DIR --check
    uv run python scripts/seed_manifest_from_published.py --output-dir DIR --publish
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from os import getenv
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from loguru import logger

from spicy_docs.transport.credentials import scrub_credential

from spicy_regs.manifest import MANIFEST_FILE, MANIFEST_SCHEMA
from spicy_regs.schemas import COMMENT, DOCKET, DOCUMENT
from spicy_regs.sources import iceberg, r2
from spicy_regs.sources.publication import _missing

RECEIPT = "manifest-seed.json"
PUBLISH_RECEIPT = "manifest-publish.json"
# boto3's multipart part size: the ETag R2 reports for an object uploaded with it.
_ETAG_PART = 8 * 1024 * 1024
_BATCH_ROWS = 500_000

# Mirrulations path segment, published object and id column per record type.
KINDS = {
    "docket": (DOCKET, "dockets.parquet"),
    "documents": (DOCUMENT, "documents.parquet"),
    "comments": (COMMENT, "comments.parquet"),
}
# The two tables the ETL merges through the catalog; documents stay whole-file.
CATALOG_KINDS = {"docket": DOCKET, "comments": COMMENT}

DERIVATION = (
    "raw-data/{agency}/{docket}/text-{docket}/{kind}/{id}.json; docket = docket_id, or for a "
    "document/comment without one its id minus the last '-' segment; agency = agency_code, else "
    "the docket's leading run before '-' or '_'"
)


def _key_sql(kind: str, source: str) -> str:
    """One SELECT yielding ``(kind, key)`` for every non-null id of ``source``."""
    record_type = KINDS[kind][0]
    id_col = record_type.dedup_key
    docket = "docket_id" if kind == "docket" else f"coalesce(docket_id, regexp_replace({id_col}, '-[^-]*$', ''))"
    return f"""
        SELECT '{kind}' AS kind,
               'raw-data/' || coalesce(agency_code, regexp_extract(d, '^([^-_]+)', 1)) || '/' || d
               || '/text-' || d || '/{kind}/' || {id_col} || '.json' AS key
        FROM (SELECT {id_col}, agency_code, {docket} AS d
              FROM read_parquet('{iceberg._sql_str(source)}') WHERE {id_col} IS NOT NULL)
    """


def file_identity(path: Path) -> dict:
    """Size, sha256 and the 8 MiB multipart ETag of one file, in one read."""
    digest = hashlib.sha256()
    parts = []
    with path.open("rb") as handle:
        while chunk := handle.read(_ETAG_PART):
            digest.update(chunk)
            parts.append(hashlib.md5(chunk).digest())
    # A single-part upload's ETag is the file's MD5; a multipart one hashes the part MD5s.
    etag = parts[0].hex() if len(parts) == 1 else f"{hashlib.md5(b''.join(parts)).hexdigest()}-{len(parts)}"
    return {"bytes": path.stat().st_size, "sha256": "sha256:" + digest.hexdigest(), "etag": f'"{etag}"'}


def build(output_dir: Path, sources: dict[str, Path]) -> dict:
    """Write the seed manifest and its receipt; return the receipt."""
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = output_dir / MANIFEST_FILE
    con = duckdb.connect()
    try:
        con.execute(
            "CREATE TEMP TABLE seed AS "
            + " UNION ALL ".join(_key_sql(kind, str(path)) for kind, path in sources.items())
        )
        inputs = {}
        for kind, path in sources.items():
            id_col = KINDS[kind][0].dedup_key
            [(rows, ids)] = con.execute(
                f"SELECT count(*), count({id_col}) FROM read_parquet('{iceberg._sql_str(str(path))}')"
            ).fetchall()
            [(keys,)] = con.execute("SELECT count(*) FROM seed WHERE kind = ?", [kind]).fetchall()
            inputs[kind] = {
                "object": KINDS[kind][1],
                "path": str(path),
                **file_identity(path),
                "rows": rows,
                "null_ids": rows - ids,
                "keys": keys,
            }
        [(total, distinct)] = con.execute("SELECT count(*), count(DISTINCT key) FROM seed").fetchall()
        if total != distinct:
            raise RuntimeError(f"{total - distinct} derived keys repeat; an id is held twice")
        # Sorted, so the same inputs always produce the same bytes.
        reader = con.execute("SELECT key FROM seed ORDER BY key").to_arrow_reader(_BATCH_ROWS)
        with pq.ParquetWriter(manifest, MANIFEST_SCHEMA, compression="zstd") as writer:
            for batch in reader:
                writer.write_table(pa.Table.from_batches([batch]).cast(MANIFEST_SCHEMA))
    finally:
        con.close()

    metadata = pq.ParquetFile(manifest).metadata
    receipt = {
        "observed_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "script": {
            "path": "scripts/seed_manifest_from_published.py",
            "sha256": "sha256:" + hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        },
        "derivation": DERIVATION,
        "inputs": inputs,
        "output": {
            "path": str(manifest),
            **file_identity(manifest),
            "keys": metadata.num_rows,
            "distinct_keys": distinct,
            "row_groups": metadata.num_row_groups,
            "schema": MANIFEST_SCHEMA.to_string(),
            "created_by": metadata.created_by,
        },
        "remote_writes": 0,
        "publication": False,
    }
    (output_dir / RECEIPT).write_text(json.dumps(receipt, indent=2) + "\n")
    return receipt


def _head(client, bucket: str, key: str) -> dict | None:
    from botocore.exceptions import ClientError

    try:
        return client.head_object(Bucket=bucket, Key=key)
    except ClientError as error:
        if _missing(error):
            return None
        raise


def check(output_dir: Path) -> list[str]:
    """Every reason the seed must not be published now (empty means publishable).

    Each finding is logged as it is made, so the R2 results stand even when the
    catalog cannot be reached. That failure, and a missing catalog table, are
    reported as problems rather than raised.
    """
    receipt = json.loads((output_dir / RECEIPT).read_text())
    manifest = output_dir / MANIFEST_FILE
    problems: list[str] = []

    def refuse(problem: str) -> None:
        logger.error(problem)
        problems.append(problem)

    digest = file_identity(manifest)["sha256"]
    if digest != receipt["output"]["sha256"]:
        refuse(f"{manifest} is {digest}, not the {receipt['output']['sha256']} its receipt describes")
    else:
        logger.info("{} matches its receipt ({})", manifest, digest)

    bucket = getenv("R2_BUCKET_NAME", "spicy-regs")
    client = r2.get_r2_client()
    for built in receipt["inputs"].values():
        live = _head(client, bucket, built["object"])
        if live is None or (live["ETag"], live["ContentLength"]) != (built["etag"], built["bytes"]):
            found = "absent" if live is None else f"{live['ETag']} ({live['ContentLength']:,} bytes)"
            refuse(f"{built['object']} is now {found}, not the {built['etag']} the seed was built from")
        else:
            logger.info("{} is still {} ({:,} bytes)", built["object"], built["etag"], built["bytes"])
    if _head(client, bucket, MANIFEST_FILE) is not None:
        refuse(f"{MANIFEST_FILE} already exists on R2; the seed only bootstraps an absent checkpoint")
    else:
        logger.info("{} is absent on R2", MANIFEST_FILE)

    try:
        con = iceberg._connect()
    except Exception as error:  # noqa: BLE001 — a finding, reported with the others
        refuse(f"the R2 Data Catalog cannot be reached: {_reason(error)}")
        return problems
    try:
        con.execute(
            f"CREATE TEMP TABLE seed AS SELECT regexp_extract(key, '/([a-z]+)/([^/]+)\\.json$', ['kind', 'id']) "
            f"AS k FROM read_parquet('{iceberg._sql_str(str(manifest))}')"
        )
        for kind, record_type in CATALOG_KINDS.items():
            table = iceberg._qualified(record_type)
            try:
                rows, ids = con.execute(
                    f'SELECT count(*), count(DISTINCT "{record_type.dedup_key}") FROM {table}'
                ).fetchone()
            except duckdb.Error as error:
                refuse(f"catalog table {table} is missing or unreadable; seed it first: {_reason(error)}")
                continue
            absent = con.execute(
                f"SELECT count(*) FROM seed s WHERE s.k.kind = '{kind}' AND NOT EXISTS "
                f'(SELECT 1 FROM {table} t WHERE t."{record_type.dedup_key}" = s.k.id)'
            ).fetchone()[0]
            seeded = receipt["inputs"][kind]["keys"]
            logger.info(
                "catalog {}: {:,} rows, {:,} ids; seed marks {:,}, {:,} absent from the catalog",
                record_type.name,
                rows,
                ids,
                seeded,
                absent,
            )
            if absent:
                refuse(f"{absent:,} of the seed's {seeded:,} {record_type.name} ids are not in the catalog")
    finally:
        con.close()
    return problems


def _reason(error: Exception) -> str:
    """An exception as one scrubbed, bounded line: it may render the catalog endpoint or token."""
    return scrub_credential(f"{type(error).__name__}: {error}", getenv("R2_CATALOG_TOKEN", "")).splitlines()[0][:300]


def publish(output_dir: Path) -> dict:
    """Upload the checked seed, read it back over the public URL, and record both."""
    manifest = output_dir / MANIFEST_FILE
    r2.upload_file(manifest, remote_key=MANIFEST_FILE)
    live = _head(r2.get_r2_client(), getenv("R2_BUCKET_NAME", "spicy-regs"), MANIFEST_FILE)
    with TemporaryDirectory() as scratch:
        readback = Path(scratch) / MANIFEST_FILE
        if not r2.download_from_r2(MANIFEST_FILE, readback):
            raise RuntimeError(f"{MANIFEST_FILE} was uploaded but the public URL does not serve it")
        served = file_identity(readback)
    local = file_identity(manifest)
    record = {
        "observed_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "key": MANIFEST_FILE,
        "local": local,
        "etag": live["ETag"] if live else None,
        "public_download_matches": served["sha256"] == local["sha256"],
    }
    (output_dir / PUBLISH_RECEIPT).write_text(json.dumps(record, indent=2) + "\n")
    if not record["public_download_matches"]:
        raise RuntimeError("The public manifest.parquet differs from the local seed")
    return record


def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output-dir", type=Path, required=True, help="Where the seed and its receipt live")
    for _, name in KINDS.values():
        parser.add_argument(f"--{name.removesuffix('.parquet')}", type=Path, help=f"Local copy of {name} (build)")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true", help="Verify against the live bucket and catalog only")
    mode.add_argument("--publish", action="store_true", help="Verify, then upload the seed to R2")
    args = parser.parse_args()

    if args.check or args.publish:
        problems = check(args.output_dir)
        if problems:
            logger.error("Not publishable: {} problem(s) above", len(problems))
            return 1
        logger.info("Seed is publishable: inputs unchanged, manifest absent, every id in the catalog")
        if args.publish:
            record = publish(args.output_dir)
            logger.info("Published {} (ETag {}); public download matches", MANIFEST_FILE, record["etag"])
        return 0

    sources = {kind: getattr(args, name.removesuffix(".parquet")) for kind, (_, name) in KINDS.items()}
    if missing := [f"--{KINDS[kind][1].removesuffix('.parquet')}" for kind, path in sources.items() if path is None]:
        parser.error("build needs " + ", ".join(missing))
    receipt = build(args.output_dir, sources)
    counts = {kind: item["keys"] for kind, item in receipt["inputs"].items()}
    logger.info(
        "Seed {}: {:,} keys {} ({})",
        receipt["output"]["path"],
        receipt["output"]["keys"],
        counts,
        receipt["output"]["sha256"],
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
