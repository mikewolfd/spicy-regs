#!/usr/bin/env python3
"""Take the multi-file design's four measurements (owner decision 64) into one JSON receipt.

1. R2 limits: PUT a 1.1 GiB object in one request, copy it with one ``CopyObject``, compare them.
2. Unchanged share: split the current bill-family generation's ``bill_sections`` and the one it
   replaced by congress (the ``bill_id`` prefix, a function of the identity), and add up the split
   bytes of the congresses whose rows are identical in both directions.
3. Read-back: time reading and hashing every member of the current bill-family generation as
   admission does, then the split members with eight workers.
4. Query latency: time the MCP's view shape (``read_parquet`` over public URLs) against the one
   published file and against the split members, cold, three runs each.

Everything written goes under ``probes/multifile-<run>/`` and is deleted in a ``finally``. Needs
the R2 credentials, and ``R2_PUBLIC_URL`` for the public reads (docs/research/multi-file-tables-2026-09-26.md §5).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import statistics
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

PROBE_BYTES = 1_181_116_006  # 1.1 GiB, just over the design's provisional 1 GiB member cap.
CHUNK = 8 * 1024 * 1024
RUNS = 3


def _timed(action) -> dict:
    """Run ``action`` and report its seconds; a refusal is recorded as the measurement, not raised."""
    started = time.monotonic()
    try:
        action()
        return {"ok": True, "seconds": round(time.monotonic() - started, 2)}
    except Exception as exc:
        return {"ok": False, "seconds": round(time.monotonic() - started, 2), "error": f"{type(exc).__name__}: {exc}"[:400]}


def _download(client, bucket: str, key: str, target: Path) -> dict:
    digest = hashlib.sha256()
    body = client.get_object(Bucket=bucket, Key=key)["Body"]
    with target.open("wb") as fh:
        for chunk in iter(lambda: body.read(CHUNK), b""):
            digest.update(chunk)
            fh.write(chunk)
    return {"bytes": target.stat().st_size, "sha256": f"sha256:{digest.hexdigest()}"}


def _hash_remote(client, bucket: str, key: str) -> int:
    digest, total = hashlib.sha256(), 0
    body = client.get_object(Bucket=bucket, Key=key)["Body"]
    for chunk in iter(lambda: body.read(CHUNK), b""):
        digest.update(chunk)
        total += len(chunk)
    return total


def r2_limits(client, bucket: str, prefix: str, work: Path) -> dict:
    source, copy = f"{prefix}/copy-source.bin", f"{prefix}/copy-target.bin"
    path = work / "probe.bin"
    block = os.urandom(64 * 1024 * 1024)
    with path.open("wb") as fh:
        written = 0
        while written < PROBE_BYTES:
            fh.write(block[: min(len(block), PROBE_BYTES - written)])
            written += min(len(block), PROBE_BYTES - written)
    try:
        with path.open("rb") as fh:
            put = _timed(lambda: client.put_object(Bucket=bucket, Key=source, Body=fh))
        copied = _timed(lambda: client.copy_object(Bucket=bucket, Key=copy, CopySource={"Bucket": bucket, "Key": source}))
        heads = {key: client.head_object(Bucket=bucket, Key=key) for key in (source, copy) if _exists(client, bucket, key)}
        return {
            "bytes": PROBE_BYTES, "single_put": put, "single_copy_object": copied,
            "sizes": {key: head["ContentLength"] for key, head in heads.items()},
            "etags_equal": len({head["ETag"] for head in heads.values()}) == 1 and len(heads) == 2,
        }
    finally:
        path.unlink(missing_ok=True)


def _scalar(con, sql: str):
    row = con.execute(sql).fetchone()
    assert row is not None
    return row[0]


def _exists(client, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def bill_sections(client, bucket: str, public: str, prefix: str, work: Path) -> dict:
    import duckdb

    from spicy_regs.sources import publication as pub

    index = pub.load_index(public)
    entry = index["families"]["bill-family"]
    _, root = pub.load_family_root(public, entry)
    prior = root["spec"]["readSnapshot"]["families"]["bill-family"]
    key = "bill_sections.parquet"
    receipt: dict = {
        "current": entry["artifactDigest"], "replaced": prior["artifactDigest"],
        "carried_forward": sorted(root["spec"].get("carriedForward", {})),
    }

    # 3a. Read-back of the whole current family, as admission reads it.
    started = time.monotonic()
    family_bytes = sum(_hash_remote(client, bucket, f"{entry['prefix']}/{name}") for name in entry["tables"])
    receipt["family_read_back"] = {"members": len(entry["tables"]), "bytes": family_bytes,
                                   "seconds": round(time.monotonic() - started, 2)}

    current, replaced = work / "current.parquet", work / "replaced.parquet"
    receipt["downloads"] = {
        "current": _download(client, bucket, f"{entry['prefix']}/{key}", current),
        "replaced": _download(client, bucket, f"{prior['prefix']}/{key}", replaced),
    }
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute(f"SET temp_directory='{work / 'spill'}'")
    congress = "split_part(bill_id, '-', 1)"
    split = work / "split"
    con.execute(
        f"COPY (SELECT *, {congress} AS congress FROM read_parquet('{current}') ORDER BY bill_id, version_code, source, seq) "
        f"TO '{split}' (FORMAT PARQUET, PARTITION_BY (congress), COMPRESSION ZSTD, ROW_GROUP_SIZE 50000)"
    )
    members = {path.parent.name.removeprefix("congress="): path for path in sorted(split.glob("congress=*/*.parquet"))}

    # 2. Congresses whose rows are identical in both directions.
    unchanged, per_congress = 0, {}
    for number, path in members.items():
        where = f"WHERE {congress} = '{number}'"
        added, dropped = (_scalar(
            con, f"SELECT count(*) FROM (SELECT * FROM read_parquet('{a}') {where} EXCEPT ALL SELECT * FROM read_parquet('{b}') {where})"
        ) for a, b in ((current, replaced), (replaced, current)))
        size = path.stat().st_size
        per_congress[number] = {"bytes": size, "rows_only_current": added, "rows_only_replaced": dropped}
        unchanged += size if added == dropped == 0 else 0
    total = sum(item["bytes"] for item in per_congress.values())
    receipt["unchanged_share"] = {"split_bytes": total, "unchanged_bytes": unchanged,
                                  "share": round(unchanged / total, 4) if total else None, "per_congress": per_congress}

    # 3b and 4. Upload the split members, read them back in parallel, then time the MCP's view shape.
    keys = {number: f"{prefix}/bill_sections/congress={number}.parquet" for number in members}
    for number, path in members.items():
        client.upload_file(str(path), bucket, keys[number])
    started = time.monotonic()
    with ThreadPoolExecutor(8) as pool:
        split_bytes = sum(pool.map(lambda k: _hash_remote(client, bucket, k), keys.values()))
    receipt["split_read_back"] = {"members": len(keys), "bytes": split_bytes, "workers": 8,
                                  "seconds": round(time.monotonic() - started, 2)}

    sample = con.execute(f"SELECT bill_id, version_code FROM read_parquet('{current}') WHERE bill_id LIKE '117-%' LIMIT 1").fetchone()
    assert sample is not None, "bill_sections holds no 117th-Congress row to look up"
    single = f"'{public}/{entry['prefix']}/{key}'"
    many = "[" + ", ".join(f"'{public}/{k}'" for k in keys.values()) + "]"
    queries = {
        "count": "SELECT count(*) FROM v",
        "one_congress": "SELECT count(*) FROM v WHERE bill_id LIKE '118-%'",
        "point": f"SELECT heading, body_chars FROM v WHERE bill_id = '{sample[0]}' AND version_code = '{sample[1]}'",
        "scan": "SELECT version_code, count(*) FROM v GROUP BY 1 ORDER BY 2 DESC LIMIT 5",
    }
    latency = {}
    for shape, source in (("single", single), ("split", many)):
        for name, sql in queries.items():
            runs = []
            for _ in range(RUNS):
                cold = duckdb.connect()
                cold.execute("INSTALL httpfs; LOAD httpfs;")
                cold.execute(f"CREATE VIEW v AS SELECT * FROM read_parquet({source})")
                started = time.monotonic()
                cold.execute(sql).fetchall()
                runs.append(time.monotonic() - started)
                cold.close()
            latency.setdefault(name, {})[shape] = round(statistics.median(runs), 3)
    receipt["query_latency_seconds"] = latency
    con.close()
    return receipt


def main(argv=None) -> int:
    from spicy_regs.sources import r2

    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    bucket, public = os.environ["R2_BUCKET_NAME"], os.environ["R2_PUBLIC_URL"].rstrip("/")
    prefix = f"probes/multifile-{os.environ.get('GITHUB_RUN_ID', os.getpid())}"
    client = r2.get_r2_client()
    receipt: dict = {"prefix": prefix, "started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    try:
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp)
            receipt["r2_limits"] = r2_limits(client, bucket, prefix, work)
            receipt["bill_sections"] = bill_sections(client, bucket, public, prefix, work)
    finally:
        pages = client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{prefix}/")
        leftover = [item["Key"] for page in pages for item in page.get("Contents", ())]
        for key in leftover:
            client.delete_object(Bucket=bucket, Key=key)
        receipt["deleted"] = leftover
        args.output.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(receipt, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
