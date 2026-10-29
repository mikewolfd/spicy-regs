"""Compare local Parquet source inventories with bounded DuckDB memory.

Identity presence and declared version equality are separate results.
Rows never establish inferred original-byte custody.
The small descriptors use the future selected-input manifest's inventory fields.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import resource
import sys
import time

import duckdb


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def identifier(value):
    return '"' + str(value).replace('"', '""') + '"'


def digest(path):
    with path.open("rb") as stream:
        return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()


def load_descriptor(path):
    path = Path(path).resolve()
    spec = json.loads(path.read_text())
    if spec.get("format") != "parquet" or spec.get("role") not in {"expected", "used", "available"}:
        raise ValueError("Inventory requires format=parquet and role expected/used/available")
    files = spec.get("files", [spec["path"]] if "path" in spec else [])
    if not files:
        raise ValueError("Inventory requires path or an explicit files array")
    resolved = [(path.parent / item).resolve() for item in files]
    if len(set(resolved)) != len(resolved):
        raise ValueError("Duplicate inventory files")
    if any(not item.is_file() or not item.name.endswith(".parquet") for item in resolved):
        raise ValueError("Inventory paths must be existing local Parquet files; glob/remote reads are excluded")
    if "sha256" in spec or "bytes" in spec:
        if len(resolved) != 1 or set(spec) & {"sha256", "bytes"} != {"sha256", "bytes"}:
            raise ValueError("Pinned descriptor requires one file, sha256 and bytes")
        if resolved[0].stat().st_size != spec["bytes"] or digest(resolved[0]) != spec["sha256"]:
            raise ValueError("Inventory pin differs")
    columns = spec.get("columns", {})
    if not columns.get("source_id") or not spec.get("asset_kind"):
        raise ValueError("Inventory requires source_id column and asset_kind")
    if columns.get("source_version") and not spec.get("version_kind"):
        raise ValueError("Versioned inventory requires version_kind")
    if not isinstance(spec.get("complete"), bool):
        raise ValueError("Inventory must explicitly state complete=true/false for its declared scope")
    scope = spec.get("scope", {})
    if not scope.get("id") or not isinstance(scope.get("finite"), bool):
        raise ValueError("Inventory requires scope.id and scope.finite")
    if spec["complete"] and not scope["finite"]:
        raise ValueError("Complete inventory requires a finite scope")
    return spec, resolved, {"path": str(path), "sha256": digest(path), "bytes": path.stat().st_size}


def prepare(con, name, spec, files):
    columns = spec["columns"]
    def column(field, dtype):
        return f"CAST({identifier(columns[field])} AS {dtype})" if columns.get(field) else f"NULL::{dtype}"
    predicates = []
    for key, value in spec.get("filters", {}).items():
        if value is None:
            predicates.append(f"{identifier(key)} IS NULL")
        elif isinstance(value, (str, int)) and not isinstance(value, bool):
            predicates.append(f"{identifier(key)} = {literal(value)}")
        else:
            raise ValueError("Filters support string, integer or null equality only")
    where = " WHERE " + " AND ".join(predicates) if predicates else ""
    version_kind = literal(spec.get("version_kind", "")) if columns.get("source_version") else "NULL::VARCHAR"
    con.execute(f"""CREATE TEMP TABLE {name}_rows AS
        SELECT {column('source_id', 'VARCHAR')} AS source_id,
               {literal(spec['asset_kind'])} AS asset_kind,
               NULLIF({column('source_version', 'VARCHAR')}, '') AS source_version,
               {version_kind} AS version_kind, {column('bytes', 'BIGINT')} AS source_bytes
        FROM read_parquet([{','.join(map(literal, files))}], hive_partitioning=false){where}""")
    invalid = con.execute(f"SELECT count(*) FROM {name}_rows WHERE source_id IS NULL OR source_id='' OR source_bytes<0").fetchone()[0]
    if invalid:
        raise ValueError(f"{name} inventory contains {invalid} invalid identity/size rows")
    # Group before joining: duplicate copies cannot multiply the comparison.
    con.execute(f"""CREATE TEMP TABLE {name} AS
        SELECT source_id, asset_kind, count(*) AS input_rows,
               count(DISTINCT (version_kind, source_version, source_bytes)) AS variants,
               min(version_kind) AS version_kind, min(source_version) AS source_version,
               min(source_bytes) AS source_bytes
        FROM {name}_rows GROUP BY source_id, asset_kind""")
    con.execute(f"DROP TABLE {name}_rows")


def compare(expected_path, actual_path, output, *, memory_limit="256MB", threads=2):
    output = Path(output).resolve()
    expected, expected_files, expected_pin = load_descriptor(expected_path)
    actual, actual_files, actual_pin = load_descriptor(actual_path)
    if expected["role"] != "expected" or actual["role"] not in {"used", "available"}:
        raise ValueError("Compare requires expected on the left and used/available on the right")
    if expected["scope"] != actual["scope"]:
        raise ValueError("Inventory scopes differ; explicitly select the same source population first")
    observations = {str(path): (path.stat().st_size, path.stat().st_mtime_ns)
                    for path in (*expected_files, *actual_files)}
    output.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    with duckdb.connect(str(output / "scratch.duckdb")) as con:
        con.execute(f"SET memory_limit={literal(memory_limit)}")
        con.execute(f"SET threads={int(threads)}")
        con.execute("SET preserve_insertion_order=false")
        con.execute(f"SET temp_directory={literal(output / 'spill')}")
        prepare(con, "expected", expected, expected_files)
        prepare(con, "actual", actual, actual_files)
        con.execute("""CREATE TEMP TABLE comparison AS
            SELECT coalesce(e.source_id,a.source_id) AS source_id,
                coalesce(e.asset_kind,a.asset_kind) AS asset_kind,
                e.version_kind AS expected_version_kind, e.source_version AS expected_source_version,
                a.version_kind AS actual_version_kind, a.source_version AS actual_source_version,
                e.source_bytes AS expected_bytes, a.source_bytes AS actual_bytes,
                e.input_rows AS expected_rows, a.input_rows AS actual_rows,
                CASE WHEN e.source_id IS NULL THEN 'unexpected'
                     WHEN a.source_id IS NULL THEN 'missing'
                     ELSE 'present' END AS identity_status,
                CASE WHEN e.source_id IS NULL THEN 'unexpected'
                     WHEN a.source_id IS NULL THEN 'missing'
                     WHEN e.variants<>1 OR a.variants<>1 THEN 'unknown'
                     WHEN e.source_version IS NULL OR a.source_version IS NULL
                       OR e.version_kind<>a.version_kind THEN 'unknown'
                     WHEN e.source_version<>a.source_version THEN 'stale'
                     WHEN e.source_bytes IS NOT NULL AND a.source_bytes IS NOT NULL
                       AND e.source_bytes<>a.source_bytes THEN 'unknown'
                     ELSE 'same' END AS status,
                CASE WHEN e.source_id IS NULL THEN 'outside-expected-scope'
                     WHEN a.source_id IS NULL THEN 'absent-from-selected-actual-inventory'
                     WHEN e.variants<>1 OR a.variants<>1 THEN 'conflicting-versions-for-identity'
                     WHEN e.source_version IS NULL OR a.source_version IS NULL THEN 'version-unrecorded'
                     WHEN e.version_kind<>a.version_kind THEN 'incomparable-version-kinds'
                     WHEN e.source_version<>a.source_version THEN 'selected-version-differs'
                     WHEN e.source_bytes IS NOT NULL AND a.source_bytes IS NOT NULL
                       AND e.source_bytes<>a.source_bytes THEN 'matching-version-but-size-conflicts'
                     ELSE 'identity-and-declared-version-match' END AS reason
            FROM expected e FULL OUTER JOIN actual a USING(source_id,asset_kind)""")
        con.execute(f"COPY comparison TO {literal(output / 'diff.parquet')} (FORMAT PARQUET, COMPRESSION ZSTD)")
        counts = [dict(zip(("asset_kind", "status", "reason", "identities"), row)) for row in con.execute(
            "SELECT asset_kind,status,reason,count(*) FROM comparison GROUP BY ALL ORDER BY ALL").fetchall()]
        identity_counts = [dict(zip(("asset_kind", "status", "identities"), row)) for row in con.execute(
            "SELECT asset_kind,identity_status,count(*) FROM comparison GROUP BY ALL ORDER BY ALL").fetchall()]
        # Equality is qualified only for complete inventories of the same finite scope.
        identity_parity = ("unqualified" if not (expected["complete"] and actual["complete"]) else
                           "different" if any(row["status"] != "present" for row in identity_counts) else "equal")
        measures = {}
        for name in ("expected", "actual"):
            rows, identities, duplicate_identities = con.execute(
                f"SELECT coalesce(sum(input_rows),0),count(*),count(*) FILTER (WHERE input_rows>1) FROM {name}"
            ).fetchone()
            measures[name] = {"input_rows": rows, "identities": identities,
                              "duplicate_identities": duplicate_identities, "duplicate_rows": rows - identities}
        samples = con.execute("SELECT source_id,asset_kind,status,reason,identity_status FROM comparison LIMIT 5").fetchall()
    if any((Path(path).stat().st_size, Path(path).stat().st_mtime_ns) != before
           for path, before in observations.items()):
        raise ValueError("An inventory changed during comparison; result is unqualified")
    summary = {
        "format": "spicy-source-inventory-diff", "version": 1,
        "scope": expected["scope"], "expected_descriptor": expected_pin, "actual_descriptor": actual_pin,
        "actual_role": actual["role"], "expected_complete": expected["complete"], "actual_complete": actual["complete"],
        "consumption_claim": "none; a used inventory must separately be bound to exact selected generation receipts",
        "custody_claim": "none; version equality is not original-byte possession",
        "identity_parity": identity_parity, "identity_counts": identity_counts,
        "identity_semantics": "present matches source_id and asset_kind regardless of versions; "
                              "equal parity requires both inventories complete for the same finite scope",
        "version_semantics": "stale means different from selected expected version; chronological age is not inferred",
        "missing_semantics": "absent from selected actual inventory only; not publisher-wide or archive-wide absence",
        "resources": {"duckdb_memory_limit": memory_limit, "threads": threads, "spill_directory": str(output / "spill"),
                      "elapsed_seconds": round(time.monotonic()-started, 3),
                      "process_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == 'darwin' else 1024),
                      "limit_scope": "DuckDB managed memory; process RSS can exceed it"},
        "measurements": measures, "counts": counts,
        "samples": [dict(zip(("source_id", "asset_kind", "status", "reason", "identity_status"), row)) for row in samples],
        "input_files": {"expected": list(map(str, expected_files)), "actual": list(map(str, actual_files))},
        "file_observations": [{"path": path, "bytes": before[0], "mtime_ns": before[1]}
                              for path, before in observations.items()],
    }
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    # No scratch database is retained; durable results are the compact diff/summary.
    (output / "scratch.duckdb").unlink(missing_ok=True)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected", required=True, type=Path)
    parser.add_argument("--actual", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--memory-limit", default="256MB")
    parser.add_argument("--threads", type=int, default=2)
    args = parser.parse_args()
    if args.threads < 1:
        parser.error("threads must be positive")
    summary = compare(args.expected, args.actual, args.output, memory_limit=args.memory_limit, threads=args.threads)
    print(json.dumps({"identity_parity": summary["identity_parity"], "identity_counts": summary["identity_counts"],
                      "counts": summary["counts"], "resources": summary["resources"]}))


if __name__ == "__main__":
    main()
