"""Split a frozen Mirrulations comment manifest into identity-complete source groups."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from contextlib import ExitStack
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.duckdb_settings import ExportResources
from spicy_regs.pipelines.comment_fields import _reader_sha256
from spicy_regs.pipelines.comment_fields_evidence import REFETCH_SUFFIX, SOURCE_PIN_COLUMNS, file_sha256

_RESOURCES = ExportResources(memory="512MB", threads=1)
_KEY_SCHEMA = pa.schema([("key", pa.string())])


def _sql(value):
    return "'" + str(value).replace("'", "''") + "'"


def _comment_identity(key):
    # Use the same refetch suffix rule as the reader's payload/key identity check.
    return f"regexp_replace(regexp_extract({key}, '([^/]+)\\.json$', 1), {_sql(REFETCH_SUFFIX)}, '')"


def _file(path):
    return {"file": path.name, "bytes": path.stat().st_size, "sha256": file_sha256(path)}


def _write_keys(batches, ready):
    """Keep one bounded row buffer and one Parquet writer per nonempty group."""
    buffers = defaultdict(list)
    writers = {}
    with ExitStack() as open_writers:
        def flush(group):
            if group not in writers:
                directory = ready / group
                directory.mkdir()
                writers[group] = open_writers.enter_context(
                    pq.ParquetWriter(directory / "manifest.parquet", _KEY_SCHEMA, compression="zstd"))
            writers[group].write_table(pa.Table.from_arrays([pa.array(buffers[group])], schema=_KEY_SCHEMA))
            buffers[group].clear()

        for batch in batches:
            keys, groups = batch.column(0).to_pylist(), batch.column(1).to_pylist()
            for key, group in zip(keys, groups, strict=True):
                buffers[group].append(key)
                if len(buffers[group]) == 2_048:
                    flush(group)
        for group, keys in buffers.items():
            if keys:
                flush(group)


def split(manifest, source_pins, out, *, local_groups=8, runner_groups=128, max_keys=250_000, max_spill="1GB"):
    """Write one key-only manifest and an optional archive pin file per group.

    A comment with any local-held copy stays local, including its other
    refetch keys and R2-held copies. Runner groups contain no local pins.
    """
    if min(local_groups, runner_groups, max_keys) < 1:
        raise ValueError("Group counts and the per-group key bound must be positive")
    if out.exists():
        raise FileExistsError(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    manifest_pin, source_pin = _file(manifest), _file(source_pins)
    implementation, reader = file_sha256(Path(__file__)), _reader_sha256()
    with TemporaryDirectory(prefix=".comment-source-groups-", dir=out.parent) as scratch:
        temp = Path(scratch)
        ready = temp / "groups"
        ready.mkdir()
        group_receipts = []
        with duckdb.connect() as con:
            _RESOURCES.configure(con, temp / "spill")
            con.execute("SET max_temp_directory_size = ?", [max_spill])
            con.execute(f"""
                CREATE TEMP VIEW selected_keys AS
                SELECT key, {_comment_identity('key')} AS comment_id FROM read_parquet({_sql(manifest)})
                WHERE key LIKE 'raw-data/%/comments/%' AND key LIKE '%.json'
            """)
            if con.execute("SELECT 1 FROM selected_keys WHERE comment_id = '' LIMIT 1").fetchone():
                raise ValueError("Cannot group a comment key without an identity")
            con.execute(f"""
                CREATE TEMP TABLE held AS
                SELECT key, {', '.join(SOURCE_PIN_COLUMNS)} FROM read_parquet({_sql(source_pins)})
            """)
            bad_pin = con.execute("""
                SELECT key FROM held GROUP BY key
                HAVING key IS NULL OR count(*) > 1 OR bool_or(
                    raw_custody IS NULL OR raw_custody NOT IN ('local', 'r2') OR raw_sha256 IS NULL)
                LIMIT 1
            """).fetchone()
            if bad_pin:
                raise ValueError(f"Invalid or repeated held source key: {bad_pin[0]!r}")
            con.execute(f"""
                CREATE TEMP TABLE local_ids AS
                SELECT DISTINCT {_comment_identity('key')} AS comment_id FROM held WHERE raw_custody='local'
            """)
            expected = con.execute("SELECT count(*) FROM selected_keys").fetchone()[0]
            if not expected:
                raise ValueError("The selected manifest has no comment keys")
            with con.execute(f"""
                    SELECT k.key,
                           CASE WHEN l.comment_id IS NOT NULL THEN
                               printf('local-%03d', bucket % {int(local_groups)})
                           ELSE printf('runner-%03d', bucket % {int(runner_groups)}) END AS source_group
                    FROM (SELECT *, ('0x' || left(sha256(comment_id), 15))::UBIGINT AS bucket
                          FROM selected_keys) k
                    LEFT JOIN local_ids l USING (comment_id)
            """).to_arrow_reader(20_000) as batches:
                _write_keys(batches, ready)
            for dest in sorted(ready.iterdir()):
                group = dest.name
                keys = dest / "manifest.parquet"
                table = f"read_parquet({_sql(keys)})"
                count, distinct, local, hosted = con.execute(f"""
                    SELECT count(*), count(DISTINCT key),
                           count(*) FILTER (WHERE h.raw_custody='local'),
                           count(*) FILTER (WHERE h.raw_custody='r2')
                    FROM {table} k LEFT JOIN held h USING (key)
                """).fetchone()
                if count != distinct or count > max_keys or (group.startswith("runner-") and local):
                    raise ValueError(f"Source group {group} violates key uniqueness, custody or size ({count} keys)")
                pins = None
                if local or hosted:
                    pins = dest / "source-pins.parquet"
                    con.execute(f"""
                        COPY (SELECT key, {', '.join(f'h.{column}' for column in SOURCE_PIN_COLUMNS)}
                              FROM held h SEMI JOIN {table} k USING (key) ORDER BY key)
                        TO {_sql(pins)} (FORMAT PARQUET, COMPRESSION ZSTD)
                    """)
                group_receipts.append({
                    "group": group, "keys": count, "local": local, "r2": hosted, "external": count - local - hosted,
                    "manifest": _file(keys), "source_pins": _file(pins) if pins is not None else None,
                })
        if sum(group["keys"] for group in group_receipts) != expected:
            raise ValueError("Source groups do not conserve the frozen manifest key count")
        if (_file(manifest) != manifest_pin or _file(source_pins) != source_pin
                or file_sha256(Path(__file__)) != implementation):
            raise ValueError("Manifest or archive pins changed while splitting source groups")
        result = {
            "format": "comment-source-groups/1", "input_manifest": manifest_pin, "input_source_pins": source_pin,
            "implementation_sha256": implementation, "reader_sha256": reader, "profile": "attachments",
            "local_groups": local_groups, "runner_groups": runner_groups,
            "keys": expected, "max_group_keys": max_keys, "groups": group_receipts,
        }
        (ready / "groups.json").write_text(json.dumps(result, indent=2) + "\n")
        ready.rename(out)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--source-pins", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--local-groups", default=8, type=int)
    parser.add_argument("--runner-groups", default=128, type=int)
    parser.add_argument("--max-keys", default=250_000, type=int)
    parser.add_argument("--max-spill", default="1GB")
    result = split(**vars(parser.parse_args()))
    print(json.dumps({"keys": result["keys"], "groups": len(result["groups"])}))


if __name__ == "__main__":
    main()
