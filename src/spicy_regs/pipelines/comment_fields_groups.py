"""Bound attachment fill inputs by original native data file, preserving every source copy per identity.

This only plans and stages reads. The existing comment_fields prepare/write/journal/undo commands own writes.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
from cyclopts import App

from spicy_regs.duckdb_settings import ExportResources
from spicy_regs.pipelines import comment_fields as cf
from spicy_regs.pipelines import comment_fields_write as cfw
from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg
from spicy_regs.sources.publication import file_identity

RESOURCES = ExportResources(memory="256MB", threads=1)
COLUMNS = cfw.read_columns("attachments")


def _input(stage: Path) -> tuple[Path, dict]:
    receipt = json.loads((stage / "staging.json").read_text())
    reads = stage / "reads.parquet"
    if (receipt["profile"] != "attachments" or receipt["columns"] != list(COLUMNS)
            or not all(receipt["checks"].values()) or cfw._sha256(reads) != receipt["file"]["sha256"]
            or pq.read_schema(reads).names != list(COLUMNS)):
        raise ValueError("Attachment stage differs from its checked receipt")
    return reads, receipt


def _relation(path: Path) -> str:
    return f"read_parquet({cfw._sql(path)}, hive_partitioning=false)"


def _bytes() -> str:
    return " + ".join(f'coalesce(octet_length(encode(CAST(r."{c}" AS VARCHAR))), 0)' for c in COLUMNS)


def plan(workdir: Path, stage: Path, *, max_rows: int, max_read_bytes: int,
         max_spill: str = "3GB", con=None) -> dict:
    """Freeze narrow native membership; split within files by ID/byte limits, never split one ID's copies.

    Limits cover candidate identities and source-value bytes, not decoded receipt RAM. Qualify the actual writer
    separately. A single identity may use at most half the byte budget, leaving room at a grouping boundary.
    """
    if max_rows < 1 or max_read_bytes < 2:
        raise ValueError("max_rows must be positive and max_read_bytes must be at least two")
    reads, source_receipt = _input(stage)
    if workdir.exists() and any(workdir.iterdir()):
        raise FileExistsError(f"Use a fresh group workdir: {workdir}")
    workdir.mkdir(parents=True, exist_ok=True)
    own = con is None
    con = con or iceberg._connect()
    index_path = workdir / "catalog-index.parquet"
    members_path = workdir / "members.parquet"
    excluded_path = workdir / "excluded.parquet"
    classified_path = workdir / "classified.parquet"
    parts, index, classified = _relation(reads), _relation(index_path), _relation(classified_path)
    before = reads.stat()
    try:
        RESOURCES.configure(con, workdir / "spill")
        con.execute("SET max_temp_directory_size = ?", [max_spill])
        cfw.storage_access(con)
        namespace = iceberg._selected_catalog_namespace(con, COMMENT)
        snapshot = iceberg._read_snapshot(con, COMMENT, namespace=namespace)
        native = iceberg._snapshot_query(COMMENT, snapshot, namespace=namespace)
        native = native.replace("SELECT *", f"SELECT comment_id, modify_date, {cfw.FILE_COLUMN} AS _file", 1)
        # Repeated source IDs do not multiply native rows in a SEMI JOIN. Avoid a second hash table;
        # classification below still reads every source copy, and duplicate native rows still refuse.
        membership_sql = f"""COPY (SELECT n.* FROM ({native}) n
                            SEMI JOIN (SELECT comment_id FROM {parts}) r USING (comment_id))
                            TO {cfw._sql(index_path)} (FORMAT PARQUET, COMPRESSION ZSTD)"""
        # Ordinary EXPLAIN binds/plans but does not execute COPY or scan native rows.
        # Keep bounded physical-operator evidence even if the following COPY refuses.
        explained = "\n".join(str(value) for row in con.execute("EXPLAIN " + membership_sql).fetchall() for value in row)
        evidence = {
            "format": "comment-membership-explain/1", "analyze": False,
            "snapshot": snapshot.__dict__, "namespace": namespace,
            "resources": {"memory": RESOURCES.memory, "threads": RESOURCES.threads, "max_spill": max_spill},
            "truncated": False, "text": explained,
        }
        while True:
            content = json.dumps(evidence, ensure_ascii=False, indent=2) + "\n"
            if len(content.encode()) <= 65536:
                break
            if not explained:
                raise ValueError("Membership EXPLAIN metadata exceeds its 64KiB record limit")
            explained = explained[:len(explained) // 2]
            evidence["text"] = explained
            evidence["truncated"] = True
        (workdir / "membership-explain.json").write_text(content)
        con.execute(membership_sql)
        if con.execute(f"SELECT comment_id FROM {index} GROUP BY 1 HAVING count(*) > 1 LIMIT 1").fetchone():
            raise ValueError("A staged comment identity appears more than once in the native catalog")
        con.execute(f"""COPY (
            SELECT r.comment_id, any_value(c.modify_date) AS modify_date, any_value(c._file) AS _file,
                   count(*) AS read_copies, sum({_bytes()}) AS read_value_bytes,
                   CASE WHEN count(c.comment_id)=0 THEN 'absent-from-catalog'
                        WHEN NOT bool_or(r.modify_date IS NOT DISTINCT FROM c.modify_date) THEN 'other-version-only'
                        WHEN NOT bool_or(r.modify_date IS NOT DISTINCT FROM c.modify_date
                                         AND r.attachments_json IS NOT NULL) THEN 'no-stated-value'
                        ELSE 'candidate' END AS outcome
            FROM {parts} r LEFT JOIN {index} c USING (comment_id) GROUP BY r.comment_id
        ) TO {cfw._sql(classified_path)} (FORMAT PARQUET, COMPRESSION ZSTD)""")
        oversized = con.execute(f"""SELECT comment_id, read_value_bytes FROM {classified}
                                    WHERE outcome='candidate' AND read_value_bytes > ? LIMIT 1""",
                                 [max_read_bytes // 2]).fetchone()
        if oversized:
            raise ValueError(f"One identity's copies exceed half the group byte budget: {oversized}")
        con.execute(f"""COPY (
            SELECT comment_id, modify_date, _file, read_copies, read_value_bytes,
                   dense_rank() OVER (ORDER BY _file, row_bucket, byte_bucket)-1 AS group_id
            FROM (
                SELECT *, (row_number() OVER w-1) // {max_rows} AS row_bucket,
                       (sum(read_value_bytes) OVER w) // {max_read_bytes // 2} AS byte_bucket
                FROM {classified} WHERE outcome='candidate'
                WINDOW w AS (PARTITION BY _file ORDER BY comment_id ROWS UNBOUNDED PRECEDING)
            ) ORDER BY group_id, comment_id
        ) TO {cfw._sql(members_path)} (FORMAT PARQUET, COMPRESSION ZSTD)""")
        con.execute(f"COPY (SELECT * FROM {classified} WHERE outcome<>'candidate') "
                    f"TO {cfw._sql(excluded_path)} (FORMAT PARQUET, COMPRESSION ZSTD)")
        groups = con.execute(f"""SELECT group_id, min(_file) AS original_file, count(*) AS identities,
                                       sum(read_copies)::BIGINT AS read_copies,
                                       sum(read_value_bytes)::BIGINT AS read_value_bytes
                                FROM {_relation(members_path)} GROUP BY group_id ORDER BY group_id""").to_arrow_table().to_pylist()
        if any(g["identities"] > max_rows or g["read_value_bytes"] > max_read_bytes for g in groups):
            raise RuntimeError("Group exceeds the requested identity or source-value byte bound")
        outcomes = dict(con.execute(f"SELECT outcome,count(*) FROM {classified} GROUP BY outcome").fetchall())
        read_copies = con.execute(f"SELECT coalesce(sum(read_copies), 0) FROM {classified}").fetchone()[0]
        if sum(outcomes.values()) != source_receipt["distinct_comment_ids"] or read_copies != source_receipt["rows"]:
            raise ValueError("Group candidates and exclusions do not account for the complete source stage")
        if iceberg._read_snapshot(con, COMMENT, namespace=namespace) != snapshot:
            raise RuntimeError("Catalog moved while grouping; use a fresh group plan")
    finally:
        if own:
            con.close()
    after = reads.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns) or cfw._sha256(reads) != source_receipt["file"]["sha256"]:
        raise ValueError("Source stage changed while grouping")
    receipt = {
        "profile": "attachments", "stage": str(stage.resolve()), "reads_sha256": source_receipt["file"]["sha256"],
        "source_staging_sha256": cfw._sha256(stage / "staging.json"), "snapshot": snapshot.__dict__,
        "max_rows": max_rows, "max_read_bytes": max_read_bytes, "groups": groups, "outcomes": outcomes,
        "max_spill": max_spill,
        "members_sha256": cfw._sha256(members_path), "excluded_sha256": cfw._sha256(excluded_path),
        "source_rows": source_receipt["rows"], "source_identities": source_receipt["distinct_comment_ids"],
        "parent_source_gaps": source_receipt.get("source_gaps"),
    }
    (workdir / "groups.json").write_text(json.dumps(receipt, indent=2) + "\n")
    index_path.unlink()
    classified_path.unlink()
    shutil.rmtree(workdir / "spill")
    return receipt


def stage_group(workdir: Path, group: int, out: Path) -> dict:
    """Produce an ordinary checked attachment stage for one immutable group; no catalog scan or write.

    prepare must run against the current catalog, so original file names only guide locality and never authorize
    a write. All source copies and versions of each member are selected, preserving the existing agreement rule.
    """
    plan_path = workdir / "groups.json"
    selected = json.loads(plan_path.read_text())
    stage = Path(selected["stage"])
    reads, source_receipt = _input(stage)
    if (source_receipt["file"]["sha256"] != selected["reads_sha256"]
            or cfw._sha256(stage / "staging.json") != selected["source_staging_sha256"]
            or cfw._sha256(workdir / "members.parquet") != selected["members_sha256"]):
        raise ValueError("Group plan input or membership changed")
    matches = [g for g in selected["groups"] if g["group_id"] == group]
    if len(matches) != 1:
        raise ValueError(f"Unknown group: {group}")
    expected = matches[0]
    if out.exists() and any(out.iterdir()):
        raise FileExistsError(f"Use a fresh staged group directory: {out}")
    out.mkdir(parents=True, exist_ok=True)
    target = out / "reads.parquet"
    source = (f"(SELECT r.* FROM {_relation(reads)} r SEMI JOIN {_relation(workdir / 'members.parquet')} m "
              f"ON r.comment_id=m.comment_id AND m.group_id={int(group)})")
    before = reads.stat()
    with duckdb.connect() as con:
        RESOURCES.configure(con, out / "spill")
        con.execute("SET max_temp_directory_size = ?", [selected["max_spill"]])
        con.execute(f"COPY (SELECT * FROM {source} ORDER BY comment_id,key) TO {cfw._sql(target)} "
                    "(FORMAT PARQUET, COMPRESSION ZSTD)")
        original = cf._column_totals(con, source, list(COLUMNS))
        written = cf._column_totals(con, _relation(target), list(COLUMNS))
    arrow = cf._arrow_totals([str(target)], list(COLUMNS))
    after = reads.stat()
    checks = {
        "source stayed fixed during staging": (before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns),
        "complete source-copy values equal the group": original == written,
        "every planned copy is present": written["rows"] == expected["read_copies"],
        "every planned identity is present": written["distinct_comment_ids"] == expected["identities"],
        "one row per source key": written["distinct_keys"] == written["rows"],
        "no blank identity": written["blank_comment_ids"] == 0,
        "Arrow rows and nonnull values equal DuckDB": arrow["rows"] == arrow["footer_rows"] == written["rows"]
            and all(arrow[f"nn:{c}"] == written[f"nn:{c}"] for c in COLUMNS),
    }
    if not all(checks.values()):
        target.unlink()
        raise ValueError(f"Staged group failed checks: {checks}")
    identity = file_identity(target)
    digest = identity["sha256"].removeprefix("sha256:")
    receipt = {
        "profile": "attachments", "columns": list(COLUMNS), "record_shape": source_receipt["record_shape"],
        "rows": written["rows"], "distinct_keys": written["distinct_keys"],
        "distinct_comment_ids": written["distinct_comment_ids"], "checks": checks,
        "file": {"name": target.name, "bytes": identity["bytes"], "sha256": digest, "etag": identity["etag"]},
        "group": expected, "group_plan_sha256": cfw._sha256(plan_path),
        "source_staging_sha256": selected["source_staging_sha256"], "original_snapshot": selected["snapshot"],
        "parent_source_gaps": selected["parent_source_gaps"],
    }
    (out / "reads.parquet.sha256").write_text(f"{digest}  reads.parquet\n")
    (out / "staging.json").write_text(json.dumps(receipt, indent=2) + "\n")
    shutil.rmtree(out / "spill")
    return receipt


app = App(help=__doc__)


@app.command(name="plan")
def plan_command(*, workdir: Path, stage: Path, max_rows: int, max_read_bytes: int, max_spill: str = "3GB"):
    print(json.dumps(plan(workdir, stage, max_rows=max_rows, max_read_bytes=max_read_bytes, max_spill=max_spill), indent=2))


@app.command(name="stage")
def stage_command(*, workdir: Path, group: int, out: Path):
    print(json.dumps(stage_group(workdir, group, out), indent=2))

if __name__ == "__main__":
    app()
