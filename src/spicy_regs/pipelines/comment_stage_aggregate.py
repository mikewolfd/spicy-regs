"""Combine every checked source-group attachment stage without downloading raw records."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
from collections import Counter
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.duckdb_settings import ExportResources
from spicy_regs.pipelines import comment_fields as cf
from spicy_regs.pipelines.comment_fields_evidence import file_sha256
from spicy_regs.pipelines.comment_source_groups import _comment_identity, _sql
from spicy_regs.sources.publication import file_identity

_RESOURCES = ExportResources(memory="384MB", threads=1)
_COLUMNS = list(cf.read_columns("attachments"))
_SCHEMA = pa.schema([(column, pa.string()) for column in _COLUMNS])
PERMANENT_SOURCE_GAPS = frozenset({
    "source-missing", "source-changed", "unlisted", "oversized", "invalid-source-metadata",
    "unreadable", "requested-empty", "refused",
})


def _read(path: Path) -> str:
    return f"read_parquet({_sql(path)}, hive_partitioning=false)"


def _file(path: Path, pin: dict, *, name: str) -> Path:
    if ((pin.get("file") or pin.get("name")) != name or path.stat().st_size != pin["bytes"]
            or file_sha256(path) != pin["sha256"]):
        raise ValueError(f"Artifact differs from its pin: {path}")
    return path


def repair_admission(path: Path, pin: str | None, master: dict, master_sha256: str, *, candidate_only=False) -> dict:
    """Admit exact derived results, keeping the baseline reader for every carried group."""
    from spicy_regs.transforms import regulations_shape

    if not pin or file_sha256(path) != pin:
        raise ValueError("Repair admission differs from its exact SHA-256")
    admission = json.loads(path.read_text())
    producer = admission["producer"]
    executor = Path(__file__).resolve().parents[3] / "scripts/repair_comment_attachment_group.py"
    groups = {g["group"] for g in master["groups"]}
    if (admission["format"] != "comment-attachment-repair-stages/1"
            or admission["groups_sha256"] != master_sha256
            or admission["previous_reader_sha256"] != master["reader_sha256"]
            or set(producer) != {"runtime_ref", "reader_sha256", "executor_sha256", "shape_sha256", "candidate_only"}
            or not re.fullmatch(r"[0-9a-f]{40}", producer["runtime_ref"])
            or producer["reader_sha256"] != cf._reader_sha256()
            or producer["executor_sha256"] != file_sha256(executor)
            or producer["shape_sha256"] != file_sha256(Path(regulations_shape.__file__))
            or producer["candidate_only"] is not candidate_only
            or not admission["groups"] or not admission["groups"].keys() <= groups
            or any(not re.fullmatch(r"runner-[0-9]{3}", name) for name in admission["groups"])):
        raise ValueError("Repair admission does not match the exact producer and frozen groups")
    for selected in admission["groups"].values():
        records = selected["records"]
        files = selected["files"]
        plans = [name for name in files if re.fullmatch(r"(?:read/)?plan/pinned-[a-z0-9-]+\.json", name)]
        required = {"result.json", "repair.json", "repair-request.json", "execution.json", "stage/staging.json",
                    "stage/reads.parquet.sha256", *plans}
        if (not records or len({row["key"] for row in records}) != len(records)
                or len(plans) != 1 or set(files) != required
                or any(type(member["bytes"]) is not int or not 0 < member["bytes"] <= 4 * 2**20
                       or not re.fullmatch(r"[0-9a-f]{64}", member["sha256"]) for member in files.values())
                or selected["request_sha256"] != files["repair-request.json"]["sha256"]
                or not re.fullmatch(r"[0-9a-f]{64}", selected["previous_archive_sha256"])):
            raise ValueError("Repair admission requires exact request, archive and distinct source keys")
    return admission


def _group_result(root: Path, expected: dict, master_sha256: str, reader_sha256: str,
                  repair: dict | None = None, repair_producer: dict | None = None) -> tuple[Path, dict, dict[Path, str]]:
    name = expected["group"]
    directory = root / name
    result_path = directory / "result.json"
    stage_dir = directory / "stage"
    stage_path = stage_dir / "staging.json"
    plan_paths = list(directory.glob("read/plan/pinned-*.json")) + list(directory.glob("plan/pinned-*.json"))
    if len(plan_paths) != 1:
        raise ValueError(f"Source group needs one pinned read plan receipt: {name}")
    documents = {path: path.read_bytes() for path in (result_path, stage_path, plan_paths[0])}
    result, stage, plan = (json.loads(content) for content in documents.values())
    receipt_pins = {path: hashlib.sha256(content).hexdigest() for path, content in documents.items()}
    result_reader = reader_sha256
    if repair is not None:
        if repair_producer is None:
            raise ValueError("Repair requires its exact producer admission")
        report_path = directory / "repair.json"
        report = json.loads(report_path.read_text())
        request_path = directory / "repair-request.json"
        request = json.loads(request_path.read_text())
        execution = json.loads((directory / "execution.json").read_text())
        def records(rows):
            return [{key: row[key] for key in ("key", "raw_sha256", "raw_bytes")} for row in rows]

        for relative, member in repair["files"].items():
            path = directory / relative
            _file(path, {"file": path.name, **member}, name=path.name)
        limits = execution["limits"]
        elapsed = execution["elapsed_seconds"]
        if (execution["status"] != "complete" or execution["remaining_processes"]
                or limits != report["resource_admission"]["limits"]
                or not 0 < limits["wall_seconds"] <= 5400
                or limits["rss_bytes"] != 2 * 2**30 or limits["output_bytes"] != 2**30
                or limits["stop_free_bytes"] != 4 * 2**30 or limits["start_free_bytes"] != 4 * 2**30
                or type(elapsed) not in (int, float) or not math.isfinite(elapsed) or elapsed < 0
                or elapsed > limits["wall_seconds"]
                or execution["peaks"]["rss"] > limits["rss_bytes"]
                or execution["peaks"]["allocated"] > limits["output_bytes"]
                or execution["last"]["free"] < limits["stop_free_bytes"]
                or report["previous_active_seconds"] + report["supplementary_active_seconds"] + elapsed > 5400):
            raise ValueError(f"Repair lacks successful bounded supervisor closure: {name}")
        if (receipt_pins[result_path] != repair["files"]["result.json"]["sha256"]
                or file_sha256(report_path) != repair["files"]["repair.json"]["sha256"]
                or result.get("repair_sha256") != file_sha256(report_path)
                or report["format"] != "comment-attachment-size-repair/1"
                or report["group"] != name or report["groups_sha256"] != master_sha256
                or report["previous"]["identity"]["reader_sha256"] != reader_sha256
                or report["previous"]["identity"]["group"] != name
                or report["previous"]["identity"]["groups_sha256"] != master_sha256
                or file_sha256(request_path) != repair["request_sha256"]
                or report["request_sha256"] != repair["request_sha256"]
                or report["previous"] != request["previous"]
                or report["previous"]["archive"]["sha256"] != repair["previous_archive_sha256"]
                or report["producer"] != repair_producer or request["producer"] != repair_producer
                or records(report["repaired_records"]) != repair["records"]
                or records(request["records"]) != repair["records"]
                or report["preserved_rows"] + report["added_rows"] != expected["keys"]
                or report["added_rows"] != len(repair["records"])
                or report["source_usage"]["attempts"] > 2 * expected["keys"]
                or report["source_usage"]["bytes"] > 3 * 2**30
                or report["previous_active_seconds"] + report["supplementary_active_seconds"] + report["repair_seconds"] > 5400
                or not report["checks"] or not all(report["checks"].values()) or report["new_source_gets"] != 0
                or not report["repaired_records"] or len({row["key"] for row in report["repaired_records"]}) != len(report["repaired_records"])
                or any(result[key] != report["producer"][key] for key in repair_producer)):
            raise ValueError(f"Repair differs from its exact admitted derivation: {name}")
        result_reader = report["producer"]["reader_sha256"]
        receipt_pins[report_path] = result["repair_sha256"]
        receipt_pins[request_path] = repair["request_sha256"]
        receipt_pins[directory / "execution.json"] = repair["files"]["execution.json"]["sha256"]
    elif result.get("repair_sha256"):
        raise ValueError(f"Repaired source group requires explicit admission: {name}")
    # The runner uses these names; local receipts predate the runner.
    if "groups_sha256" in result:
        master_pin, descriptor, stage_file = result["groups_sha256"], result["group_descriptor"], result["stage_file"]
        if result["status"] != "complete" or result["reader_sha256"] != result_reader:
            raise ValueError(f"Runner did not complete the pinned reader: {name}")
    else:
        master_pin, descriptor, stage_file = result["master_receipt_sha256"], result["inputs"], result["stage"]
    source_pins = expected["source_pins"]
    if (result["group"] != name or master_pin != master_sha256 or descriptor != expected
            or stage_file != stage["file"]
            or result.get("source_gaps") != stage.get("source_gaps")
            or stage["profile"] != "attachments" or stage["record_shape"] != cf.RECORD_SHAPE
            or stage["columns"] != _COLUMNS or not stage["checks"] or not all(stage["checks"].values())
            or plan["reader_sha256"] != reader_sha256 or plan["profile"] != stage["profile"]
            or plan["record_shape"] != stage["record_shape"] or plan["plan_sha256"] != stage["plan_sha256"]
            or plan["manifest_sha256"] != expected["manifest"]["sha256"]
            or plan["source_pins_sha256"] != (source_pins["sha256"] if source_pins else None)
            or plan["keys"] != expected["keys"] or not plan["observe_source"] or not stage["observe_source"]):
        raise ValueError(f"Stage receipt does not match the frozen group: {name}")
    reads = _file(stage_dir / "reads.parquet", stage["file"], name="reads.parquet")
    schema = pq.read_schema(reads)
    if schema.names != _COLUMNS or not all(pa.types.is_string(field.type) for field in schema):
        raise ValueError(f"Stage does not have the exact attachment read columns: {name}")
    outcomes = result["read"]
    checkpoints = stage["source_checkpoints"]
    if (outcomes["stopped"] or outcomes["deferred_chunks"] or not checkpoints or len(checkpoints) != plan["chunks"]
            or any(item["plan_sha256"] != stage["plan_sha256"] or item["profile"] != "attachments"
                   or not 0 <= item["unresolved"] <= item["keys"]
                   for item in checkpoints)
            or len({(item["part_sha256"], item["evidence_sha256"]) for item in checkpoints}) != len(checkpoints)
            or sum(item["keys"] for item in checkpoints) != expected["keys"]):
        raise ValueError(f"Source group has incomplete checkpoints or transient reads: {name}")
    receipt_pins[reads] = stage["file"]["sha256"]
    return reads, stage, receipt_pins


def _expected_keys(
    con: duckdb.DuckDBPyConnection, manifest: Path, reads: Path, stage: dict, receipt_pins: dict[Path, str],
) -> tuple[dict[str, int], int]:
    """Check exact membership; a permanent gap excludes its whole identity."""
    staged = _read(reads)
    identity = _comment_identity("key")
    expected = _read(manifest)
    gaps = stage.get("source_gaps")
    outcomes = {}
    withheld = 0
    unresolved = sum(item["unresolved"] for item in stage["source_checkpoints"])
    if unresolved:
        gap_file = reads.parent / "source-gaps.parquet"
        if not gaps or file_sha256(gap_file) != gaps["sha256"]:
            raise ValueError("Permanent source gaps require their exact quarantine report")
        receipt_pins[gap_file] = gaps["sha256"]
        source = _read(gap_file)
        if con.execute(f"""
            SELECT 1 FROM {source} g LEFT JOIN {expected} m ON g.key=m.key
            WHERE m.key IS NULL OR g.comment_id IS DISTINCT FROM {_comment_identity('g.key')}
                  OR coalesce(g.comment_id, '') = '' LIMIT 1
        """).fetchone():
            raise ValueError("Source gap points outside its manifest or carries a different identity")
        outcomes = dict(con.execute(f"SELECT status, count(*) FROM {source} GROUP BY status").fetchall())
        [count, distinct, identities] = con.execute(
            f"SELECT count(*), count(DISTINCT key), count(DISTINCT comment_id) FROM {source}"
        ).fetchall()[0]
        if (set(outcomes) - PERMANENT_SOURCE_GAPS or count != distinct or count != unresolved
                or count != gaps["unresolved_keys"] or identities != gaps["withheld_comment_ids"]):
            raise ValueError("Source quarantine contains transient, unaccounted or repeated outcomes")
        expected = (f"(SELECT m.key FROM {expected} m ANTI JOIN {source} g "
                    f"ON {_comment_identity('m.key')}=g.comment_id)")
        withheld = identities
    elif gaps:
        raise ValueError("Stage declares quarantine despite having no unresolved reads")
    if con.execute(f"""
        SELECT 1 FROM {staged} r ANTI JOIN {expected} m USING (key)
        UNION ALL
        SELECT 1 FROM {expected} m ANTI JOIN {staged} r USING (key)
        LIMIT 1
    """).fetchone():
        raise ValueError("Staged keys differ from the frozen manifest and its quarantine")
    if con.execute(f"""
        SELECT 1 FROM {staged} WHERE comment_id IS DISTINCT FROM {identity} OR trim(comment_id) = '' LIMIT 1
    """).fetchone():
        raise ValueError("Staged payload identity differs from its source key")
    return outcomes, withheld


def _check_routing(con: duckdb.DuckDBPyConnection, manifests: dict[str, Path], master: dict) -> None:
    local = [_read(path) for name, path in manifests.items() if name.startswith("local-")]
    if local:
        joined = " UNION ALL ".join(f"SELECT key FROM {source}" for source in local)
        con.execute(f"CREATE TEMP TABLE local_ids AS SELECT DISTINCT {_comment_identity('key')} AS id FROM ({joined})")
    else:
        con.execute("CREATE TEMP TABLE local_ids (id VARCHAR)")
    for group, manifest in manifests.items():
        lane, index = group.split("-")
        buckets = master["local_groups" if lane == "local" else "runner_groups"]
        identity = _comment_identity("key")
        scope = " OR EXISTS (SELECT 1 FROM local_ids l WHERE l.id=m.comment_id)" if lane == "runner" else ""
        bad = con.execute(f"""
            SELECT 1 FROM (SELECT key, {identity} AS comment_id FROM {_read(manifest)}) m
            WHERE key IS NULL OR key NOT LIKE 'raw-data/%/comments/%.json' OR comment_id = ''
                  OR ('0x' || left(sha256(comment_id), 15))::UBIGINT % {int(buckets)} <> {int(index)}
            {scope} LIMIT 1
        """).fetchone()
        if bad:
            raise ValueError(f"Source identity is routed to another group: {group}")


def aggregate(master_path: Path, master_sha256: str, reads_root: Path, out: Path, *, max_spill: str = "512MB",
              repair_admissions: Path | None = None, repair_admissions_sha256: str | None = None,
              candidate_only: bool = False) -> dict:
    """Verify all compact group stages and emit one ordinary checked attachment stage."""
    master_content = master_path.read_bytes()
    if hashlib.sha256(master_content).hexdigest() != master_sha256:
        raise ValueError("Master source group receipt differs from its pinned digest")
    master = json.loads(master_content)
    # The caller pins this previously verified partition of the full manifest.
    # Check routing again before summing group counts as globally distinct.
    groups = {group["group"]: group for group in master["groups"]}
    repairs = {}
    admitted = None
    if repair_admissions is not None:
        admitted = repair_admission(repair_admissions, repair_admissions_sha256, master, master_sha256,
                                    candidate_only=candidate_only)
        repairs = admitted["groups"]
    elif repair_admissions_sha256:
        raise ValueError("Repair SHA-256 requires its admission file")
    if (master["format"] != "comment-source-groups/1" or master["profile"] != "attachments"
            or min(master["local_groups"], master["runner_groups"], master["max_group_keys"]) < 1
            or not groups or len(groups) != len(master["groups"])
            or sum(group["keys"] for group in groups.values()) != master["keys"]):
        raise ValueError("Master source group receipt is incomplete or inconsistent")
    manifests = {}
    for name, group in groups.items():
        lane, separator, index = name.partition("-")
        if (lane not in {"local", "runner"} or not separator or not index.isdecimal()
                or name != f"{lane}-{int(index):03d}" or int(index) >= master[f"{lane}_groups"]
                or not 0 < group["keys"] <= master["max_group_keys"]
                or min(group["local"], group["r2"], group["external"]) < 0
                or group["keys"] != group["local"] + group["r2"] + group["external"]
                or bool(group["local"]) != (lane == "local")):
            raise ValueError(f"Invalid source group: {name}")
        manifests[name] = _file(master_path.parent / name / "manifest.parquet", group["manifest"], name="manifest.parquet")
    if out.exists():
        raise FileExistsError(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=".comment-stage-", dir=out.parent) as scratch:
        temporary = Path(scratch)
        ready = temporary / "stage"
        ready.mkdir()
        target = ready / "reads.parquet"
        inputs: list[str] = []
        totals = Counter()
        gaps = Counter()
        summaries = []
        frozen = {master_path: master_sha256, **{
            path: groups[name]["manifest"]["sha256"] for name, path in manifests.items()
        }}
        if repair_admissions is not None:
            frozen[repair_admissions] = repair_admissions_sha256
        with duckdb.connect() as con:
            _RESOURCES.configure(con, temporary / "spill")
            con.execute("SET max_temp_directory_size = ?", [max_spill])
            _check_routing(con, manifests, master)
            with pq.ParquetWriter(target, _SCHEMA, compression="zstd") as writer:
                for name, group in sorted(groups.items()):
                    reads, stage, receipt_pins = _group_result(
                        reads_root, group, master_sha256, master["reader_sha256"], repairs.get(name),
                        admitted["producer"] if admitted else None)
                    outcomes, withheld = _expected_keys(con, manifests[name], reads, stage, receipt_pins)
                    frozen.update(receipt_pins)
                    counts = cf._column_totals(con, _read(reads), _COLUMNS)
                    planned, unique = con.execute(
                        f"SELECT count(*), count(DISTINCT key) FROM {_read(manifests[name])}"
                    ).fetchall()[0]
                    if (planned != unique or planned != group["keys"] or counts["rows"] != counts["distinct_keys"]
                            or counts["rows"] != stage["rows"] or counts["distinct_keys"] != stage["distinct_keys"]
                            or counts["blank_comment_ids"]
                            or counts["distinct_comment_ids"] != stage["distinct_comment_ids"]):
                        raise ValueError(f"Group counts disagree with its manifest or stage: {name}")
                    totals.update({"rows": counts["rows"], "ids": counts["distinct_comment_ids"],
                                   "withheld": withheld, "quarantined_keys": planned - counts["rows"],
                                   **{f"hash:{c}": int(counts[f"hash:{c}"] or 0) for c in _COLUMNS}})
                    gaps.update(outcomes)
                    inputs.append(str(reads))
                    summaries.append({"group": name, "file": stage["file"],
                                      "receipts_sha256": {path.name: digest for path, digest in receipt_pins.items()
                                                          if path.suffix == ".json"},
                                      "source_gaps": stage.get("source_gaps"), "source_gap_statuses": outcomes})
                    for batch in pq.ParquetFile(reads).iter_batches(batch_size=20_000):
                        writer.write_batch(batch.replace_schema_metadata(None))
            hashes = ", ".join(f'coalesce(sum(hash("{column}")), 0)' for column in _COLUMNS)
            written = con.execute(f"SELECT count(*), {hashes} FROM {_read(target)}").fetchall()[0]
        arrow_inputs = cf._arrow_totals(inputs, _COLUMNS)
        arrow_output = cf._arrow_totals([str(target)], _COLUMNS)
        checks = {
            "every planned key staged or quarantined": totals["rows"] + totals["quarantined_keys"] == master["keys"],
            "DuckDB output row count and column hashes agree": written == (
                totals["rows"], *(totals[f"hash:{column}"] for column in _COLUMNS)),
            "Arrow input and output values agree": arrow_inputs == arrow_output,
            "master and group inputs stayed fixed": all(file_sha256(path) == pin for path, pin in frozen.items()),
        }
        if not all(checks.values()):
            raise ValueError(f"Aggregate attachment stage failed checks: {checks}")
        identity = file_identity(target)
        digest = identity["sha256"].removeprefix("sha256:")
        receipt = {
            "profile": "attachments", "columns": _COLUMNS, "record_shape": cf.RECORD_SHAPE,
            "rows": totals["rows"], "distinct_keys": totals["rows"], "distinct_comment_ids": totals["ids"],
            "non_null_by_column": {column: arrow_output[f"nn:{column}"] for column in _COLUMNS},
            "file": {"name": target.name, "bytes": identity["bytes"], "sha256": digest, "etag": identity["etag"]},
            "master_source_groups_sha256": master_sha256, "group_stages": summaries, "checks": checks,
        }
        if admitted:
            receipt["repair_admission_sha256"] = repair_admissions_sha256 or ""
            receipt["candidate_only"] = candidate_only
        if gaps:
            receipt["source_gaps"] = {"unresolved_keys": sum(gaps.values()), "withheld_comment_ids": totals["withheld"],
                                      "quarantined_keys": totals["quarantined_keys"], "statuses": dict(gaps)}
        (ready / "reads.parquet.sha256").write_text(f"{digest}  reads.parquet\n")
        (ready / "staging.json").write_text(json.dumps(receipt, indent=2) + "\n")
        ready.rename(out)
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--master-path", required=True, type=Path)
    parser.add_argument("--master-sha256", required=True)
    parser.add_argument("--reads-root", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--max-spill", default="512MB")
    receipt = aggregate(**vars(parser.parse_args()))
    print(json.dumps({"rows": receipt["rows"], "file": receipt["file"], "source_gaps": receipt.get("source_gaps")}))


if __name__ == "__main__":
    main()
