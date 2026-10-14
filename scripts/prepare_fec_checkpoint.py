"""Prepare FEC checkpoint segments using shared original references; assemble once."""

import argparse
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.fec_observation_segments import (
    observation_segments,
    checkpoint_executor,
    verify_checkpoint_origin,
    _write_new,
    _file,
    _existing_directory,
    _sha,
)
from spicy_regs.fec_native_conversion import _identity_conversion_row, validate_identity_schema
from spicy_regs.transforms.assemble_fec_query import TypedInput
from spicy_regs.sources import publication
from spicy_regs.etl_bulk import bulk_limits


def operation_original_references(checkpoint, family, mapping, original_work, *, create):
    """Bind the selected originals to already charged aliases in this operation."""
    operation = Path(checkpoint).absolute().parent.parent
    work = Path(original_work).absolute()
    _existing_directory(work)
    _existing_directory(operation)
    operation, work = operation.resolve(), work.resolve()
    if not work.is_relative_to(operation / "invocations"):
        raise ValueError("Original references must stay inside this cumulative operation")
    members = {}
    references = {}
    for key in sorted(family["tables"]):
        for member in publication.table_members({"families": {"fec-observations": family}}, key):
            original = Path(mapping[member.path])
            alias = work / "retained" / member.key
            _existing_directory(alias.parent)
            if any(p.is_symlink() or not p.is_file() for p in (original, alias)):
                raise ValueError("Operation original references require regular files")
            left, right = original.stat(), alias.stat()
            if (left.st_dev, left.st_ino, left.st_size) != (right.st_dev, right.st_ino, member.byte_size):
                raise ValueError("Operation original references differ from the selected original")
            references[member.path] = str(alias)
            members[member.path] = {
                "path": alias.relative_to(operation).as_posix(),
                "sha256": member.sha256,
                "byte_size": member.byte_size,
                "rows": member.rows,
            }
    value = {"original_header_sha256": _sha(Path(checkpoint) / "checkpoint.json"), "members": members}
    record = operation / "output" / "original-references.json"
    if not record.exists():
        if not create:
            raise ValueError("Final assembly requires admitted operation original references")
        _write_new(record, value)  # Caller holds the exclusive checkpoint lock.
    _file(record)
    if json.loads(record.read_text()) != value:
        raise ValueError("Operation original reference selection changed")
    return references


def prepare_checkpoint(
    *,
    checkpoint,
    captured,
    source_pin,
    retained_members,
    hosted_main=None,
    origin=None,
    check_resources=lambda: None,
    original_reference_work=None,
):
    """Use the maintained verified preparation API; create no aliases or assembly."""
    family = captured["families"]["fec-observations"]
    expected = {m.path for key in family["tables"] for m in publication.table_members(captured, key)}
    if set(retained_members) != expected or set(family["tables"]) != {
        "fec_collections.parquet",
        "fec_relationships.parquet",
        "fec_source_records.parquet",
    }:
        raise ValueError("Checkpoint original mapping or observations membership differs")
    with observation_segments(
        checkpoint, captured_family=family, source_pin=source_pin, hosted_main=hosted_main, origin=origin
    ) as segments:
        if original_reference_work is not None:
            retained_members = operation_original_references(
                checkpoint, family, retained_members, original_reference_work, create=True
            )
        for key in sorted(family["tables"]):
            members = publication.table_members(captured, key)
            if len(members) != 1:
                raise ValueError("Checkpoint resource worker requires single-member originals")
            member = members[0]
            if member.sha256 is None or member.rows is None or member.byte_size is None:
                raise ValueError("Checkpoint originals require complete digest, row and size pins")
            path = Path(retained_members[member.path])
            if path.is_symlink() or not path.is_file():
                raise ValueError("Checkpoint resource worker requires regular originals")
            table = key.removesuffix(".parquet")
            schema = pq.read_schema(path)
            validate_identity_schema(table, schema)
            witness = dict(
                source_id=member.path,
                source_uri=None,
                sha256=member.sha256,
                locator=None,
                body_version=family["artifactDigest"],
            )
            segments.prepare_segments(
                table,
                [TypedInput(path, member.sha256, member.rows, None)],
                [witness],
                schema,
                prepare_row=lambda row: _identity_conversion_row(table, row, schema, None),
                check_resources=check_resources,
            )
        segments.finish()
    return {
        "segments_complete": True,
        "complete_family_qualified": False,
        "original_aliases_created": 0,
        "table_consolidations": 0,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkout", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expect-main", required=True)
    parser.add_argument("--retained-member-map", type=Path, required=True)
    parser.add_argument("--observation-checkpoint", type=Path, required=True)
    parser.add_argument("--original-producer", required=True)
    parser.add_argument("--original-reference-work", type=Path, required=True)
    parser.add_argument("--original-checkpoint-sha256", required=True)
    parser.add_argument("--assemble", action="store_true", help="One final maintained complete-family conversion")
    args = parser.parse_args(argv)
    import os

    from spicy_regs import fec_observation_segments

    if args.checkout.resolve() != Path(fec_observation_segments.__file__).resolve().parents[2]:
        raise ValueError("Executor imports differ from its selected checkout")
    os.chdir(args.checkout)
    pa.set_cpu_count(1)
    pa.set_io_thread_count(1)
    checkpoint = args.observation_checkpoint
    header = json.loads((checkpoint / "checkpoint.json").read_text())
    source_pin = header["identity"]["source_pin"]
    family = header["identity"]["captured_family"]
    origin = {
        "commit": args.original_producer,
        "header_sha256": args.original_checkpoint_sha256,
        "executor_commit": args.expect_main,
        "remote": "native-release-https",
    }
    proof = verify_checkpoint_origin(checkpoint, family, source_pin, origin)
    if any(p.is_symlink() for p in (checkpoint, *checkpoint.parents, args.output, *args.output.parents)):
        raise ValueError("Checkpoint executor paths contain a symlink")
    output = args.output.resolve()
    operation = checkpoint.parent.parent.resolve()
    if not output.is_relative_to(operation / "invocations") or output.exists():
        raise ValueError("Checkpoint executor needs a fresh invocation output under its cumulative operation")
    output.mkdir(parents=True)
    evidence = {
        "originAdmission": proof,
        "executorScriptSHA256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "mode": "final-assembly" if args.assemble else "segments-only",
        "published": False,
    }
    (output / "execution.json").write_text(json.dumps(evidence, indent=2) + "\n")
    base = (os.getenv("R2_PUBLIC_URL") or "").rstrip("/")
    if not base:
        raise ValueError("R2_PUBLIC_URL is required")
    captured = publication.current_index(base)
    if captured["families"]["fec-observations"] != family:
        raise ValueError("Checkpoint selected published family changed")
    mapping = json.loads(args.retained_member_map.read_text())
    with bulk_limits(threads=1, memory_limit="512MiB", require_bulk=True):
        if args.assemble:
            closed = [json.loads(p.read_text())["spec"] for p in sorted((checkpoint / "closed").glob("*.json"))]
            rows = {}
            for entry in closed:
                rows[entry["table"]] = rows.get(entry["table"], 0) + entry["rows"]
            expected_rows = {k.removesuffix(".parquet"): v["rows"] for k, v in family["tables"].items()}
            if rows != expected_rows:
                raise ValueError("Final assembly requires the complete closed family first")
            mapping = operation_original_references(
                checkpoint, family, mapping, args.original_reference_work, create=False
            )
            from spicy_regs.native_conversion import convert

            with checkpoint_executor(origin):
                result = convert(
                    "fec-observations",
                    allowed=["fec-observations"],
                    work=output / "assembly",
                    expected_main=args.expect_main,
                    expected_spicy_docs="0.65.0",
                    remote="native-release-https",
                    retained_members=mapping,
                    observation_checkpoint=checkpoint,
                )
        else:
            result = prepare_checkpoint(
                checkpoint=checkpoint,
                captured=captured,
                source_pin=source_pin,
                retained_members=mapping,
                hosted_main=proof["executorSource"]["main"],
                origin=origin,
                original_reference_work=args.original_reference_work,
            )
    evidence["result"] = result
    (output / "execution.json").write_text(json.dumps(evidence, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
