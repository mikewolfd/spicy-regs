"""Prepare a managed wrapper around accepted native outputs without rerunning sources."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
from tempfile import TemporaryDirectory

import pyarrow.parquet as pq

from rulespec_artifacts import LocalMemberSource, describe_member, publish_directory_no_replace

from spicy_regs import generations
from spicy_regs.etl_policy_registry import installed_policies, require_registered_receipts
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.sources import publication
from spicy_regs.sources.retained_native_collections import POLICIES, TABLES


def _digest(path: Path) -> str:
    with path.open("rb") as stream:
        return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()


def _pinned_json(pin: dict) -> dict:
    path = Path(pin["path"])
    size = pin["bytes"]
    if type(size) is not int or not 0 < size <= 8 * 1024**2:
        raise ValueError("Accepted metadata must have a bounded byte size")
    with path.open("rb") as stream:
        raw = stream.read(size + 1)
    if len(raw) != size or hashlib.sha256(raw).hexdigest() != pin["sha256"].removeprefix("sha256:"):
        raise ValueError("Accepted native metadata pin changed")
    return json.loads(raw)


def _check_file(path: Path, size: int, digest: str) -> None:
    if path.is_symlink() or not path.is_file() or path.stat().st_size != size or _digest(path) != digest:
        raise ValueError("Accepted native member bytes changed")


def prepare_selected(
    collection: dict,
    *,
    family: str,
    read_snapshot: dict,
    output: Path,
    preview: bool = False,
) -> dict:
    """Keep accepted subject and receipt bytes; qualify only the new packaging.

    The caller supplies a pinned, coordinator-accepted collection description
    and the explicit target family/index. Initial selected-family wrappers
    refuse replacement or table ownership collisions. Preview wrappers stay
    local-partial and cannot pass the maintained publication gate.
    """
    if not isinstance(family, str) or not re.fullmatch(r"[a-z][a-z0-9_-]*", family):
        raise ValueError("Explicit native publication family required")
    publication.parse_index(json.dumps(read_snapshot).encode())
    if family in read_snapshot["families"]:
        raise ValueError("Initial selected wrapper cannot replace a published family")
    if output.exists():
        raise FileExistsError("Managed wrapper destination already exists")
    if collection.get("local_content_accepted") is not True:
        raise ValueError("Coordinator-accepted selected native content required")
    acceptance = _pinned_json(collection["coordinator_acceptance"])
    accepted_commit = acceptance.get("commit") or acceptance.get("code_commit") or collection.get("coordinator_commit")
    if not isinstance(accepted_commit, str) or not re.fullmatch(r"[0-9a-f]{40}", accepted_commit):
        raise ValueError("Accepted implementation commit must have an exact immutable identity")
    if collection.get("coordinator_commit") is not None and accepted_commit != collection["coordinator_commit"]:
        raise ValueError("Accepted implementation commit differs")
    build = _pinned_json(collection["build_metadata"])
    selection = _pinned_json(collection["selection_metadata"])
    candidate = Path(collection["candidate_path"]).resolve()
    if output.resolve().is_relative_to(candidate):
        raise ValueError("Wrapper must not write inside accepted candidate")
    selected_generation = collection["accepted_selected_generation"]
    if build["generation"] != selected_generation:
        raise ValueError("Accepted receipt generation differs")
    kinds = {unit["kind"] for unit in selection["sources"]}
    if not kinds or not kinds <= set(TABLES):
        raise ValueError("Unknown accepted native collection kind")
    expected = {table for kind in kinds for table in TABLES[kind]}
    if (
        expected != set(build["outputs"])
        or expected != {item["table"] for item in collection["members"]}
        or len(collection["members"]) != len(expected)
    ):
        raise ValueError("Wrapper omits or adds an accepted native table")
    for table in expected:
        if publication.table_owner(read_snapshot, table + ".parquet") is not None:
            raise ValueError("Selected table already belongs to another published family")
        if any(
            table in entry.get("etlReceipts", {}).get("datasets", []) for entry in read_snapshot["families"].values()
        ):
            raise ValueError("Selected receipt dataset already belongs to another published family")
    policies = [POLICIES[table] for table in sorted(expected)]
    registered = installed_policies()
    missing = []
    for policy in policies:
        if policy.dataset not in registered:
            missing.append(policy.dataset)
        elif registered[policy.dataset].descriptor() != policy.descriptor():
            raise ValueError("Installed native receipt policy differs")
    if missing and not preview:
        raise ValueError("Native receipt policies must be installed before complete packaging")

    declarations = []
    for item in collection["members"]:
        table = item["table"]
        info = build["outputs"][table]
        source = candidate / info["path"]
        if not source.resolve().is_relative_to(candidate) or source.resolve() != Path(item["absolute_path"]).resolve():
            raise ValueError("Accepted member path differs")
        if (item["path"], item["rows"], item["sha256"]) != (info["path"], info["rows"], info["sha256"]):
            raise ValueError("Accepted member declaration differs")
        declarations.append((source, table + ".parquet", item["current_stat_bytes"], info["sha256"]))
    receipt = collection["combined_receipts"]
    receipt_path = candidate / "etl_receipts.parquet"
    if Path(receipt["path"]).resolve() != receipt_path.resolve() or receipt["sha256"] != build["receipts_sha256"]:
        raise ValueError("Accepted combined receipts differ")
    declarations.append((receipt_path, "etl_receipts.parquet", receipt["current_stat_bytes"], receipt["sha256"]))
    for source, _, size, digest in declarations:
        _check_file(source, size, digest)

    output.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=".retained-native-wrapper-", dir=output.parent) as temporary:
        stage = Path(temporary)
        generation = stage / "generation"
        generation.mkdir()
        for source, key, _, _ in declarations:
            # EXDEV refuses: a payload copy is never a fallback. Never chmod an alias.
            os.link(source, generation / key)
            if (generation / key).is_symlink():
                raise ValueError("Accepted payload alias must be a regular hardlink")
        tables = {
            table + ".parquet": generations._table_info(generation / (table + ".parquet")) for table in sorted(expected)
        }
        for table in sorted(expected):
            info = tables[table + ".parquet"]
            if info["rows"] != build["outputs"][table]["rows"]:
                raise ValueError("Accepted native row count differs")
            if not pq.read_schema(generation / (table + ".parquet")).equals(POLICIES[table].subject_schema):
                raise ValueError("Accepted native schema differs from its receipt policy")
        receipt_info = generations._table_info(generation / "etl_receipts.parquet")
        receipt_spec = {
            "key": "etl_receipts.parquet",
            "generationId": selected_generation,
            "policies": [policy.descriptor() for policy in policies],
            **receipt_info,
        }
        require_registered_receipts(tables, receipt_spec)
        evidence = CaptureEvidence(stage, family)
        evidence.inherit(read_snapshot, public_url=None)
        evidence.event(
            "accepted-native-packaging",
            source_generation=selected_generation,
            accepted_commit=accepted_commit,
            accepted_proof_controls={
                key: {
                    "bytes": collection[key]["bytes"],
                    "sha256": "sha256:" + collection[key]["sha256"].removeprefix("sha256:"),
                }
                for key in ("coordinator_acceptance", "build_metadata", "selection_metadata")
            },
            accepted_scope=collection["accepted_scope_limits"],
            source_reads=0,
            new_capture_claim=False,
            new_asset_acquisition=False,
            metadata_basis="Pinned accepted source proofs; source bodies are not reread or added to this artifact",
            subject_members=[{"key": key, "sha256": digest, "bytes": size} for _, key, size, digest in declarations],
        )
        inputs = evidence.inputs()
        member_source = LocalMemberSource(generation)
        members = [
            describe_member(
                member_source,
                object_key=key,
                role="table",
                media_type="application/vnd.apache.parquet",
                record_count=receipt_info["rows"] if key == "etl_receipts.parquet" else tables[key]["rows"],
            )
            for _, key, _, _ in sorted(declarations, key=lambda value: value[1])
        ]
        generations._write_generation_metadata(
            generation,
            family=family,
            tables=tables,
            members=members,
            read_snapshot=read_snapshot,
            publication_status="local-partial" if preview else "complete-family",
            inputs=inputs,
            etl_receipts=receipt_spec,
        )
        artifact = generations.verify_generation(generation)
        from spicy_regs.source_evidence import verify_evidence

        verify_evidence(evidence.artifact_dir)
        for source, _, size, digest in declarations:
            _check_file(source, size, digest)
        for key in ("coordinator_acceptance", "build_metadata", "selection_metadata"):
            _pinned_json(collection[key])
        # The payload aliases and source-evidence metadata move together.
        result = {
            "state": "local-preview-wrapper" if preview else "managed-wrapper-prepared",
            "family": family,
            "artifact": artifact.pin.as_dict(),
            "source_generation": selected_generation,
            "receipt_generation_preserved": True,
            "source_reads": 0,
            "payload_copies": 0,
            "missing_installed_policies": missing,
            "generation_directory": "generation",
            "evidence_directory": str(evidence.artifact_dir.relative_to(stage)),
            "publication_or_reader_admission": "not performed",
        }
        (stage / "preparation.json").write_text(json.dumps(result, indent=2) + "\n")
        evidence.finish()
        publish_directory_no_replace(stage, output)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--lane", required=True)
    parser.add_argument("--family", required=True)
    parser.add_argument("--read-snapshot", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--preview", action="store_true")
    args = parser.parse_args()
    supplement = json.loads(args.metadata.read_bytes())
    collections = [item for item in supplement["collections"] if item["lane"] == args.lane]
    if len(collections) != 1:
        raise ValueError("Exact accepted collection lane required")
    print(
        json.dumps(
            prepare_selected(
                collections[0],
                family=args.family,
                read_snapshot=json.loads(args.read_snapshot.read_bytes()),
                output=args.output,
                preview=args.preview,
            )
        )
    )


if __name__ == "__main__":
    main()
