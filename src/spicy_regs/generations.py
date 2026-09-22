"""Complete rollup artifacts, verified with Rulespec before publication.

The platform library owns manifests, byte identity and membership. This module
only binds a declared set of Parquet outputs to their observed schema/counts.
It does not qualify source coverage or interpreted fields.
"""

from __future__ import annotations

import hashlib
import shutil
from collections.abc import Mapping, Sequence
from importlib.metadata import version
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

KIND = "spicy-regs-rollup-generation"
MANIFEST = "members.json"


def _table_info(path: Path) -> dict:
    with duckdb.connect() as con:
        columns = con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
    # A readable footer does not establish readable data pages. Decode every
    # column in bounded batches before qualifying these bytes for publication.
    with pq.ParquetFile(path) as parquet:
        rows = sum(batch.num_rows for batch in parquet.iter_batches(batch_size=65536))
        if rows != parquet.metadata.num_rows:
            raise ValueError(f"Parquet body/footer row mismatch: {path.name}")
    return {
        "columns": [[row[0], row[1]] for row in columns],
        "rows": rows,
    }


def _implementation_id() -> str:
    digest = hashlib.sha256()
    root = Path(__file__).parent
    for path in sorted(root.rglob("*.py")):
        digest.update(path.relative_to(root).as_posix().encode() + b"\0")
        digest.update(path.read_bytes())
    return "urn:spicy-regs:implementation:sha256:" + digest.hexdigest()


def verify_generation(directory: Path, *, expected_pin=None):
    """Hash every member and reconcile actual Parquet counts/shape; fail closed."""
    from rulespec_artifacts import LocalMemberSource, admit_artifact, iter_member_descriptors

    source = LocalMemberSource(directory)
    artifact = admit_artifact(source, expected_pin=expected_pin)
    root = artifact.root
    if root["kind"] != KIND:
        raise ValueError("Not a SpicyRegs rollup generation")
    spec = root["spec"]
    if set(spec) != {"family", "tables", "packages", "readSnapshot", "carriedForward", "publicationStatus"}:
        raise ValueError("Invalid rollup generation specification")
    if spec["publicationStatus"] not in {"complete-family", "local-partial"}:
        raise ValueError("Invalid generation publication status")
    tables = spec["tables"]
    from spicy_regs.sources.publication import parse_index, table_location
    from rulespec_artifacts import canonical_json_bytes

    snapshot = spec["readSnapshot"]
    if snapshot:
        parse_index(canonical_json_bytes(snapshot))
    carried = spec["carriedForward"]
    from spicy_regs.source_evidence import INPUT_ROLE, PRIOR_ROLE

    inputs = root["inputs"]
    evidence = [item for item in inputs if item["role"] == INPUT_ROLE]
    prior_inputs = [item for item in inputs if item["role"] == PRIOR_ROLE]
    if inputs:
        prior = snapshot.get("families", {}).get(spec["family"])
        expected_prior = ([{"role": PRIOR_ROLE, "logicalId": prior["logicalId"],
                            "artifactDigest": prior["artifactDigest"]}] if prior else [])
        if len(evidence) != 1 or prior_inputs != expected_prior or len(inputs) != 1 + len(prior_inputs):
            raise ValueError("Generation evidence lineage differs from its captured prior")
    if not isinstance(carried, dict) or not set(carried) <= set(tables):
        raise ValueError("Invalid carried-forward declarations")
    members = list(iter_member_descriptors(artifact, source))
    if not tables or {m.object_key for m in members} != set(tables):
        raise ValueError("Generation membership differs from its table declarations")
    for member in members:
        key = member.object_key
        if key is None or Path(key).name != key or not key.endswith(".parquet"):
            raise ValueError("Generation members must be plain Parquet filenames")
        actual = _table_info(directory / key)
        if actual != tables[key] or member.record_count != actual["rows"]:
            raise ValueError(f"Generation table shape/count mismatch: {key}")
        if key in carried:
            prior = snapshot.get("families", {}).get(spec["family"])
            descriptor = table_location(snapshot, key)[1] if prior else None
            if (
                prior is None
                or carried[key] != prior["artifactDigest"]
                or descriptor is None
                or descriptor["sha256"] != member.sha256
                or descriptor["byteSize"] != member.byte_size
            ):
                raise ValueError(f"Carried-forward bytes differ from their captured generation: {key}")
    return artifact


def build_generation(
    directory: Path,
    *,
    family: str,
    files: Sequence[Path],
    expected_keys: Sequence[str],
    schemas: Mapping[str, list[tuple[str, str]]] | None = None,
    read_snapshot: Mapping | None = None,
    carried_forward: Mapping[str, str] | None = None,
    publication_status: str = "complete-family",
    inputs=(),
):
    """Snapshot exactly one declared family into a new immutable artifact.

    A successful empty table is a Parquet file with zero rows. An omitted
    output is a failure, never an empty success. No root is written until the
    complete set and all declared schemas have been inspected.
    """
    from rulespec_artifacts import (
        LocalMemberSource,
        Producer,
        build_artifact_root,
        canonical_json_bytes,
        describe_member,
        write_member_manifest,
    )

    expected = set(expected_keys)
    names = [path.name for path in files]
    if not expected or len(expected) != len(expected_keys) or len(set(names)) != len(names) or set(names) != expected:
        raise ValueError("Build outputs differ from the declared complete family")
    if any(Path(key).name != key or not key.endswith(".parquet") for key in expected):
        raise ValueError("Output keys must be plain Parquet filenames")
    directory.mkdir(parents=True, exist_ok=False)
    tables = {}
    for path in sorted(files):
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"Output is not a regular file: {path.name}")
        target = directory / path.name
        shutil.copyfile(path, target)
        info = _table_info(target)
        declared = (schemas or {}).get(path.stem)
        if declared is not None and info["columns"] != [list(column) for column in declared]:
            raise ValueError(f"Output differs from the declared schema: {path.name}")
        tables[path.name] = info
    source = LocalMemberSource(directory)
    members = [
        describe_member(
            source,
            object_key=key,
            role="table",
            media_type="application/vnd.apache.parquet",
            record_count=tables[key]["rows"],
        )
        for key in sorted(tables)
    ]
    with (directory / MANIFEST).open("wb") as stream:
        manifest = write_member_manifest(
            stream, scope_kind="global", scope_id=family, object_key=MANIFEST, members=members
        )
    implementation = _implementation_id()
    root = build_artifact_root(
        kind=KIND,
        spec={
            "family": family,
            "tables": tables,
            "packages": {name: version(name) for name in ("spicy-regs", "spicy-docs", "rulespec-artifacts")},
            "readSnapshot": dict(read_snapshot or {}),
            "carriedForward": dict(carried_forward or {}),
            "publicationStatus": publication_status,
        },
        producer=Producer("spicy-regs", implementation, "urn:spicy-regs:rollup-verifier", "1", implementation),
        manifests=[manifest],
        inputs=inputs,
    )
    (directory / "artifact.json").write_bytes(canonical_json_bytes(root))
    return verify_generation(directory)
