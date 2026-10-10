"""Complete rollup artifacts, verified with Rulespec before publication.

The platform library owns manifests, byte identity and membership. This module
only binds a declared set of Parquet outputs to their observed schema/counts.
It does not qualify source coverage or interpreted fields.
"""

from __future__ import annotations

import functools
import hashlib
import re
import shutil
import stat
from collections.abc import Callable, Mapping, Sequence
from importlib.metadata import version
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

KIND = "spicy-regs-rollup-generation"
MANIFEST = "members.json"
_PART_NAME = re.compile(r"part-\d{6}\.parquet\Z")


def _table_info(path: Path) -> dict:
    """Observed columns and row count for one Parquet file; decoding all pages catches a body/footer mismatch.

    Hive partitioning is off: a split member's ``col=value`` directory must not add a column its file lacks.
    """
    info = _table_footer_info(path)
    # A readable footer does not establish readable data pages. Decode every
    # column in bounded batches before qualifying these bytes for publication.
    with pq.ParquetFile(path) as parquet:
        rows = sum(batch.num_rows for batch in parquet.iter_batches(batch_size=2000))
        if rows != parquet.metadata.num_rows:
            raise ValueError(f"Parquet body/footer row mismatch: {path.name}")
    return info


def _table_footer_info(path: Path) -> dict:
    """Describe a footer; complete body decoding must be owned by the caller's admission."""
    with duckdb.connect() as con:
        columns = con.execute("DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning = false)",
                              [str(path)]).fetchall()
    return {"columns": [[row[0], row[1]] for row in columns], "rows": pq.read_metadata(path).num_rows}


def _check_partition(path: Path, partition: Mapping[str, str]) -> None:
    """Every row of a split member holds the partition values its key names, compared as text."""
    selected = ", ".join('CAST("' + column.replace('"', '""') + '" AS VARCHAR)' for column in partition)
    with duckdb.connect() as con:
        found = con.execute(f"SELECT DISTINCT {selected} FROM read_parquet(?, hive_partitioning = false)",
                            [str(path)]).fetchall()
    if any(row != tuple(partition.values()) for row in found):
        raise ValueError(f"Split member rows differ from the partition its key names: {path.name}")


def source_digest(root: Path, suffixes: tuple[str, ...] = (".py",)) -> str:
    """SHA-256 over every file under ``root`` with one of ``suffixes``: its relative path, then its bytes."""
    digest = hashlib.sha256()
    for path in sorted(p for p in root.rglob("*") if p.suffix in suffixes and "__pycache__" not in p.parts):
        digest.update(path.relative_to(root).as_posix().encode() + b"\0")
        digest.update(path.read_bytes())
    return digest.hexdigest()


@functools.cache
def spicy_docs_code() -> str:
    """Digest of the installed SpicyDocs' code and data (.py, .json, .xsd): what reads, derives and shapes a source.

    A re-read keyed on it, not on the release string, skips a version-only release (0.39.2
    changed one README line against 0.39.1). The whole package is digested, not the modules a
    caller imports, because a missed transitive module would leave stale rows silently; a
    release touching only another source still re-reads, which costs requests, not correctness.
    """
    import spicy_docs

    return source_digest(Path(spicy_docs.__file__).parent, (".py", ".json", ".xsd"))


def implementation_id() -> str:
    """Content digest of this package's Python sources, recorded in every generation root."""
    return "urn:spicy-regs:implementation:sha256:" + source_digest(Path(__file__).parent)


def verify_generation(directory: Path, *, expected_pin=None):
    """Hash every member and reconcile actual Parquet counts/shape; fail closed."""
    from rulespec_artifacts import LocalMemberSource

    return verify_generation_source(
        LocalMemberSource(directory), lambda key: _table_info(directory / key), expected_pin=expected_pin
    )


def verify_generation_source(source, table_info: Callable[[str], dict], *, expected_pin=None):
    """Apply the same artifact and table checks to local or pinned remote bytes."""
    from spicy_regs.etl_receipts import validate_receipt_bundle

    return _verify_generation_source(source, table_info, expected_pin=expected_pin,
                                     admit_receipts=validate_receipt_bundle)


def _verify_generation_source(source, table_info, *, expected_pin, admit_receipts):
    """Same product checks with admission retained only by an explicit conversion owner."""
    from rulespec_artifacts import admit_artifact

    artifact = admit_artifact(source, expected_pin=expected_pin)
    try:
        return _verify_admitted_generation(artifact, source, table_info, admit_receipts)
    except BaseException:
        if artifact.local_member_states is not None:
            close = getattr(artifact.local_member_states, 'close', None)
            if close is not None:
                close()
        raise


def _verify_admitted_generation(artifact, source, table_info, admit_receipts):
    from rulespec_artifacts import iter_member_descriptors

    root = artifact.root
    if root["kind"] != KIND:
        raise ValueError("Not a SpicyRegs rollup generation")
    spec = root["spec"]
    if set(spec) - {"parents", "etlReceipts"} != {"family", "tables", "packages", "readSnapshot", "carriedForward", "publicationStatus"}:
        raise ValueError("Invalid rollup generation specification")
    if spec["publicationStatus"] not in {"complete-family", "local-partial"}:
        raise ValueError("Invalid generation publication status")
    tables = spec["tables"]
    from spicy_regs.sources.publication import member_table, parse_index, table_descriptor, table_entries
    from rulespec_artifacts import canonical_json_bytes

    snapshot = spec["readSnapshot"]
    if snapshot:
        parse_index(canonical_json_bytes(snapshot))
    _check_parents(spec.get("parents", {}), snapshot)
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
    if any("partitionColumns" in tables[key] for key in carried):
        raise ValueError("A split table is carried forward member by member at publication, not declared whole")
    members = list(iter_member_descriptors(artifact, source))
    receipt_spec = spec.get("etlReceipts")
    receipt_members = [member for member in members if member.object_key == "etl_receipts.parquet"]
    if bool(receipt_spec) != bool(receipt_members) or len(receipt_members) > 1:
        raise ValueError("Generation receipt membership differs from its declaration")
    from spicy_regs.receipt_key_index import KEY as RECEIPT_INDEX_KEY, validate_descriptor, verify_key_index
    index_members = [member for member in members if member.object_key == RECEIPT_INDEX_KEY]
    index_spec = receipt_spec.get("keyIndex") if receipt_spec else None
    declared_index = receipt_spec is not None and "keyIndex" in receipt_spec
    if declared_index != bool(index_members) or len(index_members) > 1:
        raise ValueError("Generation receipt key index membership differs from its declaration")
    if declared_index:
        if not isinstance(index_spec, dict):
            raise ValueError("Invalid generation receipt key index declaration")
        receipt_member = receipt_members[0]
        receipt_pin = {"sha256": receipt_member.sha256, "byteSize": receipt_member.byte_size,
                       "rows": receipt_member.record_count}
        validate_descriptor(index_spec, receipt_pin)
        member = index_members[0]
        if (member.sha256, member.byte_size, member.record_count) != (
                index_spec["sha256"], index_spec["byteSize"], index_spec["rows"]):
            raise ValueError("Generation receipt key index differs from its declared member")
        verify_key_index(lambda: source.open("etl_receipts.parquet"), lambda: source.open(RECEIPT_INDEX_KEY),
                         index_spec, receipt_pin)
    table_members_only = [member for member in members
                          if member.object_key not in {"etl_receipts.parquet", RECEIPT_INDEX_KEY}]
    if tables or table_members_only:
        table_entries(tables, table_members_only)
    elif not receipt_spec:
        raise ValueError("Generation has neither subjects nor receipts")
    if receipt_spec:
        if receipt_members[0].record_count != receipt_spec['rows']:
            raise ValueError('Generation receipt member count differs from receipt declaration')
        _verify_receipts(source, tables, table_members_only, receipt_spec, table_info, admit_receipts)
    for member in table_members_only:
        key = member.object_key
        if key is None:
            raise ValueError("Generation members must be plain Parquet filenames")
        table = tables[member_table(key)]
        if "partitionColumns" in table:
            actual = table_info(key)
            if actual["columns"] != table["columns"] or member.record_count != actual["rows"]:
                raise ValueError(f"Generation table shape/count mismatch: {key}")
            continue
        if Path(key).name != key or not key.endswith(".parquet"):
            raise ValueError("Generation members must be plain Parquet filenames")
        actual = table_info(key)
        if actual != tables[key] or member.record_count != actual["rows"]:
            raise ValueError(f"Generation table shape/count mismatch: {key}")
        if key in carried:
            prior = snapshot.get("families", {}).get(spec["family"])
            descriptor = table_descriptor(snapshot, key) if prior else None
            if (
                prior is None
                or carried[key] != prior["artifactDigest"]
                or descriptor is None
                or descriptor["sha256"] != member.sha256
                or descriptor["byteSize"] != member.byte_size
            ):
                raise ValueError(f"Carried-forward bytes differ from their captured generation: {key}")
    return artifact


_PARENT_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")


def _check_parents(parents, snapshot: Mapping) -> None:
    """Each parent names its bytes or storage version; a managed one agrees with the captured index."""
    from spicy_regs.sources.publication import table_owner, table_pin

    if not isinstance(parents, dict):
        raise ValueError("Invalid generation parents")
    for key, parent in parents.items():
        fields = set(parent) - {"family", "artifactDigest"}
        size = parent.get("byteSize")
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            raise ValueError(f"Invalid parent size: {key}")
        if fields == {"sha256", "byteSize"}:
            if not _PARENT_DIGEST.fullmatch(str(parent["sha256"])):
                raise ValueError(f"Invalid parent digest: {key}")
        elif fields == {"tableDescriptorDigest", "byteSize"}:
            if ("family" not in parent
                    or not _PARENT_DIGEST.fullmatch(str(parent["tableDescriptorDigest"]))):
                raise ValueError(f"Invalid split-table parent digest: {key}")
        elif fields != {"etag", "byteSize"} or not isinstance(parent["etag"], str) or not parent["etag"]:
            raise ValueError(f"Invalid parent version: {key}")
        if ("family" in parent) != ("artifactDigest" in parent):
            raise ValueError(f"Parent family and generation must be stated together: {key}")
        if "family" in parent:
            owner = table_owner(snapshot or {"families": {}}, key)
            if (owner is None or owner[0] != parent["family"]
                    or owner[1]["artifactDigest"] != parent["artifactDigest"]
                    or owner[1]["tables"][key]["byteSize"] != size
                    or ("sha256" in parent or "tableDescriptorDigest" in parent)
                    and table_pin(snapshot, key) != parent):
                raise ValueError(f"Parent differs from its captured family: {key}")


def output_keys(files: Sequence[Path], partitioned: Mapping[str, Sequence[str]]) -> list[str]:
    """Each output's table key: a split table's directory ``<name>`` is ``<name>.parquet``, any other file its name."""
    return [f"{path.name}.parquet" if f"{path.name}.parquet" in partitioned else path.name for path in files]


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
    parents: Mapping[str, Mapping] | None = None,
    partitioned: Mapping[str, Sequence[str]] | None = None,
    receipt_path: Path | None = None,
    receipt_policies: Sequence | None = None,
    receipt_generation_id: str | None = None,
    receipt_index: tuple[Path, Mapping] | None = None,
    adopt_owned_receipt: bool = False,
    read_operation=None,
    canonical_root: Path | None = None,
):
    """Snapshot exactly one declared family into a new immutable artifact.

    ``parents`` records each table of another family the build read: the digest
    and size of its bytes, or the storage version of an input read in place,
    plus the family and generation when a managed family published it. The
    family's own prior is its prior-generation input, never a parent; a parent
    of the generation's own family is refused here, while the verifier still
    admits the roots published that way before this rule.

    ``partitioned`` maps a table stored as several files to its partition
    columns. Its entry in ``files`` is a directory named for the table holding
    ``<col>=<value>/.../part-NNNNNN.parquet`` in column order; every row of a
    member must hold its key's values, and every member the same columns.

    A successful empty table is a Parquet file with zero rows. An omitted
    output is a failure, never an empty success. No root is written until the
    complete set and all declared schemas have been inspected.

    ``adopt_owned_receipt`` moves a caller-owned temporary receipt into this
    generation instead of copying it. It requires a regular file on the same
    filesystem. After the move, this directory retains the receipt even if
    admission fails; callers must preserve the directory for recovery. Retained
    source receipts use the default copy behavior.

    ``receipt_index`` explicitly adopts an already built key sidecar and its
    descriptor. Normal builds create no index. Full generation admission checks
    its exact receipt binding and every position before this artifact can publish.
    """
    from rulespec_artifacts import (
        LocalMemberSource,
        describe_member,
    )

    if any(parent.get("family") == family for parent in (parents or {}).values()):
        raise ValueError("A generation's own prior is its prior-generation input, not a parent")
    expected, partitioned = set(expected_keys), dict(partitioned or {})
    names = output_keys(files, partitioned)
    if adopt_owned_receipt and receipt_path is None:
        raise ValueError("Owned receipt adoption requires a receipt file")
    if receipt_index is not None and receipt_path is None:
        raise ValueError("Receipt index adoption requires a receipt file")
    if (not expected and receipt_path is None) or len(expected) != len(expected_keys) or len(set(names)) != len(names) or set(names) != expected:
        raise ValueError("Build outputs differ from the declared complete family")
    if any(Path(key).name != key or not key.endswith(".parquet") for key in expected):
        raise ValueError("Output keys must be plain Parquet filenames")
    directory.mkdir(parents=True, exist_ok=False)
    table_info = _table_footer_info if read_operation is not None else _table_info
    tables, rows = {}, {}
    for key, path in sorted(zip(names, files)):
        if key in partitioned:
            columns = list(partitioned[key])
            tables[key] = {**_copy_split(path, directory, columns, rows, table_info=table_info), "partitionColumns": columns}
        else:
            if path.is_symlink() or not path.is_file():
                raise ValueError(f"Output is not a regular file: {path.name}")
            shutil.copyfile(path, directory / key)
            tables[key] = table_info(directory / key)
            rows[key] = tables[key]["rows"]
        declared = (schemas or {}).get(Path(key).stem)
        if declared is not None and tables[key]["columns"] != [list(column) for column in declared]:
            raise ValueError(f"Output differs from the declared schema: {key}")
    receipt_spec = None
    if receipt_path is not None:
        from spicy_regs.etl_receipts import RECEIPT_KEY
        if not receipt_policies or not receipt_generation_id or RECEIPT_KEY in tables:
            raise ValueError("Receipt admission needs policies and a generation identity")
        if adopt_owned_receipt:
            source_stat = receipt_path.lstat()
            if not stat.S_ISREG(source_stat.st_mode):
                raise ValueError("Owned receipt adoption requires a regular file without a symlink")
            if source_stat.st_dev != directory.stat().st_dev:
                raise ValueError("Owned receipt adoption requires the same filesystem")
            receipt_path.rename(directory / RECEIPT_KEY)
        else:
            shutil.copyfile(receipt_path, directory / RECEIPT_KEY)
        receipt_spec = {"key": RECEIPT_KEY, "generationId": receipt_generation_id,
                        "policies": [policy.descriptor() for policy in receipt_policies],
                        **table_info(directory / RECEIPT_KEY)}
        rows[RECEIPT_KEY] = receipt_spec["rows"]
        if receipt_index is not None:
            from spicy_regs.receipt_key_index import KEY as RECEIPT_INDEX_KEY
            index_path, index_descriptor = receipt_index
            if (index_path.is_symlink() or not index_path.is_file()
                    or RECEIPT_INDEX_KEY in tables):
                raise ValueError("Receipt index adoption requires a separate regular sidecar")
            shutil.copyfile(index_path, directory / RECEIPT_INDEX_KEY)
            receipt_spec["keyIndex"] = dict(index_descriptor)
            rows[RECEIPT_INDEX_KEY] = table_info(directory / RECEIPT_INDEX_KEY)["rows"]
    elif receipt_policies is not None or receipt_generation_id is not None:
        raise ValueError("Receipt admission cannot omit the shared receipt member")
    from spicy_regs.etl_policy_registry import require_registered_receipts
    require_registered_receipts(tables, receipt_spec)
    source = LocalMemberSource(directory)
    members = [
        describe_member(
            source,
            object_key=key,
            role="table",
            media_type="application/vnd.apache.parquet",
            record_count=count,
        )
        for key, count in sorted(rows.items())
    ]
    _write_generation_metadata(
        directory, family=family, tables=tables, members=members, read_snapshot=read_snapshot,
        carried_forward=carried_forward, publication_status=publication_status, inputs=inputs,
        parents=parents, etl_receipts=receipt_spec,
    )
    if read_operation is not None:
        from rulespec_artifacts import ArtifactPin, parse_admitted_json, publish_directory_no_replace
        if canonical_root is not None:
            root = parse_admitted_json((directory / "artifact.json").read_bytes())
            pin = ArtifactPin(root["logicalId"], root["artifactDigest"])
            destination = canonical_root / pin.artifact_digest.removeprefix("sha256:")
            if not destination.exists():
                publish_directory_no_replace(directory, destination)
            directory = destination
        else:
            pin = None
        return read_operation.admit_generation(directory, expected_pin=pin)[0]
    if canonical_root is not None:
        raise ValueError("Canonical adoption requires an owned conversion read operation")
    return verify_generation(directory)


def _copy_split(path: Path, directory: Path, columns: list[str], rows: dict[str, int], *, table_info=_table_info) -> dict:
    """Copy one split table's members into ``directory`` under ``<table>/``; return its columns and summed rows.

    Each member's row count is recorded in ``rows`` by its object key.
    """
    if path.is_symlink() or not path.is_dir():
        raise ValueError(f"A split output is not a directory: {path.name}")
    found = sorted(member for member in path.rglob("*") if member.is_symlink() or not member.is_dir())
    if not found:
        raise ValueError(f"A split output has no members: {path.name}")
    info, total = None, 0
    for member in found:
        relative = member.relative_to(path).as_posix()
        partition = relative.split("/")[:-1]
        if (member.is_symlink() or not member.is_file() or not _PART_NAME.fullmatch(member.name)
                or [part.partition("=")[0] for part in partition] != columns or not all("=" in p for p in partition)):
            raise ValueError(f"A split member is not <col>=<value>/.../part-NNNNNN.parquet: {path.name}/{relative}")
        key = f"{path.name}/{relative}"
        (directory / key).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(member, directory / key)
        observed = table_info(directory / key)
        if info is not None and observed["columns"] != info["columns"]:
            raise ValueError(f"Members of a split table differ in columns: {key}")
        _check_partition(directory / key, dict(part.split("=", 1) for part in partition))
        info, rows[key] = observed, observed["rows"]
        total += observed["rows"]
    assert info is not None
    return {"columns": info["columns"], "rows": total}


def _write_generation_metadata(
    directory: Path, *, family: str, tables: Mapping, members,
    read_snapshot: Mapping | None = None, carried_forward: Mapping[str, str] | None = None,
    publication_status: str = "complete-family", inputs=(), extra_packages: Sequence[str] = (),
    parents: Mapping[str, Mapping] | None = None,
    etl_receipts: Mapping | None = None,
):
    """Write the existing manifest/root format; callers must then verify bytes."""
    from rulespec_artifacts import Producer, build_artifact_root, canonical_json_bytes, write_member_manifest

    with (directory / MANIFEST).open("wb") as stream:
        manifest = write_member_manifest(
            stream, scope_kind="global", scope_id=family, object_key=MANIFEST, members=members
        )
    implementation = implementation_id()
    root = build_artifact_root(
        kind=KIND,
        spec={
            "family": family,
            "tables": tables,
            "packages": {
                name: version(name) for name in ("spicy-regs", "spicy-docs", "rulespec-artifacts", *extra_packages)
            },
            "readSnapshot": dict(read_snapshot or {}),
            "carriedForward": dict(carried_forward or {}),
            "publicationStatus": publication_status,
            **({"etlReceipts": dict(etl_receipts)} if etl_receipts is not None else {}),
            **({"parents": {key: dict(value) for key, value in parents.items()}} if parents else {}),
        },
        producer=Producer("spicy-regs", implementation, "urn:spicy-regs:rollup-verifier", "1", implementation),
        manifests=[manifest],
        inputs=inputs,
    )
    (directory / "artifact.json").write_bytes(canonical_json_bytes(root))


def _verify_receipts(source, tables, members, receipt_spec, table_info, admit_receipts):
    """Recheck all row joins through the same local or pinned remote byte source."""
    from spicy_regs.etl_receipts import DatasetPolicy, RECEIPT_KEY
    from spicy_regs.sources.publication import member_table

    if (not {"key", "generationId", "policies", "columns", "rows"} <= set(receipt_spec) <= {"key", "generationId", "policies", "columns", "rows", "keyIndex"}
            or receipt_spec["key"] != RECEIPT_KEY or not isinstance(receipt_spec["generationId"], str)
            or not receipt_spec["generationId"]):
        raise ValueError("Invalid generation receipt declaration")
    if table_info(RECEIPT_KEY) != {key: receipt_spec[key] for key in ("columns", "rows")}:
        raise ValueError("Receipt member differs from its declared shape/count")
    policies = [DatasetPolicy.from_descriptor(value) for value in receipt_spec["policies"]]
    if {p.dataset + ".parquet" for p in policies if not p.receipt_only} != set(tables):
        raise ValueError("Receipt policies must classify every subject output in a generation")
    subjects = {p.dataset: [] for p in policies}
    for member in members:
        subjects[member_table(member.object_key).removesuffix(".parquet")].append(
            lambda key=member.object_key: source.open(key))
    admit_receipts(subjects, [lambda: source.open(RECEIPT_KEY)], policies,
                   generation_id=receipt_spec["generationId"])
