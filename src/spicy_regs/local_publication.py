"""Publish one locally built generation of a family that no hosted workflow builds.

``--dry-run`` runs the real publication guards against a private disk store seeded with the public
index: no credentials are read and nothing leaves this machine. Without it the command compares with
the stored index, writes to the verified R2 target, then reads the family back as an anonymous reader
and records what was served. The per-family steps are in docs/local-family-refresh.md.

``unpublish-families`` (:func:`unpublish_main`) is the reverse hand write: it takes whole families out of the
index through the same conditional pointer write, a dry run unless told the index ETag to replace.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
from tempfile import TemporaryDirectory
from typing import Any

from spicy_regs.catalog_refresh_request import REQUEST_ENV
from spicy_regs.generations import implementation_id
from spicy_regs.source_evidence import ARCHIVE_REFERENCE, INPUT_ROLE, JOURNAL, is_original
from spicy_regs.sources import publication, r2
from spicy_regs.sources.publication import PublicationError

#: Families refreshed by hand. A family with a hosted workflow is published by that workflow, never from here.
#: court-opinions has none: its 51.5 GiB export takes longer to download than a hosted job may run
#: (docs/court-receipt-archive.md). uscode-editions has none: each edition is added by an explicit decision, after
#: an independent audit of its original (docs/uscode-editions.md).
LOCAL_FAMILIES = frozenset({
    "gsa-iolp",
    "census-acs",
    "census-geocoder",
    "cms-medicaid",
    "cms-open-payments",
    "cms-part-b",
    "cms-part-d",
    "court-opinions",
    "cms-medicare-provider",
    "executive-disclosures",
    "fec-election-results",
    "fec-research-profiles",
    "lobbying-quarterly-profiles",
    "fara",
    "grants-status",
    "historical-vetoes",
    "hospital-cost-reports",
    "house-disbursement-pages",
    "house-disclosures",
    "fjc-judicial-careers",
    "irs-527",
    "irs-form990",
    "sam-exclusions",
    "sam-opportunities",
    "sec-edgar",
    "table3-history",
    "foreign-assistance",
    "usaspending-awards",
    "uscode-editions",
    "white-house-actions",
})
RAW_ROOT_ENV = "SPICYGOV_RAW_ROOT"
#: Either one loosens the publisher's shrink guard (``r2._assert_upload_safe``).
SHRINK_OVERRIDES = ("R2_ALLOW_SHRINK", "R2_MIN_SIZE_RATIO")
#: Free space that ``publish-local-generation --dry-run`` must leave on the target volume (40 GiB).
DRY_RUN_FREE_FLOOR_BYTES = 40 * 1024**3


def _family_schemas(family: str) -> dict | None:
    """Declared Arrow ``SCHEMAS`` for local families with explicit Arrow schema modules."""
    if family == "census-acs":
        from spicy_regs.transforms.build_census_acs import SCHEMAS
        return SCHEMAS
    if family == "census-geocoder":
        from spicy_regs.transforms.build_census_geocoder import SCHEMAS
        return SCHEMAS
    if family == "cms-medicaid":
        from spicy_regs.transforms.build_cms_medicaid import SCHEMA, TABLE
        return {TABLE: SCHEMA}
    if family == "cms-medicare-provider":
        from spicy_regs.transforms.build_cms_provider import SCHEMA, TABLE
        return {TABLE: SCHEMA}
    if family == "cms-open-payments":
        from spicy_regs.transforms.build_cms_open_payments import SCHEMA, TABLE
        return {TABLE: SCHEMA}
    if family == "cms-part-b":
        from spicy_regs.transforms.build_cms_part_b import SCHEMA, TABLE
        return {TABLE: SCHEMA}
    if family == "cms-part-d":
        from spicy_regs.transforms.build_cms_part_d import SCHEMA, TABLE
        return {TABLE: SCHEMA}
    if family == "executive-disclosures":
        from spicy_regs.transforms.build_executive_disclosures import SCHEMAS
        return SCHEMAS
    if family == "fara":
        from spicy_regs.transforms.build_fara import SCHEMAS
        return SCHEMAS
    if family == "fec-election-results":
        from spicy_regs.transforms.fec_election_results import SCHEMAS
        return SCHEMAS
    if family == "fec-research-profiles":
        from spicy_regs.pipelines.fec_research_profiles import SCHEMAS
        return SCHEMAS
    if family == "fjc-judicial-careers":
        from spicy_regs.transforms.build_fjc import SCHEMAS
        return SCHEMAS
    if family == "foreign-assistance":
        from spicy_regs.transforms.build_foreign_assistance import SCHEMAS
        return SCHEMAS
    if family == "grants-status":
        from spicy_regs.transforms.build_grants_status import SCHEMAS
        return SCHEMAS
    if family == "gsa-iolp":
        from spicy_regs.transforms.build_gsa_iolp import SCHEMAS
        return SCHEMAS
    if family == "historical-vetoes":
        from spicy_regs.transforms.build_historical_vetoes import SCHEMAS
        return SCHEMAS
    if family == "hospital-cost-reports":
        from spicy_regs.transforms.build_hospital_cost_reports import SCHEMAS
        return SCHEMAS
    if family == "house-disbursement-pages":
        from spicy_regs.schemas.house_disbursement_pages import SCHEMAS
        return SCHEMAS
    if family == "house-disclosures":
        from spicy_regs.pipelines.house_disclosures import SCHEMAS
        return SCHEMAS
    if family == "irs-527":
        from spicy_regs.transforms.build_irs_527 import SCHEMAS
        return SCHEMAS
    if family == "irs-form990":
        from spicy_regs.transforms.build_irs_form990 import SCHEMAS
        return SCHEMAS
    if family == "lobbying-quarterly-profiles":
        from spicy_regs.transforms.build_lobbying_quarterly_profiles import SCHEMA, TABLE
        return {TABLE: SCHEMA}
    if family == "sam-exclusions":
        from spicy_regs.transforms.build_sam_sources import SCHEMAS, SNAPSHOTS, TABLES
        return {key: SCHEMAS[key] for key in (TABLES["exclusions"], SNAPSHOTS["exclusions"])}
    if family == "sam-opportunities":
        from spicy_regs.transforms.build_sam_sources import SCHEMAS, SNAPSHOTS, TABLES
        return {key: SCHEMAS[key] for key in (TABLES["opportunities"], SNAPSHOTS["opportunities"])}
    if family == "sec-edgar":
        from spicy_regs.transforms.build_sec_edgar import SCHEMAS
        return SCHEMAS
    if family == "table3-history":
        from spicy_regs.sources.table3_history import SCHEMAS
        return SCHEMAS
    if family == "usaspending-awards":
        from spicy_regs.transforms.build_usaspending_awards import SCHEMAS
        return SCHEMAS
    if family == "uscode-editions":
        from spicy_regs.sources.uscode_editions import SCHEMAS
        return SCHEMAS
    if family == "white-house-actions":
        from spicy_regs.pipelines.white_house import SCHEMA, TABLE
        return {TABLE: SCHEMA}
    return None


def _parquet_schema_diff(expected, actual) -> list[str]:
    from spicy_regs.odcs_contracts import parquet_schema_diff

    return parquet_schema_diff(expected, actual, check_order=True)


def _resolve_family_odcs_contract(
    family: str,
    *,
    odcs_contract: Path | Mapping[str, Any] | None = None,
    odcs_contracts_dir: Path | None = None,
    require_odcs_contract: bool = False,
) -> tuple[dict[str, Any], str] | None:
    """Load and validate an explicit or manifest-enrolled ODCS v3.2.0 DataContract for ``family``."""
    from spicy_regs.odcs_contracts import (
        DEFAULT_ODCS_DIR,
        ODCSContractError,
        load_enrolled_family_contract,
        validate_odcs_v320_contract,
    )

    if odcs_contract is not None:
        if isinstance(odcs_contract, (str, Path)):
            contract_path = Path(odcs_contract)
            if contract_path.is_symlink():
                raise PublicationError(f"{family} ODCS contract path must not be a symlink: {contract_path}")
            if not contract_path.is_file():
                raise PublicationError(f"{family} ODCS contract file is missing: {contract_path}")
            try:
                contract = json.loads(contract_path.read_bytes())
            except (OSError, ValueError) as exc:
                raise PublicationError(f"{family} ODCS contract {contract_path} could not be parsed: {exc}") from exc
            source = str(contract_path)
        elif isinstance(odcs_contract, Mapping):
            contract = dict(odcs_contract)
            source = str(contract.get("id") or "<in-memory>")
        else:
            raise PublicationError(f"Unsupported ODCS contract input type: {type(odcs_contract).__name__}")
        if errors := validate_odcs_v320_contract(contract):
            raise PublicationError(f"{family} ODCS contract ({source}) is invalid: " + "; ".join(errors))
        declared_family = next(
            (
                cp.get("value")
                for cp in contract.get("customProperties", ())
                if isinstance(cp, Mapping) and cp.get("vendor") == "spicy-regs" and cp.get("property") == "family"
            ),
            None,
        )
        if declared_family is not None and declared_family != family:
            raise PublicationError(
                f"ODCS contract ({source}) declares family {declared_family!r}, not the expected {family!r}"
            )
        return contract, source

    explicit_dir = odcs_contracts_dir is not None
    contracts_dir = DEFAULT_ODCS_DIR if odcs_contracts_dir is None else Path(odcs_contracts_dir)
    if explicit_dir:
        if contracts_dir.is_symlink():
            raise PublicationError(f"{family} ODCS contracts directory must not be a symlink: {contracts_dir}")
        if not contracts_dir.is_dir():
            raise PublicationError(f"{family} ODCS contracts directory does not exist: {contracts_dir}")
        manifest_path = contracts_dir / "manifest.json"
        if manifest_path.is_symlink():
            raise PublicationError(f"{family} ODCS manifest must not be a symlink: {manifest_path}")
        if not manifest_path.is_file():
            raise PublicationError(f"{family} ODCS contracts directory is missing manifest.json: {manifest_path}")
    try:
        loaded = load_enrolled_family_contract(family, contracts_dir=contracts_dir)
    except ODCSContractError as exc:
        raise PublicationError(f"{family} ODCS contract check failed: {exc}") from exc
    if loaded is None:
        if require_odcs_contract:
            raise PublicationError(
                f"{family} requires an enrolled ODCS v3.2.0 DataContract in {contracts_dir}; "
                "pass --odcs-contract or enroll the family before publishing"
            )
        return None
    contract, contract_path = loaded
    return contract, str(contract_path)


def _candidate_table_files(directory: Path, family: str, root: Mapping) -> dict[str, list[Path]]:
    """Resolve every declared table's local Parquet file(s) in ``directory``, failing closed if absent."""
    tables = root.get("spec", {}).get("tables") or {}
    resolved: dict[str, list[Path]] = {}
    for table_key, table_spec in sorted(tables.items()):
        table_name = table_key.removesuffix(".parquet")
        if "partitionColumns" in table_spec:
            member_dir = directory / table_name
            files = sorted(member_dir.rglob("*.parquet")) if member_dir.is_dir() else []
            if not files:
                raise PublicationError(f"{family} partitioned table {table_key!r} has no Parquet member files")
        else:
            parquet_path = directory / table_key
            if not parquet_path.is_file():
                raise PublicationError(f"{family} table {table_key!r} is missing its Parquet file")
            files = [parquet_path]
        resolved[table_name] = files
    return resolved


def _verify_family_schemas(
    directory: Path,
    family: str,
    root: dict,
    *,
    odcs_contract: Path | Mapping[str, Any] | None = None,
    odcs_contracts_dir: Path | None = None,
    require_odcs_contract: bool = False,
) -> dict[str, Any] | None:
    """Refuse any candidate generation whose Parquet schemas drift from declared Arrow or ODCS v3.2.0 contracts."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from spicy_regs.odcs_contracts import (
        ODCSContractError,
        contract_declared_schemas,
        verify_parquet_schema_parity,
    )

    declared = _family_schemas(family)
    resolved_odcs = _resolve_family_odcs_contract(
        family,
        odcs_contract=odcs_contract,
        odcs_contracts_dir=odcs_contracts_dir,
        require_odcs_contract=require_odcs_contract,
    )
    if declared is None and resolved_odcs is None:
        return None

    tables = root.get("spec", {}).get("tables") or {}
    candidate_files = _candidate_table_files(directory, family, root)

    if declared is not None:
        for table_key in sorted(tables):
            table_name = table_key.removesuffix(".parquet")
            if table_name not in declared:
                raise PublicationError(
                    f"{family} candidate generation declares unexpected table {table_key!r}; "
                    f"declared tables are {sorted(declared)}"
                )
            expected = declared[table_name]
            for parquet_path in candidate_files[table_name]:
                relative = parquet_path.relative_to(directory).as_posix()
                try:
                    actual = pq.read_schema(parquet_path)
                except (pa.ArrowException, OSError) as exc:
                    raise PublicationError(
                        f"{family} schema parity check could not read Parquet schema for {table_key} ({relative}): "
                        f"{exc}"
                    ) from exc
                if issues := _parquet_schema_diff(expected, actual):
                    raise PublicationError(
                        f"{family} schema parity check failed for {table_key} ({relative}): "
                        + "; ".join(issues)
                    )

    if resolved_odcs is None:
        return None

    contract, contract_source = resolved_odcs
    try:
        odcs_declared = contract_declared_schemas(contract)
        if declared is not None:
            for table_name in sorted(candidate_files):
                if table_name in odcs_declared and table_name in declared:
                    if contract_issues := _parquet_schema_diff(odcs_declared[table_name], declared[table_name]):
                        raise PublicationError(
                            f"{family} ODCS contract ({contract_source}) disagrees with declared Arrow schema "
                            f"for {table_name!r}: " + "; ".join(contract_issues)
                        )
        parity_summary = verify_parquet_schema_parity(
            odcs_declared,
            candidate_files,
            check_order=True,
            allow_subset_tables=True,
        )
    except ODCSContractError as exc:
        raise PublicationError(f"{family} ODCS schema parity check failed ({contract_source}): {exc}") from exc

    return {
        "contract_id": contract["id"],
        "odcs_api_version": contract["apiVersion"],
        "domain": contract["domain"],
        "source": contract_source,
        "tables": parity_summary,
    }


def _candidate(
    directory: Path,
    family: str,
    evidence: tuple[Path, ...],
    *,
    odcs_contract: Path | Mapping[str, Any] | None = None,
    odcs_contracts_dir: Path | None = None,
    require_odcs_contract: bool = False,
) -> dict:
    """The generation's root, refused unless it is the expected family and pins source evidence.

    Every family here builds with evidence, so a root without it is not one of their builds. Publication
    admits the members and holds ``evidence`` to the root's pins later.
    """
    root = json.loads((directory / "artifact.json").read_bytes())
    if root["spec"]["family"] != family:
        raise PublicationError(f"Generation is family {root['spec']['family']}, not the expected {family}")
    if not evidence or not any(item["role"] == INPUT_ROLE for item in root["inputs"]):
        raise PublicationError(f"A {family} generation is published with the source evidence it pins; pass --evidence")
    _verify_family_schemas(
        directory,
        family,
        root,
        odcs_contract=odcs_contract,
        odcs_contracts_dir=odcs_contracts_dir,
        require_odcs_contract=require_odcs_contract,
    )
    return root


def _require_prior(live: dict | None, expected: str | None, family: str) -> None:
    """Refuse when the live family is not the generation the build was compared against."""
    if expected is None:
        return
    wanted = expected.removeprefix(f"generations/{family}/").removeprefix("sha256:")
    if not re.fullmatch(r"[0-9a-f]{8,64}", wanted):
        raise ValueError("--expect-prior takes a generation digest, its prefix path or its first 8 or more hex digits")
    held = live["artifactDigest"].removeprefix("sha256:") if live else None
    if held is None or not held.startswith(wanted):
        raise PublicationError(
            f"Live {family} is {held or 'absent'}, not the expected prior {wanted}; compare the build with it again"
        )


def _cited_originals(evidence: Path) -> set[str]:
    """The digest of every publisher original an evidence journal says the build read or downloaded.

    Files the build wrote for itself (a selection, a manifest, a normalized input) are published with the
    evidence, not kept in the raw archive, so they are not asked of it.
    """
    cited = set()
    for line in (evidence / JOURNAL).read_text().splitlines():
        event = json.loads(line)
        if is_original(event) and event.get("sha256"):
            cited.add(event["sha256"])
    return cited


def _require_shared_raw(evidence: tuple[Path, ...]) -> None:
    """Refuse a build unless every original it cites is in the shared raw archive.

    A scratch build's tables and evidence pins equal a real build's, so two things are checked. The pointer
    each build leaves beside its evidence must name the shared archive; an evidence directory moved away from
    its pointer is refused. And each original the journal cites must be present in the shared archive by
    digest, which also catches a build that read its originals from another archive than its pointer names.
    Presence is checked by name; the archive's own verification hashes the bytes.
    """
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore, default_raw_archive_root

    if os.environ.get(RAW_ROOT_ENV):
        raise PublicationError(f"Unset {RAW_ROOT_ENV}: a publish reads its originals from the shared raw archive")
    shared = default_raw_archive_root().resolve()
    store = CentralRawArchiveStore(shared, create=False)
    for directory in evidence:
        pointer = directory.parent / ARCHIVE_REFERENCE
        if not pointer.exists():
            raise PublicationError(f"{directory} is not beside the {ARCHIVE_REFERENCE} its build wrote; "
                                   "publish the evidence from its build directory")
        if Path(json.loads(pointer.read_bytes())["root"]).resolve() != shared:
            raise PublicationError("Generation was built on another raw archive than the shared one")
        absent = []
        for digest in sorted(_cited_originals(directory)):
            try:
                with store.open(digest):
                    pass
            except FileNotFoundError:
                absent.append(digest)
        if absent:
            raise PublicationError(f"{len(absent)} cited original(s) are not in the shared raw archive, "
                                   f"first {absent[0]}; acquire into it before publishing")


def _require_default_thresholds() -> None:
    """Refuse when the shrink guard is loosened, so a dry run and the publish it rehearses apply one threshold."""
    if loosened := sorted(name for name in SHRINK_OVERRIDES if os.environ.get(name)):
        raise PublicationError("Unset " + " and ".join(loosened) + ": a hand publish keeps the default shrink guard")


def _prior_index(root: dict, stored: dict) -> dict:
    """The index the build read, when it recorded one: the publisher then refuses a family changed since."""
    return publication.parse_index(json.dumps(root["spec"]["readSnapshot"]).encode()) if (
        root["spec"]["readSnapshot"]) else stored


@contextmanager
def _no_local_side_effects():
    """A dry run neither mirrors its unpublished generation nor asks the catalogue to refresh."""
    held = {name: os.environ.pop(name) for name in (publication.PUBLICATION_MIRROR, REQUEST_ENV) if name in os.environ}
    try:
        yield
    finally:
        os.environ.update(held)


def _public_readback(family: str, entry: dict) -> int:
    """Hold what an anonymous reader is served to the published pins; returns the member bytes read."""
    served = publication.load_index(r2.VERIFIED_PUBLIC_URL)["families"].get(family)
    if served != entry:
        raise PublicationError(f"The public index serves another {family} generation than the one published")
    publication.load_family_root(r2.VERIFIED_PUBLIC_URL, entry)
    return sum(
        publication.verify_member(r2.VERIFIED_PUBLIC_URL, member)
        for member in publication.generation_files(entry).values() if member.sha256 is not None
    )


def _receipt(status: str, root: dict, entry: dict, prior: dict | None, **facts) -> dict:
    prior_tables = (prior or {}).get("tables", {})
    return {
        "status": status,
        "observed_at": datetime.now(UTC).isoformat(),
        "family": root["spec"]["family"],
        **{key: entry.get(key) for key in ("prefix", "artifactDigest", "logicalId", "publishedAt")},
        "prior": prior and {key: prior.get(key) for key in ("prefix", "artifactDigest", "publishedAt")},
        "tables": {
            key: {**{field: table[field] for field in ("rows", "byteSize", "sha256") if field in table},
                  "priorRows": prior_tables.get(key, {}).get("rows")}
            for key, table in sorted(entry["tables"].items())
        },
        "etlReceipts": entry.get("etlReceipts") and {
            field: entry["etlReceipts"][field] for field in ("key", "rows", "byteSize", "sha256")},
        "evidence": [{key: item[key] for key in ("logicalId", "artifactDigest")}
                     for item in root["inputs"] if item["role"] == INPUT_ROLE],
        "verifier_implementation_id": implementation_id(),
        **facts,
    }


def _unrelated_changes(before: dict, after: dict, family: str) -> list[str]:
    names = (before["families"].keys() | after["families"].keys()) - {family}
    return sorted(name for name in names if before["families"].get(name) != after["families"].get(name))


def _existing_ancestor(path: Path) -> Path:
    """Walk up ``path`` to the nearest existing ancestor so ``disk_usage`` probes before creating directories."""
    current = path.resolve(strict=False)
    while not current.exists():
        parent = current.parent
        if parent == current:
            break
        current = parent
    return current


def _file_opener(path: Path):
    return lambda: path.open("rb")


def _member_opener(source, key: str):
    return lambda: source.open(key)


def _manifest_member_sizes(directory: Path, root: Mapping) -> dict[str, int]:
    sizes: dict[str, int] = {}
    for reference in root.get("memberManifests", ()):
        object_key = reference.get("objectKey")
        if not isinstance(object_key, str):
            continue
        manifest_path = directory / object_key
        if not manifest_path.is_file():
            continue
        try:
            manifest = json.loads(manifest_path.read_bytes())
        except (OSError, ValueError):
            continue
        for member in manifest.get("members", ()):
            key, byte_size = member.get("objectKey"), member.get("byteSize")
            if isinstance(key, str) and isinstance(byte_size, int) and byte_size >= 0:
                sizes[key] = byte_size
    return sizes


def _dry_run_local_sources(
    generation: Path,
    family: str,
    root: Mapping,
    live: Mapping | None,
    evidence: tuple[Path, ...],
) -> dict[str, Any]:
    """Map dry-run store object keys to read-only local sources so multi-GB blobs are verified without copying."""
    from rulespec_artifacts import LocalMemberSource

    from spicy_regs.source_evidence import EvidenceMemberSource, SourceEvidenceError
    from spicy_regs.sources.disk_store import LocalObjectSource

    digest = root["artifactDigest"].removeprefix("sha256:")
    prefix = f"generations/{family}/{digest}"
    proof_prefix = f"generation-inputs/{family}/{digest}"
    sources: dict[str, LocalObjectSource] = {}

    for key in LocalMemberSource(generation).keys():
        path = generation / key
        if path.is_file():
            local_source = LocalObjectSource(size=path.stat().st_size, open_stream=_file_opener(path), path=path)
            sources[f"{prefix}/{key}"] = local_source
            if key == "artifact.json" or any(ref.get("objectKey") == key for ref in root.get("memberManifests", ())):
                sources[f"{proof_prefix}/generation/{key}"] = local_source

    for key, member in publication.generation_files(live or {"prefix": "", "tables": {}}).items():
        local = generation / key
        if member.sha256 is not None and local.is_file() and local.stat().st_size == member.byte_size:
            sources[member.path] = LocalObjectSource(size=local.stat().st_size, open_stream=_file_opener(local), path=local)

    for index, ev_dir in enumerate(evidence):
        ev_root_path = ev_dir / "artifact.json"
        if not ev_root_path.is_file():
            continue
        try:
            ev_root = json.loads(ev_root_path.read_bytes())
            ev_source = EvidenceMemberSource(ev_dir)
        except (OSError, ValueError, SourceEvidenceError):
            continue
        ev_digest = str(ev_root.get("artifactDigest", "")).removeprefix("sha256:")
        manifest_sizes = _manifest_member_sizes(ev_dir, ev_root)
        for key in ev_source.keys():
            if publication._BLOB_KEY.fullmatch(key):
                remote_key = f"{publication.EVIDENCE_PREFIX}/{key}"
                blob_hex = key.rsplit("/", 1)[1]
                if key in ev_source.blobs:
                    size = manifest_sizes.get(key)
                    if size is None:
                        recorded = ev_source.blobs[key].get("byte_size")
                        size = int(recorded) if isinstance(recorded, int) and recorded >= 0 else 0
                    raw_path: Path | None = None
                    if ev_source.store is not None:
                        candidate = ev_source.store.root / "objects" / "sha256" / blob_hex[:2] / blob_hex
                        if candidate.is_file():
                            raw_path = candidate
                    blob_source = LocalObjectSource(size=size, open_stream=_member_opener(ev_source, key), path=raw_path)
                else:
                    blob_path = ev_dir / key
                    if not blob_path.is_file():
                        continue
                    blob_source = LocalObjectSource(
                        size=blob_path.stat().st_size, open_stream=_file_opener(blob_path), path=blob_path
                    )
                sources[remote_key] = blob_source
                sources[f"{proof_prefix}/inventories/{blob_hex}.parquet"] = blob_source
            else:
                meta_path = ev_dir / key
                if not meta_path.is_file():
                    continue
                meta_source = LocalObjectSource(
                    size=meta_path.stat().st_size, open_stream=_file_opener(meta_path), path=meta_path
                )
                if ev_digest:
                    sources[f"{publication.EVIDENCE_PREFIX}/{ev_digest}/{key}"] = meta_source
                sources[f"{proof_prefix}/evidence-{index}/{key}"] = meta_source
    return sources


def _projected_dry_run_bytes(
    generation: Path,
    family: str,
    root: Mapping,
    evidence: tuple[Path, ...],
    raw_index: bytes,
    *,
    local_sources: Mapping[str, Any] | None = None,
) -> int:
    """Upper bound on temporary bytes a dry run will write under ``--output``'s volume."""
    from rulespec_artifacts import LocalMemberSource

    from spicy_regs.source_evidence import EvidenceMemberSource, SourceEvidenceError

    digest = root["artifactDigest"].removeprefix("sha256:")
    prefix = f"generations/{family}/{digest}"
    root_size = (generation / "artifact.json").stat().st_size
    index_seed = len(raw_index)
    index_update = min(len(raw_index) + root_size + 4096, publication.INDEX_LIMIT)
    receipt_bytes = max(root_size + 4096, 16 * 1024)

    control_bytes = root_size
    for ref in root.get("memberManifests", ()):
        object_key = ref.get("objectKey")
        if isinstance(object_key, str) and (generation / object_key).is_file():
            control_bytes += (generation / object_key).stat().st_size

    unreferenced: list[int] = []
    for key in LocalMemberSource(generation).keys():
        path = generation / key
        if path.is_file() and (local_sources is None or f"{prefix}/{key}" not in local_sources):
            unreferenced.append(path.stat().st_size)

    for ev_dir in evidence:
        ev_root_path = ev_dir / "artifact.json"
        if not ev_root_path.is_file():
            continue
        control_bytes += ev_root_path.stat().st_size
        try:
            ev_root = json.loads(ev_root_path.read_bytes())
        except (OSError, ValueError):
            continue
        for ref in ev_root.get("memberManifests", ()):
            object_key = ref.get("objectKey")
            if isinstance(object_key, str) and (ev_dir / object_key).is_file():
                control_bytes += (ev_dir / object_key).stat().st_size
        if (ev_dir / JOURNAL).is_file():
            control_bytes += (ev_dir / JOURNAL).stat().st_size
        ev_digest = str(ev_root.get("artifactDigest", "")).removeprefix("sha256:")
        manifest_sizes = _manifest_member_sizes(ev_dir, ev_root)
        try:
            ev_source = EvidenceMemberSource(ev_dir)
        except (OSError, ValueError, SourceEvidenceError):
            continue
        for key in ev_source.keys():
            if publication._BLOB_KEY.fullmatch(key):
                remote_key = f"{publication.EVIDENCE_PREFIX}/{key}"
                if local_sources is None or remote_key not in local_sources:
                    size = manifest_sizes.get(key)
                    if size is None and key in ev_source.blobs:
                        recorded = ev_source.blobs[key].get("byte_size")
                        size = int(recorded) if isinstance(recorded, int) and recorded >= 0 else 0
                    elif size is None and (ev_dir / key).is_file():
                        size = (ev_dir / key).stat().st_size
                    if size:
                        unreferenced.append(size)
            elif ev_digest:
                remote_key = f"{publication.EVIDENCE_PREFIX}/{ev_digest}/{key}"
                if (local_sources is None or remote_key not in local_sources) and (ev_dir / key).is_file():
                    unreferenced.append((ev_dir / key).stat().st_size)

    inputs_json_bound = max(2 * control_bytes, 64 * 1024)
    # Any blob not backed by local_sources may traverse multipart scratch + reassembly (~2x peak of largest).
    unreferenced_peak = sum(unreferenced) + (max(unreferenced) if unreferenced else 0)
    return index_seed + index_update + inputs_json_bound + receipt_bytes + unreferenced_peak


def _require_dry_run_headroom(
    target_dir: Path,
    projected_bytes: int,
    *,
    disk_usage: Callable[[Path], Any] | None = None,
    floor_bytes: int = DRY_RUN_FREE_FLOOR_BYTES,
) -> dict[str, int]:
    """Refuse a dry run before writing when projected temporary bytes would leave less than ``floor_bytes`` free."""
    probe = _existing_ancestor(target_dir)
    usage = (shutil.disk_usage if disk_usage is None else disk_usage)(probe)
    free_bytes = int(usage.free)
    remaining_bytes = free_bytes - projected_bytes
    if remaining_bytes < floor_bytes:
        gib = 1024**3
        raise PublicationError(
            f"Refusing dry run on {probe}: projected {projected_bytes / gib:.3f} GiB of temporary writes "
            f"would leave {remaining_bytes / gib:.2f} GiB free, below the {floor_bytes / gib:.0f} GiB floor "
            f"(currently {free_bytes / gib:.2f} GiB free)"
        )
    return {
        "free_bytes": free_bytes,
        "projected_temp_bytes": projected_bytes,
        "floor_bytes": floor_bytes,
    }


def _seed_unchanged_members(store, live: dict | None, generation: Path) -> None:
    """Give the dry-run store each live member the candidate holds unchanged, from the candidate's own file.

    The publisher copies such a member inside the bucket instead of uploading it, so the copy needs its
    source. The bytes are the live member's by digest; that R2 still holds it is what a dry run cannot see.
    """
    for key, member in publication.generation_files(live or {"prefix": "", "tables": {}}).items():
        local = generation / key
        if (member.sha256 is not None and local.is_file() and local.stat().st_size == member.byte_size
                and publication.file_identity(local)["sha256"] == member.sha256):
            with local.open("rb") as body:
                store.put_object(Bucket=store.bucket, Key=member.path, Body=body)


def dry_run(args, root: dict, *, disk_usage: Callable[[Path], Any] | None = None) -> dict:
    """Every guard of a real publish, against a disk store holding a copy of the public index."""
    from spicy_regs.sources.disk_store import DiskStore

    odcs_parity = _verify_family_schemas(
        args.generation,
        args.family,
        root,
        odcs_contract=getattr(args, "odcs_contract", None),
        odcs_contracts_dir=getattr(args, "odcs_contracts_dir", None),
        require_odcs_contract=bool(getattr(args, "require_odcs_contract", False)),
    )
    if not args.scratch_raw:
        _require_shared_raw(tuple(args.evidence))
    _require_default_thresholds()
    raw = args.live_index.read_bytes() if args.live_index else publication.load_index_bytes(r2.VERIFIED_PUBLIC_URL)
    if raw is None:
        raise PublicationError("The public index is absent; pass --live-index for a rehearsal without one")
    stored = publication.parse_index(raw)
    live = stored["families"].get(args.family)
    _require_prior(live, args.expect_prior, args.family)
    local_sources = _dry_run_local_sources(args.generation, args.family, root, live, tuple(args.evidence))
    projected_bytes = _projected_dry_run_bytes(
        args.generation, args.family, root, tuple(args.evidence), raw, local_sources=local_sources
    )
    _require_dry_run_headroom(args.output.parent, projected_bytes, disk_usage=disk_usage)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=args.output.parent, prefix=".dry-run-store-") as scratch, _no_local_side_effects():
        store = DiskStore(Path(scratch) / "store", bucket="dry-run", local_sources=local_sources)
        store.put_object(Bucket=store.bucket, Key=publication.INDEX_V2_KEY, Body=raw)
        _seed_unchanged_members(store, live, args.generation)
        seeded = len(store.calls)
        after = publication.publish_generation(
            args.generation, client=store, bucket=store.bucket, prior_index=_prior_index(root, stored),
            evidence_directories=tuple(args.evidence), added_tables=frozenset(args.added_table),
        )
        # Bytes a publish would upload: the pointer and every member and evidence blob it does not copy.
        puts = [call for call in store.calls[seeded:] if call["method"] in ("put_object", "upload_part")]
        copies = [call for call in store.calls if call["method"] == "copy_object"]
        disk_bytes_written = store.disk_bytes_written
    return _receipt(
        "dry_run_accepted", root, after["families"][args.family], live,
        added_tables=sorted(args.added_table),
        other_family_changes=_unrelated_changes(stored, after, args.family),
        raw_archive="scratch: not publishable" if args.scratch_raw else "shared",
        store_bytes_written=sum(call["bytes"] for call in puts),
        store_disk_bytes_written=disk_bytes_written,
        store_members_copied=len(copies),
        **({"odcs_schema_parity": odcs_parity} if odcs_parity is not None else {}),
    )


def publish(args, root: dict) -> dict:
    """Publish to the verified target unless the family already is this generation, then read it back publicly."""
    from dotenv import load_dotenv

    odcs_parity = _verify_family_schemas(
        args.generation,
        args.family,
        root,
        odcs_contract=getattr(args, "odcs_contract", None),
        odcs_contracts_dir=getattr(args, "odcs_contracts_dir", None),
        require_odcs_contract=bool(getattr(args, "require_odcs_contract", False)),
    )
    _require_shared_raw(tuple(args.evidence))
    load_dotenv(args.env_file)
    _require_default_thresholds()
    r2.require_credentials("Local generation publication")
    client, bucket = r2.verified_target_client()
    before, _ = publication._stored_index(client, bucket)
    live = before["families"].get(args.family)
    # A rerun after an uncertain result finds its own generation selected: it writes nothing and only reads back.
    write = live is None or live["artifactDigest"] != root["artifactDigest"]
    if write:
        if live is not None and args.expect_prior is None:
            raise PublicationError(
                f"{args.family} is live: pass --expect-prior with the digest the dry run printed, so a generation "
                "published since then is not replaced unseen")
        _require_prior(live, args.expect_prior, args.family)
        publication.publish_generation(
            args.generation, client=client, bucket=bucket, prior_index=_prior_index(root, before),
            evidence_directories=tuple(args.evidence), added_tables=frozenset(args.added_table),
        )
    after, _ = publication._stored_index(client, bucket)
    entry = after["families"].get(args.family)
    if entry is None or entry["artifactDigest"] != root["artifactDigest"]:
        raise PublicationError(f"Stored {args.family} is not this generation after publication")
    return _receipt(
        "published_public_readback" if write else "already_published_public_readback", root, entry,
        live if write else None,
        added_tables=sorted(args.added_table),
        other_family_changes=_unrelated_changes(before, after, args.family),
        public_index_url=f"{r2.VERIFIED_PUBLIC_URL}/{publication.INDEX_V2_KEY}",
        public_bytes_read=_public_readback(args.family, entry),
        **({"odcs_schema_parity": odcs_parity} if odcs_parity is not None else {}),
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generation", type=Path, required=True, help="The built generation directory")
    parser.add_argument("--family", required=True, choices=sorted(LOCAL_FAMILIES), help="The family it must be")
    parser.add_argument("--evidence", type=Path, action="append", default=[],
                        help="A source-evidence artifact directory the generation pins, repeatable")
    parser.add_argument("--added-table", action="append", default=[],
                        help="A table key (NAME.parquet) this generation adds to the family, repeatable")
    parser.add_argument("--expect-prior", help="Digest (or its first 8+ hex digits) of the live generation the "
                                               "build was compared against")
    parser.add_argument("--output", type=Path, required=True, help="A fresh directory for the receipt")
    parser.add_argument("--dry-run", action="store_true", help="Run every guard on a private disk store; no credentials")
    parser.add_argument("--live-index", type=Path, help="Dry run only: a saved publication.v2.json instead of the "
                                                        "public one")
    parser.add_argument("--env-file", type=Path, default=Path(".env"), help="Publish only: where R2_* values load from")
    parser.add_argument("--scratch-raw", action="store_true",
                        help="Dry run only: the build is a rehearsal on a scratch raw archive, which a publish refuses")
    parser.add_argument("--odcs-contract", type=Path, default=None,
                        help="Explicit ODCS v3.2.0 DataContract JSON file to enforce Arrow schema parity against")
    parser.add_argument("--odcs-contracts-dir", type=Path, default=None,
                        help="Directory holding ODCS manifest.json and families/*.odcs.json for enrolled families")
    parser.add_argument("--require-odcs-contract", action="store_true",
                        help="Fail closed unless this family has an explicit or manifest-enrolled ODCS v3.2.0 contract")
    args = parser.parse_args(argv)
    if (args.live_index or args.scratch_raw) and not args.dry_run:
        parser.error("--live-index and --scratch-raw are dry-run inputs; a publish uses the stored index "
                     "and the shared raw archive")
    if args.output.exists():
        parser.error(f"--output {args.output} exists; a receipt directory is written once")
    root = _candidate(
        args.generation,
        args.family,
        tuple(args.evidence),
        odcs_contract=args.odcs_contract,
        odcs_contracts_dir=args.odcs_contracts_dir,
        require_odcs_contract=args.require_odcs_contract,
    )
    receipt = dry_run(args, root) if args.dry_run else publish(args, root)
    # Created only now, so a refused run leaves nothing behind and the same command can be run again.
    args.output.mkdir(parents=True)
    name = "dry-run.json" if args.dry_run else "receipt.json"
    (args.output / name).write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(json.dumps(receipt, indent=2, sort_keys=True))


def _current_roots(client, bucket: str, index: dict) -> dict[str, dict]:
    """Every family's current root, read with credentials and held to its index entry."""
    roots = {}
    for family, entry in sorted(index["families"].items()):
        stored = publication._get_bounded(client, bucket, f"{entry['prefix']}/artifact.json")
        if stored is None:
            raise PublicationError(f"{family}'s current root is not stored")
        roots[family] = publication.family_root(stored[0], entry)
    return roots


def _held_elsewhere(client, bucket: str, index: dict) -> dict[str, str]:
    """What pins each family name or bare generation digest outside the index, by the readers retention uses."""
    from spicy_regs import generation_retention as retention

    held = {}
    for dataset, pin in retention.receipt_archive_pins().items():
        reason = f"{retention.RECEIPT_ARCHIVE.name} ({dataset})"
        held[pin["family"]] = held[pin["artifactDigest"].removeprefix("sha256:")] = reason
    held |= retention.docspec_pins(retention._read_pin_source(retention.DOCSPEC_PINS, client=client))
    held |= retention.archive_pins(retention._read_pin_source(retention.ARCHIVE_PINS, client=client))
    snapshot_id, inputs, native = retention._snapshot_inputs(client, bucket)
    for name, digest in native.items():
        held[digest] = f"rulemaking snapshot {snapshot_id} (native input {name})"
    for key in inputs:
        if (owner := publication.table_owner(index, key)) is not None:
            held[owner[0]] = f"rulemaking snapshot {snapshot_id} (input {key})"
    return held


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _removal_summary(removal: publication.FamilyRemoval) -> dict:
    before = publication.parse_index(removal.before)["families"]
    return {
        "index_etag": removal.etag,
        "index_sha256": _sha256(removal.before),
        "families_before": len(before),
        "families_after": len(before) - len(removal.removed),
        "removed": {name: {
            **{key: entry.get(key) for key in ("artifactDigest", "prefix", "publishedAt")},
            "tables": {key: table["rows"] for key, table in sorted(entry["tables"].items())},
            "rows": sum(table["rows"] for table in entry["tables"].values()),
        } for name, entry in removal.removed.items()},
        "write_sha256": _sha256(removal.after),
        "write_bytes": len(removal.after),
    }


def _write_summary(output: Path, summary: dict) -> None:
    (output / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")


def _execute_removal(client, bucket: str, removal: publication.FamilyRemoval, summary: dict, output: Path) -> None:
    """Write, settle and read back the removal; the record files are written before any refusal is raised."""
    from spicy_regs.sources.cloudflare import purge_urls

    try:
        outcome = publication.write_family_removal(client, bucket, removal)
    except Exception as failure:
        summary |= {"status": "unknown", "error": f"{type(failure).__name__}: {failure}"}
        _write_summary(output, summary)
        raise
    (output / "stored-after.json").write_bytes(outcome.stored)
    summary |= {"status": outcome.status, "read_back": {
        "stored_sha256": _sha256(outcome.stored), "stored_equals_planned": outcome.stored == removal.after,
        **({"note": outcome.note} if outcome.note else {}),
    }}
    if outcome.status != "removed":
        _write_summary(output, summary)
        raise PublicationError(outcome.note)
    summary["read_back"]["stored"] = publication.check_family_removal(removal.before, outcome.stored, removal.removed)
    purge_urls([f"{r2.VERIFIED_PUBLIC_URL}/{publication.INDEX_V2_KEY}"])
    served = publication.load_index_bytes(r2.VERIFIED_PUBLIC_URL) or b""
    summary["read_back"] |= {"public_sha256": _sha256(served), "public_equals_stored": served == outcome.stored}
    if served != outcome.stored:
        _write_summary(output, summary)
        raise PublicationError(f"The stored index has the removal, but {r2.VERIFIED_PUBLIC_URL} serves other "
                               "bytes; fetch it again before anything else")


def unpublish_main(argv=None) -> int:
    """Take whole families out of the publication index; a dry run unless --execute and --expect-etag are given.

    The dry run reads the stored index, every current root and the pin documents, and prints the index's ETag and
    SHA-256, each named family's entry and the SHA-256 of the exact bytes a run would store. ``--execute`` writes
    those bytes only if the index still has ``--expect-etag``, reads the stored and the public index back, and proves
    the families are gone and every other entry is byte-identical. Generations stay stored, so a family is put back
    with ``publication.restore_family`` from the entry saved in ``before.json``.
    """
    parser = argparse.ArgumentParser(description=(unpublish_main.__doc__ or "").split("\n\n")[0])
    parser.add_argument("family", nargs="+", help="A family to take out of the index")
    parser.add_argument("--output", type=Path, required=True,
                        help="A fresh directory for the index read (before.json), the planned bytes and the summary")
    parser.add_argument("--execute", action="store_true", help="Write the index; without it nothing is written")
    parser.add_argument("--expect-etag", help="With --execute: the index_etag the dry run printed")
    args = parser.parse_args(argv)
    if args.execute != (args.expect_etag is not None):
        parser.error("--execute and --expect-etag go together: pass the index_etag the dry run printed")
    if args.output.exists():
        parser.error(f"--output {args.output} exists; a removal record is written once")
    r2.require_credentials("Reading the stored publication index")
    client, bucket = r2.verified_target_client()
    stored = publication._get_bounded(client, bucket, publication.INDEX_V2_KEY)
    if stored is None:
        raise PublicationError("The publication index is absent")
    raw, etag = stored
    if args.execute and etag.strip('"') != args.expect_etag.strip('"'):
        raise PublicationError(f"The index ETag is {etag}, not {args.expect_etag}; nothing was written. Dry-run again")
    index = publication.parse_index(raw)
    removal = publication.plan_family_removal(
        raw, etag, args.family, roots=_current_roots(client, bucket, index), held=_held_elsewhere(client, bucket, index))
    summary = {"status": "dry_run", **_removal_summary(removal)}
    args.output.mkdir(parents=True)
    (args.output / "before.json").write_bytes(removal.before)
    (args.output / "planned.json").write_bytes(removal.after)
    if args.execute:
        _execute_removal(client, bucket, removal, summary, args.output)
    _write_summary(args.output, summary)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    main()
