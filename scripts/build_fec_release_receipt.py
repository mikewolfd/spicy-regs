#!/usr/bin/env python3
"""Build, compact, and end-to-end verify a VERSION = 2 FEC release receipt.

Assembles exact table-membership pins, installed interpretation file digests,
registered SQL SHA-256 digests, and recovery/rollback pins from either local
sealed generation directories or a publication.v2.json index, compacts them
with :func:`spicy_regs.fec_release.compact_receipt`, enforces the byte budget,
roundtrip-verifies with :func:`spicy_regs.fec_release.parse_receipt`, runs
:func:`spicy_regs.fec_release.check_view` on every view, and optionally binds
every view in DuckDB via :func:`spicy_regs.fec_release.install_views`.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
import json
from pathlib import Path
import re
import sys
from tempfile import TemporaryDirectory
from typing import Any

from spicy_regs import data_dictionary as dd
from spicy_regs import fec_release, mcp_server
from spicy_regs.fec_receipt_adapter import (
    ReceiptAdapter,
    is_v2_fec_query_family,
    qualified_views,
    selected_qualified_views,
)
from spicy_regs.relationship_views.core import quoted
from spicy_regs.sources import publication

DEFAULT_MAX_BYTES = 250_000
DEFAULT_PRIOR_QUERY_GENERATION = "sha256:784b23da0ea9ee7807e09715d38508cd8e0cd654af1047ae6a5b1fc63efb7300"
DEFAULT_PRIOR_OBSERVATIONS_GENERATIONS: tuple[str, ...] = (
    "sha256:4cb0dffa91c278eb4e7a6048e266a5e4389748666428a82bc58c35ee3fe77f2f",
    "sha256:9c289dfec822ff7e54f9d5719276579452a7b35cad573e701a51e0a15c2a06c0",
)
DEFAULT_ROLLBACK_RECEIPTS: tuple[str, ...] = (
    "sha256:a11c7f9228d7b7eb216417d218721e470ded57d0c5dad51179a4c4c36049b275",
    "sha256:5744e2c09a02b8a2d6889abda6ef60ad7f066b5f7629530d0f332f92f837c11f",
)
_DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}\Z")
_MANIFEST_DIGEST_RE = re.compile(r"manifest (sha256:[0-9a-f]{64})")
_FAMILY_RE = re.compile(r"[a-z][a-z0-9_-]*\Z")


def _require_digest(value: str, label: str) -> str:
    if not isinstance(value, str) or not _DIGEST_RE.fullmatch(value):
        raise ValueError(f"Invalid SHA-256 digest for {label}: {value!r}")
    return value


def _dedupe(items: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(items))


def _default_source_archive_sha256(scope: Mapping[str, Any]) -> str | None:
    population = scope.get("population")
    if isinstance(population, str):
        match = _MANIFEST_DIGEST_RE.search(population)
        if match is not None:
            return match.group(1)
    return None


def load_generation_entry(
    generation_dir: Path,
    *,
    expected_family: str | None = None,
    verify_member_bytes: bool = False,
) -> dict[str, Any]:
    """Load and verify a local sealed generation directory into publication.v2.json family shape."""
    from rulespec_artifacts import LocalMemberSource, MemberDescriptor, admit_artifact, stamp_root

    directory = Path(generation_dir).resolve()
    artifact_path = directory / "artifact.json"
    members_path = directory / "members.json"
    if not artifact_path.is_file():
        raise ValueError(f"Missing generation artifact.json: {artifact_path}")
    if not members_path.is_file():
        raise ValueError(f"Missing generation members.json: {members_path}")

    root = json.loads(artifact_path.read_text(encoding="utf-8"))
    if stamp_root(root) != root:
        raise ValueError(f"Generation artifactDigest does not match artifact.json contents: {artifact_path}")
    if root.get("kind") != "spicy-regs-rollup-generation":
        raise ValueError(f"Unexpected generation artifact kind: {root.get('kind')!r}")
    spec = root.get("spec")
    if not isinstance(spec, Mapping) or spec.get("publicationStatus") != "complete-family":
        raise ValueError(f"Generation is not a complete-family publication artifact: {artifact_path}")
    family = spec.get("family")
    if not isinstance(family, str) or not _FAMILY_RE.fullmatch(family):
        raise ValueError(f"Invalid generation family: {family!r}")
    if expected_family is not None and family != expected_family:
        raise ValueError(f"Expected generation family {expected_family!r}, got {family!r}")

    refs = root.get("memberManifests")
    if not isinstance(refs, list) or len(refs) != 1 or refs[0].get("objectKey") != "members.json":
        raise ValueError(f"Generation requires a single members.json manifest: {artifact_path}")
    manifest_bytes = members_path.read_bytes()
    manifest_ref = refs[0]
    if (
        len(manifest_bytes) != manifest_ref.get("byteSize")
        or fec_release.sha256(manifest_bytes) != manifest_ref.get("sha256")
    ):
        raise ValueError(f"Generation members.json differs from artifact.json manifest pin: {members_path}")

    if verify_member_bytes:
        admit_artifact(LocalMemberSource(directory))

    manifest = json.loads(manifest_bytes.decode("utf-8"))
    members = [MemberDescriptor.from_dict(item, path="member") for item in manifest.get("members", ())]
    if (
        len(members) != manifest_ref.get("memberCount")
        or len({m.object_key for m in members}) != len(members)
        or sum(m.byte_size for m in members) != manifest_ref.get("totalMemberByteSize")
        or sum(m.record_count or 0 for m in members) != manifest_ref.get("totalRecordCount")
    ):
        raise ValueError(f"Generation members.json counts disagree with artifact.json: {members_path}")

    receipt_members = [m for m in members if m.object_key == "etl_receipts.parquet"]
    if len(receipt_members) > 1 or bool(receipt_members) != ("etlReceipts" in spec):
        raise ValueError(
            f"Generation etlReceipts declaration disagrees with etl_receipts.parquet membership: {artifact_path}"
        )
    table_members = [m for m in members if m.object_key != "etl_receipts.parquet"]
    tables = publication.table_entries(spec["tables"], table_members) if spec.get("tables") else {}
    digest = root["artifactDigest"]
    entry: dict[str, Any] = {
        "prefix": f"generations/{family}/{digest.removeprefix('sha256:')}",
        "logicalId": root["logicalId"],
        "artifactDigest": digest,
        "tables": tables,
    }
    if "etlReceipts" in spec:
        from spicy_regs.subject_catalog import shared_receipt_logs

        declared = spec["etlReceipts"]
        receipt = receipt_members[0]
        entry["etlReceipts"] = {
            "key": receipt.object_key,
            "sha256": receipt.sha256,
            "byteSize": receipt.byte_size,
            "rows": receipt.record_count,
            "columns": declared["columns"],
            "generationId": declared["generationId"],
            "datasets": [p["dataset"] for p in declared["policies"] if p["dataset"] not in shared_receipt_logs()],
        }
    return entry


def load_candidate_index(
    *,
    index_path: Path | None = None,
    observations_dir: Path | None = None,
    query_dir: Path | None = None,
    verify_member_bytes: bool = False,
) -> dict[str, Any]:
    """Assemble and validate a publication.v2.json index from an index file and/or local generation dirs."""
    if index_path is None and (observations_dir is None or query_dir is None):
        raise ValueError(
            "Provide --index and/or both --observations-generation and --query-generation"
        )
    if index_path is not None:
        index = publication.parse_index(Path(index_path).read_bytes())
        index = {"format": "spicy-regs-publication", "version": 2, "families": dict(index["families"])}
    else:
        index = {"format": "spicy-regs-publication", "version": 2, "families": {}}

    if observations_dir is not None:
        index["families"]["fec-observations"] = load_generation_entry(
            observations_dir,
            expected_family="fec-observations",
            verify_member_bytes=verify_member_bytes,
        )
    if query_dir is not None:
        index["families"]["fec-query"] = load_generation_entry(
            query_dir,
            expected_family="fec-query",
            verify_member_bytes=verify_member_bytes,
        )

    validated = publication.parse_index(json.dumps(index, sort_keys=True).encode("utf-8"))
    for required_family in ("fec-observations", "fec-query"):
        if required_family not in validated["families"]:
            raise ValueError(f"Candidate publication index is missing required family: {required_family}")
    return validated


def _validate_consumer(consumer: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(consumer, Mapping) or set(consumer) != {"image_digest", "code_sha256", "package_versions"}:
        raise ValueError("Consumer identity must declare exact keys: image_digest, code_sha256, package_versions")
    image_digest = _require_digest(str(consumer["image_digest"]), "consumer.image_digest")
    code_sha256 = _require_digest(str(consumer["code_sha256"]), "consumer.code_sha256")
    package_versions = consumer["package_versions"]
    if (
        not isinstance(package_versions, Mapping)
        or not package_versions
        or any(not isinstance(k, str) or not k or not isinstance(v, str) or not v for k, v in package_versions.items())
    ):
        raise ValueError("Consumer package_versions must be a nonempty mapping of package names to version strings")
    return {
        "image_digest": image_digest,
        "code_sha256": code_sha256,
        "package_versions": dict(package_versions),
    }


def _check_query_table_dictionary_parity(
    query_entry: Mapping[str, Any],
    *,
    table_metadata_path: Path,
    allow_legacy: bool,
) -> None:
    metadata = json.loads(table_metadata_path.read_text(encoding="utf-8"))
    tables = query_entry["tables"]
    published_names = {key.removesuffix(".parquet") for key in tables}
    if not allow_legacy:
        expected_names = set(dd.FEC_TYPED_TABLES)
        missing_tables = sorted(expected_names - published_names)
        unexpected_tables = sorted(published_names - expected_names)
        if missing_tables or unexpected_tables:
            parts = []
            if missing_tables:
                parts.append(f"missing {len(missing_tables)} table(s): {', '.join(missing_tables[:5])}")
            if unexpected_tables:
                parts.append(f"unexpected {len(unexpected_tables)} table(s): {', '.join(unexpected_tables[:5])}")
            raise ValueError("fec-query table set disagrees with canonical 50-table FEC_TYPED_TABLES (" + "; ".join(parts) + ")")

    for key, descriptor in sorted(tables.items()):
        table = key.removesuffix(".parquet")
        if table not in metadata:
            raise ValueError(f"Published fec-query table is missing from installed table_metadata.json: {table}")
        expected_columns = [[c["column_name"], c["column_type"]] for c in metadata[table]["columns"]]
        actual_columns = descriptor["columns"]
        if actual_columns != expected_columns:
            raise ValueError(
                f"Published fec-query table {table} schema disagrees with installed table_metadata.json "
                f"(expected {len(expected_columns)} columns, got {len(actual_columns)})"
            )


def _verify_duckdb_view_binding(
    specs: Sequence[fec_release.QualifiedView],
    verified_config: dict[str, Any],
    index: Mapping[str, Any],
    available_tables: Sequence[str],
    *,
    require_receipts_free: bool,
) -> None:
    import duckdb

    with duckdb.connect(config={"threads": 1, "memory_limit": "256MB", "max_temp_directory_size": "0B"}) as con:
        for entry in index["families"].values():
            for key, descriptor in sorted(entry["tables"].items()):
                table = key.removesuffix(".parquet")
                col_ddl = ", ".join(f"{quoted(col_name)} {col_type}" for col_name, col_type in descriptor["columns"])
                con.execute(f"CREATE TABLE {quoted(table)} ({col_ddl})")
        adapter = ReceiptAdapter(con, index, "unused")
        installed = fec_release.install_views(
            con,
            specs,
            verified_config,
            index,
            available_tables,
            {},
            read_tables=mcp_server._tables_named,
            prepare=adapter.prepare_qualified,
        )
        failures = [
            f"{name}: status={entry['status']} release={entry['release_compatibility']['status']} reason={entry.get('reason')}"
            for name, entry in sorted(installed.items())
            if entry["status"] != "available" or entry["release_compatibility"]["status"] != "compatible"
        ]
        if failures:
            raise ValueError("DuckDB view binding failed for qualified FEC views:\n" + "\n".join(failures))
        if require_receipts_free:
            restored = [
                name
                for (name,) in con.execute("SHOW TABLES").fetchall()
                if name.startswith(("_spicy_fec_processing_", "_spicy_fec_current_"))
            ]
            if restored:
                raise ValueError(f"Receipts-free qualification unexpectedly created restored tables: {restored}")


def build_and_verify_fec_release_receipt(
    *,
    index: Mapping[str, Any],
    consumer: Mapping[str, Any],
    acceptance_receipts: Sequence[str],
    source_archive_sha256: str | None = None,
    scope: Mapping[str, Any] | None = None,
    retained_generations: Mapping[str, Sequence[str]] | None = None,
    rollback_receipts: Sequence[str] = DEFAULT_ROLLBACK_RECEIPTS,
    include_default_prior_generations: bool = True,
    max_bytes: int = DEFAULT_MAX_BYTES,
    allow_legacy: bool = False,
    require_receipts_free: bool = True,
    verify_duckdb_binding: bool = True,
    package_root: Path | None = None,
) -> dict[str, Any]:
    """Build, compact, roundtrip-parse, and verify every qualified FEC view for a VERSION = 2 receipt."""
    if type(max_bytes) is not int or max_bytes <= 0 or max_bytes > fec_release.LIMIT:
        raise ValueError(f"max_bytes must be between 1 and {fec_release.LIMIT}, got {max_bytes!r}")

    validated_index = publication.parse_index(json.dumps(dict(index), sort_keys=True).encode("utf-8"))
    for required_family in ("fec-observations", "fec-query"):
        if required_family not in validated_index["families"]:
            raise ValueError(f"Publication index is missing required family: {required_family}")

    query_entry = validated_index["families"]["fec-query"]
    obs_entry = validated_index["families"]["fec-observations"]
    if require_receipts_free:
        for family_name, entry in (("fec-observations", obs_entry), ("fec-query", query_entry)):
            if "etlReceipts" in entry:
                raise ValueError(f"Family {family_name} declares etlReceipts, violating receipts-free INVARIANT 1")

    validated_consumer = _validate_consumer(consumer)
    if not acceptance_receipts:
        raise ValueError("At least one acceptance receipt SHA-256 is required")
    checked_acceptance = _dedupe([_require_digest(pin, "acceptance_receipt") for pin in acceptance_receipts])
    checked_rollbacks = _dedupe([_require_digest(pin, "rollback_receipt") for pin in rollback_receipts])

    pkg_root = Path(package_root) if package_root is not None else Path(fec_release.__file__).resolve().parent
    resolved_scope = (
        dict(scope)
        if scope is not None
        else json.loads((pkg_root / "fec_query_scope.json").read_text(encoding="utf-8"))
    )
    resolved_archive = source_archive_sha256 or _default_source_archive_sha256(resolved_scope)
    if resolved_archive is None:
        raise ValueError("source_archive_sha256 is required (and could not be inferred from scope.population)")
    resolved_archive = _require_digest(resolved_archive, "source_archive_sha256")

    expected_obs_pin = resolved_scope.get("selected_observations_pin") or resolved_scope.get("source_generation_pin")
    if obs_entry["artifactDigest"] != expected_obs_pin:
        raise ValueError(
            f"fec-observations artifactDigest {obs_entry['artifactDigest']} disagrees with "
            f"fec_query_scope selected_observations_pin {expected_obs_pin}"
        )

    is_v2_layout = is_v2_fec_query_family(query_entry)
    if not allow_legacy and not is_v2_layout:
        raise ValueError(
            "fec-query family does not match receipts-free 50-table /2 layout "
            "(pass --allow-legacy only when intentionally building a legacy /1 receipt)"
        )

    _check_query_table_dictionary_parity(
        query_entry,
        table_metadata_path=pkg_root / "table_metadata.json",
        allow_legacy=allow_legacy,
    )

    native_specs = qualified_views(resolved_scope, legacy_associations=False, package_root=pkg_root)
    legacy_specs = qualified_views(resolved_scope, legacy_associations=True, package_root=pkg_root)
    selected_specs = selected_qualified_views(native_specs, legacy_specs, validated_index)
    if not allow_legacy:
        legacy_fallbacks = [
            spec.view.name
            for spec, native in zip(selected_specs, native_specs, strict=True)
            if spec != native
        ]
        if legacy_fallbacks:
            raise ValueError(
                "One or more qualified FEC views fell back to legacy /1 SQL: " + ", ".join(legacy_fallbacks)
            )

    capture = fec_release.capture_configuration(
        selected_specs,
        receipt_digest=None,
        image_digest=validated_consumer["image_digest"],
        base_url="unused",
        publication={},
        consumer=validated_consumer,
    )
    if capture["consumer_error"]:
        raise ValueError(f"Consumer measurement failed: {capture['consumer_error']}")
    view_errors = {name: state["error"] for name, state in capture["views"].items() if state["error"]}
    if view_errors:
        raise ValueError(f"Application view identity capture failed: {view_errors}")

    output_membership: dict[str, Any] = {}
    for key in sorted(query_entry["tables"]):
        table = key.removesuffix(".parquet")
        pin = fec_release.captured_table(validated_index, table)
        if pin is None:
            raise ValueError(f"Failed to capture fec-query output table descriptor: {table}")
        output_membership[table] = pin

    retained_map: dict[str, list[str]] = {}

    def _retain(family: str, digest: str) -> None:
        if not _FAMILY_RE.fullmatch(family):
            raise ValueError(f"Invalid retained generation family: {family!r}")
        _require_digest(digest, f"retained_generations.{family}")
        pins = retained_map.setdefault(family, [])
        if digest not in pins:
            pins.append(digest)

    _retain("fec-query", query_entry["artifactDigest"])
    _retain("fec-observations", obs_entry["artifactDigest"])
    if include_default_prior_generations:
        _retain("fec-query", DEFAULT_PRIOR_QUERY_GENERATION)
        for prior_obs in DEFAULT_PRIOR_OBSERVATIONS_GENERATIONS:
            _retain("fec-observations", prior_obs)
    for family, pins in (retained_generations or {}).items():
        for pin in pins:
            _retain(family, pin)

    views_payload: dict[str, Any] = {}
    for spec in selected_specs:
        view_name = spec.view.name
        state = capture["views"][view_name]
        deps: dict[str, Any] = {}
        for dep_table in sorted(spec.view.required):
            dep_pin = fec_release.captured_table(validated_index, dep_table)
            if dep_pin is None:
                raise ValueError(f"View {view_name} requires missing table in publication index: {dep_table}")
            deps[dep_table] = dep_pin
            _retain(dep_pin["family"], dep_pin["generation"])
        for ev_family, ev_pin in state["evidence_generations"].items():
            _retain(ev_family, ev_pin)
        views_payload[view_name] = {
            "dependencies": deps,
            "sql_sha256": state["sql_sha256"],
            "interpretation": state["interpretation"],
            "population": state["population"],
            "as_of": state["as_of"],
            "evidence_generations": state["evidence_generations"],
            "acceptance_receipts": list(checked_acceptance),
        }

    uncompacted = {
        "format": fec_release.FORMAT,
        "version": 1,
        "consumer": validated_consumer,
        "output_membership": output_membership,
        "recovery": {
            "source_archive_sha256": resolved_archive,
            "retained_generations": retained_map,
            "rollback_receipts": checked_rollbacks,
        },
        "views": views_payload,
    }

    compacted = fec_release.compact_receipt(uncompacted)
    raw_bytes = fec_release.serialize_receipt(compacted)
    receipt_bytes_len = len(raw_bytes)
    if receipt_bytes_len >= fec_release.LIMIT:
        raise ValueError(
            f"Compacted release receipt is {receipt_bytes_len:,} bytes (must be < {fec_release.LIMIT:,} LIMIT)"
        )
    if receipt_bytes_len >= max_bytes:
        raise ValueError(
            f"Compacted release receipt is {receipt_bytes_len:,} bytes (exceeds configured --max-bytes {max_bytes:,})"
        )

    receipt_sha256 = fec_release.sha256(raw_bytes)
    parsed = fec_release.parse_receipt(raw_bytes, receipt_sha256)
    if parsed["version"] != 2:
        raise ValueError(f"Expected compacted VERSION = 2 receipt, parsed version {parsed['version']!r}")

    available_tables = sorted(
        key.removesuffix(".parquet")
        for entry in validated_index["families"].values()
        for key in entry["tables"]
    )
    with TemporaryDirectory(prefix="verify-fec-release-receipt-") as tmp_dir:
        tmp_receipt = Path(tmp_dir) / f"{receipt_sha256.removeprefix('sha256:')}.json"
        tmp_receipt.write_bytes(raw_bytes)
        verified_config = fec_release.capture_configuration(
            selected_specs,
            receipt_digest=receipt_sha256,
            image_digest=validated_consumer["image_digest"],
            base_url="unused",
            local_path=tmp_receipt,
            local_mode=True,
            publication={},
            consumer=validated_consumer,
        )
        if verified_config["receipt_error"]:
            raise ValueError(f"Receipt verification capture failed: {verified_config['receipt_error']}")
        check_results = {
            spec.view.name: fec_release.check_view(spec, verified_config, validated_index, available_tables)
            for spec in selected_specs
        }
        disabled = {name: res["reasons"] for name, res in check_results.items() if res["status"] != "compatible"}
        if disabled:
            raise ValueError(
                f"Receipt check_view reported {len(disabled)} disabled view(s): "
                + json.dumps(disabled, sort_keys=True)
            )
        if verify_duckdb_binding:
            _verify_duckdb_view_binding(
                selected_specs,
                verified_config,
                validated_index,
                available_tables,
                require_receipts_free=require_receipts_free,
            )

    views_sql_sha256 = {spec.view.name: capture["views"][spec.view.name]["sql_sha256"] for spec in selected_specs}
    summary = {
        "format": fec_release.FORMAT,
        "version": compacted["version"],
        "receipt_sha256": receipt_sha256,
        "bytes": receipt_bytes_len,
        "limit_bytes": fec_release.LIMIT,
        "headroom_bytes": fec_release.LIMIT - receipt_bytes_len,
        "selected_view_mode": "v2" if is_v2_layout else "legacy",
        "query_generation": query_entry["artifactDigest"],
        "observations_generation": obs_entry["artifactDigest"],
        "output_tables_count": len(output_membership),
        "view_count": len(selected_specs),
        "compatible_views": len(check_results),
        "disabled_views": 0,
        "consumer": validated_consumer,
        "recovery": compacted["recovery"],
        "fec_filing_reference_resolution_sql_sha256": views_sql_sha256.get("fec_filing_reference_resolution"),
        "views_sql_sha256": views_sql_sha256,
    }
    return {
        "receipt_bytes": raw_bytes,
        "receipt": compacted,
        "summary": summary,
    }


def _parse_retained_arg(value: str) -> tuple[str, str]:
    family, sep, pin = value.partition("=")
    if not sep or not family or not pin:
        raise ValueError(f"--retained-generation must use family=sha256:... format, got {value!r}")
    return family, _require_digest(pin, f"retained_generation({family})")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index", type=Path, default=None, help="Path to a publication.v2.json index file.")
    parser.add_argument(
        "--observations-generation",
        type=Path,
        default=None,
        help="Path to a local sealed fec-observations generation directory (artifact.json + members.json).",
    )
    parser.add_argument(
        "--query-generation",
        type=Path,
        default=None,
        help="Path to a local sealed fec-query generation directory (artifact.json + members.json).",
    )
    parser.add_argument(
        "--verify-member-bytes",
        action="store_true",
        help="Rehash every Parquet member file on disk when loading local generation directories.",
    )
    consumer_group = parser.add_mutually_exclusive_group(required=True)
    consumer_group.add_argument(
        "--image-digest",
        default=None,
        help="Container image sha256:... digest; measures runtime_consumer(image_digest) from this checkout.",
    )
    consumer_group.add_argument(
        "--consumer-json",
        type=Path,
        default=None,
        help="Path to a JSON file containing pre-measured runtime_consumer(image_digest) output.",
    )
    parser.add_argument(
        "--acceptance-receipt",
        action="append",
        default=[],
        help="Acceptance evidence sha256:... digest (repeatable).",
    )
    parser.add_argument(
        "--acceptance-receipt-file",
        action="append",
        type=Path,
        default=[],
        help="Path to a local acceptance receipt file whose bytes are SHA-256 hashed (repeatable).",
    )
    parser.add_argument(
        "--source-archive-sha256",
        default=None,
        help="Recovery source_archive_sha256 pin (defaults to manifest sha256:... in fec_query_scope.json).",
    )
    parser.add_argument(
        "--scope",
        type=Path,
        default=None,
        help="Path to fec_query_scope.json (defaults to installed src/spicy_regs/fec_query_scope.json).",
    )
    parser.add_argument(
        "--retained-generation",
        action="append",
        default=[],
        help="Additional recovery.retained_generations entry as family=sha256:... (repeatable).",
    )
    parser.add_argument(
        "--no-default-prior-generations",
        action="store_true",
        help="Omit default historical prior fec-query / fec-observations generations from recovery.retained_generations.",
    )
    parser.add_argument(
        "--rollback-receipt",
        action="append",
        default=None,
        help="Override recovery.rollback_receipts with explicit sha256:... digest(s) (repeatable).",
    )
    parser.add_argument(
        "--no-default-rollback-receipts",
        action="store_true",
        help="Set recovery.rollback_receipts to [] unless --rollback-receipt is provided.",
    )
    parser.add_argument(
        "--max-bytes",
        type=int,
        default=DEFAULT_MAX_BYTES,
        help=f"Maximum permitted compact receipt byte size (default: {DEFAULT_MAX_BYTES}, hard cap < {fec_release.LIMIT}).",
    )
    parser.add_argument(
        "--allow-legacy",
        action="store_true",
        help="Allow legacy Oct-3 /1 fec-query layouts instead of enforcing canonical 50-table /2.",
    )
    parser.add_argument(
        "--allow-receipts",
        action="store_true",
        help="Allow etlReceipts on fec-query / fec-observations (disabled by default per INVARIANT 1).",
    )
    parser.add_argument(
        "--skip-duckdb-binding",
        action="store_true",
        help="Skip DuckDB empty-schema install_views binding check (check_view and parse_receipt still run).",
    )
    parser.add_argument("--output", type=Path, required=True, help="Output path for the exact compact VERSION = 2 receipt bytes.")
    parser.add_argument("--summary", type=Path, default=None, help="Optional path for the JSON build/verification summary.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    index = load_candidate_index(
        index_path=args.index,
        observations_dir=args.observations_generation,
        query_dir=args.query_generation,
        verify_member_bytes=args.verify_member_bytes,
    )
    if args.consumer_json is not None:
        consumer = json.loads(args.consumer_json.read_text(encoding="utf-8"))
    else:
        consumer = fec_release.runtime_consumer(_require_digest(args.image_digest, "image_digest"))

    acceptance = list(args.acceptance_receipt)
    for receipt_file in args.acceptance_receipt_file:
        acceptance.append(fec_release.sha256(receipt_file.read_bytes()))

    retained: dict[str, list[str]] = {}
    for item in args.retained_generation:
        family, pin = _parse_retained_arg(item)
        retained.setdefault(family, []).append(pin)

    if args.rollback_receipt is not None:
        rollback_receipts: Sequence[str] = args.rollback_receipt
    elif args.no_default_rollback_receipts:
        rollback_receipts = ()
    else:
        rollback_receipts = DEFAULT_ROLLBACK_RECEIPTS

    scope = json.loads(args.scope.read_text(encoding="utf-8")) if args.scope is not None else None

    result = build_and_verify_fec_release_receipt(
        index=index,
        consumer=consumer,
        acceptance_receipts=acceptance,
        source_archive_sha256=args.source_archive_sha256,
        scope=scope,
        retained_generations=retained,
        rollback_receipts=rollback_receipts,
        include_default_prior_generations=not args.no_default_prior_generations,
        max_bytes=args.max_bytes,
        allow_legacy=args.allow_legacy,
        require_receipts_free=not args.allow_receipts,
        verify_duckdb_binding=not args.skip_duckdb_binding,
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(result["receipt_bytes"])
    summary_text = json.dumps(result["summary"], indent=2, sort_keys=True) + "\n"
    if args.summary is not None:
        args.summary.parent.mkdir(parents=True, exist_ok=True)
        args.summary.write_text(summary_text, encoding="utf-8")
    sys.stdout.write(summary_text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
