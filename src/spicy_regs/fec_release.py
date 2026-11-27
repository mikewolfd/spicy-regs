"""Exact per-view FEC release checks using the existing publication reader.

Receipts contain identities, never executable SQL. Deployment selects receipt
bytes after the image is built. Registered application SQL and locally measured
interpretation files supply the running side of every comparison. This reader
does not publish data, select historical parents, or import source processors.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, replace
import hashlib
from importlib import metadata
import json
from pathlib import Path
import re
from typing import Any, Mapping, Sequence

from .runtime_bounds import checkpoint

FORMAT = "spicy-regs-fec-release"
VERSION = 2
SUPPORTED_VERSIONS = (1, 2)
LIMIT = 1024 * 1024
_MAX_EXPANDED_BYTES = 16 * LIMIT
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_FAMILY = re.compile(r"[a-z][a-z0-9_-]*\Z")
_IDENTITY_GROUPS = ("policies", "dictionaries", "definitions")
_V1_FIELDS = frozenset({"format", "version", "output_membership", "views", "consumer", "recovery"})
_V2_FIELDS = _V1_FIELDS | frozenset({"shared"})
_VIEW_FIELDS = frozenset({
    "dependencies",
    "sql_sha256",
    "interpretation",
    "population",
    "as_of",
    "acceptance_receipts",
    "evidence_generations",
})
_SHARED_REF_FIELDS = (
    "acceptance_receipts",
    "interpretation",
    "population",
    "as_of",
    "evidence_generations",
)
_SHARED_SECTIONS = frozenset((*_SHARED_REF_FIELDS, "dependencies"))
_V2_REF_ONLY_FIELDS = frozenset(f"{field}_ref" for field in _SHARED_REF_FIELDS) | frozenset(
    {"output_dependencies", "dependency_refs"}
)


def sha256(raw: bytes) -> str:
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _pairs(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"Receipt repeats key: {key}")
        value[key] = item
    return value


def _constant(value):
    raise ValueError(f"Non-finite JSON constant: {value}")


def _file_digest(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return "sha256:" + digest.hexdigest()


def _valid_acceptance_receipts(value: Any) -> bool:
    return (
        isinstance(value, list)
        and bool(value)
        and all(isinstance(pin, str) and bool(_DIGEST.fullmatch(pin)) for pin in value)
    )


def _valid_dependency_pin(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == {"family", "generation", "descriptor"}
        and isinstance(value["family"], str)
        and bool(_FAMILY.fullmatch(value["family"]))
        and isinstance(value["generation"], str)
        and bool(_DIGEST.fullmatch(value["generation"]))
        and isinstance(value["descriptor"], dict)
    )


def _valid_shared_value(section: str, value: Any) -> bool:
    if section == "acceptance_receipts":
        return _valid_acceptance_receipts(value)
    if section == "dependencies":
        return _valid_dependency_pin(value)
    if section == "interpretation":
        return isinstance(value, dict) and bool(value)
    if section in ("population", "as_of"):
        return isinstance(value, str) and bool(value)
    if section == "evidence_generations":
        try:
            _evidence_generations(value)
        except ValueError:
            return False
        return True
    return False


def _normalize_v2_receipt(receipt: dict[str, Any]) -> dict[str, Any]:
    shared: dict[str, dict[str, Any]] = {}
    shared_sizes: dict[str, dict[str, int]] = {}
    if "shared" in receipt:
        raw_shared = receipt["shared"]
        if not isinstance(raw_shared, dict) or not raw_shared or not (set(raw_shared) <= _SHARED_SECTIONS):
            raise ValueError("Invalid release receipt shared section")
        for section, entries in raw_shared.items():
            if not isinstance(entries, dict) or not entries:
                raise ValueError(f"Invalid shared {section} inventory")
            section_sizes: dict[str, int] = {}
            for ref_id, value in entries.items():
                if not isinstance(ref_id, str) or not ref_id or not _valid_shared_value(section, value):
                    raise ValueError(f"Invalid shared {section} entry: {ref_id!r}")
                section_sizes[ref_id] = len(_json(value).encode())
            shared[section] = entries
            shared_sizes[section] = section_sizes

    output_membership = receipt["output_membership"]
    output_sizes = {name: len(_json(pin).encode()) for name, pin in output_membership.items()}
    used_refs: dict[str, set[str]] = {section: set() for section in shared}
    expanded_bytes = 0
    normalized_views: dict[str, Any] = {}

    for view_name, raw_view in receipt["views"].items():
        if not isinstance(raw_view, dict):
            normalized_views[view_name] = raw_view
            continue
        view: dict[str, Any] = {
            key: value
            for key, value in raw_view.items()
            if key not in _V2_REF_ONLY_FIELDS and key not in _SHARED_REF_FIELDS and key != "dependencies"
        }
        for field in _SHARED_REF_FIELDS:
            ref_key = f"{field}_ref"
            if ref_key in raw_view:
                if field in raw_view:
                    raise ValueError(f"View repeats inline and shared {field}: {view_name}")
                ref_id = raw_view[ref_key]
                if not isinstance(ref_id, str) or not ref_id:
                    raise ValueError(f"Invalid shared {field} reference in view: {view_name}")
                pool = shared.get(field)
                if pool is None or ref_id not in pool:
                    raise ValueError(f"Unknown shared {field} reference in view {view_name}: {ref_id}")
                expanded_bytes += shared_sizes[field][ref_id]
                if expanded_bytes > _MAX_EXPANDED_BYTES:
                    raise ValueError("Expanded release receipt exceeds byte limit")
                used_refs[field].add(ref_id)
                view[field] = deepcopy(pool[ref_id])
            elif field in raw_view:
                view[field] = raw_view[field]

        has_inline = "dependencies" in raw_view
        has_output = "output_dependencies" in raw_view
        has_refs = "dependency_refs" in raw_view
        if has_output or has_refs:
            deps: dict[str, Any] = {}
            if has_inline:
                if not isinstance(raw_view["dependencies"], dict):
                    raise ValueError(f"Invalid view dependencies: {view_name}")
                deps.update(raw_view["dependencies"])
            if has_output:
                out_deps = raw_view["output_dependencies"]
                if not isinstance(out_deps, list) or not out_deps:
                    raise ValueError(f"Invalid output_dependencies in view: {view_name}")
                for table in out_deps:
                    if not isinstance(table, str) or not table or table not in output_membership:
                        raise ValueError(f"Unknown output_membership dependency in view {view_name}: {table!r}")
                    if table in deps:
                        raise ValueError(f"Duplicate dependency table in view {view_name}: {table}")
                    expanded_bytes += output_sizes[table]
                    if expanded_bytes > _MAX_EXPANDED_BYTES:
                        raise ValueError("Expanded release receipt exceeds byte limit")
                    deps[table] = deepcopy(output_membership[table])
            if has_refs:
                dep_refs = raw_view["dependency_refs"]
                if not isinstance(dep_refs, dict) or not dep_refs:
                    raise ValueError(f"Invalid dependency_refs in view: {view_name}")
                dep_pool = shared.get("dependencies")
                for table, ref_id in dep_refs.items():
                    if not isinstance(table, str) or not table or not isinstance(ref_id, str) or not ref_id:
                        raise ValueError(f"Invalid dependency_refs entry in view: {view_name}")
                    if dep_pool is None or ref_id not in dep_pool:
                        raise ValueError(f"Unknown shared dependencies reference in view {view_name}: {ref_id}")
                    if table in deps:
                        raise ValueError(f"Duplicate dependency table in view {view_name}: {table}")
                    expanded_bytes += shared_sizes["dependencies"][ref_id]
                    if expanded_bytes > _MAX_EXPANDED_BYTES:
                        raise ValueError("Expanded release receipt exceeds byte limit")
                    used_refs["dependencies"].add(ref_id)
                    deps[table] = deepcopy(dep_pool[ref_id])
            view["dependencies"] = deps
        elif has_inline:
            view["dependencies"] = raw_view["dependencies"]
        normalized_views[view_name] = view

    for section, entries in shared.items():
        unused = sorted(set(entries) - used_refs[section])
        if unused:
            raise ValueError(f"Unreferenced shared {section} entries: {', '.join(unused)}")

    normalized = {key: receipt[key] for key in _V1_FIELDS if key != "views"}
    normalized["views"] = normalized_views
    return normalized


def _validate_and_normalize_receipt(receipt: Any) -> dict:
    if not isinstance(receipt, dict):
        raise ValueError("Invalid release receipt format or fields")
    version = receipt.get("version")
    if receipt.get("format") != FORMAT or type(version) is not int or version not in SUPPORTED_VERSIONS:
        raise ValueError("Invalid release receipt format or fields")
    if version == 1:
        if set(receipt) != _V1_FIELDS:
            raise ValueError("Invalid release receipt format or fields")
    elif not (_V1_FIELDS <= set(receipt) <= _V2_FIELDS):
        raise ValueError("Invalid release receipt format or fields")
    if any(not isinstance(receipt[key], dict) for key in _V1_FIELDS - {"format", "version"}):
        raise ValueError("Release receipt sections must be objects")
    if not receipt["output_membership"] or not receipt["views"]:
        raise ValueError("Release receipt requires output membership and views")
    # Delegate descriptor/schema validation to the existing publisher. The
    # temporary index is only a shape check, never a published generation claim.
    from .sources.publication import parse_index

    tables, generations = {}, set()
    for name, pin in receipt["output_membership"].items():
        if not isinstance(pin, dict) or set(pin) != {"family", "generation", "descriptor"} or pin["family"] != "fec-query":
            raise ValueError("Output membership must name the fec-query family")
        generations.add(pin["generation"])
        tables[name + ".parquet"] = pin["descriptor"]
    if len(generations) != 1:
        raise ValueError("Typed output membership names several generations")
    generation = next(iter(generations))
    if not isinstance(generation, str) or not _DIGEST.fullmatch(generation):
        raise ValueError("Invalid typed output generation")
    parse_index(_json({"format": "spicy-regs-publication", "version": 2, "families": {"fec-query": {
        "prefix": "generations/fec-query/" + generation[7:], "logicalId": "urn:fec-release:shape-check",
        "artifactDigest": generation, "tables": tables,
    }}}).encode())
    consumer = receipt["consumer"]
    if (set(consumer) != {"image_digest", "code_sha256", "package_versions"}
            or not all(isinstance(consumer.get(k), str) and _DIGEST.fullmatch(consumer[k]) for k in ("image_digest", "code_sha256"))
            or not isinstance(consumer["package_versions"], dict) or not consumer["package_versions"]):
        raise ValueError("Invalid release consumer identities")
    recovery = receipt["recovery"]
    if (set(recovery) != {"source_archive_sha256", "retained_generations", "rollback_receipts"}
            or not isinstance(recovery.get("source_archive_sha256"), str)
            or not _DIGEST.fullmatch(recovery["source_archive_sha256"])
            or not isinstance(recovery["retained_generations"], dict)
            or not isinstance(recovery["rollback_receipts"], list)):
        raise ValueError("Invalid release recovery identities")
    if any(not isinstance(pins, list) or not pins for pins in recovery["retained_generations"].values()):
        raise ValueError("Retained generations must be nonempty digest lists")
    pins = [pin for pins in recovery["retained_generations"].values() for pin in pins] + recovery["rollback_receipts"]
    if any(not isinstance(pin, str) or not _DIGEST.fullmatch(pin) for pin in pins):
        raise ValueError("Invalid retained generation or rollback digest")
    if version == 2:
        return _normalize_v2_receipt(receipt)
    return receipt


def parse_receipt(raw: bytes, expected_digest: str) -> dict:
    """Verify the deployment pin before parsing a bounded, non-executable receipt."""
    if not _DIGEST.fullmatch(expected_digest) or len(raw) > LIMIT:
        raise ValueError("Invalid release digest or receipt byte limit")
    if sha256(raw) != expected_digest:
        raise ValueError("Release receipt bytes differ from deployment digest")
    receipt = json.loads(raw, object_pairs_hook=_pairs, parse_constant=_constant)
    return _validate_and_normalize_receipt(receipt)


def serialize_receipt(receipt: Mapping[str, Any]) -> bytes:
    """Encode a receipt in canonical compact sorted JSON with trailing newline."""
    return (_json(dict(receipt)) + "\n").encode()


def compact_receipt(receipt: Mapping[str, Any]) -> dict[str, Any]:
    """Factor repeated view metadata into a canonical VERSION = 2 receipt.

    Deduplicates repeated acceptance inventories, interpretation/scope blocks,
    and table dependencies (reusing ``output_membership`` directly and sharing
    repeated non-output dependencies) while preserving exact normalized values,
    digest order, and every validation and rollback invariant.
    """
    normalized = _validate_and_normalize_receipt(json.loads(_json(dict(receipt))))
    output_membership = normalized["output_membership"]
    views = normalized["views"]
    sorted_view_names = sorted(views)

    counts: dict[str, dict[str, int]] = {section: {} for section in _SHARED_SECTIONS}
    order: dict[str, list[tuple[str, Any]]] = {section: [] for section in _SHARED_SECTIONS}
    for view_name in sorted_view_names:
        view = views[view_name]
        if not isinstance(view, dict):
            continue
        if not (set(view) <= _VIEW_FIELDS):
            raise ValueError(f"Invalid release receipt view fields: {view_name}")
        for field in _SHARED_REF_FIELDS:
            if field in view and _valid_shared_value(field, view[field]):
                canon = _json(view[field])
                if canon not in counts[field]:
                    counts[field][canon] = 0
                    order[field].append((canon, view[field]))
                counts[field][canon] += 1
        raw_deps = view.get("dependencies")
        if isinstance(raw_deps, dict):
            for table, pin in sorted(raw_deps.items()):
                if table in output_membership and pin == output_membership[table]:
                    continue
                if _valid_dependency_pin(pin):
                    canon = _json(pin)
                    if canon not in counts["dependencies"]:
                        counts["dependencies"][canon] = 0
                        order["dependencies"].append((canon, pin))
                    counts["dependencies"][canon] += 1

    shared: dict[str, dict[str, Any]] = {}
    ref_ids: dict[str, dict[str, str]] = {section: {} for section in _SHARED_SECTIONS}
    for section in (*_SHARED_REF_FIELDS, "dependencies"):
        section_entries: dict[str, Any] = {}
        ref_extra = 2 if section == "dependencies" else 6
        next_index = 1
        for canon, value in order[section]:
            count = counts[section][canon]
            if count < 2:
                continue
            ref_id = str(next_index)
            val_bytes = len(canon.encode())
            if (count - 1) * val_bytes <= count * (ref_extra + len(ref_id)) + len(ref_id) + 3:
                continue
            section_entries[ref_id] = deepcopy(value)
            ref_ids[section][canon] = ref_id
            next_index += 1
        if section_entries:
            shared[section] = section_entries

    compacted_views: dict[str, Any] = {}
    for view_name in sorted_view_names:
        raw_view = views[view_name]
        if not isinstance(raw_view, dict):
            compacted_views[view_name] = raw_view
            continue
        compact_view: dict[str, Any] = {}
        if "sql_sha256" in raw_view:
            compact_view["sql_sha256"] = raw_view["sql_sha256"]
        for field in _SHARED_REF_FIELDS:
            if field not in raw_view:
                continue
            value = raw_view[field]
            canon = _json(value) if _valid_shared_value(field, value) else None
            if canon is not None and canon in ref_ids[field]:
                compact_view[f"{field}_ref"] = ref_ids[field][canon]
            else:
                compact_view[field] = deepcopy(value)
        if "dependencies" in raw_view:
            raw_deps = raw_view["dependencies"]
            if not isinstance(raw_deps, dict):
                compact_view["dependencies"] = deepcopy(raw_deps)
            elif not raw_deps:
                compact_view["dependencies"] = {}
            else:
                out_deps: list[str] = []
                dep_refs: dict[str, str] = {}
                inline_deps: dict[str, Any] = {}
                for table, pin in sorted(raw_deps.items()):
                    if not isinstance(table, str):
                        raise ValueError(f"Invalid dependency table name in view: {view_name}")
                    if table in output_membership and pin == output_membership[table]:
                        out_deps.append(table)
                        continue
                    canon = _json(pin) if _valid_dependency_pin(pin) else None
                    if canon is not None and canon in ref_ids["dependencies"]:
                        dep_refs[table] = ref_ids["dependencies"][canon]
                    else:
                        inline_deps[table] = deepcopy(pin)
                if out_deps:
                    compact_view["output_dependencies"] = out_deps
                if dep_refs:
                    compact_view["dependency_refs"] = dep_refs
                if inline_deps:
                    compact_view["dependencies"] = inline_deps
        compacted_views[view_name] = compact_view

    compacted: dict[str, Any] = {
        "format": FORMAT,
        "version": VERSION,
        "consumer": deepcopy(normalized["consumer"]),
        "output_membership": deepcopy(output_membership),
        "recovery": deepcopy(normalized["recovery"]),
        "views": compacted_views,
    }
    if shared:
        compacted["shared"] = shared
    return compacted


def _evidence_generations(value):
    """Application and receipt declarations use explicit family/digest pairs."""
    if not isinstance(value, Mapping) or any(
        not isinstance(family, str) or not _FAMILY.fullmatch(family)
        or not isinstance(pin, str) or not _DIGEST.fullmatch(pin)
        for family, pin in value.items()
    ):
        raise ValueError("Invalid evidence generation declarations")
    return dict(value)


def load_receipt(expected_digest: str, base_url: str, *, local_path: Path | None = None) -> dict:
    """Read a selected immutable evidence blob through the publication owner's bounded I/O."""
    if not _DIGEST.fullmatch(expected_digest):
        raise ValueError("Invalid deployment receipt digest")
    if local_path is not None:
        with local_path.open("rb") as stream:
            raw = stream.read(LIMIT + 1)
    else:
        from .sources.publication import EVIDENCE_PREFIX, _bounded_get

        key = f"{EVIDENCE_PREFIX}/blobs/sha256/{expected_digest.removeprefix('sha256:')}"
        raw = _bounded_get(f"{base_url.rstrip('/')}/{key}", allow_missing=False, limit=LIMIT)
        if raw is None:
            raise ValueError("Release receipt is unavailable")
    return parse_receipt(raw, expected_digest)


def runtime_consumer(image_digest: str | None, *, package_root: Path | None = None,
                     packages=("spicy-regs", "duckdb", "mcp")) -> dict:
    """Measure installed code/data definitions and package versions, not expected receipt values.

    The image value is explicitly a deployment assertion. FR13 must independently
    verify the running image; this process cannot measure its container digest.
    """
    root = package_root or Path(__file__).parent
    members = []
    for path in sorted(root.rglob("*")):
        if path.is_file() and path.suffix in {".py", ".json", ".sql"}:
            members.append((path.relative_to(root).as_posix(), _file_digest(path)))
    if not members:
        raise ValueError("Running package code files are unavailable")
    versions = {}
    for package in packages:
        try:
            versions[package] = metadata.version(package)
        except metadata.PackageNotFoundError:
            versions[package] = None
    return {"image_digest": image_digest, "code_sha256": sha256(_json(members).encode()), "package_versions": versions}


@dataclass(frozen=True)
class QualifiedView:
    """One trusted SQLView and the actual files its interpretation requires.

    Identity group maps use stable application names and installed file paths.
    Missing files/groups fail this spec closed. The receipt cannot add SQL or
    choose files to read. SQLView.required is the application-owned dependency
    declaration; qualified SQL must not read undeclared tables.
    """

    view: Any  # relationship_views.sql_views.SQLView, without importing its registry
    mapping_version: str
    identity_version: str
    identities: Mapping[str, Mapping[str, Path]]
    population: str
    as_of: str
    # These generations provide evidence for interpretation without being SQL
    # inputs. Keep this declaration separate from SQLView.required.
    evidence_generations: Mapping[str, str]


def capture_configuration(specs, *, receipt_digest, image_digest, base_url, local_path=None,
                          local_mode=False, publication=None, consumer=None) -> dict:
    """Capture inputs once for a new connection; all returned content is JSON data."""
    checkpoint()
    consumer_error = None
    try:
        running = consumer if consumer is not None else runtime_consumer(image_digest)
    except TimeoutError:
        raise
    except Exception as exc:
        running = {}
        consumer_error = f"consumer_measurement_unavailable: {type(exc).__name__}: {exc}"
    config = {"receipt_sha256": receipt_digest, "receipt": None, "receipt_error": None,
              "consumer": running, "consumer_error": consumer_error, "views": {}}
    if not receipt_digest:
        config["receipt_error"] = "deployment_receipt_digest_missing"
    else:
        try:
            if local_mode and local_path is None:
                raise ValueError("Local serving requires a local receipt path")
            config["receipt"] = load_receipt(receipt_digest, base_url, local_path=local_path)
        except TimeoutError:
            raise
        except Exception as exc:
            config["receipt_error"] = f"receipt_unavailable_or_invalid: {type(exc).__name__}: {exc}"
    # Repeated views share installed interpretation files. Reuse each digest
    # only within this capture; the next refresh measures every path again.
    file_digests: dict[Path, str] = {}

    def identity_digest(path):
        path = Path(path).resolve()
        if path not in file_digests:
            file_digests[path] = _file_digest(path)
        return file_digests[path]

    for spec in specs:
        checkpoint()
        name = spec.view.name
        if name in config["views"]:
            raise ValueError(f"Repeated qualified application view: {name}")
        state = {"error": None}
        try:
            if (set(spec.identities) != set(_IDENTITY_GROUPS) or not spec.mapping_version
                    or not spec.identity_version or not spec.population or not spec.as_of):
                raise ValueError("Application interpretation identities are incomplete")
            evidence_generations = _evidence_generations(spec.evidence_generations)
            files = {}
            for group, named in spec.identities.items():
                if not named or any(not isinstance(name, str) or not name for name in named):
                    raise ValueError(f"Application {group} identities are missing")
                files[group] = {name: identity_digest(path) for name, path in named.items()}
            sql = spec.view.query(publication or {})
            state.update(sql=sql, sql_sha256=sha256(sql.encode()), interpretation={
                "mapping_version": spec.mapping_version, "identity_version": spec.identity_version, **files,
            }, population=spec.population, as_of=spec.as_of, evidence_generations=evidence_generations)
        except TimeoutError:
            raise
        except Exception as exc:
            state["error"] = f"application_identity_unavailable: {type(exc).__name__}: {exc}"
        config["views"][name] = state
    # Break caller references; later mutation cannot alter a captured connection.
    return json.loads(_json(config))


def captured_table(index: Mapping, name: str) -> dict | None:
    """Use the publication owner's exact descriptor, including every split member."""
    from .sources.publication import table_descriptor, table_owner

    owner = table_owner(index, name + ".parquet")
    if owner is None:
        from .fec_receipt_adapter import receipt_owner
        owner = receipt_owner(index, name)
        if owner is None:
            return None
        return json.loads(_json({"family": owner[0], "generation": owner[1]["artifactDigest"],
                                 "descriptor": owner[1]["etlReceipts"]}))
    return json.loads(_json({"family": owner[0], "generation": owner[1]["artifactDigest"],
                             "descriptor": table_descriptor(index, name + ".parquet")}))


def check_view(spec: QualifiedView, configuration: dict, index: Mapping, available_tables) -> dict:
    """Compare each actual dependency and interpretation; do not accept equal schemas as equal pins."""
    name = spec.view.name
    actual = configuration["views"][name]
    receipt = configuration["receipt"]
    expected = receipt["views"].get(name) if receipt else None
    tables = {table: captured_table(index, table) for table in spec.view.required}
    required_evidence = actual.get("evidence_generations", {})
    evidence = {}
    issues = []

    def issue(path, reason, expected=None, actual=None):
        issues.append(dict(path=path, reason=reason, expected=expected, actual=actual))

    # The captured publication index is immutable for this connection. A new
    # connection/refresh checks its new index, including non-SQL evidence parents.
    for family, required_pin in required_evidence.items():
        entry = index.get("families", {}).get(family)
        pin = entry.get("artifactDigest") if isinstance(entry, Mapping) else None
        valid = (isinstance(pin, str) and _DIGEST.fullmatch(pin)
                 and entry.get("prefix") == f"generations/{family}/{pin[7:]}")
        evidence[family] = pin if valid else None
        if not valid:
            issue("evidence_generations." + family, "captured_evidence_generation_unavailable_or_invalid", required_pin, pin)
        elif pin != required_pin:
            issue("evidence_generations." + family, "exact_evidence_generation_mismatch", required_pin, pin)

    if configuration["receipt_error"]:
        issue("receipt", configuration["receipt_error"])
    if actual["error"]:
        issue("application", actual["error"])
    if configuration["consumer_error"]:
        issue("consumer", configuration["consumer_error"])
    if receipt is not None:
        for field in ("image_digest", "code_sha256", "package_versions"):
            required, running = receipt["consumer"][field], configuration["consumer"].get(field)
            if required != running or (field == "package_versions" and any(v is None for v in running.values())):
                issue("consumer." + field, "consumer_identity_mismatch", required, running)
        if not isinstance(expected, dict) or set(expected) != _VIEW_FIELDS:
            issue("views." + name, "view_receipt_missing_or_invalid")
        else:
            try:
                declared_evidence = _evidence_generations(expected["evidence_generations"])
            except ValueError:
                issue("evidence_generations", "view_evidence_declaration_invalid")
            else:
                if declared_evidence != required_evidence:
                    issue("evidence_generations", "application_and_receipt_evidence_generations_differ",
                          required_evidence, declared_evidence)
            for family, pin in required_evidence.items():
                if pin not in receipt["recovery"]["retained_generations"].get(family, []):
                    issue("recovery." + family, "required_evidence_generation_not_retained", pin)
            dependencies = expected["dependencies"]
            if not isinstance(dependencies, dict) or set(dependencies) != set(tables):
                issue("dependencies", "application_and_receipt_dependency_sets_differ", list(tables), dependencies)
            else:
                for table, pin in tables.items():
                    if table not in available_tables or pin is None:
                        issue("dependencies." + table, "captured_managed_table_unavailable", dependencies[table], pin)
                    elif pin != dependencies[table]:
                        issue("dependencies." + table, "exact_table_pin_mismatch", dependencies[table], pin)
                    if pin is not None and pin["family"] == "fec-query" and "datasets" not in pin["descriptor"] and receipt["output_membership"].get(table) != dependencies[table]:
                        issue("output_membership." + table, "typed_output_membership_mismatch")
                    if (pin is not None and (not isinstance(dependencies[table], dict)
                            or dependencies[table].get("generation") not in receipt["recovery"]["retained_generations"].get(pin["family"], []))):
                        issue("recovery." + table, "required_generation_not_retained")
            for field in ("sql_sha256", "interpretation", "population", "as_of"):
                if expected[field] != actual.get(field):
                    issue(field, "interpretation_identity_mismatch", expected[field], actual.get(field))
            acceptance = expected["acceptance_receipts"]
            if not _valid_acceptance_receipts(acceptance):
                issue("acceptance_receipts", "acceptance_evidence_pin_missing_or_invalid")
    return dict(
        status="compatible" if not issues else "disabled", reasons=issues,
        receipt_sha256=configuration["receipt_sha256"], dependencies=tables,
        evidence_generations=evidence, required_evidence_generations=required_evidence,
        consumer=configuration["consumer"], sql_sha256=actual.get("sql_sha256"),
        interpretation=actual.get("interpretation"), population=actual.get("population"), as_of=actual.get("as_of"),
        image_identity_basis="deployment_assertion_requires_FR13_external_running_image_verification",
        acceptance_receipts=expected.get("acceptance_receipts") if isinstance(expected, dict) else None,
    )


#: What a query reply states of each qualified view it read: the pins to compare after a refresh.
QUERY_RELEASE_FIELDS = ("status", "receipt_sha256", "sql_sha256", "evidence_generations", "population", "as_of")
#: What a release summary leaves out of a ``check_view`` record; ``describe_table(detail=true)`` returns them.
RELEASE_INVENTORIES = ("dependencies[].descriptor", "acceptance_receipts")


def release_summary(record: Mapping, fields: Sequence[str] | None = None) -> dict:
    """A ``check_view`` record without its inventories, as a new dict; the record itself is not changed.

    Each dependency keeps its family and generation but not its storage
    descriptor (every column and member file), and the acceptance receipts
    become their count. Every other field stays whole, failure reasons
    included. ``fields`` narrows the summary to those keys.
    """
    summary = {key: value for key, value in record.items() if key not in ("dependencies", "acceptance_receipts")}
    if "dependencies" in record:
        summary["dependencies"] = {
            table: None if pin is None else {"family": pin["family"], "generation": pin["generation"]}
            for table, pin in (record["dependencies"] or {}).items()
        }
    if "acceptance_receipts" in record:
        receipts = record["acceptance_receipts"]
        summary["acceptance_receipt_count"] = None if receipts is None else len(receipts)
    if fields is not None:
        summary = {key: summary[key] for key in fields}
    return deepcopy(summary)


def install_views(connection, specs, configuration, index, available_tables, publication, *, read_tables, prepare=None, defer_admission=False, admit_names=None):
    """Check first, then reuse the relationship-view owner for compatible trusted SQL."""
    from .relationship_views.sql_views import install_sql_views

    result = {}
    schemas = {}
    # Only this exact adapter's unchanged legacy branch promises no input writes.
    # Native preparation and arbitrary callbacks keep fresh physical checks.
    from .fec_receipt_adapter import ReceiptAdapter
    owner = getattr(prepare, "__self__", None)
    unchanged_adapter = (type(owner) is ReceiptAdapter and owner.connection is connection
                         and getattr(prepare, "__func__", None) is ReceiptAdapter.prepare_qualified)
    for spec in specs:
        checkpoint()
        release = check_view(spec, configuration, index, available_tables)
        if release["status"] == "compatible":
            sql = configuration["views"][spec.view.name]["sql"]
            try:
                statements = connection.extract_statements(sql)
                if len(statements) != 1 or statements[0].type.name != "SELECT":
                    release["reasons"].append(dict(path="sql", reason="registered_sql_must_be_one_select", expected=None, actual=None))
                elif read_tables(connection, sql) != set(spec.view.required):
                    release["reasons"].append(dict(path="dependencies", reason="registered_sql_dependency_set_differs", expected=None, actual=None))
            except TimeoutError:
                raise
            except Exception as exc:
                release["reasons"].append(dict(path="sql", reason=f"registered_sql_invalid: {exc}", expected=None, actual=None))
            if release["reasons"]:
                release["status"] = "disabled"
        if release["status"] == "compatible" and (defer_admission or admit_names is not None and spec.view.name not in admit_names):
            entry = dict(status="deferred", reason="Release pins match; exact receipt row and output schema admission remains pending.",
                         dependencies=list(spec.view.required), metadata=dict(
                             label=spec.view.name.replace("_", " "), summary=spec.view.meaning, kind="derived",
                             rule_version=spec.view.rule_version, identity_columns=list(spec.view.identity_columns),
                             input_publications={table: publication.get(table) for table in spec.view.required},
                             query_admission="pending_receipt_rows"))
        elif release["status"] == "compatible":
            sql = configuration["views"][spec.view.name]["sql"]
            prepared = replace(spec.view, query=lambda _pins, sql=sql: sql)
            if prepare is not None:
                if not unchanged_adapter:
                    schemas.clear()
                original = prepared
                try:
                    prepared = prepare(original, sql)
                except BaseException:
                    schemas.clear()
                    raise
                if prepared is not original:
                    schemas.clear()
            entry = install_sql_views(connection, set(available_tables) | set(prepared.required), [prepared], publication,
                                      _schemas=schemas)[spec.view.name]
            entry["dependencies"] = list(spec.view.required)
            entry["metadata"]["input_publications"] = {table: publication.get(table) for table in spec.view.required}
            if entry["status"] != "available":
                release["status"] = "disabled"
                release["reasons"].append(dict(path="schema", reason=entry["reason"], expected=None, actual=None))
        else:
            entry = dict(status="unavailable", reason="; ".join(i["path"] + ": " + i["reason"] for i in release["reasons"]),
                         dependencies=list(spec.view.required), metadata=dict(
                             label=spec.view.name.replace("_", " "), summary=spec.view.meaning, kind="derived",
                             rule_version=spec.view.rule_version, identity_columns=list(spec.view.identity_columns),
                             input_publications={table: publication.get(table) for table in spec.view.required}))
        entry["release_compatibility"] = release
        result[spec.view.name] = entry
    return result
