"""Exact per-view FEC release checks using the existing publication reader.

Receipts contain identities, never executable SQL. Deployment selects receipt
bytes after the image is built. Registered application SQL and locally measured
interpretation files supply the running side of every comparison. This reader
does not publish data, select historical parents, or import source processors.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
from importlib import metadata
import json
from pathlib import Path
import re
from typing import Any, Mapping

FORMAT = "spicy-regs-fec-release"
VERSION = 1
LIMIT = 1024 * 1024
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_FAMILY = re.compile(r"[a-z][a-z0-9_-]*\Z")
_IDENTITY_GROUPS = ("policies", "dictionaries", "definitions")


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


def parse_receipt(raw: bytes, expected_digest: str) -> dict:
    """Verify the deployment pin before parsing a bounded, non-executable receipt."""
    if not _DIGEST.fullmatch(expected_digest) or len(raw) > LIMIT:
        raise ValueError("Invalid release digest or receipt byte limit")
    if sha256(raw) != expected_digest:
        raise ValueError("Release receipt bytes differ from deployment digest")
    receipt = json.loads(raw, object_pairs_hook=_pairs, parse_constant=_constant)
    fields = {"format", "version", "output_membership", "views", "consumer", "recovery"}
    if (not isinstance(receipt, dict) or set(receipt) != fields
            or receipt["format"] != FORMAT or type(receipt["version"]) is not int or receipt["version"] != VERSION):
        raise ValueError("Invalid release receipt format or fields")
    if any(not isinstance(receipt[key], dict) for key in fields - {"format", "version"}):
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
    return receipt


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
    consumer_error = None
    try:
        running = consumer if consumer is not None else runtime_consumer(image_digest)
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
        fields = {"dependencies", "sql_sha256", "interpretation", "population", "as_of", "acceptance_receipts", "evidence_generations"}
        if not isinstance(expected, dict) or set(expected) != fields:
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
            if not isinstance(acceptance, list) or not acceptance or any(not isinstance(pin, str) or not _DIGEST.fullmatch(pin) for pin in acceptance):
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


def install_views(connection, specs, configuration, index, available_tables, publication, *, read_tables, prepare=None):
    """Check first, then reuse the relationship-view owner for compatible trusted SQL."""
    from .relationship_views.sql_views import install_sql_views

    result = {}
    for spec in specs:
        release = check_view(spec, configuration, index, available_tables)
        if release["status"] == "compatible":
            sql = configuration["views"][spec.view.name]["sql"]
            try:
                statements = connection.extract_statements(sql)
                if len(statements) != 1 or statements[0].type.name != "SELECT":
                    release["reasons"].append(dict(path="sql", reason="registered_sql_must_be_one_select", expected=None, actual=None))
                elif read_tables(connection, sql) != set(spec.view.required):
                    release["reasons"].append(dict(path="dependencies", reason="registered_sql_dependency_set_differs", expected=None, actual=None))
            except Exception as exc:
                release["reasons"].append(dict(path="sql", reason=f"registered_sql_invalid: {exc}", expected=None, actual=None))
            if release["reasons"]:
                release["status"] = "disabled"
        if release["status"] == "compatible":
            sql = configuration["views"][spec.view.name]["sql"]
            prepared = replace(spec.view, query=lambda _pins, sql=sql: sql)
            if prepare is not None:
                prepared = prepare(prepared, sql)
            entry = install_sql_views(connection, set(available_tables) | set(prepared.required), [prepared], publication)[spec.view.name]
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
