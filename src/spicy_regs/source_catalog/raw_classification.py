"""Bounded content recognition and exact-byte reviews of retained objects.

The calling importer establishes producer provenance and verifies original bytes.
Schema matching identifies content kind; it does not admit a release or assert
source completeness. Receipt aliases alone never establish generated content.
"""

from __future__ import annotations

from functools import lru_cache
import json
from pathlib import Path
import re

from .raw_content import _json

RETAINED_REVIEWS = Path(__file__).with_name("config") / "retained-content-reviews.v1.json"


def retained_content_reviews() -> dict:
    """Exact-byte local findings; never generalize a filename or response shape."""
    document = json.loads(RETAINED_REVIEWS.read_text())
    if (set(document) != {"version", "description", "objects"} or document["version"] != 1
            or not isinstance(document["objects"], dict)):
        raise ValueError("Invalid retained content reviews")
    roles = {"source-original": "original-response", "derived-support": "processing-evidence",
             "acquisition-receipt": "acquisition-receipt", "source-error": "failed-response",
             "uncertain": "unknown"}
    for sha, entry in document["objects"].items():
        if (not re.fullmatch(r"sha256:[0-9a-f]{64}", sha) or not isinstance(entry, dict)
                or set(entry) != {"bytes", "result"} or type(entry["bytes"]) is not int or entry["bytes"] < 0):
            raise ValueError("Invalid reviewed object identity")
        result = entry["result"]
        if (not isinstance(result, dict)
                or set(result) != {"outcome", "classification", "content_kind", "role", "scope", "evidence"}
                or result["classification"] not in roles
                or result["role"] != roles[result["classification"]]
                or result["outcome"] != ("uncertain" if result["classification"] == "uncertain" else "ok")
                or not isinstance(result["content_kind"], str) or not result["content_kind"]
                or not isinstance(result["scope"], dict) or not result["scope"].get("basis")
                or not result["scope"].get("limits")
                or not isinstance(result["evidence"], list) or not result["evidence"]
                or not all(isinstance(e, dict) and e for e in result["evidence"])):
            raise ValueError("Invalid retained content finding")
    return document["objects"]


def fec_processing_metadata(body: bytes) -> dict | None:
    """Recognize bounded FEC v1 caller context, never a generic evidence label.

    This is the source/coverage shape consumed by build_fec_observations.
    Historical SourceEvidence members also include actual publisher responses;
    unknown, expanded or malformed shapes stay unclassified.
    """
    if len(body) > 1024 * 1024:
        return None
    try:
        value = _json(body)
    except (ValueError, UnicodeDecodeError):
        return None
    fields = {
        "version", "source_capture", "source_family", "bulk_group", "object_key", "member",
        "related_collection_ids", "source_authority", "selection", "parsing", "relationships",
        "evidence", "coverage_limits",
    }
    if (not isinstance(value, dict) or set(value) - fields
            or not isinstance(value.get("version"), int) or isinstance(value["version"], bool) or value["version"] != 1
            or not any(isinstance(value.get(key), dict) for key in ("parsing", "selection", "source_capture"))
            or "coverage_limits" in value and (
                not isinstance(value["coverage_limits"], list)
                or not all(isinstance(limit, str) for limit in value["coverage_limits"]))):
        return None
    # These context fields are optional in the owning reader. Empty limits do
    # not turn a locally produced processing record into a publisher response.
    context_field = next(key for key in ("parsing", "selection", "source_capture") if isinstance(value.get(key), dict))
    return {
        "outcome": "ok", "role": "processing-evidence",
        "scope": {"basis": "legacy FEC v1 processing context; not publisher response bytes"},
        "evidence": [{"locator": "/" + context_field, "rule": "legacy-fec-processing-context", "target": "role",
                      "value": "processing-evidence"}],
    }


@lru_cache(maxsize=1)
def _release_validators():
    from jsonschema import Draft202012Validator
    from spicy_docs.releases.format import installed_release_schema_bundle

    return {
        name.removesuffix(".schema.json"): Draft202012Validator(schema)
        for name, schema in installed_release_schema_bundle().items()
    }


def _result(kind: str, role: str, locator: str, rule: str) -> dict:
    return {
        "outcome": "ok", "role": role, "content_kind": kind,
        "classification": "acquisition-receipt" if role == "acquisition-receipt" else "derived-support",
        "scope": {"basis": "Producer-backed retained bytes; generated content, not publisher response",
                  "limits": "Content classification only; no release admission or source completeness claim"},
        "evidence": [{"locator": locator, "rule": rule, "target": "role", "value": role}],
    }


def classification_inputs() -> tuple[Path, ...]:
    """External maintained rule sources shared by import and candidate refresh."""
    import inspect
    from importlib.resources import files
    from rulespec_artifacts import expected_artifact_digest
    from spicy_docs.releases import format as release_format
    from spicy_docs.sources.fec import filings, metadata

    schemas = files("spicy_docs").joinpath("schemas/source_native_release/2.0")
    return tuple(sorted({
        RETAINED_REVIEWS,
        Path(inspect.getfile(expected_artifact_digest)), Path(release_format.__file__),
        Path(filings.__file__), Path(metadata.__file__),
        *(Path(str(entry)) for entry in schemas.iterdir() if entry.is_file()),
    }))


def retained_content_metadata(body: bytes) -> dict:
    """Keep verified originals distinct from generated and unresolved content."""
    if result := generated_content_metadata(body):
        return result
    from io import BytesIO
    from spicy_docs.sources.fec.filings import filing_encoding, filing_records_from_stream
    from spicy_docs.sources.fec.metadata import api_page, parse_api

    kind, records = None, None
    if body.startswith((b"HDR", b"/*")):
        try:  # the maintained choice (strict UTF-8, then cp1252, else refuse), not an assumed latin-1
            encoding = filing_encoding(BytesIO(body))
            records = sum(1 for _ in filing_records_from_stream(
                BytesIO(body), sha256="verified-by-caller", encoding=encoding))
            kind = "fec-filing"
        except ValueError:
            pass
    else:
        try:
            value = parse_api(body)
            if isinstance(value.get("results"), list) and isinstance(value.get("pagination"), dict):
                page = value["pagination"].get("page")
                rows, _, _ = api_page(value, url=f"https://api.open.fec.gov/v1/?page={page}", mode="page")
                kind, records = "openfec-api-response", len(rows)
        except (ValueError, UnicodeDecodeError):
            pass
    if kind:
        return {
            "outcome": "ok", "classification": "source-original", "content_kind": kind,
            "role": "original-response", "scope": {
                "basis": "Verified retained original parsed by maintained FEC source reader", "records": records,
                "limits": "Observed response only; no source completeness or currentness claim"},
            "evidence": [{"locator": "/", "rule": "spicy-docs-fec/" + kind,
                          "target": "role", "value": "original-response"}],
        }
    return {"outcome": "uncertain", "classification": "uncertain", "content_kind": "unclassified-legacy-evidence",
            "scope": {"reason": "Legacy producer retained mixed source and support bytes; content kind unresolved"},
            "evidence": []}


def _artifact_metadata(value: dict) -> dict | None:
    from rulespec_artifacts import (
        ArtifactVerificationError, Producer, expected_artifact_digest, expected_logical_id,
    )

    if value.get("format") != "spicy-artifact" or value.get("formatVersion") != "1.0":
        return None
    # Identity and the producer use their maintained owners. Classifying these
    # bytes as a generated root does not verify its referenced members.
    try:
        producer = Producer.from_dict(value.get("producer"), path="$/producer")
        if (producer.product != "spicy-docs"
                or value.get("artifactDigest") != expected_artifact_digest(value)
                or value.get("logicalId") != expected_logical_id(value)):
            return None
    except (ArtifactVerificationError, ValueError, TypeError):
        return None
    return _result("artifact-manifest", "processing-evidence", "/producer", "rulespec-artifact-identity")


def generated_content_metadata(body: bytes) -> dict | None:
    """Recognize closed maintained shapes, including homogeneous JSONL parts.

    Must only be called after producer provenance and byte identity are checked.
    Unknown bytes return None; a receipt alias is never evidence of content kind.
    """
    if len(body) > 1024 * 1024:
        return None
    if result := fec_processing_metadata(body):
        return dict(result, content_kind="legacy-fec-processing-context", classification="derived-support")
    try:
        values = [_json(body)]
    except (ValueError, UnicodeDecodeError):
        try:
            values = [_json(line) for line in body.splitlines() if line.strip()]
        except (ValueError, UnicodeDecodeError):
            return None
    if not values or any(not isinstance(value, dict) for value in values):
        return None
    if len(values) == 1 and (result := _artifact_metadata(values[0])):
        return result
    roles = {
        "acquisition-page": "acquisition-receipt",
        "acquisition-ledger": "acquisition-receipt",
        "source-native-record": "processing-evidence",
        "publication-receipt": "processing-evidence",
        "scope": "processing-evidence",
        "source-schema-declaration": "processing-evidence",
        "rendition-index": "processing-evidence",
        "release": "processing-evidence",
        "failure": "processing-evidence",
    }
    for kind, role in roles.items():
        validator = _release_validators()[kind]
        fields = set(validator.schema["required"])
        if all(set(value) == fields for value in values) and all(validator.is_valid(value) for value in values):
            if kind == "publication-receipt":
                from spicy_docs.releases.format import FORMAT, FORMAT_VERSION, VERIFIER_ID

                if any(value.get("format") != FORMAT or value.get("formatVersion") != FORMAT_VERSION
                       or value.get("verifierId") != VERIFIER_ID for value in values):
                    continue
            result = _result(kind, role, "/", "spicy-docs-source-native-2.0/" + kind)
            result["scope"]["records"] = len(values)
            return result
    # A generated-looking record that fails its maintained schema is explicitly
    # unresolved. Ordinary responses (including OpenFEC JSON) remain untouched.
    markers = {"evidenceBlobRef", "observationRef", "schemaDigest", "fieldDiagnostics", "artifactDigest"}
    if any(markers.intersection(value) or value.get("format") == "spicyregs-source-native-release"
           for value in values):
        return {"outcome": "uncertain", "classification": "uncertain", "content_kind": "unrecognized-generated-shape",
                "scope": {"reason": "Producer-backed shape does not match maintained schemas"}, "evidence": []}
    return None
