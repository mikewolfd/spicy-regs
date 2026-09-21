"""Build selected FEC metadata companions and reported relationships offline.

SpicyDocs verifies and parses the inputs. This adapter keeps complete provider
records, exact evidence coordinates, and the scope of every selected collection.
It never selects current amendments or combines overlapping source observations.
All outputs enter a new directory together after every input has been consumed.
"""

from __future__ import annotations

import importlib
import json
import re
from contextlib import ExitStack
from pathlib import Path
from tempfile import TemporaryDirectory, TemporaryFile

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.transforms.fec_relationships import SCHEMA as RELATIONSHIP_SCHEMA
from spicy_regs.transforms.fec_relationships import api_relationships, bulk_relationships, statement_relationships

RECORD_COLUMNS = (
    "collection_id",
    "source_family",
    "profile",
    "source_record_id",
    "committee_id",
    "candidate_id",
    "filing_id",
    "legal_doc_id",
    "audit_case_id",
    "source_sha256",
    "source_url",
    "observed_at",
    "source_locator_json",
    "metadata_json",
    "assets_json",
    "embedded_bodies_json",
    "source_record_json",
)
COLLECTION_COLUMNS = (
    "collection_id",
    "source_family",
    "profile",
    "source_system_id",
    "source_state_scope",
    "record_count",
    "relationship_count",
    "record_outcome",
    "requested_scope_json",
    "coverage_limits_json",
    "collection_outcome_json",
    "artifact_sha256",
)
RECORD_SCHEMA = pa.schema([(name, pa.string()) for name in RECORD_COLUMNS])
COLLECTION_SCHEMA = pa.schema([(name, pa.string()) for name in COLLECTION_COLUMNS])
OUTPUTS = ("fec_source_records.parquet", "fec_collections.parquet", "fec_relationships.parquet")
_PROFILES = {
    "committee": ("profile", "FEC_COMMITTEE_CENSUS_PROFILE", "iter_retained_committee_pages"),
    "candidate": ("candidate_profile", "FEC_CANDIDATE_QUERY_PROFILE", "iter_retained_candidate_pages"),
    "filing": ("filing_profile", "FEC_FILING_QUERY_PROFILE", "iter_retained_filing_pages"),
    "legal": ("legal_profile", "FEC_LEGAL_QUERY_PROFILE", "iter_retained_legal_pages"),
    "audit": ("audit_profile", "FEC_AUDIT_QUERY_PROFILE", "iter_retained_audit_pages"),
    "bulk": ("bulk_profile", "FEC_BULK_FILES_PROFILE", None),
    "positional": ("row_profile", "FEC_POSITIONAL_ROWS_PROFILE", None),
}


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _profile(name):
    module_name, constant, iterator = _PROFILES[name]
    module = importlib.import_module("spicy_docs.sources.fec." + module_name)
    return getattr(module, constant), getattr(module, iterator) if iterator else None


def _path(value, base):
    if not isinstance(value, str) or not value:
        raise ValueError("FEC input paths must be nonempty strings")
    path = Path(value)
    return path if path.is_absolute() else base / path


def _load_manifest(path):
    from spicy_docs.sources.fec.catalog import official_sources

    value = json.loads(path.read_bytes())
    if (
        not isinstance(value, dict)
        or set(value) != {"version", "collections"}
        or type(value["version"]) is not int
        or value["version"] != 1
    ):
        raise ValueError("expected FEC manifest version 1 and collections")
    collections = value["collections"]
    if not isinstance(collections, list) or not collections:
        raise ValueError("FEC collections must be a nonempty list")
    families = {item["id"] for item in official_sources()}
    seen = set()
    common = {"collection_id", "source_family", "profile", "blob_root"}
    for item in collections:
        if not isinstance(item, dict):
            raise ValueError("FEC collection must be an object")
        mode = (
            {"scope"}
            if "scope" in item
            else {"captures"}
            if "captures" in item
            else {"release_path", "artifact_sha256", "verifier_implementation_id"}
        )
        required = common | mode
        if set(item) - {"field_mapping"} != required:
            raise ValueError("FEC collection fields differ from the selected input mode")
        identity = item["collection_id"]
        if not isinstance(identity, str) or not identity or identity in seen:
            raise ValueError("FEC collection_id must be nonempty and unique")
        seen.add(identity)
        if item["source_family"] not in families or item["profile"] not in _PROFILES:
            raise ValueError("unknown official FEC source family or reader profile")
        if "captures" in item and _PROFILES[item["profile"]][2] is None:
            raise ValueError("direct captures support API query profiles only")
        if "scope" in item and item["profile"] != "positional":
            raise ValueError("direct scope supports the positional row profile only")
        if "field_mapping" in item:
            mapping = item["field_mapping"]
            if item["profile"] != "positional" or not isinstance(mapping, dict):
                raise ValueError("field_mapping requires positional input")
            fields = {"header_collection_id", "header_row_ordinal", "data_has_header"}
            if "relationship_family" in mapping:
                fields |= {"relationship_family", "cycle"}
                if (
                    mapping["relationship_family"]
                    not in {"linkage", "candidate_master", "committee_master", "form1_bulk", "leadership"}
                    or type(mapping.get("cycle")) is not int
                ):
                    raise ValueError("bulk relationships require an explicit supported family and cycle")
            if (
                set(mapping) != fields
                or not isinstance(mapping["header_collection_id"], str)
                or not mapping["header_collection_id"]
                or type(mapping["header_row_ordinal"]) is not int
                or mapping["header_row_ordinal"] != 0
                or type(mapping["data_has_header"]) is not bool
            ):
                raise ValueError("field_mapping requires a selected header collection, ordinal zero and header flag")
            if mapping["data_has_header"] != (mapping["header_collection_id"] == item["collection_id"]):
                raise ValueError(
                    "embedded headers must name their own collection; external headers belong to an earlier collection"
                )
        if "artifact_sha256" in item and (
            not isinstance(item["artifact_sha256"], str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", item["artifact_sha256"]) is None
            or not isinstance(item["verifier_implementation_id"], str)
            or not item["verifier_implementation_id"]
        ):
            raise ValueError("release inputs require an expected digest and verifier implementation")
    return collections


def _release(item, base, profile):
    from rulespec_artifacts import ArtifactPin, LocalMemberSource
    from spicy_docs.source_native import SourceNativeReleaseReader
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore

    root = _path(item["release_path"], base)
    artifact = json.loads((root / "artifact.json").read_bytes())
    reader = SourceNativeReleaseReader(
        LocalMemberSource(root),
        blob_source=LocalSourceNativeBlobStore(_path(item["blob_root"], base)),
        profile=profile,
        expected_pin=ArtifactPin(logical_id=artifact["logicalId"], artifact_digest=item["artifact_sha256"]),
        accepted_verifier_implementation_ids=frozenset({item["verifier_implementation_id"]}),
    )
    if reader.collection_outcome["failedRecordCount"] or reader.collection_outcome["discardedObservationCount"]:
        raise ValueError("FEC companion delivery requires a release without failed or discarded records")
    return reader.iter_records(), reader.collection_outcome


def _query(item, base, profile, iterator):
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore

    scope = profile.validate_query_scope({"captures": item["captures"]})
    outcome = {
        "requestedScope": scope,
        "sourceStateScope": profile.source_state_scope,
        "acquisitionPolicy": profile.acquisition_policy(scope),
        "verification": "retained-query-digests-membership-counts-and-source-identities",
    }

    def records():
        traversal = profile.traversal_check()
        seen = set()
        count = 0
        window = scope["captures"][0]["requestUrl"]
        for index, page in enumerate(
            iterator(scope["captures"], blob_source=LocalSourceNativeBlobStore(_path(item["blob_root"], base)))
        ):
            response = profile.parse_page_response(page.response_bytes)
            if not profile.records_included(response, query_scope=scope, page_window=window):
                raise ValueError("FEC query page falls outside the selected captures")
            traversal.add(response, page_index=index)
            for value in response["results"]:
                record = profile.classify_record(value)
                profile.validate_record_scope(record, query_scope=scope, page_window=window)
                wrapped = profile.wrap_record(record, schema_digest=profile.source_schema_digest())
                identity = wrapped["sourceRecordId"]
                if identity in seen:
                    raise ValueError("FEC selected query repeats a source identity")
                seen.add(identity)
                count += 1
                yield wrapped
        traversal.finish()
        outcome.update(
            recordOutcome="no-record-rejections" if count else "empty",
            publishedRecordCount=count,
            failedRecordCount=0,
            discardedObservationCount=0,
            acquisitionEvidenceCount=len(scope["captures"]),
        )

    return records(), outcome


def _positional(item, base, profile):
    from spicy_docs.sources.fec.row_profile import iter_retained_positional_rows
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore, iter_verified_blob

    scope = profile.validate_query_scope(item["scope"])
    outcome = {
        "requestedScope": scope,
        "sourceStateScope": profile.source_state_scope,
        "acquisitionPolicy": profile.acquisition_policy(scope),
        "verification": "retained-original-digest-and-complete-selected-stream",
    }

    def records():
        store = LocalSourceNativeBlobStore(_path(item["blob_root"], base))
        (page,) = iter_retained_positional_rows(scope, blob_source=store)
        count = 0
        traversal = profile.traversal_check()
        # Verify the entire original before parsing. The provider verifies ZIP
        # membership/CRC and bounds; spooling avoids holding large files in RAM.
        with TemporaryFile() as original:
            for chunk in iter_verified_blob(store, page.blob_ref, page.byte_size):
                original.write(chunk)
            original.seek(0)
            for index, response in enumerate(
                profile.parse_file_stream(
                    original,
                    query_scope=scope,
                    request_key=page.request_key,
                    evidence_ref=page.blob_ref,
                    byte_size=page.byte_size,
                    media_type=page.evidence_media_type,
                )
            ):
                traversal.add(response, page_index=index)
                for value in response["results"]:
                    record = profile.classify_record(value)
                    profile.validate_record_scope(record, query_scope=scope, page_window=0)
                    if record["ordinal"] != count:
                        raise ValueError("FEC positional stream skips or repeats an ordinal")
                    count += 1
                    yield profile.wrap_record(record, schema_digest=profile.source_schema_digest())
            traversal.finish()
        outcome.update(
            recordOutcome="no-record-rejections" if count else "empty",
            publishedRecordCount=count,
            failedRecordCount=0,
            discardedObservationCount=0,
            acquisitionEvidenceCount=1,
        )

    return records(), outcome


def _shape(item, wrapped, outcome):
    record = wrapped["record"]
    scope = outcome["requestedScope"]
    capture = record.get("capture", scope.get("capture"))
    if not capture:
        raise ValueError("FEC record has no source capture")
    locator = {"collection_id": item["collection_id"], "source_record_id": wrapped["sourceRecordId"]}
    metadata = record.get("metadata", record)
    if item["profile"] == "positional":
        metadata = record["record"]
        if "source" in metadata:
            locator.update(metadata["source"])
        else:
            locator["embedded_bodies"] = metadata["embedded_bodies"]
        locator.update(member=record["member"], ordinal=record["ordinal"])
    elif "source_pointer" in record:
        locator["json_pointer"] = record["source_pointer"]
    else:
        locator["object_key"] = capture["objectKey"]
    row = dict.fromkeys(RECORD_COLUMNS)
    row.update(
        collection_id=item["collection_id"],
        source_family=item["source_family"],
        profile=item["profile"],
        source_record_id=wrapped["sourceRecordId"],
        source_sha256=capture["responseSha256"],
        source_url=capture["requestUrl"],
        observed_at=capture["observedAt"],
        source_locator_json=_json(locator),
        metadata_json=_json(metadata),
        assets_json=_json(record.get("assets", [])),
        embedded_bodies_json=_json(metadata.get("embedded_bodies", record.get("embedded_bodies", []))),
        source_record_json=_json(wrapped),
    )
    for column, field in (
        ("committee_id", "committee_id"),
        ("candidate_id", "candidate_id"),
        ("filing_id", "sub_id"),
        ("legal_doc_id", "doc_id"),
        ("audit_case_id", "audit_case_id"),
    ):
        value = metadata.get(field)
        row[column] = value if isinstance(value, str) else None
    return row, locator


def _named_fields(item, wrapped, row, locator, headers):
    """Use only explicitly selected, byte-verified source header cells."""
    if item["profile"] != "positional":
        return None
    native = wrapped["record"]
    fields = native["record"].get("fields")
    if native["ordinal"] == 0 and isinstance(fields, list):
        headers[item["collection_id"]] = (
            fields,
            {
                "collection_id": item["collection_id"],
                "source_record_id": wrapped["sourceRecordId"],
                "source_sha256": row["source_sha256"],
                "source_url": row["source_url"],
                "source_locator": dict(locator),
            },
        )
    mapping = item.get("field_mapping")
    if mapping is None:
        return None
    selected = headers.get(mapping["header_collection_id"])
    if selected is None:
        raise ValueError("selected field header must be present in its own or an earlier collection")
    names, evidence = selected
    if not names or any(not name for name in names) or len(set(names)) != len(names):
        raise ValueError("source field header names must be nonempty and unique")
    locator["field_mapping"] = evidence
    row["source_locator_json"] = _json(locator)
    if mapping["data_has_header"] and native["ordinal"] == 0 or fields == []:
        return None
    if not isinstance(fields, list) or len(fields) != len(names):
        raise ValueError("source row field count differs from its selected header")
    mapped = dict(zip(names, fields, strict=True))
    row["metadata_json"] = _json(mapped)
    # These are literal source ID fields, not validated entity classifications.
    # In particular, aggregate candidate placeholders remain source values.
    for column, aliases in (
        ("committee_id", ("committee_id", "CMTE_ID", "COMMITTEE_ID", "Committee_Id")),
        ("candidate_id", ("candidate_id", "CAND_ID", "CANDIDATE_ID", "Cand_Id", "cand_id")),
    ):
        values = [mapped[name] for name in aliases if name in mapped]
        if values:
            if len(set(values)) != 1:
                raise ValueError(f"conflicting source aliases for {column}")
            row[column] = values[0]
    return mapped


def _relationships(item, wrapped, row, locator, version, mapped):
    evidence = {
        "sha256": row["source_sha256"],
        "url": row["source_url"],
        "observed_at": row["observed_at"],
        "locator": locator,
    }
    if mapped is not None and "relationship_family" in item["field_mapping"]:
        mapping = item["field_mapping"]
        yield from bulk_relationships(mapping["relationship_family"], mapped, evidence=evidence, cycle=mapping["cycle"])
    elif item["profile"] == "committee":
        yield from api_relationships(wrapped["record"]["metadata"], evidence=evidence)
    elif item["profile"] == "positional":
        record = wrapped["record"]["record"]
        fields = record.get("fields")
        if (
            isinstance(fields, dict)
            and fields.get("0") in {"F1N", "F1A", "F1S", "F2N", "F2A", "F2S"}
            and version in {"8.3", "8.4"}
        ):
            yield from statement_relationships(record, version=version, evidence=evidence)


def build_fec_observations(manifest: Path, output_dir: Path, *, batch_size: int = 2000) -> tuple[Path, ...]:
    """Verify selected inputs and install one new directory; existing outputs refuse.

    Relative input paths resolve beside the manifest. No HTTP request, prior-table
    merge, source mutation, or publication occurs. Replays use a new destination.
    """
    if type(batch_size) is not int or batch_size < 1:
        raise ValueError("batch_size must be a positive integer")
    manifest, output_dir = Path(manifest), Path(output_dir)
    if output_dir.exists():
        raise FileExistsError("FEC output directory already exists; select a new generation directory")
    collections = _load_manifest(manifest)
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=".fec-observations-", dir=output_dir.parent) as scratch:
        stage = Path(scratch) / "tables"
        stage.mkdir()
        schemas = (RECORD_SCHEMA, COLLECTION_SCHEMA, RELATIONSHIP_SCHEMA)
        with ExitStack() as stack:
            writers = [
                stack.enter_context(pq.ParquetWriter(stage / name, schema, compression="zstd"))
                for name, schema in zip(OUTPUTS, schemas, strict=True)
            ]
            buffers = [[], [], []]
            headers = {}

            def append(index, row):
                buffers[index].append(row)
                if len(buffers[index]) >= batch_size:
                    flush(index)

            def flush(index):
                if buffers[index]:
                    writers[index].write_table(pa.Table.from_pylist(buffers[index], schema=schemas[index]))
                    buffers[index].clear()

            for item in collections:
                profile, iterator = _profile(item["profile"])
                if "scope" in item:
                    records, outcome = _positional(item, manifest.parent, profile)
                elif "captures" in item:
                    records, outcome = _query(item, manifest.parent, profile, iterator)
                else:
                    records, outcome = _release(item, manifest.parent, profile)
                record_count = relationship_count = 0
                version = None
                for wrapped in records:
                    row, locator = _shape(item, wrapped, outcome)
                    if item["profile"] == "positional":
                        native = wrapped["record"]["record"]
                        if native.get("kind") == "header":
                            version = native["format_version"]
                    mapped = _named_fields(item, wrapped, row, locator, headers)
                    append(0, row)
                    record_count += 1
                    for relationship in _relationships(item, wrapped, row, locator, version, mapped):
                        append(2, relationship)
                        relationship_count += 1
                if record_count != outcome["publishedRecordCount"]:
                    raise ValueError("FEC output count differs from selected input membership")
                if "field_mapping" in item:
                    outcome = {**outcome, "tableFieldMapping": item["field_mapping"]}
                append(
                    1,
                    {
                        "collection_id": item["collection_id"],
                        "source_family": item["source_family"],
                        "profile": item["profile"],
                        "source_system_id": profile.source_system_id,
                        "source_state_scope": outcome["sourceStateScope"],
                        "record_count": str(record_count),
                        "relationship_count": str(relationship_count),
                        "record_outcome": outcome["recordOutcome"],
                        "requested_scope_json": _json(outcome["requestedScope"]),
                        "coverage_limits_json": _json(outcome["acquisitionPolicy"].get("coverageLimits", [])),
                        "collection_outcome_json": _json(outcome),
                        "artifact_sha256": item.get("artifact_sha256"),
                    },
                )
            for index in range(3):
                flush(index)
        # A failed input never replaces any member of an earlier generation.
        stage.rename(output_dir)
    return tuple(output_dir / name for name in OUTPUTS)
