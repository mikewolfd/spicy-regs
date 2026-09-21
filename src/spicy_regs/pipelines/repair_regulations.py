"""Repair local Parquet from an explicitly pinned, admitted source release.

Run ``uv run --frozen python -m spicy_regs.pipelines.repair_regulations --help``.
This does not acquire source objects, update acquisition manifests, mutate an
Iceberg catalog, or publish. Every invocation rereads its selected input, so a
failed attempt remains retryable without resetting unrelated acquisition state.
Raw-reader callers may use ``repair_records`` with their own retained input pins.
"""

from collections.abc import Iterable, Mapping
from importlib.metadata import version
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from cyclopts import App
from rulespec_artifacts import ArtifactPin, LocalMemberSource
from spicy_docs.schemas.regulations import RECORD_TYPES as SOURCE_TYPES
from spicy_docs.source_native import SourceNativeReleaseReader
from spicy_docs.sources.regulations_gov.profile import (
    REGULATIONS_GOV_COMMENT_PROFILE,
    REGULATIONS_GOV_DOCKET_PROFILE,
    REGULATIONS_GOV_DOCUMENT_PROFILE,
)
from spicy_docs.storage.blobs import LocalSourceNativeBlobStore

from spicy_regs.schemas import RECORD_TYPES
from spicy_regs.transforms.merge_comments_partitioned import merge_comments_partitioned
from spicy_regs.transforms.merge_staging_files import merge_staging_files
from spicy_regs.transforms.update_comments_index import update_comments_index
from spicy_regs.transforms.write_staging import write_staging


PROFILES = {
    "dockets": REGULATIONS_GOV_DOCKET_PROFILE,
    "documents": REGULATIONS_GOV_DOCUMENT_PROFILE,
    "comments": REGULATIONS_GOV_COMMENT_PROFILE,
}


def repair_records(records: Iterable[Mapping], *, table: str, output_dir: Path) -> dict:
    """Stage a complete explicit reread before correcting local output rows.

    Source-native release callers pass selected records after admission. Raw
    reader callers must retain object pins and raise on unresolved outcomes;
    neither raw reads nor this operation establish collection completeness.
    Current owner extractors supply source facts; the host retains its extra
    enrichment columns. Unrelated rows and newer source observations survive.
    Missing source fields are genuine NULL values, not requests to retain stale
    mapped facts. An exception during input consumption prevents every merge.
    """
    if table not in PROFILES:
        raise ValueError(f"unsupported regulatory source table: {table}")
    output_dir.mkdir(parents=True, exist_ok=True)
    host = RECORD_TYPES[table]
    source = SOURCE_TYPES[table]
    count = 0
    with TemporaryDirectory(prefix=".source-repair-", dir=output_dir) as directory:
        staging = Path(directory)
        batch = []
        for raw in records:
            row = source.extract(dict(raw))
            identity = row.get(host.dedup_key)
            if not isinstance(identity, str) or not identity.strip():
                raise ValueError(f"source correction requires a nonblank {host.dedup_key}")
            batch.append({column: row.get(column) for column in host.schema})
            count += 1
            if len(batch) == 1000:
                write_staging(str(count), table, batch, staging, host.schema)
                batch.clear()
        write_staging(str(count), table, batch, staging, host.schema)

        changed: list[Path] = []
        if count:
            if table == "comments":
                changed = merge_comments_partitioned(
                    staging,
                    output_dir,
                    host.schema,
                    host.dedup_key,
                    source_correction=True,
                    download_existing=False,
                )
                update_comments_index(output_dir, changed)
            else:
                merge_staging_files(
                    staging,
                    output_dir,
                    [table],
                    {table: host.schema},
                    {table: host.dedup_key},
                    source_correction=True,
                )
                changed = [output_dir / f"{table}.parquet"]
    return {
        "table": table,
        "input_records": count,
        "output_paths": [str(path) for path in changed],
        "spicy_docs_version": version("spicy-docs"),
        "scope": "explicit retained input into local Parquet; no acquisition, Iceberg update or publication",
        "acquisition_manifest_changed": False,
    }


app = App(help=__doc__)


@app.default
def main(
    *,
    table: str,
    release: Path,
    blob_store: Path,
    logical_id: str,
    artifact_digest: str,
    accepted_verifier_implementation_id: str,
    output_dir: Path,
) -> None:
    """Correct local rows from one explicitly trusted source release.

    Existing local Parquet files are the prior generation. No remote prior is
    downloaded: operators must retain the intended files before this command.
    Comments use the existing partition layout and preserve its local index.
    """
    if table not in PROFILES:
        raise ValueError(f"table must be one of {', '.join(PROFILES)}")
    reader = SourceNativeReleaseReader(
        LocalMemberSource(release),
        blob_source=LocalSourceNativeBlobStore(blob_store, create=False),
        profile=PROFILES[table],
        expected_pin=ArtifactPin(logical_id, artifact_digest),
        accepted_verifier_implementation_ids=frozenset({accepted_verifier_implementation_id}),
    )
    if reader.collection_outcome["failedRecordCount"]:
        raise ValueError("source correction requires a release without unresolved records")
    result = repair_records((row["record"] for row in reader.iter_records()), table=table, output_dir=output_dir)
    result["source_release"] = {"logical_id": logical_id, "artifact_digest": artifact_digest}
    result["collection_outcome"] = dict(reader.collection_outcome)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    app()
