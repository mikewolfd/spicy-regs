"""Repair rows from an explicitly pinned, admitted source release.

Run ``uv run --frozen python -m spicy_regs.pipelines.repair_regulations --help``.
Dockets and documents correct local Parquet. Comments correct the Iceberg
catalog: a dry run by default, which writes only a local receipt, and a catalog
write only with ``--apply``. This does not acquire source objects, update
acquisition manifests or publish. Every invocation rereads its selected input,
so a failed attempt remains retryable without resetting unrelated acquisition
state. Raw-reader callers may use ``repair_records`` with their own retained
input pins.
"""

from collections.abc import Iterable, Mapping
from dataclasses import asdict
from importlib.metadata import version
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from cyclopts import App
from rulespec_artifacts import ArtifactPin, LocalMemberSource
from spicy_docs.schemas.regulations import RECORD_TYPES as SOURCE_TYPES
from spicy_docs.source_native import SourceNativeReleaseReader
from spicy_docs.sources.regulations_gov.api import AttachmentRelationship
from spicy_docs.sources.regulations_gov.attachment_records import attachment_records_json
from spicy_docs.sources.regulations_gov.profile import (
    REGULATIONS_GOV_COMMENT_PROFILE,
    REGULATIONS_GOV_DOCKET_PROFILE,
    REGULATIONS_GOV_DOCUMENT_PROFILE,
)
from spicy_docs.storage.blobs import LocalSourceNativeBlobStore

from spicy_regs.schemas import RECORD_TYPES
from spicy_regs.sources import iceberg
from spicy_regs.transforms.comment_partitions import validate_staged_comments
from spicy_regs.transforms.merge_staging_files import merge_staging_files
from spicy_regs.transforms.regulations_correction import correction_query
from spicy_regs.transforms.write_staging import write_staging


PROFILES = {
    "dockets": REGULATIONS_GOV_DOCKET_PROFILE,
    "documents": REGULATIONS_GOV_DOCUMENT_PROFILE,
    "comments": REGULATIONS_GOV_COMMENT_PROFILE,
}
COMMENT_RECEIPT = "comments-repair.json"


def repair_records(
    records: Iterable[Mapping],
    *,
    table: str,
    output_dir: Path,
    apply: bool = False,
    expected_snapshot: int | None = None,
    source_pins: Mapping | None = None,
    attachment_relationships: Mapping[str, AttachmentRelationship] | None = None,
) -> dict:
    """Stage a complete explicit reread before correcting output rows.

    Source-native release callers pass selected records after admission. Raw
    reader callers must retain object pins and raise on unresolved outcomes;
    neither raw reads nor this operation establish collection completeness.
    Current owner extractors supply source facts; the host retains its extra
    enrichment columns. Unrelated rows and newer source observations survive.
    Missing source fields are genuine NULL values, not requests to retain stale
    mapped facts. An exception during input consumption prevents every merge.
    Explicit ``attachment_relationships`` must cover exactly the selected documents;
    ``source_pins`` retains the caller's edition association. The owner checks each
    captured relationship identity and completeness; this API does not infer that
    a current relationship belongs to a historical public document edition.
    Comments go to the catalog (see :func:`_repair_comments`); ``apply`` and
    ``expected_snapshot`` apply to comments only. ``source_pins`` retains
    provenance in the receipt for all explicit repairs.
    """
    if attachment_relationships is not None:
        if table != "documents":
            raise ValueError("attachment relationships apply only to documents")
        if not source_pins:
            raise ValueError("attachment relationships require retained source pins and edition association")
    remaining_relationships = set(attachment_relationships or {})
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
            if attachment_relationships is not None:
                if identity not in attachment_relationships:
                    raise ValueError(f"attachment relationship unread for selected document {identity}")
                row["attachment_records_json"] = attachment_records_json(dict(raw), attachment_relationships[identity])
                remaining_relationships.discard(identity)
            batch.append({column: row.get(column) for column in host.schema})
            count += 1
            if len(batch) == 1000:
                write_staging(str(count), table, batch, staging, host.schema)
                batch.clear()
        write_staging(str(count), table, batch, staging, host.schema)
        if remaining_relationships:
            raise ValueError("attachment relationships contain unselected document identities")

        if table == "comments":
            receipt = _repair_comments(staging, output_dir, count=count, apply=apply,
                                       expected_snapshot=expected_snapshot, source_pins=source_pins)
            return {
                "table": table,
                "input_records": count,
                "output_paths": [str(output_dir / COMMENT_RECEIPT)] if count else [],
                "changed_rows": len(receipt["rows"]),
                "applied_snapshot": receipt["applied_snapshot"],
                "spicy_docs_version": version("spicy-docs"),
                "scope": "explicit retained input into the comments catalog; dry run unless --apply; no publication",
                "acquisition_manifest_changed": False,
            }
        changed: list[Path] = []
        if count:
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
        "source_pins": dict(source_pins or {}),
        "attachment_relationships_read": len(attachment_relationships) if attachment_relationships is not None else None,
    }


def _repair_comments(
    staging: Path, output_dir: Path, *, count: int, apply: bool, expected_snapshot: int | None,
    source_pins: Mapping | None,
) -> dict:
    """Correct the staged comment identities in the catalog, reading priors at one pinned snapshot.

    The source-recency rule is ``correction_query``, as for local tables. The
    receipt records the pin, every changed cell and the rows to write, and is
    written before any catalog write. ``apply`` refuses a staged identity the
    catalog lacks (repair corrects rows, it does not insert them) and a snapshot
    that moved since the read, then replaces exactly the changed rows and
    verifies that each now appears once, as written.
    """
    receipt: dict = {"rows": [], "applied_snapshot": None}
    if not count:
        return receipt
    record_type = RECORD_TYPES["comments"]
    columns, key = list(record_type.schema), record_type.dedup_key
    validate_staged_comments(staging)
    files = ", ".join(f"'{iceberg._sql_str(str(path))}'" for path in sorted((staging / "comments").glob("*.parquet")))
    project = ", ".join(f'CAST("{column}" AS VARCHAR) AS "{column}"' for column in columns)
    con = iceberg._connect()
    try:
        snapshot = iceberg._read_snapshot(con, record_type)
        if expected_snapshot is not None and snapshot.snapshot_id != expected_snapshot:
            raise RuntimeError(f"catalog is at snapshot {snapshot.snapshot_id}, not the reviewed {expected_snapshot}")
        current_columns = iceberg._column_types(con, record_type)
        missing_write_columns = set(columns) - current_columns.keys()
        for column in iceberg._COMMENT_REFERENCE_COLUMNS:
            if column in current_columns and current_columns[column] != "VARCHAR":
                raise ValueError(f"comments.{column} must be VARCHAR, found {current_columns[column]}")
        prior_sql = iceberg._snapshot_query(record_type, snapshot)
        prior_columns = {row[0] for row in con.execute(f"DESCRIBE ({prior_sql})").fetchall()}
        missing_columns = set(columns) - prior_columns
        unsupported_missing = (missing_columns | missing_write_columns) - set(iceberg._COMMENT_REFERENCE_COLUMNS)
        if unsupported_missing:
            raise ValueError("Unsupported missing comment columns: " + ", ".join(sorted(unsupported_missing)))
        # A dry run must not migrate the catalog. Nullable fields absent from
        # the reviewed snapshot represent unread source values, never empty lists.
        prior_project = ", ".join(
            f'NULL::VARCHAR AS "{column}"' if column in missing_columns
            else f'CAST("{column}" AS VARCHAR) AS "{column}"' for column in columns
        )
        con.execute(f"CREATE TEMP TABLE _repair_fresh AS SELECT {project} FROM read_parquet([{files}], union_by_name=true)")
        con.execute(f"""
            CREATE TEMP TABLE _repair_prior AS
            SELECT {prior_project} FROM ({prior_sql})
            WHERE "{key}" IN (SELECT "{key}" FROM _repair_fresh)
        """)
        corrected = correction_query(con, fresh_sql="SELECT * FROM _repair_fresh",
                                     prior_sql="SELECT * FROM _repair_prior", columns=columns, key=key)
        differs = " OR ".join(f'c."{column}" IS DISTINCT FROM p."{column}"' for column in columns)
        con.execute(f"""
            CREATE TEMP TABLE _repair_write AS
            SELECT c.* FROM ({corrected}) c JOIN _repair_prior p ON c."{key}" = p."{key}" WHERE {differs}
        """)
        before = {row[key]: row for row in _rows(con, f"""
            SELECT p.* FROM _repair_prior p JOIN _repair_write w ON p."{key}" = w."{key}"
        """)}
        rows = _rows(con, f'SELECT * FROM _repair_write ORDER BY "{key}"')
        receipt = {
            "format": "spicy-regs-comment-repair",
            "version": 1,
            "mode": "apply" if apply else "dry-run",
            "source": dict(source_pins) if source_pins is not None else None,
            "catalog_snapshot": asdict(snapshot),
            "schema_migration_required": sorted(missing_write_columns),
            "snapshot_columns_null_filled": sorted(missing_columns),
            "identities": [row[0] for row in con.execute(f'SELECT "{key}" FROM _repair_fresh ORDER BY 1').fetchall()],
            "missing_identities": [row[0] for row in con.execute(f"""
                SELECT "{key}" FROM _repair_fresh EXCEPT SELECT "{key}" FROM _repair_prior ORDER BY 1
            """).fetchall()],
            "changes": [
                {key: row[key], "cells": {column: {"before": before[row[key]][column], "after": row[column]}
                                          for column in columns if before[row[key]][column] != row[column]}}
                for row in rows
            ],
            "rows": rows,
            "applied_snapshot": None,
        }
        path = output_dir / COMMENT_RECEIPT
        path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
        if not apply:
            return receipt
        if missing_write_columns:
            raise ValueError(
                "Migrate the comments schema through the catalog ingestion/export path, "
                "then rerun the repair against its current write schema; missing: " + ", ".join(sorted(missing_write_columns))
            )
        if receipt["missing_identities"]:
            raise ValueError(
                f"comment repair corrects existing rows; {len(receipt['missing_identities'])} staged "
                "identities are not in the catalog"
            )
        if iceberg._read_snapshot(con, record_type) != snapshot:
            raise RuntimeError("the comments catalog moved since the repair read it; rerun the repair")
        if rows:
            iceberg.replace_rows(con, record_type, "_repair_write", expected_prior="_repair_prior")
            same = " AND ".join(f't."{column}" IS NOT DISTINCT FROM w."{column}"' for column in columns)
            found, matching = con.execute(f"""
                SELECT count(*), count(*) FILTER (WHERE {same})
                FROM {iceberg._qualified(record_type)} t JOIN _repair_write w ON t."{key}" = w."{key}"
            """).fetchone()
            if found != len(rows) or matching != len(rows):
                raise RuntimeError(
                    f"after the replace the catalog holds {found} rows ({matching} as written) for "
                    f"{len(rows)} repaired identities; a DELETE left rows behind"
                )
            receipt["applied_snapshot"] = asdict(iceberg._read_snapshot(con, record_type))
            path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
        return receipt
    finally:
        con.close()


def _rows(con, sql: str) -> list[dict]:
    cursor = con.execute(sql)
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


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
    apply: bool = False,
    expected_snapshot: int | None = None,
) -> None:
    """Correct rows from one explicitly trusted source release.

    Dockets and documents: existing local Parquet files are the prior
    generation. No remote prior is downloaded: operators must retain the
    intended files before this command. Comments: the catalog is the prior. A
    dry run writes ``comments-repair.json``; ``--apply`` also writes the
    catalog, and ``--expected-snapshot`` pins it to a reviewed dry run.
    """
    if table not in PROFILES:
        raise ValueError(f"table must be one of {', '.join(PROFILES)}")
    if table != "comments" and (apply or expected_snapshot is not None):
        raise ValueError("--apply and --expected-snapshot apply to --table comments only")
    reader = SourceNativeReleaseReader(
        LocalMemberSource(release),
        blob_source=LocalSourceNativeBlobStore(blob_store, create=False),
        profile=PROFILES[table],
        expected_pin=ArtifactPin(logical_id, artifact_digest),
        accepted_verifier_implementation_ids=frozenset({accepted_verifier_implementation_id}),
    )
    if reader.collection_outcome["failedRecordCount"]:
        raise ValueError("source correction requires a release without unresolved records")
    source_release = {"logical_id": logical_id, "artifact_digest": artifact_digest}
    result = repair_records(
        (row["record"] for row in reader.iter_records()), table=table, output_dir=output_dir, apply=apply,
        expected_snapshot=expected_snapshot,
        source_pins={**source_release, "accepted_verifier_implementation_id": accepted_verifier_implementation_id,
                "collection_outcome": dict(reader.collection_outcome)},
    )
    result["source_release"] = source_release
    result["collection_outcome"] = dict(reader.collection_outcome)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    app()
