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

from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import asdict
from datetime import UTC, datetime
from importlib.metadata import version
import json
from pathlib import Path
import re
from tempfile import TemporaryDirectory

from cyclopts import App
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from rulespec_artifacts import ArtifactPin, LocalMemberSource
from spicy_docs.schemas.regulations import RECORD_TYPES as SOURCE_TYPES, _docket_rin
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
DOCKETS_RIN_RECEIPT = "dockets-rin-repair.json"
_EXPECT_PRIOR_RE = re.compile(r"[0-9a-f]{8,64}")


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
            receipt = _repair_comments(
                staging,
                output_dir,
                count=count,
                apply=apply,
                expected_snapshot=expected_snapshot,
                source_pins=source_pins,
            )
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
            from spicy_regs.pipelines.regulatory_publication import finish_dataset, restore_dataset

            work = staging / "processing"
            work.mkdir()
            restore_dataset(output_dir, table, work / f"{table}.parquet")
            merge_staging_files(
                staging,
                work,
                [table],
                {table: host.schema},
                {table: host.dedup_key},
                source_correction=True,
            )
            finish_dataset(output_dir, table, work / f"{table}.parquet", publish=False)
            changed = [output_dir / f"{table}.parquet"]
    return {
        "table": table,
        "input_records": count,
        "output_paths": [str(path) for path in changed],
        "spicy_docs_version": version("spicy-docs"),
        "scope": "explicit retained input into local Parquet; no acquisition, Iceberg update or publication",
        "acquisition_manifest_changed": False,
        "source_pins": dict(source_pins or {}),
        "attachment_relationships_read": len(attachment_relationships)
        if attachment_relationships is not None
        else None,
    }


def _repair_comments(
    staging: Path,
    output_dir: Path,
    *,
    count: int,
    apply: bool,
    expected_snapshot: int | None,
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
    project = ", ".join(f'CAST("{column}" AS {record_type.sql_type(column)}) AS "{column}"' for column in columns)
    con = iceberg._connect()
    try:
        from spicy_regs.sources.regulatory_catalog import processing_table, ensure_native

        if apply:
            ensure_native(con, record_type)
        snapshot = iceberg._read_snapshot(con, record_type)
        if expected_snapshot is not None and snapshot.snapshot_id != expected_snapshot:
            raise RuntimeError(f"catalog is at snapshot {snapshot.snapshot_id}, not the reviewed {expected_snapshot}")
        con.execute(
            f"CREATE TEMP TABLE _repair_fresh AS SELECT {project} FROM read_parquet([{files}], union_by_name=true)"
        )
        prior_table = processing_table(con, record_type, where=f'"{key}" IN (SELECT "{key}" FROM _repair_fresh)')
        if iceberg._read_snapshot(con, record_type) != snapshot:
            raise RuntimeError("the comments catalog moved since the repair read it; rerun the repair")
        prior_sql = f"SELECT * FROM {prior_table}"
        con.execute(f"""
            CREATE TEMP TABLE _repair_prior AS
            SELECT {project} FROM ({prior_sql})
            WHERE "{key}" IN (SELECT "{key}" FROM _repair_fresh)
        """)
        corrected = correction_query(
            con,
            fresh_sql="SELECT * FROM _repair_fresh",
            prior_sql="SELECT * FROM _repair_prior",
            columns=columns,
            key=key,
        )
        differs = " OR ".join(f'c."{column}" IS DISTINCT FROM p."{column}"' for column in columns)
        con.execute(f"""
            CREATE TEMP TABLE _repair_write AS
            SELECT c.* FROM ({corrected}) c JOIN _repair_prior p ON c."{key}" = p."{key}" WHERE {differs}
        """)
        before = {
            row[key]: row
            for row in _rows(
                con,
                f"""
            SELECT p.* FROM _repair_prior p JOIN _repair_write w ON p."{key}" = w."{key}"
        """,
            )
        }
        rows = _rows(con, f'SELECT * FROM _repair_write ORDER BY "{key}"')
        receipt = {
            "format": "spicy-regs-comment-repair",
            "version": 1,
            "mode": "apply" if apply else "dry-run",
            "source": dict(source_pins) if source_pins is not None else None,
            "catalog_snapshot": asdict(snapshot),
            "identities": [row[0] for row in con.execute(f'SELECT "{key}" FROM _repair_fresh ORDER BY 1').fetchall()],
            "missing_identities": [
                row[0]
                for row in con.execute(f"""
                SELECT "{key}" FROM _repair_fresh EXCEPT SELECT "{key}" FROM _repair_prior ORDER BY 1
            """).fetchall()
            ],
            "changes": [
                {
                    key: row[key],
                    "cells": {
                        column: {"before": before[row[key]][column], "after": row[column]}
                        for column in columns
                        if before[row[key]][column] != row[column]
                    },
                }
                for row in rows
            ],
            "rows": rows,
            "applied_snapshot": None,
        }
        path = output_dir / COMMENT_RECEIPT
        path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
        if not apply:
            return receipt
        if receipt["missing_identities"]:
            raise ValueError(
                f"comment repair corrects existing rows; {len(receipt['missing_identities'])} staged "
                "identities are not in the catalog"
            )
        if iceberg._read_snapshot(con, record_type) != snapshot:
            raise RuntimeError("the comments catalog moved since the repair read it; rerun the repair")
        if rows:
            iceberg.replace_rows(con, record_type, "_repair_write", expected_prior="_repair_prior")
            written_table = processing_table(con, record_type, where=f'"{key}" IN (SELECT "{key}" FROM _repair_write)')
            same = " AND ".join(f't."{column}" IS NOT DISTINCT FROM w."{column}"' for column in columns)
            found, matching = con.execute(f"""
                SELECT count(*), count(*) FILTER (WHERE {same})
                FROM {written_table} t JOIN _repair_write w ON t."{key}" = w."{key}"
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


def _normalize_expect_prior(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    digest = cleaned.removeprefix("sha256:")
    if not _EXPECT_PRIOR_RE.fullmatch(digest):
        raise ValueError(f"expect_prior must be 8-64 lowercase hex chars (optional sha256: prefix), got {value!r}")
    return digest


def repair_docket_rins(
    output_dir: Path,
    *,
    publish: bool = False,
    public_url: str | None = None,
    inputs=None,
    expect_prior: str | None = None,
) -> dict:
    """Normalize placeholder docket ``rin`` values on the selected ``dockets`` table.

    ``dockets`` has retired receipts (``receipts_retired: true``), so ``run-rollup-dockets``
    raises ``Unchanged``. This repair reads the selected ``dockets.parquet``, normalizes
    ``rin`` with :func:`spicy_docs.schemas.regulations._docket_rin` (setting ``'Not Assigned'``,
    ``'0000-AA00'``, and blank/whitespace values to ``NULL`` while preserving every valid and
    historical RIN and all 6 other columns verbatim), verifies row count, schema parity, and
    non-``rin`` column identity, seals the 7-column ``regulations-native-v1`` generation via
    :func:`finish_dataset`, and publishes only when ``publish=True`` and rows changed.
    """
    from spicy_regs.pipelines.regulatory_publication import finish_dataset, restore_dataset
    from spicy_regs.selected_generations import SelectedInputs, unique_build_directory
    from spicy_regs.sources import publication
    from spicy_regs.transforms.regulations_receipts import policy

    output_dir.mkdir(parents=True, exist_ok=True)
    expected_prior = _normalize_expect_prior(expect_prior)
    selected = (
        inputs
        if inputs is not None
        else SelectedInputs(output_dir, unique_build_directory(output_dir), public_url=public_url)
    )
    if publish and selected.index is None:
        raise ValueError("Publishing dockets RIN repair requires a captured remote publication index")

    family = "dockets"
    prior_digest: str | None = None
    if selected.index is not None:
        owner = publication.table_owner(selected.index, "dockets.parquet")
        family = owner[0] if owner else "dockets"
        prior_entry = selected.index.get("families", {}).get(family)
        if isinstance(prior_entry, Mapping):
            digest_value = prior_entry.get("artifactDigest")
            prior_digest = digest_value if isinstance(digest_value, str) else None

    if expected_prior is not None:
        if prior_digest is None:
            raise ValueError(f"expect_prior {expected_prior!r} requires a live remote index entry for {family!r}")
        if not prior_digest.removeprefix("sha256:").startswith(expected_prior):
            raise ValueError(f"Live {family} digest is {prior_digest}, not the expected {expected_prior}")

    declared = policy("dockets")
    expected_columns = list(declared.subject_schema.names)
    total_rows = 0
    non_null_rin_before = 0
    non_null_rin_after = 0
    changed_rows = 0
    nullified_rows = 0
    trimmed_rows = 0
    unchanged_non_null_rows = 0
    changed_counts: Counter[tuple[str | None, str | None]] = Counter()

    with TemporaryDirectory(prefix=".docket-rin-repair-", dir=output_dir) as directory:
        staging = Path(directory)
        prior_path = staging / "prior.parquet"
        repaired_path = staging / "dockets.parquet"
        if not restore_dataset(output_dir, "dockets", prior_path, inputs=selected):
            raise RuntimeError("dockets: no selected dockets generation to repair")

        with pq.ParquetFile(prior_path) as parquet_file:
            source_schema = parquet_file.schema_arrow
            if source_schema.names != expected_columns:
                raise ValueError(
                    f"dockets schema mismatch: expected {expected_columns}, got {source_schema.names}"
                )
            rin_index = source_schema.get_field_index("rin")
            rin_field = source_schema.field(rin_index)
            with pq.ParquetWriter(repaired_path, source_schema, compression="zstd") as writer:
                for batch in parquet_file.iter_batches(batch_size=50_000):
                    raw_rins = batch.column(rin_index).to_pylist()
                    normalized_rins: list[str | None] = []
                    for value in raw_rins:
                        total_rows += 1
                        if value is not None:
                            non_null_rin_before += 1
                        normalized = _docket_rin(value)
                        normalized_rins.append(normalized)
                        if normalized is not None:
                            non_null_rin_after += 1
                        if normalized != value:
                            changed_rows += 1
                            changed_counts[(value, normalized)] += 1
                            if value is not None and normalized is None:
                                nullified_rows += 1
                            elif value is not None and normalized is not None:
                                trimmed_rows += 1
                        elif value is not None:
                            unchanged_non_null_rows += 1
                    updated = batch.set_column(
                        rin_index, rin_field, pa.array(normalized_rins, type=rin_field.type)
                    )
                    writer.write_batch(updated)

        with pq.ParquetFile(repaired_path) as repaired_file:
            if repaired_file.metadata.num_rows != total_rows:
                raise RuntimeError(
                    f"dockets RIN repair row count changed: expected {total_rows}, got {repaired_file.metadata.num_rows}"
                )
            if repaired_file.schema_arrow.names != expected_columns:
                raise RuntimeError("dockets RIN repair altered table columns")
            for batch in repaired_file.iter_batches(batch_size=50_000, columns=["rin"]):
                for value in batch.column(0).to_pylist():
                    if value is not None and _docket_rin(value) != value:
                        raise RuntimeError(f"dockets RIN repair left non-normalized rin value: {value!r}")

        non_rin = ", ".join(f'"{column}"' for column in expected_columns if column != "rin")
        con = duckdb.connect()
        try:
            diff_rows = con.execute(
                f"""SELECT
                    (SELECT count(*) FROM (
                        SELECT {non_rin} FROM read_parquet(?) EXCEPT ALL SELECT {non_rin} FROM read_parquet(?)
                    ))
                    + (SELECT count(*) FROM (
                        SELECT {non_rin} FROM read_parquet(?) EXCEPT ALL SELECT {non_rin} FROM read_parquet(?)
                    ))""",
                [str(prior_path), str(repaired_path), str(repaired_path), str(prior_path)],
            ).fetchone()
            remaining_placeholders = con.execute(
                """SELECT count(*) FROM read_parquet(?)
                   WHERE rin IN ('Not Assigned', '0000-AA00')
                      OR (rin IS NOT NULL AND trim(rin, ' ' || chr(9) || chr(10) || chr(11) || chr(12) || chr(13)) = '')""",
                [str(repaired_path)],
            ).fetchone()
        finally:
            con.close()
        if diff_rows is None or diff_rows[0] != 0:
            raise RuntimeError(f"dockets RIN repair modified {diff_rows[0] if diff_rows else 'unknown'} non-rin rows")
        if remaining_placeholders is None or remaining_placeholders[0] != 0:
            raise RuntimeError(
                f"dockets RIN repair left {remaining_placeholders[0] if remaining_placeholders else 'unknown'} placeholder RINs"
            )

        publish_effective = publish and changed_rows > 0
        finish_dataset(output_dir, "dockets", repaired_path, publish=publish_effective, inputs=selected)

    final_path = output_dir / "dockets.parquet"
    if not pq.read_schema(final_path).equals(declared.subject_schema):
        raise RuntimeError("Sealed dockets.parquet schema does not match regulations-native-v1")
    if pq.ParquetFile(final_path).metadata.num_rows != total_rows:
        raise RuntimeError("Sealed dockets.parquet row count does not match input")

    published_digest: str | None = None
    if publish_effective and selected.index is not None:
        published_entry = selected.index.get("families", {}).get(family)
        if isinstance(published_entry, Mapping):
            digest_value = published_entry.get("artifactDigest")
            published_digest = digest_value if isinstance(digest_value, str) else None

    receipt = {
        "format": "spicy-regs-dockets-rin-repair",
        "version": 1,
        "table": "dockets",
        "family": family,
        "policy_version": declared.policy_version,
        "mode": "publish" if publish_effective else "dry-run",
        "requested_publish": publish,
        "published": publish_effective,
        "skipped_reason": "already_clean" if publish and changed_rows == 0 else None,
        "prior_digest": prior_digest,
        "published_digest": published_digest,
        "columns": expected_columns,
        "total_rows": total_rows,
        "non_null_rin_before": non_null_rin_before,
        "non_null_rin_after": non_null_rin_after,
        "changed_rows": changed_rows,
        "nullified_rows": nullified_rows,
        "trimmed_rows": trimmed_rows,
        "unchanged_non_null_rows": unchanged_non_null_rows,
        "remaining_placeholder_rins": 0,
        "non_rin_columns_unchanged": True,
        "changed_values_summary": [
            {"before": before, "after": after, "rows": count}
            for (before, after), count in sorted(
                changed_counts.items(), key=lambda item: (-item[1], item[0][0] or "", item[0][1] or "")
            )[:50]
        ],
        "output_path": str(final_path),
        "spicy_docs_version": version("spicy-docs"),
        "acquisition_manifest_changed": False,
        "repaired_at": datetime.now(UTC).isoformat(),
    }
    (output_dir / DOCKETS_RIN_RECEIPT).write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    return receipt


app = App(help=__doc__)


@app.command(name="repair-docket-rins")
def repair_docket_rins_command(
    *,
    output_dir: Path = Path("output"),
    skip_upload: bool = True,
    public_url: str | None = None,
    expect_prior: str | None = None,
) -> None:
    """Normalize placeholder RINs in the selected dockets table and optionally publish."""
    from dotenv import load_dotenv

    load_dotenv()
    result = repair_docket_rins(
        output_dir,
        publish=not skip_upload,
        public_url=public_url,
        expect_prior=expect_prior,
    )
    print(json.dumps(result, indent=2))


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

    Dockets and documents use the pinned local native selection and its receipts
    as the prior generation. No remote prior is downloaded. Comments: the catalog is the prior. A
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
        (row["record"] for row in reader.iter_records()),
        table=table,
        output_dir=output_dir,
        apply=apply,
        expected_snapshot=expected_snapshot,
        source_pins={
            **source_release,
            "accepted_verifier_implementation_id": accepted_verifier_implementation_id,
            "collection_outcome": dict(reader.collection_outcome),
        },
    )
    result["source_release"] = source_release
    result["collection_outcome"] = dict(reader.collection_outcome)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    app()
