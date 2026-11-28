"""Legislative-document tables without per-row receipts (ss-vse3): builder rows to public tables, and back.

The legislative builders write and merge all-VARCHAR rows in their spicy-docs contract shape; the public
table is those rows mapped by ``legislative_documents.map_subject``. A family whose installed policies are
marked ``receipts_retired`` maps its outputs here and writes no ``etl_receipts.parquet`` and no
``legislative_document_file_states`` receipts, while continuing to publish its read-outcome and
document-file-outcome tables cleanly.

- :func:`to_public` refuses the whole table on any row ``map_subject`` refuses or on any duplicate or null
  identity, and preserves custom Parquet KV metadata (such as ``spicy_regs.read_checkpoints.*``) on the
  published Parquet file.
- :func:`as_builder_input` reads published tables of :data:`READABLE` back into the builder's all-VARCHAR
  shape, preserving custom Parquet KV metadata (falling back on the first receipts-free night to
  ``source_metadata`` retained in the family's public ``*_document_file_outcomes`` table when the prior
  public table predated KV preservation). For retired ``receipt_only`` read-checkpoint datasets such as
  ``committee_report_reads``, it projects the builder's checkpoint columns from the published outcome table
  (``committee_report_read_outcomes``).
"""

from __future__ import annotations

import base64
import hashlib
import shutil
from collections.abc import Mapping, Sequence
from decimal import Decimal
from functools import cache
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from spicy_docs.schemas.tables import json_column

from spicy_regs.legislative_documents import (
    LegislativeShapeError,
    _key,
    field_registry,
    map_subject,
    subject_schema,
)
from spicy_regs.navigation_read_outcomes import (
    FILE_POLICIES,
    POLICIES as OUTCOME_POLICIES,
    READ_TABLES,
    checkpoint_rows,
)
from spicy_regs.transforms.committee_report_reads import READ_COLUMNS as COMMITTEE_REPORT_READ_COLUMNS

#: Retired legislative datasets whose published tables read back cleanly as builder inputs.
READABLE: frozenset[str] = frozenset({
    "laws",
    "law_code_sections",
    "table3_records",
    "law_sections",
    "senate_expenditures",
    "native_legal_references",
    "native_legal_reference_reads",
    "committee_reports",
    "report_sections",
    "hearing_transcripts",
    "hearing_bill_links",
    "committee_report_cbo_letters",
    "committee_report_reads",
})

#: Datasets whose pre-retirement live Parquet files lacked Parquet KV metadata because the receipt writer
#: recorded ``source_metadata`` in the family's public ``*_document_file_outcomes`` table instead.
FILE_TABLE_FOR_DATASET: Mapping[str, str] = {
    "table3_records": "laws_document_file_outcomes",
    "senate_expenditures": "senate_expenditures_document_file_outcomes",
}

# These builders seed their checkpoints from processing columns held only in receipt-era priors.
RECEIPT_CHECKPOINT_DATASETS = frozenset({"laws", "law_sections", "hearing_transcripts"})

#: Bounded batch size matching ``legislative_receipts`` (keeps wide text tables such as ``hearing_transcripts`` small).
_BATCH = 2_000


def _sha(path: Path) -> str:
    with path.open("rb") as stream:
        return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()


def _custom_metadata(path: Path) -> dict[bytes, bytes]:
    raw = pq.read_metadata(path).metadata or {}
    return {k: v for k, v in raw.items() if k != b"ARROW:schema"}


def has_checkpoint_metadata(path: Path) -> bool:
    """Whether ``path`` already carries ``spicy_regs.read_checkpoints.*`` Parquet KV metadata."""
    return any(key.startswith(b"spicy_regs.read_checkpoints.") for key in _custom_metadata(path))


def _file_outcome_metadata(dataset: str, file_outcomes: Sequence[Path]) -> dict[bytes, bytes]:
    """Recover non-Arrow custom Parquet KV metadata recorded in ``*_document_file_outcomes``."""
    recovered: dict[bytes, bytes] = {}
    for outcome_path in file_outcomes:
        with pq.ParquetFile(outcome_path) as reader:
            for batch in reader.iter_batches(columns=["source_dataset", "source_metadata"]):
                for row in batch.to_pylist():
                    if row.get("source_dataset") != dataset:
                        continue
                    for entry in row.get("source_metadata") or ():
                        key = base64.b64decode(entry["key_base64"])
                        if key == b"ARROW:schema":
                            continue
                        recovered[key] = base64.b64decode(entry["value_base64"])
    return recovered


def _source_rows(path: Path):
    # Do not infer Hive partition values: the stored source spelling is evidence.
    with pq.ParquetFile(path) as reader:
        for batch in reader.iter_batches(batch_size=_BATCH):
            yield from batch.to_pylist()


def _write_mapped_member(
    dataset: str,
    source: Path,
    destination: Path,
    *,
    schema: pa.Schema,
    identity_fields: tuple[str, ...],
    seen: set[tuple[Any, ...]],
    ordinal_start: int = 0,
) -> int:
    metadata = _custom_metadata(source)
    out_schema = schema.with_metadata(metadata) if metadata else schema
    destination.parent.mkdir(parents=True, exist_ok=True)
    staged = destination.with_name(f".{destination.name}.partial")
    ordinal = ordinal_start
    batch_rows: list[dict[str, Any]] = []
    try:
        with pq.ParquetWriter(staged, out_schema, compression="zstd") as writer:
            for raw in _source_rows(source):
                try:
                    mapped = map_subject(dataset, raw)
                except (LegislativeShapeError, ValueError, TypeError, OverflowError, pa.ArrowException) as error:
                    raise ValueError(
                        f"{dataset}: the mapping refuses source row {ordinal} ({type(error).__name__}: {error}); "
                        "nothing is published"
                    ) from None
                if mapped is None:
                    raise ValueError(f"{dataset}: source row {ordinal} produced no subject; nothing is published")
                identity = tuple(mapped[key] for key in identity_fields)
                if any(value is None or value == "" for value in identity):
                    raise ValueError(
                        f"{dataset}: source row {ordinal} has an empty identity {', '.join(identity_fields)}; "
                        "nothing is published"
                    )
                if identity in seen:
                    raise ValueError(
                        f"{dataset}: duplicate identity at source row {ordinal}; nothing is published"
                    )
                seen.add(identity)
                batch_rows.append(mapped)
                ordinal += 1
                if len(batch_rows) >= _BATCH:
                    writer.write_table(pa.Table.from_pylist(batch_rows, schema=out_schema))
                    batch_rows.clear()
            if batch_rows:
                writer.write_table(pa.Table.from_pylist(batch_rows, schema=out_schema))
    except BaseException:
        staged.unlink(missing_ok=True)
        raise
    staged.replace(destination)
    return ordinal


def to_public(dataset: str, source: Path, destination: Path) -> list[str]:
    """Write ``dataset``'s public Parquet table (or partitioned directory) from ``source``, preserving KV metadata.

    Enforces ``legislative_documents.map_subject`` and non-null unique identity across every member.
    Returns the list of relative member paths matching the ``stage`` layout expected by
    :func:`write_file_outcomes`.
    """
    spec = field_registry()[dataset]
    if spec["processing_only"]:
        raise ValueError(f"{dataset}: processing-only dataset has no public subject table")
    schema = subject_schema(dataset)
    identity_fields = tuple(spec["identity_fields"])
    seen: set[tuple[Any, ...]] = set()
    if source.is_dir():
        members = sorted(source.rglob("*.parquet"))
        staged_dir = destination.with_name(f".{destination.name}.partial")
        shutil.rmtree(staged_dir, ignore_errors=True)
        staged_dir.mkdir(parents=True)
        written: list[str] = []
        ordinal = 0
        try:
            for member in members:
                relative = member.relative_to(source)
                ordinal = _write_mapped_member(
                    dataset,
                    member,
                    staged_dir / relative,
                    schema=schema,
                    identity_fields=identity_fields,
                    seen=seen,
                    ordinal_start=ordinal,
                )
                written.append(f"{dataset}/{relative.as_posix()}")
            if destination.exists():
                shutil.rmtree(destination)
            staged_dir.replace(destination)
        except BaseException:
            shutil.rmtree(staged_dir, ignore_errors=True)
            raise
        return written
    _write_mapped_member(
        dataset,
        source,
        destination,
        schema=schema,
        identity_fields=identity_fields,
        seen=seen,
    )
    return [destination.name]


def write_read_outcomes(dataset: str, source: Path, destination: Path, *, generation_id: str) -> Path:
    """Write the public read-outcome table for ``dataset`` (e.g. ``committee_report_read_outcomes``) without receipts."""
    table_name = READ_TABLES[dataset]
    outcome_policy = OUTCOME_POLICIES[table_name]
    schema = outcome_policy.subject_schema
    identity_fields = tuple(outcome_policy.identity_fields)
    destination.parent.mkdir(parents=True, exist_ok=True)
    staged = destination.with_name(f".{destination.name}.partial")
    seen: set[tuple[Any, ...]] = set()
    batch_rows: list[dict[str, Any]] = []
    ordinal = 0
    try:
        with pq.ParquetWriter(staged, schema, compression="zstd") as writer:
            for _, row, _ in checkpoint_rows(dataset, source, generation_id=generation_id):
                identity = tuple(row.get(key) for key in identity_fields)
                if any(value is None or value == "" for value in identity):
                    raise ValueError(
                        f"{table_name}: row {ordinal} lacks identity {', '.join(identity_fields)}"
                    )
                if identity in seen:
                    raise ValueError(f"{table_name}: duplicate identity at row {ordinal}")
                seen.add(identity)
                batch_rows.append(row)
                ordinal += 1
                if len(batch_rows) >= _BATCH:
                    writer.write_table(pa.Table.from_pylist(batch_rows, schema=schema))
                    batch_rows.clear()
            if batch_rows:
                writer.write_table(pa.Table.from_pylist(batch_rows, schema=schema))
    except BaseException:
        staged.unlink(missing_ok=True)
        raise
    staged.replace(destination)
    return destination


def write_file_outcomes(
    table_name: str,
    source_outputs: Sequence[tuple[str, Path]],
    mapped_subjects: Mapping[str, Sequence[str]],
    stage: Path,
    destination: Path,
    *,
    generation_id: str,
) -> Path:
    """Write ``*_document_file_outcomes.parquet`` directly from producer outputs and mapped native tables."""
    from spicy_regs.etl_receipts import exact_json

    outcome_policy = FILE_POLICIES[table_name]
    schema = outcome_policy.subject_schema
    rows: list[dict[str, Any]] = []
    accounted: dict[str, set[str]] = {}
    for dataset, output in source_outputs:
        members = sorted(output.rglob("*.parquet")) if output.is_dir() else [output]
        subject_members = list(mapped_subjects.get(dataset, ()))
        if output.is_dir() and not members:
            if subject_members:
                raise ValueError(f"{table_name}: empty state has an invented physical member for {dataset}")
            state = {"dataset": dataset, "relative_path": None, "partitioned": True, "rows": 0}
            state_digest = "sha256:" + hashlib.sha256(exact_json(state).encode()).hexdigest()
            rows.append({
                "generation_id": generation_id,
                "source_dataset": dataset,
                "member_ordinal": 0,
                "relative_path": None,
                "partitioned": True,
                "row_count": 0,
                "state": "successful-empty-partition",
                "original_byte_sha256": None,
                "state_witness_sha256": state_digest,
                "source_schema": None,
                "source_metadata": None,
                "native_members": [],
            })
            continue
        native_dataset = READ_TABLES.get(dataset, dataset)
        mapped_members = list(mapped_subjects.get(native_dataset, subject_members))
        if not mapped_members and (native_dataset != dataset or not field_registry()[dataset]["processing_only"]):
            raise ValueError(f"{table_name}: missing native member mapping for {dataset}")
        for ordinal, member in enumerate(members):
            relative = member.relative_to(output).as_posix() if output.is_dir() else member.name
            if Path(relative).is_absolute() or ".." in Path(relative).parts:
                raise ValueError(f"{table_name}: producer member mapping requires a relative path: {relative}")
            expected = f"{native_dataset}/{relative}" if output.is_dir() else f"{native_dataset}.parquet"
            if mapped_members and (mapped_members.count(expected) != 1 or ordinal >= len(mapped_members)):
                raise ValueError(f"{table_name}: missing or duplicate native member mapping for {expected}")
            with pq.ParquetFile(member) as source:
                row_count = source.metadata.num_rows
                source_schema = [
                    {"name": f.name, "type": str(f.type), "nullable": f.nullable}
                    for f in source.schema_arrow
                ]
                source_metadata = [
                    {
                        "key_base64": base64.b64encode(k).decode(),
                        "value_base64": base64.b64encode(v).decode(),
                    }
                    for k, v in (source.metadata.metadata or {}).items()
                ]
            native_path = stage / expected
            if mapped_members:
                if pq.read_metadata(native_path).num_rows != row_count:
                    raise ValueError(f"{table_name}: native member row count differs from original for {expected}")
                matched = [{
                    "relative_path": expected,
                    "sha256": _sha(native_path),
                    "byte_size": native_path.stat().st_size,
                    "row_count": row_count,
                }]
                accounted.setdefault(native_dataset, set()).add(expected)
            else:
                matched = []
            rows.append({
                "generation_id": generation_id,
                "source_dataset": dataset,
                "member_ordinal": ordinal,
                "relative_path": relative,
                "partitioned": output.is_dir(),
                "row_count": row_count,
                "state": "converted-member" if matched else "retained-processing-member",
                "original_byte_sha256": _sha(member),
                "state_witness_sha256": None,
                "source_schema": source_schema,
                "source_metadata": source_metadata,
                "native_members": matched,
            })
    for native_dataset, members_set in accounted.items():
        expected_list = list(mapped_subjects.get(native_dataset, ()))
        if members_set != set(expected_list) or len(members_set) != len(expected_list):
            raise ValueError(f"{table_name}: unmatched or duplicate native member mapping for {native_dataset}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staged = destination.with_name(f".{destination.name}.partial")
    try:
        table = pa.Table.from_pylist(rows, schema=schema) if rows else schema.empty_table()
        pq.write_table(table, staged, compression="zstd")
    except BaseException:
        staged.unlink(missing_ok=True)
        raise
    staged.replace(destination)
    return destination


@cache
def _hearing_link_rule_names() -> tuple[str, ...]:
    from spicy_docs.interpretation.hearing_bill_links import HEARING_BILL_LINK_RULES

    return tuple(rule.name for rule in HEARING_BILL_LINK_RULES)


def _invert_link_source(package_id: Any, bill_id: Any, link_id: Any) -> str | None:
    if link_id is None:
        return None
    if isinstance(package_id, str) and package_id and isinstance(bill_id, str) and bill_id:
        for rule_name in _hearing_link_rule_names():
            if _key("hearing-link", [package_id, bill_id, rule_name]) == link_id:
                return rule_name
    if isinstance(link_id, str):
        return link_id
    raise ValueError(f"hearing_bill_links.link_id: unexpected {type(link_id).__name__}")


def _strip_candidate_ordinals(candidates: Any) -> Any:
    if candidates is None:
        return None
    cleaned = []
    for candidate in candidates:
        if candidate is None:
            cleaned.append(None)
            continue
        item = {k: v for k, v in candidate.items() if k != "candidate_ordinal"}
        if item.get("candidate_keys") is not None:
            item["candidate_keys"] = [
                None if inner is None else {k: v for k, v in inner.items() if k != "target_key_ordinal"}
                for inner in item["candidate_keys"]
            ]
        cleaned.append(item)
    return cleaned


def _format_amounts(amounts: Any) -> Any:
    if amounts is None:
        return None
    return [
        None
        if cell is None
        else [
            None
            if entry is None
            else {
                "amount": format(entry["amount"], "f")
                if isinstance(entry.get("amount"), Decimal)
                else entry.get("amount"),
                "text": entry.get("text"),
            }
            for entry in cell
        ]
        for cell in amounts
    ]


@cache
def _derived_sources(dataset: str) -> dict[str, tuple[tuple[str, str], ...]]:
    spec = field_registry()[dataset]
    grouped: dict[str, list[tuple[str, str]]] = {}
    for target_col, info in spec.get("derived_fields", {}).items():
        if "property" in info:
            grouped.setdefault(info["from"], []).append((info["property"], target_col))
    return {source_col: tuple(pairs) for source_col, pairs in grouped.items()}


def _source_row(dataset: str, row: Mapping[str, Any]) -> dict[str, str | None]:
    spec = field_registry()[dataset]
    derived_by_source = _derived_sources(dataset)
    result: dict[str, str | None] = {}
    for field in spec["fields"]:
        name = field["name"]
        target = field["target"]
        decision = field["decision"]
        if target is None:
            if pairs := derived_by_source.get(name):
                synthesized = {prop: row[col] for prop, col in pairs if row.get(col) is not None}
                result[name] = json_column(synthesized) if synthesized else None
            else:
                result[name] = None
            continue
        value = row.get(target)
        if value is None:
            result[name] = None
            continue
        if decision == "keep":
            if not isinstance(value, str):
                raise ValueError(f"{dataset}.{name}: unexpected {type(value).__name__} in text column")
            result[name] = value
        elif decision == "flatten":
            if type(value) is bool:
                result[name] = "true" if value else "false"
            elif type(value) is int:
                result[name] = str(value)
            elif isinstance(value, str):
                result[name] = value
            else:
                raise ValueError(f"{dataset}.{name}: unexpected {type(value).__name__} in flattened column")
        elif decision == "native-list":
            if name in {"match_path", "billstatus_action_code"}:
                result[name] = "\x1f".join(value)
            elif name == "target_candidates_json":
                result[name] = json_column(_strip_candidate_ordinals(value))
            elif name == "amounts_json":
                result[name] = json_column(_format_amounts(value))
            else:
                result[name] = json_column(value)
        elif decision == "stable-key":
            if name in {"text_sha256", "input_sha256"}:
                if not isinstance(value, str):
                    raise ValueError(f"{dataset}.{name}: unexpected {type(value).__name__} in body_version_id")
                result[name] = value.removeprefix("body:")
            elif name == "link_source":
                result[name] = _invert_link_source(row.get("package_id"), row.get("bill_id"), value)
            else:
                raise ValueError(f"{dataset}.{name}: no published-table inverse declared for stable-key")
        else:
            raise ValueError(f"{dataset}.{name}: unsupported decision {decision!r}")
    return result


def _as_committee_report_reads(tables: Sequence[Path], destination: Path) -> Path:
    schema = pa.schema([(name, pa.string()) for name in COMMITTEE_REPORT_READ_COLUMNS])
    destination.parent.mkdir(parents=True, exist_ok=True)
    staged = destination.with_name(f".{destination.name}.partial")
    seen: set[str] = set()
    try:
        with pq.ParquetWriter(staged, schema, compression="zstd") as writer:
            for table_path in tables:
                with pq.ParquetFile(table_path) as reader:
                    missing = sorted(set(COMMITTEE_REPORT_READ_COLUMNS) - set(reader.schema_arrow.names))
                    if missing:
                        raise ValueError(f"committee_report_read_outcomes lacks required columns: {', '.join(missing)}")
                    for batch in reader.iter_batches(batch_size=_BATCH, columns=list(COMMITTEE_REPORT_READ_COLUMNS)):
                        rows = batch.to_pylist()
                        for row in rows:
                            package_id = row.get("package_id")
                            if not isinstance(package_id, str) or not package_id:
                                raise ValueError("committee_report_read_outcomes row lacks package_id")
                            if package_id in seen:
                                raise ValueError(f"committee_report_read_outcomes repeats package_id {package_id}")
                            seen.add(package_id)
                        writer.write_table(pa.Table.from_pylist(rows, schema=schema))
    except BaseException:
        staged.unlink(missing_ok=True)
        raise
    staged.replace(destination)
    return destination


def as_builder_input(
    dataset: str,
    tables: Sequence[Path],
    destination: Path,
    *,
    file_outcomes: Sequence[Path] = (),
) -> Path:
    """Write ``dataset``'s published ``tables`` back into the builder's all-VARCHAR input shape.

    Preserves custom Parquet KV metadata from the published table, falling back to ``source_metadata`` in
    ``file_outcomes`` when the published table predates KV metadata preservation (first receipts-free run).
    """
    if dataset not in READABLE:
        raise ValueError(f"{dataset}: no reader of its published table as builder input (legislative_tables.READABLE)")
    if dataset == "committee_report_reads":
        return _as_committee_report_reads(tables, destination)
    spec = field_registry()[dataset]
    base_schema = pa.schema([(field["name"], pa.string()) for field in spec["fields"]])
    metadata = _custom_metadata(tables[0]) if tables else {}
    if not any(k.startswith(b"spicy_regs.read_checkpoints.") for k in metadata) and file_outcomes:
        metadata = {**_file_outcome_metadata(dataset, file_outcomes), **metadata}
    schema = base_schema.with_metadata(metadata) if metadata else base_schema
    destination.parent.mkdir(parents=True, exist_ok=True)
    staged = destination.with_name(f".{destination.name}.partial")
    try:
        with pq.ParquetWriter(staged, schema, compression="zstd") as writer:
            for table_path in tables:
                with pq.ParquetFile(table_path) as reader:
                    for batch in reader.iter_batches(batch_size=_BATCH):
                        rows = [_source_row(dataset, row) for row in batch.to_pylist()]
                        writer.write_table(pa.Table.from_pylist(rows, schema=schema))
    except BaseException:
        staged.unlink(missing_ok=True)
        raise
    staged.replace(destination)
    return destination
