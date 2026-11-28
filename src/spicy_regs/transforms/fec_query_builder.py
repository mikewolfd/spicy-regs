"""Receipts-free streaming 50-table ``fec-query`` builder (ss-42zk / ss-42nr).

Replaces ``fec_native_conversion.py``'s obsolete receipt-copy path with a
source-derived, table-checkpointed streaming orchestrator aligned with
post-``e5e94c8c7`` receipts retirement and ``fec-typed-observation/2``:

- Emits ALL 50 tables declared in ``data_dictionary/fec_typed_schemas.json``
  (verified schema-identical to ``fec_processing_schemas.json`` with 0 diffs),
  preserving exact ``DECIMAL(38,9)`` precision and every distinct source
  occurrence.
- Wires every repaired ``ss-u9s8`` mapper (cross-source registration dedupe,
  official archive-member header upgrades, filing-link resolution, oppexp
  trailing empty pipes, TEXT resolved-body status, rollup pseudo-candidate
  separation, quality-notice candidate typing, agency SafeLinks unwrapping &
  Word table rows, legal AF/audit party typing, non-null date events, strict
  ``api_key=<redacted>`` MUR qualification) plus ``bulk_receipt``,
  ``map_bulk_financial``, ``map_api_financial``, ``map_filing_cover``,
  ``map_filing_physical_text``, committee/candidate API & history mappers,
  ``fec_filing_definitions``, and the 4 ``REBUILT_TABLES`` via
  ``fec_original_tables.rebuild()``.
- Streams rows in bounded 4,096-row Arrow batches with table-level
  checkpoint/resume, ``source_namespace`` partitioning for large financial
  tables (``part_rows=1_048_576``, ``row_group_rows=32_768``), single-connection
  ``association_records`` per table/partition, streaming ``id_mapping.parquet``
  (``table, old_record_id, new_record_id``) for ``ss-42nr`` bidirectional parity,
  DuckDB ``memory_limit='2GiB'`` / ``max_temp_directory_size='2GiB'``, full
  ``refuse_c1_controls()`` scan, and zero-copy ``build_generation(...,
  receipt_path=None, owned_output_root=build)`` sealing.
- Keeps ``fec_repaired_builder.py`` untouched as the bounded parity oracle.
"""

from __future__ import annotations

import base64
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import ExitStack
import csv
from dataclasses import asdict, dataclass, field
import hashlib
import io
import itertools
import json
from pathlib import Path
import re
import resource
import shutil
import sys
from tempfile import TemporaryDirectory
from time import monotonic
from typing import Any
from urllib.parse import parse_qsl, urlsplit

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.dictionary_resources import INPUTS as DICTIONARY_INPUTS
from spicy_regs.fec_financial_rules import IDENTITY_VERSION, LEGACY_IDENTITY_VERSION
from spicy_regs.fec_receipt_adapter import processing_declarations
from spicy_regs.generations import admit_generation_for_publication, build_generation
from spicy_regs.transforms import fec_original_tables as original_tables
from spicy_regs.transforms.assemble_fec_query import _sha
from spicy_regs.transforms.fec_agency import (
    SCHEMAS as AGENCY_SCHEMAS,
    AgencySelection,
    map_agency_collection,
)
from spicy_regs.transforms.fec_api_financial import (
    ALLOCATED_DISBURSEMENTS as API_ALLOCATED_DISBURSEMENTS,
    COORDINATED_EXPENDITURES as API_COORDINATED_EXPENDITURES,
    DEBTS as API_DEBTS,
    INAUGURAL_AGGREGATES as API_INAUGURAL_AGGREGATES,
    LOANS as API_LOANS,
    NATIONAL_PARTY_RECEIPTS as API_NATIONAL_PARTY_RECEIPTS,
    ApiMapping,
    map_api_financial,
)
from spicy_regs.transforms.fec_bulk_financial import (
    CANDIDATE_TRANSACTIONS,
    COMMUNICATION_COSTS,
    ELECTIONEERING,
    INDEPENDENT_EXPENDITURES,
    INTERCOMMITTEE,
    BulkMapping,
    map_bulk_financial,
    operating_expense,
)
from spicy_regs.transforms.fec_bulk_selection import (
    build_archive_inner_member_proof,
    build_foia_dual_edition_proof,
    build_weball_webl_subset_proof,
    cross_source_collection_selection,
    individual_snapshot_selection,
)
from spicy_regs.transforms.fec_candidate_observations import (
    map_candidate_api,
    prepare_candidate_api,
)
from spicy_regs.transforms.fec_committee_history_observations import (
    map_history,
    prepare_history,
)
from spicy_regs.transforms.fec_committee_observations import (
    map_committee_api,
    prepare_committee_api,
)
from spicy_regs.transforms.fec_filing_associations import (
    associate_filing_headers,
    association_records,
)
from spicy_regs.transforms.fec_filing_definitions import (
    bind_definition,
    definition_contexts_from_collections,
)
from spicy_regs.transforms.fec_filing_financial import (
    FilingMapping,
    load_layout,
    map_filing_financial,
)
from spicy_regs.transforms.fec_filing_forms import (
    filing_cover_mapping_for,
    filing_cover_mappings,
    filing_text_mapping_for,
    filing_text_mappings,
    map_filing_cover,
    map_filing_physical_text,
    map_filing_text,
    remaining_filing_mapping_for,
    remaining_filing_mappings,
)
from spicy_regs.transforms.fec_filing_schedules import (
    filing_mapping_for,
    retained_filing_mappings,
)
from spicy_regs.transforms.fec_identity_observations import (
    FILINGS,
    FORM1,
    FORM2,
    LINKS,
    LOBBYIST,
    QUALITY_NOTICE,
    RegistryMapping,
    map_filing_metadata,
    map_registry,
    resolve_filing_links,
)
from spicy_regs.transforms.fec_legal import legal_source_kind, map_legal, sanitize_redacted_mur_url
from spicy_regs.transforms.fec_native_subjects import FIELD_RULES, refuse_c1_controls
from spicy_regs.transforms.fec_query import (
    CollectionSelection,
    _digest,
    _json,
    bulk_receipt,
    legacy_observation_id,
)
from spicy_regs.transforms.fec_repaired_builder import qualify_retained_mur_documents
from spicy_regs.transforms.fec_summaries import (
    BUNDLING_RECIPIENT,
    CANDIDATE_CSV,
    CANDIDATE_WEB,
    COMMITTEE_CSV,
    COMMITTEE_WEB,
    LEADERSHIP,
    PRESIDENTIAL_OVERALL,
    SummaryMapping,
    map_contribution_aggregate,
    map_summary,
)
from spicy_regs.transforms.fec_typed_batch import typed_batch

FEC_TYPED_SCHEMA_PATH = DICTIONARY_INPUTS / "fec_typed_schemas.json"

DEFAULT_BATCH_ROWS = 4096
DEFAULT_ROW_GROUP_ROWS = 32_768
DEFAULT_PART_ROWS = 1_048_576
DEFAULT_DUCKDB_MEMORY_LIMIT = "2GiB"
DEFAULT_DUCKDB_TEMP_LIMIT = "2GiB"

RETIRED_FEC_QUERY_TABLES: frozenset[str] = frozenset(
    {
        "fec_research_document_observations",
        "fec_retained_csv_observations",
        "fec_research_response_outcomes",
        "fec_research_context_dispositions",
    }
)
REBUILT_FEC_QUERY_TABLES: tuple[str, ...] = original_tables.TABLES
# Evidence rows targeting any of the 4 retired tables or the 4 captured-rebuilt
# table names are excluded (matching fec_native_conversion._UNREFERENCED_EVIDENCE).
EXCLUDED_EVIDENCE_TARGETS: frozenset[str] = frozenset(
    RETIRED_FEC_QUERY_TABLES
    | {
        "fec_historical_ie_statistics",
        "fec_research_meeting_observations",
        "fec_research_filing_feed_items",
        "fec_research_source_pages",
        "fec_reference_filing_feed_items",
        "fec_reference_source_pages",
    }
)

# Large/schedule financial tables partitioned by ``source_namespace`` matching live 784b23da.
DEFAULT_PARTITIONED_TABLES: frozenset[str] = frozenset(
    {
        "fec_receipts",
        "fec_intercommittee_transactions",
        "fec_disbursements",
        "fec_inaugural_donations",
        "fec_account_transfers",
        "fec_allocated_disbursements",
        "fec_allocation_bases",
        "fec_bundled_contributions",
        "fec_coordinated_party_expenditures",
        "fec_debts",
        "fec_independent_expenditures",
        "fec_loan_guarantors",
        "fec_loan_terms",
        "fec_loans",
    }
)

ID_MAPPING_SCHEMA = pa.schema(
    [
        ("table", pa.string()),
        ("old_record_id", pa.string()),
        ("new_record_id", pa.string()),
    ]
)

_NAMESPACE_RE = re.compile(r"[a-z0-9][a-z0-9._-]*\Z")
_API_FINANCIAL_BY_ROUTE: dict[str, ApiMapping] = {
    mapping.route: mapping
    for mapping in (
        API_NATIONAL_PARTY_RECEIPTS,
        API_LOANS,
        API_DEBTS,
        API_ALLOCATED_DISBURSEMENTS,
        API_COORDINATED_EXPENDITURES,
        API_INAUGURAL_AGGREGATES,
    )
}


def load_fec_typed_schemas(*, verify_processing_parity: bool = True) -> dict[str, pa.Schema]:
    """Load the authoritative 50-table Arrow schemas from ``fec_typed_schemas.json``."""
    raw = json.loads(FEC_TYPED_SCHEMA_PATH.read_text(encoding="utf-8"))
    tables = raw["tables"]
    schemas = {
        name: pa.ipc.read_schema(pa.BufferReader(base64.b64decode(info["arrow_schema_ipc_base64"])))
        for name, info in sorted(tables.items())
    }
    if verify_processing_parity:
        proc = processing_declarations()
        missing = sorted(set(schemas) - set(proc))
        if missing:
            raise ValueError(f"fec_processing_schemas.json is missing declared typed tables: {missing}")
        for name, typed_schema in schemas.items():
            proc_schema = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(proc[name]["arrow_schema"])))
            if not typed_schema.equals(proc_schema, check_metadata=False):
                raise ValueError(
                    f"Schema drift between fec_typed_schemas.json and fec_processing_schemas.json for {name}"
                )
    return schemas


def _schema_sha256(schema: pa.Schema) -> str:
    return "sha256:" + hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()


def _peak_rss_bytes() -> int:
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(usage if sys.platform == "darwin" else usage * 1024)


def _dir_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file() and not p.is_symlink())


def _hash_json(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def legacy_record_id_for_row(
    table: str,
    source_row: Mapping[str, Any],
    authority: str,
    *,
    typed_subrecord_pointer: str | None = None,
    agency_subrecord_pointer: str | None = None,
) -> str:
    """Compute the legacy ``fec-typed-observation/1`` record_id for old->new parity mapping."""
    addressed: Mapping[str, Any] = source_row
    if typed_subrecord_pointer:
        locator = json.loads(source_row["source_locator_json"])
        locator["typed_subrecord_pointer"] = typed_subrecord_pointer
        addressed = {**source_row, "source_locator_json": _json(locator)}
    rid = legacy_observation_id(table, addressed, authority)
    if agency_subrecord_pointer is not None:
        return _hash_json([LEGACY_IDENTITY_VERSION, rid, agency_subrecord_pointer])
    return rid


class IdMappingWriter:
    """Bounded streaming Parquet writer for ``(table, old_record_id, new_record_id)`` proofs."""

    def __init__(self, path: Path, *, batch_size: int = DEFAULT_BATCH_ROWS):
        self.path = Path(path)
        self.batch_size = batch_size
        self._buffer: list[dict[str, str]] = []
        self._writer: pq.ParquetWriter | None = None
        self.count = 0

    def __enter__(self) -> IdMappingWriter:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._writer = pq.ParquetWriter(self.path, ID_MAPPING_SCHEMA, compression="zstd")
        return self

    def record(self, table: str, old_record_id: str | None, new_record_id: str | None) -> None:
        if old_record_id is None or new_record_id is None:
            return
        self._buffer.append({"table": table, "old_record_id": old_record_id, "new_record_id": new_record_id})
        if len(self._buffer) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if self._writer is None:
            raise RuntimeError("IdMappingWriter must be entered before flushing")
        if self._buffer:
            self._writer.write_batch(typed_batch(self._buffer, ID_MAPPING_SCHEMA))
            self.count += len(self._buffer)
            self._buffer.clear()

    def __exit__(self, exc_type, exc, tb) -> bool:
        try:
            if exc_type is None and self._writer is not None:
                self.flush()
        finally:
            if self._writer is not None:
                self._writer.close()
                self._writer = None
        return False


@dataclass(frozen=True)
class TableCheckpoint:
    """Verified on-disk checkpoint metadata for one completed ``fec-query`` table."""

    table: str
    rows: int
    partitioned: bool
    output_path: Path
    members: tuple[dict[str, Any], ...]
    schema_sha256: str
    source_generation_pin: str
    identity_version: str = IDENTITY_VERSION


def _checkpoint_path(checkpoints_dir: Path, table: str) -> Path:
    return checkpoints_dir / f"{table}.checkpoint.json"


def load_verified_checkpoint(
    *,
    table: str,
    tables_dir: Path,
    checkpoints_dir: Path,
    schema: pa.Schema,
    source_generation_pin: str,
) -> TableCheckpoint | None:
    """Return a verified checkpoint if every output member matches size, row count, and SHA-256."""
    meta_path = _checkpoint_path(checkpoints_dir, table)
    if not meta_path.is_file():
        return None
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    if (
        meta.get("table") != table
        or meta.get("source_generation_pin") != source_generation_pin
        or meta.get("identity_version") != IDENTITY_VERSION
        or meta.get("schema_sha256") != _schema_sha256(schema)
    ):
        return None
    partitioned = bool(meta.get("partitioned"))
    output_path = (tables_dir / table) if partitioned else (tables_dir / f"{table}.parquet")
    if partitioned and not output_path.is_dir():
        return None
    if not partitioned and not output_path.is_file():
        return None
    members = meta.get("members")
    if not isinstance(members, list) or not members:
        return None
    total_rows = 0
    for member in members:
        rel = member.get("path")
        if not isinstance(rel, str) or ".." in Path(rel).parts:
            return None
        file_path = (output_path / rel) if partitioned else output_path
        if file_path.is_symlink() or not file_path.is_file():
            return None
        if file_path.stat().st_size != member.get("bytes"):
            return None
        with pq.ParquetFile(file_path) as pf:
            if pf.metadata.num_rows != member.get("rows"):
                return None
            if not pf.schema_arrow.equals(schema, check_metadata=False):
                return None
        if _sha(file_path) != member.get("sha256"):
            return None
        total_rows += int(member["rows"])
    if total_rows != meta.get("rows"):
        return None
    return TableCheckpoint(
        table=table,
        rows=total_rows,
        partitioned=partitioned,
        output_path=output_path,
        members=tuple(members),
        schema_sha256=meta["schema_sha256"],
        source_generation_pin=source_generation_pin,
        identity_version=IDENTITY_VERSION,
    )


class StreamingTableWriter:
    """Bounded Arrow batch writer supporting flat and ``source_namespace``-partitioned Parquet tables."""

    def __init__(
        self,
        *,
        table: str,
        schema: pa.Schema,
        tables_dir: Path,
        checkpoints_dir: Path,
        source_generation_pin: str,
        partitioned: bool = False,
        batch_rows: int = DEFAULT_BATCH_ROWS,
        row_group_rows: int = DEFAULT_ROW_GROUP_ROWS,
        part_rows: int = DEFAULT_PART_ROWS,
        check_resources: Callable[[], None] = lambda: None,
    ):
        if batch_rows < 1 or row_group_rows < 1 or part_rows < 1:
            raise ValueError("Batch, row-group, and part sizes must be positive integers")
        if partitioned and "source_namespace" not in schema.names:
            raise ValueError(f"{table}: source_namespace partition requested on schema without source_namespace")
        self.table = table
        self.schema = schema
        self.tables_dir = Path(tables_dir)
        self.checkpoints_dir = Path(checkpoints_dir)
        self.source_generation_pin = _digest(source_generation_pin)
        self.partitioned = partitioned
        self.batch_rows = batch_rows
        self.row_group_rows = row_group_rows
        self.part_rows = part_rows
        self.check_resources = check_resources

        self._staging = self.tables_dir / f".{table}.partial"
        self._buffers: dict[str | None, list[dict[str, Any]]] = defaultdict(list)
        self._writers: dict[str | None, tuple[pq.ParquetWriter, Path, int, int]] = {}
        self._written_files: list[Path] = []
        self._row_count = 0
        self._closed = False

    def __enter__(self) -> StreamingTableWriter:
        self.tables_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoints_dir.mkdir(parents=True, exist_ok=True)
        if self._staging.exists():
            if self._staging.is_dir():
                shutil.rmtree(self._staging)
            else:
                self._staging.unlink()
        if self.partitioned:
            self._staging.mkdir(parents=True)
        return self

    def _align_row(self, row: Mapping[str, Any]) -> dict[str, Any]:
        # Reject unknown fields (typed_batch checks too), pad absent union fields with None.
        return {name: row.get(name) for name in self.schema.names}

    def append(self, row: Mapping[str, Any]) -> None:
        if self._closed:
            raise RuntimeError(f"Writer for {self.table} is already closed")
        unknown = set(row) - set(self.schema.names)
        if unknown:
            raise ValueError(f"{self.table}: row contains undeclared fields {sorted(unknown)}")
        aligned = self._align_row(row)
        ns: str | None = None
        if self.partitioned:
            raw_ns = aligned.get("source_namespace")
            if not isinstance(raw_ns, str) or not _NAMESPACE_RE.fullmatch(raw_ns):
                raise ValueError(f"{self.table}: partitioned row has invalid source_namespace {raw_ns!r}")
            ns = raw_ns
        buf = self._buffers[ns]
        buf.append(aligned)
        if len(buf) >= self.batch_rows:
            self._flush_namespace(ns)

    def _open_writer(self, ns: str | None, part_idx: int) -> tuple[pq.ParquetWriter, Path, int, int]:
        if self.partitioned:
            assert ns is not None
            ns_dir = self._staging / f"source_namespace={ns}"
            ns_dir.mkdir(parents=True, exist_ok=True)
            path = ns_dir / f"part-{part_idx:06d}.parquet"
        else:
            path = self._staging
        writer = pq.ParquetWriter(path, self.schema, compression="zstd")
        self._written_files.append(path)
        return (writer, path, part_idx, 0)

    def _flush_namespace(self, ns: str | None) -> None:
        rows = self._buffers.get(ns)
        if not rows:
            return
        self.check_resources()
        offset = 0
        total = len(rows)
        while offset < total:
            entry = self._writers.get(ns)
            if entry is None:
                entry = self._open_writer(ns, 0)
                self._writers[ns] = entry
            writer, path, part_idx, rows_in_part = entry
            if self.partitioned:
                room = self.part_rows - rows_in_part
                take = min(total - offset, room)
            else:
                take = total - offset
            chunk = rows[offset : offset + take]
            batch = typed_batch(chunk, self.schema)
            writer.write_table(pa.Table.from_batches([batch], schema=self.schema), row_group_size=self.row_group_rows)
            rows_in_part += take
            self._row_count += take
            offset += take
            if self.partitioned and rows_in_part >= self.part_rows:
                writer.close()
                self._writers[ns] = self._open_writer(ns, part_idx + 1)
            else:
                self._writers[ns] = (writer, path, part_idx, rows_in_part)
        rows.clear()

    def finish(self) -> TableCheckpoint:
        if self._closed:
            raise RuntimeError(f"Writer for {self.table} is already finalized")
        for ns in sorted(self._buffers, key=lambda x: "" if x is None else x):
            self._flush_namespace(ns)
        for writer, _path, _idx, _rows in self._writers.values():
            writer.close()
        self._writers.clear()
        self._closed = True

        # generations._copy_split refuses an empty partitioned directory, so 0-row
        # tables publish cleanly as flat 0-row Parquet files with the exact schema.
        actual_partitioned = self.partitioned and self._row_count > 0
        if not actual_partitioned:
            if self.partitioned and self._staging.is_dir():
                shutil.rmtree(self._staging)
            if not self._written_files or self.partitioned:
                self._written_files = [self._staging]
                pq.write_table(pa.Table.from_pylist([], schema=self.schema), self._staging, compression="zstd")

        final_path = (self.tables_dir / self.table) if actual_partitioned else (self.tables_dir / f"{self.table}.parquet")
        if final_path.exists():
            if final_path.is_dir():
                shutil.rmtree(final_path)
            else:
                final_path.unlink()
        self._staging.rename(final_path)

        final_members = sorted(final_path.rglob("*.parquet")) if actual_partitioned else [final_path]
        member_records: list[dict[str, Any]] = []
        for member_file in final_members:
            rel = member_file.relative_to(final_path).as_posix() if actual_partitioned else final_path.name
            member_records.append(
                {
                    "path": rel,
                    "sha256": _sha(member_file),
                    "bytes": member_file.stat().st_size,
                    "rows": pq.ParquetFile(member_file).metadata.num_rows,
                }
            )
        checkpoint = TableCheckpoint(
            table=self.table,
            rows=self._row_count,
            partitioned=actual_partitioned,
            output_path=final_path,
            members=tuple(member_records),
            schema_sha256=_schema_sha256(self.schema),
            source_generation_pin=self.source_generation_pin,
            identity_version=IDENTITY_VERSION,
        )
        _checkpoint_path(self.checkpoints_dir, self.table).write_text(
            json.dumps(
                {
                    "table": checkpoint.table,
                    "rows": checkpoint.rows,
                    "partitioned": checkpoint.partitioned,
                    "members": list(checkpoint.members),
                    "schema_sha256": checkpoint.schema_sha256,
                    "source_generation_pin": checkpoint.source_generation_pin,
                    "identity_version": checkpoint.identity_version,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        return checkpoint

    def __exit__(self, exc_type, exc, tb) -> bool:
        if not self._closed:
            for writer, _path, _idx, _rows in self._writers.values():
                writer.close()
            self._writers.clear()
            self._closed = True
        return False


def make_filing_body_reader(store: Any, scope: Mapping[str, Any]) -> Callable[[Mapping[str, Any]], str]:
    """Build a single-open body reader over ``store`` (ZIP member or opaque filing) inside an archive session.

    Reuses ``held_original()`` + ``selected_stream()`` so multi-body filings
    and multi-member ZIP archives are verified once per session instead of
    rehashing on every TEXT/narrative body lookup.
    """
    from spicy_docs.sources.fec.delimited import held_original, selected_stream
    from spicy_docs.sources.fec.filings import _ByteEvidence

    capture = scope["capture"]
    member = scope.get("member")
    max_members = int(scope.get("max_members", 10_000))
    max_decoded_bytes = int(scope.get("max_decoded_bytes", 1099511627776))
    cached_bytes: bytes | None = None
    cached_sha: str | None = None

    def _load_member() -> tuple[bytes, str]:
        nonlocal cached_bytes, cached_sha
        if cached_bytes is not None and cached_sha is not None:
            return cached_bytes, cached_sha
        original = held_original(store, capture["responseSha256"], capture["byteSize"])
        with selected_stream(
            original,
            capture=capture,
            member=member,
            max_members=max_members,
            max_decoded_bytes=max_decoded_bytes,
        ) as (decoded, _member_info, member_sha):
            cached_bytes = decoded.read()
            cached_sha = member_sha
        return cached_bytes, cached_sha

    def read_body(body: Mapping[str, Any], *, max_bytes: int = 8 * 1024**2) -> str:
        start, length = body["byte_offset"], body["byte_length"]
        if any(type(v) is not int or v < 0 for v in (start, length)):
            raise ValueError("FEC body needs nonnegative integer byte coordinates")
        if length > max_bytes:
            raise ValueError("FEC body exceeds its selected byte bound")
        payload, payload_sha = _load_member()
        if body["sha256"] != payload_sha:
            raise ValueError("FEC body digest differs from verified source stream")
        if start + length > len(payload):
            raise ValueError("FEC body extends beyond its retained original")
        raw = payload[start : start + length]
        evidence = _ByteEvidence()
        evidence.update(raw)
        text = raw.decode(evidence.encoding(body["encoding"]))
        if "field_index" in body:
            idx = body["field_index"]
            if type(idx) is not int or idx < 0:
                raise ValueError("FEC body field index must be a nonnegative integer")
            delim = body["delimiter"]
            if delim == "\x1c":
                fields = text.rstrip("\r\n").split("\x1c")
            elif delim == ",":
                records = csv.reader(io.StringIO(text, newline=""), strict=True)
                fields = next(records, None)
                if fields is None or next(records, None) is not None:
                    raise ValueError("FEC CSV body reference must contain exactly one record")
            else:
                raise ValueError("FEC body field reference requires ASCII FS or CSV")
            if idx >= len(fields):
                raise ValueError("FEC body field index is outside its retained record")
            return fields[idx]
        return text

    return read_body


def _stream_with_filing_associations(
    rows: Iterable[dict[str, Any]],
    *,
    table: str,
    schema: pa.Schema,
    filings: Sequence[dict[str, Any]],
    headers: Sequence[dict[str, Any]],
    source_generation_pin: str,
    namespace_evidence: Mapping[str, str],
    check_resources: Callable[[], None],
) -> Iterator[dict[str, Any]]:
    """Stream rows through one DuckDB connection per table via ``association_records``."""
    if (
        table not in FIELD_RULES
        or "filing_key" not in FIELD_RULES[table]["keep"]
        or "filing_key" not in schema.names
        or not ({"report_number", "filing_header_record_id"} & set(schema.names))
    ):
        yield from rows
        return
    for row, decision in association_records(
        rows,
        table=table,
        schema=schema,
        filings=filings,
        headers=headers,
        source_generation_pin=source_generation_pin,
        target_generation_pin=None,
        namespace_evidence=dict(namespace_evidence),
        check_resources=check_resources,
        target_generation_scope="self",
    ):
        yield {
            **row,
            "filing_key": decision["filing_key"],
            "filing_link_status": decision["filing_link_status"],
        }


@dataclass
class FecQueryBuildInputs:
    """Explicit streaming inputs for all 50 ``fec-query`` tables."""

    source_generation_pin: str
    selection_evidence_sha256: str
    collections: Sequence[dict[str, Any]] = ()
    source_counts: dict[str, int] | None = None
    individual_snapshot_proof: tuple[bytes, str] | None = None
    archive_inner_proofs: Sequence[tuple[bytes, str]] = ()
    weball_webl_proofs: Sequence[tuple[bytes, str]] = ()
    foia_edition_proofs: Sequence[tuple[bytes, str]] = ()
    filing_metadata_rows: Iterable[tuple[dict[str, Any], CollectionSelection]] = ()
    filing_headers: Sequence[dict[str, Any]] = ()
    header_scopes: dict[str, dict[str, Any]] = field(default_factory=dict)
    header_authorities: dict[str, str] = field(default_factory=dict)
    namespace_evidence: dict[str, str] = field(default_factory=dict)
    filing_definition_mappings: Sequence[FilingMapping] = ()
    filing_definition_contexts_by_workbook: dict[str, dict[str, dict[str, Any]]] = field(default_factory=dict)
    bulk_receipt_rows: Iterable[tuple[dict[str, Any], CollectionSelection]] = ()
    bulk_financial_rows: Iterable[tuple[dict[str, Any], CollectionSelection, BulkMapping]] = ()
    operating_expense_rows: Iterable[tuple[dict[str, Any], CollectionSelection, dict[str, Any]]] = ()
    api_financial_rows: Iterable[tuple[dict[str, Any], CollectionSelection, ApiMapping]] = ()
    filing_schedule_rows: Iterable[tuple[dict[str, Any], CollectionSelection, FilingMapping, dict[str, Any], dict[str, Any]]] = ()
    filing_cover_rows: Iterable[tuple[dict[str, Any], CollectionSelection, FilingMapping, dict[str, Any], dict[str, Any]]] = ()
    filing_text_rows: Iterable[tuple[dict[str, Any], CollectionSelection, dict[str, Any], Any, Any, Any]] = ()
    filing_physical_text_rows: Iterable[tuple[dict[str, Any], CollectionSelection]] = ()
    contribution_aggregate_rows: Iterable[tuple[dict[str, Any], CollectionSelection, str]] = ()
    summary_rows: Iterable[tuple[dict[str, Any], CollectionSelection, SummaryMapping]] = ()
    registry_rows: Iterable[tuple[dict[str, Any], CollectionSelection, RegistryMapping]] = ()
    candidate_api_rows: Iterable[tuple[dict[str, Any], Any]] = ()
    committee_api_rows: Iterable[tuple[dict[str, Any], Any]] = ()
    history_rows: Iterable[tuple[dict[str, Any], Any]] = ()
    legal_rows: Iterable[tuple[dict[str, Any], CollectionSelection]] = ()
    retained_mur_captures: Sequence[dict[str, Any]] = ()
    agency_collections: Iterable[tuple[list[dict[str, Any]], AgencySelection]] = ()
    rebuilt_tables_store: Any = None
    rebuilt_originals: Sequence[dict[str, Any]] | None = None
    rebuilt_dispositions: Mapping[str, dict[str, Any]] | None = None
    verify_rebuilt_expected_rows: bool = True


@dataclass(frozen=True)
class QueryBuildTelemetry:
    """Measured runtime and resource envelope for one 50-table ``fec-query`` build."""

    wall_seconds: float
    peak_rss_bytes: int
    peak_disk_bytes: int
    tables_built: int
    tables_resumed: int
    total_rows: int
    id_mapping_rows: int
    evidence_rows_written: int
    evidence_rows_excluded: int
    c1_control_matches: int


@dataclass(frozen=True)
class FecQueryBuildResult:
    """Sealed receipts-free 50-table ``fec-query`` generation plus ID-mapping proof and telemetry."""

    generation_dir: Path
    artifact_digest: str
    logical_id: str
    table_rows: dict[str, int]
    partitioned_tables: tuple[str, ...]
    id_mapping_path: Path
    refusal_paths: dict[str, Path]
    telemetry: QueryBuildTelemetry
    receipt_path: Path
    disposition_paths: dict[str, Path] = field(default_factory=dict)


def _sidecar_dir(checkpoints_dir: Path) -> Path:
    return checkpoints_dir / ".stage_sidecars"


def _verify_stage_sidecar(
    *,
    stage: str,
    checkpoints_dir: Path,
    source_generation_pin: str,
) -> dict[str, Any] | None:
    """Verify a stage's committed evidence + id_mapping sidecar metadata and checksums."""
    sdir = _sidecar_dir(checkpoints_dir)
    meta_path = sdir / f"{stage}.sidecar.json"
    if not meta_path.is_file():
        return None
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    if (
        meta.get("stage") != stage
        or meta.get("source_generation_pin") != source_generation_pin
        or meta.get("identity_version") != IDENTITY_VERSION
    ):
        return None
    for key in ("evidence", "id_mapping"):
        info = meta.get(key)
        if not isinstance(info, dict):
            return None
        path = sdir / str(info.get("path", ""))
        if path.is_symlink() or not path.is_file():
            return None
        if path.stat().st_size != info.get("bytes"):
            return None
        if _sha(path) != info.get("sha256"):
            return None
    return meta


class _StageRecorder:
    """Atomic per-stage evidence & ID-mapping recorder so crash-resume never loses earlier stages."""

    def __init__(
        self,
        *,
        stage: str,
        checkpoints_dir: Path,
        evidence_schema: pa.Schema,
        source_generation_pin: str,
        batch_rows: int,
        check_resources: Callable[[], None],
    ):
        self.stage = stage
        self.sdir = _sidecar_dir(checkpoints_dir)
        self.evidence_schema = evidence_schema
        self.source_generation_pin = source_generation_pin
        self.batch_rows = batch_rows
        self.check_resources = check_resources
        self._ev_partial = self.sdir / f".{stage}.evidence.partial"
        self._id_partial = self.sdir / f".{stage}.id_mapping.partial"
        self._ev_final = self.sdir / f"{stage}.evidence.parquet"
        self._id_final = self.sdir / f"{stage}.id_mapping.parquet"
        self._ev_buf: list[dict[str, Any]] = []
        self._ev_writer: pq.ParquetWriter | None = None
        self._id_writer: IdMappingWriter | None = None
        self.evidence_written = 0
        self.evidence_excluded = 0
        self.id_count = 0

    def __enter__(self) -> _StageRecorder:
        self.sdir.mkdir(parents=True, exist_ok=True)
        for p in (self._ev_partial, self._id_partial):
            if p.exists():
                p.unlink()
        self._ev_writer = pq.ParquetWriter(self._ev_partial, self.evidence_schema, compression="zstd")
        self._id_writer = IdMappingWriter(self._id_partial, batch_size=self.batch_rows)
        self._id_writer.__enter__()
        return self

    def emit_evidence(self, evidence_rows: Sequence[Mapping[str, Any]]) -> None:
        for item in evidence_rows:
            if item.get("target_table") in EXCLUDED_EVIDENCE_TARGETS:
                self.evidence_excluded += 1
                continue
            self._ev_buf.append(_align_to_schema(item, self.evidence_schema))
            self.evidence_written += 1
            if len(self._ev_buf) >= self.batch_rows:
                self._flush_evidence()

    def _flush_evidence(self) -> None:
        if not self._ev_buf:
            return
        assert self._ev_writer is not None
        self.check_resources()
        self._ev_writer.write_batch(typed_batch(self._ev_buf, self.evidence_schema))
        self._ev_buf.clear()

    def record_id(self, table: str, old_record_id: str | None, new_record_id: str | None) -> None:
        assert self._id_writer is not None
        self._id_writer.record(table, old_record_id, new_record_id)

    def commit(self) -> dict[str, Any]:
        self._flush_evidence()
        if self._ev_writer is not None:
            self._ev_writer.close()
            self._ev_writer = None
        if self._id_writer is not None:
            self._id_writer.__exit__(None, None, None)
            self.id_count = self._id_writer.count
            self._id_writer = None
        self._ev_partial.rename(self._ev_final)
        self._id_partial.rename(self._id_final)
        meta = {
            "stage": self.stage,
            "source_generation_pin": self.source_generation_pin,
            "identity_version": IDENTITY_VERSION,
            "evidence_excluded": self.evidence_excluded,
            "evidence": {
                "path": self._ev_final.name,
                "sha256": _sha(self._ev_final),
                "bytes": self._ev_final.stat().st_size,
                "rows": self.evidence_written,
            },
            "id_mapping": {
                "path": self._id_final.name,
                "sha256": _sha(self._id_final),
                "bytes": self._id_final.stat().st_size,
                "rows": self.id_count,
            },
        }
        (self.sdir / f"{self.stage}.sidecar.json").write_text(
            json.dumps(meta, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        return meta

    def __exit__(self, exc_type, exc, tb) -> bool:
        if self._ev_writer is not None:
            self._ev_writer.close()
            self._ev_writer = None
        if self._id_writer is not None:
            self._id_writer.__exit__(exc_type, exc, tb)
            self._id_writer = None
        return False


def _align_to_schema(row: Mapping[str, Any], schema: pa.Schema) -> dict[str, Any]:
    return {name: row.get(name) for name in schema.names}


def _hydrate_collection_row(collection: Mapping[str, Any]) -> dict[str, Any]:
    """Hydrate ``scope`` and ``field_mapping`` dicts from ``fec_collections.parquet`` JSON columns.

    ``fec_bulk_selection.individual_snapshot_selection()`` inspects ``collection["scope"]``
    and ``collection["field_mapping"]`` directly, whereas ``fec_collections.parquet`` stores
    them serialized inside ``requested_scope_json`` and ``collection_outcome_json["tableFieldMapping"]``.
    """
    hydrated = dict(collection)
    if "scope" not in hydrated:
        raw_scope = hydrated.get("requested_scope_json")
        if isinstance(raw_scope, str) and raw_scope:
            parsed_scope = json.loads(raw_scope)
            if isinstance(parsed_scope, dict):
                hydrated["scope"] = parsed_scope
    if "field_mapping" not in hydrated:
        raw_outcome = hydrated.get("collection_outcome_json")
        if isinstance(raw_outcome, str) and raw_outcome:
            parsed_outcome = json.loads(raw_outcome)
            if isinstance(parsed_outcome, dict) and "tableFieldMapping" in parsed_outcome:
                hydrated["field_mapping"] = parsed_outcome["tableFieldMapping"]
    return hydrated


def _id_mapping_files(path: Path) -> list[Path]:
    """Return Parquet files for an ``id_mapping`` path (supports both single file and zero-copy directory)."""
    if path.is_file():
        return [path]
    if path.is_dir():
        return sorted(path.glob("*.parquet"))
    return []


def build_fec_query_from_inputs(
    inputs: FecQueryBuildInputs,
    *,
    output_dir: Path,
    read_snapshot: Mapping[str, Any] | None = None,
    parents: Mapping[str, Mapping[str, Any]] | None = None,
    generation_inputs: Sequence[Mapping[str, Any]] = (),
    partitioned_tables: frozenset[str] = DEFAULT_PARTITIONED_TABLES,
    batch_rows: int = DEFAULT_BATCH_ROWS,
    row_group_rows: int = DEFAULT_ROW_GROUP_ROWS,
    part_rows: int = DEFAULT_PART_ROWS,
    c1_threads: int = 6,
    resume: bool = True,
    check_resources: Callable[[], None] = lambda: None,
) -> FecQueryBuildResult:
    """Stream all 50 ``fec-query`` tables to disk with checkpoints, C1-zero check, and receipts-free seal."""
    started = monotonic()
    _digest(inputs.source_generation_pin)
    _digest(inputs.selection_evidence_sha256)
    schemas = load_fec_typed_schemas(verify_processing_parity=True)
    if len(schemas) != 50:
        raise ValueError(f"Expected 50 fec-query schemas, found {len(schemas)}")

    output_dir = Path(output_dir).resolve()
    build_root = output_dir / "build"
    tables_dir = build_root / "tables"
    checkpoints_dir = output_dir / "checkpoints"
    refusals_dir = output_dir / "refusals"
    generation_dir = build_root / "generation"
    if generation_dir.exists():
        raise FileExistsError(f"Target fec-query generation directory already exists: {generation_dir}")
    tables_dir.mkdir(parents=True, exist_ok=True)
    checkpoints_dir.mkdir(parents=True, exist_ok=True)
    refusals_dir.mkdir(parents=True, exist_ok=True)

    id_mapping_path = output_dir / "id_mapping"
    completed: dict[str, TableCheckpoint] = {}
    resumed_tables: set[str] = set()
    refusal_paths: dict[str, Path] = {}
    disposition_paths: dict[str, Path] = {}
    stage_metas: dict[str, dict[str, Any]] = {}
    stage_order: list[str] = []
    peak_disk = 0

    # Fast path: if all 50 tables + id_mapping already finalized cleanly (e.g. crash during sealing),
    # resume every table directly without needing stage sidecars.
    all_finalized_resume = False
    if resume and bool(_id_mapping_files(id_mapping_path)):
        all_cks = {
            t: load_verified_checkpoint(
                table=t,
                tables_dir=tables_dir,
                checkpoints_dir=checkpoints_dir,
                schema=schemas[t],
                source_generation_pin=inputs.source_generation_pin,
            )
            for t in schemas
        }
        if all(ck is not None for ck in all_cks.values()):
            all_finalized_resume = True
            for t, ck in all_cks.items():
                assert ck is not None
                completed[t] = ck
                resumed_tables.add(t)

    def _try_resume_stage(stage: str, stage_tables: Sequence[str]) -> bool:
        stage_order.append(stage)
        if not resume:
            return False
        if all_finalized_resume:
            return True
        sidecar = _verify_stage_sidecar(
            stage=stage,
            checkpoints_dir=checkpoints_dir,
            source_generation_pin=inputs.source_generation_pin,
        )
        if sidecar is None:
            return False
        cks: dict[str, TableCheckpoint] = {}
        for t in stage_tables:
            ck = load_verified_checkpoint(
                table=t,
                tables_dir=tables_dir,
                checkpoints_dir=checkpoints_dir,
                schema=schemas[t],
                source_generation_pin=inputs.source_generation_pin,
            )
            if ck is None:
                return False
            cks[t] = ck
        for t, ck in cks.items():
            completed[t] = ck
            resumed_tables.add(t)
        stage_metas[stage] = sidecar
        return True

    def _stage_recorder(stage: str) -> _StageRecorder:
        return _StageRecorder(
            stage=stage,
            checkpoints_dir=checkpoints_dir,
            evidence_schema=schemas["fec_record_evidence"],
            source_generation_pin=inputs.source_generation_pin,
            batch_rows=batch_rows,
            check_resources=check_resources,
        )

    def _open_table_writer(table: str) -> StreamingTableWriter:
        return StreamingTableWriter(
            table=table,
            schema=schemas[table],
            tables_dir=tables_dir,
            checkpoints_dir=checkpoints_dir,
            source_generation_pin=inputs.source_generation_pin,
            partitioned=table in partitioned_tables,
            batch_rows=batch_rows,
            row_group_rows=row_group_rows,
            part_rows=part_rows,
            check_resources=check_resources,
        )

    def _write_rows_table(table: str, rows_iter: Iterable[Mapping[str, Any]]) -> TableCheckpoint:
        with _open_table_writer(table) as writer:
            for row in rows_iter:
                writer.append(row)
            ck = writer.finish()
        completed[table] = ck
        return ck

    # ---- STAGE 1: Foundational small tables (in-memory bounded) ----
    # 1a. fec_collection_selection
    if not _try_resume_stage("1a_collection_selection", ("fec_collection_selection",)):
        with _stage_recorder("1a_collection_selection") as rec:
            hydrated_cols = [_hydrate_collection_row(col) for col in inputs.collections]
            selection_rows: list[dict[str, Any]] = []
            if inputs.individual_snapshot_proof is not None:
                proof_bytes, proof_sha = inputs.individual_snapshot_proof
                selection_rows.extend(
                    individual_snapshot_selection(
                        proof_bytes,
                        proof_sha256=proof_sha,
                        source_generation_pin=inputs.source_generation_pin,
                        collections=hydrated_cols,
                        source_counts=inputs.source_counts or {},
                    )
                )
            if hydrated_cols:
                selection_rows.extend(
                    cross_source_collection_selection(
                        hydrated_cols,
                        source_generation_pin=inputs.source_generation_pin,
                        source_counts=inputs.source_counts,
                        archive_inner_proofs=inputs.archive_inner_proofs,
                        weball_webl_proofs=inputs.weball_webl_proofs,
                        foia_edition_proofs=inputs.foia_edition_proofs,
                    )
                )
            for row in selection_rows:
                rec.record_id("fec_collection_selection", row["record_id"], row["record_id"])
            _write_rows_table("fec_collection_selection", selection_rows)
            stage_metas["1a_collection_selection"] = rec.commit()

    # 1b. fec_filings & fec_filing_links (retained in memory for downstream resolution & associations)
    filings: list[dict[str, Any]] = []
    if _try_resume_stage("1b_filings_and_links", (FILINGS, LINKS)):
        if not all_finalized_resume:
            filings = pq.read_table(completed[FILINGS].output_path).to_pylist()
    else:
        with _stage_recorder("1b_filings_and_links") as rec:
            raw_links: list[dict[str, Any]] = []
            for row, selection in inputs.filing_metadata_rows:
                check_resources()
                mapped, ev = map_filing_metadata(row, selection)
                for f_row in mapped[FILINGS]:
                    filings.append(f_row)
                    rec.record_id(
                        FILINGS,
                        legacy_record_id_for_row(FILINGS, row, selection.source_authority),
                        f_row["record_id"],
                    )
                for l_row in mapped[LINKS]:
                    raw_links.append(l_row)
                    rec.record_id(
                        LINKS,
                        legacy_record_id_for_row(
                            LINKS,
                            row,
                            selection.source_authority,
                            typed_subrecord_pointer=l_row.get("source_pointer") or None,
                        ),
                        l_row["record_id"],
                    )
                rec.emit_evidence(ev)
            resolved_links = resolve_filing_links(raw_links, filings)
            _write_rows_table(FILINGS, filings)
            _write_rows_table(LINKS, resolved_links)
            stage_metas["1b_filings_and_links"] = rec.commit()

    # 1c. fec_filing_definitions & fec_filing_definition_evidence
    if not _try_resume_stage("1c_filing_definitions", ("fec_filing_definitions", "fec_filing_definition_evidence")):
        with _stage_recorder("1c_filing_definitions") as rec:
            def_rows: list[dict[str, Any]] = []
            def_ev_rows: list[dict[str, Any]] = []
            seen_pins: set[str] = set()
            for mapping in inputs.filing_definition_mappings:
                if mapping.layout_pin in seen_pins:
                    continue
                contexts = inputs.filing_definition_contexts_by_workbook.get(mapping.workbook_sha256)
                if not contexts:
                    continue
                check_resources()
                definition, ev_items = bind_definition(mapping, contexts, inputs.source_generation_pin)
                seen_pins.add(mapping.layout_pin)
                def_rows.append(definition)
                rec.record_id("fec_filing_definitions", definition["record_id"], definition["record_id"])
                def_ev_rows.extend(ev_items)
            _write_rows_table("fec_filing_definitions", def_rows)
            _write_rows_table("fec_filing_definition_evidence", def_ev_rows)
            stage_metas["1c_filing_definitions"] = rec.commit()

    # 1d. fec_filing_header_associations
    header_assocs: list[dict[str, Any]] = []
    if _try_resume_stage("1d_header_associations", ("fec_filing_header_associations",)):
        if not all_finalized_resume:
            header_assocs = pq.read_table(completed["fec_filing_header_associations"].output_path).to_pylist()
    else:
        with _stage_recorder("1d_header_associations") as rec:
            if inputs.filing_headers:
                header_assocs = associate_filing_headers(
                    inputs.filing_headers,
                    scopes=inputs.header_scopes,
                    authorities=inputs.header_authorities,
                    filings=filings,
                    source_generation_pin=inputs.source_generation_pin,
                    selection_evidence_sha256=inputs.selection_evidence_sha256,
                )
                for h_row in header_assocs:
                    rec.record_id("fec_filing_header_associations", h_row["record_id"], h_row["record_id"])
            _write_rows_table("fec_filing_header_associations", header_assocs)
            stage_metas["1d_header_associations"] = rec.commit()

    # 1e. Four REBUILT_TABLES via fec_original_tables.rebuild()
    for table_name in REBUILT_FEC_QUERY_TABLES:
        stage_id = f"1e_rebuilt_{table_name}"
        if _try_resume_stage(stage_id, (table_name,)):
            refusal_file = refusals_dir / f"{table_name}.refusals.json"
            if refusal_file.is_file():
                refusal_paths[table_name] = refusal_file
            disp_file = refusals_dir / f"{table_name}.dispositions.json"
            if disp_file.is_file():
                disposition_paths[table_name] = disp_file
            continue
        with _stage_recorder(stage_id) as rec:
            if inputs.rebuilt_tables_store is not None:
                check_resources()
                # Omit originals when None so fec_original_tables.rebuild() enforces
                # default runtime selection & disposition checks automatically (ss-j4vz).
                rebuild_kwargs: dict[str, Any] = {"store": inputs.rebuilt_tables_store}
                if inputs.rebuilt_originals is not None:
                    rebuild_kwargs["originals"] = inputs.rebuilt_originals
                if inputs.rebuilt_dispositions is not None:
                    rebuild_kwargs["dispositions"] = inputs.rebuilt_dispositions
                rebuilt = original_tables.rebuild(table_name, **rebuild_kwargs)
                if inputs.verify_rebuilt_expected_rows and inputs.rebuilt_originals is None:
                    expected = original_tables.expected_rows(table_name)
                    if len(rebuilt.rows) != expected:
                        raise ValueError(
                            f"{table_name}: rebuilt row count {len(rebuilt.rows)} differs from expected {expected}"
                        )
                refusal_file = refusals_dir / f"{table_name}.refusals.json"
                refusal_file.write_text(json.dumps(sorted(rebuilt.refusals), indent=1) + "\n", encoding="utf-8")
                refusal_paths[table_name] = refusal_file
                if rebuilt.dispositions:
                    disp_file = refusals_dir / f"{table_name}.dispositions.json"
                    disp_file.write_text(
                        json.dumps(rebuilt.dispositions, indent=2, sort_keys=True) + "\n",
                        encoding="utf-8",
                    )
                    disposition_paths[table_name] = disp_file
                for r_row in rebuilt.rows:
                    rec.record_id(table_name, r_row["record_id"], r_row["record_id"])
                _write_rows_table(table_name, rebuilt.rows)
            else:
                _write_rows_table(table_name, ())
            stage_metas[stage_id] = rec.commit()

    # ---- STAGE 2: Medium tables (streamed per collection / 4,096-row batches) ----
    # 2a. Candidate API, Committee API, and API Response Controls
    api_tables = (
        "fec_candidate_api_observations",
        "fec_committee_observations",
        "fec_api_response_controls",
    )
    if not _try_resume_stage("2a_api_entities", api_tables):
        with _stage_recorder("2a_api_entities") as rec, ExitStack() as api_stack:
            api_writers = {t: api_stack.enter_context(_open_table_writer(t)) for t in api_tables}
            for row, prepared in inputs.candidate_api_rows:
                check_resources()
                mapped, ev = map_candidate_api(row, prepared)
                auth = prepared.selection.source_authority
                for t, items in mapped.items():
                    for item in items:
                        api_writers[t].append(item)
                        rec.record_id(t, legacy_record_id_for_row(t, row, auth), item["record_id"])
                rec.emit_evidence(ev)
            for row, prepared in inputs.committee_api_rows:
                check_resources()
                mapped, ev = map_committee_api(row, prepared)
                auth = prepared.selection.source_authority if hasattr(prepared, "selection") else prepared.source_authority
                for t, items in mapped.items():
                    for item in items:
                        api_writers[t].append(item)
                        rec.record_id(t, legacy_record_id_for_row(t, row, auth), item["record_id"])
                rec.emit_evidence(ev)
            for t in api_tables:
                completed[t] = api_writers[t].finish()
            stage_metas["2a_api_entities"] = rec.commit()

    # 2b. Committee Master & Postgres Committee History
    history_tables = (
        "fec_committee_master_observations",
        "fec_postgres_committee_history_observations",
    )
    if not _try_resume_stage("2b_committee_history", history_tables):
        with _stage_recorder("2b_committee_history") as rec, ExitStack() as hist_stack:
            hist_writers = {t: hist_stack.enter_context(_open_table_writer(t)) for t in history_tables}
            for row, prepared in inputs.history_rows:
                check_resources()
                mapped, ev = map_history(row, prepared)
                t = prepared.table
                hist_writers[t].append(mapped)
                rec.record_id(
                    t,
                    legacy_record_id_for_row(t, row, prepared.selection.source_authority),
                    mapped["record_id"],
                )
                rec.emit_evidence(ev)
            for t in history_tables:
                completed[t] = hist_writers[t].finish()
            stage_metas["2b_committee_history"] = rec.commit()

    # 2c. Registry tables (Form1, Form2, Lobbyists, Quality Notices)
    registry_tables = (
        "fec_registration_statements",
        "fec_lobbyist_registrations",
        "fec_quality_notices",
    )
    if not _try_resume_stage("2c_registry", registry_tables):
        with _stage_recorder("2c_registry") as rec, ExitStack() as reg_stack:
            reg_writers = {t: reg_stack.enter_context(_open_table_writer(t)) for t in registry_tables}
            for row, selection, mapping in inputs.registry_rows:
                check_resources()
                mapped, ev = map_registry(row, selection, mapping)
                if mapped is not None:
                    reg_writers[mapping.table].append(mapped)
                    rec.record_id(
                        mapping.table,
                        legacy_record_id_for_row(mapping.table, row, selection.source_authority),
                        mapped["record_id"],
                    )
                    rec.emit_evidence(ev)
            for t in registry_tables:
                completed[t] = reg_writers[t].finish()
            stage_metas["2c_registry"] = rec.commit()

    # 2d. Reported Financial Summaries
    if not _try_resume_stage("2d_summaries", ("fec_reported_financial_summaries",)):
        with _stage_recorder("2d_summaries") as rec, _open_table_writer("fec_reported_financial_summaries") as summary_writer:
            for row, selection, mapping in inputs.summary_rows:
                check_resources()
                mapped, ev = map_summary(row, selection, mapping)
                summary_writer.append(mapped)
                rec.record_id(
                    "fec_reported_financial_summaries",
                    legacy_record_id_for_row("fec_reported_financial_summaries", row, selection.source_authority),
                    mapped["record_id"],
                )
                rec.emit_evidence(ev)
            completed["fec_reported_financial_summaries"] = summary_writer.finish()
            stage_metas["2d_summaries"] = rec.commit()

    # 2e. Legal matters, parties, events, documents, audit findings (+ redacted MUR captures)
    legal_tables = (
        "fec_legal_matters",
        "fec_legal_parties",
        "fec_legal_events",
        "fec_legal_documents",
        "fec_audit_findings",
    )
    if not _try_resume_stage("2e_legal", legal_tables):
        with _stage_recorder("2e_legal") as rec, ExitStack() as legal_stack:
            legal_writers = {t: legal_stack.enter_context(_open_table_writer(t)) for t in legal_tables}
            for row, selection in inputs.legal_rows:
                check_resources()
                kind = legal_source_kind(row)
                base_ptr = "/metadata" if kind == "wrapped_record" else ""
                mapped, ev = map_legal(row, selection)
                for t, items in mapped.items():
                    for item in items:
                        legal_writers[t].append(item)
                        ptr = item.get("source_pointer")
                        rec.record_id(
                            t,
                            legacy_record_id_for_row(
                                t,
                                row,
                                selection.source_authority,
                                typed_subrecord_pointer=ptr if ptr and ptr != base_ptr else None,
                            ),
                            item["record_id"],
                        )
                rec.emit_evidence(ev)
            if inputs.retained_mur_captures:
                mur_tables, mur_ev = qualify_retained_mur_documents(
                    inputs.retained_mur_captures,
                    source_generation_pin=inputs.source_generation_pin,
                    selection_evidence_sha256=inputs.selection_evidence_sha256,
                )
                for t, items in mur_tables.items():
                    for item in items:
                        legal_writers[t].append(item)
                        rec.record_id(t, item["record_id"], item["record_id"])
                rec.emit_evidence(mur_ev)
            for t in legal_tables:
                completed[t] = legal_writers[t].finish()
            stage_metas["2e_legal"] = rec.commit()

    # 2f. Agency reports, metrics, recommendations, text, documents, dispositions
    agency_tables = tuple(AGENCY_SCHEMAS)
    if not _try_resume_stage("2f_agency", agency_tables):
        with _stage_recorder("2f_agency") as rec, ExitStack() as agency_stack:
            agency_writers = {t: agency_stack.enter_context(_open_table_writer(t)) for t in agency_tables}
            for rows, agency_sel in inputs.agency_collections:
                check_resources()
                by_source_rid = {r["source_record_id"]: r for r in rows}
                mapped = map_agency_collection(rows, agency_sel)
                for t, items in mapped.tables.items():
                    for item in items:
                        agency_writers[t].append(item)
                        if "record_id" in item and item.get("source_record_id") in by_source_rid:
                            rec.record_id(
                                t,
                                legacy_record_id_for_row(
                                    t,
                                    by_source_rid[item["source_record_id"]],
                                    agency_sel.source_authority,
                                    agency_subrecord_pointer=item.get("subrecord_pointer"),
                                ),
                                item["record_id"],
                            )
                rec.emit_evidence(mapped.evidence)
            for t in agency_tables:
                completed[t] = agency_writers[t].finish()
            stage_metas["2f_agency"] = rec.commit()

    # 2g. Filing cover observations (fec_filing_report_observations)
    if not _try_resume_stage("2g_filing_covers", ("fec_filing_report_observations",)):
        with _stage_recorder("2g_filing_covers") as rec:
            def _iter_cover_rows() -> Iterator[dict[str, Any]]:
                for row, selection, mapping, layout, header_row in inputs.filing_cover_rows:
                    check_resources()
                    mapped, ev = map_filing_cover(row, selection, mapping, layout=layout, header_row=header_row)
                    rec.record_id(
                        "fec_filing_report_observations",
                        legacy_record_id_for_row("fec_filing_report_observations", row, selection.source_authority),
                        mapped["record_id"],
                    )
                    rec.emit_evidence(ev)
                    yield _align_to_schema(mapped, schemas["fec_filing_report_observations"])

            associated_covers = _stream_with_filing_associations(
                _iter_cover_rows(),
                table="fec_filing_report_observations",
                schema=schemas["fec_filing_report_observations"],
                filings=filings,
                headers=header_assocs,
                source_generation_pin=inputs.source_generation_pin,
                namespace_evidence=inputs.namespace_evidence,
                check_resources=check_resources,
            )
            _write_rows_table("fec_filing_report_observations", associated_covers)
            stage_metas["2g_filing_covers"] = rec.commit()

    # 2h. Filing narrative text observations (fec_filing_text_observations)
    if not _try_resume_stage("2h_filing_text", ("fec_filing_text_observations",)):
        with _stage_recorder("2h_filing_text") as rec:
            def _iter_text_rows() -> Iterator[dict[str, Any]]:
                for row, selection, header_row, read_body, mapping, layout in inputs.filing_text_rows:
                    check_resources()
                    mapped, ev = map_filing_text(
                        row, selection, header_row=header_row, read_body=read_body, mapping=mapping, layout=layout
                    )
                    rec.record_id(
                        "fec_filing_text_observations",
                        legacy_record_id_for_row("fec_filing_text_observations", row, selection.source_authority),
                        mapped["record_id"],
                    )
                    rec.emit_evidence(ev)
                    yield _align_to_schema(mapped, schemas["fec_filing_text_observations"])
                for row, selection in inputs.filing_physical_text_rows:
                    check_resources()
                    mapped, ev = map_filing_physical_text(row, selection)
                    rec.record_id(
                        "fec_filing_text_observations",
                        legacy_record_id_for_row("fec_filing_text_observations", row, selection.source_authority),
                        mapped["record_id"],
                    )
                    rec.emit_evidence(ev)
                    yield _align_to_schema(mapped, schemas["fec_filing_text_observations"])

            associated_texts = _stream_with_filing_associations(
                _iter_text_rows(),
                table="fec_filing_text_observations",
                schema=schemas["fec_filing_text_observations"],
                filings=filings,
                headers=header_assocs,
                source_generation_pin=inputs.source_generation_pin,
                namespace_evidence=inputs.namespace_evidence,
                check_resources=check_resources,
            )
            _write_rows_table("fec_filing_text_observations", associated_texts)
            stage_metas["2h_filing_text"] = rec.commit()

    # ---- STAGE 3: Large financial & schedule tables (per-table checkpoints, zero bulk double-write) ----
    remaining_schedule_tables = (
        "fec_inaugural_donations",
        "fec_account_transfers",
        "fec_allocated_disbursements",
        "fec_allocation_bases",
        "fec_bundled_contributions",
        "fec_communication_costs",
        "fec_contribution_aggregates",
        "fec_coordinated_party_expenditures",
        "fec_debts",
        "fec_electioneering_communications",
        "fec_independent_expenditures",
        "fec_loan_guarantors",
        "fec_loan_terms",
        "fec_loans",
    )
    raw_stage_dir = build_root / ".schedules_staged"
    pre_manifest_path = raw_stage_dir / "manifest.json"

    def _iter_staged_file(path: Path) -> Iterator[dict[str, Any]]:
        if not path.is_file():
            return
        with pq.ParquetFile(path) as pf:
            for batch in pf.iter_batches(batch_size=batch_rows, use_threads=False):
                check_resources()
                yield from batch.to_pylist()

    def _pre_manifest_valid() -> bool:
        if not pre_manifest_path.is_file():
            return False
        manifest = json.loads(pre_manifest_path.read_text(encoding="utf-8"))
        if manifest.get("source_generation_pin") != inputs.source_generation_pin:
            return False
        for rel, expected_sha in (manifest.get("files") or {}).items():
            target = raw_stage_dir / rel
            if target.is_symlink() or not target.is_file() or _sha(target) != expected_sha:
                return False
        return True

    def _all_stage3_tables_resumable() -> bool:
        if all_finalized_resume:
            return True
        if not resume:
            return False
        for s_name, s_tables in (
            ("3a_fec_receipts", ("fec_receipts",)),
            ("3b_fec_intercommittee_transactions", ("fec_intercommittee_transactions",)),
            ("3c_fec_disbursements", ("fec_disbursements",)),
            ("3d_remaining_schedules", remaining_schedule_tables),
        ):
            if _verify_stage_sidecar(
                stage=s_name,
                checkpoints_dir=checkpoints_dir,
                source_generation_pin=inputs.source_generation_pin,
            ) is None:
                return False
            for t in s_tables:
                if (
                    load_verified_checkpoint(
                        table=t,
                        tables_dir=tables_dir,
                        checkpoints_dir=checkpoints_dir,
                        schema=schemas[t],
                        source_generation_pin=inputs.source_generation_pin,
                    )
                    is None
                ):
                    return False
        return True

    # 3_pre_schedules: stage ONLY small multi-table electronic/API schedules (<150 MiB total),
    # never double-writing bulk receipts (34.8M), bulk intercommittee (10.6M), or oppexp (1.7M).
    stage_order.append("3_pre_schedules")
    pre_sidecar = (
        _verify_stage_sidecar(
            stage="3_pre_schedules",
            checkpoints_dir=checkpoints_dir,
            source_generation_pin=inputs.source_generation_pin,
        )
        if resume and not all_finalized_resume
        else None
    )
    if all_finalized_resume:
        pass
    elif pre_sidecar is not None and (_all_stage3_tables_resumable() or _pre_manifest_valid()):
        stage_metas["3_pre_schedules"] = pre_sidecar
    else:
        if raw_stage_dir.exists():
            shutil.rmtree(raw_stage_dir)
        raw_stage_dir.mkdir(parents=True)
        staged_files: dict[str, Path] = {}
        with _stage_recorder("3_pre_schedules") as rec, ExitStack() as stage_stack:
            stage_writers: dict[str, pq.ParquetWriter] = {}
            stage_buffers: dict[str, list[dict[str, Any]]] = defaultdict(list)

            def _pre_flush(table: str) -> None:
                buf = stage_buffers.get(table)
                if not buf:
                    return
                check_resources()
                writer = stage_writers.get(table)
                if writer is None:
                    path = raw_stage_dir / f"{table}.mapped.parquet"
                    staged_files[table] = path
                    writer = stage_stack.enter_context(pq.ParquetWriter(path, schemas[table], compression="zstd"))
                    stage_writers[table] = writer
                writer.write_batch(typed_batch(buf, schemas[table]))
                buf.clear()

            def _pre_append(table: str, row: dict[str, Any]) -> None:
                buf = stage_buffers[table]
                buf.append(_align_to_schema(row, schemas[table]))
                if len(buf) >= batch_rows:
                    _pre_flush(table)

            for row, selection, api_mapping in inputs.api_financial_rows:
                check_resources()
                mapped, ev = map_api_financial(row, selection, api_mapping)
                t = api_mapping.fields.table
                rec.record_id(t, legacy_record_id_for_row(t, row, selection.source_authority), mapped["record_id"])
                rec.emit_evidence(ev)
                _pre_append(t, mapped)

            for row, selection, sched_mapping, layout, header_row in inputs.filing_schedule_rows:
                check_resources()
                mapped, ev = map_filing_financial(row, selection, sched_mapping, layout=layout, header_row=header_row)
                t = sched_mapping.fields.table
                rec.record_id(t, legacy_record_id_for_row(t, row, selection.source_authority), mapped["record_id"])
                rec.emit_evidence(ev)
                _pre_append(t, mapped)

            for t in list(stage_buffers):
                _pre_flush(t)
            stage_stack.close()
            pre_manifest_path.write_text(
                json.dumps(
                    {
                        "source_generation_pin": inputs.source_generation_pin,
                        "files": {path.name: _sha(path) for path in staged_files.values()},
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            stage_metas["3_pre_schedules"] = rec.commit()

    # 3a. fec_receipts (streams bulk_receipt_rows directly -> zero 2.85 GiB .mapped.parquet copy)
    if not _try_resume_stage("3a_fec_receipts", ("fec_receipts",)):
        with _stage_recorder("3a_fec_receipts") as rec:
            def _iter_fec_receipts() -> Iterator[dict[str, Any]]:
                for row, selection in inputs.bulk_receipt_rows:
                    check_resources()
                    mapped, ev = bulk_receipt(row, selection)
                    rec.record_id(
                        "fec_receipts",
                        legacy_record_id_for_row("fec_receipts", row, selection.source_authority),
                        mapped["record_id"],
                    )
                    rec.emit_evidence(ev)
                    yield _align_to_schema(mapped, schemas["fec_receipts"])
                yield from _iter_staged_file(raw_stage_dir / "fec_receipts.mapped.parquet")

            associated_receipts = _stream_with_filing_associations(
                _iter_fec_receipts(),
                table="fec_receipts",
                schema=schemas["fec_receipts"],
                filings=filings,
                headers=header_assocs,
                source_generation_pin=inputs.source_generation_pin,
                namespace_evidence=inputs.namespace_evidence,
                check_resources=check_resources,
            )
            _write_rows_table("fec_receipts", associated_receipts)
            peak_disk = max(peak_disk, _dir_bytes(output_dir))
            stage_metas["3a_fec_receipts"] = rec.commit()

    # 3b. fec_intercommittee_transactions (streams directly; spills only tiny non-intercommittee bulk rows)
    if not _try_resume_stage("3b_fec_intercommittee_transactions", ("fec_intercommittee_transactions",)):
        with _stage_recorder("3b_fec_intercommittee_transactions") as rec, ExitStack() as bulk_other_stack:
            other_writers: dict[str, pq.ParquetWriter] = {}
            other_buffers: dict[str, list[dict[str, Any]]] = defaultdict(list)

            def _other_flush(table: str) -> None:
                buf = other_buffers.get(table)
                if not buf:
                    return
                check_resources()
                writer = other_writers.get(table)
                if writer is None:
                    raw_stage_dir.mkdir(parents=True, exist_ok=True)
                    path = raw_stage_dir / f"bulk_other_{table}.mapped.parquet"
                    writer = bulk_other_stack.enter_context(pq.ParquetWriter(path, schemas[table], compression="zstd"))
                    other_writers[table] = writer
                writer.write_batch(typed_batch(buf, schemas[table]))
                buf.clear()

            def _iter_intercommittee() -> Iterator[dict[str, Any]]:
                for row, selection, mapping in inputs.bulk_financial_rows:
                    check_resources()
                    mapped, ev = map_bulk_financial(row, selection, mapping)
                    rec.record_id(
                        mapping.table,
                        legacy_record_id_for_row(mapping.table, row, selection.source_authority),
                        mapped["record_id"],
                    )
                    rec.emit_evidence(ev)
                    aligned = _align_to_schema(mapped, schemas[mapping.table])
                    if mapping.table == "fec_intercommittee_transactions":
                        yield aligned
                    else:
                        buf = other_buffers[mapping.table]
                        buf.append(aligned)
                        if len(buf) >= batch_rows:
                            _other_flush(mapping.table)
                for t in list(other_buffers):
                    _other_flush(t)
                yield from _iter_staged_file(raw_stage_dir / "fec_intercommittee_transactions.mapped.parquet")

            associated_ic = _stream_with_filing_associations(
                _iter_intercommittee(),
                table="fec_intercommittee_transactions",
                schema=schemas["fec_intercommittee_transactions"],
                filings=filings,
                headers=header_assocs,
                source_generation_pin=inputs.source_generation_pin,
                namespace_evidence=inputs.namespace_evidence,
                check_resources=check_resources,
            )
            _write_rows_table("fec_intercommittee_transactions", associated_ic)
            bulk_other_stack.close()
            peak_disk = max(peak_disk, _dir_bytes(output_dir))
            stage_metas["3b_fec_intercommittee_transactions"] = rec.commit()

    # 3c. fec_disbursements (streams operating_expense_rows directly -> zero .mapped.parquet copy)
    if not _try_resume_stage("3c_fec_disbursements", ("fec_disbursements",)):
        with _stage_recorder("3c_fec_disbursements") as rec:
            def _iter_disbursements() -> Iterator[dict[str, Any]]:
                for row, selection, header_row in inputs.operating_expense_rows:
                    check_resources()
                    mapped, ev = operating_expense(row, selection, header_row)
                    rec.record_id(
                        "fec_disbursements",
                        legacy_record_id_for_row("fec_disbursements", row, selection.source_authority),
                        mapped["record_id"],
                    )
                    rec.emit_evidence(ev)
                    yield _align_to_schema(mapped, schemas["fec_disbursements"])
                yield from _iter_staged_file(raw_stage_dir / "fec_disbursements.mapped.parquet")
                yield from _iter_staged_file(raw_stage_dir / "bulk_other_fec_disbursements.mapped.parquet")

            associated_disb = _stream_with_filing_associations(
                _iter_disbursements(),
                table="fec_disbursements",
                schema=schemas["fec_disbursements"],
                filings=filings,
                headers=header_assocs,
                source_generation_pin=inputs.source_generation_pin,
                namespace_evidence=inputs.namespace_evidence,
                check_resources=check_resources,
            )
            _write_rows_table("fec_disbursements", associated_disb)
            peak_disk = max(peak_disk, _dir_bytes(output_dir))
            stage_metas["3c_fec_disbursements"] = rec.commit()

    # 3d. Remaining 14 schedule & aggregate tables
    if not _try_resume_stage("3d_remaining_schedules", remaining_schedule_tables):
        with _stage_recorder("3d_remaining_schedules") as rec:
            for table in remaining_schedule_tables:
                if table == "fec_contribution_aggregates":
                    def _iter_aggs() -> Iterator[dict[str, Any]]:
                        for row, selection, kind in inputs.contribution_aggregate_rows:
                            check_resources()
                            mapped, ev = map_contribution_aggregate(row, selection, kind=kind)
                            rec.record_id(
                                "fec_contribution_aggregates",
                                legacy_record_id_for_row("fec_contribution_aggregates", row, selection.source_authority),
                                mapped["record_id"],
                            )
                            rec.emit_evidence(ev)
                            yield _align_to_schema(mapped, schemas["fec_contribution_aggregates"])
                        yield from _iter_staged_file(raw_stage_dir / "fec_contribution_aggregates.mapped.parquet")
                        yield from _iter_staged_file(raw_stage_dir / "bulk_other_fec_contribution_aggregates.mapped.parquet")

                    row_source: Iterator[dict[str, Any]] = _iter_aggs()
                else:
                    def _iter_other_table(tbl: str = table) -> Iterator[dict[str, Any]]:
                        yield from _iter_staged_file(raw_stage_dir / f"bulk_other_{tbl}.mapped.parquet")
                        yield from _iter_staged_file(raw_stage_dir / f"{tbl}.mapped.parquet")

                    row_source = _iter_other_table()

                associated_rem = _stream_with_filing_associations(
                    row_source,
                    table=table,
                    schema=schemas[table],
                    filings=filings,
                    headers=header_assocs,
                    source_generation_pin=inputs.source_generation_pin,
                    namespace_evidence=inputs.namespace_evidence,
                    check_resources=check_resources,
                )
                _write_rows_table(table, associated_rem)
                peak_disk = max(peak_disk, _dir_bytes(output_dir))
            stage_metas["3d_remaining_schedules"] = rec.commit()

    if raw_stage_dir.exists():
        shutil.rmtree(raw_stage_dir)

    # ---- Stream-merge evidence & zero-copy adopt per-stage ID-mapping parts ----
    evidence_written_total = 0
    evidence_excluded_total = 0
    id_mapping_total = 0
    if all_finalized_resume:
        evidence_written_total = completed["fec_record_evidence"].rows
        id_mapping_total = sum(pq.read_metadata(p).num_rows for p in _id_mapping_files(id_mapping_path))
    else:
        sdir = _sidecar_dir(checkpoints_dir)
        with _open_table_writer("fec_record_evidence") as ev_writer:
            for stage in stage_order:
                meta = stage_metas[stage]
                evidence_excluded_total += int(meta.get("evidence_excluded", 0))
                ev_file = sdir / meta["evidence"]["path"]
                with pq.ParquetFile(ev_file) as pf:
                    for batch in pf.iter_batches(batch_size=batch_rows, use_threads=False):
                        check_resources()
                        for row in batch.to_pylist():
                            ev_writer.append(row)
                            evidence_written_total += 1
            completed["fec_record_evidence"] = ev_writer.finish()
        # Reclaim evidence sidecar disk immediately before finalizing id_mapping.
        for stage in stage_order:
            ev_file = sdir / stage_metas[stage]["evidence"]["path"]
            if ev_file.is_file():
                ev_file.unlink()

        # Zero-copy rename each committed stage id_mapping Parquet into output_dir/id_mapping/
        # (eliminates ~3-4 GiB merge duplication on full 49.4M-row corpus).
        id_staging = output_dir / ".id_mapping.partial"
        if id_staging.exists():
            if id_staging.is_dir():
                shutil.rmtree(id_staging)
            else:
                id_staging.unlink()
        id_staging.mkdir(parents=True)
        moved_parts = 0
        for idx, stage in enumerate(stage_order):
            meta = stage_metas[stage]
            id_file = sdir / meta["id_mapping"]["path"]
            rows_in_part = int(pq.read_metadata(id_file).num_rows)
            if rows_in_part > 0:
                id_file.rename(id_staging / f"part-{idx:03d}-{stage}.parquet")
                id_mapping_total += rows_in_part
                moved_parts += 1
            elif id_file.is_file():
                id_file.unlink()
        if moved_parts == 0:
            pq.write_table(
                pa.Table.from_pylist([], schema=ID_MAPPING_SCHEMA),
                id_staging / "part-000-empty.parquet",
                compression="zstd",
            )
        if id_mapping_path.exists():
            if id_mapping_path.is_dir():
                shutil.rmtree(id_mapping_path)
            else:
                id_mapping_path.unlink()
        id_staging.rename(id_mapping_path)
        peak_disk = max(peak_disk, _dir_bytes(output_dir))
        if sdir.exists():
            shutil.rmtree(sdir)

    # Verify exact 50-table completeness and schema identity before C1 check & sealing.
    if set(completed) != set(schemas):
        missing = sorted(set(schemas) - set(completed))
        extra = sorted(set(completed) - set(schemas))
        raise ValueError(f"Built fec-query table set differs from 50 declared schemas: missing={missing}, extra={extra}")

    # ---- STAGE 4: Full-output C1-zero gate & receipts-free zero-copy generation sealing ----
    output_paths = [completed[name].output_path for name in sorted(schemas)]
    refuse_c1_controls(output_paths, check_resources=check_resources, threads=c1_threads)

    partitioned_spec = {
        f"{name}.parquet": ["source_namespace"]
        for name in sorted(schemas)
        if completed[name].partitioned
    }
    peak_disk = max(peak_disk, _dir_bytes(output_dir))

    artifact = build_generation(
        generation_dir,
        family="fec-query",
        files=output_paths,
        expected_keys=[f"{name}.parquet" for name in sorted(schemas)],
        read_snapshot=read_snapshot,
        parents=parents,
        inputs=generation_inputs,
        partitioned=partitioned_spec,
        receipt_path=None,
        owned_output_root=build_root,
    )
    if tables_dir.exists() and not any(tables_dir.iterdir()):
        tables_dir.rmdir()

    table_rows = {name: completed[name].rows for name in sorted(schemas)}
    telemetry = QueryBuildTelemetry(
        wall_seconds=round(monotonic() - started, 3),
        peak_rss_bytes=_peak_rss_bytes(),
        peak_disk_bytes=max(peak_disk, _dir_bytes(output_dir)),
        tables_built=len(schemas) - len(resumed_tables),
        tables_resumed=len(resumed_tables),
        total_rows=sum(table_rows.values()),
        id_mapping_rows=id_mapping_total,
        evidence_rows_written=evidence_written_total,
        evidence_rows_excluded=evidence_excluded_total,
        c1_control_matches=0,
    )
    receipt_payload = {
        "format": "fec-query-build-receipt/1",
        "artifact_digest": artifact.pin.artifact_digest,
        "logical_id": artifact.pin.logical_id,
        "identity_version": IDENTITY_VERSION,
        "receipts_free": True,
        "source_generation_pin": inputs.source_generation_pin,
        "selection_evidence_sha256": inputs.selection_evidence_sha256,
        "table_count": len(table_rows),
        "table_rows": table_rows,
        "partitioned_tables": sorted(k.removesuffix(".parquet") for k in partitioned_spec),
        "id_mapping_path": str(id_mapping_path),
        "refusal_paths": {k: str(v) for k, v in sorted(refusal_paths.items())},
        "disposition_paths": {k: str(v) for k, v in sorted(disposition_paths.items())},
        "telemetry": asdict(telemetry),
    }
    receipt_path = output_dir / "build_receipt.json"
    receipt_path.write_text(json.dumps(receipt_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return FecQueryBuildResult(
        generation_dir=generation_dir,
        artifact_digest=artifact.pin.artifact_digest,
        logical_id=artifact.pin.logical_id,
        table_rows=table_rows,
        partitioned_tables=tuple(sorted(k.removesuffix(".parquet") for k in partitioned_spec)),
        id_mapping_path=id_mapping_path,
        refusal_paths=refusal_paths,
        telemetry=telemetry,
        receipt_path=receipt_path,
        disposition_paths=disposition_paths,
    )


# ---------------------------------------------------------------------------
# Corpus-level collection routing & production orchestrator
# ---------------------------------------------------------------------------

def _extract_cycle_from_url(url: str | None) -> int | None:
    """Extract election cycle matching live ``784b23da`` semantics.

    Presidential_Map (``2024_overall_summary.csv``, ``Contributions_by_Size.csv``,
    ``Contributions_by_3Zip.csv``) and ``data.fec.gov/leadership<YYYY>.csv`` retain
    ``source_cycle = None`` matching live ``784b23da``.
    """
    if not url:
        return None
    path = urlsplit(url).path
    if match := re.search(r"/bulk-downloads/(19[7-9][0-9]|20[0-9]{2})/", path):
        year = int(match[1])
        return year if year % 2 == 0 else None
    if match := re.search(
        r"(?:independent_expenditure_|CommunicationCosts_|ElectioneeringComm_|candidate_summary_|committee_summary_)(19[7-9][0-9]|20[0-9]{2})",
        path,
    ):
        year = int(match[1])
        return year if year % 2 == 0 else None
    if match := re.search(r"(?:indiv|oth|pas2|oppexp|weball|webl|webk|cm|cn|ccl)([0-9]{2})", path.lower()):
        yy = int(match[1])
        year = (1900 + yy) if yy >= 76 else (2000 + yy)
        return year if year % 2 == 0 else None
    return None


def _representation_role_from_url(url: str | None) -> str:
    lower = (url or "").lower()
    if "_delete" in lower or "delete." in lower:
        return "deletion"
    if "_insert" in lower or "insert." in lower:
        return "insertion"
    return "snapshot"


def classify_collection(collection_row: Mapping[str, Any]) -> tuple[str, Any]:
    """Deterministically route one ``fec_collections`` row to its maintained mapper family.

    Returns ``(route_kind, route_detail)`` where ``route_kind`` is one of:
    ``skip``, ``filing_metadata``, ``candidate_api``, ``committee_api``,
    ``api_financial``, ``legal``, ``agency``, ``committee_history``,
    ``filing_fec``, ``filing_physical_text``, ``bulk_header``, ``bulk_receipt``,
    ``bulk_financial``, ``operating_expense``, ``summary``,
    ``contribution_aggregate``, ``registry``.
    """
    profile = collection_row.get("profile")
    cid = str(collection_row.get("collection_id") or "")
    if not profile or collection_row.get("record_outcome") in {
        "retained_unparsed",
        "refused",
        "unresolved",
        "inventory_only",
        "selection_context",
    }:
        return "skip", "disposition"
    if profile in {"bulk", "committee", "candidate"}:
        return "skip", f"inventory_or_census:{profile}"
    if profile == "filing":
        return "filing_metadata", None
    if profile == "legal":
        return "legal", None
    if profile in {"agency", "agency-document"}:
        return "agency", None
    if profile == "postgres":
        return "committee_history", "postgres"

    scope_raw = collection_row.get("requested_scope_json")
    scope = json.loads(scope_raw) if isinstance(scope_raw, str) and scope_raw else {}
    raw_capture = scope.get("capture") if isinstance(scope, dict) else None
    capture: dict[str, Any] = raw_capture if isinstance(raw_capture, dict) else {}
    url = capture.get("requestUrl")
    path = urlsplit(url).path if isinstance(url, str) and url else ""

    if profile == "document":
        if path == "/swagger/":
            return "skip", "swagger"
        if path == "/v1/candidates/":
            return "candidate_api", None
        if path == "/v1/committees/":
            return "committee_api", None
        if path in {"/v1/filings/", "/v1/efile/filings/"} or re.fullmatch(r"/v1/committee/C[0-9]{8}/filings/", path):
            return "filing_metadata", None
        if path in _API_FINANCIAL_BY_ROUTE:
            return "api_financial", _API_FINANCIAL_BY_ROUTE[path]
        if path in {"/v1/legal/search/", "/v1/rulemaking/search/", "/v1/audit-case/"} or re.fullmatch(
            r"/v1/legal/docs/murs/[^/]+/?", path
        ):
            return "legal", None
        raise ValueError(f"Unrouted FEC document collection {cid}: {path!r}")

    if profile == "positional":
        fmt = scope.get("format")
        if fmt == "fec" or (fmt is None and cid.startswith("rows-") and cid.endswith(".fec")):
            return "filing_fec", None
        if cid.endswith("-physical-lines"):
            return "filing_physical_text", None
        lower_path = path.lower()
        if "/data_dictionaries/" in lower_path or lower_path.endswith("readme.txt"):
            return "bulk_header", path
        if re.search(r"/form1filer_\d{4}\.csv\Z", lower_path):
            return "registry", FORM1
        if re.search(r"/form2filer_\d{4}\.csv\Z", lower_path):
            return "registry", FORM2
        if lower_path.endswith("/lobbyist.csv"):
            return "registry", LOBBYIST
        if lower_path.endswith("/falsefictitiousfilings.csv"):
            return "registry", QUALITY_NOTICE
        if re.search(r"/indiv\d{2}(?:_insert|_delete)?\.zip\Z", lower_path):
            return "bulk_receipt", None
        if re.search(r"/oth\d{2}\.zip\Z", lower_path):
            return "bulk_financial", INTERCOMMITTEE
        if re.search(r"/pas2\d{2}\.zip\Z", lower_path):
            return "bulk_financial", CANDIDATE_TRANSACTIONS
        if re.search(r"/oppexp\d{2}\.zip\Z", lower_path):
            return "operating_expense", None
        if re.search(r"/independent_expenditure_\d{4}\.csv\Z", lower_path):
            return "bulk_financial", INDEPENDENT_EXPENDITURES
        if re.search(r"/communicationcosts_\d{4}\.csv\Z", lower_path):
            return "bulk_financial", COMMUNICATION_COSTS
        if re.search(r"/electioneeringcomm_\d{4}\.csv\Z", lower_path) or cid.startswith("rows-ElectioneeringComm_"):
            return "bulk_financial", ELECTIONEERING
        if re.search(r"/candidate_summary_\d{4}\.csv\Z", lower_path):
            return "summary", CANDIDATE_CSV
        if re.search(r"/committee_summary_\d{4}\.csv\Z", lower_path):
            return "summary", COMMITTEE_CSV
        if re.search(r"/web(?:all|l)\d{2}\.zip\Z", lower_path):
            return "summary", CANDIDATE_WEB
        if re.search(r"/webk\d{2}\.zip\Z", lower_path):
            return "summary", COMMITTEE_WEB
        if re.search(r"/\d{4}_overall_summary\.csv\Z", lower_path):
            return "summary", PRESIDENTIAL_OVERALL
        if re.search(r"/leadership\d{4}\.csv\Z", lower_path):
            return "summary", LEADERSHIP
        if lower_path.endswith("/lobbyist_bundle.csv"):
            return "summary", BUNDLING_RECIPIENT
        if lower_path.endswith("/contributions_by_3zip.csv"):
            return "contribution_aggregate", "three-digit-zip"
        if lower_path.endswith("/contributions_by_size.csv"):
            return "contribution_aggregate", "size-band"
        if re.search(r"/cm\d{2}\.zip\Z", lower_path):
            return "committee_history", "positional"
        if re.search(r"/(?:cn|ccl)\d{2}\.zip\Z", lower_path) or cid.startswith("rows-ccl"):
            return "skip", "relationships_only_bulk"
        raise ValueError(f"Unrouted FEC positional collection {cid}: format={fmt!r}, url={url!r}")

    raise ValueError(f"Unsupported FEC collection profile {profile!r} for {cid}")


def _authority_for_collection(collection_row: Mapping[str, Any]) -> str:
    cid = str(collection_row.get("collection_id") or "")
    scope_raw = collection_row.get("requested_scope_json")
    scope = json.loads(scope_raw) if isinstance(scope_raw, str) and scope_raw else {}
    url = (scope.get("capture") or {}).get("requestUrl") or ""
    if cid.startswith("senate-unofficial-"):
        return "unofficial-senate-retained"
    host = urlsplit(url).hostname or ""
    if host == "www.oversight.gov" or host.endswith(".oversight.gov"):
        return "oversight.gov"
    if host == "www.foia.gov" or host.endswith(".foia.gov"):
        return "foia.gov"
    if host in {"www.fec.gov", "beta.fec.gov"}:
        return "fec.gov"
    return "official-fec"


def _selection_for_collection(
    collection_row: Mapping[str, Any],
    *,
    source_generation_pin: str,
    selection_evidence_sha256: str,
    source_sha256: str,
) -> CollectionSelection:
    scope_raw = collection_row.get("requested_scope_json")
    scope = json.loads(scope_raw) if isinstance(scope_raw, str) and scope_raw else {}
    url = (scope.get("capture") or {}).get("requestUrl")
    return CollectionSelection(
        collection_id=collection_row["collection_id"],
        source_sha256=source_sha256,
        source_generation_pin=source_generation_pin,
        source_authority=_authority_for_collection(collection_row),
        source_cycle=_extract_cycle_from_url(url),
        representation_role=_representation_role_from_url(url),
        selection_evidence_sha256=selection_evidence_sha256,
    )


def _duckdb_config(
    memory_limit: str, temp_limit: str, threads: int, temp_dir: str
) -> dict[str, str | bool | int | float | list[str]]:
    return {
        "threads": threads,
        "memory_limit": memory_limit,
        "max_temp_directory_size": temp_limit,
        "temp_directory": temp_dir,
    }


def _iter_collections_records(
    collection_ids: Sequence[str],
    *,
    source_records_path: Path,
    duckdb_memory_limit: str,
    duckdb_temp_limit: str,
    extra_where: str = "",
    sort_keys: Mapping[str, str] | None = None,
    order_by_collection: bool = False,
) -> Iterator[dict[str, Any]]:
    """Stream rows for a group of collections in ONE bounded DuckDB Parquet scan.

    Avoids opening thousands of DuckDB connections across 9,000+ retained collections
    while honoring ``duckdb_memory_limit`` / ``duckdb_temp_limit`` and 4,096-row batches.
    """
    unique_cids = list(dict.fromkeys(collection_ids))
    if not unique_cids:
        return
    with TemporaryDirectory(prefix="fec-q-cols-") as temp_root:
        cfg = _duckdb_config(duckdb_memory_limit, duckdb_temp_limit, 2, temp_root)
        with duckdb.connect(config=cfg) as con:
            if sort_keys is not None:
                wanted = pa.table(
                    {
                        "collection_id": pa.array(unique_cids, type=pa.string()),
                        "sort_key": pa.array([sort_keys.get(cid, "") for cid in unique_cids], type=pa.string()),
                    }
                )
                order_clause = " ORDER BY w.sort_key, r.collection_id"
            else:
                wanted = pa.table({"collection_id": pa.array(unique_cids, type=pa.string())})
                order_clause = " ORDER BY r.collection_id" if order_by_collection else ""
            con.register("_wanted_cids", wanted)
            where_clause = f" WHERE {extra_where}" if extra_where else ""
            reader = con.execute(
                f"SELECT r.* FROM read_parquet(?, hive_partitioning=false) r "
                f"INNER JOIN _wanted_cids w ON r.collection_id = w.collection_id"
                f"{where_clause}{order_clause}",
                [str(source_records_path)],
            ).to_arrow_reader(DEFAULT_BATCH_ROWS)
            for batch in reader:
                yield from batch.to_pylist()


def _iter_collection_records(
    collection_id: str,
    *,
    source_records_path: Path,
    duckdb_memory_limit: str,
    duckdb_temp_limit: str,
) -> Iterator[dict[str, Any]]:
    """Stream all rows of one collection from a full ``fec_source_records.parquet``."""
    yield from _iter_collections_records(
        (collection_id,),
        source_records_path=source_records_path,
        duckdb_memory_limit=duckdb_memory_limit,
        duckdb_temp_limit=duckdb_temp_limit,
    )


def _read_store_blob(store: Any, sha256_value: str, *, expected_bytes: int | None = None) -> bytes:
    """Read raw original bytes from ``CentralRawArchiveStore`` or compatible store with digest/size check."""
    digest = _digest(sha256_value)
    bare = digest.removeprefix("sha256:")
    try:
        ctx = store.open(bare)
    except (FileNotFoundError, KeyError, ValueError):
        ctx = store.open(digest)
    with ctx as stream:
        raw = stream.read()
    if expected_bytes is not None and len(raw) != expected_bytes:
        raise ValueError(f"Retained blob size mismatch for {digest}: expected {expected_bytes}, got {len(raw)}")
    if "sha256:" + hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError(f"Retained blob digest mismatch for {digest}")
    return raw


def discover_retained_mur_captures(
    collections: Sequence[Mapping[str, Any]],
    *,
    store: Any,
) -> list[dict[str, Any]]:
    """Discover retained HTTP-200 ``api_key=<redacted>`` MUR captures from ``fec_collections`` + ``store``.

    Selects disposition/context rows whose ``callerContext.facts.source_capture``
    (or unwrapped ``callerContext.source_capture``) points to
    ``https://api.open.fec.gov/v1/legal/docs/murs/<id>?...api_key=...`` with HTTP 200
    and non-truncated responses, validates EVERY candidate URL through
    :func:`sanitize_redacted_mur_url` before deduplicating by payload SHA-256
    (preferring ``native-context-*`` over ``coverage-capture-*``), and loads
    verified payload bytes from ``store``.
    """
    captures: list[dict[str, Any]] = []
    seen_shas: set[str] = set()

    def _mur_sort_key(c: Mapping[str, Any]) -> tuple[int, str]:
        cid = str(c.get("collection_id") or "")
        return (0 if cid.startswith("native-context-") else 1, cid)

    for col in sorted(collections, key=_mur_sort_key):
        raw_outcome = col.get("collection_outcome_json")
        if not isinstance(raw_outcome, str) or not raw_outcome:
            continue
        outcome = json.loads(raw_outcome)
        if not isinstance(outcome, dict):
            continue
        caller_ctx = (outcome.get("receiverDisposition") or {}).get("callerContext") or outcome.get("callerContext")
        if not isinstance(caller_ctx, dict):
            continue
        facts = caller_ctx.get("facts") if isinstance(caller_ctx.get("facts"), dict) else caller_ctx
        sc = facts.get("source_capture")
        if not isinstance(sc, dict):
            continue
        url = sc.get("url")
        if not isinstance(url, str) or not url:
            continue
        parsed = urlsplit(url)
        if parsed.hostname != "api.open.fec.gov" or not re.fullmatch(r"/v1/legal/docs/murs/[^/]+/?", parsed.path):
            continue
        if not any(k.casefold() == "api_key" for k, _ in parse_qsl(parsed.query, keep_blank_values=True)):
            continue
        statuses = sc.get("http_statuses")
        if not isinstance(statuses, list) or not statuses or not all(s == 200 for s in statuses):
            continue
        truncated = sc.get("truncated")
        if isinstance(truncated, list) and any(t is True for t in truncated):
            continue
        # Fail closed if ANY retained HTTP-200 MUR URL (including duplicate coverage rows) carries a real credential.
        sanitize_redacted_mur_url(url)
        sha = _digest(sc["sha256"])
        if sha in seen_shas:
            continue
        seen_shas.add(sha)
        expected_bytes = sc.get("bytes") if isinstance(sc.get("bytes"), int) else None
        payload_bytes = _read_store_blob(store, sha, expected_bytes=expected_bytes)
        obs_raw = sc.get("observed_at")
        observed_at = obs_raw[0] if isinstance(obs_raw, list) and obs_raw else (obs_raw if isinstance(obs_raw, str) else None)
        captures.append(
            {
                "collection_id": str(col["collection_id"]),
                "source_sha256": sha,
                "source_url": url,
                "observed_at": observed_at,
                "payload_bytes": payload_bytes,
            }
        )
    return captures


def build_fec_query_generation(
    *,
    observations_dir: Path,
    output_dir: Path,
    source_generation_pin: str,
    selection_evidence_sha256: str,
    namespace_evidence: Mapping[str, str],
    store: Any,
    full_source_records_path: Path | None = None,
    individual_snapshot_proof: tuple[bytes, str] | None = None,
    retained_mur_captures: Sequence[dict[str, Any]] | None = None,
    include_rebuilt_tables: bool = True,
    verify_rebuilt_expected_rows: bool = True,
    read_snapshot: Mapping[str, Any] | None = None,
    parents: Mapping[str, Mapping[str, Any]] | None = None,
    partitioned_tables: frozenset[str] = DEFAULT_PARTITIONED_TABLES,
    batch_rows: int = DEFAULT_BATCH_ROWS,
    duckdb_memory_limit: str = DEFAULT_DUCKDB_MEMORY_LIMIT,
    duckdb_temp_limit: str = DEFAULT_DUCKDB_TEMP_LIMIT,
    c1_threads: int = 6,
    resume: bool = True,
    check_resources: Callable[[], None] = lambda: None,
) -> FecQueryBuildResult:
    """Build a complete receipts-free 50-table ``fec-query`` generation from ``fec-observations`` + raw store."""
    from spicy_docs.sources.fec.delimited import held_original, verified_archive_session

    _digest(source_generation_pin)
    _digest(selection_evidence_sha256)
    observations_dir = Path(observations_dir).resolve()
    collections_path = observations_dir / "fec_collections.parquet"
    records_path = Path(full_source_records_path).resolve() if full_source_records_path else (observations_dir / "fec_source_records.parquet")
    if not collections_path.is_file() or not records_path.is_file():
        raise ValueError("observations_dir must contain fec_collections.parquet and fec_source_records.parquet")

    records_schema = pq.read_schema(records_path)
    if "metadata_json" not in records_schema.names:
        raise ValueError(
            "Full fec-query derivation requires metadata_json in fec_source_records.parquet "
            "(supply --full-source-records when observations_dir holds subject-only fec_source_records)"
        )

    collections = [_hydrate_collection_row(col) for col in pq.read_table(collections_path).to_pylist()]
    with TemporaryDirectory(prefix="fec-q-counts-") as temp_root:
        cfg = _duckdb_config(duckdb_memory_limit, duckdb_temp_limit, 2, temp_root)
        with duckdb.connect(config=cfg) as con:
            source_counts = {
                str(cid): int(cnt)
                for cid, cnt in con.execute(
                    "SELECT collection_id, count(*) FROM read_parquet(?, hive_partitioning=false) GROUP BY collection_id",
                    [str(records_path)],
                ).fetchall()
            }

    routed: dict[str, list[tuple[dict[str, Any], Any]]] = defaultdict(list)
    for col in collections:
        kind, detail = classify_collection(col)
        routed[kind].append((col, detail))

    # Load bulk header rows (tiny, 1 row per header collection) in one Parquet scan.
    bulk_headers: dict[str, dict[str, Any]] = {}
    if routed["bulk_header"]:
        for row in _iter_collections_records(
            [col["collection_id"] for col, _ in routed["bulk_header"]],
            source_records_path=records_path,
            duckdb_memory_limit=duckdb_memory_limit,
            duckdb_temp_limit=duckdb_temp_limit,
        ):
            bulk_headers.setdefault(row["collection_id"], row)

    # Load workbook contexts from fec_collections for fec_filing_definitions
    all_def_mappings: tuple[FilingMapping, ...] = (
        *retained_filing_mappings(),
        *remaining_filing_mappings(),
        *filing_cover_mappings(),
        *filing_text_mappings(),
    )
    workbooks = sorted({m.workbook_sha256 for m in all_def_mappings})
    contexts_by_workbook = {
        wb: ctx
        for wb in workbooks
        if (ctx := definition_contexts_from_collections(collections, workbook_sha256=wb))
    }

    # Build archive inner-member proofs (e.g., cm24) directly from store when duplicate cm.txt collections exist.
    # Fail closed on digest/byte mismatch rather than silently dropping a deduplication family.
    archive_inner_proofs: list[tuple[bytes, str]] = []
    cm24_cols = [
        col
        for col, kind in routed["committee_history"]
        if kind == "positional" and _extract_cycle_from_url((json.loads(col.get("requested_scope_json") or "{}").get("capture") or {}).get("requestUrl")) == 2024
    ]
    if len(cm24_cols) >= 2 and store is not None:
        with verified_archive_session():
            archive_map: dict[str, tuple[str, Path | bytes]] = {}
            for col in cm24_cols:
                scope = json.loads(col["requested_scope_json"])
                cap = scope["capture"]
                spool = held_original(store, cap["responseSha256"], cap["byteSize"])
                spool.seek(0)
                archive_map[col["collection_id"]] = (cap["responseSha256"], spool.read())
        canonical_cm24 = next((c["collection_id"] for c in cm24_cols if c["collection_id"] == "bulk-cm24-zip"), sorted(archive_map)[0])
        archive_inner_proofs.append(
            build_archive_inner_member_proof(
                archive_map,
                member_name="cm.txt",
                selected_collection_id=canonical_cm24,
                source_generation_pin=source_generation_pin,
            )
        )

    # Filing headers for fec_filing_header_associations (ONE scan across all filing_fec collections).
    filing_col_by_cid = {col["collection_id"]: col for col, _ in routed["filing_fec"]}
    filing_headers: list[dict[str, Any]] = []
    header_scopes: dict[str, dict[str, Any]] = {}
    header_authorities: dict[str, str] = {}
    header_by_cid: dict[str, dict[str, Any]] = {}
    if filing_col_by_cid:
        for row in _iter_collections_records(
            list(filing_col_by_cid),
            source_records_path=records_path,
            duckdb_memory_limit=duckdb_memory_limit,
            duckdb_temp_limit=duckdb_temp_limit,
            extra_where="json_extract_string(r.metadata_json, '$.kind') = 'header'",
        ):
            cid = row["collection_id"]
            if cid in header_by_cid:
                continue
            col = filing_col_by_cid[cid]
            filing_headers.append(row)
            header_scopes[cid] = json.loads(col["requested_scope_json"])
            header_authorities[cid] = _authority_for_collection(col)
            header_by_cid[cid] = row

    layout_cache: dict[str, dict[str, Any]] = {}

    def _cached_layout(mapping: FilingMapping) -> dict[str, Any]:
        if mapping.layout_pin not in layout_cache:
            layout_cache[mapping.layout_pin] = load_layout(mapping)
        return layout_cache[mapping.layout_pin]

    def _iter_filing_metadata() -> Iterator[tuple[dict[str, Any], CollectionSelection]]:
        by_cid = {col["collection_id"]: col for col, _ in routed["filing_metadata"]}
        for row in _iter_collections_records(
            list(by_cid),
            source_records_path=records_path,
            duckdb_memory_limit=duckdb_memory_limit,
            duckdb_temp_limit=duckdb_temp_limit,
        ):
            col = by_cid[row["collection_id"]]
            sel = _selection_for_collection(
                col,
                source_generation_pin=source_generation_pin,
                selection_evidence_sha256=selection_evidence_sha256,
                source_sha256=row["source_sha256"],
            )
            yield row, sel

    # Deduplicate proven-equivalent individual snapshot date members (e.g. indiv26 by_date/*)
    # from fec_receipts while keeping every member row in fec_collection_selection; fail closed
    # if multiple bulk_receipt collections share an archive responseSha256 without a matching proof.
    excluded_indiv_cids: set[str] = set()
    if individual_snapshot_proof is not None:
        proof_bytes, proof_sha = individual_snapshot_proof
        indiv_selection = individual_snapshot_selection(
            proof_bytes,
            proof_sha256=proof_sha,
            source_generation_pin=source_generation_pin,
            collections=collections,
            source_counts=source_counts,
        )
        excluded_indiv_cids = {
            row["collection_id"]
            for row in indiv_selection
            if row["selection_status"] == "duplicate"
        }
    remaining_bulk_receipt_archives: dict[str, list[str]] = defaultdict(list)
    for col, _ in routed["bulk_receipt"]:
        cid = col["collection_id"]
        if cid in excluded_indiv_cids:
            continue
        sha = (col.get("scope", {}).get("capture") or {}).get("responseSha256")
        if isinstance(sha, str) and sha:
            remaining_bulk_receipt_archives[sha].append(cid)
    unproven_multi_archives = {
        sha: sorted(cids) for sha, cids in remaining_bulk_receipt_archives.items() if len(cids) > 1
    }
    if unproven_multi_archives:
        raise ValueError(
            "Multiple bulk_receipt collections share an archive responseSha256 without a matching "
            f"individual_snapshot_proof ({unproven_multi_archives}); supply individual_snapshot_proof "
            "to exclude proven-equivalent duplicate members from fec_receipts"
        )

    def _iter_bulk_receipts() -> Iterator[tuple[dict[str, Any], CollectionSelection]]:
        by_cid = {
            col["collection_id"]: col
            for col, _ in routed["bulk_receipt"]
            if col["collection_id"] not in excluded_indiv_cids
        }
        for row in _iter_collections_records(
            list(by_cid),
            source_records_path=records_path,
            duckdb_memory_limit=duckdb_memory_limit,
            duckdb_temp_limit=duckdb_temp_limit,
        ):
            native = json.loads(row["metadata_json"])
            if native.get("kind") == "row" and native.get("fields") == []:
                continue  # blank line retained by build_fec_observations
            col = by_cid[row["collection_id"]]
            sel = _selection_for_collection(
                col,
                source_generation_pin=source_generation_pin,
                selection_evidence_sha256=selection_evidence_sha256,
                source_sha256=row["source_sha256"],
            )
            yield row, sel

    def _iter_bulk_financial() -> Iterator[tuple[dict[str, Any], CollectionSelection, BulkMapping]]:
        by_cid = {col["collection_id"]: (col, mapping) for col, mapping in routed["bulk_financial"]}
        for row in _iter_collections_records(
            list(by_cid),
            source_records_path=records_path,
            duckdb_memory_limit=duckdb_memory_limit,
            duckdb_temp_limit=duckdb_temp_limit,
        ):
            native = json.loads(row["metadata_json"])
            if native.get("kind") == "row":
                continue  # header or blank row
            col, mapping = by_cid[row["collection_id"]]
            sel = _selection_for_collection(
                col,
                source_generation_pin=source_generation_pin,
                selection_evidence_sha256=selection_evidence_sha256,
                source_sha256=row["source_sha256"],
            )
            yield row, sel, mapping

    def _iter_oppexp() -> Iterator[tuple[dict[str, Any], CollectionSelection, dict[str, Any]]]:
        if not routed["operating_expense"]:
            return
        opp_hdr = next(
            (row for cid, row in bulk_headers.items() if "oppexp" in cid.lower()),
            None,
        )
        if opp_hdr is None:
            raise ValueError("Operating expense collection lacks its retained oppexp header row")
        by_cid = {col["collection_id"]: col for col, _ in routed["operating_expense"]}
        for row in _iter_collections_records(
            list(by_cid),
            source_records_path=records_path,
            duckdb_memory_limit=duckdb_memory_limit,
            duckdb_temp_limit=duckdb_temp_limit,
        ):
            col = by_cid[row["collection_id"]]
            sel = _selection_for_collection(
                col,
                source_generation_pin=source_generation_pin,
                selection_evidence_sha256=selection_evidence_sha256,
                source_sha256=row["source_sha256"],
            )
            yield row, sel, opp_hdr

    def _iter_api_financial() -> Iterator[tuple[dict[str, Any], CollectionSelection, ApiMapping]]:
        by_cid = {col["collection_id"]: (col, mapping) for col, mapping in routed["api_financial"]}
        for row in _iter_collections_records(
            list(by_cid),
            source_records_path=records_path,
            duckdb_memory_limit=duckdb_memory_limit,
            duckdb_temp_limit=duckdb_temp_limit,
        ):
            wrapped = json.loads(row["metadata_json"])
            if wrapped.get("kind") != "api-record-observation":
                continue
            col, mapping = by_cid[row["collection_id"]]
            sel = _selection_for_collection(
                col,
                source_generation_pin=source_generation_pin,
                selection_evidence_sha256=selection_evidence_sha256,
                source_sha256=row["source_sha256"],
            )
            yield row, sel, mapping

    def _iter_filing_schedules() -> Iterator[tuple[dict[str, Any], CollectionSelection, FilingMapping, dict[str, Any], dict[str, Any]]]:
        active_cids = [cid for cid in filing_col_by_cid if cid in header_by_cid]
        ver_by_cid = {cid: json.loads(header_by_cid[cid]["metadata_json"])["format_version"] for cid in active_cids}
        for row in _iter_collections_records(
            active_cids,
            source_records_path=records_path,
            duckdb_memory_limit=duckdb_memory_limit,
            duckdb_temp_limit=duckdb_temp_limit,
            extra_where="json_extract_string(r.metadata_json, '$.kind') = 'record'",
        ):
            native = json.loads(row["metadata_json"])
            rt = native.get("record_type") or ""
            if rt == "TEXT" or not rt:
                continue
            cid = row["collection_id"]
            mapping = filing_mapping_for(ver_by_cid[cid], rt) or remaining_filing_mapping_for(ver_by_cid[cid], rt)
            if mapping is None:
                continue
            col = filing_col_by_cid[cid]
            sel = _selection_for_collection(
                col,
                source_generation_pin=source_generation_pin,
                selection_evidence_sha256=selection_evidence_sha256,
                source_sha256=row["source_sha256"],
            )
            yield row, sel, mapping, _cached_layout(mapping), header_by_cid[cid]

    def _iter_filing_covers() -> Iterator[tuple[dict[str, Any], CollectionSelection, FilingMapping, dict[str, Any], dict[str, Any]]]:
        active_cids = [cid for cid in filing_col_by_cid if cid in header_by_cid]
        ver_by_cid = {cid: json.loads(header_by_cid[cid]["metadata_json"])["format_version"] for cid in active_cids}
        for row in _iter_collections_records(
            active_cids,
            source_records_path=records_path,
            duckdb_memory_limit=duckdb_memory_limit,
            duckdb_temp_limit=duckdb_temp_limit,
            extra_where="json_extract_string(r.metadata_json, '$.kind') = 'record'",
        ):
            native = json.loads(row["metadata_json"])
            rt = native.get("record_type") or ""
            cid = row["collection_id"]
            mapping = filing_cover_mapping_for(ver_by_cid[cid], rt)
            if mapping is None:
                continue
            col = filing_col_by_cid[cid]
            sel = _selection_for_collection(
                col,
                source_generation_pin=source_generation_pin,
                selection_evidence_sha256=selection_evidence_sha256,
                source_sha256=row["source_sha256"],
            )
            yield row, sel, mapping, _cached_layout(mapping), header_by_cid[cid]

    def _iter_filing_text() -> Iterator[tuple[dict[str, Any], CollectionSelection, dict[str, Any], Any, Any, Any]]:
        active_cids = [cid for cid in filing_col_by_cid if cid in header_by_cid]
        if not active_cids:
            return
        ver_by_cid = {cid: json.loads(header_by_cid[cid]["metadata_json"])["format_version"] for cid in active_cids}
        # Sort by outer capture sha256 so ZIP members share one held spool in ONE Parquet scan!
        sort_keys = {
            cid: str((header_scopes[cid].get("capture") or {}).get("responseSha256", ""))
            for cid in active_cids
        }
        current_cid: str | None = None
        current_reader: Any = None
        with verified_archive_session():
            for row in _iter_collections_records(
                active_cids,
                source_records_path=records_path,
                duckdb_memory_limit=duckdb_memory_limit,
                duckdb_temp_limit=duckdb_temp_limit,
                extra_where=(
                    "(json_extract_string(r.metadata_json, '$.kind') = 'text' "
                    "OR (json_extract_string(r.metadata_json, '$.kind') = 'record' "
                    "AND json_extract_string(r.metadata_json, '$.record_type') = 'TEXT'))"
                ),
                sort_keys=sort_keys,
            ):
                cid = row["collection_id"]
                if cid != current_cid:
                    current_cid = cid
                    current_reader = make_filing_body_reader(store, header_scopes[cid])
                col = filing_col_by_cid[cid]
                hdr = header_by_cid[cid]
                native = json.loads(row["metadata_json"])
                kind = native.get("kind")
                if kind == "text":
                    sel = _selection_for_collection(
                        col,
                        source_generation_pin=source_generation_pin,
                        selection_evidence_sha256=selection_evidence_sha256,
                        source_sha256=row["source_sha256"],
                    )
                    yield row, sel, hdr, current_reader, None, None
                elif kind == "record" and native.get("record_type") == "TEXT":
                    mapping = filing_text_mapping_for(ver_by_cid[cid], "TEXT")
                    if mapping is None:
                        continue
                    sel = _selection_for_collection(
                        col,
                        source_generation_pin=source_generation_pin,
                        selection_evidence_sha256=selection_evidence_sha256,
                        source_sha256=row["source_sha256"],
                    )
                    yield row, sel, hdr, current_reader, mapping, _cached_layout(mapping)

    def _iter_physical_text() -> Iterator[tuple[dict[str, Any], CollectionSelection]]:
        by_cid = {col["collection_id"]: col for col, _ in routed["filing_physical_text"]}
        for row in _iter_collections_records(
            list(by_cid),
            source_records_path=records_path,
            duckdb_memory_limit=duckdb_memory_limit,
            duckdb_temp_limit=duckdb_temp_limit,
        ):
            col = by_cid[row["collection_id"]]
            sel = _selection_for_collection(
                col,
                source_generation_pin=source_generation_pin,
                selection_evidence_sha256=selection_evidence_sha256,
                source_sha256=row["source_sha256"],
            )
            yield row, sel

    def _iter_summaries() -> Iterator[tuple[dict[str, Any], CollectionSelection, SummaryMapping]]:
        by_cid = {col["collection_id"]: (col, mapping) for col, mapping in routed["summary"]}
        for row in _iter_collections_records(
            list(by_cid),
            source_records_path=records_path,
            duckdb_memory_limit=duckdb_memory_limit,
            duckdb_temp_limit=duckdb_temp_limit,
        ):
            native = json.loads(row["metadata_json"])
            if native.get("kind") == "row":
                continue
            col, mapping = by_cid[row["collection_id"]]
            sel = _selection_for_collection(
                col,
                source_generation_pin=source_generation_pin,
                selection_evidence_sha256=selection_evidence_sha256,
                source_sha256=row["source_sha256"],
            )
            yield row, sel, mapping

    def _iter_aggregates() -> Iterator[tuple[dict[str, Any], CollectionSelection, str]]:
        by_cid = {col["collection_id"]: (col, kind) for col, kind in routed["contribution_aggregate"]}
        for row in _iter_collections_records(
            list(by_cid),
            source_records_path=records_path,
            duckdb_memory_limit=duckdb_memory_limit,
            duckdb_temp_limit=duckdb_temp_limit,
        ):
            native = json.loads(row["metadata_json"])
            if native.get("kind") == "row":
                continue
            col, kind = by_cid[row["collection_id"]]
            sel = _selection_for_collection(
                col,
                source_generation_pin=source_generation_pin,
                selection_evidence_sha256=selection_evidence_sha256,
                source_sha256=row["source_sha256"],
            )
            yield row, sel, kind

    def _iter_registry() -> Iterator[tuple[dict[str, Any], CollectionSelection, RegistryMapping]]:
        by_cid = {col["collection_id"]: (col, mapping) for col, mapping in routed["registry"]}
        for row in _iter_collections_records(
            list(by_cid),
            source_records_path=records_path,
            duckdb_memory_limit=duckdb_memory_limit,
            duckdb_temp_limit=duckdb_temp_limit,
        ):
            col, mapping = by_cid[row["collection_id"]]
            sel = _selection_for_collection(
                col,
                source_generation_pin=source_generation_pin,
                selection_evidence_sha256=selection_evidence_sha256,
                source_sha256=row["source_sha256"],
            )
            yield row, sel, mapping

    def _iter_candidate_api() -> Iterator[tuple[dict[str, Any], Any]]:
        prepared_by_cid: dict[str, Any] = {}
        for col, _ in routed["candidate_api"]:
            entry = {
                "collection_id": col["collection_id"],
                "profile": col["profile"],
                "scope": json.loads(col["requested_scope_json"]),
            }
            prepared_by_cid[col["collection_id"]] = prepare_candidate_api(
                entry, source_generation_pin=source_generation_pin
            )
        for row in _iter_collections_records(
            list(prepared_by_cid),
            source_records_path=records_path,
            duckdb_memory_limit=duckdb_memory_limit,
            duckdb_temp_limit=duckdb_temp_limit,
        ):
            yield row, prepared_by_cid[row["collection_id"]]

    def _iter_committee_api() -> Iterator[tuple[dict[str, Any], Any]]:
        prepared_by_cid: dict[str, Any] = {}
        for col, _ in routed["committee_api"]:
            entry = {
                "collection_id": col["collection_id"],
                "profile": col["profile"],
                "source_cycle": None,
                "scope": json.loads(col["requested_scope_json"]),
            }
            prepared_by_cid[col["collection_id"]] = prepare_committee_api(
                entry, source_generation_pin=source_generation_pin
            )
        for row in _iter_collections_records(
            list(prepared_by_cid),
            source_records_path=records_path,
            duckdb_memory_limit=duckdb_memory_limit,
            duckdb_temp_limit=duckdb_temp_limit,
        ):
            yield row, prepared_by_cid[row["collection_id"]]

    def _iter_history() -> Iterator[tuple[dict[str, Any], Any]]:
        cm_hdr = next((row for cid, row in bulk_headers.items() if "cm_header" in cid.lower()), None)
        prepared_by_cid: dict[str, Any] = {}
        for col, kind in routed["committee_history"]:
            outcome = json.loads(col["collection_outcome_json"])
            entry: dict[str, Any] = {
                "collection_id": col["collection_id"],
                "source_family": col["source_family"],
                "profile": col["profile"],
                "scope": json.loads(col["requested_scope_json"]),
            }
            if "tableFieldMapping" in outcome:
                entry["field_mapping"] = outcome["tableFieldMapping"]
            prepared_by_cid[col["collection_id"]] = prepare_history(
                entry,
                source_generation_pin=source_generation_pin,
                header_row=cm_hdr if kind == "positional" else None,
            )
        for row in _iter_collections_records(
            list(prepared_by_cid),
            source_records_path=records_path,
            duckdb_memory_limit=duckdb_memory_limit,
            duckdb_temp_limit=duckdb_temp_limit,
        ):
            yield row, prepared_by_cid[row["collection_id"]]

    def _iter_legal() -> Iterator[tuple[dict[str, Any], CollectionSelection]]:
        by_cid = {col["collection_id"]: col for col, _ in routed["legal"]}
        for row in _iter_collections_records(
            list(by_cid),
            source_records_path=records_path,
            duckdb_memory_limit=duckdb_memory_limit,
            duckdb_temp_limit=duckdb_temp_limit,
        ):
            col = by_cid[row["collection_id"]]
            sel = _selection_for_collection(
                col,
                source_generation_pin=source_generation_pin,
                selection_evidence_sha256=selection_evidence_sha256,
                source_sha256=row["source_sha256"],
            )
            yield row, sel

    def _iter_agency() -> Iterator[tuple[list[dict[str, Any]], AgencySelection]]:
        by_cid = {col["collection_id"]: col for col, _ in routed["agency"]}
        rows_iter = _iter_collections_records(
            list(by_cid),
            source_records_path=records_path,
            duckdb_memory_limit=duckdb_memory_limit,
            duckdb_temp_limit=duckdb_temp_limit,
            order_by_collection=True,
        )
        for cid, group in itertools.groupby(rows_iter, key=lambda r: r["collection_id"]):
            rows = list(group)
            if not rows:
                continue
            col = by_cid[cid]
            sel = AgencySelection(
                collection_id=cid,
                source_sha256=rows[0]["source_sha256"],
                source_generation_pin=source_generation_pin,
                source_authority=_authority_for_collection(col),
                selection_evidence_sha256=selection_evidence_sha256,
            )
            yield rows, sel

    # Build weball/webl & FOIA dual-edition proofs from mapped bounded subsets when present.
    weball_webl_proofs: list[tuple[bytes, str]] = []
    weball_cols = [col for col, m in routed["summary"] if m == CANDIDATE_WEB and "weball" in col["collection_id"].lower()]
    webl_cols = [col for col, m in routed["summary"] if m == CANDIDATE_WEB and "webl" in col["collection_id"].lower()]
    if weball_cols and webl_cols:
        by_cycle_sup = {_extract_cycle_from_url(json.loads(c["requested_scope_json"])["capture"]["requestUrl"]): c for c in weball_cols}
        pairs_data = []
        for sub_col in webl_cols:
            sub_scope = json.loads(sub_col["requested_scope_json"])
            cycle = _extract_cycle_from_url(sub_scope["capture"]["requestUrl"])
            sup_col = by_cycle_sup.get(cycle)
            if sup_col is None:
                continue
            sup_scope = json.loads(sup_col["requested_scope_json"])
            sup_rows = [
                map_summary(
                    r,
                    _selection_for_collection(sup_col, source_generation_pin=source_generation_pin, selection_evidence_sha256=selection_evidence_sha256, source_sha256=r["source_sha256"]),
                    CANDIDATE_WEB,
                )[0]
                for r in _iter_collection_records(sup_col["collection_id"], source_records_path=records_path, duckdb_memory_limit=duckdb_memory_limit, duckdb_temp_limit=duckdb_temp_limit)
                if json.loads(r["metadata_json"]).get("kind") != "row"
            ]
            sub_rows = [
                map_summary(
                    r,
                    _selection_for_collection(sub_col, source_generation_pin=source_generation_pin, selection_evidence_sha256=selection_evidence_sha256, source_sha256=r["source_sha256"]),
                    CANDIDATE_WEB,
                )[0]
                for r in _iter_collection_records(sub_col["collection_id"], source_records_path=records_path, duckdb_memory_limit=duckdb_memory_limit, duckdb_temp_limit=duckdb_temp_limit)
                if json.loads(r["metadata_json"]).get("kind") != "row"
            ]
            pairs_data.append(
                {
                    "superset_collection_id": sup_col["collection_id"],
                    "superset_sha256": sup_scope["capture"]["responseSha256"],
                    "superset_rows": sup_rows,
                    "subset_collection_id": sub_col["collection_id"],
                    "subset_sha256": sub_scope["capture"]["responseSha256"],
                    "subset_rows": sub_rows,
                }
            )
        if pairs_data:
            weball_webl_proofs.append(build_weball_webl_subset_proof(pairs_data, source_generation_pin=source_generation_pin))

    foia_edition_proofs: list[tuple[bytes, str]] = []
    foia_cols = [col for col, _ in routed["agency"] if "foia" in col["collection_id"].lower()]
    if len(foia_cols) >= 2:
        by_fy: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for col in foia_cols:
            rows = list(_iter_collection_records(col["collection_id"], source_records_path=records_path, duckdb_memory_limit=duckdb_memory_limit, duckdb_temp_limit=duckdb_temp_limit))
            if not rows:
                continue
            sel = AgencySelection(col["collection_id"], rows[0]["source_sha256"], source_generation_pin, _authority_for_collection(col), selection_evidence_sha256)
            mapped = map_agency_collection(rows, sel)
            metrics = mapped.tables.get("fec_report_metrics", [])
            reports = mapped.tables.get("fec_agency_reports", [])
            fy = reports[0].get("fiscal_year") if reports else None
            if fy is not None and metrics:
                by_fy[fy].append({"collection_id": col["collection_id"], "source_sha256": rows[0]["source_sha256"], "metrics": metrics})
        for fy, editions in sorted(by_fy.items()):
            primary_editions = [e for e in editions if "archive" not in e["collection_id"]]
            archive_editions = [e for e in editions if "archive" in e["collection_id"]]
            # Only prove dual-edition equivalence for fiscal years declaring both primary & archive mirror editions;
            # fail closed if a declared dual-edition pair's metrics diverge.
            if primary_editions and archive_editions:
                foia_edition_proofs.append(
                    build_foia_dual_edition_proof(
                        editions,
                        selected_collection_id=primary_editions[0]["collection_id"],
                        source_generation_pin=source_generation_pin,
                    )
                )

    resolved_mur_captures: Sequence[dict[str, Any]]
    if retained_mur_captures is None:
        resolved_mur_captures = discover_retained_mur_captures(collections, store=store) if store is not None else ()
    else:
        resolved_mur_captures = retained_mur_captures

    build_inputs = FecQueryBuildInputs(
        source_generation_pin=source_generation_pin,
        selection_evidence_sha256=selection_evidence_sha256,
        collections=collections,
        source_counts=source_counts,
        individual_snapshot_proof=individual_snapshot_proof,
        archive_inner_proofs=tuple(archive_inner_proofs),
        weball_webl_proofs=tuple(weball_webl_proofs),
        foia_edition_proofs=tuple(foia_edition_proofs),
        filing_metadata_rows=_iter_filing_metadata(),
        filing_headers=filing_headers,
        header_scopes=header_scopes,
        header_authorities=header_authorities,
        namespace_evidence=dict(namespace_evidence),
        filing_definition_mappings=all_def_mappings,
        filing_definition_contexts_by_workbook=contexts_by_workbook,
        bulk_receipt_rows=_iter_bulk_receipts(),
        bulk_financial_rows=_iter_bulk_financial(),
        operating_expense_rows=_iter_oppexp(),
        api_financial_rows=_iter_api_financial(),
        filing_schedule_rows=_iter_filing_schedules(),
        filing_cover_rows=_iter_filing_covers(),
        filing_text_rows=_iter_filing_text(),
        filing_physical_text_rows=_iter_physical_text(),
        contribution_aggregate_rows=_iter_aggregates(),
        summary_rows=_iter_summaries(),
        registry_rows=_iter_registry(),
        candidate_api_rows=_iter_candidate_api(),
        committee_api_rows=_iter_committee_api(),
        history_rows=_iter_history(),
        legal_rows=_iter_legal(),
        retained_mur_captures=resolved_mur_captures,
        agency_collections=_iter_agency(),
        rebuilt_tables_store=store if include_rebuilt_tables else None,
        verify_rebuilt_expected_rows=verify_rebuilt_expected_rows,
    )
    return build_fec_query_from_inputs(
        build_inputs,
        output_dir=output_dir,
        read_snapshot=read_snapshot,
        parents=parents,
        partitioned_tables=partitioned_tables,
        batch_rows=batch_rows,
        c1_threads=c1_threads,
        resume=resume,
        check_resources=check_resources,
    )


def _table_parquet_paths(root: Path, table: str) -> list[Path]:
    flat = root / f"{table}.parquet"
    if flat.is_file():
        return [flat]
    directory = root / table
    if directory.is_dir():
        return sorted(directory.rglob("*.parquet"))
    raise FileNotFoundError(f"Table {table} not found under {root}")


def verify_fec_query_parity(
    *,
    prior_dir: Path,
    candidate_dir: Path,
    id_mapping_path: Path,
    tables: Sequence[str] | None = None,
    declared_changed_collections: Sequence[str] = (),
    declared_repaired_tables: Sequence[str] = (),
    ignored_value_columns: Sequence[str] = ("source_generation_pin",),
    expected_source_generation_pin: str | None = None,
    memory_limit: str = DEFAULT_DUCKDB_MEMORY_LIMIT,
    temp_limit: str = DEFAULT_DUCKDB_TEMP_LIMIT,
    threads: int = 4,
    admit_candidate: bool = True,
) -> dict[str, Any]:
    """Verify receipts-free admission, C1-zero, and bidirectional value parity under ``id_mapping.parquet``."""
    prior_dir = Path(prior_dir).resolve()
    candidate_dir = Path(candidate_dir).resolve()
    id_mapping_path = Path(id_mapping_path).resolve()

    artifact_info: dict[str, Any] = {}
    if admit_candidate:
        artifact = admit_generation_for_publication(candidate_dir)
        spec = artifact.root["spec"]
        if "etlReceipts" in spec:
            raise ValueError("Candidate fec-query generation unexpectedly declares etlReceipts")
        if (candidate_dir / "etl_receipts.parquet").exists():
            raise ValueError("Candidate fec-query generation unexpectedly contains etl_receipts.parquet")
        artifact_info = {
            "artifact_digest": artifact.pin.artifact_digest,
            "logical_id": artifact.pin.logical_id,
            "receipts_free": True,
        }

    schemas = load_fec_typed_schemas(verify_processing_parity=True)
    target_tables = tuple(tables) if tables is not None else tuple(sorted(schemas))

    # Full C1 scan across all selected candidate tables
    candidate_targets = [
        (candidate_dir / f"{t}.parquet") if (candidate_dir / f"{t}.parquet").is_file() else (candidate_dir / t)
        for t in target_tables
    ]
    refuse_c1_controls(candidate_targets, threads=threads)

    changed_cids = tuple(sorted(set(declared_changed_collections)))
    repaired_set = set(declared_repaired_tables) | set(REBUILT_FEC_QUERY_TABLES)
    skip_value_cols = {"record_id", "identity_version", *ignored_value_columns}
    table_results: dict[str, Any] = {}

    with TemporaryDirectory(prefix="fec-query-parity-") as temp_root:
        cfg = _duckdb_config(memory_limit, temp_limit, threads, temp_root)
        with duckdb.connect(config=cfg) as con:
            for table in target_tables:
                cand_paths = [str(p) for p in _table_parquet_paths(candidate_dir, table)]
                cand_cols = [f.name for f in pq.read_schema(cand_paths[0])]
                [(cand_rows,)] = con.execute(
                    "SELECT count(*) FROM read_parquet(?, hive_partitioning=false)",
                    [cand_paths],
                ).fetchall()
                if "identity_version" in cand_cols and cand_rows > 0:
                    [(bad_iv,)] = con.execute(
                        "SELECT count(*) FROM read_parquet(?, hive_partitioning=false) "
                        "WHERE identity_version IS DISTINCT FROM ?",
                        [cand_paths, IDENTITY_VERSION],
                    ).fetchall()
                    if bad_iv != 0:
                        raise ValueError(
                            f"Candidate table {table} has {bad_iv} rows whose identity_version != {IDENTITY_VERSION}"
                        )
                if expected_source_generation_pin is not None and "source_generation_pin" in cand_cols and cand_rows > 0:
                    [(bad_pin,)] = con.execute(
                        "SELECT count(*) FROM read_parquet(?, hive_partitioning=false) "
                        "WHERE source_generation_pin IS DISTINCT FROM ?",
                        [cand_paths, expected_source_generation_pin],
                    ).fetchall()
                    if bad_pin != 0:
                        raise ValueError(
                            f"Candidate table {table} has {bad_pin} rows whose source_generation_pin != {expected_source_generation_pin}"
                        )
                if table in repaired_set:
                    table_results[table] = {
                        "status": "declared_repaired_or_rebuilt",
                        "candidate_rows": int(cand_rows),
                    }
                    continue
                prior_exists = (prior_dir / f"{table}.parquet").exists() or (prior_dir / table).is_dir()
                if not prior_exists:
                    if tables is not None:
                        raise FileNotFoundError(f"Prior table {table} not found under {prior_dir}")
                    table_results[table] = {
                        "status": "prior_not_staged",
                        "candidate_rows": int(cand_rows),
                    }
                    continue
                prior_paths = [str(p) for p in _table_parquet_paths(prior_dir, table)]
                prior_cols = [f.name for f in pq.read_schema(prior_paths[0])]
                shared_cols = [c for c in prior_cols if c in cand_cols and c not in skip_value_cols]
                has_record_id = "record_id" in prior_cols and "record_id" in cand_cols
                has_cid = "collection_id" in shared_cols
                p_proj = ", ".join(f'p."{c}"' for c in shared_cols)
                c_proj = ", ".join(f'c."{c}"' for c in shared_cols)

                id_files = [str(p) for p in _id_mapping_files(id_mapping_path)]
                if has_record_id and shared_cols and not id_files:
                    raise FileNotFoundError(f"id_mapping Parquet files not found under {id_mapping_path}")

                def _where_clause(alias: str, ns_filter: tuple[bool, str | None] | None) -> tuple[str, list[Any]]:
                    preds: list[str] = []
                    params: list[Any] = []
                    if has_cid and changed_cids:
                        in_cids = ", ".join("'" + c.replace("'", "''") + "'" for c in changed_cids)
                        preds.append(f"{alias}.collection_id NOT IN ({in_cids})")
                    if ns_filter is not None:
                        is_null, ns_val = ns_filter
                        if is_null:
                            preds.append(f"{alias}.source_namespace IS NULL")
                        else:
                            preds.append(f"{alias}.source_namespace = ?")
                            params.append(ns_val)
                    return (("WHERE " + " AND ".join(preds)) if preds else ""), params

                # Partition large/multi-namespace tables by source_namespace to keep DuckDB temp spill bounded.
                ns_slices: list[tuple[bool, str | None] | None] = [None]
                if "source_namespace" in shared_cols and (
                    (candidate_dir / table).is_dir() or (prior_dir / table).is_dir()
                ):
                    p_where_all, _ = _where_clause("p", None)
                    c_where_all, _ = _where_clause("c", None)
                    p_ns = {
                        row[0]
                        for row in con.execute(
                            f"SELECT DISTINCT p.source_namespace FROM read_parquet(?, hive_partitioning=false) p {p_where_all}",
                            [prior_paths],
                        ).fetchall()
                    }
                    c_ns = {
                        row[0]
                        for row in con.execute(
                            f"SELECT DISTINCT c.source_namespace FROM read_parquet(?, hive_partitioning=false) c {c_where_all}",
                            [cand_paths],
                        ).fetchall()
                    }
                    all_ns = p_ns | c_ns
                    if all_ns:
                        ns_slices = [(ns is None, ns) for ns in sorted(all_ns, key=lambda v: (v is None, v or ""))]

                if has_record_id and shared_cols:
                    [(ambiguous_ids,)] = con.execute(
                        'SELECT count(*) FROM ('
                        'SELECT old_record_id FROM read_parquet(?, hive_partitioning=false) '
                        'WHERE "table" = ? GROUP BY old_record_id HAVING count(DISTINCT new_record_id) > 1)',
                        [id_files, table],
                    ).fetchall()
                    if ambiguous_ids != 0:
                        raise ValueError(
                            f"id_mapping has {ambiguous_ids} ambiguous old_record_id mappings for {table}"
                        )
                    prior_only = 0
                    cand_only = 0
                    for ns_slice in ns_slices:
                        w_p, p_params = _where_clause("p", ns_slice)
                        w_c, c_params = _where_clause("c", ns_slice)
                        [(p_diff,)] = con.execute(
                            'WITH m AS (SELECT DISTINCT old_record_id, new_record_id '
                            'FROM read_parquet(?, hive_partitioning=false) WHERE "table" = ?), '
                            f"p_mapped AS (SELECT COALESCE(m.new_record_id, p.record_id) AS mapped_id, {p_proj} "
                            f"FROM read_parquet(?, hive_partitioning=false) p LEFT JOIN m ON p.record_id = m.old_record_id {w_p}), "
                            f"c_rows AS (SELECT c.record_id AS mapped_id, {c_proj} FROM read_parquet(?, hive_partitioning=false) c {w_c}) "
                            "SELECT count(*) FROM (SELECT * FROM p_mapped EXCEPT ALL SELECT * FROM c_rows)",
                            [id_files, table, prior_paths, *p_params, cand_paths, *c_params],
                        ).fetchall()
                        [(c_diff,)] = con.execute(
                            'WITH m AS (SELECT DISTINCT old_record_id, new_record_id '
                            'FROM read_parquet(?, hive_partitioning=false) WHERE "table" = ?), '
                            f"p_mapped AS (SELECT COALESCE(m.new_record_id, p.record_id) AS mapped_id, {p_proj} "
                            f"FROM read_parquet(?, hive_partitioning=false) p LEFT JOIN m ON p.record_id = m.old_record_id {w_p}), "
                            f"c_rows AS (SELECT c.record_id AS mapped_id, {c_proj} FROM read_parquet(?, hive_partitioning=false) c {w_c}) "
                            "SELECT count(*) FROM (SELECT * FROM c_rows EXCEPT ALL SELECT * FROM p_mapped)",
                            [id_files, table, prior_paths, *p_params, cand_paths, *c_params],
                        ).fetchall()
                        prior_only += int(p_diff)
                        cand_only += int(c_diff)
                else:
                    prior_only = 0
                    cand_only = 0
                    for ns_slice in ns_slices:
                        w_p, p_params = _where_clause("p", ns_slice)
                        w_c, c_params = _where_clause("c", ns_slice)
                        [(p_diff,)] = con.execute(
                            f"SELECT count(*) FROM ("
                            f"SELECT {p_proj} FROM read_parquet(?, hive_partitioning=false) p {w_p} "
                            f"EXCEPT ALL "
                            f"SELECT {c_proj} FROM read_parquet(?, hive_partitioning=false) c {w_c})",
                            [prior_paths, *p_params, cand_paths, *c_params],
                        ).fetchall()
                        [(c_diff,)] = con.execute(
                            f"SELECT count(*) FROM ("
                            f"SELECT {c_proj} FROM read_parquet(?, hive_partitioning=false) c {w_c} "
                            f"EXCEPT ALL "
                            f"SELECT {p_proj} FROM read_parquet(?, hive_partitioning=false) p {w_p})",
                            [cand_paths, *c_params, prior_paths, *p_params],
                        ).fetchall()
                        prior_only += int(p_diff)
                        cand_only += int(c_diff)

                if prior_only != 0 or cand_only != 0:
                    raise ValueError(
                        f"Unintended value parity diff on {table}: prior_only={prior_only}, candidate_only={cand_only}"
                    )
                table_results[table] = {
                    "status": "parity_verified",
                    "candidate_rows": int(cand_rows),
                    "prior_only_rows": int(prior_only),
                    "candidate_only_rows": int(cand_only),
                }

    return {
        "format": "fec-query-parity-report/1",
        "identity_version": IDENTITY_VERSION,
        "candidate": artifact_info,
        "c1_control_matches": 0,
        "tables": table_results,
    }
