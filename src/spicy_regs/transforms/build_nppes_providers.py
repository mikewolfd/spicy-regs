"""NPPES V2 literal observation tables, archive/cohort reader, and lifecycle engine.

Maintains both the literal ``nppes_provider_observations`` observation table and
immutable monthly/weekly V2 directory transitions. Every main and sidecar row
retains literal source cells, exact byte coordinates, and cryptographic source
pins. NPIs remain strictly distinct from CMS Certification Numbers (CCNs), and
unmatched join requests are never discarded.

Qualified CMS NPPES V2 semantics (witnessed 2026-11-27 against the November 2026
monthly baseline and three contiguous weekly V2 releases):
- Sparse deactivated main rows disclose only ``NPI`` and ``NPI Deactivation Date``
  (``NPI Deactivation Reason Code`` is not publicly disseminated per V2 README p. 7).
- Reactivated main rows retain their historical ``NPI Deactivation Date`` alongside
  ``NPI Reactivation Date`` (with ``reactivation_date >= deactivation_date``) and
  restore ``Entity Type Code`` (``1`` or ``2``) plus active descriptive fields.
- Weekly sidecar files (``othername``, ``practice_location``, ``endpoint``) emit
  only NPIs present in that week's ``npidata`` main CSV and carry the NPI's full
  current set for that sidecar kind (so omission for an NPI present in the week's
  main CSV represents removal, whereas absence of an NPI from the week's main CSV
  leaves its prior sidecars untouched). Deactivated NPIs continue to appear in
  CMS sidecar exports, so sidecar consumers must check the current main lifecycle
  state rather than treating sidecar presence as active provider status.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
import zipfile
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from spicy_regs.parquet_rows import write_rows

if TYPE_CHECKING:
    from spicy_docs.sources.nppes import NppesCsv, NppesRow

FAMILY = "nppes-providers"
TABLE = "nppes_provider_observations"
OUTPUT = TABLE + ".parquet"
SIDECAR_KINDS = frozenset({"othername", "practice_location", "endpoint"})
ALL_KINDS = frozenset({"main", *SIDECAR_KINDS})
MAIN_LIFECYCLE_COLUMNS = ("NPI", "Entity Type Code", "NPI Deactivation Date", "NPI Reactivation Date")

# Exact byte-pinned V2 fileheader CSV metadata verified across the live November 2026
# monthly full replacement archive and all three November 2026 weekly V2 archives.
V2_FILEHEADER_PINS: dict[str, dict[str, Any]] = {
    "main": {
        "column_count": 330,
        "byte_size": 12267,
        "sha256": "sha256:1f781040d7dae44496be1729250e79114b6dd03f17c10d7d8965486052177679",
    },
    "othername": {
        "column_count": 4,
        "byte_size": 101,
        "sha256": "sha256:131707e64518721fb70e173b0357611e809c987fc5053fdd55e3402fc9a96b58",
    },
    "practice_location": {
        "column_count": 10,
        "byte_size": 578,
        "sha256": "sha256:99a384a4ef38149817c390af29c0c8bf733dec96076c2abf7103214c944bbc93",
    },
    "endpoint": {
        "column_count": 19,
        "byte_size": 431,
        "sha256": "sha256:ce2daf6615e2e30bfadd1adfa5c63399942515abfed9317c52e5371bb3e48de9",
    },
}

OBSERVATION_SCHEMA = pa.schema(
    [
        ("npi", pa.string()),
        ("kind", pa.string()),
        ("scope", pa.string()),
        ("source_url", pa.string()),
        ("source_member", pa.string()),
        ("source_sha256", pa.string()),
        ("source_bytes", pa.int64()),
        ("record_number", pa.int64()),
        ("byte_offset", pa.int64()),
        ("byte_length", pa.int64()),
        ("entity_type_code", pa.string()),
        ("organization_name", pa.string()),
        ("last_name", pa.string()),
        ("first_name", pa.string()),
        ("deactivation_date", pa.string()),
        ("reactivation_date", pa.string()),
        ("fields_json", pa.string()),
    ]
)
SCHEMA = OBSERVATION_SCHEMA
SCHEMAS = {TABLE: SCHEMA}


def _parse_mmddyyyy(value: str, field_name: str) -> date:
    if not re.fullmatch(r"[0-9]{2}/[0-9]{2}/[0-9]{4}", value):
        raise ValueError(f"NPPES {field_name} is not a literal MM/DD/YYYY date")
    try:
        return date(int(value[6:]), int(value[:2]), int(value[3:5]))
    except ValueError as error:
        raise ValueError(f"NPPES {field_name} is not a literal MM/DD/YYYY date") from error


def classify_v2_main_state(fields: Mapping[str, str]) -> str:
    """Classify one NPPES V2 main row's lifecycle state under qualified V2 rules.

    - ``deactivated``: ``NPI Deactivation Date`` set, ``NPI Reactivation Date``
      empty, and no field outside ``NPI`` / ``NPI Deactivation Date`` populated
      (including empty ``NPI Deactivation Reason Code`` per V2 README p. 7).
    - ``reactivated``: ``NPI Reactivation Date`` set with active
      ``Entity Type Code`` (``1`` or ``2``); when historical
      ``NPI Deactivation Date`` is also present (100% of witnessed live V2
      reactivations), requires ``reactivation_date >= deactivation_date``.
    - ``active``: neither lifecycle date set and ``Entity Type Code`` in ``{"1", "2"}``.
    """
    deact_raw = fields["NPI Deactivation Date"]
    react_raw = fields["NPI Reactivation Date"]
    deact_date = _parse_mmddyyyy(deact_raw, "NPI Deactivation Date") if deact_raw else None
    react_date = _parse_mmddyyyy(react_raw, "NPI Reactivation Date") if react_raw else None
    if deact_date is not None and react_date is None:
        if any(value for key, value in fields.items() if key not in {"NPI", "NPI Deactivation Date"}):
            raise ValueError("Deactivated NPPES V2 row discloses fields beyond NPI and deactivation date")
        return "deactivated"
    if fields["Entity Type Code"] not in {"1", "2"}:
        raise ValueError("Active or reactivated NPPES V2 row needs individual or organization Entity Type Code")
    if react_date is not None:
        if deact_date is not None and react_date < deact_date:
            raise ValueError("NPPES V2 NPI Reactivation Date precedes NPI Deactivation Date")
        return "reactivated"
    return "active"


class _PinnedStream:
    """Hash and bound-check a binary CSV stream in one pass."""

    def __init__(self, stream: Any, byte_size: int) -> None:
        self.stream = stream
        self.byte_size = byte_size
        self.size = 0
        self.digest = hashlib.sha256()

    def readline(self, size: int) -> bytes:
        raw = self.stream.readline(min(size, self.byte_size - self.size + 1))
        if not isinstance(raw, bytes):
            raise ValueError("NPPES needs a binary stream")
        self.size += len(raw)
        if self.size > self.byte_size:
            raise ValueError("NPPES input exceeds its pinned byte size")
        self.digest.update(raw)
        return raw


def read_v2_csv_cohort(
    stream: Any,
    *,
    kind: str,
    scope: str,
    source_url: str,
    member_name: str,
    expected_sha256: str,
    expected_bytes: int,
    columns: tuple[str, ...],
    cohort: tuple[str, ...],
    validate_npi: Callable[[str], str],
    max_scanned_rows: int,
    max_selected_rows: int,
    max_record_bytes: int = 1024 * 1024,
) -> NppesCsv:
    """Consume, hash, and validate one pinned NPPES V2 CSV cohort atomically.

    Applies qualified V2 lifecycle validation (accepting combined deactivation
    and reactivation dates where ``reactivation_date >= deactivation_date``)
    while reusing SpicyDocs' positional CSV reader, NPI literal checks, and
    ``NppesCsv``/``NppesRow`` data types. Refuses malformed headers, wrong row
    widths, duplicate main NPIs, out-of-range bytes/rows, and digest mismatches
    without returning any partial result.
    """
    from spicy_docs.sources.fec.delimited import delimited_rows
    from spicy_docs.sources.nppes import NppesCsv, NppesRow, NppesSourceError, literal_npi

    for bound in (expected_bytes, max_scanned_rows, max_selected_rows, max_record_bytes):
        if type(bound) is not int or bound < 1:
            raise NppesSourceError("NPPES byte/row bounds must be positive integers")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", expected_sha256):
        raise NppesSourceError("NPPES requires an exact SHA-256 pin")
    if kind not in ALL_KINDS or scope not in {"member", "excerpt"} or not source_url or not member_name:
        raise NppesSourceError("Declare NPPES kind, member/excerpt scope, URL and member identity")
    if not columns or len(set(columns)) != len(columns) or any(not c for c in columns) or "NPI" not in columns:
        raise NppesSourceError("NPPES needs unique nonempty reviewed columns including NPI")
    if kind == "main" and not set(MAIN_LIFECYCLE_COLUMNS) <= set(columns):
        raise NppesSourceError("NPPES main header is missing lifecycle columns")
    if not cohort or len(set(cohort)) != len(cohort):
        raise NppesSourceError("Declare a nonempty NPPES cohort without duplicates")
    for npi in cohort:
        literal_npi(npi, validate_npi)

    selected = set(cohort)
    seen: set[str] = set()
    rows: list[NppesRow] = []
    scanned = 0
    pinned = _PinnedStream(stream, expected_bytes)
    records = delimited_rows(
        pinned,
        sha256=expected_sha256,
        encoding="utf-8",
        delimiter=",",
        quoting="csv",
        max_record_bytes=max_record_bytes,
    )
    try:
        header = next(records, None)
        if header is None or tuple(header["fields"]) != columns:
            raise NppesSourceError("NPPES CSV header differs from reviewed columns")
        npi_index = columns.index("NPI")
        for record_number, row in enumerate(records, 2):
            scanned += 1
            if scanned > max_scanned_rows:
                raise NppesSourceError("NPPES scan exceeds max_scanned_rows")
            values = row["fields"]
            if len(values) != len(columns):
                raise NppesSourceError(f"NPPES record {record_number} has a malformed field count")
            npi = literal_npi(values[npi_index], validate_npi)
            if npi not in selected:
                continue
            if kind == "main":
                if npi in seen:
                    raise NppesSourceError("NPPES main CSV repeats a cohort NPI")
                seen.add(npi)
                try:
                    classify_v2_main_state(dict(zip(columns, values, strict=True)))
                except ValueError as error:
                    raise NppesSourceError(str(error)) from error
            if len(rows) == max_selected_rows:
                raise NppesSourceError("NPPES cohort exceeds max_selected_rows")
            source = row["source"]
            rows.append(NppesRow(npi, record_number, source["byte_offset"], source["byte_length"], tuple(values)))
    except (csv.Error, UnicodeDecodeError) as error:
        raise NppesSourceError("NPPES CSV has malformed quotes or UTF-8") from error
    if pinned.size != expected_bytes or "sha256:" + pinned.digest.hexdigest() != expected_sha256:
        raise NppesSourceError("NPPES CSV differs from its byte/digest pin")
    return NppesCsv(
        kind, scope, source_url, member_name, expected_sha256, expected_bytes, columns, cohort, scanned, tuple(rows)
    )


def verify_zip_pin(zip_path: Path, *, expected_sha256: str, expected_bytes: int) -> None:
    """Refuse a changed or truncated CMS ZIP archive before reading any member."""
    from spicy_docs.sources.nppes import NppesSourceError

    if type(expected_bytes) is not int or expected_bytes < 1:
        raise NppesSourceError("NPPES ZIP expected_bytes must be a positive integer")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", expected_sha256):
        raise NppesSourceError("NPPES ZIP requires an exact SHA-256 pin")
    digest = hashlib.sha256()
    total = 0
    with Path(zip_path).open("rb") as fh:
        while chunk := fh.read(1024 * 1024):
            total += len(chunk)
            if total > expected_bytes:
                raise NppesSourceError("NPPES ZIP archive exceeds its pinned byte size")
            digest.update(chunk)
    if total != expected_bytes or "sha256:" + digest.hexdigest() != expected_sha256:
        raise NppesSourceError("NPPES ZIP archive differs from its byte/digest pin")


def read_v2_zip_member(
    zip_path: Path,
    *,
    archive_sha256: str,
    archive_bytes: int,
    kind: str,
    scope: str,
    source_url: str,
    member_name: str,
    member_sha256: str,
    member_bytes: int,
    columns: tuple[str, ...],
    cohort: tuple[str, ...],
    validate_npi: Callable[[str], str],
    max_scanned_rows: int,
    max_selected_rows: int,
    max_record_bytes: int = 1024 * 1024,
) -> NppesCsv:
    """Verify a pinned CMS V2 ZIP archive and read one exact CSV member."""
    from spicy_docs.sources.nppes import NppesSourceError

    verify_zip_pin(zip_path, expected_sha256=archive_sha256, expected_bytes=archive_bytes)
    try:
        with zipfile.ZipFile(zip_path) as archive:
            if member_name not in archive.namelist():
                raise NppesSourceError(f"NPPES ZIP archive is missing member {member_name!r}")
            with archive.open(member_name, "r") as stream:
                return read_v2_csv_cohort(
                    stream,
                    kind=kind,
                    scope=scope,
                    source_url=source_url,
                    member_name=member_name,
                    expected_sha256=member_sha256,
                    expected_bytes=member_bytes,
                    columns=columns,
                    cohort=cohort,
                    validate_npi=validate_npi,
                    max_scanned_rows=max_scanned_rows,
                    max_selected_rows=max_selected_rows,
                    max_record_bytes=max_record_bytes,
                )
    except zipfile.BadZipFile as error:
        raise NppesSourceError("NPPES archive is not a valid ZIP file") from error


def observation_rows(csv_input: NppesCsv) -> Iterable[dict[str, Any]]:
    """Retain every selected occurrence, original strings, and source coordinates."""
    for row in csv_input.rows:
        fields = dict(zip(csv_input.columns, row.values, strict=True))
        yield {
            "npi": row.npi,
            "kind": csv_input.kind,
            "scope": csv_input.scope,
            "source_url": csv_input.source_url,
            "source_member": csv_input.member_name,
            "source_sha256": csv_input.sha256,
            "source_bytes": csv_input.byte_size,
            "record_number": row.record_number,
            "byte_offset": row.byte_offset,
            "byte_length": row.byte_length,
            "entity_type_code": fields.get("Entity Type Code"),
            "organization_name": fields.get("Provider Organization Name (Legal Business Name)"),
            "last_name": fields.get("Provider Last Name (Legal Name)"),
            "first_name": fields.get("Provider First Name"),
            "deactivation_date": fields.get("NPI Deactivation Date"),
            "reactivation_date": fields.get("NPI Reactivation Date"),
            "fields_json": json.dumps(fields, ensure_ascii=False, separators=(",", ":")),
        }


def build_observations(csv_input: NppesCsv, destination: Path) -> Path:
    """Atomically write a local candidate table; an excerpt stays labelled excerpt."""
    return write_rows(observation_rows(csv_input), destination, OBSERVATION_SCHEMA)


def build_observations_multi(csv_inputs: Iterable[NppesCsv], destination: Path) -> Path:
    """Atomically write observations from one or more pinned NPPES CSV inputs.

    Refuses empty input lists and duplicate ``(source_sha256, source_member, record_number)``
    identities before replacing ``destination``.
    """
    inputs = tuple(csv_inputs)
    if not inputs:
        raise ValueError("Declare at least one pinned NPPES CSV input")

    def rows() -> Iterable[dict[str, Any]]:
        seen_ids: set[tuple[str, str, int]] = set()
        for csv_input in inputs:
            for item in observation_rows(csv_input):
                identity = (item["source_sha256"], item["source_member"], item["record_number"])
                if identity in seen_ids:
                    raise ValueError(f"Duplicate NPPES observation identity: {identity}")
                seen_ids.add(identity)
                yield item

    return write_rows(rows(), destination, OBSERVATION_SCHEMA)


@dataclass(frozen=True, slots=True)
class MainRelease:
    """An explicitly dated, completely read main CSV."""

    release_id: str
    kind: str
    start: date
    end: date
    predecessor: str | None
    csv_input: NppesCsv

    @property
    def digest(self) -> str:
        value = [
            self.release_id,
            self.kind,
            self.start.isoformat(),
            self.end.isoformat(),
            self.predecessor,
            self.csv_input.sha256,
            self.csv_input.source_url,
            self.csv_input.member_name,
            self.csv_input.cohort,
            self.csv_input.columns,
        ]
        return "sha256:" + hashlib.sha256(json.dumps(value, separators=(",", ":")).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class MainSnapshot:
    release: MainRelease
    # Rows retain their own CSV source, including unchanged providers from an older input.
    providers: tuple[tuple[str, NppesCsv, NppesRow], ...]


def apply_main_release(previous: MainSnapshot | None, release: MainRelease, *, sidecars: tuple[Any, ...] = ()) -> MainSnapshot:
    """Replace monthly state or apply contiguous whole-row weekly replacements.

    This main-only transition refuses any sidecar inputs (use :func:`apply_v2_release`
    for qualified V2 main + sidecar transitions). No previous snapshot is mutated,
    including when a late validation refuses.
    """
    if sidecars:
        raise ValueError("Main-only transition refuses sidecars; use apply_v2_release for qualified V2 sidecars")
    data = release.csv_input
    if data.kind != "main" or data.scope != "member":
        raise ValueError("NPPES lifecycle needs a complete main member; excerpt is changes only")
    if release.kind not in {"monthly", "weekly"} or not release.release_id or release.start > release.end:
        raise ValueError("Declare NPPES release identity, kind and data period")
    if previous is not None and data.cohort != previous.release.csv_input.cohort:
        raise ValueError("NPPES lifecycle cohort changed")
    if release.kind == "weekly":
        if previous is None or release.predecessor != previous.release.digest:
            raise ValueError("NPPES weekly update has a missing or different predecessor")
        if release.start != previous.release.end + timedelta(days=1):
            raise ValueError("NPPES weekly sequence has a gap or overlap")
        if data.columns != previous.release.csv_input.columns:
            raise ValueError("NPPES main layout changed across releases")
        providers = {npi: (source, row) for npi, source, row in previous.providers}
    else:
        if release.predecessor is not None:
            raise ValueError("NPPES monthly replacement must not claim incremental predecessor")
        if previous is not None and release.end <= previous.release.end:
            raise ValueError("NPPES monthly replacement does not advance the data period")
        providers = {}
    # Every update replaces the WHOLE row. A sparse deactivation therefore clears
    # old names, addresses and entity type without erasing its historical source.
    for row in data.rows:
        providers[row.npi] = (data, row)
    return MainSnapshot(
        release, tuple((npi, providers[npi][0], providers[npi][1]) for npi in data.cohort if npi in providers)
    )


@dataclass(frozen=True, slots=True)
class V2Snapshot:
    """Immutable NPPES V2 directory snapshot covering main records and qualified sidecars."""

    main: MainSnapshot
    # Mapping of sidecar kind -> tuple of (npi, supplying NppesCsv, NppesRow) in cohort/source order.
    sidecars: tuple[tuple[str, tuple[tuple[str, NppesCsv, NppesRow], ...]], ...]


def apply_v2_release(
    previous: V2Snapshot | None,
    release: MainRelease,
    *,
    sidecars: tuple[NppesCsv, ...] = (),
) -> V2Snapshot:
    """Apply a qualified NPPES V2 monthly or weekly main + sidecar release atomically.

    Sidecar replacement rules (witnessed across three consecutive November 2026
    live V2 weekly archives):
    - Every sidecar input must be a complete ``member`` ``NppesCsv`` of a distinct
      kind in ``{"othername", "practice_location", "endpoint"}`` matching the
      main release's cohort.
    - On a weekly release, every sidecar row's NPI must appear in that week's
      ``main`` CSV (100% ``in_week_main`` invariant); orphan weekly sidecar rows
      refuse atomically.
    - For NPIs present in a weekly ``main`` CSV, each supplied sidecar kind
      replaces that NPI's prior sidecar rows in full (so zero rows for that NPI
      in the week's sidecar file removes its prior rows for that kind).
    - For NPIs absent from a weekly ``main`` CSV, prior sidecar rows carry
      forward untouched (absence from a weekly file is never deletion).
    - On a monthly replacement, all sidecar sets reset to the monthly release.
    """
    from spicy_docs.sources.nppes import NppesCsv

    next_main = apply_main_release(previous.main if previous is not None else None, release)
    seen_kinds: set[str] = set()
    by_kind_input: dict[str, NppesCsv] = {}
    for sc in sidecars:
        if not isinstance(sc, NppesCsv):
            raise ValueError("NPPES V2 sidecars must be pinned NppesCsv inputs")
        if sc.kind not in SIDECAR_KINDS or sc.scope != "member":
            raise ValueError("NPPES V2 sidecars require complete member othername, practice_location, or endpoint CSVs")
        if sc.kind in seen_kinds:
            raise ValueError(f"Duplicate NPPES V2 sidecar kind in one release: {sc.kind}")
        if sc.cohort != release.csv_input.cohort:
            raise ValueError("NPPES V2 sidecar cohort differs from main release cohort")
        seen_kinds.add(sc.kind)
        by_kind_input[sc.kind] = sc

    touched_npis = {row.npi for row in release.csv_input.rows}
    prior_map: dict[str, dict[str, list[tuple[NppesCsv, NppesRow]]]] = {}
    if release.kind == "weekly" and previous is not None:
        for kind, entries in previous.sidecars:
            per_npi: dict[str, list[tuple[NppesCsv, NppesRow]]] = {}
            for npi, src, row in entries:
                per_npi.setdefault(npi, []).append((src, row))
            prior_map[kind] = per_npi

    all_kinds = sorted(set(prior_map) | set(by_kind_input))
    next_sidecars: list[tuple[str, tuple[tuple[str, NppesCsv, NppesRow], ...]]] = []
    for kind in all_kinds:
        if kind in by_kind_input:
            sc_csv = by_kind_input[kind]
            if release.kind == "weekly":
                for row in sc_csv.rows:
                    if row.npi not in touched_npis:
                        raise ValueError(
                            f"NPPES weekly {kind} sidecar references NPI {row.npi} absent from week's main CSV"
                        )
            per_npi = {
                npi: list(items)
                for npi, items in prior_map.get(kind, {}).items()
                if release.kind == "weekly" and npi not in touched_npis
            }
            for row in sc_csv.rows:
                per_npi.setdefault(row.npi, []).append((sc_csv, row))
        else:
            per_npi = prior_map.get(kind, {})

        ordered: list[tuple[str, NppesCsv, NppesRow]] = []
        for npi in release.csv_input.cohort:
            for src, row in per_npi.get(npi, ()):
                ordered.append((npi, src, row))
        next_sidecars.append((kind, tuple(ordered)))

    return V2Snapshot(next_main, tuple(next_sidecars))


def snapshot_rows(snapshot: MainSnapshot | V2Snapshot) -> Iterable[dict[str, Any]]:
    """Yield current main observations with each row's actual supplying input."""
    main_snapshot = snapshot.main if isinstance(snapshot, V2Snapshot) else snapshot
    release_digest = main_snapshot.release.digest
    for _npi, data, row in main_snapshot.providers:
        fields = dict(zip(data.columns, row.values, strict=True))
        yield {
            "npi": row.npi,
            "fields": fields,
            "lifecycle_state": classify_v2_main_state(fields),
            "source_sha256": data.sha256,
            "source_member": data.member_name,
            "record_number": row.record_number,
            "main_release_digest": release_digest,
            "product": "main-records-only",
        }


def v2_snapshot_sidecar_rows(snapshot: V2Snapshot) -> Iterable[dict[str, Any]]:
    """Yield current V2 sidecar rows paired with each NPI's current main lifecycle state.

    CMS continues emitting historical sidecar rows for deactivated NPIs; pairing
    each sidecar row with ``main_lifecycle_state`` and ``main_deactivation_date``
    prevents deactivated providers' sidecars from masquerading as active facts.
    """
    main_states: dict[str, tuple[str, str]] = {}
    for npi, data, row in snapshot.main.providers:
        fields = dict(zip(data.columns, row.values, strict=True))
        main_states[npi] = (classify_v2_main_state(fields), fields["NPI Deactivation Date"])
    release_digest = snapshot.main.release.digest
    for kind, entries in snapshot.sidecars:
        for npi, data, row in entries:
            fields = dict(zip(data.columns, row.values, strict=True))
            state, deact_date = main_states.get(npi, ("absent_from_main", ""))
            yield {
                "npi": npi,
                "kind": kind,
                "fields": fields,
                "main_lifecycle_state": state,
                "main_deactivation_date": deact_date,
                "source_sha256": data.sha256,
                "source_member": data.member_name,
                "record_number": row.record_number,
                "main_release_digest": release_digest,
            }
