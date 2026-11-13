"""NPPES literal observation tables and a main-record-only lifecycle candidate.

The source reader owns byte, header, NPI and row validation. The candidate here
replaces whole main rows, never sparse-patches old provider facts. Sidecar update
rules are unqualified: raw sidecar observations are usable, applying them to a
directory refuses. No function selects a shared catalog or publication.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa

from spicy_regs.parquet_rows import write_rows

if TYPE_CHECKING:
    from spicy_docs.sources.nppes import NppesCsv, NppesRow

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


def observation_rows(csv_input):
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


def build_observations(csv_input, destination: Path) -> Path:
    """Atomically write a local candidate table; an excerpt stays labelled excerpt."""
    return write_rows(observation_rows(csv_input), destination, OBSERVATION_SCHEMA)


@dataclass(frozen=True, slots=True)
class MainRelease:
    """An explicitly dated, completely read main CSV, not a full directory."""

    release_id: str
    kind: str
    start: date
    end: date
    predecessor: str | None
    csv_input: NppesCsv

    @property
    def digest(self):
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


def apply_main_release(previous: MainSnapshot | None, release: MainRelease, *, sidecars=()) -> MainSnapshot:
    """Replace monthly state or apply contiguous whole-row weekly replacements.

    This bounded candidate contains main records only. It has no sidecar-removal
    policy and cannot be promoted as a current provider directory. No previous
    snapshot is mutated, including when a late validation refuses.
    """
    if sidecars:
        raise ValueError("NPPES V2 weekly sidecar replacement/removal semantics remain unqualified")
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


def snapshot_rows(snapshot):
    """Yield current main observations with each row's actual supplying input."""
    release_digest = snapshot.release.digest
    for _npi, data, row in snapshot.providers:
        fields = dict(zip(data.columns, row.values, strict=True))
        yield {
            "npi": row.npi,
            "fields": fields,
            "source_sha256": data.sha256,
            "source_member": data.member_name,
            "record_number": row.record_number,
            "main_release_digest": release_digest,
            "product": "main-records-only",
        }
