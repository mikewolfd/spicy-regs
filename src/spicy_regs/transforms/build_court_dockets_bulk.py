"""Build a complete-population court docket candidate from retained bulk bytes.

The source-owned reader decodes the CSV. The mapper retains docket facts and
publisher blocked/date_blocked status; unavailable party, attorney and firm
lists remain NULL. Source references and raw date conversion inputs become
receipts. Original dates and timestamps remain diagnosable alongside normalized
filing dates. The source must reach its end before selecting the output pair.

This creates a local candidate without a prior search merge or publication.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.transforms.build_courtlistener import CL_BASE_URL, COLUMNS
from spicy_regs.court_receipts import file_witness, finish_court_output

BATCH_ROWS = 50_000

_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])

#: Native columns whose empty string is NULL in the host schema.
_EMPTY_IS_NULL = (
    "case_name_full",
    "date_filed",
    "date_terminated",
    "date_argued",
    "nature_of_suit",
    "cause",
    "jurisdiction_type",
    "jury_demand",
    "assigned_to_str",
    "referred_to_str",
)


def is_apa_nature(nature: str | None) -> bool:
    """Whether a bulk ``nature_of_suit`` text classifies a docket as APA litigation.

    Measured against the retained 7,766-row APA search selection on
    2026-09-22: the publisher spells the 899 nature-of-suit three ways —
    with the numeric code (``899 Other Statutes: Administrative Procedures
    Act/…``), as bare prose without the code (``Administrative Procedure
    Act/Review or Appeal of Agency Decision`` and variants), and under the
    district-remapped codes 2899/3899 (``2899 Other Statutes - APA
    Review/Appeal``, ``2899 Admin Proc Act/Review`` and variants, including
    the publisher's own ``Adminstrative Review Act`` misspelling). All three
    name the same category; the numeric prefix alone misses 3,197 of the
    10,849 bulk rows the full rule classifies. Plain ``2899 Other Statutes``
    rows without any APA wording are not APA and stay excluded.
    """
    if not nature or not nature.strip():
        return False
    text = nature.strip()
    if text.startswith("899"):
        return True
    lowered = text.lower()
    if "administrative procedure" in lowered or "adminstrative review act" in lowered:
        return True
    if ("2899" in text or "3899" in text) and ("apa" in lowered or "admin" in lowered):
        return True
    return False

#: Native columns mapped 1:1 onto host columns.
_DIRECT = (
    ("id", "cl_docket_id"),
    ("case_name", "case_name"),
    ("case_name_full", "case_name_full"),
    ("court_id", "court_id"),
    ("docket_number", "docket_number"),
    ("date_filed", "date_filed"),
    ("date_terminated", "date_terminated"),
    ("date_argued", "date_argued"),
    ("nature_of_suit", "nature_of_suit"),
    ("cause", "cause"),
    ("jurisdiction_type", "jurisdiction_type"),
    ("jury_demand", "jury_demand"),
    ("assigned_to_str", "assigned_to"),
    ("referred_to_str", "referred_to"),
    ("pacer_case_id", "pacer_case_id"),
    ("date_created", "date_created"),
    ("blocked", "blocked"),
    ("date_blocked", "date_blocked"),
)


def _empty_to_null(value: object, *, normalize: bool) -> str | None:
    if value is None:
        return None
    text = str(value)
    if normalize and text == "":
        return None
    return text


def _date_prefix(value: object) -> str | None:
    text = _empty_to_null(value, normalize=False)
    return text[:10] if text is not None else None


def _created_at(value: object) -> str | None:
    text = _empty_to_null(value, normalize=True)
    return text.replace(" ", "T") if text else None


def _absolute_url(row: dict) -> str:
    docket_id = str(row["id"])
    slug = (row.get("slug") or "").strip()
    if slug:
        return f"{CL_BASE_URL}/docket/{docket_id}/{slug}/"
    return f"{CL_BASE_URL}/docket/{docket_id}/"


def shape_bulk_docket(row: dict, courts: dict[str, dict]) -> dict:
    """Map one native bulk docket row onto the host columns."""
    out: dict[str, str | None] = {}
    for native, host in _DIRECT:
        out[host] = _empty_to_null(row.get(native), normalize=False)
    out["date_filed"] = _date_prefix(row.get("date_filed"))
    out["date_terminated"] = _date_prefix(row.get("date_terminated"))
    out["date_argued"] = _date_prefix(row.get("date_argued"))
    out["date_created"] = _created_at(row.get("date_created"))
    court = courts.get(str(row.get("court_id") or "")) or {}
    out["court"] = _empty_to_null(court.get("full_name"), normalize=True)
    out["court_citation_string"] = _empty_to_null(court.get("citation_string"), normalize=True)
    out["parties_json"] = None
    out["attorneys_json"] = None
    out["firms_json"] = None
    out["absolute_url"] = _absolute_url(row)
    out['raw_source_record'] = json.dumps({name: row.get(name) for name in
        ('id', 'date_filed', 'date_terminated', 'date_argued', 'date_created', 'slug')}, ensure_ascii=False)
    return out


def load_courts(*, local_file: Path | None = None, dump_date: date | None = None) -> dict[str, dict]:
    """The courts dump as ``court_id`` → ``{full_name, citation_string}``."""
    from spicy_docs.sources.courtlistener.bulk import CourtListenerBulkReader

    reader = CourtListenerBulkReader("courts", dump_date=dump_date, local_file=local_file)
    courts = {}
    for row in reader.iter_records():
        if row.get("id"):
            courts[str(row["id"])] = {
                "full_name": row.get("full_name"),
                "citation_string": row.get("citation_string"),
            }
    return courts


def build_court_dockets_bulk(
    output_dir: Path,
    *,
    dockets_file: Path,
    courts_file: Path,
    dump_date: date,
    batch_rows: int = BATCH_ROWS,
) -> Path:
    """Stream the bulk dockets dump into a fresh ``court_dockets.parquet`` candidate."""
    from spicy_docs.sources.courtlistener.bulk import CourtListenerBulkReader

    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / f".court-dockets-{uuid4().hex}.source.parquet"
    courts = load_courts(local_file=courts_file, dump_date=dump_date)
    logger.info("Court dockets bulk: {:,} courts loaded", len(courts))

    writer = None
    reader = CourtListenerBulkReader("dockets", dump_date=dump_date, local_file=dockets_file)
    rows_mapped = 0
    missing_courts = 0
    null_case_name = 0
    blocked = 0
    receipts = []
    try:
        batch: list[dict] = []
        for row in reader.iter_records():
            mapped = shape_bulk_docket(row, courts)
            if mapped["court"] is None and mapped["court_id"] is not None:
                missing_courts += 1
            if not mapped["case_name"]:
                null_case_name += 1
            if (row.get("blocked") or "").strip() == "t":
                blocked += 1
            batch.append(mapped)
            if len(batch) >= batch_rows:
                table = pa.Table.from_pylist(batch, schema=_SCHEMA)
                if writer is None:
                    writer = pq.ParquetWriter(out_file, schema=_SCHEMA, compression="zstd")
                writer.write_table(table)
                rows_mapped += len(batch)
                receipts.append({
                    "rows": rows_mapped,
                    "compressed_bytes": out_file.stat().st_size,
                })
                batch = []
        if batch:
            table = pa.Table.from_pylist(batch, schema=_SCHEMA)
            if writer is None:
                writer = pq.ParquetWriter(out_file, schema=_SCHEMA, compression="zstd")
            writer.write_table(table)
            rows_mapped += len(batch)
    finally:
        if writer is not None:
            writer.close()

    if reader.stopped_early:
        raise RuntimeError("dockets dump was not consumed to EOF")
    if writer is None:
        pq.write_table(_SCHEMA.empty_table(), out_file)
    digest = hashlib.sha256()
    with out_file.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    sha256 = digest.hexdigest()
    identity = {
        "source": str(dockets_file),
        "source_bytes": dockets_file.stat().st_size,
        "dump_date": dump_date.isoformat(),
        "reader": {name: getattr(reader, name) for name in ("rows_scanned", "rows_yielded",
                                                           "compressed_bytes", "decompressed_bytes", "stopped_early")},
        "rows_mapped": rows_mapped,
        "missing_courts": missing_courts,
        "null_case_name": null_case_name,
        "blocked": blocked,
        "output": str(out_file),
        "output_bytes": out_file.stat().st_size,
        "sha256": sha256,
        "schema": COLUMNS,
    }
    (output_dir / "court_dockets.parquet.receipt.json").write_text(
        json.dumps(identity, indent=2, sort_keys=True) + "\n")
    logger.info("Court dockets bulk: {:,} rows, {:,} blocked, {:,} missing courts, sha={}",
                rows_mapped, blocked, missing_courts, sha256[:24])
    return finish_court_output('court_dockets', out_file, output_dir,
                               witnesses=[file_witness(dockets_file), file_witness(courts_file), file_witness(out_file)])
