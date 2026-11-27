"""Maintained builder wiring repaired FEC mappers into query-ready Arrow/Parquet tables (ss-u9s8 / ss-42zk).

Connects all repaired consumer mappers into one deterministic, schema-validated
build path without dropping distinct source occurrences or altering source bytes:

- Cross-source registration deduplication (``fec_collection_selection`` via
  ``cross_source_collection_selection``)
- Source-backed filing link target resolution (``resolve_filing_links``)
- Official bulk archive-member & API-URL header associations (``associate_filing_headers``)
- Materialized ``filing_key`` / ``filing_link_status`` on electronic/paper
  schedules, narrative TEXT rows, and bulk operating expenses via ``association_records``
- Repaired operating-expense trailing empty pipe, TEXT body resolution status,
  rollup pseudo-candidate separation, quality-notice candidate typing,
  agency SafeLinks unwrapping & Word table row preservation, and legal AF/audit
  party typing, non-null date events, and strict ``api_key=<redacted>`` MUR
  qualification (keeping ``spicy_docs.sources.fec.catalog.official_url`` intact).
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .fec_agency import SCHEMAS as AGENCY_SCHEMAS, map_agency_collection
from .fec_bulk_financial import OPERATING_EXPENSES, operating_expense
from .fec_bulk_selection import (
    SELECTION_SCHEMA,
    cross_source_collection_selection,
    individual_snapshot_selection,
)
from .fec_filing_associations import (
    ASSOCIATION_SCHEMA,
    associate_filing_headers,
    association_records,
)
from .fec_filing_financial import map_filing_financial
from .fec_filing_forms import FILING_TEXT_SCHEMA, map_filing_text
from .fec_identity_observations import (
    FILINGS,
    LINKS,
    SCHEMAS as IDENTITY_SCHEMAS,
    map_filing_metadata,
    map_registry,
    resolve_filing_links,
)
from .fec_legal import SCHEMAS as LEGAL_SCHEMAS, map_legal, sanitize_redacted_mur_url
from .fec_query import EVIDENCE_SCHEMA, CollectionSelection, _digest, _json
from .fec_summaries import AGGREGATE_SCHEMA, SUMMARY_SCHEMA, map_contribution_aggregate, map_summary


@dataclass
class RepairedFecBuild:
    """In-memory and optional on-disk Parquet outputs from the repaired FEC builder."""

    source_generation_pin: str
    tables: dict[str, list[dict[str, Any]]] = field(default_factory=lambda: defaultdict(list))
    schemas: dict[str, pa.Schema] = field(default_factory=dict)
    evidence: list[dict[str, Any]] = field(default_factory=list)
    paths: dict[str, Path] = field(default_factory=dict)

    def arrow_table(self, name: str) -> pa.Table:
        schema = self.schemas[name]
        rows = self.tables.get(name, [])
        return pa.Table.from_pylist(rows, schema=schema)


def _unify_schema(existing: pa.Schema | None, new_schema: pa.Schema) -> pa.Schema:
    if existing is None:
        return new_schema
    return pa.unify_schemas([existing, new_schema])


def _apply_filing_associations(
    rows: list[dict[str, Any]],
    *,
    table: str,
    schema: pa.Schema,
    filings: list[dict[str, Any]],
    headers: list[dict[str, Any]],
    source_generation_pin: str,
    namespace_evidence: dict[str, str],
) -> list[dict[str, Any]]:
    if not rows:
        return []
    materialized = []
    for row, decision in association_records(
        rows,
        table=table,
        schema=schema,
        filings=filings,
        headers=headers,
        source_generation_pin=source_generation_pin,
        target_generation_pin=None,
        namespace_evidence=namespace_evidence,
        check_resources=lambda: None,
        target_generation_scope="self",
    ):
        updated = dict(row)
        updated["filing_key"] = decision["filing_key"]
        updated["filing_link_status"] = decision["filing_link_status"]
        materialized.append(updated)
    return materialized


def qualify_retained_mur_documents(
    mur_captures: Sequence[dict[str, Any]],
    *,
    source_generation_pin: str,
    selection_evidence_sha256: str,
    source_authority: str = "official-fec",
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    """Qualify and map retained HTTP-200 MUR JSON captures whose URL carries ``api_key=<redacted>``.

    Each capture item requires ``collection_id``, ``source_sha256``, ``source_url``,
    ``payload_bytes`` (raw JSON bytes matching ``source_sha256``), and optional
    ``source_record_id`` / ``observed_at``. Non-``<redacted>`` credential values
    always raise ``ValueError`` via :func:`sanitize_redacted_mur_url`.
    """
    import hashlib

    _digest(source_generation_pin)
    _digest(selection_evidence_sha256)
    combined: dict[str, list[dict[str, Any]]] = {name: [] for name in LEGAL_SCHEMAS}
    evidence: list[dict[str, Any]] = []
    for capture in mur_captures:
        sha = _digest(capture["source_sha256"])
        raw = bytes(capture["payload_bytes"])
        if "sha256:" + hashlib.sha256(raw).hexdigest() != sha:
            raise ValueError(f"Retained MUR payload bytes differ from pinned digest: {capture['collection_id']}")
        sanitized_url, stripped = sanitize_redacted_mur_url(capture["source_url"])
        payload = json.loads(raw)
        docs = payload.get("docs") if isinstance(payload, dict) else None
        if not isinstance(docs, list):
            raise ValueError(f"Retained MUR payload lacks docs array: {capture['collection_id']}")
        selection = CollectionSelection(
            capture["collection_id"],
            sha,
            source_generation_pin,
            source_authority,
            None,
            "snapshot",
            selection_evidence_sha256,
        )
        for index, doc in enumerate(docs):
            rid = capture.get("source_record_id") or f"{sha}/docs/{index:06d}"
            locator = {
                "collection_id": capture["collection_id"],
                "source_record_id": rid,
                "json_pointer": f"/docs/{index}",
                "redacted_api_key_param_stripped": stripped,
            }
            row = {
                "collection_id": capture["collection_id"],
                "source_record_id": rid,
                "source_sha256": sha,
                "source_url": sanitized_url,
                "observed_at": capture.get("observed_at"),
                "profile": "document",
                "source_locator_json": _json(locator),
                "metadata_json": _json({"kind": "api-record-observation", "metadata": doc}),
            }
            tables, ev = map_legal(row, selection)
            for table_name, items in tables.items():
                combined[table_name].extend(items)
            evidence.extend(ev)
    return combined, evidence


def build_repaired_fec_tables(
    *,
    source_generation_pin: str,
    selection_evidence_sha256: str,
    collections: Sequence[dict[str, Any]] = (),
    source_counts: dict[str, int] | None = None,
    individual_snapshot_proof: tuple[bytes, str] | None = None,
    archive_inner_proofs: Sequence[tuple[bytes, str]] = (),
    weball_webl_proofs: Sequence[tuple[bytes, str]] = (),
    foia_edition_proofs: Sequence[tuple[bytes, str]] = (),
    filing_metadata_rows: Sequence[tuple[dict[str, Any], CollectionSelection]] = (),
    filing_headers: Sequence[dict[str, Any]] = (),
    header_scopes: dict[str, dict[str, Any]] | None = None,
    header_authorities: dict[str, str] | None = None,
    namespace_evidence: dict[str, str] | None = None,
    filing_schedule_rows: Sequence[tuple[dict[str, Any], CollectionSelection, Any, Any, dict[str, Any]]] = (),
    filing_text_rows: Sequence[tuple[dict[str, Any], CollectionSelection, dict[str, Any], Any, Any, Any]] = (),
    operating_expense_rows: Sequence[tuple[dict[str, Any], CollectionSelection, dict[str, Any]]] = (),
    contribution_aggregate_rows: Sequence[tuple[dict[str, Any], CollectionSelection, str]] = (),
    summary_rows: Sequence[tuple[dict[str, Any], CollectionSelection, Any]] = (),
    registry_rows: Sequence[tuple[dict[str, Any], CollectionSelection, Any]] = (),
    legal_rows: Sequence[tuple[dict[str, Any], CollectionSelection]] = (),
    retained_mur_captures: Sequence[dict[str, Any]] = (),
    agency_collections: Sequence[tuple[list[dict[str, Any]], Any]] = (),
    output_dir: Path | None = None,
) -> RepairedFecBuild:
    """Run all repaired FEC mappers end-to-end and materialize query-ready tables.

    Returns a :class:`RepairedFecBuild` holding every produced table, schema,
    record-evidence rows, and (when ``output_dir`` is given) written Parquet paths.
    """
    _digest(source_generation_pin)
    _digest(selection_evidence_sha256)
    ns_evidence = dict(namespace_evidence or {})
    build = RepairedFecBuild(source_generation_pin=source_generation_pin)

    # 1. Cross-source registration deduplication + optional individual snapshot selection
    selection_rows: list[dict[str, Any]] = []
    if individual_snapshot_proof is not None:
        proof_bytes, proof_sha = individual_snapshot_proof
        selection_rows.extend(
            individual_snapshot_selection(
                proof_bytes,
                proof_sha256=proof_sha,
                source_generation_pin=source_generation_pin,
                collections=collections,
                source_counts=source_counts or {},
            )
        )
    if collections:
        selection_rows.extend(
            cross_source_collection_selection(
                collections,
                source_generation_pin=source_generation_pin,
                source_counts=source_counts,
                archive_inner_proofs=archive_inner_proofs,
                weball_webl_proofs=weball_webl_proofs,
                foia_edition_proofs=foia_edition_proofs,
            )
        )
    build.schemas["fec_collection_selection"] = SELECTION_SCHEMA
    build.tables["fec_collection_selection"] = selection_rows

    # 2. Filing metadata + source-backed filing links
    filings: list[dict[str, Any]] = []
    raw_links: list[dict[str, Any]] = []
    for row, selection in filing_metadata_rows:
        mapped, ev = map_filing_metadata(row, selection)
        filings.extend(mapped[FILINGS])
        raw_links.extend(mapped[LINKS])
        build.evidence.extend(ev)
    resolved_links = resolve_filing_links(raw_links, filings)
    build.schemas[FILINGS] = IDENTITY_SCHEMAS[FILINGS]
    build.tables[FILINGS] = filings
    build.schemas[LINKS] = IDENTITY_SCHEMAS[LINKS]
    build.tables[LINKS] = resolved_links

    # 3. Header associations (direct API URL + official FEC electronic/paper bulk archive members)
    header_assocs: list[dict[str, Any]] = []
    if filing_headers:
        header_assocs = associate_filing_headers(
            filing_headers,
            scopes=header_scopes or {},
            authorities=header_authorities or {},
            filings=filings,
            source_generation_pin=source_generation_pin,
            selection_evidence_sha256=selection_evidence_sha256,
        )
    build.schemas["fec_filing_header_associations"] = ASSOCIATION_SCHEMA
    build.tables["fec_filing_header_associations"] = header_assocs

    # 4. Electronic/paper filing schedules + materialized filing_key
    schedule_by_table: dict[str, list[dict[str, Any]]] = defaultdict(list)
    schedule_schemas: dict[str, pa.Schema] = {}
    for row, selection, mapping, layout, header_row in filing_schedule_rows:
        mapped_row, ev = map_filing_financial(row, selection, mapping, layout=layout, header_row=header_row)
        table = mapping.fields.table
        schedule_by_table[table].append(mapped_row)
        schedule_schemas[table] = _unify_schema(schedule_schemas.get(table), mapping.schema)
        build.evidence.extend(ev)

    # 5. Bulk operating expenses (repaired empty trailing pipe) + materialized filing_key
    oppexp_mapped: list[dict[str, Any]] = []
    for row, selection, header_row in operating_expense_rows:
        mapped_row, ev = operating_expense(row, selection, header_row)
        oppexp_mapped.append(mapped_row)
        build.evidence.extend(ev)
    if oppexp_mapped:
        schedule_by_table["fec_disbursements"].extend(oppexp_mapped)
        schedule_schemas["fec_disbursements"] = _unify_schema(
            schedule_schemas.get("fec_disbursements"), OPERATING_EXPENSES.schema
        )

    for table, rows in schedule_by_table.items():
        schema = schedule_schemas[table]
        aligned = [{name: item.get(name) for name in schema.names} for item in rows]
        materialized = _apply_filing_associations(
            aligned,
            table=table,
            schema=schema,
            filings=filings,
            headers=header_assocs,
            source_generation_pin=source_generation_pin,
            namespace_evidence=ns_evidence,
        )
        build.schemas[table] = schema
        build.tables[table] = materialized

    # 6. Narrative TEXT rows (repaired body resolution mapping_status) + materialized filing_key
    text_rows: list[dict[str, Any]] = []
    for row, selection, header_row, read_body, mapping, layout in filing_text_rows:
        mapped_row, ev = map_filing_text(
            row, selection, header_row=header_row, read_body=read_body, mapping=mapping, layout=layout
        )
        text_rows.append(mapped_row)
        build.evidence.extend(ev)
    if text_rows:
        build.schemas["fec_filing_text_observations"] = FILING_TEXT_SCHEMA
        build.tables["fec_filing_text_observations"] = _apply_filing_associations(
            text_rows,
            table="fec_filing_text_observations",
            schema=FILING_TEXT_SCHEMA,
            filings=filings,
            headers=header_assocs,
            source_generation_pin=source_generation_pin,
            namespace_evidence=ns_evidence,
        )

    # 7. Contribution aggregates (repaired rollup pseudo-candidate separation)
    if contribution_aggregate_rows:
        build.schemas["fec_contribution_aggregates"] = AGGREGATE_SCHEMA
        for row, selection, kind in contribution_aggregate_rows:
            mapped_row, ev = map_contribution_aggregate(row, selection, kind=kind)
            build.tables["fec_contribution_aggregates"].append(mapped_row)
            build.evidence.extend(ev)

    # 8. Reported financial summaries (preserves weball & webl occurrences; deduped via fec_collection_selection)
    if summary_rows:
        build.schemas["fec_reported_financial_summaries"] = SUMMARY_SCHEMA
        for row, selection, mapping in summary_rows:
            mapped_row, ev = map_summary(row, selection, mapping)
            build.tables["fec_reported_financial_summaries"].append(mapped_row)
            build.evidence.extend(ev)

    # 9. Registry observations (including repaired fec_quality_notices candidate classification)
    for row, selection, mapping in registry_rows:
        mapped_row, ev = map_registry(row, selection, mapping)
        if mapped_row is not None:
            build.schemas[mapping.table] = IDENTITY_SCHEMAS[mapping.table]
            build.tables[mapping.table].append(mapped_row)
            build.evidence.extend(ev)

    # 10. Legal matters, parties, events, documents, findings (+ strict redacted MUR qualification)
    if legal_rows or retained_mur_captures:
        for table_name, schema in LEGAL_SCHEMAS.items():
            build.schemas[table_name] = schema
            build.tables.setdefault(table_name, [])
        for row, selection in legal_rows:
            mapped, ev = map_legal(row, selection)
            for table_name, items in mapped.items():
                build.tables[table_name].extend(items)
            build.evidence.extend(ev)
        if retained_mur_captures:
            mur_tables, mur_ev = qualify_retained_mur_documents(
                retained_mur_captures,
                source_generation_pin=source_generation_pin,
                selection_evidence_sha256=selection_evidence_sha256,
            )
            for table_name, items in mur_tables.items():
                build.tables[table_name].extend(items)
            build.evidence.extend(mur_ev)

    # 11. Agency reports, metrics, text (repaired Word tables), documents (repaired SafeLinks), dispositions
    if agency_collections:
        for table_name, schema in AGENCY_SCHEMAS.items():
            build.schemas[table_name] = schema
            build.tables.setdefault(table_name, [])
        for rows, selection in agency_collections:
            mapped = map_agency_collection(rows, selection)
            for table_name, items in mapped.tables.items():
                build.tables[table_name].extend(items)
            build.evidence.extend(mapped.evidence)

    build.schemas["fec_record_evidence"] = EVIDENCE_SCHEMA
    build.tables["fec_record_evidence"] = build.evidence

    if output_dir is not None:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        for name in sorted(build.schemas):
            path = out / f"{name}.parquet"
            pq.write_table(build.arrow_table(name), path, compression="zstd")
            build.paths[name] = path

    return build


def register_repaired_fec_tables(con, build: RepairedFecBuild) -> list[str]:
    """Register every table in ``build`` inside a DuckDB connection for query verification."""
    registered = []
    for name in sorted(build.schemas):
        con.register(name, build.arrow_table(name))
        registered.append(name)
    return registered
