"""Transform: build ``rule_targets.parquet`` — the normalized docket ↔ CFR ↔ RIN rule-identity spine.

Reads the dockets, documents, federal_register and fr_docket_links parquet inputs from
``output_dir``; a missing input raises FileNotFoundError.
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.ontology.citations import (
    CfrCitation,
    normalize_regsgov_identifier,
    normalize_rin,
    parse_cfr_citation,
)
from spicy_regs.ontology.common import (
    ATTESTATION_COLUMNS,
    JsonReadStats,
    RunContext,
    canonical_json,
    iter_parquet_rows,
    parse_json_list,
    write_parquet_rows,
)

from spicy_regs.ontology.federal_register import FederalRegisterIndex, record_id, resolved_id

OUTPUT = "rule_targets.parquet"
ACTOR_ID = "spicy-regs:rule-targets:v2"

COLUMNS = (
    "docket_id",
    "cfr_ref",
    "cfr_title",
    "cfr_part",
    "cfr_section",
    "rin",
    "source",
    "evidence_id",
    "first_seen",
    "last_seen",
    *ATTESTATION_COLUMNS,
    "fr_references_json",
)

SOURCES = frozenset(
    {
        "fr_cfr_ref",
        "docket_rin",
        "document_rin",
        "document_fr_doc",
    }
)


def _date_bounds(*values: object) -> tuple[str | None, str | None]:
    dates = sorted(str(value) for value in values if value)
    return (dates[0], dates[-1]) if dates else (None, None)


def _require_inputs(output_dir: Path, names: tuple[str, ...]) -> dict[str, Path]:
    paths = {name: output_dir / f"{name}.parquet" for name in names}
    missing = [path.name for path in paths.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"rule_targets inputs missing from {output_dir}: {', '.join(missing)}")
    return paths


def build_rule_targets(
    output_dir: Path,
    *,
    run_id: str | None = None,
    asserted_at: str | None = None,
) -> Path:
    """Build action-specific docket-to-RIN and docket-to-CFR evidence.

    Unified Agenda values describe an editioned observation of a durable
    agenda item and are deliberately absent here: equality on a RIN does not
    authorize projecting an agenda-level CFR reference onto every docket that
    happens to carry that RIN.
    """
    paths = _require_inputs(
        output_dir,
        ("dockets", "documents", "federal_register", "fr_docket_links"),
    )
    context = RunContext.resolve(run_id=run_id, asserted_at=asserted_at, prefix="rule-targets")
    provenance = context.provenance(method="deterministic", actor_id=ACTOR_ID)
    json_stats = JsonReadStats()
    fr_index = FederalRegisterIndex(paths["federal_register"])

    # The table's specified logical key deliberately retains corroborating
    # sources while folding repeated evidence from the same source into a date
    # span. ``evidence_id`` remains one concrete (lexicographically stable)
    # source row that can be inspected.
    edges: dict[tuple[str, str | None, str | None, str], dict] = {}
    # Each edge's FR references by canonical form, serialized once after the walk: re-sorting
    # and re-serializing on every repeat was quadratic in an edge's references and took most
    # of this stage's 8.5 minutes (measured 2026-09-23, 517,311 edges).
    references: dict[tuple[str, str | None, str | None, str], dict[str, dict]] = {}
    trusted_dockets: set[str] = set()

    def add_edge(
        *,
        docket_id: object,
        citation: CfrCitation | None,
        rin: object,
        source: str,
        evidence_id: object,
        first_seen: object = None,
        last_seen: object = None,
        fr_reference: dict | None = None,
    ) -> None:
        docket = normalize_regsgov_identifier(docket_id)
        if docket is None or docket not in trusted_dockets or source not in SOURCES:
            return
        normalized_rin = normalize_rin(rin)
        cfr_ref = citation.cfr_ref if citation else None
        key = (docket, cfr_ref, normalized_rin, source)
        first, last = _date_bounds(first_seen, last_seen)
        candidate = {
            "docket_id": docket,
            "cfr_ref": cfr_ref,
            "cfr_title": citation.title if citation else None,
            "cfr_part": citation.part if citation else None,
            "cfr_section": citation.section if citation else None,
            "rin": normalized_rin,
            "source": source,
            "evidence_id": None if evidence_id is None else str(evidence_id),
            "first_seen": first,
            "last_seen": last,
            **provenance,
        }
        if fr_reference is not None:
            references.setdefault(key, {})[canonical_json(fr_reference)] = fr_reference
        existing = edges.get(key)
        if existing is None:
            edges[key] = candidate
            return
        existing["first_seen"], existing["last_seen"] = _date_bounds(
            existing.get("first_seen"),
            existing.get("last_seen"),
            first,
            last,
        )
        evidence = sorted(value for value in (existing.get("evidence_id"), candidate.get("evidence_id")) if value)
        existing["evidence_id"] = evidence[0] if evidence else None

    for row in iter_parquet_rows(paths["dockets"]):
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        if docket is None:
            continue
        trusted_dockets.add(docket)
        rin = normalize_rin(row.get("rin"))
        if rin:
            add_edge(
                docket_id=docket,
                citation=None,
                rin=rin,
                source="docket_rin",
                evidence_id=docket,
                first_seen=row.get("modify_date"),
                last_seen=row.get("modify_date"),
            )

    documents_by_fr_doc: dict[str, list[dict]] = defaultdict(list)
    for row in iter_parquet_rows(paths["documents"]):
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        if docket is not None:
            # Documents are themselves sourced from Regulations.gov. They can
            # legitimately arrive before the corresponding docket record.
            trusted_dockets.add(docket)
        document_id = row.get("document_id")
        raw_rins = parse_json_list(
            row.get("additional_rins"),
            stats=json_stats,
            table="documents",
            row_id=document_id,
            column="additional_rins",
        )
        if raw_rins is not None:
            for raw_rin in raw_rins:
                rin = normalize_rin(raw_rin)
                if docket is None or not rin:
                    continue
                add_edge(
                    docket_id=docket,
                    citation=None,
                    rin=rin,
                    source="document_rin",
                    evidence_id=document_id,
                    first_seen=row.get("posted_date"),
                    last_seen=row.get("modify_date") or row.get("posted_date"),
                )
        fr_doc_num = row.get("fr_doc_num")
        if fr_doc_num and docket is not None:
            reference = {
                "source": "documents.fr_doc_num",
                "evidence_id": document_id,
                **fr_index.reference(str(fr_doc_num)),
            }
            if identity := resolved_id(reference):
                documents_by_fr_doc[identity].append({**row, "docket_id": docket, "fr_reference": reference})
            else:
                add_edge(
                    docket_id=docket,
                    citation=None,
                    rin=None,
                    source="document_fr_doc",
                    evidence_id=document_id,
                    first_seen=row.get("posted_date"),
                    last_seen=row.get("modify_date"),
                    fr_reference=reference,
                )

    linked_dockets_by_fr_doc: dict[str, set[str]] = defaultdict(set)
    for row in iter_parquet_rows(paths["fr_docket_links"]):
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        if row.get("document_number") and docket in trusted_dockets:
            reference = {
                "source": "fr_docket_links",
                "evidence_id": docket,
                **fr_index.reference(str(row["document_number"]), row.get("publication_date")),
            }
            if identity := resolved_id(reference):
                linked_dockets_by_fr_doc[identity].add(docket)
            else:
                add_edge(
                    docket_id=docket,
                    citation=None,
                    rin=None,
                    source="fr_cfr_ref",
                    evidence_id=docket,
                    fr_reference=reference,
                )

    for row in iter_parquet_rows(paths["federal_register"]):
        document_number = row.get("document_number")
        if not document_number:
            continue
        identity = record_id(row)
        raw_cfr = parse_json_list(
            row.get("cfr_references_json"),
            stats=json_stats,
            table="federal_register",
            row_id=document_number,
            column="cfr_references_json",
        )
        raw_rins = parse_json_list(
            row.get("regulation_id_numbers_json"),
            stats=json_stats,
            table="federal_register",
            row_id=document_number,
            column="regulation_id_numbers_json",
        )
        if raw_cfr is None or raw_rins is None:
            continue
        citations = [citation for raw in raw_cfr for citation in parse_cfr_citation(raw)]
        citations = list(dict.fromkeys(citations))
        rins = list(dict.fromkeys(rin for value in raw_rins if (rin := normalize_rin(value))))
        publication_date = row.get("publication_date")

        linked_dockets = linked_dockets_by_fr_doc.get(identity, set())
        for docket in linked_dockets:
            for citation in citations:
                for rin in rins or (None,):
                    add_edge(
                        docket_id=docket,
                        citation=citation,
                        rin=rin,
                        source="fr_cfr_ref",
                        evidence_id=identity,
                        fr_reference={
                            "source": "federal_register",
                            "evidence_id": identity,
                            **fr_index.reference(str(document_number), publication_date),
                        },
                        first_seen=publication_date,
                        last_seen=publication_date,
                    )

        # A regulations.gov document's frDocNum is independent corroboration of
        # the same target, so it intentionally receives a different source value.
        for document in documents_by_fr_doc.get(identity, ()):
            for citation in citations or (None,):
                for rin in rins or (None,):
                    add_edge(
                        docket_id=document.get("docket_id"),
                        citation=citation,
                        rin=rin,
                        source="document_fr_doc",
                        evidence_id=document.get("document_id"),
                        fr_reference=document["fr_reference"],
                        first_seen=document.get("posted_date") or publication_date,
                        last_seen=document.get("modify_date") or publication_date,
                    )

    for key, row in edges.items():
        held = references.get(key, {})
        row["fr_references_json"] = canonical_json([held[form] for form in sorted(held)])
    rows = sorted(
        edges.values(),
        key=lambda row: (
            row["docket_id"] or "",
            row["cfr_ref"] or "",
            row["rin"] or "",
            row["source"] or "",
        ),
    )
    out_file = write_parquet_rows(output_dir / OUTPUT, columns=COLUMNS, rows=rows)
    json_stats.log("rule_targets")
    logger.info("Rule targets: {:,} rows across {:,} dockets", len(rows), len({r["docket_id"] for r in rows}))
    assert pq.ParquetFile(out_file).schema_arrow.names == list(COLUMNS)
    return out_file
