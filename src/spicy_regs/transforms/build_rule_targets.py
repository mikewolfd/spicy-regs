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
    eastern_day_text,
    iter_parquet_rows,
    parse_json_list,
    write_parquet_rows,
)

from spicy_regs.ontology.federal_register import FederalRegisterIndex, resolved_id, rule_stage

OUTPUT = "rule_targets.parquet"
# v3: labelled FR docket values join (linked_docket_id), zero-padded and en-dash FR numbers
# resolve on their comparison key, and first_seen/last_seen are Eastern days, not instants.
# v4: a docket value naming several dockets joins each (linked_docket_ids), one edge per docket.
# v5: SpicyDocs 0.35.0 reads a docket named after prose (D1) and folds Regulations.gov's typed FR-number separators (D2);
# rulemaking snapshots record no package version, so the actor carries the reader change.
# v6: a docket's own document citing an FR document that is action evidence (a RIN or a rule
# stage) is a typed edge, docket_document_cites_action_notice, with no CFR or RIN target: it
# records the citation decision 32 counts, and unions no proceedings (decision 33 as amended).
ACTOR_ID = "spicy-regs:rule-targets:v6"

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

#: A docket's own document cites, by its fr_doc_num, an FR document that states a RIN or a
#: rule stage. The notice is related to the docket by that citation; it is part of the
#: docket's proceeding only if proceedings lists it there.
CITES_ACTION_NOTICE = "docket_document_cites_action_notice"

SOURCES = frozenset(
    {
        "fr_cfr_ref",
        "docket_rin",
        "document_rin",
        "document_fr_doc",
        CITES_ACTION_NOTICE,
    }
)


def _date_bounds(*days: str | None) -> tuple[str | None, str | None]:
    """The earliest and latest of ISO days (``eastern_day_text``), ignoring absent ones."""
    present = sorted(day for day in days if day)
    return (present[0], present[-1]) if present else (None, None)


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
    fr_index: FederalRegisterIndex | None = None,
) -> Path:
    """Build action-specific docket-to-RIN and docket-to-CFR evidence.

    Unified Agenda values describe an editioned observation of a durable
    agenda item and are deliberately absent here: equality on a RIN does not
    authorize projecting an agenda-level CFR reference onto every docket that
    happens to carry that RIN. ``fr_index`` is the generation's shared index of
    ``federal_register.parquet``; it is built here when not supplied.
    """
    paths = _require_inputs(
        output_dir,
        ("dockets", "documents", "federal_register", "fr_docket_links"),
    )
    context = RunContext.resolve(run_id=run_id, asserted_at=asserted_at, prefix="rule-targets")
    provenance = context.provenance(method="deterministic", actor_id=ACTOR_ID)
    json_stats = JsonReadStats()
    fr_index = fr_index or FederalRegisterIndex(paths["federal_register"])

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
        docket: str,
        citation: CfrCitation | None,
        rin: str | None,
        source: str,
        evidence_id: object,
        first_seen: str | None = None,
        last_seen: str | None = None,
        fr_reference: tuple[str, dict] | None = None,
    ) -> None:
        """Fold one observation into its edge.

        Callers pass a normalized trusted docket, a normalized RIN, Eastern days and the
        FR reference with its canonical form, each computed once per source row: this runs
        once per docket x citation x RIN of every Federal Register row.
        """
        if docket not in trusted_dockets or source not in SOURCES:
            return
        cfr_ref = citation.cfr_ref if citation else None
        key = (docket, cfr_ref, rin, source)
        if fr_reference is not None:
            form, reference = fr_reference
            references.setdefault(key, {})[form] = reference
        evidence = None if evidence_id is None else str(evidence_id)
        existing = edges.get(key)
        if existing is None:
            first, last = _date_bounds(first_seen, last_seen)
            edges[key] = {
                "docket_id": docket,
                "cfr_ref": cfr_ref,
                "cfr_title": citation.title if citation else None,
                "cfr_part": citation.part if citation else None,
                "cfr_section": citation.section if citation else None,
                "rin": rin,
                "source": source,
                "evidence_id": evidence,
                "first_seen": first,
                "last_seen": last,
                **provenance,
            }
            return
        existing["first_seen"], existing["last_seen"] = _date_bounds(
            existing["first_seen"],
            existing["last_seen"],
            first_seen,
            last_seen,
        )
        if evidence and (not existing["evidence_id"] or evidence < existing["evidence_id"]):
            existing["evidence_id"] = evidence

    def with_form(reference: dict) -> tuple[str, dict]:
        return canonical_json(reference), reference

    for row in iter_parquet_rows(paths["dockets"], columns=("docket_id", "rin", "modify_date")):
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        if docket is None:
            continue
        trusted_dockets.add(docket)
        rin = normalize_rin(row.get("rin"))
        if rin:
            modified = eastern_day_text(row.get("modify_date"))
            add_edge(
                docket=docket,
                citation=None,
                rin=rin,
                source="docket_rin",
                evidence_id=docket,
                first_seen=modified,
                last_seen=modified,
            )

    documents_by_fr_doc: dict[str, list[dict]] = defaultdict(list)
    for row in iter_parquet_rows(
        paths["documents"],
        columns=("document_id", "docket_id", "additional_rins", "fr_doc_num", "posted_date", "modify_date"),
    ):
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        if docket is not None:
            # Documents are themselves sourced from Regulations.gov. They can
            # legitimately arrive before the corresponding docket record.
            trusted_dockets.add(docket)
        document_id = row.get("document_id")
        posted = eastern_day_text(row.get("posted_date"))
        modified = eastern_day_text(row.get("modify_date"))
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
                    docket=docket,
                    citation=None,
                    rin=rin,
                    source="document_rin",
                    evidence_id=document_id,
                    first_seen=posted,
                    last_seen=modified or posted,
                )
        fr_doc_num = row.get("fr_doc_num")
        if fr_doc_num and docket is not None:
            reference = {
                "source": "documents.fr_doc_num",
                "evidence_id": document_id,
                **fr_index.reference(str(fr_doc_num)),
            }
            if identity := resolved_id(reference):
                documents_by_fr_doc[identity].append(
                    {
                        "docket": docket,
                        "document_id": document_id,
                        "posted": posted,
                        "modified": modified,
                        "fr_reference": with_form(reference),
                    }
                )
            else:
                add_edge(
                    docket=docket,
                    citation=None,
                    rin=None,
                    source="document_fr_doc",
                    evidence_id=document_id,
                    first_seen=posted,
                    last_seen=modified,
                    fr_reference=with_form(reference),
                )

    # parse_cfr_citation reads only the Register's CFR objects; count anything else it drops.
    unread_cfr: list[object] = []
    linked_dockets_by_fr_doc: dict[str, set[str]] = defaultdict(set)
    for docket, reference in fr_index.docket_links(paths["fr_docket_links"]):
        if docket not in trusted_dockets:
            continue
        if identity := resolved_id(reference):
            linked_dockets_by_fr_doc[identity].add(docket)
        else:
            add_edge(
                docket=docket,
                citation=None,
                rin=None,
                source="fr_cfr_ref",
                evidence_id=docket,
                fr_reference=with_form(reference),
            )

    for row in iter_parquet_rows(
        paths["federal_register"],
        columns=(
            "document_number",
            "publication_date",
            "cfr_references_json",
            "regulation_id_numbers_json",
            "document_type",
            "title",
        ),
    ):
        document_number = row.get("document_number")
        if not document_number:
            continue
        identity = fr_index.record_id(row)
        linked_dockets = linked_dockets_by_fr_doc.get(identity, ())
        documents = documents_by_fr_doc.get(identity, ())
        if not linked_dockets and not documents:
            continue
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
        publication_date = row.get("publication_date")
        published = eastern_day_text(publication_date)
        # Action evidence as proceedings reads it, a RIN or a rule stage, whatever the CFR
        # list holds; the citing documents' own references carry the typed edge.
        if documents and (
            any(normalize_rin(value) for value in raw_rins or ())
            or rule_stage(row.get("document_type"), row.get("title"))
        ):
            for document in documents:
                add_edge(
                    docket=document["docket"],
                    citation=None,
                    rin=None,
                    source=CITES_ACTION_NOTICE,
                    evidence_id=document["document_id"],
                    fr_reference=document["fr_reference"],
                    first_seen=document["posted"] or published,
                    last_seen=document["modified"] or published,
                )
        if raw_cfr is None or raw_rins is None:
            continue
        unread_cfr.extend(raw for raw in raw_cfr if not isinstance(raw, dict))
        citations = list(dict.fromkeys(citation for raw in raw_cfr for citation in parse_cfr_citation(raw)))
        rins = list(dict.fromkeys(rin for value in raw_rins if (rin := normalize_rin(value)))) or [None]

        if linked_dockets and citations:
            # Once per FR row, not per docket x citation x RIN.
            reference = with_form(
                {
                    "source": "federal_register",
                    "evidence_id": identity,
                    **fr_index.reference(str(document_number), publication_date),
                }
            )
            for docket in linked_dockets:
                for citation in citations:
                    for rin in rins:
                        add_edge(
                            docket=docket,
                            citation=citation,
                            rin=rin,
                            source="fr_cfr_ref",
                            evidence_id=identity,
                            fr_reference=reference,
                            first_seen=published,
                            last_seen=published,
                        )

        # A regulations.gov document's frDocNum is independent corroboration of
        # the same target, so it intentionally receives a different source value.
        for document in documents:
            for citation in citations or (None,):
                for rin in rins:
                    add_edge(
                        docket=document["docket"],
                        citation=citation,
                        rin=rin,
                        source="document_fr_doc",
                        evidence_id=document["document_id"],
                        fr_reference=document["fr_reference"],
                        first_seen=document["posted"] or published,
                        last_seen=document["modified"] or published,
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
    if unread_cfr:
        logger.warning(
            "rule_targets: dropped {:,} Federal Register CFR references that are not objects; examples: {}",
            len(unread_cfr),
            "; ".join(repr(raw)[:80] for raw in unread_cfr[:5]),
        )
    logger.info("Rule targets: {:,} rows across {:,} dockets", len(rows), len({r["docket_id"] for r in rows}))
    assert pq.ParquetFile(out_file).schema_arrow.names == list(COLUMNS)
    return out_file
