"""Dated FR record keys and explicitly scoped number-only references.

SpicyDocs owns the source key; RefSpec's number-only matter IRIs are a separate
vocabulary decision. No unpadding, case folding or IRI minting happens here.
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

from spicy_regs.ontology.common import JsonReadStats, canonical_json, iter_parquet_rows, parse_json_list


def record_id(row: dict) -> str:
    """Validate literal key fields with their owner and reuse its encoding."""
    from spicy_docs.sources.federal_register.native import classify_document, federal_register_source_record_id

    identity = {name: row.get(name) for name in ("document_number", "publication_date")}
    return federal_register_source_record_id(classify_document(identity))


class FederalRegisterIndex:
    """Resolve references against one held generation, retaining ambiguity."""

    def __init__(self, path: Path):
        self.by_number: dict[str, set[str]] = defaultdict(set)
        for row in iter_parquet_rows(path, columns=("document_number", "publication_date")):
            self.by_number[row["document_number"]].add(record_id(row))

    def reference(self, number: str, publication_date: str | None = None) -> dict:
        candidates = sorted(self.by_number.get(number, ()))
        if publication_date:
            dated = record_id({"document_number": number, "publication_date": publication_date})
            candidates = [dated] if dated in candidates else []
        status = (
            "missing"
            if not candidates
            else "ambiguous"
            if len(candidates) > 1
            else "dated"
            if publication_date
            else "single_candidate_in_input"
        )
        return {
            "document_number": number,
            "publication_date": publication_date,
            "status": status,
            "candidate_ids": candidates,
        }

    def proceeding_ids(self, row: dict, stats: JsonReadStats) -> tuple[set[str], list[dict]]:
        """Read dated keys; migrate old number-only rows only when unambiguous.

        The returned unresolved observations are candidates, never identities.
        Callers can retain them alongside their source proceeding's key.
        """
        column = "fr_document_ids_json" if row.get("fr_document_ids_json") is not None else "fr_document_numbers_json"
        values = (
            parse_json_list(
                row.get(column),
                stats=stats,
                table="proceedings",
                row_id=row.get("proceeding_id"),
                column=column,
            )
            or []
        )
        unresolved = (
            parse_json_list(
                row.get("unresolved_fr_references_json"),
                stats=stats,
                table="proceedings",
                row_id=row.get("proceeding_id"),
                column="unresolved_fr_references_json",
            )
            or []
        )
        if column == "fr_document_ids_json":
            return set(map(str, values)), unresolved
        resolved: set[str] = set()
        for value in values:
            reference = self.reference(str(value))
            if reference["status"] == "single_candidate_in_input":
                resolved.update(reference["candidate_ids"])
            else:
                unresolved.append({"source": "proceedings", "evidence_id": row.get("proceeding_id"), **reference})
        return resolved, unresolved


def references_json(references: list[dict]) -> str:
    """Keep each observation once when predecessors meet again on reruns."""
    by_value = {canonical_json(reference): reference for reference in references}
    return canonical_json([by_value[key] for key in sorted(by_value)])


def resolved_id(reference: dict) -> str | None:
    candidates = reference["candidate_ids"]
    return candidates[0] if len(candidates) == 1 else None


def record_url(identity: str) -> str:
    """Dated publisher route; the number-only /d route can select another date."""
    number, day = identity.rsplit("@", 1)
    record_id({"document_number": number, "publication_date": day})
    return f"https://www.federalregister.gov/documents/{day.replace('-', '/')}/{number}"
