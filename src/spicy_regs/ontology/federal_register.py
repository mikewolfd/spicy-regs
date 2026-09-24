"""Dated FR record keys, number-only references and the docket an FR link names.

SpicyDocs owns the source key and the comparison key a zero-padded number reduces
to; RefSpec's number-only matter IRIs are a separate vocabulary decision. No case
folding or IRI minting happens here.
"""

from __future__ import annotations

from pathlib import Path

from spicy_regs.ontology.citations import normalize_regsgov_identifier
from spicy_regs.ontology.common import JsonReadStats, canonical_json, iter_parquet_rows, parse_json_list


def record_id(row: dict) -> str:
    """Validate literal key fields with their owner and reuse its encoding."""
    from spicy_docs.sources.federal_register.native import classify_document, federal_register_source_record_id

    identity = {name: row.get(name) for name in ("document_number", "publication_date")}
    return federal_register_source_record_id(classify_document(identity))


def linked_docket_id(value: object) -> str | None:
    """The Regulations.gov docket id a Federal Register docket value names, or ``None``.

    The Register writes most dockets behind a label ("Docket No. SSA-2010-0037"), which
    the syntax-only :func:`normalize_regsgov_identifier` refuses: on the 2026-09-23 parents
    it joined 48,169 of 899,227 link rows to a Regulations.gov docket, and SpicyDocs'
    label-aware reader joins 154,940. That reader refuses a docket its column shape does
    not cover (``GIPSA-2010-FGIS-0014-NONRULEMAKING``, 69 of the source's dockets), so a
    value that is already a literal identifier stays one; no link that joined before stops
    joining. Callers still join only dockets the Regulations.gov records assert.
    """
    # SpicyDocs is the source-readers extra; a base install imports this module without it.
    from spicy_docs.interpretation.identifier_shapes import normalize_docket_reference

    return normalize_docket_reference(value) or normalize_regsgov_identifier(value)


class FederalRegisterIndex:
    """Resolve references against one held generation, retaining ambiguity.

    Built once per generation and shared by every rulemaking stage: each stage used to
    rebuild it (5.2 s, plus a 4.2 s ``record_id`` pass over the same rows).
    """

    def __init__(self, path: Path):
        from spicy_docs.interpretation.identifier_shapes import unpadded_federal_register_document_number

        self._unpadded = unpadded_federal_register_document_number
        # number -> publication_date -> dated record id, as read from the held rows.
        self.by_number: dict[str, dict[str, str]] = {}
        for row in iter_parquet_rows(path, columns=("document_number", "publication_date")):
            self.by_number.setdefault(row["document_number"], {})[row["publication_date"]] = record_id(row)
        # The unpadded comparison key -> the one held number that reduces to it; a key two
        # held numbers reduce to (five 1994-1997 pairs) is refused rather than chosen.
        self._number_by_key: dict[str, str] = {}
        self._colliding_keys: dict[str, list[str]] = {}
        for number in self.by_number:
            key = self._unpadded(number)
            if key is None:
                continue
            held = self._number_by_key.setdefault(key, number)
            if held != number:
                self._colliding_keys.setdefault(key, [held]).append(number)

    def record_id(self, row: dict) -> str:
        """The dated id of a row of the indexed table, validated once when the index was built."""
        held = self.by_number.get(str(row.get("document_number")), {}).get(str(row.get("publication_date")))
        return held if held is not None else record_id(row)

    def _candidates(self, number: str, publication_date: str | None) -> list[str]:
        candidates = sorted(self.by_number.get(number, {}).values())
        if publication_date:
            dated = record_id({"document_number": number, "publication_date": publication_date})
            return [dated] if dated in candidates else []
        return candidates

    def reference(self, number: str, publication_date: str | None = None) -> dict:
        """Classify a number (optionally dated) reference against this generation's held keys.

        The literal number is matched first. Only a number the generation does not hold is
        compared on its unpadded key (the Register pads every number from 2013 and
        Regulations.gov pads older ones too), reducing both sides.

        ``status`` is ``missing`` when neither match finds a record, ``ambiguous`` when the
        reference reaches more than one held record (by one number's several dates, or by
        a key two held numbers share), ``dated`` when the supplied publication date resolves
        it, and ``single_candidate_in_input`` when exactly one candidate is held and no date
        was supplied. A match through the unpadded key is ``unpadded_dated`` or
        ``unpadded_single_candidate_in_input``, so every row says how it resolved.
        """
        method = ""
        if number in self.by_number:
            candidates = self._candidates(number, publication_date)
        elif (key := self._unpadded(number)) is None or key not in self._number_by_key:
            candidates = []
        elif key in self._colliding_keys:
            candidates = sorted(
                identity for held in self._colliding_keys[key] for identity in self.by_number[held].values()
            )
        else:
            method = "unpadded_"
            candidates = self._candidates(self._number_by_key[key], publication_date)
        status = (
            "missing"
            if not candidates
            else "ambiguous"
            if len(candidates) > 1
            else f"{method}dated"
            if publication_date
            else f"{method}single_candidate_in_input"
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
            if identity := resolved_id(reference):
                resolved.add(identity)
            else:
                unresolved.append({"source": "proceedings", "evidence_id": row.get("proceeding_id"), **reference})
        return resolved, unresolved


def references_json(references: list[dict]) -> str:
    """Keep each observation once when predecessors meet again on reruns."""
    by_value = {canonical_json(reference): reference for reference in references}
    return canonical_json([by_value[key] for key in sorted(by_value)])


def resolved_id(reference: dict) -> str | None:
    """The one candidate id, or ``None`` when the reference is missing or ambiguous."""
    candidates = reference["candidate_ids"]
    return candidates[0] if len(candidates) == 1 else None


def record_url(identity: str) -> str:
    """Dated publisher route; the number-only /d route can select another date."""
    number, day = identity.rsplit("@", 1)
    record_id({"document_number": number, "publication_date": day})
    return f"https://www.federalregister.gov/documents/{day.replace('-', '/')}/{number}"
