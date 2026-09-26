"""Dated FR record keys, number-only references, the dockets an FR link names, a document's rule stage and the catch-all dockets.

SpicyDocs owns the source key and the comparison key a reference number reduces to
(dashes and case folded, the sequence's zero padding removed); RefSpec's number-only
matter IRIs are a separate vocabulary decision. No IRI minting happens here.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from pathlib import Path

from spicy_regs.ontology.common import JsonReadStats, canonical_json, iter_parquet_rows, parse_json_list


def record_id(row: dict) -> str:
    """Validate literal key fields with their owner and reuse its encoding."""
    from spicy_docs.sources.federal_register.native import classify_document, federal_register_source_record_id

    identity = {name: row.get(name) for name in ("document_number", "publication_date")}
    return federal_register_source_record_id(classify_document(identity))


def linked_docket_ids(value: object) -> tuple[str, ...]:
    """Every Regulations.gov docket id a Federal Register docket value names, in order; ``()`` for none.

    The Register writes most dockets behind a label ("Docket No. SSA-2010-0037"), which
    the syntax-only :func:`~spicy_regs.ontology.citations.normalize_regsgov_identifier`
    refuses: on the 2026-09-23 parents it joined 48,169 of 899,227 link rows to a
    Regulations.gov docket, and SpicyDocs' label-aware single reader 154,941. That reader
    keeps a ``-RULE``-family suffix and returns every held docket id but ``GSA-NA-2005``,
    which states no sequence and is no link value.

    SpicyDocs' plural reader answers the single reader's docket alone whenever there is
    one, and otherwise reads a longer label, a note after the docket, or a list ("Docket
    Nos. X and Y"). On the same parents it joins 160,397 link rows, 817 of them to several
    dockets, and loses no pair the single reader joined. Since 0.35.0 it also reads a
    docket named after prose ("Public Notice: X", "FAR Case 2017-014, Docket No. X"; owner
    ruling D1 of the 2026-09-26 drift audit), but not a former one ("formerly X") or a
    number behind another system's label ("File No. SR-…"): on the audit's parents that
    reads 432 of the 442 held pairs the prose rule missed, 169 of them from action
    documents (receipt ``drift-qualification-2026-09-26/regulatory/d1d2-fix/``). It reads
    shape, not existence, so callers join only the dockets Regulations.gov asserts.
    """
    # SpicyDocs is the source-readers extra; a base install imports this module without it.
    from spicy_docs.interpretation.identifier_shapes import normalize_docket_references

    return normalize_docket_references(value)


def rule_stage(document_type: object, title: object) -> str | None:
    """The rule stage a document's type and title state, or ``None``.

    Read the same way off a Federal Register row and a Regulations.gov document; a stage
    makes either one action evidence (fork delivery decisions 32 and 33), as a RIN does.
    """
    kind = str(document_type or "").casefold()
    text = f"{kind} {str(title or '').casefold()}"
    if "withdraw" in text:
        return "withdrawn"
    if "supplement" in text and ("proposed" in text or "proposal" in text):
        return "supplemental"
    if kind == "rule" or "final rule" in text:
        return "final"
    if kind == "proposed rule" or "proposed rule" in text:
        return "proposed"
    return None


#: Regulations.gov's Federal Register feed dockets, one per agency (``EPA_FRDOC_0001``).
_CATCH_ALL_DOCKET = re.compile(r"[A-Z0-9]+_FRDOC_\d{4}")


def catch_all_docket(docket: str) -> bool:
    """Whether a normalized Regulations.gov docket id is an agency's Federal Register feed docket.

    A feed docket holds the FR documents of that agency's rulemakings, so none of its own
    documents is evidence of its own proceeding: their RINs, stages and citations belong to
    the rulemakings they post. It forms a proceeding only on its own RIN or docket type, like
    any other docket (fork delivery decision 32 as amended, owner ruling 2026-09-26).

    On the R5 re-audit's parents the identifier is the whole test. All 176 such dockets end
    ``_FRDOC_0001`` and are typed Rulemaking; 155 of the 175 with documents are titled
    "Recently Posted ... Rules and Notices" and the rest are feed dockets titled after a
    document ("FR Pending Documents", "Temporary Holding Docket"). Their documents are FR
    documents (median 100%), they cite up to 1,324 distinct action notices (median 18), no
    FR document names one of them, and through those documents they held 6,025 RINs of
    which 7 were their own. No data test finds the rest: 50 or more cited action notices
    and no RIN of the docket's own also takes 18 program series (FEMA flood-elevation
    determinations, the National Priorities List, NMFS in-season actions) beside 6
    feed-titled dockets (receipt ``frdoc-catchalls-2026-09-26/``).
    """
    return _CATCH_ALL_DOCKET.fullmatch(docket) is not None


#: The digits a document number ends on: its sequence, whatever separates it.
_SEQUENCE = re.compile(r"(\d+)\s*$")


def _sequence(number: str) -> str:
    match = _SEQUENCE.search(number)
    return match.group(1) if match else ""


def _padded(sequence: str) -> bool:
    return len(sequence) > 1 and sequence.startswith("0")


def _key_match_method(reference: str, held: str) -> str | None:
    """How a reference that shares a held number's comparison key matches it, or ``None``.

    ``folded_`` when the sequences are equal, so only dashes or case differ
    (``2018–28359`` for ``2018-28359``); ``unpadded_`` when one side's sequence has no
    leading zero (``2010-02394`` for ``2010-2394``, ``2013-123`` for ``2013-00123``). Two
    sequences both padded, to different widths (``2015-0674`` against ``2015-00674``), are
    refused: neither spelling is the unpadded number, and all five on the 2026-09-23 parents
    named another document.
    """
    ours, theirs = _sequence(reference), _sequence(held)
    if ours == theirs:
        return "folded_"
    if not _padded(ours) or not _padded(theirs):
        return "unpadded_"
    return None


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
        # The comparison key -> the one held number that reduces to it; a key two held numbers
        # reduce to (five 1994-1997 pairs) is refused rather than chosen, even when a
        # reference's publication date would pick one: the date qualifies a number, and here
        # the number itself is in doubt.
        self._number_by_key: dict[str, str] = {}
        self._colliding_keys: dict[str, list[str]] = {}
        for number in self.by_number:
            key = self._unpadded(number)
            if key is None:
                continue
            held = self._number_by_key.setdefault(key, number)
            if held != number:
                self._colliding_keys.setdefault(key, [held]).append(number)
        # fr_docket_links path -> each row's dockets and its resolved reference; see docket_links.
        # Keyed on the path alone: an index must not outlive a rebuild of the file it cached.
        self._docket_links: dict[Path, list[tuple[tuple[str, ...], dict]]] = {}

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
        compared on SpicyDocs' comparison key, reducing both sides: dashes and case fold, and
        the sequence's zero padding goes (the Register pads every number from 2013;
        Regulations.gov pads older ones too, and writes en dashes). A key match is accepted
        as :func:`_key_match_method` says.

        ``status`` is ``missing`` when no match finds a record, ``ambiguous`` when the
        reference reaches more than one held record (by one number's several dates, or by
        a key two held numbers share, whatever the date), ``dated`` when the supplied
        publication date resolves it, and ``single_candidate_in_input`` when exactly one
        candidate is held and no date was supplied. A key match prefixes the status with
        its method (``folded_dated``, ``unpadded_single_candidate_in_input``, ...), so every
        row says how it resolved.
        """
        method: str | None = ""
        if number in self.by_number:
            candidates = self._candidates(number, publication_date)
        elif (key := self._unpadded(number)) is None or key not in self._number_by_key:
            candidates = []
        elif key in self._colliding_keys:
            candidates = sorted(
                identity for held in self._colliding_keys[key] for identity in self.by_number[held].values()
            )
        elif (method := _key_match_method(number, self._number_by_key[key])) is None:
            candidates = []
        else:
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

    def docket_links(self, path: Path) -> Iterator[tuple[str, dict]]:
        """Each docket an ``fr_docket_links`` row names, with that row's reference resolved here.

        A value naming several dockets (:func:`linked_docket_ids`) yields one pair per docket,
        each reference carrying its own docket as ``evidence_id`` and the row's document
        number, date and status otherwise; a value naming none, or a row with no number,
        yields nothing. So a stage keys a link by its (FR document, docket) pair, as it did
        when a value named one docket. Every docket-shaped value comes back: which dockets a
        stage trusts is its own join.

        The table is read once per index and replayed to every stage that shares it, as the
        index itself is; the plural reader costs about twice the single one, which each stage
        used to run. In paired builds of the 2026-09-23 parents the four stages took 110.5 s
        reading once with the plural reader and 118.6 s reading per stage with the single
        (receipt ``docket-lists-2026-09-24``). The cache is keyed on ``path`` alone, so an
        index must not be reused after the links file at that path is rebuilt. Each reference
        is a fresh dict per pair, and its ``candidate_ids`` a tuple, since every stage shares it.
        """
        rows = self._docket_links.get(path)
        if rows is None:
            rows = self._docket_links[path] = []
            for row in iter_parquet_rows(path, columns=("docket_id", "document_number", "publication_date")):
                if row.get("document_number") and (dockets := linked_docket_ids(row.get("docket_id"))):
                    reference = self.reference(str(row["document_number"]), row.get("publication_date"))
                    rows.append((dockets, {**reference, "candidate_ids": tuple(reference["candidate_ids"])}))
        for dockets, reference in rows:
            for docket in dockets:
                yield docket, {"source": "fr_docket_links", "evidence_id": docket, **reference}

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
