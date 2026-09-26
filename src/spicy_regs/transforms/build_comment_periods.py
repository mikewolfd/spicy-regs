"""Transform: materialize continuous and reopened public-comment intervals.

Reads the proceedings, dockets, documents, federal_register and fr_docket_links
parquet inputs from ``output_dir`` and writes ``comment_periods.parquet``; a missing
input raises FileNotFoundError.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import quote

import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.ontology.citations import normalize_regsgov_identifier, normalize_rin
from spicy_regs.ontology.rins import proceeding_rins
from spicy_regs.ontology.common import (
    ATTESTATION_COLUMNS,
    JsonReadStats,
    RunContext,
    canonical_json,
    eastern_day,
    iter_parquet_rows,
    parse_json_list,
    stable_id,
    write_parquet_rows,
)

from spicy_regs.ontology.federal_register import (
    FederalRegisterIndex,
    record_url,
    references_json,
    resolved_id,
)

OUTPUT = "comment_periods.parquet"
# v5: regulations.gov close dates are the Eastern day, one day earlier than v4 (see eastern_day).
# v6: labelled FR docket values join (linked_docket_id), so more FR intervals carry a docket.
# v7: a docket value naming several dockets joins each (linked_docket_ids), and an FR interval
# whose notice several proceedings hold lists every one of them (decision 33), not none.
# v8: SpicyDocs 0.35.0 reads a docket named after prose (D1) and folds Regulations.gov's typed FR-number separators (D2).
ACTOR_ID = "spicy-regs:comment-periods:v8"

COLUMNS = (
    "comment_period_id",
    "proceeding_ids_json",
    "rins_json",
    "docket_ids_json",
    "open_date",
    "close_date",
    "source",
    "opened_by_artifact_ids_json",
    "evidence_ids_json",
    *ATTESTATION_COLUMNS,
    "unresolved_fr_references_json",
)


@dataclass(frozen=True)
class _Interval:
    proceeding_ids: tuple[str, ...]
    rins: tuple[str, ...]
    docket_ids: tuple[str, ...]
    start: date
    end: date
    source: str
    evidence_id: str
    opened_by_artifact_id: str


def _artifact_url(source: str, identifier: object) -> str | None:
    """The public URL of the artifact that opened the period, or None for a source with no URL rule."""
    value = str(identifier or "").strip()
    if not value:
        return None
    escaped = quote(value, safe="-._~")
    if source == "documents.comment_end_date":
        return f"https://www.regulations.gov/document/{escaped}"
    if source == "federal_register.comments_close_on":
        return record_url(value)
    return None


def _merge_intervals(intervals: list[_Interval]) -> list[dict]:
    """Coalesce extensions without losing joint or unresolved anchors.

    Docket anchors are the grouping key when available because they remain
    usable while Proceeding identity is unresolved. Proceeding anchors are
    unioned only from uniquely resolved source assertions. A docket-less
    interval falls back to its resolved Proceeding anchors.
    """
    grouped: dict[tuple[str, tuple[str, ...]], list[_Interval]] = defaultdict(list)
    for interval in intervals:
        anchor = ("dockets", interval.docket_ids) if interval.docket_ids else ("proceedings", interval.proceeding_ids)
        if not interval.docket_ids and not interval.proceeding_ids:
            anchor = ("artifact", (interval.opened_by_artifact_id,))
        grouped[anchor].append(interval)

    merged: list[dict] = []
    for _, values in grouped.items():
        values.sort(
            key=lambda interval: (
                interval.start,
                interval.end,
                interval.source,
                interval.evidence_id,
            )
        )
        current_start: date | None = None
        current_end: date | None = None
        proceeding_ids: set[str] = set()
        rins: set[str] = set()
        docket_ids: set[str] = set()
        sources: set[str] = set()
        evidence: set[str] = set()
        opened_by: set[str] = set()

        def flush() -> None:
            if current_start is None or current_end is None:
                return
            sorted_proceedings = sorted(proceeding_ids)
            sorted_dockets = sorted(docket_ids)
            merged.append(
                {
                    "comment_period_id": stable_id(
                        "comment_period",
                        canonical_json(sorted_proceedings),
                        canonical_json(sorted_dockets),
                        current_start.isoformat(),
                        *(() if sorted_proceedings or sorted_dockets else (canonical_json(sorted(opened_by)),)),
                    ),
                    "proceeding_ids_json": canonical_json(sorted_proceedings),
                    "rins_json": canonical_json(sorted(rins)),
                    "docket_ids_json": canonical_json(sorted_dockets),
                    "open_date": current_start.isoformat(),
                    "close_date": current_end.isoformat(),
                    "source": "+".join(sorted(sources)),
                    "opened_by_artifact_ids_json": canonical_json(sorted(opened_by)),
                    "evidence_ids_json": canonical_json(sorted(evidence)),
                }
            )

        def begin(interval: _Interval) -> None:
            nonlocal current_start, current_end
            nonlocal proceeding_ids, rins, docket_ids, sources, evidence, opened_by
            current_start, current_end = interval.start, interval.end
            proceeding_ids = set(interval.proceeding_ids)
            rins = set(interval.rins)
            docket_ids = set(interval.docket_ids)
            sources = {interval.source}
            evidence = {interval.evidence_id}
            opened_by = {interval.opened_by_artifact_id}

        for interval in values:
            if current_start is None:
                begin(interval)
                continue
            assert current_end is not None
            if interval.start <= current_end + timedelta(days=1):
                current_end = max(current_end, interval.end)
                proceeding_ids.update(interval.proceeding_ids)
                rins.update(interval.rins)
                docket_ids.update(interval.docket_ids)
                sources.add(interval.source)
                evidence.add(interval.evidence_id)
                if interval.start == current_start:
                    opened_by.add(interval.opened_by_artifact_id)
                continue
            flush()
            begin(interval)
        flush()
    return merged


def build_comment_periods(
    output_dir: Path,
    *,
    run_id: str | None = None,
    asserted_at: str | None = None,
    fr_index: FederalRegisterIndex | None = None,
) -> Path:
    """Build comment periods, retaining docket-only and joint intervals.

    Every open and close date is :func:`eastern_day`. ``fr_index`` is the
    generation's shared index of ``federal_register.parquet``; it is built here
    when not supplied.
    """
    required = {
        name: output_dir / f"{name}.parquet"
        for name in (
            "proceedings",
            "dockets",
            "documents",
            "federal_register",
            "fr_docket_links",
        )
    }
    missing = [path.name for path in required.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"comment_periods inputs missing from {output_dir}: {', '.join(missing)}")
    context = RunContext.resolve(
        run_id=run_id,
        asserted_at=asserted_at,
        prefix="comment-periods",
    )
    provenance = context.provenance(method="deterministic", actor_id=ACTOR_ID)
    json_stats = JsonReadStats()
    fr_index = fr_index or FederalRegisterIndex(required["federal_register"])
    unresolved_by_fr: dict[str, list[dict]] = defaultdict(list)

    proceeding_by_id: dict[str, dict] = {}
    dockets_by_proceeding: dict[str, set[str]] = {}
    proceeding_ids_by_docket: dict[str, set[str]] = defaultdict(set)
    proceeding_ids_by_fr_document: dict[str, set[str]] = defaultdict(set)
    for row in iter_parquet_rows(
        required["proceedings"],
        # Every column proceeding_rins and FederalRegisterIndex.proceeding_ids read.
        columns=(
            "proceeding_id",
            "docket_ids_json",
            "fr_document_ids_json",
            "fr_document_numbers_json",
            "unresolved_fr_references_json",
            "rins_json",
            "rin",
        ),
    ):
        proceeding_id = str(row["proceeding_id"])
        proceeding_by_id[proceeding_id] = row
        dockets = parse_json_list(
            row.get("docket_ids_json"),
            stats=json_stats,
            table="proceedings",
            row_id=proceeding_id,
            column="docket_ids_json",
        )
        docket_set = (
            set()
            if dockets is None
            else {normalized for docket in dockets if (normalized := normalize_regsgov_identifier(docket)) is not None}
        )
        dockets_by_proceeding[proceeding_id] = docket_set
        for docket in docket_set:
            proceeding_ids_by_docket[docket].add(proceeding_id)
        fr_documents, unresolved = fr_index.proceeding_ids(row, json_stats)
        for identity in fr_documents:
            proceeding_ids_by_fr_document[identity].add(proceeding_id)
        for reference in unresolved:
            for candidate in reference["candidate_ids"]:
                unresolved_by_fr[candidate].append(reference)

    trusted_dockets = {
        normalized
        for row in iter_parquet_rows(required["dockets"], columns=("docket_id",))
        if (normalized := normalize_regsgov_identifier(row.get("docket_id"))) is not None
    }

    intervals: list[_Interval] = []
    inverted_by_source: Counter[str] = Counter()
    inverted_examples: list[str] = []
    ambiguous_document_intervals = 0
    shared_fr_intervals = 0
    unanchored_intervals = 0

    def add_interval(
        *,
        proceeding_ids: set[str],
        docket_ids: set[str],
        rins: set[str],
        start: object,
        end: object,
        source: str,
        evidence_id: object,
        retain_unresolved: bool = False,
    ) -> None:
        nonlocal unanchored_intervals
        open_date, close_date = eastern_day(start), eastern_day(end)
        evidence = str(evidence_id or "").strip()
        opened_by = _artifact_url(source, evidence)
        if open_date is None or close_date is None or not evidence or opened_by is None:
            return
        if close_date < open_date:
            inverted_by_source[source] += 1
            if len(inverted_examples) < 5:
                inverted_examples.append(f"{source} {evidence}: {open_date.isoformat()}..{close_date.isoformat()}")
            return
        if not proceeding_ids and not docket_ids and not retain_unresolved:
            unanchored_intervals += 1
            return
        resolved_rins = set(rins)
        resolved_rins.update(
            rin
            for proceeding_id in proceeding_ids
            for rin in proceeding_rins(proceeding_by_id[proceeding_id], json_stats)
        )
        intervals.append(
            _Interval(
                proceeding_ids=tuple(sorted(proceeding_ids)),
                rins=tuple(sorted(resolved_rins)),
                docket_ids=tuple(sorted(docket_ids)),
                start=open_date,
                end=close_date,
                source=source,
                evidence_id=evidence,
                opened_by_artifact_id=opened_by,
            )
        )

    for row in iter_parquet_rows(
        required["documents"],
        columns=(
            "document_id",
            "docket_id",
            "additional_rins",
            "posted_date",
            "comment_start_date",
            "comment_end_date",
        ),
    ):
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        if docket is None or not row.get("comment_end_date"):
            continue
        # The document endpoint is itself a source-of-record membership signal.
        trusted_dockets.add(docket)
        docket_targets = set(proceeding_ids_by_docket.get(docket, ()))
        raw_rins = parse_json_list(
            row.get("additional_rins"),
            stats=json_stats,
            table="documents",
            row_id=row.get("document_id"),
            column="additional_rins",
        )
        rins = set() if raw_rins is None else {rin for value in raw_rins if (rin := normalize_rin(value)) is not None}
        # The source-backed docket is action identity. A RIN is retained as
        # interval metadata but never filters or selects a Proceeding.
        candidates = docket_targets
        proceeding_ids = candidates if len(candidates) == 1 else set()
        if len(candidates) > 1:
            ambiguous_document_intervals += 1
        add_interval(
            proceeding_ids=proceeding_ids,
            docket_ids={docket},
            rins=rins,
            start=row.get("comment_start_date") or row.get("posted_date"),
            end=row.get("comment_end_date"),
            source="documents.comment_end_date",
            evidence_id=row.get("document_id"),
        )

    linked_dockets_by_fr: dict[str, set[str]] = defaultdict(set)
    for docket, reference in fr_index.docket_links(required["fr_docket_links"]):
        if docket not in trusted_dockets:
            continue
        if identity := resolved_id(reference):
            linked_dockets_by_fr[identity].add(docket)
        else:
            for candidate in reference["candidate_ids"]:
                unresolved_by_fr[candidate].append(reference)

    for row in iter_parquet_rows(
        required["federal_register"],
        columns=("document_number", "publication_date", "comments_close_on", "regulation_id_numbers_json"),
    ):
        if not row.get("comments_close_on") or not row.get("publication_date"):
            continue
        document_number = str(row.get("document_number") or "")
        identity = fr_index.record_id(row)
        raw_rins = parse_json_list(
            row.get("regulation_id_numbers_json"),
            stats=json_stats,
            table="federal_register",
            row_id=document_number,
            column="regulation_id_numbers_json",
        )
        rins = set() if raw_rins is None else {rin for value in raw_rins if (rin := normalize_rin(value)) is not None}
        dockets = set(linked_dockets_by_fr.get(identity, ()))
        docket_targets: set[str] = set()
        for docket in dockets:
            docket_targets.update(proceeding_ids_by_docket.get(docket, ()))
        artifact_targets = set(proceeding_ids_by_fr_document.get(identity, ()))
        # Direct artifact membership is strongest. Docket membership is the
        # fallback for older rows that predate the artifact projection. A notice that
        # is no action evidence attaches to every proceeding whose dockets it names
        # (decision 33), and its period opens in each of them, so it lists them all.
        proceeding_ids = artifact_targets or docket_targets
        if len(proceeding_ids) > 1:
            shared_fr_intervals += 1
        add_interval(
            proceeding_ids=proceeding_ids,
            docket_ids=dockets,
            rins=rins,
            start=row.get("publication_date"),
            end=row.get("comments_close_on"),
            source="federal_register.comments_close_on",
            evidence_id=identity,
            retain_unresolved=bool(unresolved_by_fr[identity]),
        )

    rows = _merge_intervals(intervals)
    for row in rows:
        row.update(provenance)
        references = [
            reference
            for evidence in json.loads(row["evidence_ids_json"])
            for reference in unresolved_by_fr.get(evidence, ())
        ]
        row["unresolved_fr_references_json"] = references_json(references)
    rows.sort(
        key=lambda row: (
            row["docket_ids_json"],
            row["proceeding_ids_json"],
            row["open_date"],
        )
    )
    out_file = write_parquet_rows(output_dir / OUTPUT, columns=COLUMNS, rows=rows)
    json_stats.log("comment_periods")
    if inverted_by_source:
        logger.warning(
            "comment_periods: skipped {:,} inverted source intervals ({}); examples: {}",
            sum(inverted_by_source.values()),
            ", ".join(f"{source}={count:,}" for source, count in sorted(inverted_by_source.items())),
            "; ".join(inverted_examples),
        )
    if shared_fr_intervals:
        logger.info(
            "comment_periods: {:,} FR intervals open in several proceedings and list each",
            shared_fr_intervals,
        )
    if ambiguous_document_intervals:
        logger.warning(
            "comment_periods: retained {:,} ambiguous document intervals with docket-only anchors",
            ambiguous_document_intervals,
        )
    if unanchored_intervals:
        logger.warning(
            "comment_periods: skipped {:,} intervals with neither a resolved Proceeding nor a source-backed Docket",
            unanchored_intervals,
        )
    anchor_counts = Counter((row["proceeding_ids_json"], row["docket_ids_json"]) for row in rows)
    reopenings = sum(count - 1 for count in anchor_counts.values() if count > 1)
    docket_only = sum(row["proceeding_ids_json"] == "[]" and row["docket_ids_json"] != "[]" for row in rows)
    logger.info(
        "Comment periods: {:,} rows ({:,} reopened; {:,} docket-only)",
        len(rows),
        reopenings,
        docket_only,
    )
    assert pq.ParquetFile(out_file).schema_arrow.names == list(COLUMNS)
    return out_file
