"""Transform: materialize one public-comment period per notice, lengthened only by an extension that names it.

Reads the proceedings, dockets, documents, federal_register and fr_docket_links
parquet inputs from ``output_dir`` and writes ``comment_periods.parquet``; a missing
input raises FileNotFoundError.
"""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import quote

import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.ontology.citations import action_evidence_rin, normalize_regsgov_identifier, normalize_rin
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
    catch_all_docket,
    copy_of,
    extends_comment_period,
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
# v9 (one bump over published v8): code unchanged; its rows move with proceedings v9. A period
# in a Federal Register feed docket takes none of the posted rules' RINs through its
# proceeding, only its document's own, and one in a feed with no RIN of its own is
# docket-anchored, as that proceeding goes, and re-keys with its evidence intact.
# v10 (one bump over published v9): code unchanged; its rows move with proceedings v10
# (owner decisions 56, 58-61): on the 2026-09-26 parents 286,065 -> 281,635 periods, as those of
# removed proceedings re-key to what still anchors them or go when nothing does. A Register
# document that joins a docketed proceeding (decision 56) re-keys or folds its periods there,
# every evidence id kept.
# v11: one period per notice (owner decisions 2026-09-28). A notice is a Register record with its
# Regulations.gov copies, or a Regulations.gov document the Register does not hold; it merges
# only with an extension that names it. Dockets and proceedings anchor a period (anchor_kind) and
# never merge two; a feed docket anchors nothing; a notice with no anchor is kept; a placeholder
# close states nothing; each source's own close is kept beside the period's.
# v12 (one bump over published v11): a notice's Register record is evidence of its period even
# when only its copies state a window (31,497 rows on snapshot_8758191f's inputs); ids and dates
# are unchanged. Documents stating a close but no opening are counted as they are left out.
# v13 (one bump over published v12): code unchanged; 124 periods drop a proceeding that proceedings v13 no longer
# forms (a docket that was a rulemaking only through a withdrawn posting); ids, dates and anchors are unchanged.
ACTOR_ID = "spicy-regs:comment-periods:v13"

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
    "anchor_kind",
    "register_close_date",
    "regulations_gov_close_date",
)

REGISTER = "federal_register.comments_close_on"
REGULATIONS_GOV = "documents.comment_end_date"

#: A close on or after this day is a placeholder, not a deadline. On the 2026-09-28 parents the
#: latest real closes fall in 2032 (EPA and HHS dockets held open) and none in 2033-2049; from
#: 2050 on every close is a stand-in: FDA's 2050-02-21 open dockets, Regulations.gov's 2099 and
#: 2100 sentinels (46 documents with those three) and the Register's year typos, 3001-3008 for
#: 2001-2008 (6 notices). The rule reads the close alone: a window measured from its open would
#: also discard real 2006 closes of legacy documents posted "1982". Revisit it as real deadlines
#: near 2050.
PLACEHOLDER_CLOSE = date(2050, 1, 1)

#: A Register citation in an abstract: volume, "FR", and the page a document starts on.
_REGISTER_CITATION = re.compile(r"\b(\d{1,3})\s+FR\s+(\d{1,6})\b")


@dataclass(frozen=True, slots=True)
class _Window:
    """One source record's statement of a notice's window."""

    start: date
    end: date
    source: str
    evidence_id: str


@dataclass(frozen=True, slots=True)
class _Notice:
    """A Register record with its Regulations.gov copies, or a Regulations.gov document the Register does not hold.

    ``register_record`` is the Register record's dated id, which is listed in the period even
    when only its copies state a window; ``rins`` are the ones its records state; ``extends``
    and ``cites`` are what an extension link reads: whether its title extends or reopens a
    comment period, and the Register pages its abstract cites.
    """

    register_record: str | None
    windows: tuple[_Window, ...]
    docket_ids: tuple[str, ...]
    proceeding_ids: tuple[str, ...]
    rins: tuple[str, ...]
    extends: bool
    cites: tuple[tuple[int, int], ...]

    @property
    def start(self) -> date:
        return min(window.start for window in self.windows)

    @property
    def end(self) -> date:
        return max(window.end for window in self.windows)


def _artifact_url(source: str, identifier: object) -> str | None:
    """The public URL of the artifact that opened the period, or None for a source with no URL rule."""
    value = str(identifier or "").strip()
    if not value:
        return None
    escaped = quote(value, safe="-._~")
    if source == REGULATIONS_GOV:
        return f"https://www.regulations.gov/document/{escaped}"
    if source == REGISTER:
        return record_url(value)
    return None


def _one(values: set[str]) -> str | None:
    return next(iter(values)) if len(values) == 1 else None


def _extended_notice(
    key: str,
    notice: _Notice,
    notices: dict[str, _Notice],
    by_rin: dict[str, list[str]],
    cited_pages: dict[tuple[int, int], str],
) -> tuple[str | None, str]:
    """The earlier notice an extension names and whose window it adjoins, with how it names it.

    An extension or reopening (:func:`extends_comment_period`) names a notice by citing the page
    it starts on, or by sharing an action-evidence RIN with it alone among the notices open when
    the extension opens. A citation and a RIN naming different notices name none. An extension
    that opens after the notice closed is a reopening and starts its own period.
    """
    if not notice.extends:
        return None, "not_an_extension"
    start = notice.start

    def adjoins(other_key: str) -> bool:
        # A cited record that states no window is no notice, so nothing to extend.
        other = notices.get(other_key)
        return other is not None and other_key != key and other.start < start <= other.end + timedelta(days=1)

    cited = _one({target for page in notice.cites if (target := cited_pages.get(page)) and adjoins(target)})
    shared = _one({other for rin in notice.rins if action_evidence_rin(rin) for other in by_rin[rin] if adjoins(other)})
    if cited and shared and cited != shared:
        return None, "conflict"
    if cited:
        return cited, "citation"
    return (shared, "rin") if shared else (None, "unlinked")


def _periods(windows: list[tuple[_Window, _Notice]]) -> Iterator[list[tuple[_Window, _Notice]]]:
    """Linked windows that overlap or adjoin, one list per period; a gap starts the next (a reopening)."""
    windows.sort(key=lambda pair: (pair[0].start, pair[0].end, pair[0].source, pair[0].evidence_id))
    period: list[tuple[_Window, _Notice]] = []
    end: date | None = None
    for window, notice in windows:
        if end is not None and window.start > end + timedelta(days=1):
            yield period
            period, end = [], None
        period.append((window, notice))
        end = window.end if end is None else max(end, window.end)
    if period:
        yield period


def build_comment_periods(
    output_dir: Path,
    *,
    run_id: str | None = None,
    asserted_at: str | None = None,
    fr_index: FederalRegisterIndex | None = None,
) -> Path:
    """Build one period per notice and its named extensions, anchored or not.

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
        for docket in dockets or ():
            if (normalized := normalize_regsgov_identifier(docket)) is not None:
                proceeding_ids_by_docket[normalized].add(proceeding_id)
        fr_documents, unresolved = fr_index.proceeding_ids(row, json_stats)
        for identity in fr_documents:
            proceeding_ids_by_fr_document[identity].add(proceeding_id)
        for reference in unresolved:
            for candidate in reference["candidate_ids"]:
                unresolved_by_fr[candidate].append(reference)

    trusted_dockets: set[str] = set()
    feeds: set[str] = set()
    for row in iter_parquet_rows(required["dockets"], columns=("docket_id", "title")):
        if (normalized := normalize_regsgov_identifier(row.get("docket_id"))) is not None:
            trusted_dockets.add(normalized)
            if catch_all_docket(normalized, row.get("title")):
                feeds.add(normalized)

    def anchor_docket(docket: str | None) -> str | None:
        """A docket that can anchor a period: any but a Federal Register feed (decision 32)."""
        return None if docket is None or docket in feeds or catch_all_docket(docket) else docket

    skipped: Counter[str] = Counter()
    skipped_examples: dict[str, list[str]] = defaultdict(list)

    def window(start: object, end: object, source: str, evidence_id: object) -> _Window | None:
        """A source's stated window, or None when it states no usable one (counted by reason)."""
        open_date, close_date = eastern_day(start), eastern_day(end)
        evidence = str(evidence_id or "").strip()
        if not evidence:
            return None
        reason = (
            "undated"
            if open_date is None or close_date is None
            else "placeholder"
            if close_date >= PLACEHOLDER_CLOSE
            else "inverted"
            if close_date < open_date
            else None
        )
        if reason:
            skipped[f"{reason} {source}"] += 1
            if len(skipped_examples[reason]) < 5:
                skipped_examples[reason].append(f"{evidence}: {open_date or '?'}..{close_date or '?'}")
            return None
        assert open_date is not None and close_date is not None
        return _Window(open_date, close_date, source, evidence)

    def stated_rins(value: object, table: str, row_id: object, column: str) -> set[str]:
        raw = parse_json_list(value, stats=json_stats, table=table, row_id=row_id, column=column)
        return {rin for item in raw or () if (rin := normalize_rin(item)) is not None}

    # Regulations.gov documents. A copy of a Register record (its fr_doc_num resolves to one) is
    # that notice's, whatever docket holds it; its ordinary docket anchors the notice when the
    # Register names none (decision 56's B). Any other document with a window is its own notice.
    notices: dict[str, _Notice] = {}
    copy_dockets_by_fr: dict[str, set[str]] = defaultdict(set)
    copy_windows_by_fr: dict[str, list[_Window]] = defaultdict(list)
    copy_rins_by_fr: dict[str, set[str]] = defaultdict(set)
    ambiguous_document_notices = 0
    for row in iter_parquet_rows(
        required["documents"],
        columns=(
            "document_id",
            "docket_id",
            "fr_doc_num",
            "title",
            "additional_rins",
            "posted_date",
            "comment_start_date",
            "comment_end_date",
        ),
    ):
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        anchor = anchor_docket(docket)
        copy = copy_of(row, fr_index)
        if copy is not None and anchor is not None:
            copy_dockets_by_fr[copy].add(anchor)
        if not row.get("comment_end_date"):
            continue
        if docket is not None:
            # The document endpoint is itself a source-of-record membership signal.
            trusted_dockets.add(docket)
        document_id = row.get("document_id")
        rins = stated_rins(row.get("additional_rins"), "documents", document_id, "additional_rins")
        stated = window(
            row.get("comment_start_date") or row.get("posted_date"),
            row.get("comment_end_date"),
            REGULATIONS_GOV,
            document_id,
        )
        if copy is not None:
            copy_rins_by_fr[copy].update(rins)
            if stated is not None:
                copy_windows_by_fr[copy].append(stated)
            continue
        if stated is None:
            continue
        # The source-backed docket is action identity. A RIN is retained as
        # interval metadata but never filters or selects a Proceeding.
        candidates = proceeding_ids_by_docket.get(anchor or "", set())
        ambiguous_document_notices += len(candidates) > 1
        notices[stated.evidence_id] = _Notice(
            register_record=None,
            windows=(stated,),
            docket_ids=(anchor,) if anchor else (),
            proceeding_ids=tuple(candidates) if len(candidates) == 1 else (),
            rins=tuple(sorted(rins)),
            extends=extends_comment_period(row.get("title")),
            cites=(),
        )

    # The dockets a Register record names; its label wins over its copies' dockets.
    linked_dockets_by_fr: dict[str, set[str]] = defaultdict(set)
    for docket, reference in fr_index.docket_links(required["fr_docket_links"]):
        if docket not in trusted_dockets or anchor_docket(docket) is None:
            continue
        if identity := resolved_id(reference):
            linked_dockets_by_fr[identity].add(docket)
        else:
            for candidate in reference["candidate_ids"]:
                unresolved_by_fr[candidate].append(reference)

    shared_fr_notices = 0
    for row in iter_parquet_rows(
        required["federal_register"],
        columns=(
            "document_number",
            "publication_date",
            "comments_close_on",
            "regulation_id_numbers_json",
            "title",
            "abstract",
        ),
    ):
        document_number = str(row.get("document_number") or "")
        identity = fr_index.record_id(row)
        stated = (
            window(row.get("publication_date"), row.get("comments_close_on"), REGISTER, identity)
            if row.get("comments_close_on") and row.get("publication_date")
            else None
        )
        windows = ((stated,) if stated else ()) + tuple(copy_windows_by_fr.pop(identity, ()))
        if not windows:
            continue
        dockets = linked_dockets_by_fr.get(identity) or copy_dockets_by_fr.get(identity) or set()
        docket_targets = {proceeding for docket in dockets for proceeding in proceeding_ids_by_docket.get(docket, ())}
        # Direct artifact membership is strongest. Docket membership is the
        # fallback for older rows that predate the artifact projection. A notice that
        # is no action evidence attaches to every proceeding whose dockets it names
        # (decision 33), and its period opens in each of them, so it lists them all.
        proceeding_ids = proceeding_ids_by_fr_document.get(identity) or docket_targets
        shared_fr_notices += len(proceeding_ids) > 1
        rins = stated_rins(
            row.get("regulation_id_numbers_json"), "federal_register", document_number, "regulation_id_numbers_json"
        ) | copy_rins_by_fr.get(identity, set())
        extends = extends_comment_period(row.get("title"))
        notices[identity] = _Notice(
            register_record=identity,
            windows=windows,
            docket_ids=tuple(sorted(dockets)),
            proceeding_ids=tuple(sorted(proceeding_ids)),
            rins=tuple(sorted(rins)),
            extends=extends,
            cites=tuple(
                (int(match.group(1)), int(match.group(2)))
                for match in _REGISTER_CITATION.finditer(str(row.get("abstract") or ""))
            )
            if extends
            else (),
        )

    # The one Register record each cited page starts; a page two records start names neither.
    wanted = {page for notice in notices.values() for page in notice.cites}
    starting: dict[tuple[int, int], set[str]] = defaultdict(set)
    if wanted:
        for row in iter_parquet_rows(
            required["federal_register"], columns=("document_number", "publication_date", "volume", "start_page")
        ):
            try:
                page = (int(row["volume"]), int(row["start_page"]))
            except (KeyError, TypeError, ValueError):
                continue
            if page in wanted:
                starting[page].add(fr_index.record_id(row))
    cited_pages = {page: identity for page, identities in starting.items() if (identity := _one(identities))}

    by_rin: dict[str, list[str]] = defaultdict(list)
    for key, notice in notices.items():
        for rin in notice.rins:
            if action_evidence_rin(rin):
                by_rin[rin].append(key)
    extended_by: dict[str, str] = {}
    links: Counter[str] = Counter()
    for key, notice in notices.items():
        target, how = _extended_notice(key, notice, notices, by_rin, cited_pages)
        if notice.extends:
            links[how] += 1
        if target is not None:
            extended_by[key] = target

    def root(key: str) -> str:
        # Each link names a notice that opened earlier, so the chains end.
        while key in extended_by:
            key = extended_by[key]
        return key

    members: dict[str, list[str]] = defaultdict(list)
    for key in notices:
        members[root(key)].append(key)

    rows: list[dict] = []
    reopened_notices = 0
    for keys in members.values():
        linked = [(window, notices[key]) for key in keys for window in notices[key].windows]
        periods = list(_periods(linked))
        reopened_notices += len(periods) > 1
        # A Register record that states no window of its own (its copies do) is listed with its
        # notice's first period, so every record of a notice sits in exactly one period.
        listed = {window.evidence_id for window, _ in linked}
        for period in periods:
            quiet = {
                record
                for _, notice in period
                if (record := notice.register_record) is not None and record not in listed
            }
            listed |= quiet
            rows.append(_period_row(period, quiet, proceeding_by_id, json_stats) | provenance)
    for row in rows:
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
            row["comment_period_id"],
        )
    )
    out_file = write_parquet_rows(output_dir / OUTPUT, columns=COLUMNS, rows=rows)
    json_stats.log("comment_periods")
    for reason, examples in skipped_examples.items():
        logger.warning(
            "comment_periods: skipped {} source windows ({}); examples: {}",
            reason,
            ", ".join(
                f"{key.split(' ', 1)[1]}={count:,}" for key, count in sorted(skipped.items()) if key.startswith(reason)
            ),
            "; ".join(examples),
        )
    if shared_fr_notices:
        logger.info(
            "comment_periods: {:,} Register notices open in several proceedings and list each", shared_fr_notices
        )
    if ambiguous_document_notices:
        logger.info(
            "comment_periods: {:,} Regulations.gov notices keep only their docket (its proceeding is ambiguous)",
            ambiguous_document_notices,
        )
    kinds = Counter(row["anchor_kind"] for row in rows)
    logger.info(
        "Comment periods: {:,} rows from {:,} notices ({}); extensions {}; {:,} linked groups reopen",
        len(rows),
        len(notices),
        ", ".join(f"{kind} {count:,}" for kind, count in sorted(kinds.items())),
        ", ".join(f"{how} {count:,}" for how, count in sorted(links.items())),
        reopened_notices,
    )
    assert pq.ParquetFile(out_file).schema_arrow.names == list(COLUMNS)
    return out_file


def _period_row(
    period: list[tuple[_Window, _Notice]],
    quiet_records: set[str],
    proceeding_by_id: dict[str, dict],
    json_stats: JsonReadStats,
) -> dict:
    """One period's row: its windows' span, every anchor of the notices they state, and every record.

    ``quiet_records`` are the Register records of its notices that state no window of their own;
    they are evidence of the period but open and close nothing, so its id and dates stand without them.
    """
    windows = [window for window, _ in period]
    notices = list({id(notice): notice for _, notice in period}.values())
    open_date = min(window.start for window in windows)
    dockets = sorted({docket for notice in notices for docket in notice.docket_ids})
    proceedings = sorted({proceeding for notice in notices for proceeding in notice.proceeding_ids})
    rins = {rin for notice in notices for rin in notice.rins}
    rins.update(rin for proceeding in proceedings for rin in proceeding_rins(proceeding_by_id[proceeding], json_stats))
    opened_by = sorted(
        {
            url
            for window in windows
            if window.start == open_date and (url := _artifact_url(window.source, window.evidence_id))
        }
    )

    def latest(source: str) -> str | None:
        closes = [window.end for window in windows if window.source == source]
        return max(closes).isoformat() if closes else None

    return {
        "comment_period_id": stable_id("comment_period", canonical_json(opened_by)),
        "proceeding_ids_json": canonical_json(proceedings),
        "rins_json": canonical_json(sorted(rins)),
        "docket_ids_json": canonical_json(dockets),
        "open_date": open_date.isoformat(),
        "close_date": max(window.end for window in windows).isoformat(),
        "source": "+".join(sorted({window.source for window in windows})),
        "opened_by_artifact_ids_json": canonical_json(opened_by),
        "evidence_ids_json": canonical_json(sorted({window.evidence_id for window in windows} | quiet_records)),
        "anchor_kind": "docket" if dockets else "proceeding" if proceedings else "none",
        "register_close_date": latest(REGISTER),
        "regulations_gov_close_date": latest(REGULATIONS_GOV),
    }
