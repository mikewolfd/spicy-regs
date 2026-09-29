"""Transform: build ``gao_recommendations.parquet``, every recommendation GAO's open-recommendations export has listed.

**An accumulator over a snapshot.** SpicyDocs reads GAO's export (``spicy_docs.sources.gao.recommendations``), which
lists only open recommendations: an implemented or closed one drops out. So each run folds that day's export into the
prior published table (:func:`fold`). A listed row takes the export's values and keeps the prior ``first_seen``; a
prior row the export no longer lists stays, with ``listed_open = false`` and the ``last_seen`` it had. Nothing is
deleted, so the table only grows, and the R2 shrink guard cannot see a bad export. :func:`fold` therefore refuses a run
that would retire more than half the rows the prior lists open, the shrink guard's own ratio.

**The key is the contract's.** The fold reads ``recommendation_id`` through ``GAO_RECOMMENDATIONS.key`` and never
computes it, so a change of key is a change to SpicyDocs' ``gao_recommendation_id`` alone. Every prior row then
carries the old spelling, which the guard refuses as a mass retirement until the prior is re-keyed explicitly.

**No director phone is published** (owner decision, 2026-09-28: the director's name only). The table has no phone
column, and the source evidence, which lives in the public bucket, retains the export with that column emptied
(:func:`redact_director_phone`), every other byte kept, beside the raw file's SHA-256 and size. The raw bytes are
held in memory only: never written under the output directory, whose failures CI uploads, and never retained, a
refused export included.

One Zyte request a run: ``www.gao.gov`` refuses plain clients. ``ZYTE_TOKEN`` is read from the environment.
"""

from __future__ import annotations

import dataclasses
import re
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.transforms.table_merge import merge_contract_table, published_table

if TYPE_CHECKING:
    import httpx
    from spicy_docs.sources.gao.recommendations import GaoRecommendationsBudget, GaoRecommendationsExport
    from spicy_docs.transport.captured import CapturedBodyResponse
    from spicy_docs.transport.zyte import ZyteTransport

    from spicy_regs.source_evidence import CaptureEvidence

TABLE = "gao_recommendations"
OUTPUT = f"{TABLE}.parquet"
STAGE = "gao-recommendations-export"
PHONE_COLUMN = "Director Phone"
#: The R2 shrink guard's default ratio (``sources/r2.py``): at least half the rows the prior lists open must stay listed.
MIN_STILL_LISTED = 0.5
#: One field and what ends it: a quoted field (with anything GAO leaves after its closing quote, as the preamble's
#: ``"Source: ..." ,`` does), or an unquoted one; then a comma, a line end, or the end of the export.
_FIELD = re.compile(rb'("(?:[^"]|"")*"[^,\r\n"]*|[^,\r\n"]*)(,|\r?\n|\Z)')


class GaoRecommendationsFoldError(RuntimeError):
    """The export would retire more of the open rows than a real day's closures can."""


def redact_director_phone(body: bytes) -> tuple[bytes, int]:
    """``body`` with every Director Phone field emptied and every other byte kept, and how many held a value.

    The header is the first record naming the column; the preamble before it is kept whole. An emptied field is
    spelled as GAO spells an empty one, with nothing between its commas. Refuses bytes that are not CSV or name no
    such column, since then no field can be proved to be the phone.
    """
    pieces: list[bytes] = []
    kept_from, position, column, index, blanked = 0, 0, None, 0, 0
    header: list[bytes] = []
    while position < len(body):
        match = _FIELD.match(body, position)
        if match is None:
            raise ValueError(f"GAO export is not CSV at byte {position}; nothing can be retained")
        field, separator = match.group(1), match.group(2)
        if column is None:
            header.append(field.strip(b'"'))
        elif index == column and field:
            pieces.append(body[kept_from : match.start(1)])
            kept_from = match.end(1)
            blanked += field not in (b"", b'""')
        index += 1
        if separator != b",":
            if column is None and PHONE_COLUMN.encode() in header:
                column = header.index(PHONE_COLUMN.encode())
            header, index = [], 0
        position = match.end()
    if column is None:
        raise ValueError(f"GAO export names no {PHONE_COLUMN!r} column; nothing can be retained")
    pieces.append(body[kept_from:])
    return b"".join(pieces), blanked


def _retain_redacted(evidence: CaptureEvidence, capture: CapturedBodyResponse, *, stage: str) -> None:
    """Retain ``capture`` with its phone column emptied, bound to the raw bytes' digest and size by a journal event."""
    try:
        redacted, blanked = redact_director_phone(capture.body)
    except ValueError as error:
        evidence.event("unretained", stage=stage, reason=str(error), sha256=capture.sha256, byte_size=capture.byte_size)
        return
    kept = dataclasses.replace(capture, body=redacted)
    evidence.capture(kept, stage=stage)
    evidence.event(
        "redacted",
        stage=stage,
        column=PHONE_COLUMN,
        fields_emptied=blanked,
        original_sha256=capture.sha256,
        original_byte_size=capture.byte_size,
        retained_sha256=kept.sha256,
        retained_byte_size=kept.byte_size,
    )


def _zyte_transport(budget: GaoRecommendationsBudget, evidence: CaptureEvidence | None) -> ZyteTransport:
    """One Zyte request's transport, its token read from ``ZYTE_TOKEN`` and scrubbed from the evidence journal."""
    from spicy_docs.sources.zyte import ZyteHttpFetcher, require_zyte_token_from_environment
    from spicy_docs.transport.zyte import ZyteBudget, ZyteTransport

    token = require_zyte_token_from_environment()
    if evidence is not None:
        evidence.credential = token
    return ZyteTransport(
        ZyteHttpFetcher(token=token),
        max_bytes=budget.max_bytes,
        timeout_seconds=budget.timeout_seconds,
        budget=ZyteBudget(1),
    )


def _acquire(
    transport: httpx.BaseTransport | None, evidence: CaptureEvidence | None
) -> tuple[GaoRecommendationsExport, CapturedBodyResponse]:
    """One capture of the export, through Zyte unless a caller supplies the transport."""
    from spicy_docs.sources.gao.recommendations import GaoRecommendationsAcquirer, GaoRecommendationsBudget

    budget = GaoRecommendationsBudget()
    zyte = None
    if transport is None:
        transport = zyte = _zyte_transport(budget, evidence)
    try:
        with GaoRecommendationsAcquirer(budget=budget, transport=transport) as acquirer:
            return acquirer.acquire_export()
    finally:
        if evidence is not None and zyte is not None and zyte.records:
            record = zyte.records[-1]
            evidence.event(
                "proxied",
                stage=STAGE,
                proxied_client=record.proxied_client,
                mode=record.mode,
                zyte_request_id=record.zyte_request_id,
                status_code=record.status_code,
                original_sha256=f"sha256:{record.sha256}",
                original_byte_size=record.byte_size,
            )


def fold(
    prior: Iterable[Mapping[str, str | None]],
    fresh: Iterable[Mapping[str, str | None]],
    *,
    allow_mass_close_reason: str | None = None,
) -> tuple[list[dict], dict]:
    """The accumulated table: every ``fresh`` row keeping its prior ``first_seen``, then every prior row it no longer
    lists, with ``listed_open = false``; and what the run retires.

    Refuses when the prior's open rows the export still lists fall below :data:`MIN_STILL_LISTED` of them, unless
    ``allow_mass_close_reason`` states why a real mass closure is expected; the retirement then names that reason.
    A prior with no open row, including none at all, refuses nothing.
    """
    from spicy_docs.schemas.gao_recommendation_tables import GAO_RECOMMENDATIONS

    held = {GAO_RECOMMENDATIONS.key(row): row for row in prior}
    rows = [dict(row) for row in fresh]
    listed = {GAO_RECOMMENDATIONS.key(row) for row in rows}
    open_before = [key for key, row in held.items() if row.get("listed_open") == "true"]
    still = sum(key in listed for key in open_before)
    tripped = bool(open_before) and still < MIN_STILL_LISTED * len(open_before)
    retirement = {
        "open_before": len(open_before),
        "still_listed": still,
        "retired": len(open_before) - still,
        "mass_close_reason": allow_mass_close_reason if tripped else None,
    }
    if tripped and not allow_mass_close_reason:
        raise GaoRecommendationsFoldError(
            f"GAO's export lists {still:,} of the {len(open_before):,} recommendations the prior table lists open; "
            f"retiring the rest needs at least {MIN_STILL_LISTED:.0%} still listed, so this export is refused "
            "(a dispatched run can state allow_mass_close_reason)"
        )
    for row in rows:
        before = held.pop(GAO_RECOMMENDATIONS.key(row), None)
        if before is not None and before.get("first_seen"):
            row["first_seen"] = before["first_seen"]
    return rows + [{**row, "listed_open": "false"} for row in held.values()], retirement


def build_gao_recommendations(
    output_dir: Path,
    *,
    evidence: CaptureEvidence | None = None,
    transport: httpx.BaseTransport | None = None,
    download_prior: Callable[[str, Path], bool] = r2.download,
    allow_mass_close_reason: str | None = None,
) -> Path:
    """Read today's export, fold it into the prior published table, and write the whole table.

    ``allow_mass_close_reason`` lets a dispatched run past :func:`fold`'s guard; the reason and the rows it retired
    are journaled. A scheduled run never states one.
    """
    from spicy_docs.schemas.gao_recommendation_tables import shape_gao_recommendation
    from spicy_docs.transport.captured import attached_capture

    output_dir.mkdir(parents=True, exist_ok=True)
    if evidence is not None:
        evidence.event("selection", stage=STAGE, requests=1, retained=f"export with {PHONE_COLUMN!r} emptied")
    try:
        export, capture = _acquire(transport, evidence)
    except Exception as error:
        refused = attached_capture(error)
        if evidence is not None and refused is not None:
            _retain_redacted(evidence, refused, stage=f"{STAGE}:refused")
        # The raw bytes ride on the error; the run's refusal record would retain them whole.
        for carried in ("capture", "refused_response"):
            error.__dict__.pop(carried, None)
        raise
    if evidence is not None:
        _retain_redacted(evidence, capture, stage=STAGE)
    fresh = [
        shape_gao_recommendation(item, status_as_of=export.status_as_of, as_of=export.as_of)
        for item in export.recommendations
    ]
    prior_file = published_table(output_dir, TABLE, download_prior)
    prior = pq.read_table(prior_file).to_pylist() if prior_file is not None else []
    rows, retirement = fold(prior, fresh, allow_mass_close_reason=allow_mass_close_reason)
    listed = sum(row["listed_open"] == "true" for row in rows)
    counts = {"listed": listed, "not_listed": len(rows) - listed, "new": len(rows) - len(prior)}
    logger.info("GAO recommendations: export of {} lists {:,}; table {:,} ({})", export.status_as_of, len(fresh),
                len(rows), dict(counts))
    if (reason := retirement["mass_close_reason"]) is not None:
        logger.warning("GAO recommendations: guard overridden ({!r}); {:,} of {:,} open rows retired", reason,
                       retirement["retired"], retirement["open_before"])
    if evidence is not None:
        evidence.event("fold", stage=STAGE, status_as_of=export.status_as_of, prior_rows=len(prior), **counts,
                       retired=retirement["retired"])
        if reason is not None:
            evidence.event("mass-close-allowed", stage=STAGE, reason=reason, **retirement)
    return merge_contract_table(
        output_dir, TABLE, rows, download_prior=download_prior, prior_present=prior_file is not None
    )
