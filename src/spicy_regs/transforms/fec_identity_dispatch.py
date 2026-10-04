"""Maintained, explicit build dispatch for the retained FEC identity mappers.

The caller verifies selected source-table bytes and owns streaming. This module
never chooses a current record or guesses a mapper from column names.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json

from .fec_query import CollectionSelection, _digest


@dataclass(frozen=True)
class IdentityJob:
    kind: str
    collection_id: str
    prepared: object


def prepare_identity_job(kind, entry, *, source_generation_pin, header_row=None):
    """Prepare one explicitly selected source collection using its native checks."""
    _digest(source_generation_pin)
    if kind == "candidate_api":
        from .fec_candidate_observations import prepare_candidate_api

        prepared = prepare_candidate_api(entry, source_generation_pin=source_generation_pin)
    elif kind == "committee_history":
        from .fec_committee_history_observations import prepare_history

        prepared = prepare_history(entry, source_generation_pin=source_generation_pin, header_row=header_row)
    elif kind in {"committee_api", "registry", "filings"}:
        capture = entry["scope"]["capture"]
        prepared = CollectionSelection(
            entry["collection_id"],
            _digest(capture["responseSha256"]),
            source_generation_pin,
            "official-fec",
            entry.get("source_cycle"),
            "snapshot",
            "sha256:" + hashlib.sha256(json.dumps(entry, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
        )
    else:
        raise ValueError(f"Unsupported FEC identity mapping job: {kind}")
    return IdentityJob(kind, entry["collection_id"], prepared)


def map_identity_record(row, job):
    """Return table rows plus every source witness from the maintained mapper."""
    if row.get("collection_id") != job.collection_id:
        raise ValueError("FEC identity row is outside the selected collection")
    if job.kind == "candidate_api":
        from .fec_candidate_observations import map_candidate_api

        return map_candidate_api(row, job.prepared)
    if job.kind == "committee_api":
        from .fec_committee_observations import map_committee_api

        return map_committee_api(row, job.prepared)
    if job.kind == "committee_history":
        from .fec_committee_history_observations import map_history

        mapped, evidence = map_history(row, job.prepared)
        return {job.prepared.table: [mapped]}, evidence
    if job.kind == "registry":
        from .fec_identity_observations import map_registry, registry_mapping_for

        mapping = registry_mapping_for(row)
        mapped, evidence = map_registry(row, job.prepared, mapping)
        return {mapping.table: [] if mapped is None else [mapped]}, evidence
    if job.kind == "filings":
        from .fec_identity_observations import map_filing_metadata

        return map_filing_metadata(row, job.prepared)
    raise ValueError(f"Unsupported prepared FEC identity mapping job: {job.kind}")


def map_context_rows(collections, *, source_generation_pin, filing_feed=False):
    """Keep RSS parts together so one item retains all contributing witnesses."""
    from .fec_research_context import map_filing_feed_contexts, map_research_context

    _digest(source_generation_pin)
    if filing_feed:
        yield map_filing_feed_contexts(collections, source_generation_pin)
    else:
        for collection in collections:
            yield map_research_context(collection, source_generation_pin)
