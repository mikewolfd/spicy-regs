"""Recover newly exposed meeting references from exact retained detail responses.

This reads the selected prior's evidence history, never the Congress API. It
changes only the two added fields; all other processing facts stay unchanged.
"""
from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Callable, Mapping, cast
from urllib.parse import urlsplit

import pyarrow.parquet as pq
from spicy_docs.reading.json_input import load_decimal_json
from spicy_docs.reading.paged_json import PagedJsonSourceError
from spicy_docs.schemas.congress_index_tables import shape_committee_meeting

from spicy_regs.source_evidence import SourceEvidenceError
from spicy_regs.sources import publication
from spicy_regs.transforms.table_merge import set_column, write_in_place

FIELDS = ("nomination_references_json", "treaty_references_json")
IDENTITY = ("congress", "chamber", "event_id")
FAMILY = "committee-meetings"
MAX_HISTORY = 32
MAX_CAPTURES = 20_000
MAX_BODY_BYTES = 128 * 2**20


class _ReplayBound(RuntimeError):
    """A local replay limit leaves the row available to the bounded API queue."""


def _checked(raw: bytes, expected: str, size: int | None = None) -> bytes:
    if ("sha256:" + hashlib.sha256(raw).hexdigest() != expected
            or (size is not None and len(raw) != size)):
        raise SourceEvidenceError("Retained meeting evidence differs from its declared bytes")
    return raw


def retained_details(pin: Mapping, fetch: Callable[[str], bytes], wanted: set[tuple]) -> list[dict]:
    """Check each generation, source root, member manifest and journal in its pinned history."""
    captures, seen = [], set()
    entry = dict(pin)
    for _ in range(MAX_HISTORY):
        if entry["artifactDigest"] in seen:
            raise SourceEvidenceError("Repeated meeting evidence generation")
        seen.add(entry["artifactDigest"])
        root = publication.family_root(fetch(entry["prefix"] + "/artifact.json"), entry)
        if root["spec"]["family"] != FAMILY:
            raise SourceEvidenceError("Retained meeting history changed family")
        for source in (item for item in root["inputs"] if item.get("role") == "source-evidence"):
            prefix = "source-evidence/" + source["artifactDigest"].removeprefix("sha256:")
            source_root = publication.family_root(fetch(prefix + "/artifact.json"), source)
            if source_root["spec"].get("family") != FAMILY or source_root["spec"].get("outcome") != "build-complete":
                raise SourceEvidenceError("Retained meeting source has a different family or incomplete build")
            members = []
            for descriptor in source_root["memberManifests"]:
                raw = _checked(fetch(prefix + "/" + descriptor["objectKey"]), descriptor["sha256"], descriptor["byteSize"])
                members.extend(json.loads(raw)["members"])
            declared = {member["objectKey"]: member for member in members}
            journal = declared.get("journal.jsonl")
            if journal is None:
                raise SourceEvidenceError("Retained meeting evidence has no declared journal")
            raw = _checked(fetch(prefix + "/journal.jsonl"), journal["sha256"], journal["byteSize"])
            for position, line in enumerate(raw.splitlines(), 1):
                event = json.loads(line)
                url = urlsplit(event.get("requested_url", ""))
                match = re.fullmatch(r"/v3/committee-meeting/(\d+)/(house|senate|nochamber)/(\d+)", url.path)
                if (event.get("event") != "capture" or event.get("method") != "GET"
                        or url.scheme != "https" or url.hostname != "api.congress.gov" or not match
                        or match.groups() not in wanted or event.get("status_code") != 200
                        or event.get("response_complete", True) is not True
                        or event.get("body_retained", source_root["spec"].get("evidence_version") is None) is not True):
                    continue
                digest = event.get("sha256", "")
                if not re.fullmatch(r"sha256:[a-f0-9]{64}", digest):
                    raise SourceEvidenceError("Retained meeting detail has no exact body digest")
                key = "blobs/sha256/" + digest.removeprefix("sha256:")
                body = declared.get(key)
                if (body is None or body["sha256"] != digest or body["byteSize"] != event.get("byte_size")
                        or event.get("blob_member", key) != key):
                    raise SourceEvidenceError("Retained meeting detail is not declared by its source artifact")
                captures.append({"identity":match.groups(), "key":"source-evidence/" + key,
                                 "sha256":digest, "byteSize":body["byteSize"],
                                 "sourceEvidenceDigest":source["artifactDigest"], "journalPosition":position,
                                 "captureId":event.get("capture_id"), "observedAt":event.get("observed_at"),
                                 "requestedUrl":event["requested_url"]})
                if len(captures) > MAX_CAPTURES:
                    raise _ReplayBound("retained-capture-limit")
        priors = [item for item in root["inputs"] if item.get("role") == "prior-generation"]
        if not priors:
            return captures
        if len(priors) != 1:
            raise SourceEvidenceError("Meeting history has ambiguous prior generations")
        entry = {**priors[0], "prefix":"generations/" + FAMILY + "/" + priors[0]["artifactDigest"].removeprefix("sha256:")}
    raise _ReplayBound("retained-history-limit")


def replay_meeting_references(prior: Path, wanted: set[tuple], evidence, *, base_url: str,
                              fetch: Callable[[str], bytes] | None = None) -> tuple[set[tuple], set[tuple]]:
    """Repair qualified missing fields; keep conflicting observations explicitly ambiguous."""
    pin = (getattr(evidence, "read_snapshot", None) or {}).get("families", {}).get(FAMILY)
    if not wanted or pin is None or not base_url:
        return set(), set()
    if fetch is None:
        def fetch(key):
            raw = publication._bounded_get(base_url.rstrip("/") + "/" + key,
                                           allow_missing=False, limit=publication.EVIDENCE_CONTROL_LIMIT)
            assert raw is not None
            return raw
    table = pq.read_table(prior)
    rows = table.to_pylist()
    held = {tuple(row[name] for name in IDENTITY):row for row in rows}
    if len(held) != len(rows) or not wanted <= held.keys():
        raise SourceEvidenceError("Meeting replay selection differs from the held identities")
    try:
        captures = retained_details(pin, fetch, wanted)
        unique = {capture["sha256"]:capture for capture in captures}
        if sum(capture["byteSize"] for capture in unique.values()) > MAX_BODY_BYTES:
            raise _ReplayBound("retained-body-byte-limit")
    except _ReplayBound as error:
        # A partial history cannot qualify one observation over a possible
        # conflicting older response. Keep every selected row unresolved.
        for key in sorted(wanted):
            evidence.event("committee-meeting-reference-replay", **dict(zip(IDENTITY, key, strict=True)),
                           outcome="unavailable", source_update_date=held[key]["update_date"],
                           variants=[], refusals=[str(error)])
        return set(), set()

    def shape(capture):
        raw = _checked(fetch(capture["key"]), capture["sha256"], capture["byteSize"])
        try:
            document = load_decimal_json(raw, source="retained Congress meeting", error_type=PagedJsonSourceError)
            detail = cast(Mapping[str, Any], document).get("committeeMeeting") if isinstance(document, Mapping) else None
            if not isinstance(detail, Mapping) or not detail:
                raise ValueError("Retained response has no one meeting detail")
            return capture["sha256"], shape_committee_meeting(detail, detail)
        except (ValueError, TypeError, PagedJsonSourceError):
            return capture["sha256"], None

    with ThreadPoolExecutor(max_workers=publication.EVIDENCE_WORKERS) as pool:
        shaped = dict(pool.map(shape, unique.values()))
    variants = defaultdict(lambda:defaultdict(list))
    refused = defaultdict(set)
    for capture in captures:
        key, candidate = capture["identity"], shaped[capture["sha256"]]
        source = held[key]
        was_read = (source.get("detail_read") == "true"
                    or (source.get("detail_read") is None and source.get("committees_json") is not None))
        if (candidate is None or tuple(candidate[name] for name in IDENTITY) != key
                or not source.get("update_date") or candidate["update_date"] != source["update_date"] or not was_read):
            refused[key].add("identity-version-or-read-state")
            continue
        differing = [name for name in source if name not in (*FIELDS, "url", "detail_read")
                     and source[name] != candidate.get(name)]
        if differing:
            refused[key].add("source-facts:" + ",".join(sorted(differing)))
            continue
        values = tuple(candidate[name] for name in FIELDS)
        variants[key][values].append({name:value for name, value in capture.items() if name not in ("identity", "key")})
    resolved, ambiguous = set(), set()
    changed = 0
    for key in sorted(wanted):
        observations = variants[key]
        values = next(iter(observations)) if len(observations) == 1 else None
        conflict = values is not None and any(held[key].get(name) is not None and held[key][name] != value
                                             for name, value in zip(FIELDS, values, strict=True))
        outcome = "ambiguous" if len(observations) > 1 or conflict else "replayed" if values is not None else "unavailable"
        if outcome == "ambiguous":
            ambiguous.add(key)
        elif outcome == "replayed":
            assert values is not None
            resolved.add(key)
            for name, value in zip(FIELDS, values, strict=True):
                if held[key].get(name) is None:
                    changed += value is not None
                    held[key][name] = value
        evidence.event("committee-meeting-reference-replay", **dict(zip(IDENTITY, key, strict=True)),
                       outcome=outcome, source_update_date=held[key]["update_date"],
                       variants=[{"references":dict(zip(FIELDS, value, strict=True)), "witnesses":witnesses}
                                 for value, witnesses in observations.items()], refusals=sorted(refused[key]))
    if changed:
        for name in FIELDS:
            table = set_column(table, name, [row.get(name) for row in rows])
        write_in_place(table, prior)
    return resolved, ambiguous
