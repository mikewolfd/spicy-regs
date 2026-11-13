"""Capture explicit White House windows and replay retained HTML into an ordinary generation.

Only nominations/appointments are discovered in this initial route. Existing
official records can replace an exact URL-and-HTML-digest match. Other possible
matches stay unresolved; no title matching or legal equivalence is inferred.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import date
import hashlib
import json
from pathlib import Path

import pyarrow as pa

from spicy_docs.sources.white_house import (
    BODY_SELECTOR,
    INDEX_URL,
    MAX_HTML_BYTES,
    PARSER_VERSION,
    WhiteHouseAcquirer,
    WhiteHouseError,
    parse_action,
    parse_index,
    select_window,
)
from spicy_regs.generations import build_generation
from spicy_regs.native_types import described_schema
from spicy_regs.parquet_rows import write_rows
from spicy_regs.pipelines.raw_releases import open_raw, raw_original
from spicy_regs.source_evidence import CaptureEvidence

FAMILY = "white-house-actions"
TABLE = "white_house_action_versions"
SCHEMA = pa.schema(
    [
        (name, pa.string())
        for name in (
            "action_version_id",
            "source_url",
            "native_post_id",
            "title",
            "document_type",
            "categories_json",
            "listing_url",
            "listing_sha256",
            "listing_record_index",
            "published_time",
            "display_time",
            "modified_time",
            "modified_time_status",
            "body_text",
            "body_html",
            "body_selector",
            "source_sha256",
            "observed_at",
            "parser_version",
            "official_match_status",
        )
    ]
)


def _json(value):
    return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def _read(store, source):
    size = source["bytes"]
    if type(size) is not int or not 0 < size <= MAX_HTML_BYTES:
        raise ValueError("White House capture exceeds HTML bounds")
    path = raw_original(store, source["sha256"], size)
    body = path.read_bytes()
    if len(body) != size or "sha256:" + hashlib.sha256(body).hexdigest() != source["sha256"]:
        raise ValueError("White House original differs from selected capture")
    return body, path


def _prior(selection):
    versions = {}
    for source in selection["actions"] + selection.get("prior_actions", []):
        versions.setdefault((source["url"], source["sha256"]), source)
    return list(versions.values())


def acquire(
    start: str,
    end: str,
    output_dir: Path,
    *,
    max_pages: int = 4,
    archive_root: Path | None = None,
    prior: dict | None = None,
    transport=None,
) -> dict:
    """Capture a descending window, aborting on a cap, source drift or refusal.

    Repeated exact originals keep the earlier observation; changed originals
    remain separate versions. Prior captures must select the same window.
    """
    if type(max_pages) is not int or not 1 <= max_pages <= 20:
        raise ValueError("max_pages must be between 1 and 20")
    if date.fromisoformat(start) > date.fromisoformat(end):
        raise ValueError("White House window starts after it ends")
    if prior and (prior["start"], prior["end"]) != (start, end):
        raise ValueError("Refresh requires the same declared White House window")
    evidence = CaptureEvidence(output_dir, FAMILY, archive_root=archive_root)
    indices, actions, pages = [], [], []
    try:
        retained = evidence.transport(transport, stage="white-house-html", max_bytes=MAX_HTML_BYTES)
        with WhiteHouseAcquirer(transport=retained, max_requests=max_pages * 11) as reader:

            def get(url, index=False):
                result, capture = reader.acquire(url, index=index)
                return result, {
                    "url": url,
                    "sha256": capture.sha256,
                    "bytes": capture.byte_size,
                    "observed_at": capture.observed_at,
                }

            url = INDEX_URL
            for _ in range(max_pages):
                page, descriptor = get(url, True)
                pages.append(page)
                indices.append(descriptor)
                # select_window performs the order, continuation and boundary checks.
                try:
                    selected = select_window(tuple(pages), start=start, end=end)
                except WhiteHouseError as error:
                    if str(error) != "White House window requires the next captured page":
                        raise
                    url = page.next_url
                    continue
                break
            else:
                raise WhiteHouseError("White House page cap reached before window boundary")
            for entry in selected:
                _, descriptor = get(entry.source_url)
                held = (
                    next(
                        (
                            s
                            for s in _prior(prior)
                            if s["url"] == descriptor["url"] and s["sha256"] == descriptor["sha256"]
                        ),
                        None,
                    )
                    if prior
                    else None
                )
                actions.append(held or descriptor)
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return {
        "format": "white-house-captures-v1",
        "start": start,
        "end": end,
        "indices": indices,
        "actions": actions,
        "prior_actions": _prior(prior) if prior else [],
        "coverage": "Observed category window only; not a historical completeness assertion.",
    }


def existing_route(source: dict, records: list[dict]) -> dict | None:
    """Reuse only proven representation identity. Titles cannot establish a match."""
    matches = []
    for row in records:
        if not all(row.get(k) for k in ("source_url", "source_sha256", "document_id", "route")):
            raise ValueError(
                "Existing route needs exact source URL, SHA-256, document ID and route; titles do not match"
            )
        if row["route"] not in {"federal-register", "govinfo", "administration-policy"}:
            raise ValueError("Unsupported existing official-document route")
        if (row["source_url"], row["source_sha256"]) == (source["url"], source["sha256"]):
            matches.append(row)
    if len(matches) > 1:
        raise ValueError("Ambiguous existing official representation identity")
    return matches[0] if matches else None


def build_from_captures(
    selection: dict, output_dir: Path, *, archive_root: Path | None = None, existing_records: list[dict] | None = None
):
    """Verify originals, reconcile the selected URLs, and build a source-evidenced local generation."""
    if selection.get("format") != "white-house-captures-v1":
        raise ValueError("Unsupported White House capture selection")
    if output_dir.exists():
        raise FileExistsError("White House candidate requires a fresh output directory")
    store = open_raw(archive_root)
    evidence = CaptureEvidence(output_dir, FAMILY, archive_root=archive_root)
    try:
        pages, files = [], {}
        for source in selection["indices"]:
            body, path = _read(store, source)
            pages.append(parse_index(body, url=source["url"]))
            files[source["url"], source["sha256"]] = (source, path)
        selected = {
            e.source_url: e for e in select_window(tuple(pages), start=selection["start"], end=selection["end"])
        }
        listing = {
            entry.source_url: (source, str(ordinal))
            for source, page in zip(selection["indices"], pages, strict=True)
            for ordinal, entry in enumerate(page.entries)
        }
        current = selection["actions"]
        if len({s["url"] for s in current}) != len(current) or {s["url"] for s in current} != set(selected):
            raise ValueError("White House current captures differ from the selected index window")
        rows, decisions = [], []
        for source in _prior(selection):
            identity = source["url"], source["sha256"]
            if source["url"] not in selected:
                raise ValueError("Prior White House version is outside the selected URL cohort")
            body, path = _read(store, source)
            action = parse_action(body, url=source["url"])
            entry = selected[source["url"]]
            if source in current and (action.title, action.display_time) != (entry.title, entry.published_time):
                raise ValueError("White House index and current article changed during capture")
            files[identity] = (source, path)
            route = existing_route(source, existing_records or [])
            decisions.append(
                {
                    "source_url": source["url"],
                    "source_sha256": source["sha256"],
                    "outcome": "reuse-existing" if route else "unresolved-distinct-source-version",
                    "existing": route,
                }
            )
            if route:
                continue
            row = asdict(action)
            listing_source, listing_ordinal = listing[source["url"]]
            row.update(
                {
                    "action_version_id": "sha256:" + hashlib.sha256("\n".join(identity).encode()).hexdigest(),
                    "native_post_id": entry.native_post_id,
                    "categories_json": json.dumps(entry.categories),
                    "listing_url": listing_source["url"],
                    "listing_sha256": listing_source["sha256"],
                    "listing_record_index": listing_ordinal,
                    "modified_time_status": "missing"
                    if action.modified_time is None
                    else "empty"
                    if action.modified_time == ""
                    else "stated",
                    "body_selector": BODY_SELECTOR,
                    "source_sha256": source["sha256"],
                    "observed_at": source["observed_at"],
                    "parser_version": PARSER_VERSION,
                    "official_match_status": "unresolved",
                }
            )
            rows.append(row)
        for source, path in files.values():
            evidence.retain_file(
                path, stage="white-house-html", original_location=source["url"], observed_at=source["observed_at"]
            )
            evidence.event("white-house-source", **source)
        evidence.event(
            "white-house-window",
            start=selection["start"],
            end=selection["end"],
            observed_index_pages=len(pages),
            selected_urls=len(selected),
            versions=len(rows),
            coverage="Observed source window; related official facts may exist; equivalence unresolved",
        )
        table = output_dir / (TABLE + ".parquet")
        write_rows(rows, table, SCHEMA)
        artifact = build_generation(
            output_dir / "generation",
            family=FAMILY,
            files=[table],
            expected_keys=[table.name],
            schemas={TABLE: described_schema(SCHEMA)},
            inputs=evidence.inputs(),
        )
        (output_dir / "route-decisions.json").write_text(_json(decisions))
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir, {"rows": len(rows), "decisions": decisions}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    acquire_cmd = commands.add_parser("acquire")
    acquire_cmd.add_argument("--start", required=True)
    acquire_cmd.add_argument("--end", required=True)
    acquire_cmd.add_argument("--max-pages", type=int, default=4)
    acquire_cmd.add_argument("--prior-captures", type=Path)
    acquire_cmd.add_argument("--captures-out", required=True, type=Path)
    build_cmd = commands.add_parser("build")
    build_cmd.add_argument("--captures", required=True, type=Path)
    build_cmd.add_argument(
        "--existing-records",
        type=Path,
        help="JSON list of exact representation identities from existing official routes",
    )
    for command in (acquire_cmd, build_cmd):
        command.add_argument("--output-dir", required=True, type=Path)
        command.add_argument("--archive-root", type=Path)
    args = parser.parse_args()
    if args.command == "acquire":
        prior = json.loads(args.prior_captures.read_text()) if args.prior_captures else None
        result = acquire(
            args.start, args.end, args.output_dir, max_pages=args.max_pages, archive_root=args.archive_root, prior=prior
        )
        args.captures_out.write_text(_json(result))
    else:
        artifact, evidence, result = build_from_captures(
            json.loads(args.captures.read_text()),
            args.output_dir,
            archive_root=args.archive_root,
            existing_records=json.loads(args.existing_records.read_text()) if args.existing_records else None,
        )
        result.update({"artifactDigest": artifact.pin.artifact_digest, "sourceEvidence": str(evidence)})
    print(_json(result))


if __name__ == "__main__":
    main()
