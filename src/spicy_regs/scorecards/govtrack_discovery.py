"""Reconcile discovery metadata without modifying publisher facts or source status.

Run ``python -m spicy_regs.scorecards.govtrack_discovery --help`` for the
private capture and offline review workflow. Source parsing lives in SpicyDocs.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path

DEFAULT_COMMIT = "fa63a2b5326edd4b8835386082c2317627f058ce"
REPOSITORY = "govtrack/advocacy-organization-scorecards"


class DiscoveryReconciliationError(ValueError):
    """The discovery review cannot be produced without an explicit decision."""


def _instant(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value)
    except (ValueError, TypeError) as error:
        raise DiscoveryReconciliationError("import time must be an ISO timestamp") from error
    if parsed.tzinfo is None:
        raise DiscoveryReconciliationError("import time must have a timezone")
    return value


def _name(value: str) -> str:
    return " ".join(value.split()).casefold()


def reconcile(leads: list[dict], catalog: dict, aliases: dict, *, imported_at: str) -> dict:
    """Return matches and proposed additions; input status/evidence stays untouched."""
    _instant(imported_at)
    if not isinstance(catalog, dict) or not isinstance(aliases, dict):
        raise DiscoveryReconciliationError("catalog and aliases must be objects")
    sources = catalog.get("sources")
    if not isinstance(sources, list) or not sources:
        raise DiscoveryReconciliationError("catalog requires nonempty sources")
    by_id, names, urls = defaultdict(list), defaultdict(set), defaultdict(set)
    source_ids = set()
    for source in sources:
        if not isinstance(source, dict):
            raise DiscoveryReconciliationError("catalog source must be an object")
        publisher = source.get("publisher_id")
        if not isinstance(publisher, str) or not publisher:
            raise DiscoveryReconciliationError("catalog publisher ID is missing")
        source_id = source.get("source_id")
        if not isinstance(source_id, str) or not source_id or source_id in source_ids:
            raise DiscoveryReconciliationError("catalog source IDs must be nonempty and unique")
        source_ids.add(source_id)
        if not isinstance(source.get("publisher_name"), str) or not source["publisher_name"].strip():
            raise DiscoveryReconciliationError("catalog publisher name is missing")
        by_id[publisher].append(source)
        other_names = source.get("publisher_aliases", [])
        if not isinstance(other_names, list) or any(not isinstance(n, str) or not n.strip() for n in other_names):
            raise DiscoveryReconciliationError("catalog aliases must be nonempty text")
        for name in [source["publisher_name"], *other_names]:
            names[_name(name)].add(publisher)
        for field in ("homepage_url", "scorecard_index_url", "archive_url"):
            if source.get(field):
                urls[source[field]].add(publisher)
    if (
        set(aliases) != {"version", "rules"}
        or type(aliases["version"]) is not int
        or aliases["version"] != 1
        or not isinstance(aliases["rules"], list)
    ):
        raise DiscoveryReconciliationError("aliases require version 1 and rules")
    rules = {}
    for rule in aliases["rules"]:
        if not isinstance(rule, dict) or set(rule) != {"repository_path", "publisher_id", "reason"}:
            raise DiscoveryReconciliationError("alias must state path, publisher and reason")
        if (
            not isinstance(rule["repository_path"], str)
            or not re.fullmatch(r"scorecards/[A-Za-z0-9_.-]+\.yaml", rule["repository_path"])
            or not isinstance(rule["publisher_id"], str)
            or rule["repository_path"] in rules
            or rule["publisher_id"] not in by_id
            or not isinstance(rule["reason"], str)
            or not rule["reason"].strip()
        ):
            raise DiscoveryReconciliationError("duplicate, unknown or unexplained alias")
        rules[rule["repository_path"]] = rule
    if not leads:
        raise DiscoveryReconciliationError("empty discovery is not an accepted source absence")
    results, seen, commits, paths = [], set(), set(), set()
    for lead in leads:
        allowed = {
            "discovery_id",
            "repository",
            "commit",
            "repository_path",
            "repository_blob_sha1",
            "capture_sha256",
            "metadata_sha256",
            "metadata_line_end",
            "observed_at",
            "name",
            "abbreviation",
            "homepage_url",
            "original_scorecard_url",
            "updated_text",
            "period_text",
            "rating_unit_text",
            "discovered_via",
        }
        needed = {
            "discovery_id",
            "repository",
            "commit",
            "repository_path",
            "name",
            "homepage_url",
            "original_scorecard_url",
            "updated_text",
            "observed_at",
            "discovered_via",
            "capture_sha256",
        }
        if (
            not isinstance(lead, dict)
            or set(lead) != allowed
            or any(not isinstance(lead[k], str) or not lead[k] for k in needed)
        ):
            raise DiscoveryReconciliationError("discovery metadata is incomplete")
        if lead["discovery_id"] in seen or lead["repository_path"] in paths or lead["repository"] != REPOSITORY:
            raise DiscoveryReconciliationError("discovery repeats identity or changes repository")
        seen.add(lead["discovery_id"])
        paths.add(lead["repository_path"])
        commits.add(lead["commit"])
        _instant(lead["observed_at"])
        candidates = (
            names.get(_name(lead["name"]), set())
            | urls.get(lead["homepage_url"], set())
            | urls.get(lead["original_scorecard_url"], set())
        )
        rule = rules.get(lead["repository_path"])
        if rule:
            candidates = candidates | {rule["publisher_id"]}
        candidates = sorted(candidates)
        status = "ambiguous" if len(candidates) > 1 else "matched" if candidates else "new_lead"
        publisher = candidates[0] if status == "matched" else None
        addition = None
        if status == "new_lead":
            proposed_id = "govtrack_" + hashlib.sha256(lead["repository_path"].encode()).hexdigest()[:16]
            if proposed_id in by_id:
                raise DiscoveryReconciliationError("proposed identity collides with catalog")
            addition = {
                "publisher_id": proposed_id,
                "publisher_name": lead["name"],
                "homepage_url": lead["homepage_url"],
                "scorecard_index_url": lead["original_scorecard_url"],
                "discovery_status": "discovered",
                "rights_status": "unreviewed",
                "discovered_at": imported_at,
                "last_verified_at": None,
            }
        results.append(
            {
                "discovery": lead,
                "imported_at": imported_at,
                "reconciliation_status": status,
                "candidate_publisher_ids": candidates,
                "candidate_count": len(candidates),
                "publisher_id": publisher,
                "rule": "explicit_alias_with_conflict_check"
                if rule
                else "exact_name_or_literal_url"
                if candidates
                else "no_exact_match",
                "alias_reason": rule["reason"] if rule else None,
                "catalog_status_at_import": by_id[publisher][0].get("discovery_status")
                if publisher and len(by_id[publisher]) == 1
                else None,
                "catalog_sources_at_import": [
                    {key: source.get(key) for key in ("source_id", "scorecard_name", "discovery_status")}
                    for source in by_id[publisher]
                ]
                if publisher
                else [],
                "proposed_addition": addition,
                "proposed_discovery_reference": {
                    "kind": "govtrack_pinned_metadata",
                    "url": lead["discovered_via"],
                    "repository": REPOSITORY,
                    "commit": lead["commit"],
                    "path": lead["repository_path"],
                    "sha256": lead["capture_sha256"],
                    "observed_at": lead["observed_at"],
                    "imported_at": imported_at,
                },
                "updated_semantics": "upstream publication-or-import date; never promoted to publisher publication/verification time",
                "original_publisher_verified_by_this_import": False,
            }
        )
    if len(commits) != 1 or not re.fullmatch(r"[0-9a-f]{40}", next(iter(commits))):
        raise DiscoveryReconciliationError("discovery must come from one full repository commit")
    if set(rules) - paths:
        raise DiscoveryReconciliationError("alias contains an absent repository path; review changed input membership")
    groups = defaultdict(list)
    for row in results:
        if row["publisher_id"]:
            groups[row["publisher_id"]].append(row["discovery"]["discovery_id"])
    return {
        "schema_version": 1,
        "dataset": "govtrack_scorecard_discovery_review",
        "repository": REPOSITORY,
        "commit": next(iter(commits)),
        "imported_at": imported_at,
        "authority": {
            "ratings_imported": False,
            "canonical_catalog_modified": False,
            "production_status_modified": False,
            "original_links_fetched": False,
        },
        "counts": dict(Counter(row["reconciliation_status"] for row in results)),
        "duplicate_lead_groups": [
            {"publisher_id": p, "discovery_ids": ids} for p, ids in sorted(groups.items()) if len(ids) > 1
        ],
        "results": results,
    }


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        json.dump(value, stream, indent=2, ensure_ascii=False)
        stream.write("\n")
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _failure_metadata(error: Exception, *, commit: str) -> dict:
    """Keep allowlisted failure evidence separately; never retain response bodies."""
    from spicy_docs.reading.refusals import RefusedResponse
    from spicy_docs.sources.scorecards.govtrack_discovery import MAX_BYTES
    from spicy_docs.transport.captured import attached_capture

    result: dict = {"error_type": type(error).__name__, "body_retained": False}
    context = getattr(error, "govtrack_discovery", None)
    if isinstance(context, dict) and context.get("commit") == commit:
        path = context.get("path")
        if isinstance(path, str) and (
            path in {"commit", "tree"} or re.fullmatch(r"scorecards/[A-Za-z0-9_.-]+\.yaml", path)
        ):
            result["context"] = {"commit": commit, "path": path}
            count = context.get("requestCount")
            if type(count) is int and count >= 0:
                result["context"]["request_count"] = count
    allowed_url = re.compile(
        rf"https://(?:api\.github\.com/repos/{re.escape(REPOSITORY)}/git/(?:commits/[0-9a-f]{{40}}|trees/[0-9a-f]{{40}}\?recursive=1)|raw\.githubusercontent\.com/{re.escape(REPOSITORY)}/{commit}/scorecards/[A-Za-z0-9_.-]+\.yaml)"
    )
    capture = attached_capture(error)
    refused = getattr(error, "refused_response", None)
    http_response = getattr(error, "response", None)
    url = (
        capture.requested_url
        if capture
        else refused.request_key
        if isinstance(refused, RefusedResponse)
        else str(getattr(http_response, "url", ""))
    )
    if isinstance(url, str) and allowed_url.fullmatch(url):
        result["requested_url"] = url
    status = capture.status_code if capture else getattr(http_response, "status_code", None)
    if type(status) is int and 100 <= status <= 599:
        result["http_status"] = status
    if capture:
        result.update(sha256=capture.sha256, byte_size=capture.byte_size, observed_at=capture.observed_at)
        if capture.content_type and len(capture.content_type) < 200 and all(ord(c) >= 32 for c in capture.content_type):
            result["content_type"] = capture.content_type
    elif isinstance(refused, RefusedResponse):
        if refused.response_bytes is not None and len(refused.response_bytes) <= MAX_BYTES:
            result.update(
                sha256="sha256:" + hashlib.sha256(refused.response_bytes).hexdigest(),
                byte_size=len(refused.response_bytes),
            )
        if type(refused.observed_byte_size) is int and refused.observed_byte_size >= 0:
            result["observed_byte_size"] = refused.observed_byte_size
    return result


def acquire(directory: Path, *, commit: str = DEFAULT_COMMIT) -> dict:
    """Keep raw mixed files only in caller-selected private retained storage."""
    from spicy_docs.sources.scorecards.govtrack_discovery import GovTrackDiscoveryAcquirer

    directory.mkdir(parents=True, exist_ok=True)
    if any(directory.iterdir()):
        raise DiscoveryReconciliationError("capture directory must be empty; preserve earlier attempts")
    capture_rows: list[dict] = []
    manifest: dict = {
        "schema_version": 1,
        "commit": commit,
        "status": "incomplete",
        "captures": capture_rows,
        "body_retained_publicly": False,
    }

    def retain(key, response):
        filename = f"{len(capture_rows):04d}.body"
        (directory / filename).write_bytes(response.body)
        fields = {
            name: getattr(response, name)
            for name in (
                "requested_url",
                "resolved_url",
                "status_code",
                "content_type",
                "observed_at",
                "content_encoding",
                "method",
            )
        }
        capture_rows.append({"key": key, "body_file": filename, "sha256": response.sha256, **fields})
        _write_json(directory / "manifest.json", manifest)

    try:
        with GovTrackDiscoveryAcquirer() as source:
            source.acquire(commit=commit, retain=retain)
        manifest["status"] = "complete"
    except Exception as error:
        manifest["status"] = "failed"
        manifest["failed_attempt"] = _failure_metadata(error, commit=commit)
        _write_json(directory / "manifest.json", manifest)
        raise
    _write_json(directory / "manifest.json", manifest)
    return manifest


def load_retained(directory: Path) -> tuple[list[dict], dict]:
    from spicy_docs.sources.scorecards.govtrack_discovery import MAX_BYTES, MAX_FILES, parse_retained
    from spicy_docs.transport.captured import CapturedBodyResponse

    manifest_path = directory / "manifest.json"
    with manifest_path.open("rb") as stream:
        raw = stream.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise DiscoveryReconciliationError("retained manifest exceeds bound")
    manifest = json.loads(raw)
    if (
        not isinstance(manifest, dict)
        or manifest.get("schema_version") != 1
        or manifest.get("status") != "complete"
        or manifest.get("body_retained_publicly") is not False
    ):
        raise DiscoveryReconciliationError("retained acquisition is not a complete private capture")
    rows = manifest.get("captures")
    if not isinstance(rows, list) or not 2 < len(rows) <= MAX_FILES + 2:
        raise DiscoveryReconciliationError("retained membership is outside bounds")
    captures = {}
    for row in rows:
        if (
            not isinstance(row, dict)
            or not isinstance(row.get("body_file"), str)
            or not re.fullmatch(r"[0-9]{4}\.body", row["body_file"])
        ):
            raise DiscoveryReconciliationError("retained body filename is unsafe")
        path = directory / row["body_file"]
        if path.is_symlink() or path.resolve().parent != directory.resolve():
            raise DiscoveryReconciliationError("retained body left its directory")
        with path.open("rb") as stream:
            body = stream.read(MAX_BYTES + 1)
        if (
            len(body) > MAX_BYTES
            or "sha256:" + hashlib.sha256(body).hexdigest() != row["sha256"]
            or row["key"] in captures
        ):
            raise DiscoveryReconciliationError("retained body hash, size or membership differs")
        fields = {
            key: row[key]
            for key in (
                "requested_url",
                "resolved_url",
                "status_code",
                "content_type",
                "observed_at",
                "content_encoding",
                "method",
            )
        }
        captures[row["key"]] = CapturedBodyResponse(body=body, **fields)
    leads = parse_retained(captures, commit=manifest["commit"])
    return [lead.to_dict() for lead in leads], {
        "manifest_sha256": hashlib.sha256(raw).hexdigest(),
        "commit": manifest["commit"],
        "capture_count": len(captures),
    }


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    capture = commands.add_parser("acquire")
    capture.add_argument("--retained-dir", type=Path, required=True)
    capture.add_argument("--commit", default=DEFAULT_COMMIT)
    review = commands.add_parser("reconcile")
    review.add_argument("--retained-dir", type=Path, required=True)
    review.add_argument("--catalog", type=Path, required=True)
    review.add_argument("--aliases", type=Path, required=True)
    review.add_argument("--output", type=Path, required=True)
    review.add_argument("--imported-at", default=datetime.now(UTC).isoformat())
    args = parser.parse_args(argv)
    if args.command == "acquire":
        result = acquire(args.retained_dir, commit=args.commit)
        print(json.dumps({"status": result["status"], "capture_count": len(result["captures"])}))
        return
    if (
        args.output.resolve() in {args.catalog.resolve(), args.aliases.resolve()}
        or args.retained_dir.resolve() in args.output.resolve().parents
    ):
        raise DiscoveryReconciliationError("review output must not overwrite source inputs")
    leads, receipt = load_retained(args.retained_dir)
    catalog_raw, aliases_raw = args.catalog.read_bytes(), args.aliases.read_bytes()
    result = reconcile(leads, json.loads(catalog_raw), json.loads(aliases_raw), imported_at=args.imported_at)
    result["input_pins"] = {
        "catalog_sha256": hashlib.sha256(catalog_raw).hexdigest(),
        "aliases_sha256": hashlib.sha256(aliases_raw).hexdigest(),
        **receipt,
    }
    _write_json(args.output, result)
    print(json.dumps(result["counts"]))


if __name__ == "__main__":
    main()
