"""Plan which superseded rollup generations to delete, reading only (owner decision 36, 2026-09-26).

Storage grows as runs times family bytes, so the owner chose to keep every
generation something cites and the last ``KEEP_LAST`` per family. Kept:

* the family's current generation and its predecessors, ``KEEP_LAST`` in
  all. Each root's ``readSnapshot`` is the index its publish swapped from, so
  its own family's entry there is the generation it replaced. A family the
  index no longer names keeps its newest ``KEEP_LAST`` by upload time;
* every generation that a hex prefix of eight or more digits anywhere under
  ``docs/`` names. The output ledger states its audits that way, and the MCP's
  qualification record is parsed from the ledger, so this covers both;
* every generation DocSpec pins in its ``docs/pins/fork-generations.json``;
* the managed parents a kept generation records, transitively, so its inputs
  stay readable;
* a generation that stopped being current less than ``GRACE`` ago, when its
  successor on the chain was written, since a run may still be reading it;
* a generation off the chain written after the current one: a publish uploads
  its prefix before it swaps the pointer, so it may be in flight. One off the
  chain and older than current lost its swap and was never read.

Everything else under ``generations/`` is planned for deletion. Source evidence
under ``source-evidence/`` is content-addressed and shared, and is not planned
here. Planning deletes nothing. ``execute`` takes a plan a person reviewed and
deletes only the prefixes a fresh plan also marks, so anything published,
pinned or cited since is spared. Within a prefix it deletes the members before
the root, so an interrupted run leaves a root that still links the chain. Each
execution leaves its record under ``RECORDS``.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from os import getenv
from pathlib import Path

from spicy_regs.output_ledger import LEDGER
from spicy_regs.sources import publication

PLAN_FORMAT = "spicy-regs-generation-retention-plan"
KEEP_LAST = 3
#: A rollup reads its parents through the index it captured at start, and a
#: GitHub-hosted job runs at most 6 hours, so a generation stays readable that
#: long after it stops being current.
GRACE = timedelta(hours=6)
PREFIX = "generations/"
ROOT = "artifact.json"
RECORDS = "retention/"
#: DeleteObjects accepts at most this many keys per request.
DELETE_BATCH = 1000
DOCS = LEDGER.parents[1]
DOCSPEC_PINS = "https://raw.githubusercontent.com/mikewolfd/DocSpec/main/docs/pins/fork-generations.json"
DOCSPEC_PINS_FORMAT = "docspec-fork-generation-pins"
_KEY = re.compile(r"generations/([a-z][a-z0-9_-]*)/([0-9a-f]{64})/")
_HEX = re.compile(r"(?<![0-9a-f])[0-9a-f]{8,64}(?![0-9a-f])")
_DIGEST = re.compile(r"sha256:([0-9a-f]{64})\Z")
_NEVER = datetime.min.replace(tzinfo=timezone.utc)


@dataclass
class Generation:
    family: str
    digest: str
    bytes: int = 0
    objects: int = 0
    newest: datetime = _NEVER
    keep: list[str] = field(default_factory=list)

    @property
    def prefix(self) -> str:
        return f"{PREFIX}{self.family}/{self.digest}"


def inventory(client, bucket: str) -> tuple[dict[str, Generation], list[str]]:
    """Every generation prefix in the bucket by digest, with its bytes; keys outside the layout come back apart."""
    found: dict[str, Generation] = {}
    stray: list[str] = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=PREFIX):
        for item in page.get("Contents", ()):
            match = _KEY.match(item["Key"])
            if match is None:
                stray.append(item["Key"])
                continue
            generation = found.setdefault(match[2], Generation(match[1], match[2]))
            if generation.family != match[1]:
                raise publication.PublicationError(f"Generation {match[2][:8]} is stored under two families")
            generation.bytes += item["Size"]
            generation.objects += 1
            generation.newest = max(generation.newest, item["LastModified"])
    return found, stray


def note_pins(notes: Iterable[tuple[str, str]], generations: Mapping[str, Generation]) -> dict[str, set[str]]:
    """Digests that a hex run of eight or more digits in a note prefixes, with the notes that name each."""
    by_prefix: dict[str, list[str]] = {}
    for digest in generations:
        by_prefix.setdefault(digest[:8], []).append(digest)
    named: dict[str, set[str]] = {}
    for name, text in notes:
        for token in _HEX.findall(text):
            for digest in by_prefix.get(token[:8], ()):
                if digest.startswith(token):
                    named.setdefault(digest, set()).add(name)
    return named


def docspec_pins(raw: bytes) -> dict[str, str]:
    """DocSpec's pinned digests and why each is held; a document outside the stated shape refuses."""
    try:
        document = json.loads(raw)
        if document["format"] != DOCSPEC_PINS_FORMAT or document["version"] != 1:
            raise ValueError("unknown format")
        pins = {}
        for pin in document["pins"]:
            match = _DIGEST.fullmatch(pin["artifactDigest"])
            if match is None or not isinstance(pin["family"], str):
                raise ValueError("invalid pin")
            pins[match[1]] = f"DocSpec {pin['role']} pin ({pin['cites']})"
    except (ValueError, TypeError, KeyError) as exc:
        raise publication.PublicationError("DocSpec pins are not a docspec-fork-generation-pins v1 document") from exc
    return pins


def plan(client, bucket: str, *, notes: Iterable[tuple[str, str]], docspec: Mapping[str, str],
         now: datetime) -> dict:
    """Mark every generation kept or deletable under decision 36 and return the plan as a JSON-able record."""
    from rulespec_artifacts import expected_artifact_digest, parse_canonical_json

    index, etag = publication._stored_index(client, bucket)
    generations, stray = inventory(client, bucket)
    roots: dict[str, dict | None] = {}

    def root(digest: str) -> dict | None:
        """The generation's own root, checked against its digest; ``None`` while it is still uploading."""
        if digest not in roots:
            stored = publication._get_bounded(client, bucket, f"{generations[digest].prefix}/{ROOT}")
            value = None if stored is None else parse_canonical_json(stored[0])
            if value is not None and expected_artifact_digest(value) != f"sha256:{digest}":
                raise publication.PublicationError(f"Root of {generations[digest].prefix} differs from its digest")
            roots[digest] = value
        return roots[digest]

    def keep(digest: str, reason: str) -> bool:
        generation = generations.get(digest)
        if generation is None:
            return False
        first = not generation.keep
        generation.keep.append(reason)
        return first

    by_family: dict[str, list[Generation]] = {}
    for generation in generations.values():
        by_family.setdefault(generation.family, []).append(generation)
    for family, members in by_family.items():
        entry = index["families"].get(family)
        if entry is None:
            for generation in sorted(members, key=lambda g: g.newest, reverse=True)[:KEEP_LAST]:
                keep(generation.digest, f"last {KEEP_LAST} by upload time: {family} is not in the index")
            continue
        current = entry["artifactDigest"].removeprefix("sha256:")
        if current not in generations:
            raise publication.PublicationError(f"Current {family} generation {current[:8]} is not in the listing")
        chain: list[str] = []
        step: str | None = current
        while step is not None and step in generations and step not in chain:
            chain.append(step)
            replaced = (root(step) or {}).get("spec", {}).get("readSnapshot", {}).get("families", {}).get(family)
            step = replaced["artifactDigest"].removeprefix("sha256:") if replaced else None
        for place, digest in enumerate(chain):
            superseded = generations[chain[place - 1]].newest if place else now
            if place < KEEP_LAST:
                keep(digest, "current" if place == 0 else f"last {KEEP_LAST}: {place} before current")
            elif now - superseded < GRACE:
                keep(digest, f"current until {superseded:%Y-%m-%dT%H:%MZ}, within the {GRACE.seconds // 3600}h grace")
        for generation in members:
            if generation.digest not in chain and generation.newest > generations[current].newest:
                keep(generation.digest, "written after the current generation: a publish may be in flight")
    for digest, names in note_pins(notes, generations).items():
        keep(digest, "named in " + ", ".join(sorted(names)))
    for digest, reason in docspec.items():
        keep(digest, reason)

    pending = [digest for digest, generation in generations.items() if generation.keep]
    while pending:
        child = generations[pending.pop()]
        for key, parent in ((root(child.digest) or {}).get("spec", {}).get("parents") or {}).items():
            if "artifactDigest" in parent:
                read = parent["artifactDigest"].removeprefix("sha256:")
                if keep(read, f"parent of {child.family}/{child.digest[:8]} ({key})"):
                    pending.append(read)

    families: dict[str, dict] = {}
    for generation in sorted(generations.values(), key=lambda g: (g.family, g.newest), reverse=True):
        family = families.setdefault(generation.family, {
            "current": (index["families"].get(generation.family) or {}).get("artifactDigest"), "generations": []})
        family["generations"].append({
            "digest": generation.digest, "bytes": generation.bytes, "objects": generation.objects,
            "newest": generation.newest.isoformat(),
            "keep": generation.keep,
        })
    kept = [g for g in generations.values() if g.keep]
    deleted = sorted((g for g in generations.values() if not g.keep), key=lambda g: g.prefix)
    return {
        "format": PLAN_FORMAT, "version": 1, "planned_at": now.isoformat(), "bucket": bucket,
        "index_etag": etag, "keep_last": KEEP_LAST,
        "totals": {
            "generations": len(generations), "bytes": sum(g.bytes for g in generations.values()),
            "keep_generations": len(kept), "keep_bytes": sum(g.bytes for g in kept),
            "delete_generations": len(deleted), "delete_bytes": sum(g.bytes for g in deleted),
        },
        "unlisted_docspec_pins": sorted(set(docspec) - set(generations)),
        "stray_keys": stray,
        "delete": [g.prefix for g in deleted],
        "families": dict(sorted(families.items())),
    }


def execute(client, bucket: str, approved: Mapping, fresh: Mapping) -> dict:
    """Delete what both the reviewed plan and a fresh one mark deletable, and store the record of it."""
    if approved.get("format") != PLAN_FORMAT or approved.get("version") != 1 or approved.get("bucket") != bucket:
        raise publication.PublicationError(f"The approved plan is not a {PLAN_FORMAT} v1 plan for {bucket}")
    doomed = sorted(set(approved["delete"]) & set(fresh["delete"]))
    for prefix in doomed:
        pages = client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{prefix}/")
        keys = sorted(item["Key"] for page in pages for item in page.get("Contents", ()))
        root = f"{prefix}/{ROOT}"
        members = [key for key in keys if key != root]
        for start in range(0, len(members), DELETE_BATCH):
            batch = members[start:start + DELETE_BATCH]
            failed = client.delete_objects(
                Bucket=bucket, Delete={"Objects": [{"Key": key} for key in batch], "Quiet": True}).get("Errors")
            if failed:
                raise publication.PublicationError(f"Deleting {prefix} failed for {len(failed)} objects")
        if root in keys:
            client.delete_object(Bucket=bucket, Key=root)
    record = {
        "format": "spicy-regs-generation-retention-record", "version": 1,
        "approved_plan": approved["planned_at"], "fresh_plan": fresh["planned_at"],
        "deleted": doomed, "spared": sorted(set(approved["delete"]) - set(doomed)),
    }
    client.put_object(Bucket=bucket, Key=f"{RECORDS}{fresh['planned_at']}.json", IfNoneMatch="*",
                      Body=json.dumps(record, indent=2).encode(), ContentType="application/json")
    return record


def summary(record: Mapping) -> list[str]:
    """A per-family table of the plan for a person to review."""
    gib = 1024 ** 3
    lines = ["| family | generations | keep | keep GiB | delete | delete GiB |", "|---|---:|---:|---:|---:|---:|"]
    for family, entry in record["families"].items():
        held = [g for g in entry["generations"] if g["keep"]]
        gone = [g for g in entry["generations"] if not g["keep"]]
        lines.append(f"| {family} | {len(entry['generations'])} | {len(held)} | "
                     f"{sum(g['bytes'] for g in held) / gib:.2f} | {len(gone)} | "
                     f"{sum(g['bytes'] for g in gone) / gib:.2f} |")
    totals = record["totals"]
    lines.append(f"| **all** | {totals['generations']} | {totals['keep_generations']} | "
                 f"{totals['keep_bytes'] / gib:.2f} | {totals['delete_generations']} | "
                 f"{totals['delete_bytes'] / gib:.2f} |")
    if record["unlisted_docspec_pins"]:
        lines.append(f"\nDocSpec pins no longer stored: {', '.join(d[:8] for d in record['unlisted_docspec_pins'])}")
    if record["stray_keys"]:
        lines.append(f"\nKeys under {PREFIX} outside the generation layout, left alone: {len(record['stray_keys'])}")
    return lines


def _notes() -> list[tuple[str, str]]:
    return [(str(path.relative_to(DOCS.parent)), path.read_text(encoding="utf-8"))
            for path in sorted(DOCS.rglob("*")) if path.suffix in {".md", ".json"} and path.is_file()]


def main(argv: Sequence[str] | None = None) -> int:
    from spicy_regs.sources import r2

    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("--output", type=Path, required=True, help="Write the JSON plan here")
    parser.add_argument("--docspec-pins", default=DOCSPEC_PINS, help="DocSpec's pin document, a URL or a path")
    parser.add_argument("--execute", type=Path, help="A reviewed plan: delete what it and this fresh plan both mark")
    args = parser.parse_args(argv)
    source = args.docspec_pins
    raw = (publication._bounded_get(source, allow_missing=False) if source.startswith("https://")
           else Path(source).read_bytes())
    assert raw is not None
    bucket = getenv("R2_BUCKET_NAME")
    if not bucket:
        parser.error("R2_BUCKET_NAME is not set")
    client = r2.get_r2_client()
    record = plan(client, bucket, notes=_notes(), docspec=docspec_pins(raw), now=datetime.now(timezone.utc))
    args.output.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    print("\n".join(summary(record)))
    if args.execute:
        done = execute(client, bucket, json.loads(args.execute.read_text(encoding="utf-8")), record)
        print(f"\nDeleted {len(done['deleted'])} generations; spared {len(done['spared'])} the fresh plan keeps. "
              f"Record: {RECORDS}{record['planned_at']}.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
