#!/usr/bin/env python3
"""Fail while a family's current generation journals a source read its build refused but published around.

A build that refuses one source and still publishes the rest of its family
reports success, so a refusal that repeats every run would stop that source
updating in silence. The laws rollup does this for Table III: a bulk file whose
member is renamed (it has been, twice since 2020), whose release point went
backwards, or that drops acts is refused, and ``laws`` and ``law_code_sections``
publish as usual. The build journals ``table3-bulk-refused``; this check reads
the current generation's source-evidence journal, each hop checked against the
digest above it, and fails while it holds one. A later run that reads the file
cleanly publishes a generation without the event, which clears the failure.

A declared family with no published generation, or whose generation retained no
source evidence, cannot show its refusals, so that fails too. Read-only.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence

import httpx

from spicy_regs.public_url import resolve_r2_base_url
from spicy_regs.source_evidence import INPUT_ROLE
from spicy_regs.sources import publication

#: Family -> the journal events that mean its build refused a source read and published the rest.
REFUSAL_EVENTS: dict[str, tuple[str, ...]] = {"laws": ("table3-bulk-refused",)}


def refusals(base_url: str, index: Mapping, declared: Mapping[str, tuple[str, ...]] = REFUSAL_EVENTS) -> list[str]:
    """One human-readable failure per refusal the current generations journal, or per family that cannot show them."""
    failures: list[str] = []
    for family, events in declared.items():
        entry = index["families"].get(family)
        if entry is None:
            failures.append(f"{family}: no published generation, so its source refusals cannot be read")
            continue
        _, root = publication.load_family_root(base_url, entry)
        pin = next((item for item in root["inputs"] if item.get("role") == INPUT_ROLE), None)
        if pin is None:
            failures.append(f"{family}: generation {entry['artifactDigest']} retained no source evidence")
            continue
        found = [event for line in publication.load_evidence_journal(base_url, pin).splitlines()
                 if (event := json.loads(line)).get("event") in events]
        print(f"{'REFUSED' if found else 'OK'}: {family} generation {entry['artifactDigest']} — "
              f"{len(found)} of {', '.join(events)}")
        for event in found:
            detail = {key: value for key, value in event.items() if key not in ("event", "recorded_at")}
            failures.append(f"{family}: {event['event']} at {event.get('recorded_at')}: {json.dumps(detail)}")
    return failures


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url")
    args = parser.parse_args(argv)
    base_url = resolve_r2_base_url(args.base_url)
    try:
        with publication.snapshot(base_url) as index:
            failures = refusals(base_url, index)
    except (httpx.HTTPError, ValueError, KeyError, publication.PublicationError) as exc:
        print(f"Source refusals could not be read: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    if failures:
        print("\nSource refusals:")
        for failure in failures:
            print(f"- {failure}")
        return 1
    print("\nNo current generation journals a refused source read.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
