#!/usr/bin/env python3
"""Report whether each output-ledger row's qualified generation is still the live one.

The ledger records the audited generation per output as
``qualified at `<pin>…` (<YYYY-MM-DD>)``. Scheduled runs publish newer
generations before audit, so prose calling a pin "live" goes stale. A rollup
pin is the first 8 hex digits of its family's ``artifactDigest`` in
``publication.json``; a ``snapshot_<8 hex>`` pin names the materialized
rulemaking generation behind ``materialized/rulemaking/latest.json``. Read-only.
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path

import httpx
from dotenv import load_dotenv

from spicy_regs.public_url import resolve_r2_base_url
from spicy_regs.sources import publication

LEDGER = Path(__file__).resolve().parents[1] / "docs" / "research" / "fork-output-ledger-2026-09-21.md"
SNAPSHOT_POINTER = "materialized/rulemaking/latest.json"
FAILING = ("DRIFT", "NOT-LIVE", "MALFORMED")
_ROW = re.compile(r"\| T\d")
_CODE = re.compile(r"`([^`]+)`")
_PIN = re.compile(r"qualified at `((?:snapshot_)?[0-9a-f]{8})…` \((\d{4}-\d{2}-\d{2})\)")

Live = Mapping[str, tuple[str, int]]  # table -> (pin, rows)


def ledger_rows(text: str) -> Iterator[tuple[str, list[str], str]]:
    """Yield ``(task, tables, delivery state)`` for each ``| T<n>`` table row."""
    for line in text.splitlines():
        if not _ROW.match(line):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|", 3)]
        if len(cells) != 4:
            raise ValueError(f"Ledger row is not Task | Producer | Output | Delivery state: {line}")
        tables = [name for name in _CODE.findall(cells[2]) if "<" not in name and "/" not in name]
        yield cells[0], tables, cells[3]


def rollup_pins(index: Mapping) -> dict[str, tuple[str, int]]:
    """Map each indexed table to its family's short artifact pin and its row count."""
    return {
        key: (entry["artifactDigest"].removeprefix("sha256:")[:8], table["rows"])
        for entry in index["families"].values()
        for key, table in entry["tables"].items()
    }


def snapshot_pins(pointer: Mapping, manifest: Mapping) -> dict[str, tuple[str, int]]:
    """Map each public rulemaking artifact to the pointer's short snapshot pin and its row count."""
    if manifest.get("snapshot_id") != pointer["snapshot_id"]:
        raise publication.PublicationError("Rulemaking pointer and manifest name different snapshots")
    pin = pointer["snapshot_id"][: len("snapshot_") + 8]
    return {
        name: (pin, record["rows"])
        for name, record in manifest["artifacts"].items()
        if record.get("visibility", "public") == "public"
    }


def check(text: str, rollups: Live, snapshots: Live) -> list[tuple[str, str]]:
    """Return ``(status, detail)`` per ledger row.

    A state that mentions a qualified pin without exactly one conforming phrase
    (or a pinned row naming no table) is MALFORMED, so a convention slip cannot
    pass as an unpinned row.
    """
    results = []
    for task, tables, state in ledger_rows(text):
        pins = _PIN.findall(state)
        if len(pins) == 1 and tables:
            pin, day = pins[0]
            source = snapshots if pin.startswith("snapshot_") else rollups
            live = [source.get(table) for table in tables]
            status = "NOT-LIVE" if None in live else "OK" if all(hit[0] == pin for hit in live if hit) else "DRIFT"
            qualified = f"{pin} ({day})"
        else:
            live = [rollups.get(table) or snapshots.get(table) for table in tables]
            status = "NO-PIN" if not pins and "qualified at `" not in state else "MALFORMED"
            qualified = "-"
        live_pins = ",".join(dict.fromkeys(hit[0] for hit in live if hit)) or "-"
        rows = "/".join(f"{hit[1]:,}" if hit else "-" for hit in live) or "-"
        detail = f"{task} {', '.join(tables) or '-'}  qualified={qualified}  live={live_pins}  rows={rows}"
        results.append((status, detail))
    return results


def _get_json(url: str) -> dict | None:
    """GET one small public JSON object; a 404 means unpublished."""
    headers = {"User-Agent": "spicy-regs", "Cache-Control": "no-cache"}
    response = httpx.get(url, headers=headers, follow_redirects=True, timeout=60)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


def fetch_live(base_url: str) -> tuple[dict, dict]:
    """Read the publication index and the rulemaking pointer once each."""
    rollups = rollup_pins(publication.load_index(base_url))
    pointer = _get_json(f"{base_url}/{SNAPSHOT_POINTER}")
    if pointer is None:
        return rollups, {}
    manifest = _get_json(f"{base_url}/{pointer['manifest_key']}")
    if manifest is None:
        raise publication.PublicationError(f"Rulemaking pointer names a missing manifest: {pointer['manifest_key']}")
    return rollups, snapshot_pins(pointer, manifest)


def main(argv: Sequence[str] | None = None) -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, default=LEDGER)
    parser.add_argument("--index-url", help="Public base URL holding publication.json (default: R2_PUBLIC_URL)")
    args = parser.parse_args(argv)
    try:
        base_url = resolve_r2_base_url(args.index_url)
        results = check(args.ledger.read_text(encoding="utf-8"), *fetch_live(base_url))
    except (OSError, ValueError, KeyError, RuntimeError, httpx.HTTPError) as exc:
        print(f"Ledger pins could not be checked: {exc}", file=sys.stderr)
        return 1
    print(f"Ledger {args.ledger} against {base_url}")
    for status, detail in results:
        print(f"{status:<9} {detail}")
    counts = Counter(status for status, _ in results)
    print("\n" + " ".join(f"{status}={counts[status]}" for status in ("OK", "NO-PIN", *FAILING)))
    return 1 if any(counts[status] for status in FAILING) else 0


if __name__ == "__main__":
    sys.exit(main())
