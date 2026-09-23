#!/usr/bin/env python3
"""Report whether each output-ledger row's qualified generation is still the live one.

The ledger records the audited generation per output as
``qualified at `<pin>…` (<YYYY-MM-DD>)``. Scheduled runs publish newer
generations before audit, so prose calling a pin "live" goes stale. A rollup
pin is the first 8 hex digits of its family's ``artifactDigest`` in
``publication.json``; a ``snapshot_<8 hex>`` pin names the materialized
rulemaking generation behind ``materialized/rulemaking/latest.json``. A base
object outside the index (dockets, documents, comments and its index) records
``verified at table digest `<8 hex>…` (<date>; ETag `<8 hex>…`)``, and its ETag is
compared with a HEAD of the public object. Read-only.
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
_BASE = re.compile(r"verified at table digest `[0-9a-f]{8}…` \((\d{4}-\d{2}-\d{2}); ETag `([0-9a-f]{8})…`\)")

Live = Mapping[str, tuple[str, int]]  # table -> (pin, rows), or object -> (ETag prefix, bytes)


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


def base_object_keys(text: str) -> list[str]:
    """The base objects whose ledger rows record an ETag, in row order."""
    return [table for _, tables, state in ledger_rows(text) if _BASE.search(state) for table in tables]


def check(text: str, rollups: Live, snapshots: Live, objects: Live | None = None) -> list[tuple[str, str]]:
    """Return ``(status, detail)`` per ledger row.

    A state that mentions a qualified pin without exactly one conforming phrase
    (or a pinned row naming no table) is MALFORMED, so a convention slip cannot
    pass as an unpinned row. A base object's row is checked by ETag when it
    records one, and is NO-PIN until it does.
    """
    objects = objects or {}
    results = []
    for task, tables, state in ledger_rows(text):
        pins, bases, unit = _PIN.findall(state), _BASE.findall(state), "rows"
        if len(pins) == 1 and tables:
            pin, day = pins[0]
            source = snapshots if pin.startswith("snapshot_") else rollups
        elif not pins and len(bases) == 1 and tables:
            (day, pin), source, unit = bases[0], objects, "bytes"
        else:
            live = [rollups.get(table) or snapshots.get(table) for table in tables]
            status = "NO-PIN" if not pins and "qualified at `" not in state else "MALFORMED"
            qualified = "-"
            results.append((status, _detail(task, tables, qualified, live, unit)))
            continue
        live = [source.get(table) for table in tables]
        status = "NOT-LIVE" if None in live else "OK" if all(hit[0] == pin for hit in live if hit) else "DRIFT"
        results.append((status, _detail(task, tables, f"{pin} ({day})", live, unit)))
    return results


def _detail(task: str, tables: list[str], qualified: str, live: list, unit: str) -> str:
    live_pins = ",".join(dict.fromkeys(hit[0] for hit in live if hit)) or "-"
    sizes = "/".join(f"{hit[1]:,}" if hit else "-" for hit in live) or "-"
    return f"{task} {', '.join(tables) or '-'}  qualified={qualified}  live={live_pins}  {unit}={sizes}"


def _get_json(url: str) -> dict | None:
    """GET one small public JSON object; a 404 means unpublished."""
    headers = {"User-Agent": "spicy-regs", "Cache-Control": "no-cache"}
    response = httpx.get(url, headers=headers, follow_redirects=True, timeout=60)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


def object_pins(base_url: str, keys: Sequence[str]) -> dict[str, tuple[str, int]]:
    """HEAD each public base object; map it to its ETag's first 8 hex digits and its size."""
    pins = {}
    headers = {"User-Agent": "spicy-regs", "Cache-Control": "no-cache"}
    for key in keys:
        response = httpx.head(f"{base_url}/{key}", headers=headers, follow_redirects=True, timeout=60)
        if response.status_code == 404:
            continue
        response.raise_for_status()
        pins[key] = (response.headers["ETag"].strip('"')[:8], int(response.headers["Content-Length"]))
    return pins


def fetch_live(base_url: str, ledger: str) -> tuple[dict, dict, dict]:
    """Read the publication index and the rulemaking pointer once each, and HEAD the ledger's base objects."""
    rollups = rollup_pins(publication.load_index(base_url))
    objects = object_pins(base_url, base_object_keys(ledger))
    pointer = _get_json(f"{base_url}/{SNAPSHOT_POINTER}")
    if pointer is None:
        return rollups, {}, objects
    manifest = _get_json(f"{base_url}/{pointer['manifest_key']}")
    if manifest is None:
        raise publication.PublicationError(f"Rulemaking pointer names a missing manifest: {pointer['manifest_key']}")
    return rollups, snapshot_pins(pointer, manifest), objects


def main(argv: Sequence[str] | None = None) -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, default=LEDGER)
    parser.add_argument("--index-url", help="Public base URL holding publication.json (default: R2_PUBLIC_URL)")
    args = parser.parse_args(argv)
    try:
        base_url = resolve_r2_base_url(args.index_url)
        ledger = args.ledger.read_text(encoding="utf-8")
        results = check(ledger, *fetch_live(base_url, ledger))
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
