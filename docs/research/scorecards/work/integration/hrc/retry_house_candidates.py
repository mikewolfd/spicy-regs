"""Retry only source-mismatching HRC member scopes as smaller unchanged-data images."""

from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event

from house_source_validation import source_inventory
from probe_gemini_house_bands import observe
from spicy_docs.transport.credentials import read_api_key


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", type=Path, required=True)
    p.add_argument("--audit", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--credentials", type=Path, required=True)
    p.add_argument("--resolve-ip")
    p.add_argument("--workers", type=int, default=4, choices=range(1, 5))
    args = p.parse_args()
    args.stop_event = Event()
    source = args.source.read_bytes()
    inventory = source_inventory(source)
    audit = json.loads(args.audit.read_text())
    if audit["source_sha256"] != inventory["source_sha256"]:
        raise ValueError("source/audit mismatch")
    args.output.mkdir(parents=True, exist_ok=False)
    selections = sorted({(r["physical_page"], row) for r in audit["reports"] for row in r["retry_source_rows"]})
    (args.output / "selected-source-rows.json").write_text(json.dumps(selections, indent=2))
    jobs = []
    for number, row_number in selections:
        page = next(p for p in inventory["pages"] if p["physical_page"] == number)
        row = next(r for r in page["rows"] if r["source_row"] == row_number)
        jobs.append((page, [row]))
    credential = read_api_key(args.credentials, "GEMINI_API_KEY")
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        results = list(pool.map(lambda job: observe(args, source, job[0], job[1], credential), jobs))
    (args.output / "receipts.json").write_text(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
