"""Observe bounded House row bands using paired source crops and original headers.

This diagnostic reuses the installed structured Gemini API. Source geometry and
native text guide input selection and independent checks; the model receives
source images only and returns semantic records. No outcome is auto-qualified.
"""

from __future__ import annotations

import argparse
import json
import pickle
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from hashlib import sha256
from importlib.metadata import version
from pathlib import Path
from threading import Event

from house_source_validation import compare, source_inventory
from probe_gemini import SOURCE_SHA256, resolved_client
from spicy_docs.extraction import DefaultReader
from spicy_docs.extraction.gemini import Gemini, GeminiClient
from spicy_docs.extraction.model import Box
from spicy_docs.extraction.pages import crop
from spicy_docs.sources.scorecards.hrc_outcomes import (
    HOUSE_ITEMS,
    RATING_PERIODS,
    SCHEMA_VERSION,
    outcome_prompt,
    outcome_schema,
)
from spicy_docs.transport.credentials import CredentialRefusedError, read_api_key


def observe(args, source, page, rows, credential):
    number = page["physical_page"]
    if args.stop_event.is_set():
        return {"status": "skipped_after_credential_refusal", "physical_page": number}
    output = args.output / f"page-{number}-rows-{rows[0]['source_row']}-{rows[-1]['source_row']}"
    output.mkdir(parents=True, exist_ok=False)
    y0 = max(0, (rows[0]["y_center"] - 9) / page["height"])
    y1 = min(1, (rows[-1]["y_center"] + 9) / page["height"])
    heading = rows[0]["state_heading"]
    h0 = (heading["bbox"][1] - 2) / page["height"]
    h1 = (heading["bbox"][3] + 2) / page["height"]
    selections = [
        (number, Box(0.19, y0, 0.485, y1), "primary member band: identities, scores and A–N"),
        (number, Box(0.513, y0, 0.79, y1), "same original row band: O–OO continuation"),
        (
            10,
            Box(0.19, 0.08, 0.485, 0.12),
            "original repeated table header for identities, score periods and A–N; no member records",
        ),
        (10, Box(0.513, 0.08, 0.79, 0.12), "original repeated continuation header O–OO; no member records"),
        (
            heading["page"],
            Box(0.19, h0, 0.485, h1),
            "original state heading preceding the first target member; context only",
        ),
    ]
    with DefaultReader(dpi=300).open(source, media_type="application/pdf") as document:
        renders = {p: document.page(p).render() for p in {entry[0] for entry in selections}}
        images = [crop(renders[p], box) for p, box, _ in selections]
    image_receipts = []
    for index, (image, (p, _, role)) in enumerate(zip(images, selections, strict=True), 1):
        name = f"input-{index}.png"
        (output / name).write_bytes(image.data)
        image_receipts.append(
            {
                "name": name,
                "physical_page": p,
                "role": role,
                "sha256": sha256(image.data).hexdigest(),
                "box": [image.box.x0, image.box.y0, image.box.x1, image.box.y1],
                "pixels": [image.width, image.height],
            }
        )
    prompt = (
        outcome_prompt(number)
        + """
The first two images are corresponding source bands with identical vertical
limits. Image 1 contains identities, ratings and actions A–N. Image 2 continues
those SAME source rows with O–OO. Associate records across the source band using
the original alignment. Return ONLY members or vacant seats in these first two
images. Images 3 and 4 are original table headers for interpreting periods and
item letters. Image 5 is the original state heading preceding the first target
member; it continues until another state heading appears inside the target
band. Context/header images do not add member records. Return semantic records,
not row positions or coordinates. Copy party delimiters '(R)'/'(D)' and all
source accents literally. Superscript footnote numbers belong only in footnote_markers, never appended to member_name_text. Do not fill unreadable cells. An
empty result cell on a vacant seat stays null, never an invented N/A or person.
"""
    )
    generation = {
        "maxOutputTokens": 32768,
        "temperature": 0,
        "mediaResolution": "MEDIA_RESOLUTION_HIGH",
        "thinkingConfig": {"thinkingLevel": "medium"},
    }
    settings = {
        "model": "gemini-3.8-flash",
        "schema_version": SCHEMA_VERSION,
        "provider_version": version("spicy-docs"),
        "source_sha256": SOURCE_SHA256,
        "physical_page": number,
        "source_rows": [r["source_row"] for r in rows],
        "images": image_receipts,
        "generation": generation,
        "prompt": prompt,
    }
    (output / "settings.json").write_text(json.dumps(settings, ensure_ascii=False, indent=2))
    try:
        transport = resolved_client(args.resolve_ip, output) if args.resolve_ip else None
        with transport or nullcontext(), GeminiClient(api_key=credential, timeout=240, http_client=transport) as client:
            observed = Gemini(
                client,
                model="gemini-3.8-flash",
                mode="structured",
                outcome_schema=outcome_schema(number),
                prompt=prompt,
                generation=generation,
            ).recognize(images[0], additional_images=tuple(images[1:]))
        with (output / "observation.pickle").open("wb") as stream:
            pickle.dump(observed, stream)
        outcome = observed.raw["outcome"]
        (output / "outcome.json").write_text(json.dumps(outcome, ensure_ascii=False, indent=2))
        for field in ("request", "response"):
            (output / f"{field}.json").write_text(
                json.dumps(observed.raw["calls"][0][field], ensure_ascii=False, indent=2)
            )
        problems = []
        if outcome["uncertainties"]:
            problems.append("reported_source_uncertainty")
        expected_members = [r["name"] for r in rows if r["name"] != "Vacant"]
        if sorted(m["member_name_text"] for m in outcome["members"]) != sorted(expected_members):
            problems.append("member_identity_coverage_differs")
        if len(outcome["vacant_seats"]) != sum(r["name"] == "Vacant" for r in rows):
            problems.append("vacancy_coverage_differs")
        for record in outcome["members"] + outcome["vacant_seats"]:
            keys = [r["item_key"] for r in record["item_results"]]
            periods = [r["period_text"] for r in record["ratings"]]
            if len(keys) != len(HOUSE_ITEMS) or set(keys) != set(HOUSE_ITEMS):
                problems.append("item_coverage_differs")
            if len(periods) != len(RATING_PERIODS) or set(periods) != set(RATING_PERIODS):
                problems.append("rating_period_coverage_differs")
        comparison = compare(page, outcome)
        (output / "native-diagnostic-comparison.json").write_text(json.dumps(comparison, ensure_ascii=False, indent=2))
        if comparison["differences"]:
            problems.append("source_diagnostic_mismatch")
        receipt = {
            "status": "refused_candidate" if problems else "requires_independent_source_readback",
            "problems": sorted(set(problems)),
            "source_sha256": SOURCE_SHA256,
            "schema_version": SCHEMA_VERSION,
            "provider_version": version("spicy-docs"),
            "physical_page": number,
            "source_rows": settings["source_rows"],
            "members": len(outcome["members"]),
            "vacant_seats": len(outcome["vacant_seats"]),
            "images": image_receipts,
            "source_diagnostic_counts": comparison["counts"],
            "unclassified_cells": len(comparison["unclassified_cells"]),
            "full_edition": False,
        }
    except Exception as error:
        if isinstance(error, CredentialRefusedError):
            args.stop_event.set()
        (output / "failure.json").write_text(
            json.dumps(
                {"error_type": type(error).__name__, "details": getattr(error, "details", None)},
                ensure_ascii=False,
                indent=2,
            )
        )
        receipt = {
            "status": "observation_failed",
            "physical_page": number,
            "source_rows": settings["source_rows"],
            "error_type": type(error).__name__,
            "full_edition": False,
        }
    (output / "receipt.json").write_text(json.dumps(receipt, indent=2))
    print(json.dumps({k: v for k, v in receipt.items() if k != "images"}), flush=True)
    return receipt


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--credentials", type=Path, required=True)
    p.add_argument("--resolve-ip")
    p.add_argument("--pages", type=int, nargs="+", required=True)
    p.add_argument("--band-size", type=int, default=7, choices=range(1, 9))
    p.add_argument("--start-row", type=int, default=1)
    p.add_argument("--end-row", type=int)
    p.add_argument("--workers", type=int, default=4, choices=range(1, 5))
    args = p.parse_args()
    args.stop_event = Event()
    source = args.source.read_bytes()
    inventory = source_inventory(source)
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "source-diagnostic.json").write_text(json.dumps(inventory, ensure_ascii=False, indent=2))
    jobs = []
    for page in inventory["pages"]:
        if page["physical_page"] not in args.pages:
            continue
        rows = [
            r
            for r in page["rows"]
            if r["source_row"] >= args.start_row and (args.end_row is None or r["source_row"] <= args.end_row)
        ]
        jobs.extend((page, rows[start : start + args.band_size]) for start in range(0, len(rows), args.band_size))
    credential = read_api_key(args.credentials, "GEMINI_API_KEY")
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        results = list(pool.map(lambda job: observe(args, source, job[0], job[1], credential), jobs))
    (args.output / "receipts.json").write_text(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
