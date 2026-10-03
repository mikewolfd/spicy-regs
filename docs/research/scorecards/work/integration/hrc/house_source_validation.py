"""Read-only HRC source checks for retained Gemini candidates; no production parser.

The original PDF renders open and filled circles with the same native character.
Use that character's retained fill/stroke flags only for the reviewed font/flags;
anything else remains unclassified and requires direct image review. Geometry
is diagnostic source evidence, never an output column or an invented identity.
"""

from __future__ import annotations

import argparse
import json
from hashlib import sha256
from pathlib import Path

from probe_gemini import SOURCE_SHA256
from spicy_docs.extraction import DefaultReader
from spicy_docs.sources.scorecards.hrc_outcomes import HOUSE_ITEMS, RATING_PERIODS


def center(span):
    x0, y0, x1, y1 = span["bbox"]
    return (x0 + x1) / 2, (y0 + y1) / 2


def source_inventory(source: bytes):
    if sha256(source).hexdigest() != SOURCE_SHA256:
        raise ValueError("not the reviewed HRC source")
    pages = []
    heading = None
    with DefaultReader().open(source, media_type="application/pdf") as document:
        for number in range(10, 27):
            raw = document.page(number).native().raw
            spans = [s for b in raw["blocks"] for line in b.get("lines", []) for s in line["spans"]]
            districts = sorted(
                (
                    s
                    for s in spans
                    if 300 < center(s)[0] < 320
                    and 73 < center(s)[1] < (260 if number == 26 else 563)
                    and (s["text"].strip().isdigit() or s["text"].strip() == "AL")
                ),
                key=lambda s: center(s)[1],
            )
            headings = sorted(
                (
                    s
                    for s in spans
                    if 309 < s["bbox"][0] < 312
                    and s["text"].strip().isupper()
                    and len(s["text"].strip()) > 2
                    and "Bold" in s["font"]
                    and 73 < center(s)[1] < (260 if number == 26 else 563)
                ),
                key=lambda s: center(s)[1],
            )
            rows = []
            for ordinal, district in enumerate(districts, 1):
                _, y = center(district)
                preceding = [s for s in headings if center(s)[1] < y]
                if preceding:
                    h = preceding[-1]
                    heading = {"page": number, "text": h["text"].strip(), "bbox": h["bbox"]}
                if heading is None:
                    raise ValueError("missing original source heading")
                vicinity = [s for s in spans if abs(center(s)[1] - y) < 8.2]
                names = sorted(
                    (s for s in vicinity if 323 < s["bbox"][0] < 417 and s["size"] >= 6 and s["text"].strip()),
                    key=lambda s: (s["bbox"][1], s["bbox"][0]),
                )
                markers = sorted(
                    (s for s in vicinity if 324 < s["bbox"][0] < 417 and s["size"] < 6 and s["text"].strip()),
                    key=lambda s: (s["bbox"][1], s["bbox"][0]),
                )
                parties = [
                    s["text"].strip()
                    for s in vicinity
                    if 417 < center(s)[0] < 434 and s["text"].strip() in {"(R)", "(D)", "(I)"}
                ]
                scores = []
                for period, low, high in zip(RATING_PERIODS, (436, 473, 510), (468, 504, 540), strict=True):
                    values = [
                        s["text"].strip()
                        for s in vicinity
                        if low < center(s)[0] < high and abs(center(s)[1] - y) < 4 and s["text"].strip()
                    ]
                    scores.append({"period_text": period, "source_values": values})
                cells = []
                for index, key in enumerate(HOUSE_ITEMS):
                    x = 551.4079 + 15.624 * index if index < 14 else 826.9379 + 15.624 * (index - 14)
                    candidates = [
                        s
                        for s in vicinity
                        if abs(center(s)[0] - x) < 5 and abs(center(s)[1] - y) < 4 and s["text"].strip()
                    ]
                    value = None
                    basis = "unclassified_requires_image_readback"
                    if len(candidates) == 1:
                        span = candidates[0]
                        if (
                            span["font"] == "Universal-NewswithCommPi"
                            and span["text"].strip() == "v"
                            and span["char_flags"] in {16, 32}
                        ):
                            value = {16: "●", 32: "○"}[span["char_flags"]]
                            basis = "original_glyph_fill_or_stroke_flags"
                        elif span["text"].strip() in {"N/A", "P"}:
                            value = span["text"].strip()
                            basis = "original_literal_text"
                    cells.append({"item_key": key, "value": value, "basis": basis, "source_spans": candidates})
                rows.append(
                    {
                        "source_row": ordinal,
                        "name": " ".join(s["text"].strip() for s in names),
                        "name_spans": names,
                        "footnote_markers": [s["text"].strip() for s in markers],
                        "party_values": parties,
                        "district": district["text"].strip(),
                        "state_heading": heading,
                        "y_center": y,
                        "ratings": scores,
                        "cells": cells,
                    }
                )
            pages.append({"physical_page": number, "width": raw["width"], "height": raw["height"], "rows": rows})
    return {
        "source_sha256": SOURCE_SHA256,
        "purpose": "independent source diagnostic only; unclassified glyphs require image readback",
        "pages": pages,
    }


def compare(page, outcome, manual_values=None):
    source_by_name = {row["name"]: row for row in page["rows"]}
    if len(source_by_name) != len(page["rows"]):
        raise ValueError("duplicate source names on page")
    differences, unknown = [], []
    counts = {"members": 0, "ratings": 0, "classified_cells": 0, "manual_image_cells": 0}
    for member in outcome["members"]:
        name = member["member_name_text"]
        row = source_by_name.get(name)
        if row is None:
            differences.append({"member": name, "field": "member_name_text", "reason": "no exact source text"})
            continue
        counts["members"] += 1
        for field, expected in [
            ("party_text", row["party_values"][0] if len(row["party_values"]) == 1 else None),
            ("state_text", row["state_heading"]["text"]),
            ("district_text", row["district"]),
            ("footnote_markers", row["footnote_markers"]),
        ]:
            if member[field] != expected:
                differences.append({"member": name, "field": field, "expected": expected, "observed": member[field]})
        for rating in member["ratings"]:
            matches = [r for r in row["ratings"] if r["period_text"] == rating["period_text"]]
            if len(matches) != 1 or matches[0]["source_values"] != [rating["value_text"]]:
                differences.append(
                    {
                        "member": name,
                        "field": rating["period_text"],
                        "expected": matches,
                        "observed": rating["value_text"],
                    }
                )
            else:
                counts["ratings"] += 1
        for result in member["item_results"]:
            matches = [r for r in row["cells"] if r["item_key"] == result["item_key"]]
            if len(matches) != 1:
                differences.append({"member": name, "field": result["item_key"], "reason": "no exact source item"})
                continue
            expected = matches[0]
            manual = (manual_values or {}).get((page["physical_page"], name, result["item_key"]))
            if expected["value"] is None and manual is not None:
                if result["result_text"] == manual:
                    counts["manual_image_cells"] += 1
                else:
                    differences.append(
                        {
                            "member": name,
                            "field": result["item_key"],
                            "expected": manual,
                            "observed": result["result_text"],
                            "basis": "direct original image readback",
                        }
                    )
            elif expected["value"] is None:
                unknown.append(
                    {
                        "member": name,
                        "item_key": result["item_key"],
                        "observed": result["result_text"],
                        "source_row": row["source_row"],
                        "reason": expected["basis"],
                    }
                )
            elif result["result_text"] != expected["value"]:
                differences.append(
                    {
                        "member": name,
                        "field": result["item_key"],
                        "expected": expected["value"],
                        "observed": result["result_text"],
                    }
                )
            else:
                counts["classified_cells"] += 1
    return {
        "status": "requires_direct_image_readback" if not differences else "source_mismatch",
        "counts": counts,
        "differences": differences,
        "unclassified_cells": unknown,
        "full_source_qualification": False,
    }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--candidate", type=Path)
    p.add_argument("--page", type=int)
    args = p.parse_args()
    inventory = source_inventory(args.source.read_bytes())
    if args.candidate:
        page = next(page for page in inventory["pages"] if page["physical_page"] == args.page)
        result = compare(page, json.loads(args.candidate.read_text()))
    else:
        result = inventory
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(
        json.dumps(
            {key: value for key, value in result.items() if key != "pages" and key != "unclassified_cells"},
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
