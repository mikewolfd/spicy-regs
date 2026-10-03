"""Render original, unclassified HRC source cells without model values as gold."""

from __future__ import annotations

import argparse
import io
import json
from hashlib import sha256
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont
from spicy_docs.extraction import DefaultReader
from spicy_docs.extraction.model import Box
from spicy_docs.extraction.pages import crop
from spicy_docs.sources.scorecards.hrc_outcomes import HOUSE_ITEMS

from house_source_validation import source_inventory


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    body = args.source.read_bytes()
    inventory = source_inventory(body)
    args.output.mkdir(parents=True, exist_ok=False)
    font = ImageFont.truetype("/System/Library/Fonts/Supplemental/Arial.ttf", 14)
    receipts = []
    with DefaultReader(dpi=300).open(body, media_type="application/pdf") as document:
        for page in inventory["pages"]:
            number = page["physical_page"]
            selected = [
                (r, cell, "unclassified")
                for r in page["rows"]
                if r["name"] != "Vacant"
                for cell in r["cells"]
                if cell["value"] is None
            ]
            for flag in ("●", "○"):
                controls = [
                    (r, cell, "control")
                    for r in page["rows"]
                    if r["name"] != "Vacant"
                    for cell in r["cells"]
                    if cell["value"] == flag
                ][:2]
                selected.extend(controls)
            image = Image.new("RGB", (1400, ((len(selected) + 4) // 5) * 155), "white")
            draw = ImageDraw.Draw(image)
            raster = document.page(number).render()
            records = []
            for i, (row, cell, role) in enumerate(selected):
                item = HOUSE_ITEMS.index(cell["item_key"])
                x = 551.4079 + 15.624 * item if item < 14 else 826.9379 + 15.624 * (item - 14)
                y = row["y_center"]
                box = Box(
                    (x - 6.5) / page["width"],
                    (y - 5.8) / page["height"],
                    (x + 6.5) / page["width"],
                    (y + 6) / page["height"],
                )
                cell_image = crop(raster, box)
                xout, yout = (i % 5) * 280, (i // 5) * 155
                draw.text(
                    (xout + 3, yout + 3), f"{i + 1}. p{number} / {cell['item_key']} / {role}", fill="black", font=font
                )
                draw.text((xout + 3, yout + 22), row["name"], fill="black", font=font)
                image.paste(Image.open(io.BytesIO(cell_image.data)).convert("RGB"), (xout + 80, yout + 46))
                records.append(
                    {
                        "index": i + 1,
                        "member_name": row["name"],
                        "item_key": cell["item_key"],
                        "source_row": row["source_row"],
                        "role": role,
                        "source_box": [cell_image.box.x0, cell_image.box.y0, cell_image.box.x1, cell_image.box.y1],
                        "image_sha256": sha256(cell_image.data).hexdigest(),
                    }
                )
            output = args.output / f"page-{number}-source-cells.png"
            image.save(output)
            manifest = {
                "physical_page": number,
                "source_sha256": inventory["source_sha256"],
                "purpose": "original rendered cells; no model values included",
                "image_sha256": sha256(output.read_bytes()).hexdigest(),
                "cells": records,
            }
            (args.output / f"page-{number}-source-cells.json").write_text(
                json.dumps(manifest, ensure_ascii=False, indent=2)
            )
            receipts.append(
                {
                    "page": number,
                    "source_unclassified_cells": sum(r["role"] == "unclassified" for r in records),
                    "controls": sum(r["role"] == "control" for r in records),
                    "image_sha256": manifest["image_sha256"],
                }
            )
    (args.output / "manifest.json").write_text(json.dumps(receipts, indent=2))
    print(json.dumps(receipts))


if __name__ == "__main__":
    main()
