"""Retain a paired-image source observation through shared structured Gemini.

Actual request/response bytes, source images and settings stay in the supplied
private output directory. This does not qualify an edition automatically.
"""

from __future__ import annotations

import argparse
import json
import pickle
from contextlib import nullcontext
from hashlib import sha256
from pathlib import Path

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
from spicy_docs.transport.credentials import read_api_key


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--credentials", type=Path, required=True)
    parser.add_argument("--resolve-ip")
    args = parser.parse_args()
    source = args.source.read_bytes()
    if sha256(source).hexdigest() != SOURCE_SHA256:
        parser.error("not the checked original HRC PDF")
    args.output.mkdir(parents=True, exist_ok=False)
    boxes = (Box(0.19, 0.08, 0.485, 0.315), Box(0.513, 0.08, 0.79, 0.315))
    with DefaultReader(dpi=300).open(source, media_type="application/pdf") as document:
        raster = document.page(10).render()
        images = [crop(raster, box) for box in boxes]
    image_receipts = []
    for index, image in enumerate(images):
        name = f"input-{index + 1}.png"
        (args.output / name).write_bytes(image.data)
        image_receipts.append(
            {
                "name": name,
                "sha256": sha256(image.data).hexdigest(),
                "box": [image.box.x0, image.box.y0, image.box.x1, image.box.y1],
                "pixels": [image.width, image.height],
            }
        )
    prompt = (
        outcome_prompt(10)
        + """
The supplied images are corresponding portions of the SAME original source
band, with identical vertical limits. The first contains member identities,
published scores and actions A–N. The second contains the continuation O–OO for
those same members, aligned by their original horizontal source rows. Associate
the continued marks with the source member using that original alignment.
Return one complete semantic member record, with results keyed by all source
item letters. Do not return row indexes, coordinates or visual reconstruction.
If the association is unclear, record uncertainty rather than invent a name.
Copy source party delimiters, for example '(R)' and '(D)'.
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
        "source_sha256": SOURCE_SHA256,
        "physical_page": 10,
        "images": image_receipts,
        "generation": generation,
        "prompt": prompt,
    }
    (args.output / "settings.json").write_text(json.dumps(settings, ensure_ascii=False, indent=2))
    transport = resolved_client(args.resolve_ip, args.output) if args.resolve_ip else None
    credential = read_api_key(args.credentials, "GEMINI_API_KEY")
    with transport or nullcontext(), GeminiClient(api_key=credential, timeout=240, http_client=transport) as client:
        observed = Gemini(
            client,
            model="gemini-3.8-flash",
            mode="structured",
            outcome_schema=outcome_schema(10),
            prompt=prompt,
            generation=generation,
        ).recognize(images[0], additional_images=tuple(images[1:]))
    with (args.output / "observation.pickle").open("wb") as stream:
        pickle.dump(observed, stream)
    body = observed.raw["calls"][0]["request"]
    response = observed.raw["calls"][0]["response"]
    (args.output / "request.json").write_text(json.dumps(body, ensure_ascii=False, indent=2))
    (args.output / "response.json").write_text(json.dumps(response, ensure_ascii=False, indent=2))
    outcome = observed.raw["outcome"]
    (args.output / "outcome.json").write_text(json.dumps(outcome, ensure_ascii=False, indent=2))
    members = outcome["members"]
    if outcome["uncertainties"] or len(members) != 7:
        raise ValueError("paired source band has uncertainty or incomplete member coverage")
    identities = set()
    for member in members:
        identity = (member["state_text"], member["district_text"], member["member_name_text"])
        if any(value is None or not value for value in identity) or identity in identities:
            raise ValueError("paired source identity missing or repeated")
        identities.add(identity)
        keys = [row["item_key"] for row in member["item_results"]]
        periods = [row["period_text"] for row in member["ratings"]]
        if len(keys) != len(HOUSE_ITEMS) or set(keys) != set(HOUSE_ITEMS):
            raise ValueError("paired source item coverage differs")
        if len(periods) != 3 or set(periods) != set(RATING_PERIODS):
            raise ValueError("paired source period coverage differs")
        if any(row["result_text"] not in {"●", "○", "⊗", "P", "N/A"} for row in member["item_results"]):
            raise ValueError("paired source result unreadable")
    receipt = {
        "status": "schema_and_key_coverage_validated_not_source_qualified",
        "source_sha256": SOURCE_SHA256,
        "physical_page": 10,
        "members": 7,
        "ratings": 21,
        "item_results": 287,
        "images": image_receipts,
        "whole_page": False,
        "full_edition": False,
    }
    (args.output / "receipt.json").write_text(json.dumps(receipt, indent=2))
    print(json.dumps(receipt), flush=True)


if __name__ == "__main__":
    main()
