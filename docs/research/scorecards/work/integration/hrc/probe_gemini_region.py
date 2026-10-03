"""Bounded HRC item or endnote observation using a source-page crop."""

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
from spicy_docs.sources.scorecards.hrc_outcomes import SCHEMA_VERSION, outcome_prompt, outcome_schema
from spicy_docs.transport.credentials import read_api_key


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--credentials", type=Path, required=True)
    parser.add_argument("--resolve-ip")
    parser.add_argument("--page", type=int, required=True)
    parser.add_argument("--box", type=float, nargs=4, required=True)
    parser.add_argument("--item-keys", nargs="+")
    parser.add_argument("--note-markers", nargs="+")
    args = parser.parse_args()
    source = args.source.read_bytes()
    if sha256(source).hexdigest() != SOURCE_SHA256:
        parser.error("not the checked original HRC PDF")
    args.output.mkdir(parents=True, exist_ok=False)
    with DefaultReader(dpi=300).open(source, media_type="application/pdf") as document:
        image = crop(document.page(args.page).render(), Box(*args.box))
    (args.output / "input.png").write_bytes(image.data)
    credential = read_api_key(args.credentials, "GEMINI_API_KEY")
    transport = resolved_client(args.resolve_ip, args.output) if args.resolve_ip else None
    with transport or nullcontext(), GeminiClient(api_key=credential, timeout=240, http_client=transport) as client:
        observed = Gemini(
            client,
            model="gemini-3.8-flash",
            mode="structured",
            outcome_schema=outcome_schema(args.page),
            prompt=outcome_prompt(args.page)
            + "\nPublisher item keys use letters, not digits. Copy typographic apostrophes and quotation marks exactly. Include only source records visible in this image; do not fill records from outside this crop.\n",
            generation={
                "maxOutputTokens": 32768,
                "temperature": 0,
                "mediaResolution": "MEDIA_RESOLUTION_HIGH",
                "thinkingConfig": {"thinkingLevel": "medium"},
            },
        ).recognize(image)
    with (args.output / "observation.pickle").open("wb") as stream:
        pickle.dump(observed, stream)
    outcome = observed.raw["outcome"]
    (args.output / "outcome.json").write_text(json.dumps(outcome, ensure_ascii=False, indent=2))
    if outcome["uncertainties"]:
        raise ValueError("source observation contains uncertainty")
    for expected, field, key in [
        (args.item_keys, "scored_items", "item_key"),
        (args.note_markers, "notes", "marker_text"),
    ]:
        if expected is not None:
            found = [row[key] for row in outcome[field]]
            if len(found) != len(expected) or set(found) != set(expected):
                raise ValueError(f"source coverage differs for {field}")
    receipt = {
        "status": "bounded_key_coverage_passed_not_source_qualified",
        "physical_page": args.page,
        "source_sha256": SOURCE_SHA256,
        "schema_version": SCHEMA_VERSION,
        "image_sha256": sha256(image.data).hexdigest(),
        "box": [image.box.x0, image.box.y0, image.box.x1, image.box.y1],
        "item_count": len(outcome.get("scored_items", [])),
        "note_count": len(outcome["notes"]),
        "whole_page": False,
        "full_edition": False,
    }
    (args.output / "receipt.json").write_text(json.dumps(receipt, indent=2))
    print(json.dumps(receipt), flush=True)


if __name__ == "__main__":
    main()
