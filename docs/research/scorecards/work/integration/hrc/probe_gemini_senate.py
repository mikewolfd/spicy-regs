"""Retain two bounded Senate grid observations and reconcile page key coverage."""

from __future__ import annotations

import argparse
import copy
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
from spicy_docs.sources.scorecards.hrc_outcomes import SCHEMA_VERSION, outcome_prompt, outcome_schema, validate_outcome
from spicy_docs.transport.credentials import read_api_key


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--credentials", type=Path, required=True)
    parser.add_argument("--resolve-ip")
    parser.add_argument("--page", type=int, choices=(4, 5), required=True)
    args = parser.parse_args()
    source = args.source.read_bytes()
    if sha256(source).hexdigest() != SOURCE_SHA256:
        parser.error("not the checked original HRC PDF")
    args.output.mkdir(parents=True, exist_ok=False)
    boxes = (Box(0.105, 0.08, 0.49, 0.845), Box(0.605, 0.08, 0.99, 0.845))
    with DefaultReader(dpi=300).open(source, media_type="application/pdf") as document:
        raster = document.page(args.page).render()
        images = [crop(raster, box) for box in boxes]
    credential = read_api_key(args.credentials, "GEMINI_API_KEY")
    transport = resolved_client(args.resolve_ip, args.output) if args.resolve_ip else None
    outcomes, receipts = [], []
    with transport or nullcontext(), GeminiClient(api_key=credential, timeout=240, http_client=transport) as client:
        for side, image in zip(("left", "right"), images, strict=True):
            (args.output / f"{side}.png").write_bytes(image.data)
            backend = Gemini(
                client,
                model="gemini-3.8-flash",
                mode="structured",
                outcome_schema=outcome_schema(args.page),
                prompt=outcome_prompt(args.page) + "\nPreserve party delimiters, for example '(R)', '(D)' and '(I)'.\n",
                generation={
                    "maxOutputTokens": 32768,
                    "temperature": 0,
                    "mediaResolution": "MEDIA_RESOLUTION_HIGH",
                    "thinkingConfig": {"thinkingLevel": "medium"},
                },
            )
            observed = backend.recognize(image)
            with (args.output / f"{side}.pickle").open("wb") as stream:
                pickle.dump(observed, stream)
            outcome = observed.raw["outcome"]
            (args.output / f"{side}.json").write_text(json.dumps(outcome, ensure_ascii=False, indent=2))
            if outcome["uncertainties"] or len(outcome["members"]) != 20:
                raise ValueError("Senate grid is uncertain or differs from inspected member count")
            outcomes.append(outcome)
            receipts.append(
                {
                    "scope": side,
                    "sha256": sha256(image.data).hexdigest(),
                    "pixels": [image.width, image.height],
                    "box": [image.box.x0, image.box.y0, image.box.x1, image.box.y1],
                }
            )
    # Keep the complete observations separate. This temporary union tests the
    # source-defined page membership/key invariants and is not an admitted table.
    combined = copy.deepcopy(outcomes[0])
    combined["members"].extend(outcomes[1]["members"])
    combined["notes"].extend(outcomes[1]["notes"])
    combined["uncertainties"].extend(outcomes[1]["uncertainties"])
    validate_outcome(args.page, combined)
    receipt = {
        "status": "page_member_key_coverage_passed_not_source_qualified",
        "source_sha256": SOURCE_SHA256,
        "schema_version": SCHEMA_VERSION,
        "physical_page": args.page,
        "members": 40,
        "ratings": 120,
        "item_results": 600,
        "image_scopes": receipts,
        "full_page_notes_or_legends_claimed": False,
        "full_edition": False,
    }
    (args.output / "receipt.json").write_text(json.dumps(receipt, indent=2))
    print(json.dumps(receipt), flush=True)


if __name__ == "__main__":
    main()
