"""Bounded semantic HRC House observation; no continued results are inferred."""

from __future__ import annotations

import argparse
import json
import pickle
from contextlib import nullcontext
from hashlib import sha256
from pathlib import Path

from jsonschema import Draft202012Validator
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


def validate_visible_scope(outcome: dict, *, expected_members: int) -> None:
    """The original crop exposes identities, three scores and only items A–N."""
    Draft202012Validator(outcome_schema(10)).validate(outcome)
    if outcome["uncertainties"] or len(outcome["members"]) != expected_members:
        raise ValueError("House crop has uncertain facts or incomplete member coverage")
    identities = set()
    for member in outcome["members"]:
        identity = (member["state_text"], member["district_text"], member["member_name_text"])
        if any(value is None or not value for value in identity) or identity in identities:
            raise ValueError("House crop member identity missing or repeated")
        identities.add(identity)
        if member["party_text"] not in {"(R)", "(D)", "(I)"}:
            raise ValueError("House crop lost the literal party presentation")
        periods = [rating["period_text"] for rating in member["ratings"]]
        keys = [result["item_key"] for result in member["item_results"]]
        if len(periods) != 3 or set(periods) != set(RATING_PERIODS):
            raise ValueError("House crop rating period coverage differs from source")
        if len(keys) != 14 or set(keys) != set(HOUSE_ITEMS[:14]):
            raise ValueError("House crop item coverage differs from the visible A–N scope")
        if any(result["result_text"] not in {"●", "○", "⊗", "P", "N/A"} for result in member["item_results"]):
            raise ValueError("House crop has missing or unqualified result glyphs")
        if any(rating["value_text"] is None for rating in member["ratings"]):
            raise ValueError("House crop has unreadable published ratings")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--credentials", type=Path, required=True)
    parser.add_argument("--resolve-ip")
    parser.add_argument("--scope", choices=("left-page", "arizona-context"), default="left-page")
    parser.add_argument("--thinking-level", choices=("medium", "high"), default="medium")
    args = parser.parse_args()
    body = args.source.read_bytes()
    if sha256(body).hexdigest() != SOURCE_SHA256:
        parser.error("not the checked original HRC PDF")
    args.output.mkdir(parents=True, exist_ok=False)
    box, dpi, expected_members = (
        (Box(0, 0, 0.5, 1), 200, 26)
        if args.scope == "left-page"
        else (Box(0.19, 0.08, 0.485, 0.60), 300, 17)
    )
    with DefaultReader(dpi=dpi).open(body, media_type="application/pdf") as document:
        image = crop(document.page(10).render(), box)
    (args.output / "input-crop.png").write_bytes(image.data)
    prompt = outcome_prompt(10).replace(
        "Associate every result with its SOURCE ITEM KEY: " + ", ".join(HOUSE_ITEMS) + ".",
        "This retained image shows results only for SOURCE ITEM KEYS: " + ", ".join(HOUSE_ITEMS[:14]) + ". "
        "Return only those visible keyed results; the remaining results are outside this image and must not be inferred.",
    )
    prompt += "\nCopy the literal party delimiters, for example '(R)' and '(D)'. Preserve district AL as AL.\n"
    credential = read_api_key(args.credentials, "GEMINI_API_KEY")
    transport = resolved_client(args.resolve_ip, args.output) if args.resolve_ip else None
    with transport or nullcontext(), GeminiClient(api_key=credential, timeout=240, http_client=transport) as client:
        observed = Gemini(
            client,
            model="gemini-3.8-flash",
            mode="structured",
            outcome_schema=outcome_schema(10),
            prompt=prompt,
            generation={
                "maxOutputTokens": 32768,
                "temperature": 0,
                "mediaResolution": "MEDIA_RESOLUTION_HIGH",
                "thinkingConfig": {"thinkingLevel": args.thinking_level},
            },
        ).recognize(image)
    with (args.output / "observation.pickle").open("wb") as stream:
        pickle.dump(observed, stream)
    outcome = observed.raw["outcome"]
    (args.output / "outcome.json").write_text(json.dumps(outcome, ensure_ascii=False, indent=2))
    validate_visible_scope(outcome, expected_members=expected_members)
    receipt = {
        "source_sha256": SOURCE_SHA256,
        "physical_page": 10,
        "input_scope": args.scope,
        "normalized_crop_box": [box.x0, box.y0, box.x1, box.y1],
        "dpi": dpi,
        "crop_pixels": [image.width, image.height],
        "crop_sha256": sha256(image.data).hexdigest(),
        "outcome_schema_version": SCHEMA_VERSION,
        "model": "gemini-3.8-flash",
        "thinking_level": args.thinking_level,
        "status": "bounded_key_coverage_validated_not_source_qualified",
        "member_count": len(outcome["members"]),
        "rating_count": sum(len(member["ratings"]) for member in outcome["members"]),
        "item_result_count": sum(len(member["item_results"]) for member in outcome["members"]),
        "observed_item_keys": list(HOUSE_ITEMS[:14]),
        "whole_page": False,
        "continued_results_joined": False,
    }
    (args.output / "receipt.json").write_text(json.dumps(receipt, indent=2))
    print(json.dumps(receipt), flush=True)


if __name__ == "__main__":
    main()
