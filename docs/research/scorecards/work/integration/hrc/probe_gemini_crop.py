"""HRC member-data diagnostic; crop geometry remains private input evidence."""

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
from spicy_docs.extraction.model import Box, ExtractionError
from spicy_docs.extraction.pages import crop
from spicy_docs.sources.scorecards.hrc_outcomes import SCHEMA_VERSION, outcome_prompt, outcome_schema, validate_outcome
from spicy_docs.transport.credentials import CredentialRefusedError, read_api_key


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--credentials", type=Path, required=True)
    parser.add_argument("--resolve-ip")
    args = parser.parse_args()
    body = args.source.read_bytes()
    if sha256(body).hexdigest() != SOURCE_SHA256:
        parser.error("not the checked original HRC PDF")
    args.output.mkdir(parents=True, exist_ok=False)
    box = Box(0, 0, 0.5, 1)
    with DefaultReader(dpi=200).open(body, media_type="application/pdf") as document:
        image = crop(document.page(6).render(), box)
    (args.output / "input-crop.png").write_bytes(image.data)
    credential = read_api_key(args.credentials, "GEMINI_API_KEY")
    transport = resolved_client(args.resolve_ip, args.output) if args.resolve_ip else None
    with transport or nullcontext(), GeminiClient(api_key=credential, timeout=240, http_client=transport) as client:
        backend = Gemini(
            client,
            mode="structured",
            outcome_schema=outcome_schema(6),
            prompt=outcome_prompt(6)
            + "\nPreserve the source party label including its delimiters: examples of the printed party_text are '(R)', '(D)' and '(I)'. Copy those delimiters as part of the source text.\n",
            generation={
                "maxOutputTokens": 32768,
                "temperature": 0,
                "mediaResolution": "MEDIA_RESOLUTION_HIGH",
                "thinkingConfig": {"thinkingLevel": "medium"},
            },
        )
        try:
            observed = backend.recognize(image)
        except (ExtractionError, CredentialRefusedError) as error:
            (args.output / "error.txt").write_text(str(error))
            with (args.output / "failure.pickle").open("wb") as stream:
                pickle.dump(getattr(error, "details", None), stream)
            raise SystemExit(type(error).__name__) from None
    with (args.output / "observation.pickle").open("wb") as stream:
        pickle.dump(observed, stream)
    outcome = observed.raw["outcome"]
    (args.output / "outcome.json").write_text(json.dumps(outcome, ensure_ascii=False, indent=2))
    validate_outcome(6, outcome, member_grid_only=True)
    receipt = {
        "source_sha256": SOURCE_SHA256,
        "physical_page": 6,
        "normalized_crop_box": [0, 0, 0.5, 1],
        "crop_pixels": [image.width, image.height],
        "crop_sha256": sha256(image.data).hexdigest(),
        "outcome_schema_version": SCHEMA_VERSION,
        "status": "source_key_coverage_validated_not_source_qualified",
        "member_count": len(outcome["members"]),
        "uncertainty_count": len(outcome["uncertainties"]),
        "whole_page": False,
        "full_page_notes_merged": False,
    }
    (args.output / "receipt.json").write_text(json.dumps(receipt, indent=2))
    print(json.dumps(receipt), flush=True)


if __name__ == "__main__":
    main()
