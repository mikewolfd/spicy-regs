"""Reproduce the HRC grid diagnostic through existing SpicyDocs extractors.

Run from the SpicyDocs environment. Every output is private source evidence,
not a scorecard bundle or a public artifact. This does not acquire a source.
"""

from __future__ import annotations

import argparse
import json
import pickle
import time
from hashlib import sha256
from pathlib import Path

from spicy_docs.extraction import (
    DefaultReader,
    Docling,
    DoclingVlmSettings,
    DocumentExtractor,
    FullPage,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mode", choices=("ovisocr2", "default"), required=True)
    parser.add_argument("--model-artifacts", type=Path)
    parser.add_argument("--pages", type=int, nargs="+", default=[6])
    args = parser.parse_args()
    body = args.source.read_bytes()
    digest = sha256(body).hexdigest()
    if digest != args.sha256 or not body.startswith(b"%PDF-"):
        parser.error("source bytes do not match the selected PDF digest")
    if args.mode == "ovisocr2" and args.model_artifacts is None:
        parser.error("ovisocr2 requires the existing local model-artifacts directory")
    args.output.mkdir(parents=True, exist_ok=False)
    if args.mode == "ovisocr2":
        settings = DoclingVlmSettings(
            model="ovisocr2",
            max_tokens=16384,
            document_timeout=120,
            artifacts_path=str(args.model_artifacts),
        )
        settings.check_platform()
        identity = settings.prepare_artifacts(local_files_only=True)
        backend = Docling(settings=settings, model_identity=identity)
        (args.output / "configuration.json").write_text(json.dumps(backend.require_reproducible_identity(), indent=2))
        extractor = DocumentExtractor(FullPage(backend), reader=DefaultReader(dpi=300))
    else:
        # Diagnostic only: default model-artifact identity is not pinned here.
        extractor = DocumentExtractor()
    with DefaultReader(dpi=200).open(body, media_type="application/pdf") as document:
        for number in args.pages:
            (args.output / f"page-{number}.png").write_bytes(document.page(number).render().data)
    started = time.monotonic()
    receipts = []
    try:
        for page in extractor.extract(body, media_type="application/pdf", pages=args.pages):
            number = page.metadata["page"]
            with (args.output / f"page-{number}.pickle").open("wb") as stream:
                pickle.dump(page, stream)
            (args.output / f"page-{number}.txt").write_text(page.text)
            (args.output / f"page-{number}-tables.json").write_text(
                json.dumps(
                    [
                        {"rows": table.row_count, "columns": table.column_count, "cells": table.cells}
                        for table in page.tables
                    ],
                    indent=2,
                    ensure_ascii=False,
                )
            )
            row = {
                "mode": args.mode,
                "page": number,
                "page_count": page.metadata["page_count"],
                "source_sha256": digest,
                "status": "extracted_not_qualified",
                "table_shapes": [[table.row_count, table.column_count] for table in page.tables],
                "seconds": round(time.monotonic() - started, 2),
            }
            receipts.append(row)
            print(json.dumps(row), flush=True)
    except Exception as error:
        receipts.append({"mode": args.mode, "status": "refused", "error_type": type(error).__name__})
        details = getattr(error, "details", None)
        if details:
            with (args.output / "failure.pickle").open("wb") as stream:
                pickle.dump(details, stream)
        raise
    finally:
        (args.output / "receipt.json").write_text(json.dumps(receipts, indent=2))


if __name__ == "__main__":
    main()
