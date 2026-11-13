"""Capture an explicit FARA cohort, retain selected PDFs, and build a replayable local generation."""

import argparse
import json
from pathlib import Path

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.pipelines.raw_releases import (
    download_original, journal_snapshot_releases, open_raw, raw_original, snapshot_readers,
)
from spicy_regs.source_evidence import CaptureEvidence, acquisition_evidence
from spicy_regs.transforms.build_fara import SCHEMAS, build_fara

FAMILY = "fara"


def _documents(selection):
    from spicy_docs.sources.fara import document_url

    documents = selection.get("documents", {})
    if not isinstance(documents, dict):
        raise ValueError("FARA document selection must be keyed by production URL")
    for url, capture in documents.items():
        document_url(url)
        if capture["request_url"] != url:
            raise ValueError("FARA document capture differs from its selected locator")
        review = capture.get("review")
        if review is not None and (not isinstance(review, dict)
                or set(review) != {"status", "note", "original_sha256", "page", "image_sha256"}
                or review["status"] not in {"supports_registration_and_principal_label", "principal_label_differs"}
                or review["original_sha256"] != capture["response_sha256"]
                or type(review["page"]) is not int or review["page"] < 1 or not isinstance(review["note"], str)):
            raise ValueError("FARA visual review differs from its selected original")
    return documents


def acquire(registration_numbers, output_dir: Path, *, document_urls=(), transport=None):
    """Capture only production exports/PDFs; any refusal aborts the selection, preserving failed evidence."""
    from spicy_docs.sources.fara import (
        HEADERS, MAX_ZIP_BYTES, acquire_export, document_url, export_url, fara_scope, validate_cohort,
    )

    registration_numbers = validate_cohort(registration_numbers)
    with acquisition_evidence(FAMILY, output_dir=Path(output_dir)) as evidence:
        scopes = []
        for kind in HEADERS:
            official = export_url(kind)

            def validate(url, expected=official):
                if url != expected:
                    raise ValueError("FARA export URL differs from the selected production export")
                return url

            descriptor = download_original(
                evidence, lambda acquirer, store, kind=kind: acquire_export(acquirer, kind, store=store),
                stage="fara-export", max_bytes=MAX_ZIP_BYTES, validate_url=validate, transport=transport)
            scopes.append(fara_scope(descriptor, kind, registration_numbers))
        # Verify membership before any PDF request; names are not download authorization.
        from spicy_docs.sources.fara import read_export

        scope = next(scope for scope in scopes if scope["kind"] == "RegistrantDocs")
        with evidence.store.open(scope["capture"]["response_sha256"]) as stream:
            result = read_export(stream.read(MAX_ZIP_BYTES + 1), scope=scope)
        listed = {row["fields"]["URL"] for row in result["rows"]}
        documents = {}
        for url in document_urls:
            if url not in listed or url in documents:
                raise ValueError("FARA document is unlisted in the cohort or selected twice")
            documents[url] = download_original(
                evidence, lambda acquirer, store, url=url: acquirer.download(
                    document_url(url), store=store, max_bytes=MAX_ZIP_BYTES,
                    validate_prefix=lambda prefix: _pdf(prefix)),
                stage="fara-document", max_bytes=MAX_ZIP_BYTES, validate_url=document_url, transport=transport)
        return {"releases": scopes, "documents": documents}


def _pdf(prefix):
    if not prefix.startswith(b"%PDF-"):
        raise ValueError("FARA document success shape is not a PDF")


def build(selection, output_dir: Path):
    """Rebuild from exact raw inputs, admitting source releases and ordinary table/evidence generations."""
    from spicy_docs.sources.fara import MAX_ZIP_BYTES, read_export, validate_scope
    from spicy_docs.sources.fara_profile import FARA_PROFILE, iter_retained_pages

    output_dir = Path(output_dir)
    if output_dir.exists():
        raise ValueError("Use a fresh FARA build directory")
    output_dir.mkdir(parents=True)
    scopes = [validate_scope(value) for value in selection["releases"]]
    documents = _documents(selection)
    store = open_raw()
    evidence = CaptureEvidence(output_dir, FAMILY)
    try:
        evidence.retain_bytes(json.dumps(selection, sort_keys=True).encode(), stage="fara-selection")
        populations = {}
        for scope in scopes:
            capture = scope["capture"]
            with store.open(capture["response_sha256"]) as stream:
                population = read_export(stream.read(MAX_ZIP_BYTES + 1), scope=scope)
            populations[scope["kind"]] = {key: value for key, value in population.items() if key != "rows"}
            evidence.retain_file(raw_original(store, capture["response_sha256"], capture["byte_size"]),
                                 stage="fara-export", raw_source=True, original_location=capture["request_url"])
        for url, capture in documents.items():
            path = raw_original(store, capture["response_sha256"], capture["byte_size"])
            if not 0 < capture["byte_size"] <= MAX_ZIP_BYTES:
                raise ValueError("FARA PDF exceeds the selected bound")
            with path.open("rb") as stream:
                _pdf(stream.read(8))
            evidence.retain_file(path, stage="fara-document", raw_source=True, original_location=url)
        readers, _archives = snapshot_readers(FARA_PROFILE, iter_retained_pages, scopes,
                                              output_dir / "source-releases", store=store)
        journal_snapshot_releases(evidence, readers, "fara-source-release")
        files = build_fara(readers, output_dir, populations=populations, documents=documents)
        evidence.event("fara-population", populations=populations,
                       document_selection_count=len(documents), financial_extraction=False,
                       association_limit="Exact source labels within a registrant; no native principal ID supplied")
        artifact = build_generation(output_dir / "generation", family=FAMILY, files=files,
                                    expected_keys=tuple(f"{name}.parquet" for name in SCHEMAS),
                                    schemas={name: described_schema(schema) for name, schema in SCHEMAS.items()},
                                    inputs=evidence.inputs(), owned_output_root=output_dir)
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    acquire_parser = sub.add_parser("acquire")
    acquire_parser.add_argument("--registrant", action="append", required=True)
    acquire_parser.add_argument("--document-url", action="append", default=[])
    acquire_parser.add_argument("--output-dir", type=Path, required=True)
    acquire_parser.add_argument("--captures-out", type=Path, required=True)
    build_parser = sub.add_parser("build")
    build_parser.add_argument("--captures", type=Path, required=True)
    build_parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "acquire":
        selected = acquire(args.registrant, args.output_dir, document_urls=args.document_url)
        args.captures_out.write_text(json.dumps(selected, indent=2) + "\n")
        print(json.dumps({"captures": str(args.captures_out)}))
    else:
        artifact, evidence = build(json.loads(args.captures.read_text()), args.output_dir)
        print(json.dumps({"generation": str(args.output_dir / "generation"),
                          "artifactDigest": artifact.pin.artifact_digest, "sourceEvidence": str(evidence)}))


if __name__ == "__main__":
    main()
