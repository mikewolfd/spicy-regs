"""Retain/resume one Open Payments year and replay it to an ordinary generation.

A small manifest points into the central raw archive. Refresh into a fresh
candidate; missing rows in a partial input never establish deletions.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.pipelines.raw_releases import download_original, open_raw, raw_original
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_cms_open_payments import SCHEMA, TABLE, build_cms_open_payments

CSV_ROWS = 50_000
CSV_BYTES = 32 * 1024**2


def load_capture(store, descriptor):
    from spicy_docs.sources.cms_open_payments import MAX_PAGE_BYTES
    from spicy_docs.transport.captured import CapturedBodyResponse

    size = descriptor["byte_size"]
    if type(size) is not int or not 0 < size <= MAX_PAGE_BYTES:
        raise ValueError("Invalid bounded Open Payments original")
    with store.open(descriptor["sha256"]) as stream:
        body = stream.read(size + 1)
    if len(body) != size or "sha256:" + hashlib.sha256(body).hexdigest() != descriptor["sha256"]:
        raise ValueError("Open Payments original digest or size differs")
    url = descriptor.get("request_url", descriptor.get("url"))
    return CapturedBodyResponse(url, url, 200, "application/json", descriptor["observed_at"], body)


def _descriptor(capture):
    return {
        "request_url": capture.requested_url,
        "sha256": capture.sha256,
        "byte_size": capture.byte_size,
        "observed_at": capture.observed_at,
    }


def _save(path, manifest):
    temporary = path.with_suffix(".pending")
    temporary.write_text(json.dumps(manifest, indent=2) + "\n")
    temporary.replace(path)


def _csv_records(store, selected, chunk, *, walk, offset, previous, evidence=None):
    from spicy_docs.sources.cms_open_payments import iter_csv_summary

    if chunk["offset"] != offset or not 0 < chunk["row_count"] <= CSV_ROWS or chunk["byte_size"] > CSV_BYTES:
        raise ValueError("Open Payments CSV ranges skip, overlap or exceed their bound")
    original = raw_original(store, chunk["sha256"], chunk["byte_size"])
    if evidence is not None:
        retained = evidence.retain_file(
            original,
            stage="open-payments-csv-range",
            raw_source=True,
            source_url=chunk["request_url"],
            original_location=chunk["request_url"],
            observed_at=chunk["observed_at"],
        )
        if retained["sha256"] != chunk["sha256"]:
            raise ValueError("Open Payments CSV original changed")
    with original.open("rb") as stream:
        yield from iter_csv_summary(
            stream,
            selected,
            chunk,
            schema=walk.schema,
            total_count=walk.count,
            offset=offset,
            previous_number=previous,
            count=chunk["row_count"],
        )


def _check_count(store, descriptor, selected, walk):
    from spicy_docs.sources.cms_open_payments import decode, page_url

    control = load_capture(store, descriptor)
    if control.requested_url != page_url(selected, size=1) + "&results=false":
        raise ValueError("Open Payments count control request differs")
    observed = decode(control.body)
    if observed.get("count") != walk.count or observed.get("schema") != walk.schema:
        raise ValueError("Open Payments final count/schema differs")
    return control


def capture(
    output_dir: Path, *, year: int, max_bytes=3 * 1024**3, max_requests=2200, timeout_seconds=1800, transport=None
):
    """Checkpoint originals before advancing. Resume verifies and reuses completed ranges."""
    from spicy_docs.sources.cms_open_payments import (
        CATALOG_URL,
        MAX_PAGE_BYTES,
        TITLES,
        SummaryWalk,
        csv_url,
        page_url,
        select_summary,
        validate_url,
    )
    from spicy_docs.transport.captured import CapturedBodyResponse
    from spicy_docs.transport.download import BoundedAcquirer

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "capture-manifest.json"
    manifest: dict = (
        json.loads(path.read_text()) if path.exists() else {"program_year": year, "families": {}, "complete": False}
    )
    if manifest["program_year"] != year:
        raise ValueError("Open Payments resume year differs")
    evidence, store = CaptureEvidence(output_dir, "cms-open-payments"), open_raw()

    def acquire(client, url, stage):
        r = client.capture(url, max_bytes=MAX_PAGE_BYTES)
        c = CapturedBodyResponse(r.url, r.resolved_url, 200, r.media_type, r.observed_at, r.body)
        evidence.capture(c, stage=stage)
        return c

    try:
        with BoundedAcquirer(
            validate_url=validate_url,
            transport=transport,
            max_requests=max_requests,
            timeout=60,
            deadline=time.monotonic() + timeout_seconds,
        ) as client:
            if "catalog" not in manifest:
                manifest["catalog"] = _descriptor(acquire(client, CATALOG_URL, "open-payments-catalog"))
                _save(path, manifest)
            catalog = load_capture(store, manifest["catalog"])
            total_bytes = sum(
                p["byte_size"] for f in manifest["families"].values() for p in [*f["pages"], *f.get("csv_chunks", [])]
            )
            if total_bytes > max_bytes:
                raise ValueError("Open Payments acquisition byte budget exhausted")
            for level in TITLES:
                selected = select_summary(catalog, aggregation_level=level, program_year=year)
                family = manifest["families"].setdefault(level, {"selection": selected.as_dict(), "pages": []})
                if family["selection"] != selected.as_dict():
                    raise ValueError("Open Payments resume release selection differs")
                walk = SummaryWalk(selected)
                for descriptor in family["pages"]:
                    for _ in walk.add(load_capture(store, descriptor)):
                        pass
                # Small populations stay JSON. A first JSON page supplies the
                # source schema/count for a large practitioner's CSV ranges.
                while walk.count is None or walk.observed < walk.count:
                    if level == "recipients" and walk.count is not None:
                        break
                    if total_bytes + MAX_PAGE_BYTES > max_bytes:
                        raise ValueError("Open Payments acquisition byte budget exhausted")
                    c = acquire(client, page_url(selected, offset=walk.observed), "open-payments-summary-page")
                    for _ in walk.add(c):
                        pass
                    if walk.count is None or walk.count > 1_500_000:
                        raise ValueError("Open Payments population exceeds its admitted row bound")
                    family["pages"].append(_descriptor(c))
                    total_bytes += c.byte_size
                    _save(path, manifest)
                offset, previous = walk.observed, walk.previous
                for chunk in family.get("csv_chunks", []):
                    for item in _csv_records(store, selected, chunk, walk=walk, offset=offset, previous=previous):
                        previous = int(item["raw"]["record_number"])
                    offset += chunk["row_count"]
                while walk.count is not None and offset < walk.count:
                    if walk.schema is None:
                        raise ValueError("Open Payments CSV lacks its source schema")
                    if len(family.get("csv_chunks", [])) >= 20:
                        raise ValueError("Open Payments CSV range request budget exhausted")
                    count = min(CSV_ROWS, walk.count - offset)
                    url = csv_url(selected, resource_id=next(iter(walk.schema)), offset=offset, count=count)
                    remaining_bytes = min(CSV_BYTES, max_bytes - total_bytes)
                    if remaining_bytes <= 0:
                        raise ValueError("Open Payments acquisition byte budget exhausted")
                    result = download_original(
                        evidence,
                        lambda acquirer, store: acquirer.download(url, store=store, max_bytes=remaining_bytes),
                        stage="open-payments-csv-range",
                        max_bytes=remaining_bytes,
                        validate_url=validate_url,
                        transport=transport,
                    )
                    chunk = {
                        "request_url": result["request_url"],
                        "sha256": result["response_sha256"],
                        "byte_size": result["byte_size"],
                        "observed_at": result["observed_at"],
                        "offset": offset,
                        "row_count": count,
                    }
                    family.setdefault("csv_chunks", []).append(chunk)
                    total_bytes += chunk["byte_size"]
                    _save(path, manifest)
                    for item in _csv_records(store, selected, chunk, walk=walk, offset=offset, previous=previous):
                        previous = int(item["raw"]["record_number"])
                    offset += count
                if not family.get("csv_chunks"):
                    walk.finish()
                family["count"] = walk.count
                if family.get("csv_chunks"):
                    control = acquire(
                        client, page_url(selected, size=1) + "&results=false", "open-payments-count-recheck"
                    )
                    family["final_count_control"] = _descriptor(control)
                    _check_count(store, family["final_count_control"], selected, walk)
            final = acquire(client, CATALOG_URL, "open-payments-release-recheck")
            if any(
                select_summary(final, aggregation_level=level, program_year=year)
                != select_summary(catalog, aggregation_level=level, program_year=year)
                for level in TITLES
            ):
                raise ValueError("Open Payments catalog/release changed during acquisition")
            manifest.update(complete=True, acquired_bytes=total_bytes, final_catalog=_descriptor(final))
            _save(path, manifest)
        evidence.finish()
        return path
    except BaseException as error:
        evidence.finish(error)
        raise


def build(manifest_path: Path, output_dir: Path):
    """Replay all declared populations into the maintained table/generation path; fetch nothing."""
    from spicy_docs.sources.cms_open_payments import TITLES, SummaryWalk, select_summary

    manifest, output_dir = json.loads(Path(manifest_path).read_text()), Path(output_dir)
    if not manifest.get("complete") or set(manifest["families"]) != set(TITLES):
        raise ValueError("Open Payments requires all declared complete summary populations")
    if (output_dir / "generation").exists():
        raise ValueError("Use a fresh Open Payments candidate output directory")
    evidence, store = CaptureEvidence(output_dir, "cms-open-payments"), open_raw()
    try:
        catalog = load_capture(store, manifest["catalog"])
        final = load_capture(store, manifest["final_catalog"])
        evidence.capture(catalog, stage="open-payments-catalog")
        evidence.capture(final, stage="open-payments-release-recheck")
        year, release = manifest["program_year"], None
        selections = {}
        for level, family in manifest["families"].items():
            selected = select_summary(catalog, aggregation_level=level, program_year=year)
            if selected.as_dict() != family["selection"] or selected != select_summary(
                final, aggregation_level=level, program_year=year
            ):
                raise ValueError("Open Payments catalog or release/category selection differs")
            if release is not None and selected.release != release:
                raise ValueError("Open Payments summary release mismatch")
            selections[level], release = selected, selected.release

        def records():
            for level, selected in selections.items():
                family = manifest["families"][level]
                walk = SummaryWalk(selected)
                for descriptor in family["pages"]:
                    c = load_capture(store, descriptor)
                    evidence.capture(c, stage="open-payments-replayed-page", aggregation_level=level)
                    yield from walk.add(c)
                offset, previous = walk.observed, walk.previous
                for chunk in family.get("csv_chunks", []):
                    for item in _csv_records(
                        store, selected, chunk, walk=walk, offset=offset, previous=previous, evidence=evidence
                    ):
                        previous = int(item["raw"]["record_number"])
                        yield item
                    offset += chunk["row_count"]
                if family.get("csv_chunks"):
                    control = _check_count(store, family["final_count_control"], selected, walk)
                    evidence.capture(control, stage="open-payments-count-recheck")
                else:
                    walk.finish()
                if offset != walk.count or offset != family["count"]:
                    raise ValueError("Open Payments tested population differs from its source count")
                evidence.event(
                    "open-payments-complete-summary",
                    aggregation_level=level,
                    rows=offset,
                    program_year=year,
                    release=release,
                    source_coverage="complete observed count-matching cohort",
                )

        table = build_cms_open_payments(records(), output_dir, year=year, release=release)
        artifact = build_generation(
            output_dir / "generation",
            family="cms-open-payments",
            files=(table,),
            expected_keys=(table.name,),
            schemas={TABLE: described_schema(SCHEMA)},
            inputs=evidence.inputs(),
            owned_output_root=output_dir,
        )
        evidence.finish()
        return artifact, evidence.artifact_dir
    except BaseException as error:
        evidence.finish(error)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    acquire = sub.add_parser("capture")
    acquire.add_argument("--year", required=True, type=int)
    acquire.add_argument("--output", required=True, type=Path)
    replay = sub.add_parser("build")
    replay.add_argument("--manifest", required=True, type=Path)
    replay.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.command == "capture":
        print(capture(args.output, year=args.year))
    else:
        artifact, evidence = build(args.manifest, args.output)
        print(json.dumps({"pin": artifact.pin.as_dict(), "evidence": str(evidence)}))


if __name__ == "__main__":
    main()
