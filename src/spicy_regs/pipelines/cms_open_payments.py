"""Capture/resume one Open Payments year, replay originals, build an ordinary generation.

Raw bytes live only in CentralRawArchiveStore through CaptureEvidence. The small
manifest is a restart cursor, never a source-acceptance certificate. A refresh
uses a fresh output directory; a missing later row is not inferred deleted.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_cms_open_payments import SCHEMA, TABLE, build_cms_open_payments


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


def capture(
    output_dir: Path, *, year: int, max_bytes=3 * 1024**3, max_requests=2200, timeout_seconds=1800, transport=None
):
    """Persist every successful page before advancing; resume exact retained pages without HTTP."""
    from spicy_docs.sources.cms_open_payments import (
        CATALOG_URL,
        MAX_PAGE_BYTES,
        TITLES,
        SummaryWalk,
        page_url,
        select_summary,
        validate_url,
    )
    from spicy_docs.transport.captured import CapturedBodyResponse
    from spicy_docs.transport.download import BoundedAcquirer
    from spicy_regs.pipelines.raw_releases import open_raw

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "capture-manifest.json"
    manifest = (
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
            if "catalog" in manifest:
                catalog = load_capture(store, manifest["catalog"])
            else:
                catalog = acquire(client, CATALOG_URL, "open-payments-catalog")
                manifest["catalog"] = _descriptor(catalog)
                _save(path, manifest)
            total_bytes = sum(p["byte_size"] for f in manifest["families"].values() for p in f["pages"])
            for level in TITLES:
                selection = select_summary(catalog, aggregation_level=level, program_year=year)
                selected = manifest["families"].setdefault(level, {"selection": selection.as_dict(), "pages": []})
                if selected["selection"] != selection.as_dict():
                    raise ValueError("Open Payments resume release selection differs")
                walk = SummaryWalk(selection)
                for descriptor in selected["pages"]:
                    for _ in walk.add(load_capture(store, descriptor)):
                        pass
                while walk.count is None or walk.observed < walk.count:
                    if total_bytes + MAX_PAGE_BYTES > max_bytes:
                        raise ValueError("Open Payments acquisition byte budget exhausted")
                    c = acquire(client, page_url(selection, offset=walk.observed), "open-payments-summary-page")
                    for _ in walk.add(c):
                        pass
                    if walk.count > 1_500_000:
                        raise ValueError("Open Payments population exceeds its admitted row bound")
                    selected["pages"].append(_descriptor(c))
                    total_bytes += c.byte_size
                    _save(path, manifest)
                walk.finish()
                selected["count"] = walk.count
            final_catalog = acquire(client, CATALOG_URL, "open-payments-release-recheck")
            if any(
                select_summary(final_catalog, aggregation_level=level, program_year=year)
                != select_summary(catalog, aggregation_level=level, program_year=year)
                for level in TITLES
            ):
                raise ValueError("Open Payments catalog/release changed during acquisition")
            manifest.update(complete=True, acquired_bytes=total_bytes, final_catalog=_descriptor(final_catalog))
            _save(path, manifest)
        evidence.finish()
        return path
    except BaseException as error:
        evidence.finish(error)
        raise


def build(manifest_path: Path, output_dir: Path):
    """Replay the exact full selection through maintained table/generation writers; fetch nothing."""
    from spicy_docs.sources.cms_open_payments import TITLES, iter_summary, select_summary
    from spicy_regs.pipelines.raw_releases import open_raw

    manifest, output_dir = json.loads(Path(manifest_path).read_text()), Path(output_dir)
    if not manifest.get("complete") or set(manifest["families"]) != set(TITLES):
        raise ValueError("Open Payments requires all declared complete summary populations")
    if (output_dir / "generation").exists():
        raise ValueError("Use a fresh Open Payments candidate output directory")
    evidence, store = CaptureEvidence(output_dir, "cms-open-payments"), open_raw()
    try:
        catalog = load_capture(store, manifest["catalog"])
        final_catalog = load_capture(store, manifest["final_catalog"])
        evidence.capture(catalog, stage="open-payments-catalog")
        evidence.capture(final_catalog, stage="open-payments-release-recheck")
        year, release = manifest["program_year"], None
        selections = {}
        for level, family in manifest["families"].items():
            selected = select_summary(catalog, aggregation_level=level, program_year=year)
            if selected.as_dict() != family["selection"] or selected != select_summary(
                final_catalog, aggregation_level=level, program_year=year
            ):
                raise ValueError("Open Payments catalog or release/category selection differs")
            if release is not None and selected.release != release:
                raise ValueError("Open Payments summary release mismatch")
            selections[level], release = selected, selected.release

        def records():
            for level, selected in selections.items():
                family = manifest["families"][level]

                def pages():
                    for descriptor in family["pages"]:
                        c = load_capture(store, descriptor)
                        evidence.capture(c, stage="open-payments-replayed-page", aggregation_level=level)
                        yield c

                count = 0
                for item in iter_summary(selected, pages()):
                    count += 1
                    yield item
                if count != family["count"]:
                    raise ValueError("Open Payments tested population differs from its manifest")
                evidence.event(
                    "open-payments-complete-summary",
                    aggregation_level=level,
                    rows=count,
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
