"""Retain and publish admitted drug-spending observations through one CMS workflow."""

from __future__ import annotations

import argparse
from datetime import UTC, date, datetime
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
from rulespec_artifacts import ArtifactPin, LocalMemberSource, Producer

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation, implementation_id
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.cms_drug_spending import IDENTITY_FIELDS

SERIES_BY_FAMILY = {
    "cms-part-d": "Medicare Part D Spending by Drug",
    "cms-medicaid": "Medicaid Spending by Drug",
    "cms-part-b": "Medicare Part B Spending by Drug",
}
FAMILY_BY_SERIES = {title: family for family, title in SERIES_BY_FAMILY.items()}


def build(reader, output_dir: Path, *, year: int, label, family, measures, schema, build_table, dictionary_fields=None):
    """Retain originals once and use the ordinary generation path; acquire nothing."""
    output_dir = Path(output_dir)
    if (output_dir / "generation").exists():
        raise ValueError(f"Use a fresh output directory for each CMS {label} generation")
    evidence = CaptureEvidence(output_dir, family)
    try:
        evidence.event(
            "cms-source-release",
            source_logical_id=reader.pin.logical_id,
            source_artifact_digest=reader.pin.artifact_digest,
            collection_outcome=reader.collection_outcome,
            selected_benefit_year=year,
        )
        retained = set()
        dictionary_seen = False
        with TemporaryDirectory(dir=output_dir) as temporary:
            for capture in reader.collection_outcome["requestedScope"]["captures"]:
                ref = capture["response_sha256"]
                if capture["role"] == "dictionary":
                    body = reader.read_evidence(ref, max_bytes=capture["byte_size"])
                    selection = reader.collection_outcome["requestedScope"]["selection"]
                    if ref != selection["dictionary_sha256"] or capture["request_url"] != selection["dictionary_url"]:
                        raise ValueError(f"CMS {label} dictionary differs from its selected source")
                    fields = {field["name"] for field in json.loads(body)["data"]["fields"]}
                    required = (
                        set(dictionary_fields)
                        if dictionary_fields is not None
                        else {
                            *IDENTITY_FIELDS,
                            "Tot_Mftr",
                            f"Outlier_Flag_{year}",
                            *(f"{prefix}_{year}" for prefix in measures.values()),
                        }
                    )
                    if not required <= fields:
                        raise ValueError(f"CMS {label} dictionary lacks a selected-year field")
                    dictionary_seen = True
                if ref not in retained:
                    # Stream the admitted original to private staging, exhausting the
                    # verified iterator before retaining it through the evidence writer.
                    original = Path(temporary) / "original"
                    with original.open("wb") as stream:
                        for chunk in reader.iter_evidence(ref):
                            stream.write(chunk)
                    saved = evidence.retain_file(
                        original,
                        stage="cms-source-" + capture["role"],
                        raw_source=True,
                        evidence_blob_ref=ref,
                        source_url=capture["request_url"],
                        # Not the staging path: this journal is published.
                        original_location=capture["request_url"],
                    )
                    if saved.get("sha256") != ref or saved.get("byte_size") != capture["byte_size"]:
                        raise ValueError(f"CMS {label} retained evidence differs from its selection")
                    retained.add(ref)
        if not dictionary_seen:
            raise ValueError(f"CMS {label} requires its selected dictionary")
        table = build_table(reader, output_dir, year=year)
        artifact = build_generation(
            output_dir / "generation",
            family=family,
            files=(table,),
            expected_keys=(table.name,),
            schemas={table.stem: described_schema(schema)},
            inputs=evidence.inputs(),
            owned_output_root=output_dir,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir


def main(build, description):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--selection", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    from spicy_docs.source_native import SourceNativeReleaseReader
    from spicy_docs.sources.cms_profile import CMS_PROFILE
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore

    selection = json.loads(args.selection.read_text())
    item = selection["source"]
    policy_version = item.get("acquisitionPolicyVersion", "1.1")
    if policy_version == "1.1":
        profile = CMS_PROFILE
    else:
        # Historical policy selection requires the matching SpicyDocs source.
        from spicy_docs.sources.cms_profile import cms_profile_for_policy

        profile = cms_profile_for_policy(policy_version)
    reader = SourceNativeReleaseReader(
        LocalMemberSource(item["release"]),
        blob_source=LocalSourceNativeBlobStore(item["blobs"], create=False),
        profile=profile,
        accepted_verifier_implementation_ids=frozenset({item["verifierImplementationId"]}),
        expected_pin=ArtifactPin(item["logicalId"], item["artifactDigest"]),
    )
    artifact, evidence = build(reader, args.output_dir, year=selection["benefit_year"])
    print(
        json.dumps(
            {
                "directory": str(args.output_dir / "generation"),
                "sourceEvidence": str(evidence),
                "logicalId": artifact.pin.logical_id,
                "artifactDigest": artifact.pin.artifact_digest,
            }
        )
    )


def acquire(
    series: str,
    output_dir: Path,
    *,
    release_date: str | None = None,
    page_size: int = 5000,
    transport=None,
) -> Path:
    """Acquire a bounded CMS drug-spending release and write ``selection.json`` for the build CLI."""
    from spicy_docs.reading.json_input import load_integer_json
    from spicy_docs.reading.paged_json import PagedJsonBudget
    from spicy_docs.source_native import SourceNativeReleaseBuild, SourceNativeReleasePublisher
    from spicy_docs.sources.cms import (
        CATALOG_URL,
        MAX_CATALOG_BYTES,
        MAX_PAGE_BYTES,
        MAX_PAGES,
        CmsError,
        CmsReader,
        cms_selection,
        resolve_release,
    )
    from spicy_docs.sources.cms_profile import CMS_PROFILE, cms_scope, iter_retained_pages
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore
    from spicy_docs.transport.captured import CapturedBodyResponse

    series_title = SERIES_BY_FAMILY.get(series, series)
    if series_title not in FAMILY_BY_SERIES:
        raise ValueError(f"Unsupported CMS drug-spending series or family: {series!r}")
    family = FAMILY_BY_SERIES[series_title]
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    selection_path = output_dir / "selection.json"
    if selection_path.exists() or (output_dir / "source-release").exists():
        raise ValueError(f"Use a fresh output directory for CMS {family} acquisition")

    evidence = CaptureEvidence(output_dir, family)
    try:
        headers = {"User-Agent": "spicy-regs/1.0", "Accept-Encoding": "identity"}

        def fetch_control(url: str, *, max_bytes: int) -> CapturedBodyResponse:
            retained = evidence.transport(transport, stage="cms-control", max_bytes=max_bytes)
            with httpx.Client(transport=retained, headers=headers, timeout=120.0, follow_redirects=False) as client:
                response = client.get(url)
                response.raise_for_status()
                return CapturedBodyResponse(
                    url,
                    str(response.url),
                    response.status_code,
                    response.headers.get("content-type"),
                    datetime.now(UTC).isoformat(),
                    response.content,
                )

        catalog = fetch_control(CATALOG_URL, max_bytes=MAX_CATALOG_BYTES)
        resolved = resolve_release(
            load_integer_json(catalog.body, source="CMS", error_type=CmsError),
            series_title=series_title,
            release_date=release_date,
        )
        dictionary = fetch_control(resolved["dictionary_url"], max_bytes=MAX_PAGE_BYTES)
        stats = fetch_control(resolved["api_url"] + "/stats", max_bytes=MAX_PAGE_BYTES)
        controls = [catalog, dictionary, stats]
        selected = cms_selection(*controls, series_title=series_title, release_date=release_date)

        budget = PagedJsonBudget(
            max_requests=MAX_PAGES,
            max_page_bytes=MAX_PAGE_BYTES,
            timeout_seconds=60.0,
            min_request_interval_seconds=0.2,
        )
        with CmsReader(
            selected,
            budget=budget,
            transport=evidence.transport(transport, stage="cms-data", max_bytes=MAX_PAGE_BYTES),
        ) as reader:
            pages = [page.capture for page in reader.read(size=page_size, max_pages=MAX_PAGES)]

        scope = cms_scope(selected, controls, pages)
        blobs = LocalSourceNativeBlobStore(output_dir / "source-blobs")
        for capture in [*controls, *pages]:
            blobs.put_blob(capture.sha256, capture.byte_size, [capture.body])
        implementation = implementation_id()
        producer = Producer(
            product="spicy-docs",
            implementation_id=implementation,
            verifier_id="urn:spicy-regs:source-native-release-verifier",
            verifier_version="2.0",
            verifier_implementation_id=implementation,
        )
        now = datetime.now(UTC)
        published = SourceNativeReleasePublisher(CMS_PROFILE, blob_store=blobs, clock=lambda: now).publish(
            iter_retained_pages(scope, blob_source=blobs),
            destination=output_dir / "source-release",
            build=SourceNativeReleaseBuild(
                query_scope=scope,
                producer=producer,
                started_at=now.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            ),
        )
        selection_doc = {
            "benefit_year": date.fromisoformat(selected["release_date"]).year,
            "source": {
                "release": str(published.root),
                "blobs": str(blobs.root),
                "logicalId": published.artifact.pin.logical_id,
                "artifactDigest": published.artifact.pin.artifact_digest,
                "verifierImplementationId": implementation,
                "acquisitionPolicyVersion": CMS_PROFILE.acquisition_policy_version,
            },
        }
        selection_path.write_text(json.dumps(selection_doc, indent=2, sort_keys=True) + "\n")
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return selection_path


def acquire_main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Acquire a bounded CMS drug-spending release.")
    parser.add_argument(
        "--series",
        required=True,
        choices=[*sorted(SERIES_BY_FAMILY), *SERIES_BY_FAMILY.values()],
        help="CMS family (cms-part-d, cms-medicaid, cms-part-b) or DCAT series title",
    )
    parser.add_argument("--release-date", help="Optional release startDate (YYYY-MM-DD); default selects latest")
    parser.add_argument("--page-size", type=int, default=5000, help="CMS API page size (1..5000)")
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args(argv)
    path = acquire(args.series, args.output_dir, release_date=args.release_date, page_size=args.page_size)
    print(json.dumps({"selection": str(path)}))


if __name__ == "__main__":
    acquire_main()
