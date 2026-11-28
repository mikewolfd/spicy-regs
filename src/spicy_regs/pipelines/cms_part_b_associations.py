"""Acquire or build CMS Part B manufacturer and drug-use association tables from retained originals."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.pipelines.raw_releases import READ_INDEX_HELP, inherit_read_index, open_raw, raw_original
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_cms_part_b_associations import (
    FAMILY,
    MAX_RESOURCES_BYTES,
    MAX_WORKBOOK_BYTES,
    SCHEMAS,
    build_cms_part_b_associations,
)

FORMAT = "cms-part-b-associations-captures-v1"
STAGE_RESOURCES = "cms-part-b-resources"
STAGE_WORKBOOK = "cms-part-b-manufacturer-workbook"
_CMS_HOST = "data.cms.gov"


def _require_cms_url(url: str, *, label: str) -> str:
    if not isinstance(url, str) or not url.strip():
        raise ValueError(f"CMS Part B {label} URL must be a non-empty HTTPS URL")
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != _CMS_HOST
        or parsed.port is not None
        or parsed.username
        or parsed.password
    ):
        raise ValueError(f"CMS Part B {label} URL must point directly to https://{_CMS_HOST}/")
    return url


def select_workbook_resource(resources_body: bytes, *, year: int) -> dict[str, Any]:
    """Select the unique supporting Manufacturer & Drug Use ``.xlsx`` resource for ``year``."""
    if type(year) is not int or year < 2000:
        raise ValueError("CMS Part B workbook selection requires an explicit integer benefit year")
    try:
        payload = json.loads(resources_body)
    except ValueError as exc:
        raise ValueError("CMS Part B dataset-resources payload is not valid JSON") from exc
    meta = payload.get("meta") if isinstance(payload, dict) else None
    if (
        not isinstance(payload, dict)
        or not isinstance(meta, dict)
        or meta.get("success") is not True
        or not isinstance(payload.get("data"), list)
    ):
        raise ValueError("CMS Part B dataset-resources payload lacks a successful data resource list")
    year_token = str(year)
    matches = []
    for item in payload["data"]:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        url = item.get("downloadURL")
        size = item.get("fileSize")
        if (
            isinstance(name, str)
            and isinstance(url, str)
            and "manufacturer" in name.lower()
            and "drug use" in name.lower()
            and year_token in name
            and urlsplit(url).path.lower().endswith(".xlsx")
        ):
            if type(size) is not int or not 0 < size <= MAX_WORKBOOK_BYTES:
                raise ValueError("CMS Part B workbook resource lacks a bounded positive integer fileSize")
            _require_cms_url(url, label="workbook download")
            matches.append({"name": name, "fileSize": size, "downloadURL": url})
    if len(matches) != 1:
        raise ValueError(
            f"Expected exactly 1 CMS Part B {year} Manufacturer & Drug Use .xlsx resource, found {len(matches)}"
        )
    return matches[0]


def acquire(
    source_resources_url: str,
    *,
    year: int,
    source_release_logical_id: str,
    source_release_digest: str,
    output_dir: Path,
    transport=None,
    archive_root: Path | None = None,
) -> dict[str, Any]:
    """Fetch the release resources JSON and supporting workbook via CaptureEvidence into the raw archive."""
    _require_cms_url(source_resources_url, label="dataset-resources")
    for label, value in (
        ("source_release_logical_id", source_release_logical_id),
        ("source_release_digest", source_release_digest),
    ):
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"CMS Part B acquisition requires a non-empty {label}")

    headers = {"User-Agent": "spicy-regs/1.0", "Accept-Encoding": "identity"}
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    evidence = CaptureEvidence(output_dir, FAMILY, archive_root=archive_root)
    try:
        def fetch(url: str, *, stage: str, max_bytes: int) -> tuple[bytes, dict[str, Any]]:
            retained = evidence.transport(transport, stage=stage, max_bytes=max_bytes)
            with httpx.Client(transport=retained, headers=headers, timeout=120.0, follow_redirects=False) as client:
                response = client.get(url)
                response.raise_for_status()
                body = response.content
                observed_at = datetime.now(UTC).isoformat()
                return body, {
                    "requested_url": url,
                    "sha256": "sha256:" + hashlib.sha256(body).hexdigest(),
                    "byte_size": len(body),
                    "observed_at": observed_at,
                }

        resources_body, resources_capture = fetch(
            source_resources_url, stage=STAGE_RESOURCES, max_bytes=MAX_RESOURCES_BYTES
        )
        resource = select_workbook_resource(resources_body, year=year)
        workbook_body, workbook_capture = fetch(
            resource["downloadURL"], stage=STAGE_WORKBOOK, max_bytes=MAX_WORKBOOK_BYTES
        )
        if len(workbook_body) != resource["fileSize"]:
            raise ValueError(
                f"Downloaded CMS Part B workbook size ({len(workbook_body)}) differs from "
                f"dataset-resources fileSize ({resource['fileSize']})"
            )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()

    return {
        "format": FORMAT,
        "benefit_year": year,
        "source_release_logical_id": source_release_logical_id,
        "source_release_digest": source_release_digest,
        "source_resources_url": source_resources_url,
        "resources_capture": resources_capture,
        "workbook_capture": {**workbook_capture, "resource_name": resource["name"]},
    }


def build(
    selection: dict[str, Any],
    output_dir: Path,
    *,
    archive_root: Path | None = None,
    read_index: Path | None = None,
    expected_hcpcs_codes=None,
):
    """Revalidate retained raw originals, build both association tables, and admit one receipts-free generation."""
    if not isinstance(selection, dict) or selection.get("format") != FORMAT:
        raise ValueError(f"CMS Part B associations selection must use format {FORMAT!r}")
    year = selection.get("benefit_year")
    if type(year) is not int or year < 2000:
        raise ValueError("CMS Part B associations selection requires an explicit integer benefit_year")
    source_resources_url = _require_cms_url(selection.get("source_resources_url", ""), label="dataset-resources")
    release_logical_id = selection.get("source_release_logical_id")
    release_digest = selection.get("source_release_digest")
    if not isinstance(release_logical_id, str) or not release_logical_id.strip():
        raise ValueError("CMS Part B associations selection requires source_release_logical_id")
    if not isinstance(release_digest, str) or not release_digest.strip():
        raise ValueError("CMS Part B associations selection requires source_release_digest")

    resources_capture = selection.get("resources_capture")
    workbook_capture = selection.get("workbook_capture")
    if not isinstance(resources_capture, dict) or not isinstance(workbook_capture, dict):
        raise ValueError("CMS Part B associations selection requires resources_capture and workbook_capture")

    output_dir = Path(output_dir)
    if output_dir.exists():
        raise ValueError("Use a fresh output directory for each CMS Part B associations generation")

    store = open_raw(archive_root)
    evidence = CaptureEvidence(output_dir, FAMILY, archive_root=archive_root)

    def retain_original(capture: dict[str, Any], *, stage: str, limit: int, native_id: str) -> bytes:
        url = _require_cms_url(capture.get("requested_url", ""), label=native_id)
        byte_size = capture.get("byte_size")
        observed_at = capture.get("observed_at")
        sha256 = capture.get("sha256")
        if (
            type(byte_size) is not int
            or not 0 < byte_size <= limit
            or not isinstance(observed_at, str)
            or not observed_at
            or not isinstance(sha256, str)
            or not sha256
        ):
            raise ValueError(f"CMS Part B {native_id} capture lacks a valid digest, bounded size, or observed_at")
        path = raw_original(store, sha256, byte_size)
        retained = evidence.retain_file(
            path,
            stage=stage,
            raw_source=True,
            original_location=url,
            source_url=url,
            dataset_id="cms-part-b-drug-spending",
            native_id=native_id,
            source_version=f"{year}",
            covered_period=f"{year}",
            observed_at=observed_at,
        )
        if retained.get("sha256") != sha256 or retained.get("byte_size") != byte_size:
            raise ValueError(f"CMS Part B retained {native_id} differs from its selected digest or byte size")
        return path.read_bytes()

    try:
        if resources_capture.get("requested_url") != source_resources_url:
            raise ValueError("CMS Part B resources_capture requested_url differs from source_resources_url")
        resources_bytes = retain_original(
            resources_capture,
            stage=STAGE_RESOURCES,
            limit=MAX_RESOURCES_BYTES,
            native_id="dataset-resources",
        )
        resource = select_workbook_resource(resources_bytes, year=year)
        if workbook_capture.get("requested_url") != resource["downloadURL"]:
            raise ValueError("CMS Part B workbook URL differs from its exact dataset-resources selection")
        if workbook_capture.get("byte_size") != resource["fileSize"]:
            raise ValueError("CMS Part B workbook byte_size differs from its dataset-resources fileSize")

        workbook_bytes = retain_original(
            workbook_capture,
            stage=STAGE_WORKBOOK,
            limit=MAX_WORKBOOK_BYTES,
            native_id="manufacturer-drug-use-workbook",
        )
        read_snapshot = inherit_read_index(evidence, read_index)
        files = build_cms_part_b_associations(
            workbook_bytes,
            output_dir / "tables",
            year=year,
            source_workbook_url=workbook_capture["requested_url"],
            source_resources_url=source_resources_url,
            source_release_logical_id=release_logical_id,
            source_release_digest=release_digest,
            source_workbook_sha256=workbook_capture["sha256"],
            source_resources_sha256=resources_capture["sha256"],
            observed_at=workbook_capture["observed_at"],
            selection_metadata={
                "resource_name": resource["name"],
                "resource_file_size": resource["fileSize"],
                **(
                    {"release_selection": selection["source_release_selection"]}
                    if isinstance(selection.get("source_release_selection"), dict)
                    else {}
                ),
            },
            expected_hcpcs_codes=expected_hcpcs_codes,
        )
        evidence.event(
            "cms-part-b-associations-build",
            benefit_year=year,
            source_release_logical_id=release_logical_id,
            source_release_digest=release_digest,
            source_resources_url=source_resources_url,
            source_resources_sha256=resources_capture["sha256"],
            source_workbook_url=workbook_capture["requested_url"],
            source_workbook_sha256=workbook_capture["sha256"],
            source_workbook_byte_size=workbook_capture["byte_size"],
            resource_name=resource["name"],
            tables=sorted(SCHEMAS),
        )
        artifact = build_generation(
            output_dir / "generation",
            family=FAMILY,
            files=files,
            expected_keys=[f"{table}.parquet" for table in SCHEMAS],
            schemas={table: described_schema(schema) for table, schema in SCHEMAS.items()},
            inputs=evidence.inputs(),
            read_snapshot=read_snapshot,
            owned_output_root=output_dir,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    acquisition = commands.add_parser("acquire")
    acquisition.add_argument("--source-resources-url", required=True)
    acquisition.add_argument("--year", required=True, type=int)
    acquisition.add_argument("--source-release-logical-id", required=True)
    acquisition.add_argument("--source-release-digest", required=True)
    acquisition.add_argument("--captures-out", required=True, type=Path)
    acquisition.add_argument("--output-dir", required=True, type=Path)
    acquisition.add_argument("--archive-root", type=Path)

    candidate = commands.add_parser("build")
    candidate.add_argument("--captures", required=True, type=Path)
    candidate.add_argument("--output-dir", required=True, type=Path)
    candidate.add_argument("--archive-root", type=Path)
    candidate.add_argument("--read-index", type=Path, help=READ_INDEX_HELP)

    args = parser.parse_args(argv)
    if args.command == "acquire":
        selection = acquire(
            args.source_resources_url,
            year=args.year,
            source_release_logical_id=args.source_release_logical_id,
            source_release_digest=args.source_release_digest,
            output_dir=args.output_dir,
            archive_root=args.archive_root,
        )
        args.captures_out.parent.mkdir(parents=True, exist_ok=True)
        args.captures_out.write_text(json.dumps(selection, indent=2, sort_keys=True) + "\n")
        print(json.dumps(selection, sort_keys=True))
    else:
        artifact, evidence = build(
            json.loads(args.captures.read_bytes()),
            args.output_dir,
            archive_root=args.archive_root,
            read_index=args.read_index,
        )
        print(
            json.dumps(
                {
                    "generation": str(args.output_dir / "generation"),
                    "evidence": str(evidence),
                    "pin": artifact.pin.as_dict(),
                },
                sort_keys=True,
            )
        )


if __name__ == "__main__":
    main()
