"""Prepare a qualified HRC scope update while preserving every other live row.

Reads the installed provider and already retained source/observation bytes.
The output is local only. Publishing requires the separate explicit publisher.
"""

from __future__ import annotations

import argparse
from collections import Counter
from contextlib import contextmanager
from hashlib import sha256
import importlib
from importlib.metadata import version
import json
import os
from pathlib import Path
import re
from uuid import UUID, uuid4

import pyarrow.parquet as pq
from rulespec_artifacts import LocalMemberSource, iter_member_descriptors
import yaml

from spicy_docs.transport.captured import CapturedBodyResponse
from spicy_regs.scorecards.etl import SOURCE_NAMES, generation_options, read_family
from spicy_regs.generations import build_generation, implementation_id, verify_generation
from spicy_regs.scorecards.registry import REGISTRY
from spicy_regs.source_evidence import CaptureEvidence, verify_evidence
from spicy_regs.sources import publication
from spicy_regs.transforms.build_scorecards import OUTPUTS, build_scorecards, installed_provider


def write(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def checked_source(body: bytes, receipt: dict, *, pdf=True) -> CapturedBodyResponse:
    """Use recorded HTTP facts; a generic successful-capture label is insufficient."""
    if (
        receipt.get("status_code") != 200
        or type(receipt.get("status_code")) is not int
        or receipt.get("byte_size") != len(body)
        or receipt.get("sha256") != "sha256:" + sha256(body).hexdigest()
        or (pdf and not body.startswith(b"%PDF-"))
    ):
        raise ValueError("The retained source lacks matching successful HTTP evidence")
    return CapturedBodyResponse(
        receipt["requested_url"],
        receipt["resolved_url"],
        receipt["status_code"],
        receipt["content_type"],
        receipt["observed_at"],
        body,
        content_encoding=receipt.get("content_encoding") or "identity",
    )


def canonical(rows, *, omit_observation_ids=False):
    omitted = {"snapshot_id", "capture_id", "capture_ids_json", "capture_roles_json"} if omit_observation_ids else set()

    def normalize(row):
        row = {key: value for key, value in row.items() if key not in omitted}
        if omit_observation_ids:

            def locator(value):
                return (
                    re.sub(
                        r"(?<=;observation=)[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}(?=;)", "<observation-id>", value
                    )
                    if isinstance(value, str)
                    else value
                )

            for name in ("source_path", "position_source_path"):
                if name in row:
                    row[name] = locator(row[name])
            if row.get("references_json"):
                references = json.loads(row["references_json"])
                for reference in references:
                    reference["source_path"] = locator(reference["source_path"])
                row["references_json"] = json.dumps(references, sort_keys=True)
        return json.dumps(row, sort_keys=True)

    return Counter(normalize(row) for row in rows)


def check_scopes(prior, current, reference):
    """Verify full preserved rows and source-qualified replacement rows separately."""
    if set(prior) != set(current) or set(reference) != set(current):
        raise ValueError("Scope comparison requires the entire source table family")
    editions = {row["scorecard_id"] for row in reference["scorecards"]}
    if not editions or {row["publisher_id"] for row in reference["scorecards"]} != {"hrc"}:
        raise ValueError("The selected reference must contain only accepted HRC editions")
    preserved_counts = {}
    for name in current:
        field, replaced = ("publisher_id", {"hrc"}) if name == "scorecard_publishers" else ("scorecard_id", editions)
        old = [row for row in prior[name] if row[field] not in replaced]
        kept = [row for row in current[name] if row[field] not in replaced]
        fresh = [row for row in current[name] if row[field] in replaced]
        if canonical(old) != canonical(kept):
            raise ValueError(f"Unselected source rows changed: {name}")
        if canonical(fresh, omit_observation_ids=True) != canonical(reference[name], omit_observation_ids=True):
            raise ValueError(f"HRC output changed from its qualified source reference: {name}")
        preserved_counts[name] = len(old)
    return editions, preserved_counts


def check_public_evidence(directory: Path):
    """Lineage metadata is allowed; publisher and model bodies are not."""
    artifact = verify_evidence(directory)
    members = list(iter_member_descriptors(artifact, LocalMemberSource(directory)))
    if any(member.role not in {"journal", "lineage-metadata"} for member in members):
        raise ValueError("Restricted source evidence contains a body member")
    events = [json.loads(line) for line in (directory / "journal.jsonl").read_text().splitlines()]
    captures = [event for event in events if event.get("event") == "capture"]
    if not captures or any(
        event.get("publisher_id") != "hrc"
        or event.get("evidence_policy") != "hash_only"
        or event.get("blob_member") is not None
        for event in captures
    ):
        raise ValueError("HRC source evidence escaped its hash-only scope")
    return artifact, len(captures)


def prepare(args):
    for directory in (args.output, args.private_observations):
        if directory.exists():
            raise ValueError("Use fresh candidate and private observation directories")
    if args.private_observations.resolve().is_relative_to(args.output.resolve()):
        raise ValueError("Private model observations must remain outside the publication output tree")
    index = publication.parse_index(args.index.read_bytes())
    if "scorecards" not in index["families"]:
        raise ValueError("HRC scope update requires the captured live source family")
    source = checked_source(args.source_body.read_bytes(), json.loads(args.source_receipt.read_bytes()))
    landing = checked_source(args.landing_body.read_bytes(), json.loads(args.landing_receipt.read_bytes()), pdf=False)
    observations = args.qualified_observations.read_bytes()
    if sha256(observations).hexdigest() != args.observations_sha256.removeprefix("sha256:"):
        raise ValueError("Qualified HRC observations differ from the accepted asset pin")
    reference = json.loads(args.reference_tables.read_bytes())
    provider = installed_provider()
    provider.validate(reference)
    args.output.mkdir(parents=True)
    args.private_observations.mkdir(parents=True)
    private_receipts = []
    active_scope = {}

    def retain_observations(document, body, **context):
        if document.body != source.body or body != observations:
            raise ValueError("Reader attempted to retain a different source or observation asset")
        target = args.private_observations / (uuid4().hex + ".json")
        with target.open("xb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        if target.read_bytes() != body:
            raise ValueError("Private observation retention changed the qualified bytes")
        scope = active_scope["scope"]
        retained = scope.retain_bytes(body, stage="hrc-qualified-observations")
        observation_id = str(UUID(retained["capture_id"]))
        scope.event("hrc-observation-source-link", observation_id=observation_id, source_capture_id=document.capture_id)
        private_receipts.append(
            {
                "observation_id": observation_id,
                "source_capture_id": document.capture_id,
                "source_sha256": "sha256:" + sha256(document.body).hexdigest(),
                "observations_sha256": "sha256:" + sha256(body).hexdigest(),
                "path": str(target),
                "stage": context.get("stage"),
            }
        )
        write(args.private_observations / "receipts.json", private_receipts)
        return {"observation_id": observation_id}

    module = importlib.import_module("spicy_docs.sources.scorecards.hrc")
    reader = module.HRCReader(qualified_observations=observations, retain_observations=retain_observations)
    original_get_adapter = provider.get_adapter
    provider.get_adapter = lambda name: reader if name == "hrc" else original_get_adapter(name)
    registry = yaml.safe_load(REGISTRY.read_bytes())
    registry["sources"] = [row for row in registry["sources"] if row["publisher_id"] == "hrc"]
    if len(registry["sources"]) != 1:
        raise ValueError("Installed consumer has no explicit HRC registry row")
    registry["sources"][0].update(
        enabled=True, evidence_policy="hash_only", qualification_id=args.qualification_id, historical_backfill=True
    )
    registry_path = args.output / "selected-registry.yaml"
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False))
    prior_rows = {}
    prior_directory = None

    def download_prior(key, destination):
        nonlocal prior_directory
        prior_directory = destination.parent
        member = publication.single_member(index, key)
        if not publication.fetch_member(args.public_url, member, destination):
            raise ValueError("The pinned live scorecard table is missing")
        prior_rows[key.removesuffix(".parquet")] = pq.ParquetFile(destination).read().to_pylist()
        return True

    requests = []

    @contextmanager
    def factory(scope):
        if scope.publisher_id != "hrc":
            raise ValueError("Only the selected HRC reader may acquire an edition")

        def fetch(url):
            captures = {source.requested_url: source, landing.requested_url: landing}
            if url not in captures or url in requests:
                raise ValueError("HRC requested an unqualified source URL or exceeded replay scope")
            requests.append(url)
            return captures[url]

        active_scope["scope"] = scope
        try:
            yield fetch
        finally:
            active_scope.pop("scope", None)

    evidence = CaptureEvidence(args.output, "scorecards")
    evidence.inherit(index, public_url=args.public_url)
    evidence.event("retained-qualified-scope-replay", publisher_id="hrc", qualification_id=args.qualification_id)
    try:
        files = build_scorecards(
            args.output,
            evidence=evidence,
            registry=registry_path,
            publishers=("hrc",),
            editions=tuple(row["scorecard_id"] for row in reference["scorecards"]),
            historical_backfill=True,
            force=True,
            provider=provider,
            fetch_factory=factory,
            download_prior=download_prior,
            receipt_public_url=args.public_url,
        )
        current = read_family(args.output, SOURCE_NAMES)
        if "etlReceipts" in index["families"]["scorecards"]:
            assert prior_directory is not None
            prior_rows = read_family(
                prior_directory,
                SOURCE_NAMES,
                generation_id=index["families"]["scorecards"]["etlReceipts"]["generationId"],
            )
        editions, preserved = check_scopes(prior_rows, current, reference)
        if not private_receipts or not requests:
            raise ValueError("Reader did not evidence both source and qualified private observations")
        artifact = build_generation(
            args.output / "generation",
            family="scorecards",
            files=files,
            expected_keys=OUTPUTS,
            **generation_options(args.output, SOURCE_NAMES),
            read_snapshot=index,
            inputs=evidence.inputs(),
        )
        verify_generation(args.output / "generation", expected_pin=artifact.pin)
        source_evidence, capture_count = check_public_evidence(evidence.artifact_dir)
        report = {
            "status": "prepared_not_published",
            "generation": artifact.pin.as_dict(),
            "generation_directory": str(args.output / "generation"),
            "evidence_directory": str(evidence.artifact_dir),
            "read_snapshot_path": str(args.index),
            "prior_generation": index["families"]["scorecards"]["artifactDigest"],
            "provider_version": version("spicy-docs"),
            "implementation_id": implementation_id(),
            "accepted_scopes": sorted(editions),
            "preserved_row_counts": preserved,
            "counts": {name: len(rows) for name, rows in current.items()},
            "qualification_id": args.qualification_id,
            "qualified_asset_sha256": "sha256:" + sha256(observations).hexdigest(),
            "source_evidence": source_evidence.pin.as_dict(),
            "hash_only_capture_observations": capture_count,
            "publisher_or_model_bodies_in_public_evidence": 0,
            "source_network_requests": 0,
            "private_observation_receipts": str(args.private_observations / "receipts.json"),
        }
        write(args.output / "preparation.json", report)
        print(json.dumps(report, indent=2))
    except BaseException as error:
        evidence.finish(error)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in (
        "index",
        "source-body",
        "source-receipt",
        "landing-body",
        "landing-receipt",
        "qualified-observations",
        "reference-tables",
        "output",
        "private-observations",
    ):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--observations-sha256", required=True)
    parser.add_argument("--qualification-id", required=True)
    parser.add_argument("--public-url", default="https://data.spicygov.ai")
    prepare(parser.parse_args())


if __name__ == "__main__":
    main()
