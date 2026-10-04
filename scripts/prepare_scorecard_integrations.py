"""Prepare one atomic scorecard family from pinned, qualified private inputs.

Use installed publisher readers and the normal replacement/ETL pipeline. This
command performs no publication and no publisher network requests. The existing
publish_candidate.py command separately publishes a verified prepared generation.
"""

import argparse
from contextlib import contextmanager
from datetime import UTC, datetime
from hashlib import sha256
from importlib.metadata import version
import json
from pathlib import Path
from uuid import UUID, uuid4

import pyarrow.parquet as pq
from rulespec_artifacts import LocalMemberSource, iter_member_descriptors
import yaml

from spicy_regs.generations import build_generation, implementation_id, verify_generation
from spicy_regs.scorecards.etl import SOURCE_NAMES, generation_options, read_family, read_indexed_family
from spicy_regs.scorecards.registry import REGISTRY
from spicy_regs.scorecards.acquisition import MAX_BYTES, MAX_REQUESTS, validate_limits
from spicy_regs.scorecards.extraction_replay import PageObservationReplay, page_bytes, read_page
from spicy_regs.scorecards.retained import RetainedScorecardBatch, canonical, check_preserved, pinned_bytes
from spicy_regs.source_evidence import CaptureEvidence, verify_evidence
from spicy_regs.sources import publication
from spicy_regs.transforms.build_scorecards import OUTPUTS, build_scorecards, installed_provider


def write(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def prepare(args):
    if args.output.exists() or args.private_observations.exists():
        raise ValueError("Use fresh candidate and private observation directories")
    if args.private_observations.resolve().is_relative_to(args.output.resolve()):
        raise ValueError("Private observations must remain outside the publication tree")
    plan_bytes = pinned_bytes(args.plan, args.plan_sha256, max_bytes=2 * 1024**2)
    plan = json.loads(plan_bytes)
    if plan.get("schema_version") != "1" or not isinstance(plan.get("entries"), list) or not plan["entries"]:
        raise ValueError("Unknown qualified scorecard plan")
    limits = plan.get("acquisition_limits", {})
    max_bytes, max_requests = limits.get("max_bytes", MAX_BYTES), limits.get("max_requests", MAX_REQUESTS)
    validate_limits(max_bytes, max_requests)
    index = publication.parse_index(args.index.read_bytes())
    provider = installed_provider()
    args.output.mkdir(parents=True)
    args.private_observations.mkdir(parents=True)
    evidence = CaptureEvidence(args.output, "scorecards")
    evidence.inherit(index, public_url=args.public_url)
    active_scope, private_receipts = {}, []

    def retain_observations(document, body, *, stage):
        scope = active_scope["scope"]
        target = args.private_observations / (uuid4().hex + ".json")
        with target.open("xb") as stream:
            stream.write(body)
        if target.read_bytes() != body:
            raise ValueError("Private observation retention changed the qualified bytes")
        receipt = scope.retain_bytes(body, stage=stage)
        identity = str(UUID(hex=receipt["capture_id"]))
        scope.event(
            "scorecard-observation-source-link",
            observation_id=identity,
            observation_capture_id=receipt["capture_id"],
            source_capture_id=document.capture_id,
        )
        private_receipts.append(
            dict(
                observation_id=identity,
                observation_capture_id=receipt["capture_id"],
                source_capture_id=document.capture_id,
                source_sha256=sha256(document.body).hexdigest(),
                observations_sha256=sha256(body).hexdigest(),
                path=str(target),
            )
        )
        write(args.private_observations / "receipts.json", private_receipts)
        return {"observation_id": identity}

    def retain_extraction(document, page, *, stage):
        retained = retain_observations(document, page_bytes(page), stage=stage)
        return {"extraction_id": retained["observation_id"]}

    grouped = {}
    for entry in plan["entries"]:
        grouped.setdefault(entry["edition"]["publisher_id"], []).append(entry)
    batches = {
        publisher: RetainedScorecardBatch(
            args.corpus,
            entries,
            retain_observations=retain_observations,
            max_bytes=max_bytes,
            max_requests=max_requests,
        )
        for publisher, entries in grouped.items()
    }
    pages = [
        read_page(pinned_bytes(batch.path(page["file"]), page["sha256"]))
        for batch in batches.values()
        for entry in batch.entries.values()
        for page in entry.get("extraction_pages", [])
    ]
    extractor = PageObservationReplay(pages) if pages else None
    original_get = provider.get_adapter
    registry = yaml.safe_load(REGISTRY.read_bytes())
    selected_rows = []
    for row in registry["sources"]:
        if row["publisher_id"] not in batches:
            continue
        entry = grouped[row["publisher_id"]][0]
        row.update(
            adapter=entry["adapter"],
            enabled=True,
            historical_backfill=True,
            evidence_policy="hash_only",
            qualification_id="scorecard-plan:sha256:" + args.plan_sha256,
        )
        selected_rows.append(row)
    if {row["publisher_id"] for row in selected_rows} != set(batches):
        raise ValueError("Qualified plan names a publisher absent from the installed registry")
    adapter_batches = {row["adapter"]: batches[row["publisher_id"]] for row in selected_rows}
    provider.get_adapter = lambda name: adapter_batches.get(name) or original_get(name)
    selected_registry = args.output / "selected-registry.yaml"
    selected_registry.write_text(yaml.safe_dump(dict(version=1, sources=selected_rows), sort_keys=False))
    prior_rows, prior_directory = {}, None

    def download_prior(key, destination):
        nonlocal prior_directory
        prior_directory = destination.parent
        member = publication.single_member(index, key)
        if not publication.fetch_member(args.public_url, member, destination):
            raise ValueError("A pinned prior scorecard table is unavailable")
        prior_rows[key.removesuffix(".parquet")] = pq.ParquetFile(destination).read().to_pylist()
        return True

    @contextmanager
    def factory(scope):
        active_scope["scope"] = scope
        try:
            yield batches[scope.publisher_id]
        finally:
            active_scope.pop("scope", None)

    evidence.event(
        "scorecard-qualified-input-plan",
        plan_sha256="sha256:" + args.plan_sha256,
        scorecard_ids=sorted(
            entry["edition_object"].scorecard_id for b in batches.values() for entry in b.entries.values()
        ),
        publisher_network_requests=0,
    )
    try:
        files = build_scorecards(
            args.output,
            evidence=evidence,
            registry=selected_registry,
            publishers=tuple(batches),
            historical_backfill=True,
            force=True,
            provider=provider,
            fetch_factory=factory,
            download_prior=download_prior,
            receipt_public_url=args.public_url,
            max_bytes=max_bytes,
            max_requests=max_requests,
            pdf_extractor=extractor,
            retain_extraction=retain_extraction if extractor else None,
            validate_acquisitions=lambda: [batch.complete() for batch in batches.values()],
        )
        for batch in batches.values():
            batch.complete()
        if extractor:
            extractor.complete()
        current = read_family(args.output, SOURCE_NAMES)
        if index["families"].get("scorecards", {}).get("etlReceipts"):
            if prior_directory is None:
                raise ValueError("Prior source family was not retained before its receipt verification")
            prior_rows = read_indexed_family(prior_directory, SOURCE_NAMES, index["families"]["scorecards"])
        elif "scorecards" not in index["families"]:
            prior_rows = {name: [] for name in SOURCE_NAMES}
        editions = {key for batch in batches.values() for key in batch.entries}
        preserved = check_preserved(prior_rows, current, scorecard_ids=editions, publisher_ids=set(batches))
        expected_publishers = [row for batch in batches.values() for row in batch.reference_publishers.values()]
        actual_publishers = [row for row in current["scorecard_publishers"] if row["publisher_id"] in batches]
        if canonical(actual_publishers, observation_ids=True) != canonical(expected_publishers, observation_ids=True):
            raise ValueError("Latest publisher observations differ from their qualified references")
        generation = build_generation(
            args.output / "generation",
            family="scorecards",
            files=files,
            expected_keys=OUTPUTS,
            **generation_options(args.output, SOURCE_NAMES),
            read_snapshot=index,
            inputs=evidence.inputs(),
        )
        verify_generation(args.output / "generation", expected_pin=generation.pin)
        source_evidence = verify_evidence(evidence.artifact_dir)
        members = list(iter_member_descriptors(source_evidence, LocalMemberSource(evidence.artifact_dir)))
        if any(member.role not in {"journal", "lineage-metadata"} for member in members):
            raise ValueError("Publisher or model bytes escaped restricted public evidence")
        journal = [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()]
        captures = [row for row in journal if row["event"] == "capture"]
        if not captures or any(
            row.get("blob_member") is not None or row["evidence_policy"] != "hash_only" for row in captures
        ):
            raise ValueError("Selected source evidence differs from hash-only publication policy")
        report = dict(
            status="prepared_not_published",
            observed_at=datetime.now(UTC).isoformat(),
            generation=generation.pin.as_dict(),
            generation_directory=str(args.output / "generation"),
            evidence_directory=str(evidence.artifact_dir),
            read_snapshot_path=str(args.index),
            prior_generation=index["families"].get("scorecards", {}).get("artifactDigest"),
            provider_version=version("spicy-docs"),
            implementation_id=implementation_id(),
            accepted_scopes=sorted(editions),
            preserved_row_counts=preserved,
            counts={name: len(rows) for name, rows in current.items()},
            source_evidence=source_evidence.pin.as_dict(),
            plan_sha256=args.plan_sha256,
            hash_only_capture_observations=len(captures),
            publisher_or_model_bodies_in_public_evidence=0,
            source_network_requests=0,
            acquisition_limits=dict(max_bytes=max_bytes, max_requests=max_requests),
            private_observation_receipts=str(args.private_observations / "receipts.json"),
        )
        write(args.output / "preparation.json", report)
        return report
    except BaseException as error:
        evidence.finish(error)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("index", "plan", "corpus", "output", "private-observations"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--public-url", default="https://data.spicygov.ai")
    print(json.dumps(prepare(parser.parse_args()), indent=2))


if __name__ == "__main__":
    main()
