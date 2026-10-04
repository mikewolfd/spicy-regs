"""Prepare a complete source family from qualified retained captures; never publish.

Run from spicy-regs with the installed, locked provider. Private capture bodies
and generated artifacts stay beneath an external output directory.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from contextlib import contextmanager
from hashlib import sha256
import importlib.util
from importlib.metadata import version
import json
from pathlib import Path

import yaml

from spicy_docs.transport.captured import CapturedBodyResponse
from spicy_regs.scorecards.etl import SOURCE_NAMES, generation_options, read_family, read_source_generation
from spicy_regs.generations import build_generation, implementation_id, verify_generation
from spicy_regs.scorecards.registry import REGISTRY
from spicy_regs.source_evidence import CaptureEvidence, verify_evidence
from spicy_regs.sources import publication
from spicy_regs.transforms.build_scorecards import OUTPUTS, build_scorecards


def write(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def load_lcv():
    path = Path(__file__).parents[1] / "lcv_2025/qualify_lcv.py"
    spec = importlib.util.spec_from_file_location("lcv_retained_qualification", path)
    if spec is None or spec.loader is None:
        raise ValueError("Missing qualified LCV replay helper")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SequenceReplay:
    """Replay the selected first/second capture sequence, plus bounded listing reads."""

    def __init__(self, directory: Path, selected: list[str], prefix: str):
        self.directory, self.used = directory, []
        self.queues = defaultdict(list)
        self.offsets = defaultdict(int)
        # Qualification acquired a single edition directly. The production
        # listing independently checks these same retained catalogs first.
        initial = [prefix + suffix for suffix in ("-index", "-app", "-common", "-bills", "-styles")]
        for stem in [*initial, *selected]:
            receipt = json.loads((directory / f"{stem}.receipt.json").read_bytes())
            if receipt.get("http_status", receipt.get("source_http_status")) != 200:
                raise ValueError("Replay selection includes an unsuccessful source response")
            self.queues[receipt["requested_url"]].append((stem, receipt))

    def fetch(self, url):
        offset = self.offsets[url]
        if url not in self.queues or offset >= len(self.queues[url]):
            raise ValueError("Reader requested an unqualified retained observation")
        stem, receipt = self.queues[url][offset]
        self.offsets[url] += 1
        body = (self.directory / f"{stem}.body").read_bytes()
        if len(body) != receipt["byte_size"] or sha256(body).hexdigest() != receipt["sha256"].removeprefix("sha256:"):
            raise ValueError("Retained response differs from its receipt")
        self.used.append(stem)
        self.last_receipt = receipt
        return CapturedBodyResponse(
            url,
            receipt["resolved_url"],
            200,
            receipt["content_type"],
            receipt["observed_at"],
            body,
            content_encoding=receipt.get("content_encoding") or "identity",
        )

    def complete(self):
        if any(self.offsets[url] != len(queue) for url, queue in self.queues.items()):
            raise ValueError("Reader did not consume the selected complete retained observation sequence")


def comparable(rows):
    omitted = {"snapshot_id", "capture_id", "capture_ids_json", "capture_roles_json"}
    return sorted(json.dumps({k: v for k, v in row.items() if k not in omitted}, sort_keys=True) for row in rows)


def prepare(args):
    if args.output.exists():
        raise ValueError("Use a new private output directory")
    args.output.mkdir(parents=True)
    index = publication.parse_index(args.index.read_bytes())
    if "scorecards" in index["families"]:
        raise ValueError(
            "This initial-publication recipe requires absent scorecards; use scoped refresh for later updates"
        )
    lcv_module = load_lcv()
    lcv_replay = lcv_module.Retained(args.lcv_captures)
    afp_selected = json.loads((args.afp_captures / "qualified.selected.json").read_bytes())
    afp_replay = SequenceReplay(args.afp_captures, afp_selected, "afp")
    ijm_selected = json.loads((args.ijm_captures / "qualified.selected.json").read_bytes())
    ijm_replay = SequenceReplay(args.ijm_captures, ijm_selected, "ijm")
    replays = {"lcv": lcv_replay, "afp": afp_replay, "ijm": ijm_replay}
    qualification_ids = {
        "lcv": "lcv-2025-retained-source-and-analysis-2026-10-03",
        "afp": "afp-3933-retained-api-and-raw-literals-2026-10-03",
        "ijm": "ijm-3995-retained-native-browser-and-raw-literals-2026-10-03",
    }
    registry = yaml.safe_load(REGISTRY.read_bytes())
    registry["sources"] = [source for source in registry["sources"] if source["publisher_id"] in replays]
    for source in registry["sources"]:
        source.update(
            enabled=True, evidence_policy="hash_only", qualification_id=qualification_ids[source["publisher_id"]]
        )
    registry_path = args.output / "selected-registry.yaml"
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False))
    evidence = CaptureEvidence(args.output, "scorecards")
    evidence.inherit(index, public_url=args.public_url)
    evidence.event(
        "retained-source-replay", publishers=sorted(replays), acquisition="previously qualified original captures"
    )

    @contextmanager
    def factory(source):
        replay = replays[source.publisher_id]

        def fetch(url):
            response = replay.fetch(url)
            if source.publisher_id == "ijm":
                receipt = replay.last_receipt
                source.event(
                    "scorecard-proxy",
                    provider="zyte",
                    mode=receipt["representation"],
                    proxied_client="zyte",
                    body_is_publisher_bytes=receipt["body_is_publisher_bytes"],
                    zyte_request_id=receipt["provider_request_id"],
                    requested_url=receipt["requested_url"],
                    resolved_url=receipt["resolved_url"],
                    status_code=receipt["source_http_status"],
                    byte_size=receipt["byte_size"],
                    body_retained=False,
                    sha256="sha256:" + receipt["sha256"],
                    acquisition="retained_replay",
                )
            return response

        yield fetch

    try:
        files = build_scorecards(
            args.output,
            evidence=evidence,
            registry=registry_path,
            publishers=tuple(replays),
            force=True,
            fetch_factory=factory,
            download_prior=lambda key, path: False,
        )
        rows = read_family(args.output, SOURCE_NAMES)
        if {row["publisher_id"] for row in rows["scorecards"]} != set(replays):
            raise ValueError("A selected publisher did not produce an accepted complete edition")
        afp_replay.complete()
        ijm_replay.complete()
        references = {
            "afp": json.loads((args.afp_captures / "qualified.tables.json").read_bytes()),
            "ijm": json.loads((args.ijm_captures / "qualified.tables.json").read_bytes()),
            "lcv": read_source_generation(args.lcv_qualified_generation),
        }
        for publisher, tables in references.items():
            ids = {row["scorecard_id"] for row in tables["scorecards"]}
            for name, expected in tables.items():
                actual = [
                    row
                    for row in rows[name]
                    if (
                        row["publisher_id"] == publisher
                        if name == "scorecard_publishers"
                        else row["scorecard_id"] in ids
                    )
                ]
                if comparable(actual) != comparable(expected):
                    raise ValueError(f"Retained literal replay changed qualified {publisher} {name}")
        lcv_module.direct_readback(lcv_replay, references["lcv"])
        generation = args.output / "generation"
        artifact = build_generation(
            generation,
            family="scorecards",
            files=files,
            expected_keys=OUTPUTS,
            **generation_options(args.output, SOURCE_NAMES),
            read_snapshot=index,
            inputs=evidence.inputs(),
        )
        verify_generation(generation, expected_pin=artifact.pin)
        verify_evidence(evidence.artifact_dir)
        if any(path.is_file() for path in (evidence.artifact_dir / "blobs").rglob("*")):
            raise ValueError("Hash-only deployment evidence unexpectedly contains body blobs")
        report = {
            "status": "prepared_not_published",
            "generation": artifact.pin.as_dict(),
            "generation_directory": str(generation),
            "evidence_directory": str(evidence.artifact_dir),
            "read_snapshot_path": str(args.index),
            "read_snapshot_sha256": sha256(args.index.read_bytes()).hexdigest(),
            "provider_version": version("spicy-docs"),
            "implementation_id": implementation_id(),
            "counts": {name: len(group) for name, group in rows.items()},
            "editions": [
                {key: row[key] for key in ("scorecard_id", "publisher_id", "title")} for row in rows["scorecards"]
            ],
            "qualification_ids": qualification_ids,
            "literal_reference_comparison": "all rows matched excluding opaque observation IDs",
            "network_source_acquisitions": 0,
            "public_evidence_policy": "hash_only",
            "original_bodies_in_evidence": 0,
        }
        write(args.output / "preparation.json", report)
        print(json.dumps(report, indent=2))
    except BaseException as error:
        evidence.finish(error)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index", type=Path, required=True)
    parser.add_argument("--lcv-captures", type=Path, action="append", required=True)
    parser.add_argument("--lcv-qualified-generation", type=Path, required=True)
    parser.add_argument("--afp-captures", type=Path, required=True)
    parser.add_argument("--ijm-captures", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--public-url", default="https://data.spicygov.ai")
    prepare(parser.parse_args())


if __name__ == "__main__":
    main()
