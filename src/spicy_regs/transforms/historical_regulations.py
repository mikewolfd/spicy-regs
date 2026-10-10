"""Offline, receipt-bound issue and dated-snapshot destinations from pinned originals."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
from dataclasses import asdict
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from urllib.parse import urlsplit

import pyarrow as pa

from spicy_regs.etl_receipts import DatasetPolicy, ReceiptContext, combine_receipts, write_dataset


def _schema(strings, integers=()):
    return pa.schema([(name, pa.string()) for name in strings] + [(name, pa.int64()) for name in integers])


SCHEMAS = {
    "federal_register_issues": _schema(
        (
            "issue_observation_id",
            "package_id",
            "publication_date",
            "source_sha256",
            "metadata_json",
            "retained_renditions_json",
        ),
        ("source_bytes",),
    ),
    "ecfr_snapshots": _schema(
        (
            "snapshot_id",
            "snapshot_date",
            "selected_part",
            "selected_section",
            "source_sha256",
            "source_url",
            "native_metadata_json",
        ),
        ("title", "source_bytes", "content_nodes"),
    ),
    "ecfr_snapshot_sections": _schema(
        ("snapshot_id", "xml_position", "kind", "native_number", "part", "heading", "text", "attributes_json"),
        ("byte_start",),
    ),
}
POLICIES = {
    name: DatasetPolicy(
        name,
        schema,
        ("snapshot_id", "xml_position")
        if name.endswith("sections")
        else ("snapshot_id",)
        if name == "ecfr_snapshots"
        else ("issue_observation_id",),
        ("selection_metadata",),
        policy_version="historical-regulations-public-v2",
    )
    for name, schema in SCHEMAS.items()
}


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _digest(body):
    return "sha256:" + hashlib.sha256(body).hexdigest()


def _publisher_url(value):
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or parsed.hostname not in {"www.govinfo.gov", "govinfo.gov"}
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("issue rendition requires a public GovInfo publisher URL")
    return value


def _read(spec, base, maximum):
    path = base / spec["path"]
    encoding = spec.get("encoding", "identity")
    if encoding not in {"identity", "gzip"}:
        raise ValueError("unsupported retained storage encoding")
    with gzip.open(path, "rb") if encoding == "gzip" else path.open("rb") as stream:
        body = stream.read(maximum + 1)
    if len(body) > maximum or _digest(body) != spec["sha256"] or len(body) != spec["bytes"]:
        raise ValueError("retained original digest or decoded byte bound differs")
    return body


def build_historical_regulations(manifest: Path, output: Path) -> Path:
    """Read one complete explicit selection, then atomically expose local outputs.

    Inputs are decoded-original SHA-256 pins, not baseline rows. No download,
    incremental prior, publication or OCR runs. Per-XML bound is 256 MiB;
    manifest is bounded to 4 MiB and 1,000 selected records. Each source is
    parsed independently; accumulated output is streamed to the shared receipt
    writer. A refused source leaves the requested output directory absent.
    """
    manifest, output = Path(manifest), Path(output)
    with manifest.open("rb") as stream:
        raw = stream.read(4 * 1024 * 1024 + 1)
    if len(raw) > 4 * 1024 * 1024:
        raise ValueError("selection manifest exceeds byte bound")
    selection: dict[str, Any] = json.loads(raw)
    if selection.get("format") != "spicy-historical-regulations" or selection.get("version") != 1:
        raise ValueError("unsupported historical regulation selection")
    sources: list[dict[str, Any]] = selection["sources"]
    if not isinstance(sources, list) or not 1 <= len(sources) <= 1000:
        raise ValueError("selection requires 1..1000 originals")
    if output.exists():
        raise FileExistsError(output)
    manifest_digest = _digest(raw)
    generation = _digest(_json(["historical-regulations-public-v2", manifest_digest]).encode())
    seen = set()

    def records(dataset):
        for ordinal, spec in enumerate(sources):
            kind = spec["kind"]
            if kind not in {"fr-issue", "ecfr-snapshot"}:
                raise ValueError("unsupported historical regulation source kind")
            if (dataset.startswith("federal_register")) != (kind == "fr-issue"):
                continue
            body = _read(spec, manifest.parent, 256 * 1024 * 1024)
            witness = {"source_id": spec["sha256"], "source_uri": spec["sha256"], "sha256": spec["sha256"]}
            context = ReceiptContext(
                generation,
                f"{dataset}:{ordinal}",
                "historical-regulations-public-v2",
                [
                    witness,
                    {"source_id": "selection-manifest", "source_uri": manifest_digest, "sha256": manifest_digest},
                ],
            )
            metadata = {"kind": kind, "source_sha256": spec["sha256"], "source_bytes": len(body)}
            if kind == "fr-issue":
                from spicy_docs.sources.federal_register.issues import read_fr_issue

                facts = read_fr_issue(body, package_id=spec["package_id"])
                for rendition in facts["renditions"]:
                    _publisher_url(rendition["url"])
                retained = []
                for original in spec.get("retained_renditions", []):
                    if original["source_url"] not in {rendition["url"] for rendition in facts["renditions"]}:
                        raise ValueError("retained rendition URL is not advertised by this issue")
                    _read(original, manifest.parent, 256 * 1024 * 1024)
                    retained.append(
                        {"source_url": original["source_url"], "sha256": original["sha256"], "bytes": original["bytes"]}
                    )
                metadata.update(package_id=facts["package_id"], retained_renditions=retained)
                identity = _digest(_json([spec["package_id"], facts["source_sha256"]]).encode())
                row = {
                    "issue_observation_id": identity,
                    "package_id": facts["package_id"],
                    "publication_date": facts["publication_date"],
                    "source_sha256": facts["source_sha256"],
                    "source_bytes": facts["source_bytes"],
                    "metadata_json": _json(facts),
                    "retained_renditions_json": _json(retained),
                }
                if (dataset, identity) in seen:
                    raise ValueError("repeated issue observation")
                seen.add((dataset, identity))
                yield {**row, "selection_metadata": metadata}, context
            else:
                from spicy_docs.sources.cfr.models import EcfrSelection
                from spicy_docs.sources.cfr.snapshots import read_ecfr_snapshot

                request = EcfrSelection(**spec["selection"])
                facts, nodes = read_ecfr_snapshot(body, selection=request, final_url=spec["source_url"])
                metadata.update(selection=asdict(request), source_url=facts["source_url"])
                identity = _digest(_json([asdict(request), spec["sha256"]]).encode())
                if (dataset, identity) in seen:
                    raise ValueError("repeated dated snapshot observation")
                seen.add((dataset, identity))
                if dataset == "ecfr_snapshots":
                    facts["native_metadata_json"] = _json(facts.pop("native_metadata"))
                    yield {**facts, "snapshot_id": identity, "selection_metadata": metadata}, context
                else:
                    for node in nodes:
                        node["attributes_json"] = _json(node.pop("attributes"))
                        yield {**node, "snapshot_id": identity, "selection_metadata": metadata}, context

    output.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=output.parent, prefix=".historical-regulations-") as temporary:
        stage = Path(temporary) / "bundle"
        stage.mkdir()
        receipts = []
        for dataset, policy in POLICIES.items():
            _, receipt = write_dataset(records(dataset), stage / dataset, policy)
            receipts.append(receipt)
        combine_receipts(receipts, stage / "etl_receipts.parquet")
        (stage / "selection.json").write_bytes(raw)
        (stage / "bundle.json").write_text(
            _json(
                {
                    "format": "spicy-historical-regulations-bundle",
                    "version": 1,
                    "generation": generation,
                    "policies": [policy.descriptor() for policy in POLICIES.values()],
                    "outcome": "complete-selected-inputs",
                    "source_count": len(sources),
                }
            )
        )
        from rulespec_artifacts import publish_directory_no_replace

        publish_directory_no_replace(stage, output)
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    print(build_historical_regulations(args.manifest, args.output))


if __name__ == "__main__":
    main()
