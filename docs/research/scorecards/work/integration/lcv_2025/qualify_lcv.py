"""Offline LCV qualification using installed readers and hash-only evidence.

Run from spicy-regs with uv run --frozen python <this-file> --help.
Original inputs and all candidate outputs belong in an external private corpus.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import csv
from dataclasses import replace
import hashlib
import io
import json
from pathlib import Path
import shutil
from urllib.parse import parse_qsl, urlsplit

from bs4 import BeautifulSoup
import yaml

from spicy_docs.sources.scorecards import lcv
from spicy_docs.transport.captured import CapturedBodyResponse
from spicy_regs.generations import build_generation, verify_generation
from spicy_regs.source_evidence import PRIOR_ROLE, CaptureEvidence, verify_evidence
from spicy_regs.scorecards.registry import REGISTRY
from spicy_regs.sources import publication
from spicy_regs.scorecards.etl import SOURCE_NAMES, generation_options, read_source_generation
from spicy_regs.transforms.build_scorecards import OUTPUTS, ScorecardRefreshError, build_scorecards, installed_provider


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()


def write(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def normalized(url: str):
    value = urlsplit(url)
    return value.scheme, value.netloc, value.path, tuple(sorted(parse_qsl(value.query, keep_blank_values=True)))


class Retained:
    def __init__(self, directories: list[Path]):
        self.index = {}
        self.used = {}
        for directory in directories:
            for path in sorted(directory.glob("*.receipt.json")):
                receipt = json.loads(path.read_bytes())
                if (
                    receipt.get("http_status") != 200
                    or urlsplit(receipt.get("requested_url", "")).netloc != "www.lcv.org"
                ):
                    continue
                body = path.with_name(path.name.replace(".receipt.json", ".body"))
                if not body.is_file():
                    continue
                key = normalized(receipt["requested_url"])
                # Explicitly select the latest retained observation for a URL.
                prior = self.index.get(key)
                if prior is None or receipt["observed_at"] > prior[1]["observed_at"]:
                    self.index[key] = path, receipt, body

    def fetch(self, url: str):
        path, receipt, body_path = self.index[normalized(url)]
        body = body_path.read_bytes()
        if len(body) != receipt["byte_size"] or hashlib.sha256(body).hexdigest() != receipt["sha256"].removeprefix(
            "sha256:"
        ):
            raise ValueError("Retained publisher bytes differ from their receipt")
        self.used[str(path)] = {
            "receipt_sha256": digest(path),
            "source_sha256": "sha256:" + hashlib.sha256(body).hexdigest(),
            "byte_size": len(body),
            "requested_url": receipt["requested_url"],
            "resolved_url": receipt["resolved_url"],
            "observed_at": receipt["observed_at"],
        }
        return CapturedBodyResponse(
            receipt["requested_url"],
            receipt["resolved_url"],
            receipt["http_status"],
            receipt["content_type"],
            receipt["observed_at"],
            body,
            content_encoding=receipt.get("content_encoding") or "identity",
            method=receipt.get("method", "GET"),
        )

    def factory(self, mutation=None):
        @contextmanager
        def selected(_source):
            def fetch(url):
                response = self.fetch(url)
                return mutation(url, response) if mutation else response

            yield fetch

        return selected


def local_registry(output: Path):
    registry = yaml.safe_load(REGISTRY.read_bytes())
    selected = next(source for source in registry["sources"] if source["publisher_id"] == "lcv")
    selected.update(enabled=True, qualification_id="local-retained-lcv-2025", evidence_policy="hash_only")
    path = output / "local-only-registry.yaml"
    path.write_text(yaml.safe_dump({"version": 1, "sources": [selected]}, sort_keys=False))
    return path


def entry(artifact, directory: Path):
    from rulespec_artifacts import LocalMemberSource, iter_member_descriptors

    members = list(iter_member_descriptors(artifact, LocalMemberSource(directory)))
    result = {
        "prefix": f"generations/{artifact.root['spec']['family']}/" + artifact.pin.artifact_digest.removeprefix("sha256:"),
        "logicalId": artifact.pin.logical_id,
        "artifactDigest": artifact.pin.artifact_digest,
        "tables": publication.table_entries(
            artifact.root["spec"]["tables"], [m for m in members if m.object_key != "etl_receipts.parquet"]
        ),
    }

    if "etlReceipts" in artifact.root["spec"]:
        spec = artifact.root["spec"]["etlReceipts"]
        member = next(m for m in members if m.object_key == "etl_receipts.parquet")
        result["etlReceipts"] = {
            "key": member.object_key,
            "sha256": member.sha256,
            "byteSize": member.byte_size,
            "rows": member.record_count,
            "columns": spec["columns"],
            "generationId": spec["generationId"],
            "datasets": [policy["dataset"] for policy in spec["policies"]],
        }
    return result


def build_source(retained: Retained, output: Path, registry: Path, *, mutation=None, prior=None):
    evidence = CaptureEvidence(output, "scorecards")
    snapshot = publication.empty_index()
    if prior:
        from rulespec_artifacts import ArtifactInput

        snapshot["families"]["scorecards"] = prior["entry"]
        evidence.prior_input = ArtifactInput(PRIOR_ROLE, prior["entry"]["logicalId"], prior["entry"]["artifactDigest"])
    # Local replay selects an explicit immutable prior; no network inheritance.
    evidence.read_snapshot = snapshot
    evidence.event("qualification-local-lineage", source_generation=prior["entry"]["artifactDigest"] if prior else None)

    def download(key, destination):
        if prior is None:
            return False
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(prior["directory"] / key, destination)
        return True

    try:
        files = build_scorecards(
            output,
            evidence=evidence,
            registry=registry,
            publishers=("lcv",),
            editions=("2025",),
            force=True,
            fetch_factory=retained.factory(mutation),
            download_prior=download,
            download_prior_receipts=lambda target: download("etl_receipts.parquet", target),
        )
    except Exception as error:
        evidence.finish(error)
        raise
    generation = output / "generation"
    artifact = build_generation(
        generation,
        family="scorecards",
        files=files,
        expected_keys=OUTPUTS,
        **generation_options(output, SOURCE_NAMES),
        read_snapshot=snapshot,
        inputs=evidence.inputs(),
    )
    verify_generation(generation, expected_pin=artifact.pin)
    assert evidence.artifact is not None, "Successful source build must seal its evidence"
    verify_evidence(evidence.artifact_dir, expected_pin=evidence.artifact.pin)
    return {
        "directory": generation,
        "artifact": artifact,
        "entry": entry(artifact, generation),
        "evidence": evidence,
        "files": files,
    }


def direct_readback(retained: Retained, tables: dict):
    from urllib.parse import urlencode

    response = retained.fetch(lcv.MEMBERS + "?" + urlencode(lcv.MEMBER_QUERY))
    raw_rows = list(csv.reader(io.StringIO(response.body.decode("utf-8-sig"))))
    katie = next(
        (n, row)
        for n, row in enumerate(raw_rows, 1)
        if len(row) == 7 and row[-1] == "https://www.lcv.org/moc/katie-britt/"
    )
    html_response = retained.fetch(lcv.MEMBERS + "?session_year=2025")
    html = BeautifulSoup(html_response.body, "html.parser")
    link = html.select_one('a.card-link[href="https://www.lcv.org/moc/katie-britt/"]')
    if link is None:
        raise ValueError("Retained HTML has no Katie Britt member link")
    card = link.find_parent(class_="congress-item")
    if card is None:
        raise ValueError("Retained HTML member link has no member card")
    observed = [n.get_text(strip=True) for n in card.select(".data-score")]
    member = next(row for row in tables["scorecard_members"] if row["publisher_member_id"] == katie[1][-1])
    ratings = [
        row
        for row in tables["scorecard_member_ratings"]
        if row["publisher_member_key"] == member["publisher_member_key"]
    ]
    assert {row["metric_id"]: row["value_text"] for row in ratings} == {"annual": katie[1][4], "lifetime": katie[1][5]}
    assert katie[1][4:6] == ["0", "2"] and observed == ["0%", "2%"]
    item = tables["scorecard_items"][0]
    detail = retained.fetch(item["source_url"])
    preamble = list(csv.reader(io.StringIO(detail.body.decode("utf-8-sig"))))
    year = next((n, row[1]) for n, row in enumerate(preamble, 1) if row and row[0] == "Year")
    assert item["item_date_text"] == year[1]
    return {
        "member_csv_sha256": response.sha256,
        "member_csv_row": katie[0],
        "csv_literal_scores": katie[1][4:6],
        "html_sha256": html_response.sha256,
        "html_locator": 'a.card-link[href="https://www.lcv.org/moc/katie-britt/"] / parent .congress-item / .data-score',
        "html_display_scores": observed,
        "publisher_member_key": member["publisher_member_key"],
        "item_id": item["item_id"],
        "item_capture_sha256": detail.sha256,
        "item_year_csv_row": year[0],
        "item_year_literal": year[1],
        "item_congress_literal": item["congress_text"],
        "item_session_literal": item["session_text"],
        "csv_blank_annual_cells": sum(len(row) == 7 and row != lcv.MEMBER_HEADER and row[4] == "" for row in raw_rows),
        "csv_na_annual_cells": sum(len(row) == 7 and row[4] == "na" for row in raw_rows),
        "direct_source_readback": True,
    }


def qualify(retained: Retained, output: Path):
    if output.exists():
        raise ValueError("Qualification output must be new; preserve previous runs")
    output.mkdir(parents=True)
    registry = local_registry(output)
    first = build_source(retained, output / "initial", registry)
    tables = read_source_generation(first["directory"])
    readback = direct_readback(retained, tables)
    second = build_source(retained, output / "replacement", registry, prior=first)
    replacement_tables = read_source_generation(second["directory"])
    for name, rows in tables.items():
        contract = installed_provider().contracts[name]
        replacement = replacement_tables[name]
        if name == "scorecard_snapshots":
            assert len(rows) == len(replacement) == 1
            assert rows[0]["snapshot_id"] != replacement[0]["snapshot_id"]
        else:
            assert {contract.key(r) for r in rows} == {contract.key(r) for r in replacement}
    before = {path.name: digest(path) for path in first["files"]}
    failures = []
    for case in ("truncated_csv", "missing_detail", "http_404", "layout_drift"):

        def mutate(url, response, case=case):
            if case == "truncated_csv" and "export-type=moc-listing" in url:
                return replace(response, body=response.body[: len(response.body) // 2])
            if "/roll-call-vote/" in url and "?" not in url:
                if case == "missing_detail":
                    raise FileNotFoundError("Selected retained detail absent")
                if case == "http_404":
                    return replace(response, status_code=404, body=b"Not found")
                if case == "layout_drift":
                    return replace(response, body=response.body.replace(b"export-post-id", b"changed-post-field"))
            return response

        try:
            build_source(retained, output / ("failure-" + case), registry, mutation=mutate, prior=first)
        except ScorecardRefreshError:
            assert before == {path.name: digest(path) for path in first["files"]}
            assert not (output / ("failure-" + case) / "generation").exists()
            failures.append({"case": case, "prior_unchanged": True, "candidate_created": False})
        else:
            raise AssertionError("Failed capture unexpectedly produced a candidate")
    for run in [first, second]:
        evidence = run["evidence"]
        assert not any(path.is_file() for path in (evidence.artifact_dir / "blobs").rglob("*"))
        journal = [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_bytes().splitlines()]
        captures = [row for row in journal if row["event"] == "capture"]
        assert captures and all(
            row["body_retained"] is False and row["evidence_policy"] == "hash_only" and "blob_member" not in row
            for row in captures
        )
    from importlib.metadata import version

    report = {
        "status": "qualified_local_source_family",
        "production_enabled": False,
        "provider_version": version("spicy-docs"),
        "parser_version": lcv.parser_version,
        "counts": {name: len(rows) for name, rows in tables.items()},
        "source_declared_counts": json.loads(tables["scorecard_snapshots"][0]["source_declared_counts_json"]),
        "completeness_rule": tables["scorecard_snapshots"][0]["completeness_rule"],
        "source_generation": first["artifact"].pin.as_dict(),
        "source_generation_directory": str(first["directory"]),
        "source_family_entry": first["entry"],
        "replacement_generation": second["artifact"].pin.as_dict(),
        "evidence_pin": first["evidence"].artifact.pin.as_dict(),
        "evidence_directory": str(first["evidence"].artifact_dir),
        "retained_input_selection": "latest observed retained response by original URL with query-order equivalence; exact body hash/size verified",
        "retained_inputs": retained.used,
        "direct_readback": readback,
        "failure_preservation": failures,
        "public_evidence_raw_blobs": 0,
        "analysis_status": "pending_official_inputs",
    }
    write(output / "qualification.json", report)
    print(json.dumps({k: report[k] for k in ("status", "counts", "parser_version")}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--retained-dir", type=Path, action="append", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    qualify(Retained(args.retained_dir), args.output_dir)


if __name__ == "__main__":
    main()
