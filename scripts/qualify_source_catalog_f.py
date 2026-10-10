"""Offline GAO history qualification over explicit retained capture selections.

This lane calls maintained GAO parsing, shaping, history folding and native
receipt writers. It never fetches missing inputs or treats a baseline as a
publisher capture. A failed comparison remains readable in its new directory.
"""
from __future__ import annotations

import argparse
from collections import Counter
import gzip
import hashlib
import io
import json
from pathlib import Path
import re

import pyarrow as pa
import pyarrow.parquet as pq
from rulespec_artifacts import ArtifactPin, admit_artifact


def pin(path: Path) -> dict:
    with path.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    return {"path": str(path.resolve()), "sha256": "sha256:" + digest,
            "byteSize": path.stat().st_size}


def assert_pin(path: Path, expected: dict) -> None:
    actual = pin(path)
    if any(actual[key] != expected[key] for key in ("sha256", "byteSize")):
        raise ValueError(f"Changed selected input: {path}")


class RetainedArtifact:
    """Adapt the canonical compressed object store to maintained admission."""

    def __init__(self, corpus: Path, digest: str, *, byte_budget: int = 256 * 1024 * 1024):
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
            raise ValueError("Expected artifact SHA-256")
        self.corpus, self.digest = corpus, digest
        self.directory = corpus / "source-evidence" / digest[7:]
        self.root = json.loads((self.directory / "artifact.json").read_text())
        self.metadata = {"artifact.json"}
        self.members = {}
        self.remaining = byte_budget
        for ref in self.root["memberManifests"]:
            key = ref["objectKey"]
            if key != "members.json":
                raise ValueError("Unsupported capture manifest key")
            self.metadata.add(key)
            for member in json.loads((self.directory / key).read_text())["members"]:
                if member["objectKey"] in self.members:
                    raise ValueError("Repeated capture member")
                self.members[member["objectKey"]] = member

    def keys(self):
        return sorted(self.metadata | self.members.keys())

    def open(self, key: str):
        if key in self.metadata:
            path = self.directory / key
            size = path.stat().st_size
            if size > 1024 * 1024:
                raise ValueError("Capture metadata exceeds bound")
            data = path.read_bytes()
        else:
            member = self.members[key]
            digest = member["sha256"]
            if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
                raise ValueError("Invalid object digest")
            if type(member["byteSize"]) is not int or not 0 <= member["byteSize"] <= 12 * 1024 * 1024:
                raise ValueError("Capture member exceeds bound")
            path = self.corpus / "source-evidence/blobs/sha256" / digest[7:]
            if path.exists():
                with path.open("rb") as stream:
                    data = stream.read(member["byteSize"] + 1)
            else:
                with gzip.open(path.with_suffix(".gz"), "rb") as stream:
                    data = stream.read(member["byteSize"] + 1)
            if len(data) != member["byteSize"] or "sha256:" + hashlib.sha256(data).hexdigest() != digest:
                raise ValueError("Retained object differs from declared bytes")
        self.remaining -= len(data)
        if self.remaining < 0:
            raise ValueError("Retained read budget exceeded")
        return io.BytesIO(data)

    def verify(self):
        return admit_artifact(self, expected_pin=ArtifactPin(self.root["logicalId"], self.digest))

    def journal(self):
        with self.open("journal.jsonl") as stream:
            return [json.loads(line) for line in stream if line.strip()]

    def body(self, event: dict) -> bytes:
        key = event.get("blob_member")
        if key is None:
            matches = [name for name, member in self.members.items()
                       if member["sha256"] == event["sha256"] and member["byteSize"] == event["byte_size"]]
            if len(matches) != 1:
                raise ValueError("Legacy capture requires one exact digest/size member")
            key = matches[0]
        member = self.members[key]
        if member["sha256"] != event["sha256"] or member["byteSize"] != event["byte_size"]:
            raise ValueError("Journal and member capture pins differ")
        with self.open(key) as stream:
            return stream.read()


def compare_rows(expected: list[dict], actual: list[dict]) -> dict:
    """Full value and duplicate-multiplicity equality; row order is separate."""
    def encoded(rows):
        return [json.dumps(row, sort_keys=True, separators=(",", ":"), default=str) for row in rows]
    left, right = encoded(expected), encoded(actual)
    missing, added = Counter(left) - Counter(right), Counter(right) - Counter(left)
    return {"expected_rows": len(left), "actual_rows": len(right),
            "missing_row_occurrences": sum(missing.values()), "added_row_occurrences": sum(added.values()),
            "all_values_and_multiplicity_equal": not missing and not added,
            "ordered_rows_equal": left == right}


def code_pins() -> dict:
    from importlib import import_module
    from importlib.metadata import version
    from spicy_regs.generations import source_digest
    modules = [import_module(name) for name in (
        "spicy_docs.sources.gao.recommendations", "spicy_docs.schemas.gao_recommendation_tables",
        "spicy_docs.schemas.tables", "spicy_regs.etl_receipts",
        "spicy_regs.transforms.build_gao_recommendations",
        "spicy_regs.transforms.government_receipts", "spicy_regs.transforms.government_source_shapes")]
    files = [Path(__file__), Path(__file__).parents[1] / "uv.lock"]
    files += [Path(module.__file__) for module in modules]
    package_code = {}
    for name in ("spicy_regs", "spicy_docs", "rulespec_artifacts"):
        directory = Path(import_module(name).__file__).parent
        package_code[name] = {"path": str(directory),
                              "source_sha256": source_digest(directory, (".py", ".json", ".xsd"))}
    return {"files": [pin(path) for path in files], "package_code": package_code, "packages": {
        name: version(name) for name in ("spicy-regs", "spicy-docs", "rulespec-artifacts", "pyarrow", "duckdb")}}


def selection(corpus: Path) -> dict:
    """Resolve journals without reading publisher payloads or running producers."""
    catalog = json.loads((corpus / "catalog.json").read_text())
    family = catalog["families"]["gao-recommendations"]
    generation = family["current_generation"]
    root_path = corpus / "lineage/generations/gao-recommendations" / generation[7:] / "artifact.json"
    root = json.loads(root_path.read_text())
    selected_sources = [ref["artifactDigest"] for ref in root["inputs"] if ref["role"] == "source-evidence"]
    if len(selected_sources) != 1:
        raise ValueError("Selected GAO generation requires one source artifact")
    captures = []
    for digest in family["source_artifacts"]:
        source = RetainedArtifact(corpus, digest)
        events = source.journal()
        export = [e for e in events if e.get("event") == "capture" and e.get("stage") == "gao-recommendations-export"]
        fold = [e for e in events if e.get("event") == "fold" and e.get("stage") == "gao-recommendations-export"]
        if len(export) != 1 or len(fold) != 1:
            raise ValueError("Capture requires one complete export and fold observation")
        capture = export[0]
        legacy = "capture_id" not in capture and "evidence_policy" not in capture
        if (capture.get("status_code") != 200 or capture.get("response_complete") is False
                or (capture.get("response_complete") is not True and not legacy)):
            raise ValueError("Export capture did not succeed completely")
        outputs = [e for e in events if e.get("event") == "retained-file" and e.get("dataset") == "gao_recommendations"]
        captures.append({"artifact": digest, "export": export[0], "fold": fold[0],
                         "expected_outputs": outputs,
                         "metadata_pins": [pin(source.directory / key) for key in sorted(source.metadata)]})
    captures.sort(key=lambda item: (item["export"]["observed_at"], item["artifact"]))
    if not captures or captures[-1]["artifact"] != selected_sources[0]:
        raise ValueError("Selection contains captures beyond or outside the selected generation")
    if len(captures[-1]["expected_outputs"]) != 1:
        raise ValueError("Selected output requires exactly one retained producer output")
    return {"format": "source-catalog-f-gao-selection/1", "family": "gao-recommendations",
            "corpus": str(corpus.resolve()), "selected_generation": generation,
            "selected_source_artifact": selected_sources[0], "catalog_pin": pin(corpus / "catalog.json"),
            "generation_root_pin": pin(root_path), "captures": captures, "code_pins": code_pins(),
            "coverage": "Explicit retained export history only; no live reads, no baseline injection"}


def _replay(manifest_path: Path, output: Path) -> dict:
    from spicy_docs.schemas.gao_recommendation_tables import GAO_RECOMMENDATIONS, shape_gao_recommendation
    from spicy_docs.sources.gao.recommendations import parse_recommendations_export
    from spicy_regs.transforms.build_gao_recommendations import fold
    from spicy_regs.transforms.government_receipts import migrate_outputs, POLICIES
    from spicy_regs.etl_receipts import read_with_receipts

    manifest = json.loads(manifest_path.read_text())
    if manifest.get("format") != "source-catalog-f-gao-selection/1":
        raise ValueError("Unsupported F selection")
    corpus = Path(manifest["corpus"])
    frozen = manifest["code_pins"]
    if code_pins() != frozen:
        raise ValueError("Code/package selection changed")
    for item in [manifest["catalog_pin"], manifest["generation_root_pin"]]:
        assert_pin(Path(item["path"]), item)
    if selection(corpus) != manifest:
        raise ValueError("Selection differs from exact canonical journals and target")
    output.mkdir(parents=True, exist_ok=False)
    (output / "selection.json").write_bytes(manifest_path.read_bytes())
    history, steps = [], []
    selected_expected = None
    for item in manifest["captures"]:
        source = RetainedArtifact(corpus, item["artifact"])
        source.verify()
        export = parse_recommendations_export(source.body(item["export"]))
        before = len(history)
        fresh = [shape_gao_recommendation(rec, status_as_of=export.status_as_of, as_of=export.as_of)
                 for rec in export.recommendations]
        history, retirement = fold(history, fresh)
        step = {"artifact": item["artifact"], "export_sha256": item["export"]["sha256"],
                "status_as_of": export.status_as_of, "source_rows": len(fresh), "prior_rows": before,
                "recorded_prior_rows": item["fold"]["prior_rows"], "retirement": retirement,
                "transport_completeness_flag": item["export"].get("response_complete"),
                "payload_validation": "maintained whole-export parser and declared member digest/size"}
        step["recorded_prior_population_matches"] = before == item["fold"]["prior_rows"]
        for expected_output in item["expected_outputs"]:
            expected = pq.read_table(io.BytesIO(source.body(expected_output))).to_pylist()
            step["output_comparison"] = compare_rows(expected, history)
            if item["artifact"] == manifest["selected_source_artifact"]:
                selected_expected = expected
        steps.append(step)
        with (output / "progress.jsonl").open("a") as stream:
            stream.write(json.dumps(step, sort_keys=True) + "\n")
    if selected_expected is None:
        raise ValueError("Selected expected output absent")
    schema = pa.schema([(column, pa.string()) for column in GAO_RECOMMENDATIONS.columns])
    candidate = output / "gao_recommendations.parquet"
    pq.write_table(pa.Table.from_pylist(history, schema=schema), candidate, compression="zstd")
    final_comparison = compare_rows(selected_expected, history)
    migration = migrate_outputs((candidate,))
    restored = [row["raw_record"] for row in read_with_receipts(
        [candidate], [Path(migration["receipt_path"])], POLICIES["gao_recommendations"],
        generation_id=migration["generation_id"])]
    roundtrip = compare_rows(history, restored)
    for item in manifest["captures"]:
        for selected_pin in item["metadata_pins"]:
            assert_pin(Path(selected_pin["path"]), selected_pin)
    for item in [manifest["catalog_pin"], manifest["generation_root_pin"]]:
        assert_pin(Path(item["path"]), item)
    if code_pins() != frozen:
        raise ValueError("Code/package selection changed during replay")
    passed = (final_comparison["all_values_and_multiplicity_equal"]
              and roundtrip["all_values_and_multiplicity_equal"]
              and all(step["recorded_prior_population_matches"] for step in steps)
              and all(step.get("output_comparison", {}).get("all_values_and_multiplicity_equal", True) for step in steps))
    result = {"state": "qualified" if passed else "failed-source-only-recovery", "task": "T15",
              "family": "gao-recommendations", "selected_generation": manifest["selected_generation"],
              "steps": steps, "final_comparison": final_comparison, "native_receipt_roundtrip": roundtrip,
              "outputs": [pin(candidate), pin(Path(migration["receipt_path"]))], "code_pins": frozen,
              "scope": "Complete retained selected producer values and duplicate multiplicity; native candidate/receipt roundtrip",
              "limits": ["Historical native receipt IDs are not recreated", "No publisher completeness or other family proof",
                         "Baseline retirement requires reference migration and H acceptance"],
              "production_actions": []}
    (output / "qualification.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def replay(manifest_path: Path, output: Path) -> dict:
    """Preserve a terminal error for a newly created failed candidate only."""
    existed = output.exists()
    try:
        return _replay(manifest_path, output)
    except Exception as error:
        if not existed and output.is_dir() and not (output / "qualification.json").exists():
            failure = {"state": "error", "family": "gao-recommendations", "task": "T15",
                       "error_type": type(error).__name__, "selection_pin": pin(manifest_path),
                       "progress": str(output / "progress.jsonl"), "production_actions": [],
                       "scope": "Incomplete replay; no source-only qualification"}
            with (output / "qualification.json").open("x") as stream:
                stream.write(json.dumps(failure, indent=2) + "\n")
        raise


COURT_DELTA_ARTIFACT = "sha256:43757a506ac03cfc68d02d9edcf1ff101ab92a5ad5e696a9fd74ef81e4d7670a"


def court_code_pins() -> dict:
    from importlib import import_module
    result = code_pins()
    result["files"] += [pin(Path(import_module(name).__file__)) for name in (
        "spicy_docs.sources.courtlistener.search", "spicy_docs.reading.paged_json",
        "spicy_regs.sources.courtlistener", "spicy_regs.transforms.build_courtlistener",
        "spicy_regs.court_receipts", "spicy_regs.court_subjects")]
    return result


def court_selection(corpus: Path, artifact: str = COURT_DELTA_ARTIFACT) -> dict:
    """Pin selected query groups and every occurrence using metadata only."""
    from spicy_docs.sources.courtlistener.search import COURTLISTENER
    from urllib.parse import parse_qs, urlsplit
    source = RetainedArtifact(corpus, artifact)
    groups, ids = [], set()
    events = source.journal()
    for ordinal, event in enumerate(events):
        if event.get("event") == "selection" and event.get("stage") == "courtlistener":
            url = COURTLISTENER.check_url(event["url"])
            if event.get("max_records") is not None or parse_qs(urlsplit(url).query).get("type") != ["r"]:
                raise ValueError("Court delta requires uncapped RECAP search groups")
            groups.append({"selection_ordinal": ordinal, "selection": event, "captures": []})
        elif event.get("event") == "capture":
            if (not groups or event.get("stage") != "courtlistener-response"
                    or event.get("method") != "GET" or event.get("request_body") is not None
                    or event.get("status_code") != 200 or event.get("response_complete") is not True
                    or event.get("body_retained") is not True or event.get("evidence_policy") != "full"
                    or event.get("content_encoding") != "identity"
                    or event.get("resolved_url") != event.get("requested_url")):
                raise ValueError("Court capture is not a complete retained GET observation")
            COURTLISTENER.check_url(event["requested_url"])
            identity = event.get("capture_id")
            if not isinstance(identity, str) or not identity or identity in ids:
                raise ValueError("Court capture occurrence needs a distinct identity")
            ids.add(identity)
            member = source.members[event["blob_member"]]
            if member["sha256"] != event["sha256"] or member["byteSize"] != event["byte_size"]:
                raise ValueError("Court capture and body member pins differ")
            groups[-1]["captures"].append({"journal_ordinal": ordinal, "event": event})
    if not groups or any(not group["captures"] for group in groups):
        raise ValueError("Court selection has an empty or missing capture group")
    if any(group["captures"][0]["event"]["requested_url"] != group["selection"]["url"] for group in groups):
        raise ValueError("Court selection start differs from its first capture")
    if not any(event.get("event") == "build-outcome" and event.get("outcome") == "build-complete" for event in events):
        raise ValueError("Court acquisition lacks completed build observation")
    captures = [c["event"] for group in groups for c in group["captures"]]
    return {"format": "source-catalog-f-court-delta/1", "corpus": str(corpus.resolve()),
            "artifact": artifact, "groups": groups,
            "metadata_pins": [pin(source.directory / key) for key in sorted(source.metadata)],
            "journal_member": source.members["journal.jsonl"],
            "capture_occurrences": len(captures), "capture_occurrence_bytes": sum(c["byte_size"] for c in captures),
            "unique_body_bytes": sum(source.members[key]["byteSize"] for key in {c["blob_member"] for c in captures}),
            "code_pins": court_code_pins(), "scope": "Captured search delta only; no prior input or historical merge"}


def _court_replay(manifest_path: Path, output: Path) -> dict:
    import httpx
    from spicy_docs.reading.paged_json import PagedJsonBudget
    from spicy_docs.sources.courtlistener.search import CourtListenerSearchReader, is_exact_count
    from spicy_regs.transforms.build_courtlistener import _shape
    from spicy_regs.court_receipts import write_court_rows, read_court_rows, local_receipt_selection, POLICIES
    from spicy_regs.court_subjects import normalize_court_row
    from spicy_regs.etl_receipts import ReceiptContext, split_record, read_attempts

    manifest = json.loads(manifest_path.read_text())
    if manifest.get("format") != "source-catalog-f-court-delta/1":
        raise ValueError("Unsupported Court delta selection")
    corpus = Path(manifest["corpus"])
    if court_selection(corpus, manifest["artifact"]) != manifest:
        raise ValueError("Court selection/code differs from exact retained journals")
    output.mkdir(parents=True, exist_ok=False)
    (output / "selection.json").write_bytes(manifest_path.read_bytes())
    source = RetainedArtifact(corpus, manifest["artifact"], byte_budget=4_000_000)
    source.verify()
    results = []
    for group_index, group in enumerate(manifest["groups"]):
        consumed, shaped, occurrences, seen = [], [], [], set()
        def held_response(request):
            ordinal = len(consumed)
            if ordinal >= len(group["captures"]):
                raise ValueError("Court replay requested an uncaptured continuation")
            capture = group["captures"][ordinal]
            event = capture["event"]
            if (request.method != event["method"] or str(request.url) != event["requested_url"]
                    or request.content or "authorization" in request.headers):
                raise ValueError("Court replay request differs from selected occurrence")
            consumed.append(capture)
            return httpx.Response(200, stream=httpx.ByteStream(source.body(event)),
                                  headers={"content-type": event["content_type"]}, request=request)
        with CourtListenerSearchReader(budget=PagedJsonBudget(1, 12 * 1024 * 1024, 30, 0),
                                       transport=httpx.MockTransport(held_response)) as reader:
            for page_index, page in enumerate(reader.search(group["selection"]["url"], max_pages=len(group["captures"]))):
                if len(consumed) != page_index + 1:
                    raise ValueError("Court replay page/capture occurrence differs")
                if page.next_url is not None and not page.records:
                    raise ValueError("Court continuation follows an empty page")
                capture = consumed[-1]
                for position, raw in enumerate(page.records):
                    # Match the high-level CourtListenerReader identity guard;
                    # the maintained page reader owns count/cursor validation.
                    identity = raw.get("docket_id")
                    if type(identity) is not int or identity <= 0 or identity in seen:
                        raise ValueError("Court search has missing, invalid or repeated docket identity")
                    seen.add(identity)
                    observation = {"artifact": manifest["artifact"], "capture_id": capture["event"]["capture_id"],
                                   "journal_ordinal": capture["journal_ordinal"], "result_position": position,
                                   "group_index": group_index, "page_index": page_index}
                    shaped.append({**_shape(dict(raw)), "source_observation": observation})
                    occurrences.append(dict(observation))
                if page.next_url is None and is_exact_count("r", page.declared_count) and len(seen) != page.declared_count:
                    raise ValueError("Court terminal distinct count differs")
        if len(consumed) != len(group["captures"]):
            raise ValueError("Court replay left selected capture occurrences unused")
        directory = output / f"group-{group_index:03d}"
        generation = "court-delta-" + hashlib.sha256((manifest["artifact"] + str(group_index)).encode()).hexdigest()
        witnesses = [{"source_id": c["event"]["capture_id"], "source_uri": c["event"]["requested_url"],
                      "sha256": c["event"]["sha256"], "locator": str(c["journal_ordinal"]), "body_version": None}
                     for c in consumed]
        context = ReceiptContext(generation, "expected", "court-delta", witnesses, {})
        accepted, refused = [], {}
        for ordinal, row in enumerate(shaped):
            try:
                normalized = normalize_court_row("court_dockets", row)
                split_record(POLICIES["court_dockets"], normalized, context)
                accepted.append(normalized)
            except (ValueError, TypeError, KeyError, pa.ArrowException):
                refused[f"court_dockets:delta:{ordinal}"] = row
        subject = write_court_rows("court_dockets", shaped, directory, witnesses=witnesses,
                                   generation_id=generation, attempt_prefix="delta", diagnostics={"group_index": group_index})
        receipts, receipt_generation = local_receipt_selection(subject)
        restored = list(read_court_rows(subject, dataset="court_dockets"))
        comparison = compare_rows(accepted, restored)
        attempts = list(read_attempts([receipts], POLICIES["court_dockets"], generation_id=receipt_generation))
        actual_refused = {a["attempt_id"]: a["processing_fields"]["raw_source_record"]
                          for a in attempts if a["outcome"] == "refused"}
        by_attempt = {a["attempt_id"]: a for a in attempts}
        for ordinal, observation in enumerate(occurrences):
            attempt_id = f"court_dockets:delta:{ordinal}"
            attempt = by_attempt.get(attempt_id)
            observation.update({"attempt_id": attempt_id, "receipt_id": attempt["receipt_id"] if attempt else None,
                                "outcome": attempt["outcome"] if attempt else "missing"})
        outcome_ok = (actual_refused == refused and len(attempts) == len(shaped)
                      and {a["attempt_id"] for a in attempts} == {f"court_dockets:delta:{i}" for i in range(len(shaped))}
                      and all(a["outcome"] in {"accepted", "refused"} for a in attempts))
        step = {"group_index": group_index, "capture_ids": [c["event"]["capture_id"] for c in consumed],
                "source_occurrences": len(shaped), "accepted": len(accepted), "refused": len(refused),
                "complete_empty_search": not shaped, "comparison": comparison, "all_outcomes_match": outcome_ok,
                "outputs": [pin(subject), pin(receipts)]}
        (directory / "occurrences.json").write_text(json.dumps(occurrences, indent=2) + "\n")
        results.append(step)
        with (output / "progress.jsonl").open("a") as stream:
            stream.write(json.dumps(step) + "\n")
    source.verify()
    if court_selection(corpus, manifest["artifact"]) != manifest:
        raise ValueError("Court selection/code changed during replay")
    passed = all(r["all_outcomes_match"] and r["comparison"]["ordered_rows_equal"] for r in results)
    result = {"state": "qualified" if passed else "failed-delta-roundtrip", "task": "T14",
              "family": "courtlistener-captured-delta", "artifact": manifest["artifact"], "groups": results,
              "capture_occurrences": sum(len(r["capture_ids"]) for r in results),
              "scope": "Exact held search delta parsing and new native subject/receipt roundtrip",
              "limits": ["No prior input, merge or full historical docket proof", "Original receipt identities not recreated",
                         "No bulk/API replacement, T14 completion, publication or baseline retirement"], "production_actions": []}
    (output / "qualification.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def court_replay(manifest_path: Path, output: Path) -> dict:
    existed = output.exists()
    try:
        return _court_replay(manifest_path, output)
    except Exception as error:
        if not existed and output.is_dir() and not (output / "qualification.json").exists():
            (output / "qualification.json").write_text(json.dumps({"state": "error", "task": "T14",
                "family": "courtlistener-captured-delta", "error_type": type(error).__name__,
                "selection_pin": pin(manifest_path), "production_actions": []}, indent=2) + "\n")
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    select = commands.add_parser("select-gao")
    select.add_argument("--corpus", type=Path, required=True)
    select.add_argument("--manifest", type=Path, required=True)
    run = commands.add_parser("replay-gao")
    run.add_argument("--manifest", type=Path, required=True)
    run.add_argument("--output", type=Path, required=True)
    court_select = commands.add_parser("select-court-delta")
    court_select.add_argument("--corpus", type=Path, required=True)
    court_select.add_argument("--artifact", default=COURT_DELTA_ARTIFACT)
    court_select.add_argument("--manifest", type=Path, required=True)
    court_run = commands.add_parser("replay-court-delta")
    court_run.add_argument("--manifest", type=Path, required=True)
    court_run.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command in {"select-gao", "select-court-delta"}:
        value = selection(args.corpus) if args.command == "select-gao" else court_selection(args.corpus, args.artifact)
        with args.manifest.open("x") as stream:
            stream.write(json.dumps(value, indent=2) + "\n")
    else:
        value = replay(args.manifest, args.output) if args.command == "replay-gao" else court_replay(args.manifest, args.output)
        print(json.dumps({"state": value["state"], "family": value["family"]}))
        if value["state"] != "qualified":
            raise SystemExit(1)


if __name__ == "__main__":
    main()
