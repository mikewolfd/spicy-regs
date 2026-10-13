"""Integrate finite retained publisher captures through maintained native readers.

No acquisition occurs here. Capture declarations belong to a caller-qualified
selection; byte identity alone does not prove an acquisition time or origin.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
from rulespec_artifacts import publish_directory_no_replace
from spicy_docs.sources.sec_comments.acquisition import read_comment_file
from spicy_docs.sources.sec_comments.pages import (
    check_continuation,
    comment_index_url,
    parse_comment_listing_page,
    parse_rule_page,
)
from spicy_docs.transport.captured import CapturedBodyResponse

from spicy_regs.etl_receipts import DatasetPolicy, ReceiptContext, combine_receipts, write_dataset
from spicy_regs.sources.retained_collections import _code_pins, _digest, _file_digest, _json, _read

VERSION = "retained-native-collections-v1"
MAX_MANIFEST = 2 * 1024**2
MAX_INPUT_READS = 1024**3
MAX_CAPTURE = 16 * 1024**2
MAX_JSON_ROWS = 64 * 1024**2
MAX_OUTPUT = 512 * 1024**2
SCHEMAS = {
    "sec_rulemakings": pa.schema(
        [
            (key, pa.string())
            for key in (
                "rulemaking_id",
                "file_number",
                "title",
                "rule_url",
                "discovery_method",
                "source_uri",
                "source_sha256",
                "observed_at",
                "source_record_json",
            )
        ]
    ),
    "sec_comment_files": pa.schema(
        [
            (key, pa.string())
            for key in (
                "file_id",
                "rulemaking_id",
                "file_url",
                "format",
                "pdf_version",
                "body_sha256",
                "observed_at",
                "occurrences_json",
            )
        ]
        + [("bytes", pa.int64()), ("linearized_length", pa.int64())]
    ),
}
POLICIES = {
    table: DatasetPolicy(
        table, schema, ("rulemaking_id",) if table == "sec_rulemakings" else ("file_id",), (), policy_version=VERSION
    )
    for table, schema in SCHEMAS.items()
}


class Captures:
    """Read pinned originals in place with a cumulative decoded-byte limit."""

    def __init__(self, manifest_path: Path):
        self.base = manifest_path.parent
        self.read_bytes = 0
        self.inputs: dict[str, dict] = {}

    def read(self, entry: dict) -> CapturedBodyResponse:
        if not isinstance(entry, dict):
            raise ValueError("capture declaration required")
        size = entry.get("bytes")
        if type(size) is not int or not 0 < size <= MAX_CAPTURE:
            raise ValueError("capture size bound")
        if entry.get("status_code") != 200 or type(entry.get("status_code")) is not int:
            raise ValueError("selection requires a successful retained capture")
        if entry.get("method", "GET") != "GET" or entry.get("content_encoding", "identity") != "identity":
            raise ValueError("unsupported capture framing")
        url, final = entry["requested_url"], entry["resolved_url"]
        if not isinstance(url, str) or final != url:
            raise ValueError("capture locator differs")
        instant = datetime.fromisoformat(entry["observed_at"].replace("Z", "+00:00"))
        if instant.tzinfo is None:
            raise ValueError("capture observation requires a timezone")
        path = Path(entry["path"])
        if not path.is_absolute():
            path = self.base / path
        self.read_bytes += size
        if self.read_bytes > MAX_INPUT_READS:
            raise ValueError("selected original read budget")
        body = _read(path, size)
        if len(body) != size or _digest(body) != entry["sha256"]:
            raise ValueError("retained capture hash/size differs")
        source = {**entry, "path": str(path.resolve())}
        identity = _json(source)
        self.inputs[identity] = source
        return CapturedBodyResponse(url, final, 200, entry["content_type"], entry["observed_at"], body)

    def recheck(self) -> None:
        for entry in list(self.inputs.values()):
            self.read(entry)


def witness(capture: CapturedBodyResponse, locator: str) -> dict:
    return {
        "source_id": capture.sha256,
        "sha256": capture.sha256,
        "source_uri": capture.requested_url,
        "locator": locator,
    }


def sec_rows(unit: dict, captures: Captures):
    """Require a complete selected listing chain and every distinct stated file."""
    docket = unit["docket"]
    listing_entries = unit["listings"]
    if not isinstance(listing_entries, list) or not 1 <= len(listing_entries) <= 100:
        raise ValueError("bounded listing selection required")
    first = listing_entries[0]["requested_url"]
    native_rule, rule_capture = None, None
    if unit.get("rule") is not None:
        rule_capture = captures.read(unit["rule"])
        native_rule = parse_rule_page(rule_capture.body, url=rule_capture.requested_url)
        if native_rule.file_number is not None and native_rule.file_number.casefold() != docket.casefold():
            raise ValueError("rule file number differs")
        if native_rule.comment_listing_urls != (first,):
            raise ValueError("select the complete single listing stated by this rule")
        discovery = "publisher-stated"
    else:
        if first != comment_index_url(docket):
            raise ValueError("legacy selection must name its explicit derived listing")
        discovery = "legacy-derived-listing"
    occurrences, stating_captures = defaultdict(list), defaultdict(list)
    pages, next_url = [], first
    seen_pages = set()
    for entry in listing_entries:
        if entry["requested_url"] != next_url or next_url in seen_pages:
            raise ValueError("missing, repeated or unordered listing continuation")
        seen_pages.add(next_url)
        capture = captures.read(entry)
        page = parse_comment_listing_page(capture.body, url=capture.requested_url)
        if page.docket.casefold() != docket.casefold():
            raise ValueError("listing docket differs")
        pages.append(page)
        for role, values in (("sec-comment-section", page.sections), ("sec-comment", page.comments)):
            for index, value in enumerate(values):
                occurrences[value.url].append(
                    {
                        "kind": role,
                        "index": index,
                        "listing_url": page.url,
                        "listing_sha256": capture.sha256,
                        "native": asdict(value),
                    }
                )
                stating_captures[value.url].append(witness(capture, f"{role}:{index}"))
        next_url = page.pager.next_url
        if next_url is not None:
            check_continuation(first, next_url, "selected SEC listing")
    if next_url is not None:
        raise ValueError("selected listing is not terminal")
    files = unit["files"]
    if not isinstance(files, list) or not 1 <= len(files) <= 5000:
        raise ValueError("bounded file selection required")
    urls = [entry["requested_url"] for entry in files]
    if len(set(urls)) != len(urls) or set(urls) != set(occurrences):
        raise ValueError("selected file set differs from all listing declarations")
    if type(unit.get("expected_files")) is not int or len(files) != unit["expected_files"]:
        raise ValueError("selected file count differs")
    anchor = rule_capture or captures.read(listing_entries[0])
    rulemaking_id = _digest(
        _json(["SEC", docket, first, anchor.sha256, [entry["sha256"] for entry in listing_entries]]).encode()
    )
    rule_row = {
        "rulemaking_id": rulemaking_id,
        "file_number": native_rule.file_number if native_rule else docket,
        "title": native_rule.title if native_rule else pages[0].title,
        "rule_url": native_rule.url if native_rule else None,
        "discovery_method": discovery,
        "source_uri": anchor.requested_url,
        "source_sha256": anchor.sha256,
        "observed_at": anchor.observed_at,
        "source_record_json": _json(
            {"rule": asdict(native_rule) if native_rule else None, "listings": [asdict(page) for page in pages]}
        ),
    }
    rule_witnesses = [witness(anchor, "rule" if native_rule else "legacy-listing")]
    # All listing captures participate in the rule observation, including a
    # terminal empty page. File witnesses also retain every repeated occurrence.
    rule_witnesses += [
        {
            "source_id": entry["sha256"],
            "sha256": entry["sha256"],
            "source_uri": entry["requested_url"],
            "locator": f"listing:{index}",
        }
        for index, entry in enumerate(listing_entries)
    ]
    yield "sec_rulemakings", rule_row, rule_witnesses
    for entry in files:
        capture = captures.read(entry)
        fmt, pdf_version, length = read_comment_file(
            capture.body,
            url=capture.requested_url,
            media_type=capture.content_type,
            final_url=capture.resolved_url,
            max_bytes=MAX_CAPTURE,
        )
        file_id = _digest(_json([rulemaking_id, capture.requested_url, capture.sha256]).encode())
        yield (
            "sec_comment_files",
            {
                "file_id": file_id,
                "rulemaking_id": rulemaking_id,
                "file_url": capture.requested_url,
                "format": fmt,
                "pdf_version": pdf_version,
                "body_sha256": capture.sha256,
                "observed_at": capture.observed_at,
                "occurrences_json": _json(occurrences[capture.requested_url]),
                "bytes": capture.byte_size,
                "linearized_length": length,
            },
            [*rule_witnesses[:1], *stating_captures[capture.requested_url], witness(capture, "file")],
        )


def build(manifest_path: Path, output_dir: Path) -> dict:
    raw = _read(manifest_path, MAX_MANIFEST)
    selection = json.loads(raw)
    if selection.get("format") != "spicy-retained-native-inputs" or selection.get("version") != 1:
        raise ValueError("unsupported native selection")
    units = selection.get("sources")
    if not isinstance(units, list) or not 1 <= len(units) <= 32:
        raise ValueError("bounded native source units required")
    if output_dir.exists():
        raise FileExistsError("candidate already exists")
    pins = {**_code_pins(), "native_adapter": _file_digest(Path(__file__))}
    generation = _digest(_json([VERSION, _digest(raw), pins]).encode())
    captures = Captures(manifest_path)
    data = {table: [] for table in SCHEMAS}
    keys, json_bytes = set(), 0
    for unit in units:
        if unit.get("kind") != "sec-comments":
            raise ValueError("unsupported native source kind")
        for table, row, witnesses in sec_rows(unit, captures):
            key = row[POLICIES[table].identity_fields[0]]
            if (table, key) in keys:
                raise ValueError("duplicate native observation")
            keys.add((table, key))
            context = ReceiptContext(generation, key, VERSION, witnesses)
            json_bytes += len(_json([row, asdict(context)]).encode())
            if json_bytes > MAX_JSON_ROWS:
                raise ValueError("native row memory/output budget")
            data[table].append((row, context))
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=output_dir.parent, prefix=".native-collections-") as scratch:
        stage = Path(scratch) / "candidate"
        stage.mkdir()
        outputs, receipts = {}, []
        for table, policy in POLICIES.items():
            subject, receipt = write_dataset(iter(data[table]), stage / table, policy, validation_backend="reference")
            outputs[table] = {
                "rows": len(data[table]),
                "path": str(subject.relative_to(stage)),
                "sha256": _file_digest(subject),
            }
            receipts.append(receipt)
        combined = combine_receipts(receipts, stage / "etl_receipts.parquet")
        captures.recheck()
        if _read(manifest_path, MAX_MANIFEST) != raw or pins != {
            **_code_pins(),
            "native_adapter": _file_digest(Path(__file__)),
        }:
            raise ValueError("selection or code changed during build")
        result = {
            "format": "spicy-retained-native-build",
            "version": 1,
            "generation": generation,
            "state": "local-selected-candidate-built",
            "manifest_sha256": _digest(raw),
            "code_pins": pins,
            "inputs": list(captures.inputs.values()),
            "outputs": outputs,
            "receipts_sha256": _file_digest(combined),
            "source_read_bytes": captures.read_bytes,
            "baseline_inputs": [],
            "limits": [
                "Complete selected capture sets only; publisher-wide coverage unknown.",
                "No acquisition, current publisher assertion or inferred Regulations.gov identity.",
                "Local candidate does not establish public serving or publication.",
            ],
        }
        (stage / "selection.json").write_bytes(raw)
        (stage / "build.json").write_text(json.dumps(result, indent=2) + "\n")
        if sum(path.stat().st_size for path in stage.rglob("*") if path.is_file()) > MAX_OUTPUT:
            raise ValueError("native candidate output budget")
        publish_directory_no_replace(stage, output_dir)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = build(args.manifest, args.output)
    print(_json({"generation": result["generation"], "outputs": result["outputs"]}))


if __name__ == "__main__":
    main()
