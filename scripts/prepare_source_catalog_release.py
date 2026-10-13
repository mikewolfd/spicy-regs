"""Pin release inputs and report prerequisites; never upload or change a pointer."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess

import pyarrow.parquet as pq

from spicy_regs.etl_policy_registry import installed_policies
from spicy_regs.etl_receipts import DatasetPolicy
from spicy_regs.generations import implementation_id, spicy_docs_code
from spicy_regs.sources import publication

LOCAL_PATH = re.compile(r"(?:^|[\s\"'])(?:/(?:Users|home|private|tmp|var|Volumes)/|[A-Za-z]:\\)|file://")


def contains_local_path(value) -> bool:
    if isinstance(value, str):
        return bool(LOCAL_PATH.search(value))
    if isinstance(value, dict):
        return any(contains_local_path(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(contains_local_path(item) for item in value)
    return False


def pin_file(path: Path, expected: str, max_bytes: int) -> dict:
    """Check existing bytes against an explicit selection, without copying them."""
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"Selection is not a regular file: {path}")
    before = path.stat()
    if before.st_size > max_bytes:
        raise ValueError(f"Selection exceeds byte bound: {path}")
    with path.open("rb") as stream:
        digest = "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ValueError(f"Selection changed while hashing: {path}")
    if digest != expected:
        raise ValueError(f"Selection digest differs: {path}")
    return {"path": str(path.resolve()), "sha256": digest, "bytes": before.st_size}


def catalog_closure(directory: Path) -> dict:
    """Follow the selected manifest; reject mixed generations and missing pages."""
    root = directory.resolve()
    manifest = json.loads((root / "manifest.json").read_text())
    generation = manifest["generation"]
    prefix = "generations/" + generation.removeprefix("sha256:") + "/"

    def read(relative):
        target = root / relative
        if not relative.startswith(prefix) or target.is_symlink() or not target.resolve().is_relative_to(root):
            raise ValueError("Catalog reference leaves selected generation")
        value = json.loads(target.read_text())
        if value["generation"] != generation:
            raise ValueError("Catalog reference generation differs")
        return value

    pages = 0
    for collection in manifest["collections"]:
        detail = read(collection["detailPath"])
        if detail["collection"]["id"] != collection["id"]:
            raise ValueError("Catalog detail collection differs")
        total = 0
        for descriptor in detail["files"]["pages"]:
            page = read(descriptor["path"])
            if page["collectionId"] != collection["id"] or len(page["files"]) != descriptor["rows"]:
                raise ValueError("Catalog file page differs")
            total += len(page["files"])
            pages += 1
        if total != detail["files"]["total"]:
            raise ValueError("Catalog file page total differs")
    return {"generation": generation, "collections": len(manifest["collections"]), "pages": pages}


def parquet_visibility(path: Path, *, receipt=False, literal_values=()) -> dict:
    """Scan every nested Parquet value in bounded batches; report counts, not private values."""
    before = path.stat()
    rows = 0
    unsafe = 0
    generations = set()
    allowed = {(item["dataset"] if receipt else None, item["field"], item["sha256"]): item["occurrences"]
               for item in literal_values}
    matched = dict.fromkeys(allowed, 0)
    literal_rows = 0
    for batch in pq.ParquetFile(path).iter_batches(batch_size=256):
        for record in batch.to_pylist():
            rows += 1
            if receipt:
                if not isinstance(record["generation_id"], str) or not record["generation_id"]:
                    raise ValueError("Receipt original generation must be nonempty")
                generations.add(record["generation_id"])
            unsafe_fields = [key for key, value in record.items() if contains_local_path(value)]
            remaining = []
            for field in unsafe_fields:
                value = record[field]
                key = (record.get("dataset") if receipt else None, field,
                       "sha256:" + hashlib.sha256(value.encode()).hexdigest()) if isinstance(value, str) else None
                if key in allowed:
                    matched[key] += 1
                else:
                    remaining.append(field)
            if unsafe_fields and not remaining:
                literal_rows += 1
            if remaining:
                unsafe += 1
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ValueError("Receipt changed during visibility scan")
    if matched != allowed:
        raise ValueError("Qualified source literal occurrence count differs")
    return {"rows": rows, "local_path_rows": unsafe, "qualified_source_literal_rows": literal_rows,
            "generation_ids": sorted(generations)}


def receipt_visibility(path: Path, *, literal_values=()) -> dict:
    return parquet_visibility(path, receipt=True, literal_values=literal_values)


def prepare(selection: dict, index: dict, *, max_bytes: int = 1024**3, check_catalog=None) -> dict:
    """Check exact candidates; semantic and deployed checks remain named prerequisites."""
    if selection.get("format") != "spicy-source-catalog-release-selection" or selection.get("version") != 1:
        raise ValueError("Unsupported release selection")
    policies = installed_policies()
    report = {"format": "spicy-source-catalog-release-preflight", "version": 1,
              "observed_at": datetime.now(timezone.utc).isoformat(),
              "implementation_id": implementation_id(), "spicy_docs_code": spicy_docs_code(),
              "publication_snapshot": index, "candidates": [], "limits": [
                  "No upload, pointer change or deployment occurs.",
                  "A captured index must be refreshed immediately before coordinator publication.",
                  "File hashes and schema checks do not prove source completeness or row receipt bindings.",
                  "Visibility scans use 256-row batches; this does not bound the bytes in an individual row.",
              ]}
    seen = set()
    for candidate in selection["candidates"]:
        family = candidate["family"]
        if family in seen:
            raise ValueError("Release selection repeats a family")
        seen.add(family)
        result = {"task": candidate["task"], "family": family, "tables": [], "blockers": [],
                  "prior_entry": index["families"].get(family), "checks_required": candidate["checks_required"]}
        proof = candidate["qualification"]
        result["qualification"] = pin_file(Path(proof["path"]), proof["sha256"], max_bytes)
        literal_entries = []
        if exceptions := candidate.get("literal_source_exceptions"):
            result["literal_source_exceptions"] = pin_file(Path(exceptions["path"]), exceptions["sha256"], max_bytes)
            qualified = json.loads(Path(exceptions["path"]).read_text())
            if qualified.get("format") != "spicy-qualified-public-source-literals" or qualified.get("version") != 1:
                raise ValueError("Unsupported source literal exceptions")
            witness = qualified["source_proof"]
            pin_file(Path(witness["path"]), witness["sha256"], max_bytes)
            literal_entries = qualified["entries"]
            identities = set()
            datasets = {table["dataset"] for table in candidate["tables"]}
            for entry in literal_entries:
                identity = (entry["kind"], entry.get("dataset"), entry["field"], entry["sha256"])
                if (identity in identities or entry["kind"] not in {"subject", "receipt"}
                        or entry.get("dataset") not in datasets
                        or not isinstance(entry["field"], str) or not entry["field"]
                        or not re.fullmatch(r"sha256:[a-f0-9]{64}", entry["sha256"])
                        or type(entry["occurrences"]) is not int or entry["occurrences"] < 1):
                    raise ValueError("Invalid qualified source literal entry")
                identities.add(identity)
        keys = set()
        for table in candidate["tables"]:
            path = Path(table["path"])
            key = table["dataset"] + ".parquet"
            if key in keys or path.name != key:
                raise ValueError("Candidate table name is duplicated or differs")
            keys.add(key)
            identity = pin_file(path, table["sha256"], max_bytes)
            parquet = pq.ParquetFile(path)
            if parquet.metadata.num_rows != table["rows"]:
                raise ValueError("Candidate row count differs")
            policy = policies.get(table["dataset"])
            if policy is None:
                proposed = table["policy"]
                pin_file(Path(proposed["path"]), proposed["sha256"], max_bytes)
                policy = DatasetPolicy.from_descriptor(json.loads(Path(proposed["path"]).read_text()))
                if policy.dataset != table["dataset"]:
                    raise ValueError("Proposed policy dataset differs")
                result["blockers"].append(f"{key}: dataset policy not installed for publication")
            if not parquet.schema_arrow.equals(policy.subject_schema, check_metadata=False):
                raise ValueError("Candidate differs from installed dataset policy")
            prior = publication.table_descriptor(index, key)
            owner = publication.table_owner(index, key)
            if owner and owner[0] != family:
                result["blockers"].append(f"{key}: owned by {owner[0]}")
            if prior and prior["rows"] > table["rows"]:
                result["blockers"].append(f"{key}: candidate smaller than live selected table")
            visibility = parquet_visibility(path, literal_values=[e for e in literal_entries
                                            if e["kind"] == "subject" and e["dataset"] == table["dataset"]])
            if visibility["local_path_rows"]:
                result["blockers"].append(f"{key}: public subjects contain local filesystem paths")
            result["tables"].append({**identity, "dataset": table["dataset"], "rows": table["rows"],
                                     "prior": prior, "policy": policy.descriptor(), "visibility": visibility})
        prior_keys = set((result["prior_entry"] or {}).get("tables", {}))
        if prior_keys - keys:
            result["blockers"].append("Complete family requires retained siblings: " + ", ".join(sorted(prior_keys - keys)))
        receipt = candidate.get("receipts")
        if receipt:
            result["receipts"] = pin_file(Path(receipt["path"]), receipt["sha256"], max_bytes)
            scan = receipt_visibility(Path(receipt["path"]), literal_values=[e for e in literal_entries if e["kind"] == "receipt"])
            result["receipts"].update(scan)
            if scan["generation_ids"] != sorted(receipt.get("original_generation_ids", [receipt["generation_id"]])):
                raise ValueError("Receipt generation differs from exact selection")
            if scan["local_path_rows"]:
                result["blockers"].append("Public receipts contain local filesystem paths")
        else:
            result["blockers"].append("Registered dataset requires reviewed shared receipts")
        result["state"] = "blocked" if result["blockers"] else "inputs-pinned-awaiting-checks"
        report["candidates"].append(result)
    if catalog := selection.get("catalog"):
        result = pin_file(Path(catalog["path"]) / "manifest.json", catalog["manifest_sha256"], max_bytes)
        if check_catalog is None:
            raise ValueError("Catalog requires maintained public-field validation")
        result["public_selection"] = check_catalog(Path(catalog["path"]))
        selected_manifest = next(item for item in result["public_selection"]["files"] if item["path"] == "manifest.json")
        if selected_manifest["sha256"] != catalog["manifest_sha256"]:
            raise ValueError("Catalog manifest changed during public validation")
        result.update(catalog_closure(Path(catalog["path"])))
        if result["generation"] != result["public_selection"]["generation"]:
            raise ValueError("Catalog generation changed during preflight")
        pin_file(Path(catalog["path"]) / "manifest.json", catalog["manifest_sha256"], max_bytes)
        report["catalog"] = result
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selection", type=Path, required=True)
    parser.add_argument("--publication", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--website", type=Path)
    args = parser.parse_args()

    def public_check(directory):
        if args.website is None:
            raise ValueError("--website required for public catalog validation")
        checked = subprocess.run(["node", str(args.website.resolve() / "scripts/stage-source-catalog.mjs"),
                                  "--inspect", str(directory)], cwd=args.website, check=True, capture_output=True, text=True)
        return json.loads(checked.stdout)

    selected_raw = args.selection.read_bytes()
    index_raw = args.publication.read_bytes()
    report = prepare(json.loads(selected_raw), publication.parse_index(index_raw),
                     check_catalog=public_check)
    report["selection_sha256"] = "sha256:" + hashlib.sha256(selected_raw).hexdigest()
    report["publication_sha256"] = "sha256:" + hashlib.sha256(index_raw).hexdigest()
    pin_file(args.selection, report["selection_sha256"], 1024**3)
    pin_file(args.publication, report["publication_sha256"], publication.INDEX_LIMIT)
    with args.output.open("x") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")


if __name__ == "__main__":
    main()
