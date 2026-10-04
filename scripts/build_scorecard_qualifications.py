"""Write bounded public qualification metadata from explicit private input pins.

The command preserves source and model bodies in the private corpus. It records
the completed source scope, input hashes and exact table counts for the generated
publisher work queue. It does not acquire, qualify or publish a new source.
"""

import argparse
from hashlib import sha256
import json
from pathlib import Path
from urllib.parse import quote

from spicy_docs.sources.scorecards import ADAPTER_PUBLISHERS, ScorecardEdition, get_adapter

from spicy_regs.scorecards.retained import pinned_bytes, qualified_reader_class, qualified_reader_inputs


def build(plan_path, plan_sha256, corpus, directory, *, observed_at, published_ledger=None):
    plan = json.loads(pinned_bytes(plan_path, plan_sha256, max_bytes=2 * 1024**2))
    corpus = corpus.resolve()
    editions, readers, api_observations = [], {}, []
    target = directory / "qualifications"
    target.mkdir(parents=True, exist_ok=True)

    def private_path(relative):
        path = (corpus / relative).resolve()
        if Path(relative).is_absolute() or not path.is_relative_to(corpus):
            raise ValueError("Qualified input leaves the selected private corpus")
        return path

    for entry in plan["entries"]:
        edition = ScorecardEdition(**entry["edition"])
        adapter = get_adapter(entry["adapter"])
        if ADAPTER_PUBLISHERS[entry["adapter"]] != edition.publisher_id:
            raise ValueError("Qualified scope and installed reader publisher differ")
        pinned_bytes(Path(adapter.__file__), entry["reader_sha256"], max_bytes=1024**2)
        reader = qualified_reader_class(adapter, entry["reader_class"]) if entry.get("reader_class") else adapter
        if entry.get("observations_file"):
            pinned_bytes(private_path(entry["observations_file"]), entry["observations_sha256"])
        if entry.get("reader_inputs"):
            qualified_reader_inputs(entry, private_path)
        reference = json.loads(pinned_bytes(private_path(entry["reference_file"]), entry["reference_sha256"]))
        qualification = json.loads(
            pinned_bytes(private_path(entry["qualification_file"]), entry["qualification_sha256"])
        )
        if qualification.get("status") == "refused" or qualification.get("error_type"):
            raise ValueError("Refused source qualification cannot advance integration coverage")
        snapshots = reference.get("scorecard_snapshots", [])
        if (
            len(snapshots) != 1
            or snapshots[0]["scorecard_id"] != edition.scorecard_id
            or snapshots[0]["completeness_status"] != "complete"
            or snapshots[0]["parser_version"] != reader.parser_version
            or not snapshots[0]["completeness_rule"]
        ):
            raise ValueError("Reference does not describe the named complete reader scope: " + edition.scorecard_id)
        counts = {name: len(rows) for name, rows in reference.items()}
        declared = next(
            (qualification[k] for k in ("counts", "parsed_counts", "table_counts") if k in qualification), None
        )
        if declared is not None and declared != counts:
            raise ValueError("Qualification table counts differ from the native reference")
        manifest = pinned_bytes(private_path(entry["captures_root"] + "/captures.json"), entry["manifest_sha256"])
        receipt = dict(
            format_version="scorecard-integration-qualified-scope/1",
            qualified=True,
            publisher_id=edition.publisher_id,
            scorecard_id=edition.scorecard_id,
            source_url=edition.source_url,
            parser_version=reader.parser_version,
            reader_sha256=entry["reader_sha256"],
            capture_manifest_sha256=entry["manifest_sha256"],
            native_reference_sha256=entry["reference_sha256"],
            native_reference_kind="source_table_family",
            source_qualification_sha256=entry["qualification_sha256"],
            completeness_rule=snapshots[0]["completeness_rule"],
            counts=counts,
            capture_count=len(json.loads(manifest)),
            evidence_policy=snapshots[0]["evidence_policy"],
            qualification_boundary="Named source rendition; remaining editions and renditions stay in scope",
            published=False,
        )
        if entry.get("observations_sha256"):
            receipt["qualified_observations_sha256"] = entry["observations_sha256"]
        if entry.get("reader_inputs"):
            receipt["qualified_reader_input_sha256"] = sha256(
                json.dumps(entry["reader_inputs"], sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
        if entry.get("extraction_pages"):
            receipt["retained_page_sha256"] = [page["sha256"] for page in entry["extraction_pages"]]
        path = target / (quote(edition.publisher_id, safe="-_") + "--" + quote(edition.edition_id, safe="-_") + ".json")
        path.write_text(json.dumps(receipt, indent=2, ensure_ascii=False) + "\n")
        record = dict(
            publisher_id=edition.publisher_id,
            scorecard_id=edition.scorecard_id,
            state="qualified",
            receipt=str(path.relative_to(directory)),
            receipt_sha256=sha256(path.read_bytes()).hexdigest(),
        )
        editions.append(record)
        readers.setdefault(edition.publisher_id, entry["adapter"])
        if entry.get("api_endpoints"):
            api_observations.append({**record, "endpoints": entry["api_endpoints"]})
    publication_pin = None
    if published_ledger is not None:
        published_bytes = published_ledger.read_bytes()
        published = json.loads(published_bytes)
        publication_pin = sha256(published_bytes).hexdigest()
        by_scope = {record["scorecard_id"]: record for record in editions}
        observed_scopes = set()
        for record in published["editions"]:
            scope = record["scorecard_id"]
            if scope in observed_scopes or record["state"] != "published":
                raise ValueError("Publication ledger repeats a scope or names an unfinished stage")
            observed_scopes.add(scope)
            if scope in by_scope and by_scope[scope]["publisher_id"] != record["publisher_id"]:
                raise ValueError("Publication and qualification publisher identities differ")
            by_scope[scope] = record
        editions = list(by_scope.values())
        readers.update(published["readers"])
    ledger = dict(
        schema_version="1",
        observed_at=observed_at,
        input_plan_sha256=plan_sha256,
        readers=readers,
        editions=editions,
        api_observations=api_observations,
    )
    if publication_pin is not None:
        ledger["publication_observations_sha256"] = publication_pin
    (directory / "integration_qualifications.json").write_text(json.dumps(ledger, indent=2) + "\n")
    return ledger


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("plan", "corpus", "directory"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--observed-at", required=True)
    parser.add_argument("--published-ledger", type=Path)
    args = parser.parse_args()
    result = build(
        args.plan,
        args.plan_sha256,
        args.corpus,
        args.directory,
        observed_at=args.observed_at,
        published_ledger=args.published_ledger,
    )
    print(json.dumps(dict(qualified_editions=len(result["editions"]), publishers=len(result["readers"]))))


if __name__ == "__main__":
    main()
