"""Build bounded FEC relationship, contribution and spending profiles from originals.

This is a downstream caller of maintained source readers and financial mappers.
No acquisition, source guessing, whole-corpus conversion or publication occurs.
"""

import argparse
import json
from pathlib import Path

from spicy_docs.sources.fec.filings import filing_encoding, filing_records_from_stream
from spicy_docs.sources.fec.metadata import parse_api
from spicy_docs.sources.fec.retained import page_request
from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.parquet_rows import write_rows
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_fec_financial_profiles import json_bytes
from spicy_regs.transforms.fec_committee_profiles import TABLE as COMMITTEES, SCHEMA as COMMITTEE_SCHEMA, committee_profiles
from spicy_regs.transforms.fec_contribution_profiles import SCHEMAS as CONTRIBUTION_SCHEMAS, contribution_profiles
from spicy_regs.transforms.fec_outside_profiles import SCHEMAS as OUTSIDE_SCHEMAS, outside_profiles
from spicy_regs.transforms.fec_query import CollectionSelection

FAMILY = "fec-research-profiles"
SCHEMAS = {COMMITTEES: COMMITTEE_SCHEMA, **CONTRIBUTION_SCHEMAS, **OUTSIDE_SCHEMAS}
MAX_ORIGINAL_BYTES = 8 * 1024**2


def _read(evidence, capture, stage):
    digest = capture.get("response_sha256", capture.get("sha256"))
    size = capture.get("byte_size", capture.get("bytes"))
    if type(size) is not int or not 0 < size <= MAX_ORIGINAL_BYTES:
        raise ValueError("FEC profile input exceeds its original byte bound")
    with evidence.store.open(digest) as stream:
        body = stream.read(size + 1)
    # CentralRawArchiveStore.open verifies raw digest and size at exhaustion.
    import hashlib

    if len(body) != size or "sha256:" + hashlib.sha256(body).hexdigest() != digest:
        raise ValueError("FEC profile original differs from its selected digest/size")
    evidence.retain_bytes(body, stage=stage, raw_source=True, source_url=capture["request_url"],
                          original_location=capture["request_url"], observed_at=capture["observed_at"])
    return body


def api_records(evidence, query):
    """Check exact retained pages of one declared small publisher aggregate query."""
    records, count = [], None
    if not 1 <= len(query["pages"]) <= 6:
        raise ValueError("FEC profile query exceeds its bounded page population")
    for index, capture in enumerate(query["pages"], 1):
        pairs, page = page_request(capture["request_url"], endpoint="https://api.open.fec.gov" + query["path"])
        if page != index or sorted((k, v) for k, v in pairs if k != "page") != sorted(
                (k, str(v)) for k, v in query["params"].items()):
            raise ValueError("FEC profile page differs from its declared request scope")
        native = parse_api(_read(evidence, capture, query["name"]))
        pagination = native.get("pagination", {})
        if (pagination.get("page") != index or pagination.get("is_count_exact") is not True
                or type(pagination.get("count")) is not int or pagination["count"] < 0
                or pagination.get("per_page") != query["params"]["per_page"]
                or max(1, pagination.get("pages", 0)) != len(query["pages"])):
            raise ValueError("FEC profile query has incomplete or uncertain pagination")
        if count is not None and count != pagination["count"]:
            raise ValueError("FEC profile count changed during its observed traversal")
        count = pagination["count"]
        rows = native.get("results")
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise ValueError("FEC profile API lacks a successful results list")
        records.extend(rows)
    if len(records) != count:
        raise ValueError("FEC profile query does not reconcile to the declared native count")
    return records


def filing_rows(evidence, capture):
    """Reuse native FEC decoding and the positional profile's source ID spelling.

    A historical original with unknown acquisition time remains unknown. It is
    a raw-reader input, not an invented timestamp for a sealed source release.
    """
    from io import BytesIO

    body = _read(evidence, capture, "selected-contribution-filing")
    encoding = filing_encoding(BytesIO(body))
    native = list(filing_records_from_stream(BytesIO(body), sha256=capture["response_sha256"], encoding=encoding))
    if not native or native[0].get("format_version") != "8.5":
        raise ValueError("Selected contribution cohort requires its reviewed 8.5 filing")
    collection = "filing-" + capture["filing_id"]
    rows = []
    for ordinal, record in enumerate(native):
        source_id = f"{capture['response_sha256']}/original/{ordinal:020d}"
        locator = {**record.get("source", {}), "collection_id": collection, "source_record_id": source_id,
                   "member": None, "ordinal": ordinal}
        rows.append(dict(collection_id=collection, source_record_id=source_id,
            source_sha256=capture["response_sha256"], source_locator_json=json_bytes(locator).decode(),
            metadata_json=json_bytes(record).decode()))
    return rows


def build(selection, output_dir):
    output_dir = Path(output_dir)
    if output_dir.exists():
        raise ValueError("Use a fresh FEC profile output directory")
    evidence = CaptureEvidence(output_dir, FAMILY)
    try:
        selection_pin = evidence.retain_bytes(json_bytes(selection), stage="fec-profile-selection")["sha256"]
        tables = {name: [] for name in SCHEMAS}
        for capture in selection["committees"]:
            _pairs, page = page_request(capture["request_url"],
                endpoint="https://api.open.fec.gov/v1/committees/", order_by="committee_id")
            native = parse_api(_read(evidence, capture, "committee-disclosure-page"))
            rows = native.get("results")
            if (not isinstance(rows, list) or len(rows) != capture["expected_results"]
                    or native.get("pagination", {}).get("page") != page):
                raise ValueError("Selected committee page does not match its retained population")
            tables[COMMITTEES].extend(committee_profiles(rows, capture))
        outside = {query["name"]: api_records(evidence, query) for query in selection["outside"]}
        qualified = outside_profiles(outside["by-spender"], outside["totals"],
            candidate_id=selection["outside_candidate"], cycle=selection["outside_cycle"], evidence=selection["outside"])
        tables.update(qualified)
        contribution = selection["contribution"]
        context = {query["name"]: api_records(evidence, query) for query in contribution["queries"]}
        if len(context["reports"]) != 1 or len(context["committee-history"]) != 1:
            raise ValueError("Select exactly one effective report and committee history observation")
        capture = contribution["original"]
        digest = capture["response_sha256"]
        mapped_selection = CollectionSelection("filing-" + capture["filing_id"], digest, digest,
            "official-fec", contribution["cycle"], "snapshot", selection_pin)
        qualified = contribution_profiles(filing_rows(evidence, capture), mapped_selection,
            report=context["reports"][0], filing_id=capture["filing_id"], source_url=capture["request_url"],
            captured_at=capture["observed_at"], candidate_id=contribution["candidate_id"],
            committee_history=context["committee-history"][0], candidates=context["candidate-search"],
            evidence=contribution["queries"])
        tables.update(qualified)
        paths = []
        for table, schema in SCHEMAS.items():
            path = output_dir / (table + ".parquet")
            write_rows(tables[table], path, schema=schema)
            paths.append(path)
        artifact = build_generation(output_dir / "generation", family=FAMILY, files=paths,
            expected_keys=[p.name for p in paths], schemas={n: described_schema(s) for n, s in SCHEMAS.items()},
            inputs=evidence.inputs(), owned_output_root=output_dir)
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selection", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    artifact, evidence = build(json.loads(args.selection.read_text()), args.output_dir)
    print(json.dumps({"artifactDigest": artifact.pin.artifact_digest, "evidence": str(evidence)}))


if __name__ == "__main__":
    main()
