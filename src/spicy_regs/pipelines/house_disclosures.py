"""Capture selected House PTRs and build source-linked filing and transaction tables.

Use an explicit hash-pinned index snapshot and selected PDF captures. Builds are
offline, renew every source hash and keep unsuccessful readings as report/page
facts. The existing generation/evidence path owns admission and publication.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re

import pyarrow as pa

from spicy_regs.generations import build_generation
from spicy_regs.native_types import described_schema
from spicy_regs.parquet_rows import write_rows
from spicy_regs.pipelines.raw_releases import inherit_read_index, open_raw
from spicy_regs.source_evidence import CaptureEvidence

FAMILY = "house-disclosures"
INDEX = "house_disclosure_filings"
REPORTS = "house_disclosure_reports"
PAGES = "house_disclosure_pages"
TRANSACTIONS = "house_disclosure_transactions"
LINKS = "house_disclosure_amendment_links"
VERSION = "house-disclosures-v1"


def _schema(strings, integers=(), dates=()):
    return pa.schema([(name, pa.string()) for name in strings.split()]
                     + [(name, pa.int64()) for name in integers]
                     + [(name, pa.date32()) for name in dates])


SCHEMAS = {
    INDEX: _schema("filing_id doc_id filing_type prefix first last suffix state_district year_raw "
                   "filing_date_raw filing_date_status report_url report_url_basis index_uri index_sha256 "
                   "observed_at raw_cells_json", ("index_year", "source_line"), ("filing_date",)),
    REPORTS: _schema("report_id filing_id doc_id source_uri source_sha256 observed_at extraction_status "
                     "refusal_reason parser_version backend backend_version model filer_name_raw "
                     "state_district_raw signed_date_raw", ("index_year", "source_bytes", "page_count", "transaction_count")),
    PAGES: _schema("page_id report_id doc_id source_uri source_sha256 page_status native_text page_locator",
                   ("page_number",)),
    TRANSACTIONS: _schema("transaction_id report_id filing_id doc_id owner_raw asset_raw action_raw "
                          "native_transaction_id transaction_date_raw transaction_date_status notification_date_raw "
                          "notification_date_status amount_raw amount_status currency filing_status_raw details_raw "
                          "capital_gains_read_status source_uri source_sha256 page_locator fragments_json",
                          ("row_number", "page_number", "page_end", "amount_lower_usd", "amount_upper_usd"),
                          ("transaction_date", "notification_date")),
    LINKS: _schema("link_id original_report_id amended_report_id original_doc_id amended_doc_id "
                    "original_source_sha256 amended_source_sha256 original_page_locator amended_page_locator "
                    "link_status source_statement basis uncertainty review_sha256",
                    ("original_page_number", "amended_page_number")),
}


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _id(*values):
    return "sha256:" + hashlib.sha256(_json(values).encode()).hexdigest()


def _date(raw):
    if not raw.strip():
        return None, "blank"
    try:
        return datetime.strptime(raw.strip(), "%m/%d/%Y").date(), "parsed"
    except ValueError:
        return None, "unparsed"


def amount_range(raw):
    """Interpret only explicit dollar bounds; retain the reported bracket unchanged."""
    value = " ".join(raw.split())
    match = re.fullmatch(r"\$([0-9][0-9,]*)\s*-\s*\$([0-9][0-9,]*)", value)
    if match:
        lower, upper = (int(v.replace(",", "")) for v in match.groups())
        if lower <= upper:
            return lower, upper, "reported_range", "USD"
    match = re.fullmatch(r"Over \$([0-9][0-9,]*)", value, re.IGNORECASE)
    if match:
        # Lower bound is exclusive; never manufacture an inclusive amount.
        return int(match[1].replace(",", "")), None, "reported_exclusive_lower_bound", "USD"
    return None, None, "blank" if not value else "unparsed", None


def _original(store, source):
    from spicy_docs.sources.house_disclosures import MAX_BYTES

    if type(source.get("bytes")) is not int or not 0 < source["bytes"] <= MAX_BYTES:
        raise ValueError("House source selection lacks a bounded original byte size")
    with store.open(source["sha256"]) as stream:
        body = stream.read(source["bytes"] + 1)
    if len(body) != source["bytes"] or "sha256:" + hashlib.sha256(body).hexdigest() != source["sha256"]:
        raise ValueError("House original differs from its selected size or SHA-256")
    return body


def _index(store, selection):
    from spicy_docs.sources.house_disclosures import index_url, parse_index_zip

    if selection.get("format") != "house-disclosures-captures" or selection.get("version") != 1:
        raise ValueError("Unsupported House capture selection")
    index = selection["index"]
    if index["url"] != index_url(index["year"]):
        raise ValueError("House index URL differs from its selected year")
    body = _original(store, index)
    return index, body, parse_index_zip(body, year=index["year"])


def _indexes(store, selection):
    """Load single-year (``index``) or multi-year (``indexes``) House index ZIPs deterministically."""
    from spicy_docs.sources.house_disclosures import index_url, parse_index_zips

    if selection.get("format") != "house-disclosures-captures" or selection.get("version") != 1:
        raise ValueError("Unsupported House capture selection")
    if "indexes" in selection:
        if "index" in selection:
            raise ValueError("House capture selection cannot specify both index and indexes")
        entries = selection["indexes"]
        if not isinstance(entries, list) or not entries:
            raise ValueError("House multi-year capture selection requires non-empty indexes")
    elif "index" in selection:
        entries = [selection["index"]]
    else:
        raise ValueError("House capture selection requires index or indexes")
    seen_years = set()
    loaded = []
    for index in entries:
        year = index["year"]
        if year in seen_years:
            raise ValueError(f"House index selection repeats year {year}")
        if index["url"] != index_url(year):
            raise ValueError("House index URL differs from its selected year")
        seen_years.add(year)
        loaded.append((index, _original(store, index)))
    rows = parse_index_zips([(index["year"], body) for index, body in loaded])
    return loaded, rows


def acquire_index(year, output_dir, *, archive_root=None, transport=None):
    """Capture one explicit annual index; callers check retained versions before refresh."""
    from spicy_docs.sources.house_disclosures import HouseDisclosureAcquirer, MAX_BYTES

    evidence = CaptureEvidence(Path(output_dir), FAMILY, archive_root=archive_root)
    try:
        retained = evidence.transport(transport, stage="house-index", max_bytes=MAX_BYTES)
        with HouseDisclosureAcquirer(transport=retained) as acquirer:
            _, capture = acquirer.acquire_index(year=year)
        selection = {"format": "house-disclosures-captures", "version": 1,
                     "scope": f"Observed complete House {year} index response; report originals are selected separately.",
                     "index": {"year": year, "url": capture.requested_url, "sha256": capture.sha256,
                               "bytes": capture.byte_size, "observed_at": capture.observed_at}, "reports": []}
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return selection


def acquire_indexes(
    years=None,
    output_dir=None,
    *,
    start_year=None,
    end_year=None,
    max_requests=None,
    archive_root=None,
    transport=None,
):
    """Capture multiple annual House disclosure index ZIPs under one bounded request budget."""
    from spicy_docs.sources.house_disclosures import (
        DEFAULT_MAX_INDEX_YEAR,
        DEFAULT_MAX_REQUESTS,
        MIN_INDEX_YEAR,
        HouseDisclosureAcquirer,
        MAX_BYTES,
        index_years,
    )

    if output_dir is None:
        raise ValueError("output_dir is required")
    selected_years = index_years(
        years,
        start_year=MIN_INDEX_YEAR if start_year is None else start_year,
        end_year=DEFAULT_MAX_INDEX_YEAR if end_year is None else end_year,
    )
    budget = DEFAULT_MAX_REQUESTS if max_requests is None else max_requests
    evidence = CaptureEvidence(Path(output_dir), FAMILY, archive_root=archive_root)
    try:
        retained = evidence.transport(transport, stage="house-index", max_bytes=MAX_BYTES)
        with HouseDisclosureAcquirer(max_requests=budget, transport=retained) as acquirer:
            captures = acquirer.acquire_indexes(selected_years)
        indexes = [
            {
                "year": item.year,
                "url": item.capture.requested_url,
                "sha256": item.capture.sha256,
                "bytes": item.capture.byte_size,
                "observed_at": item.capture.observed_at,
            }
            for item in captures
        ]
        year_span = (
            f"{selected_years[0]}"
            if len(selected_years) == 1
            else f"{selected_years[0]}-{selected_years[-1]}"
        )
        selection = {
            "format": "house-disclosures-captures",
            "version": 1,
            "scope": (
                f"Observed complete House {year_span} annual index responses ({len(selected_years)} years); "
                "report originals are selected separately."
            ),
            "indexes": indexes,
            "reports": [],
        }
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return selection


def acquire(selection: dict, doc_ids, output_dir: Path, *, max_requests: int | None = None, archive_root=None, transport=None):
    """Acquire only missing explicitly selected PTRs; exact existing captures replay locally.

    A caller checks the source catalog before this operation. Existing entries
    must have the exact expected URL, DocID, year and verified bytes to be reused.
    Source refusal aborts the operation and leaves its ordinary failed evidence.
    """
    from spicy_docs.sources.house_disclosures import (
        DEFAULT_MAX_REQUESTS,
        HouseDisclosureAcquirer,
        MAX_BYTES,
        is_native_ptr_candidate,
        report_url,
    )

    store = open_raw(archive_root)
    loaded, rows = _indexes(store, selection)
    years = {index["year"] for index, _ in loaded}
    groups = {}
    by_doc = {}
    for row in rows:
        groups.setdefault((row.index_year, row.doc_id), []).append(row)
        by_doc.setdefault(row.doc_id, []).append(row)
    filings = {key: group[0] for key, group in groups.items() if len(group) == 1}

    selected_pairs = []
    for item in doc_ids:
        if isinstance(item, tuple) and len(item) == 2:
            pair = (item[0], item[1])
        elif isinstance(item, str):
            matches = [row for row in by_doc.get(item, ()) if len(groups.get((row.index_year, row.doc_id), ())) == 1]
            if len(matches) != 1:
                raise ValueError("Select index-stated P filings; P does not cover the other categories")
            pair = (matches[0].index_year, matches[0].doc_id)
        else:
            raise ValueError("House PTR selection must be doc_id or (year, doc_id)")
        if pair not in filings or not is_native_ptr_candidate(filings[pair]):
            raise ValueError("Select index-stated P filings; P does not cover the other categories")
        selected_pairs.append(pair)
    selected_pairs = sorted(set(selected_pairs))
    if not selected_pairs:
        raise ValueError("Select index-stated P filings; P does not cover the other categories")

    held = {}
    for source in selection.get("reports", []):
        key = (source["year"], source["doc_id"])
        if key in held or key[0] not in years or source["url"] != report_url(key[0], key[1], ptr=True):
            raise ValueError("Ambiguous or mismatched House report selection")
        held[key] = source
    budget = DEFAULT_MAX_REQUESTS if max_requests is None else max_requests
    evidence = CaptureEvidence(Path(output_dir), FAMILY, archive_root=archive_root)
    try:
        retained = evidence.transport(transport, stage="house-ptr-pdf", max_bytes=MAX_BYTES)
        with HouseDisclosureAcquirer(max_requests=budget, transport=retained) as acquirer:
            for year, doc in selected_pairs:
                if (year, doc) in held:
                    _original(store, held[(year, doc)])
                    evidence.event("house-ptr-reused", year=year, doc_id=doc, sha256=held[(year, doc)]["sha256"])
                    continue
                _, capture = acquirer.acquire_ptr(year=year, doc_id=doc, reset_budget=False)
                held[(year, doc)] = {
                    "year": year,
                    "doc_id": doc,
                    "url": capture.requested_url,
                    "sha256": capture.sha256,
                    "bytes": capture.byte_size,
                    "observed_at": capture.observed_at,
                }
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return {**selection, "reports": [held[key] for key in sorted(held, key=lambda item: (item[1], item[0]))]}


def candidate(selection, output_dir, evidence, *, store):
    from spicy_docs.reading.house_ptr import read_ptr_pdf
    from spicy_docs.sources.house_disclosures import PARSER_VERSION, report_url

    loaded, filings = _indexes(store, selection)
    index_by_year = {index["year"]: index for index, _ in loaded}
    for index, body in loaded:
        evidence.retain_bytes(body, stage="house-index", raw_source=True, original_location=index["url"],
                              observed_at=index.get("observed_at"))
    tables = {name: [] for name in SCHEMAS}
    indexed = {}
    for filing in filings:
        index = index_by_year[filing.index_year]
        filing_id = _id(index["sha256"], filing.source_line)
        filing_date, date_status = _date(filing.filing_date_raw)
        row = {**asdict(filing), "filing_id": filing_id, "filing_date": filing_date,
               "filing_date_status": date_status, "report_url": filing.url,
               "report_url_basis": "publisher_route_constructed_from_category_year_docid;availability_unverified",
               "index_uri": index["url"], "index_sha256": index["sha256"], "observed_at": index.get("observed_at"),
               "raw_cells_json": _json(filing.raw_cells)}
        row.pop("raw_cells")
        tables[INDEX].append(row)
        indexed.setdefault((filing.index_year, filing.doc_id), []).append(row)
    indexed = {key: group[0] for key, group in indexed.items() if len(group) == 1}
    reports, reports_by_doc, seen = {}, {}, set()
    for source in sorted(selection.get("reports", []), key=lambda item: (item["year"], item["doc_id"])):
        key = (source["year"], source["doc_id"])
        doc = source["doc_id"]
        if key in seen or key not in indexed or indexed[key]["filing_type"] != "P":
            raise ValueError("House report selection is duplicate, absent from the index or not P")
        seen.add(key)
        if source["year"] not in index_by_year or source["url"] != report_url(source["year"], doc, ptr=True):
            raise ValueError("House report locator differs from its index identity")
        body = _original(store, source)
        evidence.retain_bytes(body, stage="house-ptr-pdf", raw_source=True,
                              original_location=source["url"], observed_at=source.get("observed_at"))
        reading = read_ptr_pdf(body, doc_id=doc)
        if reading["status"] == "native_layout":
            # The qualified originals spell the native name as these literal
            # index components in this order. Only whitespace is folded; no
            # aliases, omitted honorifics, initials or fuzzy names are inferred.
            declared_name = " ".join(" ".join(indexed[key][field].split())
                                     for field in ("prefix", "first", "last", "suffix")
                                     if indexed[key][field].strip())
            if not declared_name or " ".join((reading.get("filer_name_raw") or "").split()) != declared_name:
                raise ValueError("House native filer name differs from its selected index name")
            if reading.get("state_district_raw") != indexed[key]["state_district"]:
                raise ValueError("House report district differs from its selected filing")
        report_id = _id(indexed[key]["filing_id"], source["sha256"])
        report = {"report_id": report_id, "filing_id": indexed[key]["filing_id"], "doc_id": doc,
                  "index_year": source["year"], "source_uri": source["url"], "source_sha256": source["sha256"],
                  "source_bytes": source["bytes"], "observed_at": source.get("observed_at"),
                  "extraction_status": reading["status"], "refusal_reason": reading["reason"],
                  "page_count": reading["page_count"], "transaction_count": len(reading["transactions"]),
                  **{name: reading.get(name) for name in ("parser_version", "backend", "backend_version", "model",
                                                         "filer_name_raw", "state_district_raw", "signed_date_raw")}}
        reports[key] = report
        reports_by_doc.setdefault(doc, []).append(report)
        tables[REPORTS].append(report)
        for page in reading["pages"]:
            number = page["page_number"]
            tables[PAGES].append({"page_id": _id(report_id, number), "report_id": report_id, "doc_id": doc,
                                  "source_uri": source["url"], "source_sha256": source["sha256"],
                                  "page_number": number, "page_status": page["status"], "native_text": page["native_text"],
                                  "page_locator": source["url"] + f"#page={number}"})
        for transaction in reading["transactions"]:
            date, date_status = _date(transaction["transaction_date_raw"])
            notified, notified_status = _date(transaction["notification_date_raw"])
            lower, upper, amount_status, currency = amount_range(transaction["amount_raw"])
            fragments = transaction["fragments"]
            row = {**{name: value for name, value in transaction.items() if name != "fragments"},
                   "transaction_id": _id(report_id, transaction["row_number"]), "report_id": report_id,
                   "filing_id": indexed[key]["filing_id"], "doc_id": doc,
                   "transaction_date": date, "transaction_date_status": date_status,
                   "notification_date": notified, "notification_date_status": notified_status,
                   "amount_lower_usd": lower, "amount_upper_usd": upper, "amount_status": amount_status,
                   "currency": currency, "source_uri": source["url"], "source_sha256": source["sha256"],
                   "page_locator": source["url"] + f"#page={transaction['page_number']}",
                   "fragments_json": _json(fragments)}
            tables[TRANSACTIONS].append(row)
        evidence.event("house-report-reading", doc_id=doc, year=source["year"], source_sha256=source["sha256"],
                       status=reading["status"], reason=reading["reason"], pages=reading["page_count"],
                       transactions=len(reading["transactions"]), parser_version=reading["parser_version"],
                       backend_version=reading["backend_version"])
    links = selection.get("reviewed_amendment_links", [])
    if links:
        review = selection["review"]
        path = Path(review["path"])
        if "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest() != review["sha256"]:
            raise ValueError("House amendment review differs from its selected SHA-256")
        evidence.retain_file(path, stage="house-amendment-visual-review")
        for asset in review.get("assets", []):
            image = Path(asset["path"])
            if "sha256:" + hashlib.sha256(image.read_bytes()).hexdigest() != asset["sha256"]:
                raise ValueError("House visual review image differs from its selected SHA-256")
            evidence.retain_file(image, stage="house-raw-visual-image")
        link_ids = set()
        for link in links:
            def _resolve_report(prefix):
                doc_id = link[prefix + "_doc_id"]
                if (year := link.get(prefix + "_year")) is not None:
                    return reports[(year, doc_id)]
                matches = reports_by_doc.get(doc_id, [])
                if len(matches) != 1:
                    raise ValueError(f"House amendment link {prefix} doc_id {doc_id} is missing or ambiguous")
                return matches[0]

            original, amended = _resolve_report("original"), _resolve_report("amended")
            for key, report in (("original", original), ("amended", amended)):
                if link[key + "_source_sha256"] != report["source_sha256"]:
                    raise ValueError("House amendment link names a different original version")
                number = link[key + "_page_number"]
                if type(number) is not int or not 1 <= number <= report["page_count"]:
                    raise ValueError("House amendment review names an unavailable page")
            first, last = (
                indexed[(original["index_year"], original["doc_id"])],
                indexed[(amended["index_year"], amended["doc_id"])],
            )
            if (first["first"], first["last"], first["state_district"]) != (last["first"], last["last"], last["state_district"]):
                raise ValueError("House amendment review names different filers")
            if (original["doc_id"] == amended["doc_id"] or link["link_status"] != "reviewed_date_reference"
                    or not link["source_statement"].strip() or not link["uncertainty"].strip()
                    or _date(link["prior_filing_date_raw"])[0] != first["filing_date"]
                    or first["filing_date"] is None):
                raise ValueError("House amendment requires an explicit reviewed prior-filing-date reference")
            matching = [row for row in tables[INDEX] if row["filing_type"] == "P"
                        and (row["first"], row["last"], row["state_district"], row["filing_date"])
                        == (first["first"], first["last"], first["state_district"], first["filing_date"])]
            if len(matching) != 1:
                raise ValueError("House prior-filing-date reference is ambiguous in the selected index")
            row = {key: value for key, value in link.items()
                   if key not in {"prior_filing_date_raw", "original_year", "amended_year"}}
            row.update(link_id=_id(original["report_id"], amended["report_id"], review["sha256"]),
                       original_report_id=original["report_id"], amended_report_id=amended["report_id"],
                       review_sha256=review["sha256"],
                       original_page_locator=original["source_uri"] + f"#page={link['original_page_number']}",
                       amended_page_locator=amended["source_uri"] + f"#page={link['amended_page_number']}")
            if row["link_id"] in link_ids:
                raise ValueError("Duplicate House amendment link")
            link_ids.add(row["link_id"])
            tables[LINKS].append(row)
    output_dir.mkdir(parents=True, exist_ok=False)
    for name, schema in SCHEMAS.items():
        write_rows(tables[name], output_dir / (name + ".parquet"), schema)
    summary = {"scope": selection["scope"], "index_parser_version": PARSER_VERSION,
               "index_categories": sorted({row.filing_type for row in filings}),
               "rows": {name: len(rows) for name, rows in tables.items()},
               "reports": [{key: report[key] for key in ("doc_id", "extraction_status", "refusal_reason", "transaction_count")}
                           for report in tables[REPORTS]]}
    evidence.event("house-declared-population", **summary)
    return summary


def build(selection, output_dir, *, archive_root=None, read_index=None):
    output_dir = Path(output_dir)
    if (output_dir / "candidate").exists() or (output_dir / "generation").exists():
        raise ValueError("Use a fresh output directory for each House generation")
    evidence = CaptureEvidence(output_dir, FAMILY, archive_root=archive_root)
    try:
        snapshot = inherit_read_index(evidence, read_index)
        # The selection is derived control, never a publisher original.
        control = output_dir / "selection.json"
        control.write_text(_json(selection) + "\n")
        evidence.retain_control(control, stage="house-selection")
        summary = candidate(selection, output_dir / "candidate", evidence, store=open_raw(archive_root))
        artifact = build_generation(
            output_dir / "generation", family=FAMILY,
            files=[output_dir / "candidate" / (name + ".parquet") for name in SCHEMAS],
            expected_keys=[name + ".parquet" for name in SCHEMAS],
            schemas={name: described_schema(schema) for name, schema in SCHEMAS.items()},
            inputs=evidence.inputs(), read_snapshot=snapshot,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    (output_dir / "summary.json").write_text(_json(summary) + "\n")
    return artifact, evidence.artifact_dir, summary


def _parse_doc_arg(value: str):
    if ":" in value:
        year_text, doc_id = value.split(":", 1)
        if year_text.isdigit():
            return (int(year_text), doc_id)
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("build", "acquire", "acquire-index", "acquire-indexes"):
        sub = commands.add_parser(command)
        if command in ("build", "acquire"):
            sub.add_argument("--selection", type=Path, required=True)
        elif command == "acquire-index":
            sub.add_argument("--year", type=int, required=True)
        else:
            sub.add_argument("--year", type=int, action="append", dest="years")
            sub.add_argument("--start-year", type=int)
            sub.add_argument("--end-year", type=int)
            sub.add_argument("--max-requests", type=int)
        sub.add_argument("--output-dir", type=Path, required=True)
        sub.add_argument("--archive-root", type=Path)
        if command != "build":
            if command == "acquire":
                sub.add_argument("--doc-id", action="append", required=True)
                sub.add_argument("--max-requests", type=int)
            sub.add_argument("--captures-out", type=Path, required=True)
        else:
            sub.add_argument("--read-index", type=Path)
    args = parser.parse_args()
    if args.command != "build":
        if args.command == "acquire-index":
            selected = acquire_index(args.year, args.output_dir, archive_root=args.archive_root)
        elif args.command == "acquire-indexes":
            selected = acquire_indexes(
                args.years,
                args.output_dir,
                start_year=args.start_year,
                end_year=args.end_year,
                max_requests=args.max_requests,
                archive_root=args.archive_root,
            )
        else:
            doc_ids = [_parse_doc_arg(item) for item in args.doc_id]
            selected = acquire(
                json.loads(args.selection.read_text()),
                doc_ids,
                args.output_dir,
                max_requests=args.max_requests,
                archive_root=args.archive_root,
            )
        args.captures_out.write_text(_json(selected) + "\n")
        print(_json({"captures": str(args.captures_out), "reports": len(selected["reports"])}))
    else:
        artifact, evidence, summary = build(json.loads(args.selection.read_text()), args.output_dir, archive_root=args.archive_root,
                                             read_index=args.read_index)
        print(_json({"artifact_digest": artifact.pin.artifact_digest, "evidence": str(evidence), **summary}))


if __name__ == "__main__":
    main()
