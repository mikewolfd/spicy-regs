"""Capture a declared Senate filing year and replay exact originals into ordinary generations."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import date
from pathlib import Path
from time import monotonic

from spicy_docs.sources.senate_disclosures import (
    MAX_BYTES,
    PARSER_REVISION,
    SEARCH_URL,
    SenateDisclosureError,
    SenateEfdReader,
    SenateYear,
    parse_electronic_ptr,
    parse_search,
)
from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.pipelines.raw_releases import inherit_read_index
from spicy_regs.schemas.senate_disclosures import FAMILY, SCHEMAS, TABLES
from spicy_regs.source_evidence import CaptureEvidence

MAX_TOTAL_BYTES = 64 * 1024 * 1024


def write_json(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def original(store, descriptor):
    size = descriptor["bytes"]
    if type(size) is not int or not 0 < size <= MAX_BYTES:
        raise SenateDisclosureError("Retained Senate original byte bound differs")
    with store.open(descriptor["sha256"]) as stream:
        body = stream.read(size + 1)
    if len(body) != size or "sha256:" + hashlib.sha256(body).hexdigest() != descriptor["sha256"]:
        raise SenateDisclosureError("Retained Senate original checksum/size differs")
    return body


def descriptor(capture, **fields):
    return {
        "url": capture.requested_url,
        "sha256": capture.sha256,
        "bytes": capture.byte_size,
        "observed_at": capture.observed_at,
        **fields,
    }


def listings(manifest, store, *, evidence=None):
    selection = SenateYear(manifest["filing_year"])
    rows, counts, start, seen = [], None, 0, set()
    for item in manifest["search_pages"]:
        if item["url"] != SEARCH_URL or item["start"] != start or (counts is not None and start >= counts[1]):
            raise SenateDisclosureError("Retained Senate search page coordinates differ")
        body = original(store, item)
        total, filtered, page = parse_search(body, selection, start=start)
        if counts is not None and counts != (total, filtered):
            raise SenateDisclosureError("Senate search counts drifted during traversal")
        counts = total, filtered
        if seen.intersection(row["report_id"] for row in page):
            raise SenateDisclosureError("Senate search repeats identities across pages")
        seen.update(row["report_id"] for row in page)
        for row in page:
            row["search_sha256"] = item["sha256"]
        rows.extend(page)
        if evidence is not None:
            retain(evidence, body, item, native_id=f"ptr-search:{selection.year}:{start}", period=selection.year)
        start += len(page)
    if counts is None or len(rows) != counts[1]:
        raise SenateDisclosureError("Senate year traversal is incomplete")
    return rows, counts


def retain(evidence, body, item, *, native_id, period):
    evidence.retain_bytes(
        body,
        raw_source=True,
        stage="senate-original-replay",
        source_url=item["url"],
        original_location=item["url"],
        observed_at=item["observed_at"],
        dataset_id="senate-financial-disclosures",
        native_id=native_id,
        covered_period=str(period),
        source_version=item["sha256"],
    )


def capture_year(output, *, year, accepted_terms=False, prior=None, reader=None, archive_root=None):
    """Fresh complete listing; reuse immutable reports only against the same literal listing row."""
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    evidence = CaptureEvidence(output, FAMILY, archive_root=archive_root)
    manifest = {
        "format": "senate-ptr-captures",
        "version": 1,
        "filing_year": year,
        "parser_revision": PARSER_REVISION,
        "search_pages": [],
        "reports": {},
        "scope": "All PTR filings submitted in the declared calendar year; electronic parsing only.",
    }
    owned_reader = reader is None
    store = evidence.store
    reused, retained_bytes = 0, 0
    began = monotonic()

    def check_deadline():
        if monotonic() - began > 540:
            raise SenateDisclosureError("Senate capture reached its bounded wall allocation")

    try:
        prior_manifest = json.loads(Path(prior).read_text()) if prior else None
        if prior_manifest and prior_manifest["filing_year"] != year:
            raise SenateDisclosureError("Prior Senate captures select a different filing year")
        prior_rows = {row["report_id"]: row for row in listings(prior_manifest, store)[0]} if prior_manifest else {}
        reader = reader or SenateEfdReader.open(accepted_terms=accepted_terms)
        rows, start, counts = [], 0, None
        while True:
            check_deadline()
            cap = reader.search(SenateYear(year), start=start)
            retained_bytes += cap.byte_size
            evidence.capture(
                cap,
                stage="senate-year-search",
                dataset_id="senate-financial-disclosures",
                native_id=f"ptr-search:{year}:{start}",
                covered_period=str(year),
            )
            manifest["search_pages"].append(descriptor(cap, start=start))
            total, filtered, page = parse_search(cap.body, SenateYear(year), start=start)
            if counts is not None and counts != (total, filtered):
                raise SenateDisclosureError("Senate search counts changed during capture")
            counts = total, filtered
            rows.extend(page)
            if len(rows) == filtered:
                break
            start += len(page)
        # Reconcile page continuity, cross-page duplicate IDs and exact original hashes before body acquisition.
        rows, counts = listings(manifest, store)
        for filing in rows:
            check_deadline()
            if filing["report_kind"] != "electronic":
                continue
            identity = filing["report_id"]
            previous = prior_rows.get(identity)
            selected = (prior_manifest or {}).get("reports", {}).get(identity)
            if previous and selected and previous["raw_search_cells"] == filing["raw_search_cells"]:
                body = original(store, selected)
                parse_electronic_ptr(body, filing)
                retain(evidence, body, selected, native_id=identity, period=year)
                manifest["reports"][identity] = selected
                reused += 1
                continue
            if retained_bytes + MAX_BYTES > MAX_TOTAL_BYTES:
                raise SenateDisclosureError("Senate capture would exceed cumulative response allocation")
            cap = reader.report(filing)
            retained_bytes += cap.byte_size
            evidence.capture(
                cap,
                stage="senate-electronic-report",
                dataset_id="senate-financial-disclosures",
                native_id=identity,
                covered_period=str(year),
                source_version=cap.sha256,
            )
            manifest["reports"][identity] = descriptor(cap)
            parse_electronic_ptr(cap.body, filing)
        manifest.update(
            complete=True,
            records_total=counts[0],
            records_filtered=counts[1],
            reused_reports=reused,
            acquired_response_bytes=retained_bytes,
            protected_requests=reader.client.request_count,
        )
        write_json(output / "captures.json", manifest)
    except BaseException as error:
        manifest["complete"] = False
        write_json(output / "captures.partial.json", manifest)
        evidence.finish(error)
        raise
    else:
        evidence.finish()
    finally:
        if owned_reader and reader is not None:
            reader.close()
    return output / "captures.json"


def amount_range(value):
    """Exact printed dollar bounds; unfamiliar ranges remain qualified, with raw text."""
    match = re.fullmatch(r"\$([\d,]+) - \$([\d,]+)", value)
    if match:
        lower, upper = (int(n.replace(",", "")) for n in match.groups())
        if lower <= upper:
            return lower, upper, True, "closed_range"
    match = re.fullmatch(r"Over \$([\d,]+)", value, re.IGNORECASE)
    if match:
        return int(match[1].replace(",", "")), None, False, "open_upper"
    return None, None, None, "unresolved"


def build(captures, output, *, archive_root=None, read_index=None, local_partial=False):
    """Offline replay; explicit local partial candidates remain unpublishable."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    manifest = json.loads(Path(captures).read_text())
    if (
        manifest.get("format") != "senate-ptr-captures"
        or manifest.get("version") != 1
        or type(manifest.get("complete")) is not bool
        or (manifest["complete"] is not True and not local_partial)
    ):
        raise SenateDisclosureError("Only complete declared Senate capture selections can build tables")
    evidence = CaptureEvidence(output, FAMILY, archive_root=archive_root)
    try:
        read_snapshot = inherit_read_index(evidence, read_index)
        evidence.retain_control(Path(captures), stage="senate-capture-selection")
        rows, counts = listings(manifest, evidence.store, evidence=evidence)
        electronic_ids = {r["report_id"] for r in rows if r["report_kind"] == "electronic"}
        held_ids = set(manifest["reports"])
        missing_ids = electronic_ids - held_ids
        if held_ids - electronic_ids or (missing_ids and not local_partial):
            raise SenateDisclosureError("Senate electronic report population differs from its complete listing")
        if local_partial:
            evidence.event(
                "senate-local-source-gaps",
                unresolved_report_ids=sorted(missing_ids),
                source_refusal=manifest.get("source_refusal"),
                qualification="Local partial candidate; unacquired bodies are not empty reports.",
            )
        originals = {}
        for filing in rows:
            if filing["amendment_status"] == "none" and filing["report_kind"] == "electronic":
                key = (filing["first_name"].casefold(), filing["last_name"].casefold(), filing["report_date"])
                originals.setdefault(key, []).append(filing["report_id"])
        reports, transactions = [], []
        for filing in rows:
            item = manifest["reports"].get(filing["report_id"])
            common = {k: filing[k] for k in ("filing_year", "report_id", "source_url")}
            common["report_sha256"] = item["sha256"] if item else None
            report = {
                **common,
                **{
                    k: filing[k]
                    for k in (
                        "first_name",
                        "last_name",
                        "filer_label",
                        "report_kind",
                        "report_title",
                        "amendment_status",
                        "amendment_number",
                        "search_sha256",
                        "search_start",
                        "search_row",
                    )
                },
                "report_date": date.fromisoformat(filing["report_date"]),
                "submitted_date": date.fromisoformat(filing["submitted_date"]),
                "raw_search_cells_json": json.dumps(filing["raw_search_cells"]),
                "parse_status": (
                    "parsed"
                    if item
                    else "unsupported_paper"
                    if filing["report_kind"] == "paper"
                    else "unrequested_electronic"
                ),
                "observed_at": item["observed_at"] if item else None,
                "original_candidate_ids": [],
                "amendment_link_status": "not_applicable",
                "filer_heading": None,
                "filed_at_raw": None,
                "transaction_count": None,
            }
            if filing["amendment_status"] != "none":
                key = (filing["first_name"].casefold(), filing["last_name"].casefold(), filing["report_date"])
                candidates = sorted(originals.get(key, []))
                report["original_candidate_ids"] = candidates
                report["amendment_link_status"] = (
                    "unique_candidate" if len(candidates) == 1 else "ambiguous" if candidates else "unresolved"
                )
            if item:
                if item["url"] != filing["source_url"]:
                    raise SenateDisclosureError("Senate report capture locator differs from its listing")
                body = original(evidence.store, item)
                parsed = parse_electronic_ptr(body, filing)
                retain(evidence, body, item, native_id=filing["report_id"], period=manifest["filing_year"])
                report.update({k: parsed[k] for k in ("filer_heading", "filed_at_raw", "transaction_count")})
                for transaction in parsed["transactions"]:
                    lower, upper, inclusive, status = amount_range(transaction["amount_raw"])
                    transactions.append(
                        {
                            **common,
                            **{k: v for k, v in transaction.items() if k != "raw_cells"},
                            "transaction_date": date.fromisoformat(transaction["transaction_date"]),
                            "raw_cells_json": json.dumps(transaction["raw_cells"]),
                            "amount_lower_usd": lower,
                            "amount_upper_usd": upper,
                            "lower_inclusive": inclusive,
                            "amount_parse_status": status,
                        }
                    )
            reports.append(report)
        selected = [
            (f"ptr-search:{manifest['filing_year']}:{d['start']}", d, "json-body") for d in manifest["search_pages"]
        ]
        selected += [(identity, d, "html-body") for identity, d in manifest["reports"].items()]
        evidence.declare_used_inputs(
            [
                {"location": d["url"], "sha256": d["sha256"], "bytes": d["bytes"], "asset_kind": kind}
                for identity, d, kind in selected
            ],
            scope_id=f"senate-ptr-filing-year:{manifest['filing_year']}",
            complete=not local_partial,
        )
        paths = []
        for table, values in zip(TABLES, (reports, transactions), strict=True):
            path = output / (table + ".parquet")
            pq.write_table(pa.Table.from_pylist(values, schema=SCHEMAS[table]), path)
            paths.append(path)
        evidence.event(
            "senate-population",
            filing_year=manifest["filing_year"],
            listed_reports=len(reports),
            electronic_reports=len(electronic_ids),
            parsed_electronic_reports=len(held_ids),
            unrequested_electronic_reports=len(missing_ids),
            paper_reports=len(reports) - len(electronic_ids),
            transactions=len(transactions),
            source_counts=list(counts),
            amendment_policy="All observations retained; parent candidates are derived by literal name/report date.",
        )
        artifact = build_generation(
            output / "generation",
            family=FAMILY,
            files=paths,
            expected_keys=tuple(t + ".parquet" for t in TABLES),
            schemas={t: described_schema(s) for t, s in SCHEMAS.items()},
            read_snapshot=read_snapshot,
            inputs=evidence.inputs(),
            owned_output_root=output,
            publication_status="local-partial" if local_partial else "complete-family",
        )
        write_json(
            output / "result.json",
            {
                "pin": artifact.pin.as_dict(),
                "reports": len(reports),
                "transactions": len(transactions),
                "publication_status": "local-partial" if local_partial else "complete-family",
                "parsed_electronic_reports": len(held_ids),
                "unrequested_electronic_reports": len(missing_ids),
                "source_evidence": str(evidence.artifact_dir),
            },
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    else:
        evidence.finish()
    return artifact


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    capture = sub.add_parser("capture")
    capture.add_argument("--year", type=int, required=True)
    capture.add_argument("--accept-use-restrictions", action="store_true")
    capture.add_argument("--prior-captures", type=Path)
    replay = sub.add_parser("build")
    replay.add_argument("--captures", type=Path, required=True)
    replay.add_argument("--read-index", type=Path)
    replay.add_argument(
        "--local-partial",
        action="store_true",
        help="Build an unpublishable local candidate retaining explicit missing electronic bodies",
    )
    for command in (capture, replay):
        command.add_argument("--output", type=Path, required=True)
        command.add_argument("--archive-root", type=Path)
    args = parser.parse_args()
    if args.command == "capture":
        result = capture_year(
            args.output,
            year=args.year,
            accepted_terms=args.accept_use_restrictions,
            prior=args.prior_captures,
            archive_root=args.archive_root,
        )
        print(json.dumps({"captures": str(result)}))
    else:
        result = build(
            args.captures,
            args.output,
            archive_root=args.archive_root,
            read_index=args.read_index,
            local_partial=args.local_partial,
        )
        print(json.dumps(result.pin.as_dict()))


if __name__ == "__main__":
    main()
