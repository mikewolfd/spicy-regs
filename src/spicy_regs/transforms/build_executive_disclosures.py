"""Build a declared filing lookup and source-linked Part 6 observations, without person profiles."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

import pyarrow as pa

from spicy_regs.parquet_rows import write_rows

TABLES = ("executive_disclosure_filings", "executive_disclosure_facts", "executive_disclosure_pages")
ACCESS_STATES = frozenset({"not_requested", "requested", "available", "unavailable", "expired", "access_refused"})
ACCESS_BASES = frozenset({"public_posting", "form201_required", "agency_request_required", "human_attestation_required"})


def _schema(text, **typed):
    return pa.schema([(name, pa.string()) for name in text.split()] + list(typed.items()))


SCHEMAS = {
    TABLES[0]: _schema("""cohort_id coverage_note filing_id official_id official_name position_raw agency_raw
        report_type report_period_raw form_type filing_date filer_signed_date agency_certification_date
        oge_certification_date metadata_review_ref source_url source_sha256 acquired_at access_status access_basis
        access_reason access_evidence_ref amendment_status amendment_details_json facts_status facts_reason
        metadata_json""", report_year=pa.int64(), byte_size=pa.int64(), cover_page=pa.int64()),
    TABLES[1]: _schema("""cohort_id filing_id fact_id section row_number_raw description_raw eif_raw value_raw
        income_type_raw income_amount_raw ownership_label value_status income_status currency
        source_url source_sha256 source_locator parser_revision backend_version raw_cells_json cell_boxes_json""",
        page=pa.int64(), value_lower=pa.int64(), value_upper=pa.int64(), income_lower=pa.int64(), income_upper=pa.int64()),
    TABLES[2]: _schema("""cohort_id filing_id source_url source_sha256 source_locator page_text filer_identity_json table_bbox_json
        parser_revision backend_version""", page=pa.int64(), source_slot_count=pa.int64(), blank_slot_count=pa.int64()),
}
FILING_FIELDS = set(SCHEMAS[TABLES[0]].names) - {"cohort_id", "coverage_note", "facts_status", "facts_reason", "metadata_json"}


def amount_range(raw: str) -> tuple[str, int | None, int | None]:
    """Keep source USD integer bands; a blank is never zero and unknown text is never guessed."""
    if raw == "":
        return "blank", None, None
    match = re.fullmatch(r"\$([0-9]{1,3}(?:,[0-9]{3})*|[0-9]+) - \$([0-9]{1,3}(?:,[0-9]{3})*|[0-9]+)", raw)
    if match:
        lower, upper = (int(value.replace(",", "")) for value in match.groups())
        if lower > upper:
            return "unrecognized", None, None
        return "range", lower, upper
    return "unrecognized", None, None


def validate_selection(selection: dict) -> None:
    """Every row is one declared report; unknown access and amendment states stay explicit."""
    if not isinstance(selection, dict) or not selection.get("cohort_id") or not selection.get("coverage_note"):
        raise ValueError("selection needs a cohort identity and coverage limit")
    filings = selection.get("filings")
    if not isinstance(filings, list) or not filings:
        raise ValueError("declare a nonempty report cohort")
    seen = set()
    for item in filings:
        if not isinstance(item, dict) or set(item) != FILING_FIELDS:
            raise ValueError("filing selection fields differ from the declared schema")
        if not all(isinstance(item[key], str) and item[key].strip()
                   for key in ("filing_id", "official_id", "official_name", "position_raw", "report_type",
                               "form_type", "report_period_raw", "metadata_review_ref", "access_evidence_ref")):
            raise ValueError("each filing needs reviewed person/report identity and evidence")
        if item["official_id"] == item["position_raw"] or item["filing_id"] in seen:
            raise ValueError("person identity cannot be an office and filing identities must be unique")
        seen.add(item["filing_id"])
        if type(item["report_year"]) is not int or not 1900 <= item["report_year"] <= 9999:
            raise ValueError("native report year must be an explicit calendar year")
        if item["report_type"] == "Annual" and item["report_period_raw"] != str(item["report_year"]):
            raise ValueError("annual native reporting period must match its reviewed cover year")
        if type(item["cover_page"]) is not int or item["cover_page"] < 1:
            raise ValueError("cover page must be a one-based original locator")
        if not isinstance(item["access_reason"], str) or not item["access_reason"].strip():
            raise ValueError("each access outcome needs its explicit reason")
        if item["access_status"] not in ACCESS_STATES or item["access_basis"] not in ACCESS_BASES:
            raise ValueError("unknown access outcome or access basis")
        if item["amendment_status"] not in {"none_observed_in_retained_document", "included_unreconciled", "unknown"}:
            raise ValueError("unknown amendment state")
        if not isinstance(json.loads(item["amendment_details_json"]), list):
            raise ValueError("amendment details must be an ordered JSON list")
        if item["access_status"] == "available":
            from spicy_docs.sources.executive_disclosures import public_filing_url
            public_filing_url(item["source_url"])
            if item["access_basis"] != "public_posting":
                raise ValueError("this route only replays public postings; human access is separate")
            if not re.fullmatch(r"sha256:[0-9a-f]{64}", item["source_sha256"] or ""):
                raise ValueError("available filing requires an exact original digest")
            if type(item["byte_size"]) is not int or item["byte_size"] <= 0:
                raise ValueError("available filing requires its original byte size")
        elif item["source_sha256"] is not None or item["byte_size"] is not None:
            raise ValueError("an unavailable/requested record cannot claim available original bytes")


def build_tables(selection: dict, output_dir: Path, *, evidence, store) -> list[Path]:
    """Replay bounded originals by digest; write no output tables until the entire selection is checked."""
    from spicy_docs.sources.executive_disclosures import MAX_BYTES, SECTION, UnsupportedDisclosure, read_part6

    validate_selection(selection)
    output_dir.mkdir(parents=True, exist_ok=True)
    lookup, facts, pages = [], [], []
    for item in selection["filings"]:
        filing = {**item, "cohort_id": selection["cohort_id"], "coverage_note": selection["coverage_note"],
                  "metadata_json": json.dumps(item, ensure_ascii=False, sort_keys=True),
                  "facts_status": "not_available", "facts_reason": item["access_reason"]}
        lookup.append(filing)
        if item["access_status"] != "available":
            continue
        if item["byte_size"] > MAX_BYTES:
            raise ValueError("selected original exceeds the source reader byte bound")
        with store.open(item["source_sha256"]) as stream:
            body = stream.read(MAX_BYTES + 1)
        if len(body) != item["byte_size"] or "sha256:" + hashlib.sha256(body).hexdigest() != item["source_sha256"]:
            raise ValueError("retained original differs from selected source version")
        evidence.retain_bytes(body, stage="executive-filing-replay", raw_source=True,
                              original_location=item["source_url"], filing_id=item["filing_id"])
        try:
            if item["amendment_status"] != "none_observed_in_retained_document":
                raise UnsupportedDisclosure("amendment status requires review; original and amendment remain available")
            if item["form_type"] != "278e" or item["report_type"] != "Annual":
                raise UnsupportedDisclosure("only the qualified annual Form 278e Part 6 is supported")
            result = read_part6(body, filer_name=item["official_name"], report_year=item["report_year"])
        except UnsupportedDisclosure as error:
            filing.update(facts_status="unsupported", facts_reason=str(error))
            evidence.event("interpretation-refusal", filing_id=item["filing_id"], reason=str(error))
            continue
        filing.update(facts_status="qualified_part6", facts_reason="Only the selected Part 6; other sections unsupported")
        shared = {key: filing[key] for key in ("cohort_id", "filing_id", "source_url", "source_sha256")}
        shared.update(parser_revision=result.parser_revision, backend_version=result.backend_version)
        for page in result.pages:
            blank_count = sum(row["blank_slot"] for row in result.rows if row["page"] == page["page"])
            pages.append({**shared, **page, "blank_slot_count": blank_count})
        for raw in result.rows:
            if raw["blank_slot"]:
                continue
            value_status, value_lower, value_upper = amount_range(raw["value_raw"])
            income_status, income_lower, income_upper = amount_range(raw["income_amount_raw"])
            owner = re.match(r"^\((Self|Spouse|Joint)\)", raw["description_raw"])
            fields = {key: value for key, value in raw.items() if key != "blank_slot"}
            facts.append({**shared, **fields, "section": SECTION,
                          "fact_id": f"{item['filing_id']}:{result.source_sha256}:part6:{raw['row_number_raw']}",
                          "ownership_label": owner[1] if owner else None,
                          "value_status": value_status, "value_lower": value_lower, "value_upper": value_upper,
                          "income_status": income_status, "income_lower": income_lower, "income_upper": income_upper,
                          "currency": "USD" if "range" in (value_status, income_status) else None})
    return [write_rows(rows, output_dir / f"{name}.parquet", SCHEMAS[name])
            for name, rows in zip(TABLES, (lookup, facts, pages), strict=True)]
