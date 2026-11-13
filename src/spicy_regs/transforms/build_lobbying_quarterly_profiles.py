"""Quarterly LD-2 decisions over one pinned native filing table.

The API supplies no predecessor UUID. A unique original and a unique later amendment
in the same native registrant/client/year/quarter can be interpreted without guessing
a chain. Other competing reports remain unresolved. Posting time alone never wins.
"""

from __future__ import annotations

from collections import Counter
from contextlib import closing
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
from itertools import groupby
import json
from pathlib import Path
import re

import duckdb
import pyarrow as pa

from spicy_regs.parquet_rows import write_rows

TABLE = "lobbying_quarterly_profiles"
OUTPUT = TABLE + ".parquet"
POLICY = "lda-quarterly-selection/1"
PERIODS = {"first_quarter": 1, "second_quarter": 2, "third_quarter": 3, "fourth_quarter": 4}
FIELDS = (
    "filing_uuid filing_type filing_year filing_period dt_posted registrant_id client_id "
    "registrant_name client_name income expenses expenses_method termination_date url"
).split()
TEXT_FIELDS = (
    "profile_id scope_id registrant_id client_id registrant_name client_name filing_period "
    "selection_status selection_reason selected_filing_uuid selected_filing_type selected_source_url "
    "income income_status expenses expenses_status expenses_method termination_date "
    "original_income original_expenses income_amendment_change expenses_amendment_change "
    "witnesses_json excluded_filing_uuids_json cutoff source_table_generation source_table_sha256 "
    "coverage_status amount_basis overlap_policy selection_policy selection_sha256"
).split()
SCHEMA = pa.schema([(field, pa.string()) for field in TEXT_FIELDS] + [
    ("filing_year", pa.int32()), ("filing_count", pa.int32()), ("amendment_count", pa.int32()),
    ("period_start", pa.date32()), ("period_end", pa.date32()),
])
PIN = re.compile(r"sha256:[0-9a-f]{64}\Z")
OVERLAP = "Keep registrant income and in-house expenses separate; expenses may include outside firm fees. Never add the two."


def json_bytes(value):
    """Decimal and date observations stay lossless in source witnesses."""
    def encode(item):
        if isinstance(item, Decimal):
            return format(item, "f")
        if isinstance(item, (datetime, date)):
            return item.isoformat()
        raise TypeError(type(item).__name__)
    return json.dumps(value, default=encode, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest_file(path):
    with Path(path).open("rb") as stream:
        return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()


def instant(value):
    try:
        result = value if isinstance(value, datetime) else datetime.fromisoformat(value)
        return result.astimezone(UTC) if result.tzinfo is not None else None
    except (TypeError, ValueError):
        return None


def validate_selection(selection):
    """Require explicit scope and exact input pins; never infer population completeness."""
    required = {"filing_year", "periods", "cutoff", "registrant_ids", "source_table_generation", "source_table_sha256"}
    if set(selection) != required:
        raise ValueError("Quarterly selection requires exactly " + ", ".join(sorted(required)))
    year = selection["filing_year"]
    if type(year) is not int or not 2008 <= year <= 2100:
        raise ValueError("Quarterly LD-2 selection requires a filing year from 2008 through 2100")
    periods = selection["periods"]
    if not isinstance(periods, list) or not periods or len(set(periods)) != len(periods) or not set(periods) <= PERIODS.keys():
        raise ValueError("Choose distinct native quarterly periods")
    if instant(selection["cutoff"]) is None:
        raise ValueError("The as-of cutoff must carry a timezone")
    ids = selection["registrant_ids"]
    if not isinstance(ids, list) or any(not isinstance(v, str) or not re.fullmatch(r"[0-9]+", v) for v in ids):
        raise ValueError("registrant_ids must be literal numeric LDA identifiers; an empty list selects all held registrants")
    for field in ("source_table_generation", "source_table_sha256"):
        if not isinstance(selection[field], str) or not PIN.fullmatch(selection[field]):
            raise ValueError(field + " requires an exact SHA-256 pin")
    return selection


def filing_kind(code, quarter):
    """Recognize the publisher's quarterly report, amendment, termination and no-activity codes."""
    code = code.removesuffix("Y") if isinstance(code, str) else code
    if code in {f"Q{quarter}", f"{quarter}T"}:
        return "original"
    if code in {f"{quarter}A", f"{quarter}@"}:
        return "amendment"
    return "unsupported"


def money(value):
    """Return a reported estimate, never an exact expenditure or an inferred below-threshold zero."""
    if value is None:
        return None, "source_null"
    if value == "":
        return None, "source_empty"
    if isinstance(value, (float, bool)):
        return None, "unsupported_number"
    try:
        number = Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        return None, "unsupported_amount"
    if not number.is_finite() or number < 0:
        return None, "invalid_amount"
    # The native API has no threshold-checkbox field. Keep its literal zero in
    # witnesses, without claiming that zero proves no income/expense.
    if number < 5000:
        return None, "below_threshold_or_zero_unresolved"
    return format(number, "f"), "reported_estimate"


def measures(filing):
    if filing is None:
        return {m: (None, "unresolved_selection") for m in ("income", "expenses")}
    if filing["income"] is not None and filing["expenses"] is not None:
        return {m: (None, "both_measures_reported_unresolved") for m in ("income", "expenses")}
    result = {m: money(filing[m]) for m in ("income", "expenses")}
    if result["expenses"][0] is not None and str(filing["expenses_method"]).upper() not in {"A", "B", "C"}:
        result["expenses"] = (None, "reporting_method_unresolved")
    return result


def reported_change(after, before):
    # Decimal's default precision is smaller than the native DECIMAL(38,6).
    # Allow every whole and fractional digit of both fixed-point observations.
    with localcontext() as context:
        context.prec = max(len(v.partition(".")[0]) for v in (after, before)) + max(
            len(v.partition(".")[2]) for v in (after, before)
        ) + 2
        return format(Decimal(after) - Decimal(before), "f")


def _decision(filings, start, cutoff, quarter):
    eligible, excluded = [], []
    for filing in filings:
        posted = instant(filing["dt_posted"])
        if posted is not None and posted > cutoff:
            excluded.append(filing["filing_uuid"])
        else:
            eligible.append(filing)
    originals = [f for f in eligible if filing_kind(f["filing_type"], quarter) == "original"]
    amendments = [f for f in eligible if filing_kind(f["filing_type"], quarter) == "amendment"]
    reason, chosen = "", None
    if not eligible:
        reason = "no_filing_before_cutoff"
    elif any(not f["registrant_id"] or not f["client_id"] for f in eligible):
        reason = "missing_native_identity"
    elif any(instant(f["dt_posted"]) is None or instant(f["dt_posted"]).date() < start for f in eligible):
        reason = "unresolved_posting_time"
    elif len(originals) + len(amendments) != len(eligible):
        reason = "unsupported_quarterly_type"
    elif len(originals) != 1:
        reason = "missing_original" if not originals else "competing_originals"
    elif len(amendments) > 1:
        reason = "competing_amendments_no_predecessor_link"
    elif amendments and instant(amendments[0]["dt_posted"]) <= instant(originals[0]["dt_posted"]):
        reason = "amendment_order_unresolved"
    else:
        chosen = (amendments or originals)[0]
        reason = "unique_amendment_restatement" if amendments else "unique_original"
    return chosen, reason, originals[0] if len(originals) == 1 else None, len(amendments), excluded


def profile(filings, selection):
    first = filings[0]
    year, period = int(first["filing_year"]), first["filing_period"]
    quarter = PERIODS[period]
    start = date(year, quarter * 3 - 2, 1)
    end = date(year + 1, 1, 1) if quarter == 4 else date(year, quarter * 3 + 1, 1)
    from datetime import timedelta
    end -= timedelta(days=1)
    chosen, reason, original, amendments, excluded = _decision(filings, start, instant(selection["cutoff"]), quarter)
    scope_sha = "sha256:" + hashlib.sha256(json_bytes(selection)).hexdigest()
    # Null identities cannot accidentally merge unrelated missing-ID filings.
    key = [POLICY, first["registrant_id"], first["client_id"], year, period]
    if not first["registrant_id"] or not first["client_id"]:
        key.append(first["filing_uuid"])
    result = dict.fromkeys(TEXT_FIELDS)
    result.update(
        profile_id=hashlib.sha256(json_bytes(key)).hexdigest(), scope_id=scope_sha,
        registrant_id=first["registrant_id"], client_id=first["client_id"],
        filing_year=year, filing_period=period, period_start=start, period_end=end,
        filing_count=len(filings), amendment_count=amendments,
        selection_status="selected" if chosen else "unresolved", selection_reason=reason,
        cutoff=selection["cutoff"], source_table_generation=selection["source_table_generation"],
        source_table_sha256=selection["source_table_sha256"], selection_sha256=scope_sha,
        coverage_status="incomplete_retained_population", amount_basis="reported_estimate_usd",
        overlap_policy=OVERLAP, selection_policy=POLICY,
        witnesses_json=json_bytes(filings).decode(), excluded_filing_uuids_json=json_bytes(sorted(excluded)).decode(),
    )
    for field in ("registrant_name", "client_name"):
        values = {f[field] for f in filings if f["filing_uuid"] not in excluded}
        result[field] = chosen[field] if chosen else next(iter(values)) if len(values) == 1 else None
    if chosen:
        result.update(selected_filing_uuid=chosen["filing_uuid"], selected_filing_type=chosen["filing_type"],
                      selected_source_url=chosen["url"], expenses_method=chosen["expenses_method"],
                      termination_date=str(chosen["termination_date"]) if chosen["termination_date"] is not None else None)
    selected_measures, original_measures = measures(chosen), measures(original)
    for measure in ("income", "expenses"):
        value, status = selected_measures[measure]
        result[measure], result[measure + "_status"] = value, status
        before, _ = original_measures[measure]
        result["original_" + measure] = before
        comparable = measure == "income" or (chosen and original and
            str(chosen["expenses_method"]).upper() == str(original["expenses_method"]).upper())
        if chosen and original and chosen is not original and before is not None and value is not None and comparable:
            result[measure + "_amendment_change"] = reported_change(value, before)
    return result


def build_profiles(selection, source: Path, output: Path):
    """Stream a selected quarter through the existing DuckDB/Arrow writers, with no child joins or copies."""
    validate_selection(selection)
    if digest_file(source) != selection["source_table_sha256"]:
        raise ValueError("Native filing table differs from selected SHA-256")
    counts = Counter()
    with closing(duckdb.connect()) as con:
        con.execute("SET threads=1")
        con.execute("SET memory_limit='256MB'")
        con.execute("SET max_temp_directory_size='0B'")
        con.read_parquet(str(source)).create_view("source_filings")
        columns = {r[0] for r in con.execute("DESCRIBE source_filings").fetchall()}
        if missing := set(FIELDS) - columns:
            raise ValueError("Quarterly profiles require source columns: " + ", ".join(sorted(missing)))
        # Registration amendments and contributions never enter a monetary group.
        sql = "SELECT " + ",".join(FIELDS) + " FROM source_filings WHERE filing_year=? AND filing_period IN (SELECT unnest(?))"
        params = [selection["filing_year"], selection["periods"]]
        if selection["registrant_ids"]:
            sql += " AND registrant_id IN (SELECT unnest(?))"
            params.append(selection["registrant_ids"])
        # A missing type is an unresolved witness, not an excluded registration.
        # SQL NOT IN alone would silently discard it before interpretation.
        sql += " AND (filing_type IS NULL OR filing_type NOT IN ('RR','RA'))"
        sql += " ORDER BY registrant_id,client_id,filing_period,filing_uuid"
        cursor = con.execute(sql, params)
        def records():
            while batch := cursor.fetchmany(2000):
                for values in batch:
                    yield dict(zip(FIELDS, values, strict=True))
        seen = set()
        def grouped():
            def key(f):
                return (f["registrant_id"], f["client_id"], f["filing_period"],
                        f["filing_uuid"] if not f["registrant_id"] or not f["client_id"] else "")
            for _, group in groupby(records(), key):
                filings = list(group)
                for f in filings:
                    if not f["filing_uuid"] or f["filing_uuid"] in seen:
                        raise ValueError("Native source repeats or omits a filing_uuid")
                    seen.add(f["filing_uuid"])
                row = profile(filings, selection)
                counts["filings"] += len(filings)
                counts["profiles"] += 1
                counts[row["selection_reason"]] += 1
                yield row
            if not counts["profiles"]:
                raise ValueError("Selected quarterly population is empty; no candidate replaces prior output")
        write_rows(grouped(), output, SCHEMA)
    return dict(counts)
