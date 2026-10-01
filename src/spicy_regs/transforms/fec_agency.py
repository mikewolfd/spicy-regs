"""Agency report editions and measures from sealed SpicyDocs native facts.

The input is one complete selected collection, never source XML/HTML/Word bytes.
Metric definitions retain their XML namespace and full path; different schema
versions and repeated observations are not merged. Word runs supply readable
paragraphs, not inferred metrics. PDF URLs remain metadata with deferred bodies.
"""

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
import hashlib
import json
import re
from urllib.parse import urlsplit

import pyarrow as pa

from .fec_query import AMOUNT_TYPE, IDENTITY_VERSION, exact_amount, observation_id, record_evidence

MAPPING_VERSION = "fec-agency-native/1"
FOIA_NAMESPACES = frozenset(f"http://leisp.usdoj.gov/niem/FoiaAnnualReport/extension/{v}" for v in ("1.02", "1.03"))
NIEM = "{http://niem.gov/niem/structures/2.0}"
WORD = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
COMMON = "record_id identity_version mapping_version collection_id source_record_id source_sha256 source_locator_json source_authority selection_evidence_sha256 source_url observed_at subrecord_pointer".split()
REPORT_SCHEMA = pa.schema(
    [
        (n, pa.string())
        for n in COMMON
        + "report_id report_type title title_basis native_report_number schema_version fiscal_year_raw period_basis publication_date_raw publication_date_status organizations_json metadata_json body_status".split()
    ]
    + [
        ("fiscal_year", pa.int32()),
        ("period_start", pa.date32()),
        ("period_end", pa.date32()),
        ("publication_date", pa.date32()),
    ]
)
METRIC_SCHEMA = pa.schema(
    [
        (n, pa.string())
        for n in COMMON
        + "report_id metric_definition_id metric_label native_field definition_json dimensions_json raw_value value_status value_operator unit unit_status period_basis mapping_status".split()
    ]
    + [
        ("value", AMOUNT_TYPE),
        ("bound_value", AMOUNT_TYPE),
        ("fiscal_year", pa.int32()),
        ("period_start", pa.date32()),
        ("period_end", pa.date32()),
    ]
)
RECOMMENDATION_SCHEMA = pa.schema(
    [
        (n, pa.string())
        for n in COMMON
        + "report_id recommendation_native_id recommendation_key recommendation_text significant_raw responsible_party reported_status status_basis status_date_raw attributes_json questioned_costs_raw questioned_costs_status funds_for_better_use_raw funds_for_better_use_status".split()
    ]
    + [("questioned_costs", AMOUNT_TYPE), ("funds_for_better_use", AMOUNT_TYPE)]
)
TEXT_SCHEMA = pa.schema([(n, pa.string()) for n in COMMON + "report_id text_kind text native_location_json".split()])
DOCUMENT_SCHEMA = pa.schema(
    [(n, pa.string()) for n in COMMON + "report_id url label relation_type body_status native_location_json".split()]
)
DISPOSITION_SCHEMA = pa.schema(
    [
        (n, pa.string())
        for n in "collection_id source_record_id native_kind disposition reason target_record_ids_json".split()
    ]
)
SCHEMAS = {
    "fec_agency_reports": REPORT_SCHEMA,
    "fec_report_metrics": METRIC_SCHEMA,
    "fec_oversight_recommendations": RECOMMENDATION_SCHEMA,
    "fec_agency_report_text": TEXT_SCHEMA,
    "fec_agency_report_documents": DOCUMENT_SCHEMA,
    "fec_agency_mapping_dispositions": DISPOSITION_SCHEMA,
}

# Explicitly inspected native metric names. Suffixes alone do not admit measures.
_COUNT_FIELDS = frozenset(
    """
ReliedUponStatuteQuantity ProcessingStatisticsPendingAtStartQuantity ProcessingStatisticsReceivedQuantity
ProcessingStatisticsProcessedQuantity ProcessingStatisticsPendingAtEndQuantity RequestDispositionFullGrantQuantity
RequestDispositionPartialGrantQuantity RequestDispositionFullExemptionDenialQuantity NonExemptionDenialQuantity
RequestDispositionTotalQuantity OtherDenialReasonQuantity ComponentOtherDenialReasonQuantity AppliedExemptionQuantity
AppealDispositionAffirmedQuantity AppealDispositionPartialQuantity AppealDispositionReversedQuantity
AppealDispositionOtherQuantity AppealDispositionTotalQuantity TimeIncrementProcessedQuantity TimeIncrementTotalQuantity
PendingRequestQuantity RequestGrantedQuantity RequestDeniedQuantity AdjudicationWithinTenDaysQuantity TimesUsedQuantity
PostedbyFOIAQuantity PostedbyProgramQuantity BackloggedRequestQuantity BackloggedAppealQuantity
ItemsReceivedLastYearQuantity ItemsReceivedCurrentYearQuantity ItemsProcessedLastYearQuantity
ItemsProcessedCurrentYearQuantity BacklogLastYearQuantity BacklogCurrentYearQuantity ReceivedLastYearQuantity
ReceivedCurrentYearQuantity ProcessedLastYearQuantity ProcessedCurrentYearQuantity
""".split()
)
_DAY_FIELDS = frozenset(
    """
ResponseTimeMedianDaysValue ResponseTimeAverageDaysValue ResponseTimeLowestDaysValue ResponseTimeHighestDaysValue
OldItemPendingDaysQuantity PendingRequestMedianDaysValue PendingRequestAverageDaysValue AdjudicationMedianDaysValue
AdjudicationAverageDaysValue
""".split()
)
FOIA_UNITS = {
    **dict.fromkeys(_COUNT_FIELDS, "count"),
    **dict.fromkeys(_DAY_FIELDS, "days"),
    **dict.fromkeys("ProcessingCostAmount LitigationCostAmount TotalCostAmount FeesCollectedAmount".split(), "amount"),
    "FullTimeEmployeeQuantity": "employees",
    "EquivalentFullTimeEmployeeQuantity": "full_time_equivalent_employees",
    "TotalFullTimeStaffQuantity": "full_time_equivalent_employees",
    "FeesCollectedCostPercent": "percent",
}
OVERSIGHT_METRICS = {
    "field-report-number-of-recs": "count",
    "field-net-questioned-costs": "USD",
    "field-net-funds-for-better-use": "USD",
}


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _hash(value):
    return "sha256:" + hashlib.sha256(_json(value).encode()).hexdigest()


def _local(tag):
    return tag.rsplit("}", 1)[-1]


def _namespace(tag):
    return tag[1:].split("}", 1)[0] if tag.startswith("{") else ""


def _year(raw):
    return int(raw) if isinstance(raw, str) and re.fullmatch(r"[12][0-9]{3}", raw) else None


def _period(year):
    return (date(year - 1, 10, 1), date(year, 9, 30)) if year else (None, None)


def _date(raw):
    if raw is None:
        return None, "source_null"
    if raw == "":
        return None, "source_empty"
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date(), "source_date"
    except (ValueError, AttributeError):
        return None, "unsupported_spelling"


def _number(raw, unit):
    if not isinstance(raw, str):
        return exact_amount(raw)
    text = raw.strip()
    if unit == "USD":
        # Only explicit dollar notation or a retained numeric content value.
        if re.fullmatch(r"\$[+-]?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?", text):
            text = text[1:].replace(",", "")
    return exact_amount(text)


@dataclass(frozen=True)
class AgencySelection:
    collection_id: str
    source_sha256: str
    source_generation_pin: str
    source_authority: str
    selection_evidence_sha256: str

    def __post_init__(self):
        for value in (self.source_sha256, self.source_generation_pin, self.selection_evidence_sha256):
            if not isinstance(value, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", value):
                raise ValueError("Agency selection requires SHA-256 evidence pins")
        if not self.collection_id or not self.source_authority:
            raise ValueError("Agency selection requires collection and authority")


@dataclass
class AgencyMapping:
    tables: dict = field(default_factory=lambda: {name: [] for name in SCHEMAS})
    evidence: list = field(default_factory=list)


class _Mapper:
    def __init__(self, rows, selection):
        self.selection = selection
        self.rows = sorted(rows, key=lambda r: r["source_record_id"])
        self.native = {}
        self.dispositions = {}
        self.result = AgencyMapping()
        for row in self.rows:
            if row["collection_id"] != selection.collection_id or row["source_sha256"] != selection.source_sha256:
                raise ValueError("Agency row differs from its pinned collection")
            if row["source_record_id"] in self.native:
                raise ValueError("Duplicate agency source observation")
            n = json.loads(row["metadata_json"])
            observation_id("fec_agency_reports", row, selection.source_authority)
            self.native[row["source_record_id"]] = n
            self.dispositions[row["source_record_id"]] = dict(
                collection_id=row["collection_id"],
                source_record_id=row["source_record_id"],
                native_kind=n.get("kind"),
                disposition="unsupported",
                reason="Native kind has no qualified agency mapping",
                target_record_ids_json="[]",
            )
        report_rows = [
            r
            for r in self.rows
            if self.native[r["source_record_id"]].get("kind") in {"foia-report", "oversight-report", "word-report"}
        ]
        if len(report_rows) != 1:
            raise ValueError("Agency mapping requires one complete report edition per collection")
        self.report_row = report_rows[0]
        self.report_native = self.native[self.report_row["source_record_id"]]
        self.report_id = observation_id("fec_agency_reports", self.report_row, selection.source_authority)

    def common(self, table, row, pointer=None):
        rid = observation_id(table, row, self.selection.source_authority)
        if pointer is not None:
            rid = _hash([IDENTITY_VERSION, rid, pointer])
        return dict(
            record_id=rid,
            identity_version=IDENTITY_VERSION,
            mapping_version=MAPPING_VERSION,
            collection_id=row["collection_id"],
            source_record_id=row["source_record_id"],
            source_sha256=row["source_sha256"],
            source_locator_json=_json(json.loads(row["source_locator_json"])),
            source_authority=self.selection.source_authority,
            selection_evidence_sha256=self.selection.selection_evidence_sha256,
            source_url=row.get("source_url"),
            observed_at=row.get("observed_at"),
            subrecord_pointer=pointer,
        )

    def dispose(self, row, disposition, reason, target=None):
        d = self.dispositions[row["source_record_id"]]
        d.update(disposition=disposition, reason=reason)
        if target:
            ids = json.loads(d["target_record_ids_json"])
            if target not in ids:
                ids.append(target)
            d["target_record_ids_json"] = _json(ids)

    def emit(self, table, row, values, *, pointer=None, context=()):
        result = {**self.common(table, row, pointer), **values}
        self.result.tables[table].append(result)
        seen = set()
        for witness, role in [
            (row, "primary"),
            (self.report_row, "report_context"),
            *[(r, "dimension_context") for r in context],
        ]:
            if witness["source_record_id"] in seen:
                continue
            seen.add(witness["source_record_id"])
            evidence = record_evidence(table, result["record_id"], witness, self.selection.source_generation_pin)
            for e in evidence:
                e["role"] = role
                self.result.evidence.append(e)
        self.dispose(row, "mapped", table, result["record_id"])
        return result

    def report(self, *, context=(), **values):
        defaults = dict(
            report_id=self.report_id,
            report_type=None,
            title=None,
            title_basis=None,
            native_report_number=None,
            schema_version=None,
            fiscal_year_raw=None,
            fiscal_year=None,
            period_start=None,
            period_end=None,
            period_basis="not_stated",
            publication_date_raw=None,
            publication_date=None,
            publication_date_status="not_stated",
            organizations_json="[]",
            metadata_json=_json(self.report_native["report"]["metadata"]),
            body_status="native_non_pdf_facts",
        )
        defaults.update(values)
        return self.emit("fec_agency_reports", self.report_row, defaults, context=context)

    def metric(
        self,
        row,
        *,
        report,
        native_field,
        label,
        definition,
        dimensions,
        raw,
        unit,
        context=(),
        pointer=None,
        numeric_raw=None,
    ):
        value, status = _number(raw if numeric_raw is None else numeric_raw, unit)
        operator, bound_value = "=", None
        bound = (
            re.fullmatch(r"([<>]=?)([+]?(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+))", raw.strip())
            if isinstance(raw, str)
            else None
        )
        if bound and numeric_raw is None:
            bound_value, bound_status = exact_amount(bound[2])
            if bound_status == "exact":
                operator, status = bound[1], "reported_bound"
        if value is not None and unit in {"count", "employees"} and (value < 0 or value != value.to_integral_value()):
            value, status = None, "invalid_nonnegative_count"
        year = report["fiscal_year"]
        if "LastYear" in native_field and year:
            year -= 1
        start, end = _period(year) if year else (report["period_start"], report["period_end"])
        return self.emit(
            "fec_report_metrics",
            row,
            dict(
                report_id=self.report_id,
                metric_definition_id=_hash(["fec-agency-definition/1", definition]),
                metric_label=label,
                native_field=native_field,
                definition_json=_json(definition),
                dimensions_json=_json(dimensions),
                raw_value=raw,
                value=value,
                bound_value=bound_value,
                value_status=status,
                value_operator=operator,
                unit=unit,
                unit_status="currency_not_stated" if unit == "amount" else "native_definition",
                fiscal_year=year,
                period_start=start,
                period_end=end,
                period_basis="prior_federal_fiscal_year"
                if year and year != report["fiscal_year"]
                else report["period_basis"],
                mapping_status="mapped" if status in {"exact", "reported_bound"} else "value_unavailable",
            ),
            context=context,
            pointer=pointer,
        )


def _tree(mapper, kind):
    nodes = {}
    children = defaultdict(list)
    for row in mapper.rows:
        n = mapper.native[row["source_record_id"]]
        if n["kind"] != kind:
            continue
        ix = n["ordinal_in_report"]
        if ix in nodes:
            raise ValueError("Duplicate native element ordinal")
        nodes[ix] = (n["element"], row)
        children[n["element"]["parent"]].append(ix)
    for ix, (e, _) in nodes.items():
        if e["parent"] is not None and (e["parent"] not in nodes or e["parent"] >= ix):
            raise ValueError("Native element parent missing or out of document order")
    for values in children.values():
        values.sort()
    return nodes, children


def _path(nodes, ix):
    result = []
    while ix is not None:
        result.append(ix)
        ix = nodes[ix][0]["parent"]
    return result[::-1]


def _foia(mapper):
    metadata = mapper.report_native["report"]["metadata"]
    raw_year = (metadata.get("fiscal_year") or {}).get("value")
    year = _year(raw_year)
    start, end = _period(year)
    report = mapper.report(
        report_type="foia_annual_report",
        title="FOIA Annual Report" + (f" Fiscal Year {raw_year}" if raw_year else ""),
        title_basis="report_type_and_source_fiscal_year",
        schema_version=metadata.get("schema_version"),
        fiscal_year_raw=raw_year,
        fiscal_year=year,
        period_start=start,
        period_end=end,
        period_basis="federal_fiscal_year" if year else "not_stated",
        organizations_json=_json(metadata.get("organizations", [])),
        publication_date_status="not_stated_creation_date_retained_in_metadata",
    )
    nodes, children = _tree(mapper, "foia-element")
    ids = defaultdict(list)
    associations = defaultdict(list)
    for ix, (e, _) in nodes.items():
        if NIEM + "id" in e["attributes"]:
            ids[e["attributes"][NIEM + "id"]].append(ix)
        if _local(e["tag"]) == "ComponentDataReference" and NIEM + "ref" in e["attributes"]:
            associations[e["attributes"][NIEM + "ref"]].append(e["parent"])

    def description(ix):
        e, r = nodes[ix]
        return dict(
            element=ix,
            tag=e["tag"],
            text=e.get("text"),
            attributes=e["attributes"],
            source_record_id=r["source_record_id"],
        )

    for ix, (element, row) in nodes.items():
        tag = element["tag"]
        local = _local(tag)
        if local not in FOIA_UNITS or _namespace(tag) not in FOIA_NAMESPACES or children.get(ix):
            text = element.get("text")
            mapper.dispose(
                row,
                "definition_context" if (text and text.strip()) or element["attributes"] else "structural_nondata",
                "Native XML context retained; not an admitted measure",
            )
            if local.endswith(("Quantity", "Value", "Amount", "Percent")):
                mapper.dispose(row, "unsupported", "Metric name, namespace, or leaf shape is not admitted")
            continue
        path = _path(nodes, ix)
        contexts = set(path[1:-1])
        for ancestor in path[1:-1]:
            e = nodes[ancestor][0]
            # Exact sibling dimension values, references and section footnotes.
            for child in children[ancestor]:
                ce = nodes[child][0]
                if child not in path and _local(ce["tag"]) not in FOIA_UNITS and (ce.get("text") or ce["attributes"]):
                    contexts.add(child)
            native_id = e["attributes"].get(NIEM + "id")
            for association in associations.get(native_id, []):
                contexts.add(association)
                contexts.update(children[association])
        # Resolve identifiers within this report only; retain all ambiguous matches.
        pending = list(contexts)
        for context_ix in pending:
            ref = nodes[context_ix][0]["attributes"].get(NIEM + "ref")
            for target in ids.get(ref, []):
                contexts.add(target)
                contexts.update(child for child in children[target] if _local(nodes[child][0]["tag"]) not in FOIA_UNITS)
        context = sorted(contexts)
        definition = dict(
            source_format="foia-xml",
            namespace=_namespace(tag),
            schema_version=metadata.get("schema_version"),
            path=[nodes[i][0]["tag"] for i in path],
            unit=FOIA_UNITS[local],
        )
        mapper.metric(
            row,
            report=report,
            native_field=local,
            label=" / ".join(_local(nodes[i][0]["tag"]) for i in path[1:]),
            definition=definition,
            dimensions=dict(native_context=[description(i) for i in context]),
            raw=element.get("text"),
            unit=FOIA_UNITS[local],
            context=[nodes[i][1] for i in context],
        )


def _field(fields, name):
    found = [f for f in fields if f["native_field"] == name]
    if len(found) > 1:
        raise ValueError("Duplicate report metadata field")
    return found[0] if found else {}


def _field_date(field):
    times = field.get("times", [])
    return times[0].get("datetime") if len(times) == 1 else None


def _oversight(mapper):
    meta = mapper.report_native["report"]["metadata"]
    fields = meta["fields"]
    issued = _field_date(_field(fields, "field-report-date-issued"))
    issued_date, issued_status = _date(issued)
    start_raw = _field_date(_field(fields, "field-sarc-start-date"))
    end_raw = _field_date(_field(fields, "field-sarc-end-date"))
    start, _ = _date(start_raw)
    end, _ = _date(end_raw)
    report = mapper.report(
        report_type=_field(fields, "field-report-type").get("value"),
        title=meta.get("title"),
        title_basis="native_report_title",
        native_report_number=_field(fields, "field-report-number").get("value"),
        publication_date_raw=issued,
        publication_date=issued_date,
        publication_date_status=issued_status,
        period_start=start,
        period_end=end,
        period_basis="source_report_period" if start or end else "not_stated",
        organizations_json=_json(
            [f for f in fields if f["native_field"] in {"field-report-submitting-oig", "field-report-agency-reviewed"}]
        ),
    )
    for row in mapper.rows:
        native = mapper.native[row["source_record_id"]]
        kind = native["kind"]
        if kind == "oversight-field":
            f = native["field"]
            name = f["native_field"]
            if name in OVERSIGHT_METRICS:
                values = f.get("numeric_content", [])
                numeric = values[0] if len(values) == 1 else None
                mapper.metric(
                    row,
                    report=report,
                    native_field=name,
                    label=f.get("label"),
                    definition=dict(
                        source_namespace="https://www.oversight.gov/",
                        native_field=name,
                        label=f.get("label"),
                        unit=OVERSIGHT_METRICS[name],
                    ),
                    dimensions=dict(
                        report_number=report["native_report_number"],
                        source_position=f.get("source_position"),
                        numeric_content=values,
                    ),
                    raw=f.get("value"),
                    numeric_raw=numeric,
                    unit=OVERSIGHT_METRICS[name],
                )
            else:
                mapper.dispose(row, "definition_context", "Report metadata retained in report edition")
        elif kind == "oversight-body":
            body = native["body"]
            if body["kind"] == "recommendations":
                _recommendations(mapper, row, body, report)
            else:
                mapper.emit(
                    "fec_agency_report_text",
                    row,
                    dict(
                        report_id=mapper.report_id,
                        text_kind=body["kind"],
                        text=body.get("text"),
                        native_location_json=_json(body.get("source_position")),
                    ),
                )
        elif kind == "oversight-asset":
            asset = native["asset"]
            url = asset.get("url")
            mapper.emit(
                "fec_agency_report_documents",
                row,
                dict(
                    report_id=mapper.report_id,
                    url=url,
                    label=asset.get("label"),
                    relation_type=asset.get("role"),
                    body_status="deferred_pdf"
                    if url and urlsplit(url).path.lower().endswith(".pdf")
                    else "linked_body_not_qualified",
                    native_location_json=_json(asset.get("source_position")),
                ),
            )


def _recommendations(mapper, row, body, report):
    parsed = []
    for ti, table in enumerate(body.get("tables", [])):
        rows = table.get("rows", [])
        if not rows:
            continue
        headers = [c.get("text") for c in rows[0]["cells"]]
        expected = [
            "Recommendation Number",
            "Significant Recommendation",
            "Recommended Questioned Costs",
            "Recommended Funds for Better Use",
            "Additional Details",
            "",
        ]
        if headers != expected or (len(rows) - 1) % 2:
            mapper.dispose(row, "unsupported", "Recommendation table has an unqualified column or row layout")
            return
        for ri in range(1, len(rows), 2):
            cells, textcells = rows[ri]["cells"], rows[ri + 1]["cells"]
            if len(cells) != 6 or len(textcells) != 1 or textcells[0].get("attributes", {}).get("colspan") != "6":
                mapper.dispose(row, "unsupported", "Recommendation detail row does not match its native heading row")
                return
            if not cells[0].get("text"):
                mapper.dispose(row, "unsupported", "Recommendation number is not stated")
                return
            parsed.append((ti, ri, cells, textcells[0]))
    if not parsed:
        mapper.dispose(row, "unsupported", "No qualified recommendation table rows")
        return
    match = re.match(r"This report has ([0-9]+) open recommendations\.\s", body.get("text", ""))
    status = "open" if match and int(match[1]) == len(parsed) else None
    for ti, ri, cells, textcell in parsed:
        values = [c.get("text") for c in cells]
        questioned, qstatus = _number(values[2], "USD")
        better, bstatus = _number(values[3], "USD")
        mapper.emit(
            "fec_oversight_recommendations",
            row,
            dict(
                report_id=mapper.report_id,
                recommendation_native_id=values[0],
                recommendation_key=_json(["oversight.gov", mapper.report_row.get("source_url"), values[0]]),
                recommendation_text=textcell.get("text"),
                significant_raw=values[1],
                responsible_party=None,
                reported_status=status,
                status_basis="native_open_heading_count_matches_table" if status else "not_stated_for_row",
                status_date_raw=None,
                attributes_json=_json(
                    dict(cells=cells, description_cell=textcell, report_number=report["native_report_number"])
                ),
                questioned_costs_raw=values[2],
                questioned_costs=questioned,
                questioned_costs_status=qstatus,
                funds_for_better_use_raw=values[3],
                funds_for_better_use=better,
                funds_for_better_use_status=bstatus,
            ),
            pointer=f"/body/tables/{ti}/rows/{ri}",
        )


def _word(mapper):
    nodes, children = _tree(mapper, "word-element")
    paragraphs = defaultdict(list)
    for row in mapper.rows:
        n = mapper.native[row["source_record_id"]]
        if n["kind"] in {"word-element", "word-part"}:
            mapper.dispose(row, "structural_nondata", "Word package structure is not an agency metric")
        if n["kind"] != "word-body":
            continue
        body = n["body"]
        ix = body["element"]
        if ix not in nodes:
            raise ValueError("Word body points to a missing native element")
        path = _path(nodes, ix)
        paragraph = next((i for i in reversed(path) if nodes[i][0]["tag"] == WORD + "p"), None)
        # A tab in paragraph properties defines a tab stop, not document text.
        is_run = nodes[ix][0]["parent"] is not None and nodes[nodes[ix][0]["parent"]][0]["tag"] == WORD + "r"
        if paragraph is not None and is_run and body["tag"] in {WORD + "t", WORD + "tab", WORD + "br"}:
            text = body.get("text") if body["tag"] == WORD + "t" else "\t" if body["tag"] == WORD + "tab" else "\n"
            paragraphs[paragraph].append((ix, text or "", row))
        else:
            mapper.dispose(row, "structural_nondata", "Formatting token is not report text or a metric")
    ordered = [(i, sorted(runs)) for i, runs in sorted(paragraphs.items())]
    text_values = ["".join(r[1] for r in runs) for _, runs in ordered]
    fiscal = [match[1] for text in text_values if (match := re.fullmatch(r"Fiscal Year ([12][0-9]{3})", text.strip()))]
    raw_year = fiscal[0] if len(set(fiscal)) == 1 else None
    year = _year(raw_year)
    start, end = _period(year)
    explicit_title = next((t for t in text_values if t.strip() == "Freedom of Information Act Annual Report"), None)
    title = explicit_title or next((t for t in text_values if t.strip()), None)
    heading_rows = [
        r[2]
        for (_, runs), text in zip(ordered, text_values, strict=True)
        if text == title or re.fullmatch(r"Fiscal Year ([12][0-9]{3})", text.strip())
        for r in runs
    ]
    mapper.report(
        context=heading_rows,
        report_type="foia_annual_report" if explicit_title else "word_report",
        title=title,
        title_basis="native_report_heading" if explicit_title else "first_native_paragraph",
        fiscal_year_raw=raw_year,
        fiscal_year=year,
        period_start=start,
        period_end=end,
        period_basis="explicit_fiscal_year_paragraph" if year else "not_stated",
        body_status="native_word_paragraphs_metrics_unqualified",
    )
    for (ix, runs), text in zip(ordered, text_values, strict=True):
        paragraph, anchor = nodes[ix]
        result = mapper.emit(
            "fec_agency_report_text",
            anchor,
            dict(
                report_id=mapper.report_id,
                text_kind="word_paragraph",
                text=text,
                native_location_json=_json(dict(element=ix, child_indices=paragraph["child_indices"])),
            ),
            context=[r[2] for r in runs],
        )
        for _, _, witness in runs:
            mapper.dispose(
                witness,
                "mapped",
                "Native run supplies report paragraph text; metric interpretation unqualified",
                result["record_id"],
            )


def map_agency_collection(rows, selection: AgencySelection):
    """Map a complete bounded native report collection and account for every row.

    The caller qualifies membership against the sealed collection row count.
    This function does not acquire data, parse original bytes, select a current
    edition, combine metric definitions, or treat an absent row as a zero.
    """
    mapper = _Mapper(rows, selection)
    kind = mapper.report_native["kind"]
    {"foia-report": _foia, "oversight-report": _oversight, "word-report": _word}[kind](mapper)
    mapper.result.tables["fec_agency_mapping_dispositions"] = list(mapper.dispositions.values())
    return mapper.result
