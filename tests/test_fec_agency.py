"""Semantic checks using the shapes of retained FOIA, Oversight and Word facts."""

from copy import deepcopy
from datetime import date
from decimal import Decimal
import json

import pyarrow as pa
import pytest

from spicy_regs.transforms.fec_agency import (
    AgencySelection,
    FOIA_NAMESPACES,
    NIEM,
    SCHEMAS,
    WORD,
    map_agency_collection,
)

DIGEST = "sha256:" + "a" * 64
PIN = "sha256:" + "b" * 64
SELECTION_PIN = "sha256:" + "c" * 64
EXT = "http://leisp.usdoj.gov/niem/FoiaAnnualReport/extension/1.03"
NC = "http://niem.gov/niem/niem-core/2.0"


def row(native, ordinal, *, url="https://www.fec.gov/documents/report.xml"):
    rid = f"{DIGEST}/{ordinal:020d}"
    return dict(
        collection_id="selected-report",
        source_record_id=rid,
        source_sha256=DIGEST,
        source_url=url,
        source_locator_json=json.dumps(dict(collection_id="selected-report", source_record_id=rid, ordinal=ordinal)),
        metadata_json=json.dumps(native),
    )


def selection():
    return AgencySelection("selected-report", DIGEST, PIN, "fec.gov", SELECTION_PIN)


def element(tag, parent=None, text=None, attributes=None):
    return dict(
        tag=tag,
        parent=parent,
        text=text,
        tail=None,
        attributes=attributes or {},
        child_indices=[],
        namespace_declarations={},
    )


def foia_rows(namespace=EXT):
    def tag(local):
        return "{" + namespace + "}" + local

    elements = [
        element("{http://leisp.usdoj.gov/niem/FoiaAnnualReport/exchange/1.03}FoiaAnnualReport"),
        element("{" + NC + "}Organization", 0, attributes={NIEM + "id": "ORG0"}),
        element("{" + NC + "}OrganizationName", 1, "Federal Election Commission"),
        element(tag("ProcessedRequestSection"), 0),
        element(tag("ProcessingStatistics"), 3, attributes={NIEM + "id": "PS1"}),
        element(tag("ProcessingStatisticsReceivedQuantity"), 4, "306"),
        element(tag("ProcessingStatisticsOrganizationAssociation"), 3),
        element(tag("ComponentDataReference"), 6, attributes={NIEM + "ref": "PS1"}),
        element("{" + NC + "}OrganizationReference", 6, attributes={NIEM + "ref": "ORG0"}),
        element(tag("ProcessedRequestComparisonSection"), 0),
        element(tag("ProcessingComparison"), 9),
        element(tag("ItemsReceivedLastYearQuantity"), 10, "351"),
        element(tag("SimpleResponseTimeIncrementsSection"), 0),
        element(tag("ComponentResponseTimeIncrements"), 12),
        element(tag("TimeIncrement"), 13),
        element(tag("TimeIncrementCode"), 14, "1-20"),
        element(tag("TimeIncrementProcessedQuantity"), 14, "0"),
        element(tag("TimeIncrement"), 13),
        element(tag("TimeIncrementCode"), 17, "21-40"),
        element(tag("TimeIncrementProcessedQuantity"), 17, "0"),
        element(tag("PersonnelAndCostSection"), 0),
        element(tag("PersonnelAndCost"), 20),
        element(tag("ProcessingCostAmount"), 21, "300611.00"),
        element(tag("EquivalentFullTimeEmployeeQuantity"), 21, "1.28"),
    ]
    meta = dict(
        schema_version=namespace.rsplit("/", 1)[-1],
        fiscal_year=dict(element=99, value="2025"),
        creation_dates=[dict(element=98, value="2026-02-18")],
        organizations=[],
    )
    result = [row(dict(kind="foia-report", report=dict(metadata=meta, source=dict(sha256=DIGEST))), 0)]
    return result + [
        row(dict(kind="foia-element", ordinal_in_report=i, element=e), i + 1) for i, e in enumerate(elements)
    ]


def map_rows(rows):
    result = map_agency_collection(rows, selection())
    # Every produced value must also fit the published physical table shape.
    for table, records in result.tables.items():
        assert all(set(r) == set(SCHEMAS[table].names) for r in records)
        assert pa.Table.from_pylist(records, schema=SCHEMAS[table]).to_pylist() == records
    return result


def test_foia_measures_periods_exact_values_and_resolved_dimensions():
    result = map_rows(foia_rows())
    report = result.tables["fec_agency_reports"][0]
    metrics = result.tables["fec_report_metrics"]
    received = next(m for m in metrics if m["native_field"] == "ProcessingStatisticsReceivedQuantity")
    prior = next(m for m in metrics if m["native_field"] == "ItemsReceivedLastYearQuantity")
    money = next(m for m in metrics if m["native_field"] == "ProcessingCostAmount")
    assert received["value"] == Decimal("306")
    assert (received["period_start"], received["period_end"]) == (date(2024, 10, 1), date(2025, 9, 30))
    assert prior["fiscal_year"] == 2024 and prior["period_start"] == date(2023, 10, 1)
    assert "Federal Election Commission" in received["dimensions_json"]
    assert "ORG0" in received["dimensions_json"] and "PS1" in received["dimensions_json"]
    assert report["publication_date"] is None  # The workbook creation date is not a publication date.
    assert money["raw_value"] == "300611.00" and money["value"] == Decimal("300611.00")
    assert money["unit"] == "amount" and money["unit_status"] == "currency_not_stated"
    assert next(m for m in metrics if m["native_field"] == "EquivalentFullTimeEmployeeQuantity")["value"] == Decimal(
        "1.28"
    )


def test_metric_occurrences_dimensions_and_namespaces_do_not_collapse():
    result = map_rows(foia_rows())
    bins = [m for m in result.tables["fec_report_metrics"] if m["native_field"] == "TimeIncrementProcessedQuantity"]
    assert len(bins) == 2 and bins[0]["record_id"] != bins[1]["record_id"]
    assert bins[0]["metric_definition_id"] == bins[1]["metric_definition_id"]
    assert "1-20" in bins[0]["dimensions_json"] and "21-40" in bins[1]["dimensions_json"]
    older = map_rows(foia_rows(EXT.replace("1.03", "1.02")))
    assert (
        older.tables["fec_report_metrics"][0]["metric_definition_id"]
        != result.tables["fec_report_metrics"][0]["metric_definition_id"]
    )
    # Identical values and field spelling do not erase schema-version evidence.
    assert len(FOIA_NAMESPACES) == 2


@pytest.mark.parametrize(
    "raw,expected",
    [
        (None, "source_null"),
        ("", "source_empty"),
        ("N/A", "unsupported_spelling"),
        ("1.5", "invalid_nonnegative_count"),
        ("-1", "invalid_nonnegative_count"),
        ("1e6", "unsupported_spelling"),
    ],
)
def test_foia_preserves_unavailable_and_invalid_values(raw, expected):
    rows = foia_rows()
    native = json.loads(rows[6]["metadata_json"])
    native["element"]["text"] = raw
    rows[6]["metadata_json"] = json.dumps(native)
    metric = map_rows(rows).tables["fec_report_metrics"][0]
    assert metric["raw_value"] == raw
    assert metric["value"] is None and metric["value_status"] == expected


def test_unknown_namespace_and_unknown_metric_are_not_admitted():
    rows = foia_rows("https://example.org/same-spelling")
    result = map_rows(rows)
    assert not result.tables["fec_report_metrics"]
    assert "unsupported" in {d["disposition"] for d in result.tables["fec_agency_mapping_dispositions"]}


def test_foia_inequality_preserves_bound_without_inventing_an_exact_value():
    rows = foia_rows()
    native = json.loads(rows[6]["metadata_json"])
    native["element"]["tag"] = "{" + EXT + "}PendingRequestMedianDaysValue"
    native["element"]["text"] = "<1"
    rows[6]["metadata_json"] = json.dumps(native)
    metric = map_rows(rows).tables["fec_report_metrics"][0]
    assert metric["value"] is None and metric["bound_value"] == Decimal("1")
    assert metric["value_operator"] == "<" and metric["value_status"] == "reported_bound"
    assert metric["raw_value"] == "<1" and metric["mapping_status"] == "mapped"


def field(name, value, label=None, **extra):
    return dict(native_field=name, value=value, label=label or name, numeric_content=[], times=[], links=[], **extra)


def oversight_rows(*, heading="This report has 1 open recommendations. "):
    fields = [
        field("field-report-type", "Inspection / Evaluation"),
        field("field-report-number", "OIG-2025-EV1"),
        field("field-report-number-of-recs", "4", "Number of Recommendations"),
        {
            **field("field-report-date-issued", "Monday, July 27, 2026"),
            "times": [dict(datetime="2026-07-27T12:00:00Z")],
        },
        {**field("field-net-questioned-costs", "$1,001.25", "Questioned Costs"), "numeric_content": ["1001.25"]},
    ]
    headers = [
        "Recommendation Number",
        "Significant Recommendation",
        "Recommended Questioned Costs",
        "Recommended Funds for Better Use",
        "Additional Details",
        "",
    ]
    table = dict(
        rows=[
            dict(cells=[dict(text=h, attributes={}, tag="th") for h in headers]),
            dict(cells=[dict(text=t, attributes={}, tag="td") for t in ["1", "No", "$0", "$1,002.50", "", ""]]),
            dict(
                cells=[
                    dict(
                        text="The OIG recommends that the OCFO evaluate its workflows and processes.",
                        attributes=dict(colspan="6"),
                        tag="td",
                    )
                ]
            ),
        ]
    )
    native: list[dict[str, object]] = [
        dict(
            kind="oversight-report",
            report=dict(
                metadata=dict(title="Evaluation of the FEC’s DATA Act Compliance", fields=fields),
                source=dict(sha256=DIGEST),
            ),
        )
    ]
    native += [dict(kind="oversight-field", field=f, ordinal_in_report=i) for i, f in enumerate(fields)]
    native += [
        dict(
            kind="oversight-body",
            ordinal_in_report=0,
            body=dict(kind="recommendations", tables=[table], text=heading + "Recommendation Number"),
        )
    ]
    native += [
        dict(
            kind="oversight-asset",
            ordinal_in_report=0,
            asset=dict(
                url="https://www.oversight.gov/reports/report.pdf", role="declared_report_link", label="View Report"
            ),
        )
    ]
    return [
        row(n, i, url="https://www.oversight.gov/reports/evaluation-fecs-data-act-compliance")
        for i, n in enumerate(native)
    ]


def test_oversight_recommendation_grain_money_and_pdf_metadata():
    result = map_rows(oversight_rows())
    rec = result.tables["fec_oversight_recommendations"][0]
    assert rec["reported_status"] == "open" and rec["status_date_raw"] is None
    assert rec["responsible_party"] is None  # A name in prose is not an identified responsible-party field.
    assert rec["funds_for_better_use"] == Decimal("1002.50")
    assert rec["subrecord_pointer"] == "/body/tables/0/rows/1"
    assert result.tables["fec_report_metrics"][0]["value"] == 4  # Report total is not the retained open-row count.
    assert result.tables["fec_report_metrics"][1]["value"] == Decimal("1001.25")
    assert result.tables["fec_agency_report_documents"][0]["body_status"] == "deferred_pdf"
    assert result.tables["fec_agency_reports"][0]["publication_date"] == date(2026, 7, 27)


def test_recommendation_status_requires_matching_explicit_scope():
    result = map_rows(oversight_rows(heading="This report has 2 open recommendations. "))
    assert result.tables["fec_oversight_recommendations"][0]["reported_status"] is None


def test_changed_recommendation_table_is_explicitly_unsupported():
    rows = oversight_rows()
    n = json.loads(rows[-2]["metadata_json"])
    n["body"]["tables"][0]["rows"][0]["cells"][0]["text"] = "Different meaning"
    rows[-2]["metadata_json"] = json.dumps(n)
    result = map_rows(rows)
    assert not result.tables["fec_oversight_recommendations"]
    assert (
        next(
            d
            for d in result.tables["fec_agency_mapping_dispositions"]
            if d["source_record_id"] == rows[-2]["source_record_id"]
        )["disposition"]
        == "unsupported"
    )


def word_rows():
    elements = [
        element(WORD + "document"),
        element(WORD + "body", 0),
        element(WORD + "p", 1),
        element(WORD + "pPr", 2),
        element(WORD + "tabs", 3),
        element(WORD + "tab", 4, attributes={WORD + "pos": "180"}),
        element(WORD + "r", 2),
        element(WORD + "t", 6, "Fiscal "),
        element(WORD + "r", 2),
        element(WORD + "t", 8, "Year 2009"),
        element(WORD + "p", 1),
        element(WORD + "r", 10),
        element(WORD + "t", 11, "300,611.00"),
    ]
    native: list[dict[str, object]] = [
        dict(kind="word-report", report=dict(metadata=dict(representation="word-flat-opc"), source=dict(sha256=DIGEST)))
    ]
    native += [dict(kind="word-element", ordinal_in_report=i, element=e) for i, e in enumerate(elements)]
    native += [
        dict(
            kind="word-body",
            ordinal_in_report=i,
            body=dict(element=ix, text=elements[ix]["text"], tag=elements[ix]["tag"]),
        )
        for i, ix in enumerate([5, 7, 9, 12])
    ]
    return [row(n, i) for i, n in enumerate(native)]


def test_word_runs_reconstruct_paragraphs_and_never_become_metrics():
    result = map_rows(word_rows())
    assert [r["text"] for r in result.tables["fec_agency_report_text"]] == ["Fiscal Year 2009", "300,611.00"]
    assert result.tables["fec_agency_reports"][0]["fiscal_year"] == 2009
    assert not result.tables["fec_report_metrics"]
    assert len(result.tables["fec_agency_mapping_dispositions"]) == len(word_rows())
    assert (
        next(
            d
            for d in result.tables["fec_agency_mapping_dispositions"]
            if d["source_record_id"] == word_rows()[-4]["source_record_id"]
        )["disposition"]
        == "structural_nondata"
    )


def test_evidence_resolves_source_records_and_is_deterministic():
    rows = foia_rows()
    result = map_rows(rows)
    assert map_rows(list(reversed(rows))) == result
    source = {r["source_record_id"] for r in rows}
    assert all(e["source_record_id"] in source and e["witness_generation_pin"] == PIN for e in result.evidence)
    for table, records in result.tables.items():
        if table == "fec_agency_mapping_dispositions":
            continue
        assert {r["record_id"] for r in records} == {
            e["target_record_id"] for e in result.evidence if e["target_table"] == table and e["role"] == "primary"
        }


@pytest.mark.parametrize(
    "mutation", ["collection", "digest", "locator", "duplicate", "missing_report", "missing_parent"]
)
def test_invalid_membership_or_tree_refuses_mapping(mutation):
    rows = deepcopy(foia_rows())
    if mutation == "collection":
        rows[-1]["collection_id"] = "other"
    elif mutation == "digest":
        rows[-1]["source_sha256"] = PIN
    elif mutation == "locator":
        rows[0]["source_locator_json"] = json.dumps(
            dict(collection_id="other", source_record_id=rows[0]["source_record_id"])
        )
    elif mutation == "duplicate":
        rows.append(rows[-1])
    elif mutation == "missing_report":
        rows = rows[1:]
    else:
        n = json.loads(rows[-1]["metadata_json"])
        n["element"]["parent"] = 999999
        rows[-1]["metadata_json"] = json.dumps(n)
    with pytest.raises(ValueError):
        map_rows(rows)


@pytest.mark.parametrize(
    "text,status", [("I.", "reported"), (" \t\n\u00a0", "whitespace_only"), ("", "source_empty"), (None, "source_null")]
)
def test_agency_text_primary_rows_keep_blank_diagnostics_and_built_order(text, status):
    rows = oversight_rows()
    native = dict(
        kind="oversight-body",
        ordinal_in_report=1,
        body=dict(kind="report_description", text=text, source_position=dict(line=18, column=4)),
    )
    rows.append(row(native, 19, url="https://www.oversight.gov/reports/test"))
    original = rows[-1]["metadata_json"]
    mapped = map_rows(rows)
    result = next(
        r for r in mapped.tables["fec_agency_report_text"] if r["source_record_id"] == rows[-1]["source_record_id"]
    )
    assert result["text"] == text and result["text_status"] == status
    assert (result["source_ordinal"], result["native_line"], result["native_column"]) == (19, 18, 4)
    assert result["native_element"] is None
    assert "native_location_json" not in result
    assert rows[-1]["metadata_json"] == original
    assert any(
        e["target_record_id"] == result["record_id"] and e["source_record_id"] == rows[-1]["source_record_id"]
        for e in mapped.evidence
    )
