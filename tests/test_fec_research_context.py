"""Retained native context is evidence, never an invented source record."""

from copy import deepcopy
from datetime import date
from decimal import Decimal
import json

import pyarrow as pa
import pytest

from spicy_regs.transforms.fec_research_context import ROOT, SCHEMAS, map_filing_feed_contexts, map_research_context

PIN = "sha256:" + "a" * 64
CONTEXT_PIN = "sha256:" + "b" * 64
SOURCE_PIN = "sha256:" + "c" * 64


def context(parsing, *, cid="retained-context", url="https://www.fec.gov/retained/source"):
    return dict(
        collection_id=cid,
        collection_outcome_json=json.dumps(
            dict(
                receiverDisposition=dict(
                    callerContext=dict(
                        pin=dict(sha256=CONTEXT_PIN),
                        facts=dict(
                            source_capture=dict(url=url, sha256=SOURCE_PIN, observed_at="2026-09-11T00:00:00Z"),
                            parsing=parsing,
                        ),
                    )
                )
            )
        ),
    )


def run(row):
    result = map_research_context(row, PIN)
    outcome = json.loads(row["collection_outcome_json"])
    for table, rows in result.tables.items():
        assert all(set(r) == set(SCHEMAS[table].names) for r in rows)
        assert pa.Table.from_pylist(rows, schema=SCHEMAS[table]).to_pylist() == rows
    for e in result.evidence:
        assert e["endpoint_kind"] == "collection_context" and e["source_record_id"] is None
        assert e["witness_sha256"] == CONTEXT_PIN and e["witness_generation_pin"] == PIN
        value = outcome
        for key in e["context_pointer"].split("/")[1:]:
            value = value[int(key)] if isinstance(value, list) else value[key]
        assert value is not None
    return result


def cell(coordinate, value, *, numeric=None, formula=None, number_format="General"):
    children = []
    if formula is not None:
        children.append(dict(tag="f", text=formula))
    if numeric is not None:
        children.append(dict(tag="v", text=numeric))
    return dict(
        coordinate=coordinate,
        value=value,
        cached_value=value,
        data_type="f" if formula else "n" if numeric else "s",
        number_format=number_format,
        native_cell=dict(attributes=dict(r=coordinate), children=children),
    )


def workbook(*, table=1):
    header = ["ID #", "Committee/Individual", "Filer", "Date", "Amount", "Image"]
    if table == 2:
        header += ["Report", "Amendment"]
    header += ["Support / Oppose", "Candidate"]
    rows = [
        dict(row=1, cells=[cell("A1", f"Independent Expenditure Table {table}")]),
        dict(
            row=2,
            cells=[
                cell(
                    "A2",
                    "Committees Reporting Independent Expenditures"
                    if table == 1
                    else "Persons or Groups Reporting Independent Expenditures",
                )
            ],
        ),
        dict(row=3, cells=[cell("A3", "from January 1, 1975 through December 31, 1976")]),
        dict(
            row=5,
            cells=[
                cell(
                    "A5",
                    "Amendments have replaced original filings."
                    if table == 1
                    else "Includes original filings and all subsequent amendments; summing inflates totals.",
                )
            ],
        ),
        dict(row=6, cells=[cell(f"{chr(ord('A') + i)}6", name) for i, name in enumerate(header)]),
    ]
    values = [
        "C88000468",
        "DELEGATES FOR STEVENSON COMMITTEE",
        "Other",
        dict(type="datetime", iso8601="1976-04-09T00:00:00"),
        1438.01,
        76030271445,
    ]
    if table == 2:
        values += ["30D", "A"]
    values += ["Support", "P60001914"]
    data = [cell(f"{chr(ord('A') + i)}7", value) for i, value in enumerate(values)]
    # XML numeric spelling is the authority even if a decoded float is rounded.
    data[4] = cell("E7", 1438.0, numeric="1438.01", number_format='"$"#,##0')
    data[5] = cell("F7", 76030271445, numeric="76030271445")
    rows += [
        dict(row=7, cells=data),
        dict(
            row=8,
            cells=[
                cell("D8", "Total "),
                cell("E8", "=SUM(E7:E7)", numeric="1438.01", formula="SUM(E7:E7)", number_format='"$"#,##0'),
            ],
        ),
    ]
    return context(
        dict(
            status="complete-native-workbook",
            worksheets=[
                dict(
                    sheet_ordinal=0,
                    name=f"IE Table {table}",
                    worksheet_member="xl/worksheets/sheet1.xml",
                    worksheet_sha256=SOURCE_PIN,
                    rows=rows,
                )
            ],
        ),
        cid="retained-ie-workbook-example",
    )


def test_historical_workbook_grain_amount_and_cache_keep_source_semantics():
    result = run(workbook())
    detail, total = result.tables["fec_historical_ie_statistics"]
    assert detail["amount"] == Decimal("1438.01") and detail["amount_raw"] == "1438.01"
    assert detail["reported_date"] == date(1976, 4, 9)
    assert detail["period_start"] == date(1975, 1, 1) and detail["period_end"] == date(1976, 12, 31)
    assert detail["image_number"] == "76030271445"
    assert detail["current_total_status"] == "publisher_amendment_methodology_only"
    assert total["row_grain"] == "published_total" and total["amount_status"] == "source_formula_cache"
    assert total["amount_formula"] == "SUM(E7:E7)" and total["current_total_status"] == "not_qualified_source_total"
    assert detail["source_context_pointer"] == ROOT + "/parsing/worksheets/0/rows/5"
    assert len(result.evidence) == 12  # Each row points to itself and its five source definitions.


def test_originals_and_amendments_workbook_refuses_current_total_interpretation():
    detail = run(workbook(table=2)).tables["fec_historical_ie_statistics"][0]
    assert detail["current_total_status"] == "not_qualified_originals_and_amendments"
    assert detail["amendment_indicator"] == "A" and detail["report_type"] == "30D"
    assert detail["candidate_native_id"] == "P60001914"


def modify(row, callback):
    value = json.loads(row["collection_outcome_json"])
    callback(value["receiverDisposition"]["callerContext"]["facts"]["parsing"])
    row["collection_outcome_json"] = json.dumps(value)
    return row


def test_missing_formula_cache_is_not_evaluated_or_zero_filled():
    row = modify(
        workbook(),
        lambda p: p["worksheets"][0]["rows"][-1]["cells"][1]["native_cell"].update(
            children=[dict(tag="f", text="SUM(E7:E7)")]
        ),
    )
    total = run(row).tables["fec_historical_ie_statistics"][-1]
    assert total["amount"] is None and total["amount_status"] == "native_value_absent"
    assert total["amount_formula"] == "SUM(E7:E7)"


@pytest.mark.parametrize(
    "native_type, decoded_type, decoded",
    [
        ("s", "s", "Donation"),
        ("b", "b", True),
        ("str", "f", "=TEXT(E7,0)"),
        ("e", "f", "=1/0"),
        ("inlineStr", "s", "123"),
        ("d", "d", {"type": "datetime", "iso8601": "1976-04-09T00:00:00"}),
        ("n", "d", {"type": "datetime", "iso8601": "1976-04-09T00:00:00"}),
    ],
)
def test_nonnumeric_workbook_tokens_cannot_be_reported_as_money(native_type, decoded_type, decoded):
    def change(parsing):
        amount = parsing["worksheets"][0]["rows"][5]["cells"][4]
        amount.update(data_type=decoded_type, value=decoded)
        amount["native_cell"]["attributes"]["t"] = native_type
        amount["native_cell"]["children"] = [dict(tag="v", text="12"), dict(tag="f", text="source formula")]

    detail = run(modify(workbook(), change)).tables["fec_historical_ie_statistics"][0]
    assert detail["amount"] is None
    assert detail["amount_status"] == "unsupported_native_numeric_type"
    assert detail["amount_raw"] == "12" and detail["amount_formula"] == "source formula"


def test_changed_workbook_header_is_explicitly_unsupported():
    row = modify(workbook(), lambda p: p["worksheets"][0]["rows"][4]["cells"][4].update(value="New Amount Meaning"))
    result = run(row)
    assert not result.tables["fec_historical_ie_statistics"]
    assert result.tables["fec_research_context_dispositions"][0]["mapping_status"] == "unsupported"


def test_conflicting_cell_address_refuses():
    row = modify(workbook(), lambda p: p["worksheets"][0]["rows"][5]["cells"][1].update(coordinate="A7"))
    with pytest.raises(ValueError, match="worksheet address"):
        run(row)


def csv_context():
    header = [
        "committee_id",
        "committee_name",
        "contributor_name",
        "contribution_receipt_amount",
        "contribution_receipt_date",
        "transaction_id",
        "sub_id",
    ]
    records = [
        dict(fields=header),
        dict(
            fields=["C00401224", "ACTBLUE", "A REPORTED NAME", ".01", "2024-02-17 00:00:00", "SA11AI_123", "123456"],
            source=dict(sha256=SOURCE_PIN, byte_offset=1489, byte_length=700),
        ),
    ]
    return context(
        dict(
            complete_records=records,
            complete_record_count=2,
            unconfirmed_tail=dict(text="partial,unconfirmed", byte_offset=2189),
            parser_refusal="unexpected end of data",
        )
    )


def test_csv_maps_only_complete_rows_and_preserves_partial_scope():
    result = run(csv_context())
    rows = result.tables["fec_retained_csv_observations"]
    assert len(rows) == 1 and rows[0]["amount"] == Decimal(".01")
    assert rows[0]["reported_date"] == date(2024, 2, 17)
    assert rows[0]["coverage_status"] == "complete_records_of_truncated_capture"
    assert rows[0]["current_total_status"] == "unqualified_partial_capture"
    assert "partial,unconfirmed" not in rows[0]["native_fields_json"]


@pytest.mark.parametrize("change", ["duplicate_header", "short_row"])
def test_ambiguous_csv_prefix_is_not_mapped(change):
    row = csv_context()
    if change == "duplicate_header":
        row = modify(row, lambda p: p["complete_records"][0]["fields"].append("committee_id"))
    else:
        row = modify(row, lambda p: p["complete_records"][1]["fields"].pop())
    result = run(row)
    assert not result.tables["fec_retained_csv_observations"]
    assert result.tables["fec_research_context_dispositions"][0]["mapping_status"] == "unsupported"


def test_documentcloud_retains_third_party_search_observations_without_legal_joins():
    native = dict(
        id="4433332",
        title="FEC vs. John Swallow ruling",
        original_extension="pdf",
        canonical_url="https://www.documentcloud.org/documents/4433332/",
        created_at="2018-04-06T20:59:28.891Z",
    )
    row = context(
        dict(status="literal-json", facts=[dict(kind="json-field", field="results", value=[native, deepcopy(native)])]),
        url="https://api.www.documentcloud.org/api/documents/search/?q=Federal+Election+Commission",
    )
    docs = run(row).tables["fec_research_document_observations"]
    assert len(docs) == 2 and docs[0]["record_id"] != docs[1]["record_id"]
    assert all(r["body_status"] == "deferred_pdf" for r in docs)
    assert docs[0]["source_authority"] == "api.www.documentcloud.org"
    assert docs[0]["fec_relationship_status"] == "search_result_only_no_verified_matter_join"


def event(kind, name=None, text=None, attrs=None):
    return dict(kind=kind, name=name, text=text, attributes=attrs or [])


def meetings(title="February 11 and 13, 2025  (Canceled)"):
    facts = [
        event("start", "h2"),
        event("text", text="Executive sessions"),
        event("end", "h2"),
        event("start", "table"),
        event("start", "tr"),
        event("start", "td"),
        event("start", "a", attrs=[["href", "/updates/session/"]]),
        event("text", text=title),
        event("end", "a"),
        event("end", "td"),
        event("start", "td"),
        event("start", "a", attrs=[["href", "/notice.pdf"]]),
        event("text", text="Sunshine Act Notice"),
        event("end", "a"),
        event("end", "td"),
        event("end", "tr"),
        event("end", "table"),
    ]
    return context(dict(status="literal-native-observations", facts=facts), url="https://www.fec.gov/meetings/")


def test_meeting_listing_preserves_cancellation_multiple_dates_and_notice_deferral():
    meeting = run(meetings()).tables["fec_research_meeting_observations"][0]
    assert meeting["reported_status"] == "canceled" and meeting["meeting_type"] == "Executive sessions"
    assert json.loads(meeting["dates_json"]) == ["2025-02-11", "2025-02-13"]
    assert meeting["date_status"] == "source_listed_dates"
    assert json.loads(meeting["links_json"])[1]["body_status"] == "deferred_pdf"
    assert json.loads(meeting["links_json"])[0]["url"] == "https://www.fec.gov/updates/session/"


def test_meeting_date_range_is_not_collapsed_to_one_day():
    meeting = run(meetings("June 27-28, 2018 Public Hearing")).tables["fec_research_meeting_observations"][0]
    assert meeting["date_status"] == "source_date_range"
    assert json.loads(meeting["dates_json"]) == ["2018-06-27", "2018-06-28"]
    assert meeting["reported_status"] == "not_stated"


def test_truncated_meeting_table_refuses():
    row = modify(meetings(), lambda p: p["facts"].pop())
    with pytest.raises(ValueError, match="truncated"):
        run(row)


def test_filing_definitions_stay_context_and_are_not_economic_data():
    result = run(
        context(
            dict(status="literal-native-observations", facts=[dict(kind="cell", value=99999)]),
            cid="filing-format-example",
        )
    )
    assert not result.tables["fec_historical_ie_statistics"]
    assert result.tables["fec_research_context_dispositions"][0]["mapping_status"] == "definition_context"


def test_unsupported_api_native_context_remains_visible():
    result = run(
        context(
            dict(
                status="literal-native-observations",
                facts=[dict(value={"results": []}, profile_refusal="timestamp missing")],
            ),
            url="https://api.open.fec.gov/v1/filings/",
        )
    )
    assert result.tables["fec_research_context_dispositions"][0]["mapping_status"] == "mapped_refusal_outcome"
    outcome = result.tables["fec_research_response_outcomes"][0]
    assert outcome["observed_payload_records"] == 0
    assert outcome["outcome_status"] == "refused_native_observation_not_qualified_empty_success"


def test_court_page_retains_native_text_and_deferred_links_without_legal_identity():
    facts = [
        event("start", "h1"),
        event("text", text="Fieger v. FEC"),
        event("end", "h1"),
        event("start", "script"),
        event("text", text="ignore this script"),
        event("end", "script"),
        event("start", "p"),
        event("text", text="The court issued its opinion."),
        event("end", "p"),
        event("start", "a", attrs=[["href", "/opinion.pdf"]]),
        event("text", text="Court opinion"),
        event("end", "a"),
    ]
    result = run(
        context(
            dict(status="literal-html-events", facts=facts),
            url="https://www.fec.gov/legal-resources/court-cases/fieger-v-fec/",
        )
    )
    page = result.tables["fec_research_source_pages"][0]
    assert page["title"] == "Fieger v. FEC" and page["page_type"] == "official_court_case_reference"
    assert "The court issued its opinion." in page["text"] and "ignore this script" not in page["text"]
    assert json.loads(page["links_json"])[0]["body_status"] == "deferred_pdf"
    assert page["content_scope"] == "retained_native_context_part"


def test_html_bytes_require_source_native_reader_and_are_not_parsed_here():
    result = run(
        context(
            dict(status="literal-html-bytes", facts=[dict(kind="html-source-bytes", data="not decoded")]),
            url="https://www.fec.gov/legal-resources/court-cases/example/",
        )
    )
    assert result.tables["fec_research_context_dispositions"][0]["mapping_status"] == "requires_native_reader"
    assert not result.tables["fec_research_source_pages"]


def test_filing_software_vendor_directory_is_reference_documentation():
    row = context(
        dict(
            status="literal-html-bytes",
            facts=[dict(kind="html-source-bytes", data="not decoded", event_reader_refusal="HTML must be UTF-8")],
        ),
        url="https://efilingapps.fec.gov/registration/softwarelogs.htm",
    )
    result = run(row)
    disposition = result.tables["fec_research_context_dispositions"][0]
    assert disposition["mapping_status"] == "reference_documentation"
    assert disposition["context_sha256"] == CONTEXT_PIN
    assert "vendor directory" in disposition["mapping_reason"]
    assert not result.tables["fec_research_source_pages"]
    assert not result.tables["fec_research_filing_feed_items"]


def test_rss_item_across_context_parts_preserves_fields_and_both_witnesses():
    events = [
        event("start", "rss"),
        event("start", "item"),
        event("start", "title"),
        event("text", text="A committee filed F1A"),
        event("end", "title"),
        event("start", "guid"),
        event("text", text="FEC-2011478"),
        event("end", "guid"),
        event("start", "pubDate"),
        event("text", text="Fri, 11 Sep 2026 12:00:00 GMT"),
        event("end", "pubDate"),
        event("end", "item"),
        event("end", "rss"),
    ]
    for i, e in enumerate(events):
        e["byte_start"] = i * 20
    first = context(
        dict(status="literal-xml-events", facts=events[:6]),
        cid="rss-0",
        url="https://efilingapps.fec.gov/rss/generate?preDefinedFilingType=ALL",
    )
    second = context(
        dict(status="literal-xml-events", facts=events[6:]),
        cid="rss-1",
        url="https://efilingapps.fec.gov/rss/generate?preDefinedFilingType=ALL",
    )
    result = map_filing_feed_contexts([second, first], PIN)
    item = result.tables["fec_research_filing_feed_items"][0]
    assert item["guid"] == "FEC-2011478" and item["title"] == "A committee filed F1A"
    assert set(json.loads(item["source_parts_json"])) == {"rss-0", "rss-1"}
    assert {e["collection_id"] for e in result.evidence} == {"rss-0", "rss-1"}
    assert item["current_record_status"] == "source_feed_observation_only"
    assert pa.Table.from_pylist([item], schema=SCHEMAS["fec_research_filing_feed_items"]).to_pylist() == [item]
    with pytest.raises(ValueError, match="overlap"):
        map_filing_feed_contexts([first, first], PIN)
