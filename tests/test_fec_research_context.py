"""Retained native context is evidence, never an invented source record."""

import json

import pyarrow as pa
import pytest

from spicy_regs.transforms.fec_research_context import SCHEMAS, map_research_context

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


def documentcloud_context():
    native = dict(id="4433332", title="FEC vs. John Swallow ruling", original_extension="pdf")
    return context(
        dict(status="literal-json", facts=[dict(kind="json-field", field="results", value=[native])]),
        url="https://api.www.documentcloud.org/api/documents/search/?q=Federal+Election+Commission",
    )


@pytest.mark.parametrize("make", [csv_context, documentcloud_context])
def test_retired_csv_and_documentcloud_contexts_emit_no_table_rows(make):
    """Owner decision 2026-10-30 (ss-j4vz B10): both tables are dropped; the context keeps a disposition."""
    result = run(make())
    assert not {"fec_retained_csv_observations", "fec_research_document_observations"} & set(SCHEMAS)
    assert [t for t, rows in result.tables.items() if rows] == ["fec_research_context_dispositions"]
    assert result.tables["fec_research_context_dispositions"][0]["mapping_status"] == "retired_output"
    assert not result.evidence


@pytest.mark.parametrize('refusal, shape, expected', [
    ('provider refused', 'results_empty', 'refused'),
    (None, 'results_empty', 'empty_payload_not_verified_success'),
    ('', 'docs_present', 'captured_payload_not_verified_success'),
    (None, 'other_native_payload', 'captured_payload_not_verified_success'),
    (None, None, 'unavailable_retained_payload_shape'),
])
def test_shared_response_disposition_never_qualifies_empty_results(refusal, shape, expected):
    from spicy_regs.transforms.fec_context_shape import response_capture_disposition
    assert response_capture_disposition(refusal,shape)==expected
    with pytest.raises(ValueError, match='native text'):
        response_capture_disposition(False,shape)


def event(kind, name=None, text=None, attrs=None):
    return dict(kind=kind, name=name, text=text, attributes=attrs or [])


@pytest.mark.parametrize(
    "cid, url, table",
    [
        ("retained-ie-workbook-0001", "https://www.fec.gov/resources/ie.xlsx", "fec_historical_ie_statistics"),
        ("retained-context", "https://www.fec.gov/meetings/", "fec_research_meeting_observations"),
        ("rss-0", "https://efilingapps.fec.gov/rss/generate?preDefinedFilingType=ALL", "fec_research_filing_feed_items"),
        ("retained-context", "https://www.fec.gov/legal-resources/court-cases/fieger-v-fec/", "fec_research_source_pages"),
    ],
)
def test_tables_rebuilt_from_originals_never_come_from_receipt_context(cid, url, table):
    """Owner decision 2026-10-30 (ss-j4vz B7-B9): only a disposition comes from the context."""
    facts = [event("start", "h2"), event("text", text="Executive sessions"), event("end", "h2")]
    result = run(context(dict(status="literal-html-events", facts=facts), cid=cid, url=url))
    assert table not in SCHEMAS
    assert [t for t, rows in result.tables.items() if rows] == ["fec_research_context_dispositions"]
    disposition = result.tables["fec_research_context_dispositions"][0]
    assert disposition["mapping_status"] == "rebuilt_from_original" and table in disposition["mapping_reason"]
    assert not result.evidence


def test_filing_definitions_stay_context_and_are_not_economic_data():
    result = run(
        context(
            dict(status="literal-native-observations", facts=[dict(kind="cell", value=99999)]),
            cid="filing-format-example",
        )
    )
    assert [t for t, rows in result.tables.items() if rows] == ["fec_research_context_dispositions"]
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


def test_html_bytes_require_source_native_reader_and_are_not_parsed_here():
    result = run(
        context(
            dict(status="literal-html-bytes", facts=[dict(kind="html-source-bytes", data="not decoded")]),
            url="https://www.fec.gov/legal-resources/court-cases/example/",
        )
    )
    assert result.tables["fec_research_context_dispositions"][0]["mapping_status"] == "requires_native_reader"
    assert [t for t, rows in result.tables.items() if rows] == ["fec_research_context_dispositions"]


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
    assert [t for t, rows in result.tables.items() if rows] == ["fec_research_context_dispositions"]
