"""Legal facts keep native namespaces, exact values and replayable evidence."""

from datetime import date
from decimal import Decimal
from html.parser import HTMLParser
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.transforms import fec_legal as legal
from spicy_regs.transforms.fec_query import CollectionSelection

PIN = "sha256:" + "a" * 64
SELECTED = CollectionSelection("legal", PIN, PIN, "official-fec", None, "snapshot", PIN)


def source(native, route="/v1/legal/search/", wrapped=True, record="result/0"):
    return dict(
        collection_id="legal",
        source_record_id=record,
        source_sha256=PIN,
        source_url="https://api.open.fec.gov" + route,
        profile="document" if wrapped else "legal",
        source_locator_json=json.dumps(dict(collection_id="legal", source_record_id=record, pointer="/murs/0")),
        metadata_json=json.dumps(dict(kind="api-record-observation", metadata=native) if wrapped else native),
    )


class CanonicalURL(HTMLParser):
    def __init__(self):
        super().__init__()
        self.url = None

    def handle_starttag(self, tag, attrs):
        values = dict(attrs)
        if tag == "link" and values.get("rel") == "canonical":
            self.url = values["href"]


@pytest.mark.parametrize(
    ("matter_id", "canonical_html"),
    [("8195", '<link rel="canonical" href="https://www.fec.gov/data/legal/matter-under-review/8195/">'),
     ("4322", '<link rel="canonical" href="https://www.fec.gov/data/legal/matter-under-review/4322/">'),
     ("4650", '<link rel="canonical" href="https://www.fec.gov/data/legal/matter-under-review/4650/">')],
)
def test_audited_mur_routes_match_publisher_canonical_html(matter_id, canonical_html):
    """Exact canonical tags from retained official HTML, 2026-10-02, line 23."""
    row = source(dict(type="murs", no=matter_id, url=f"/legal/matter-under-review/{matter_id}/"))
    original = dict(row)
    baseline, baseline_evidence = legal.map_legal(source(dict(type="murs", no=matter_id)), SELECTED)
    tables, evidence = legal.map_legal(row, SELECTED)
    parser = CanonicalURL()
    parser.feed(canonical_html)
    matter = tables[legal.MATTERS][0]
    assert parser.url is not None and matter["source_url"] == parser.url
    assert matter["source_url_status"] == "resolved_legacy_matter_route"
    assert matter["mapping_version"] == "fec-retained-legal/3"
    assert matter["record_id"] == baseline[legal.MATTERS][0]["record_id"]
    assert evidence == baseline_evidence and row == original


@pytest.mark.parametrize(
    ("native_type", "raw", "expected", "status"),
    [
        ("admin_fines", "/legal/administrative-fine/42/", "/data/legal/administrative-fine/42/",
         "resolved_legacy_matter_route"),
        ("adrs", "/legal/alternative-dispute-resolution/42/", "/data/legal/alternative-dispute-resolution/42/",
         "resolved_legacy_matter_route"),
        ("murs", "/legal/matter-under-review/42/?mur_type=archived#documents",
         "/data/legal/matter-under-review/42/?mur_type=archived#documents", "resolved_legacy_matter_route"),
        ("advisory_opinions", "/legal/advisory-opinions/42/", "/legal/advisory-opinions/42/",
         "resolved_relative_to_fec"),
        ("murs", "/data/legal/matter-under-review/42/?a=1#x", "/data/legal/matter-under-review/42/?a=1#x",
         "resolved_relative_to_fec"),
        ("murs", "/legal/matters-under-review/42/", "/legal/matters-under-review/42/", "resolved_relative_to_fec"),
        ("murs", "/legal/search/enforcement/", "/legal/search/enforcement/", "resolved_relative_to_fec"),
        ("murs", "/files/42.pdf", "/files/42.pdf", "resolved_relative_to_fec"),
        ("murs", "https://www.fec.gov/legal/matter-under-review/42/?q=1#x",
         "https://www.fec.gov/legal/matter-under-review/42/?q=1#x", "source_absolute"),
        ("murs", "https://example.org/42/", "https://example.org/42/", "source_absolute"),
        ("murs", "/legal/administrative-fine/42/", None, "unsupported_matter_route"),
        ("admin_fines", "/legal/matter-under-review/42/", None, "unsupported_matter_route"),
        ("murs", "/legal/matter-under-review/43/", None, "unsupported_matter_route"),
        ("murs", "/legal/matter-under-review/42/extra", None, "unsupported_matter_route"),
        ("murs", "/legal/matter-under-review/42", None, "unsupported_matter_route"),
        ("murs", "/legal/matter-under-review/%34%32/", None, "unsupported_matter_route"),
        ("murs", "https://[invalid/", None, "unsupported_url"),
        ("murs", "javascript:alert(1)", None, "unsupported_url"),
        ("murs", "https://user:pass@www.fec.gov/legal/42/", None, "unsupported_url"),
        ("murs", " /legal/matter-under-review/42/", None, "unsupported_url"),
    ],
)
def test_matter_route_selection_is_exact_and_preserves_other_links(native_type, raw, expected, status):
    native = dict(type=native_type, no="42", url=raw, documents=[dict(url="/files/42.pdf")])
    row = source(native)
    original = dict(row)
    tables, _ = legal.map_legal(row, SELECTED)
    matter = tables[legal.MATTERS][0]
    if expected is not None and expected.startswith("/"):
        expected = "https://www.fec.gov" + expected
    assert matter["source_url"] == expected and matter["source_url_status"] == status
    if status.startswith("unsupported"):
        assert matter["mapping_status"] == "partial"
        assert json.loads(matter["mapping_reason_json"])["source_url_status"] == status
    assert row == original
    document = tables[legal.DOCUMENTS][0]
    assert document["url"] == "https://www.fec.gov/files/42.pdf"
    assert document["body_status"] == "deferred_pdf" and document["content_sha256"] is None


@pytest.mark.parametrize(("fields", "raw", "status"), [
    ({}, None, "source_missing"), ({"url": None}, None, "source_null"), ({"url": ""}, "", "source_empty"),
])
def test_matter_urls_preserve_absence_without_manufacturing_links(fields, raw, status):
    tables, _ = legal.map_legal(source(dict(type="murs", no="42", **fields)), SELECTED)
    matter = tables[legal.MATTERS][0]
    assert matter["source_url"] == raw and matter["source_url_status"] == status
    assert matter["mapping_status"] == "mapped"


def test_controls_do_not_duplicate_results_and_unrecognized_records_refuse():
    row = source({"type": "murs", "no": "1"})
    row["metadata_json"] = json.dumps(dict(kind="api-response-field", field="murs", value=[{"no": "1"}]))
    tables, evidence = legal.map_legal(row, SELECTED)
    assert not any(tables.values()) and evidence == []
    assert legal.legal_source_kind(row) == "response_control"
    row["metadata_json"] = '{"kind":"invented"}'
    with pytest.raises(ValueError, match="source-owned legal record"):
        legal.map_legal(row, SELECTED)


def test_matter_namespaces_and_retained_observations_do_not_merge():
    rows = []
    for native_type, record in (("murs", "a"), ("adrs", "c"), ("murs", "b")):
        tables, _ = legal.map_legal(source(dict(type=native_type, no="42"), record=record), SELECTED)
        rows.append(tables[legal.MATTERS][0])
    assert rows[0]["matter_id"] != rows[1]["matter_id"]
    assert rows[0]["matter_id"] == rows[2]["matter_id"]
    assert len({r["record_id"] for r in rows}) == 3
    assert all(r["current_state_status"] == "unqualified" for r in rows)
    legacy, _ = legal.map_legal(source(dict(type="murs", no="42"), wrapped=False), SELECTED)
    assert legacy[legal.MATTERS][0]["matter_id"] == rows[0]["matter_id"]


def test_party_assertions_keep_occurrences_roles_and_unknown_identity():
    native = dict(
        type="murs",
        no="42",
        participants=[{"name": "Same", "role": "Treasurer"}],
        respondents=["Same", "Same"],
        complainants=None,
    )
    tables, evidence = legal.map_legal(source(native), SELECTED)
    parties = tables[legal.PARTIES]
    assert len(parties) == 3 and len({p["record_id"] for p in parties}) == 3
    assert [p["role"] for p in parties] == ["Treasurer", "Respondent", "Respondent"]
    assert all(p["native_entity_id"] is None and p["identity_resolution_status"] == "source_name_only" for p in parties)
    assert [p["source_pointer"] for p in parties] == [
        "/metadata/participants/0",
        "/metadata/respondents/0",
        "/metadata/respondents/1",
    ]
    assert len(evidence) == 4
    assert all(e["witness_locator_json"] == evidence[0]["witness_locator_json"] for e in evidence)
    states = json.loads(tables[legal.MATTERS][0]["collection_states_json"])
    assert states["complainants"] == "source_null" and states["dispositions"] == "source_missing"


def test_dates_and_exact_penalties_preserve_missing_null_empty_and_invalid():
    native = dict(
        type="admin_fines",
        no="42",
        name="Committee",
        committee_id="C00000001",
        final_determination_amount="9007199254740993.123456789",
        payment_amount=None,
        reason_to_believe_fine_amount="",
        open_date="2025-02-30",
        close_date=None,
        commission_votes=[{"action": "Voted", "vote_date": "2025-03-03T12:34:56"}],
        dispositions=[{"disposition": "Dismissed", "penalty": "0.0000000001"}],
    )
    tables, _ = legal.map_legal(source(native), SELECTED)
    matter = tables[legal.MATTERS][0]
    assert matter["final_determination_amount"] == Decimal("9007199254740993.123456789")
    assert matter["payment_amount_status"] == "source_null"
    assert matter["reason_to_believe_fine_amount_status"] == "source_empty"
    assert matter["treasury_referral_amount_status"] == "source_missing"
    events = {r["event_type"]: r for r in tables[legal.EVENTS]}
    assert events["open"]["event_date_status"] == "invalid_date" and events["open"]["mapping_status"] == "partial"
    assert events["close"]["event_date_status"] == "source_null"
    vote = events["commission_vote"]
    assert vote["event_date"] == date(2025, 3, 3) and vote["event_date_raw"] == "2025-03-03T12:34:56"
    disposition = events["disposition"]
    assert disposition["event_date_status"] == "source_missing" and disposition["amount_status"] == "excess_precision"
    assert disposition["amount"] is None and disposition["amount_raw"] == "0.0000000001"


def test_nested_rulemaking_documents_retain_parent_and_defer_pdf_bodies():
    child = dict(
        doc_id=9,
        doc_description="Comment",
        url="/files/9.pdf",
        doc_date="2024-01-01",
        doc_entities=[dict(name="A", role="Commenter")],
    )
    parent = dict(doc_id=8, url="/files/8.pdf", level_2_labels=[dict(level_2_docs=[child])])
    tables, evidence = legal.map_legal(
        source(
            dict(type="rulemakings", rm_no="2024-01", documents=[parent], key_documents=[child]),
            route="/v1/rulemaking/search/",
        ),
        SELECTED,
    )
    docs = tables[legal.DOCUMENTS]
    assert len(docs) == 3
    assert docs[1]["parent_document_record_id"] == docs[0]["record_id"]
    assert docs[1]["document_id"] == docs[2]["document_id"] and docs[1]["record_id"] != docs[2]["record_id"]
    assert all(d["body_status"] == "deferred_pdf" and d["content_sha256"] is None for d in docs)
    assert docs[1]["url"] == "https://www.fec.gov/files/9.pdf"
    assert tables[legal.PARTIES][0]["document_record_id"] == docs[1]["record_id"]
    assert all(e["endpoint_kind"] == "source_record" and e["witness_generation_pin"] == PIN for e in evidence)


def test_audit_no_findings_category_is_not_fabricated_finding_text():
    n = dict(
        audit_case_id="2286",
        audit_id=806,
        committee_id="C00811711",
        committee_name="Committee",
        cycle=2022,
        far_release_date="2024-06-25",
        link_to_report="https://www.fec.gov/reports/audit/",
        primary_category_list=[
            dict(
                primary_category_id="16",
                primary_category_name="No Findings or Issues/Not a Committee",
                sub_category_list=[dict(sub_category_id="0", sub_category_name="No Findings or Issues")],
            )
        ],
    )
    tables, _ = legal.map_legal(source(n, route="/v1/audit-case/"), SELECTED)
    finding = tables[legal.FINDINGS][0]
    assert finding["finding_kind"] == "reported_audit_category"
    assert finding["finding_status"] == "explicit_no_findings_or_issues"
    assert finding["text"] is None and finding["amount"] is None
    assert finding["supporting_document_record_id"] == tables[legal.DOCUMENTS][0]["record_id"]
    assert tables[legal.DOCUMENTS][0]["body_status"] == "not_requested"


def test_empty_null_and_missing_document_collections_remain_distinct():
    for value, expected in (([], "source_empty"), (None, "source_null")):
        tables, _ = legal.map_legal(source(dict(type="murs", no="1", documents=value)), SELECTED)
        assert not tables[legal.DOCUMENTS]
        assert json.loads(tables[legal.MATTERS][0]["collection_states_json"])["documents"] == expected
    tables, _ = legal.map_legal(source(dict(type="murs", no="1")), SELECTED)
    assert json.loads(tables[legal.MATTERS][0]["collection_states_json"])["documents"] == "source_missing"


def test_selection_and_identity_disagreement_refuse():
    row = source(dict(type="murs", no="42"))
    row["source_sha256"] = "sha256:" + "b" * 64
    with pytest.raises(ValueError, match="pinned selection"):
        legal.map_legal(row, SELECTED)
    row["source_sha256"] = PIN
    row["source_locator_json"] = '{"collection_id":"legal","source_record_id":"wrong"}'
    with pytest.raises(ValueError, match="locator differs"):
        legal.map_legal(row, SELECTED)
    with pytest.raises(ValueError, match="Unsupported retained legal API route"):
        legal.map_legal(source(dict(type="murs", no="42"), route="/v1/committees/"), SELECTED)
    with pytest.raises(ValueError, match="identifier differs from source route"):
        legal.map_legal(source(dict(type="murs", no="42"), route="/v1/legal/docs/murs/43"), SELECTED)
    with pytest.raises(ValueError, match="identifier must remain text or integer"):
        legal.map_legal(source(dict(type="murs", no=True)), SELECTED)


def test_arrow_roundtrip_keeps_decimal_and_day_types(tmp_path):
    tables, _ = legal.map_legal(
        source(
            dict(type="admin_fines", no="1", final_determination_amount="12.34", final_determination_date="2025-02-01")
        ),
        SELECTED,
    )
    for name, rows in tables.items():
        table = pa.Table.from_pylist(rows, schema=legal.SCHEMAS[name])
        path = tmp_path / (name + ".parquet")
        pq.write_table(table, path)
        assert pq.read_table(path).equals(table)
    assert legal.SCHEMAS[legal.MATTERS].field("final_determination_amount").type == pa.decimal128(38, 9)


def test_matter_scalars_are_built_once_with_types_and_original_evidence():
    native = dict(
        type="admin_fines",
        no="42",
        cycle="2024",
        is_pending=False,
        published_flg="true",
        is_open_for_comment=True,
        report_year="2023",
        report_type="Q3",
        challenge_outcome="Withdrawn",
        mur_type="archived",
        civil_penalty_payment_status="Paid",
        committee_type="N",
        citations={"regulations": [{"text": "11 CFR 104.3"}]},
        subject=[{"text": "Reporting", "children": [{"text": "Late"}]}],
        case_serial=42,
        rm_id=3472947,
        rm_number="REG 2024-08",
    )
    original = source(native)
    metadata_before = original["metadata_json"]
    tables, evidence = legal.map_legal(original, SELECTED)
    matter = tables[legal.MATTERS][0]
    assert matter["reported_cycle"] == 2024 and matter["report_year"] == 2023
    assert matter["is_pending"] is False and matter["is_published"] is True
    assert matter["is_open_for_comment"] is True
    assert matter["challenge_outcome"] == "Withdrawn"
    assert matter["rm_id"] == "3472947" and matter["rm_number"] == "REG 2024-08"
    assert matter["civil_penalty_payment_status"] == "Paid"
    facts = json.loads(matter["native_facts_json"])
    assert facts == {"citations": native["citations"], "subject": native["subject"]}
    assert {"pending_status", "published_status"}.isdisjoint(matter)
    assert pa.Table.from_pylist([matter], schema=legal.SCHEMAS[legal.MATTERS]).to_pylist() == [matter]
    assert original["metadata_json"] == metadata_before
    primary = next(e for e in evidence if e["target_record_id"] == matter["record_id"])
    assert primary["collection_id"] == original["collection_id"]
    assert primary["source_record_id"] == original["source_record_id"]
    assert json.loads(original["metadata_json"])["metadata"]["case_serial"] == 42


@pytest.mark.parametrize("year", ["2024.5", "1e3", True, 2**40])
def test_matter_bad_scalar_values_remain_uninterpreted_with_status(year):
    original = source(dict(type="murs", no="42", report_year=year, is_pending="yes", published_flg=None))
    tables, _ = legal.map_legal(original, SELECTED)
    matter = tables[legal.MATTERS][0]
    assert matter["report_year"] is None
    assert matter["report_year_status"] in {"unsupported_spelling", "overflow"}
    assert matter["is_pending"] is None and matter["is_pending_status"] == "unsupported_spelling"
    assert matter["is_published"] is None and matter["is_published_status"] == "source_null"
    assert matter["mapping_status"] == "partial"
