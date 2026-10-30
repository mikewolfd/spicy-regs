"""Native field rules from observed publisher originals, plus refusal boundaries.

Real originals were checksum-verified before selecting these minimal shapes:
LDA 00004dc257f84af0785372d0a2866a4c3a239794841c5a7b8fdad521e10ff4e0;
GovInfo b959272c74975682bf95f607ab3d63890dd44451c9a9918d94831e3946b3c168;
MODS 00003df044b295a497c86fc50dda487e0d800771d36f8bb2be4e566fbfecc44d;
House vote 000522157fb96e9b9f995b7afe544945745efc861c6bb5c42ddd6a97a93f5483;
Senate vote 000d2a9f096bacd0fee06bc6c90cde0ed80e951d7077c7d088a4572a309d29e1;
FR 00f1793ade3131a048e06a435c84312bfe8ef900044e030d779082232a3419e8;
meeting 2be5f62c96c8441d5454816c1537d74c5ab7387838f795cc2afd61b7dd598da5.
Tests below are explicit small fixtures, not copies or claims of archive coverage.
"""
import json

import pytest

from spicy_regs.source_catalog.raw_content import extract_metadata


def extract(value, family=None):
    return extract_metadata(f"family:{family}" if family else None, json.dumps(value).encode())


def periods(result):
    return {(p["kind"], p["label"]) for p in result["periods"]}


def test_multi_record_lobbying_uses_each_native_year_not_page_url_or_capture_date():
    value = {"next": "https://lda.gov/api/v1/filings/?filing_year=2000", "results": [
        {"filing_uuid": "9d35fd06-51a8-4ba5-b563-dc0664c5681c", "filing_year": 2022,
         "filing_period": "year_end", "dt_posted": "2023-01-02T00:00:00Z"},
        {"filing_uuid": "5573588b-51a8-4ba5-b563-dc0664c5681c", "filing_year": 2020}]}
    result = extract(value)
    assert result["outcome"] == "ok"
    assert result["dataset_id"] == "family:lobbying-filings"
    assert result["native_id"] is None
    assert periods(result) == {("filing-year", "2022"), ("filing-year", "2020")}
    assert [p["field"] for p in result["periods"]] == ["/results/0/filing_year", "/results/1/filing_year"]
    assert all(p["origin"] == "content" and p["rule"] for p in result["periods"])


def test_bill_package_matches_primary_fields_and_ignores_related_bill():
    value = {"packageId": "BILLS-112hr2626ih", "congress": "112", "billNumber": "2626",
             "billType": "hr", "billVersion": "ih", "dateIssued": "2011-07-22",
             "related": {"packageId": "BILLSTATUS-119s1", "congress": 119}}
    result = extract(value)
    assert (result["native_id"], result["source_version"]) == ("BILLS-112hr2626ih", "ih")
    assert periods(result) == {("congress", "112"), ("issue-date", "2011-07-22")}
    value["congress"] = "111"
    assert extract(value)["outcome"] == "error"


def test_bill_list_does_not_claim_one_identity_or_version():
    result = extract({"bills": [{"congress": 119, "type": "HR", "number": 1},
                                {"congress": 118, "type": "S", "number": 2}]})
    assert result["native_id"] is None and result["source_version"] is None
    assert periods(result) == {("congress", "119"), ("congress", "118")}
    assert result["primary_records"] == 2
    repeated = extract({"bills": [{"congress": 119, "type": "HR", "number": 1}] * 2})
    assert repeated["native_id"] is None and repeated["primary_records"] == 2


def test_house_communication_and_committee_meeting_primary_dates():
    result = extract({"houseCommunication": {"chamber": "House", "congress": 119, "number": 200,
                      "communicationType": {"code": "EC"}, "congressionalRecordDate": "2025-01-28",
                      "updateDate": "2026-04-19"}, "request": {"congress": 118}})
    assert result["native_id"] == "119:ec:200"
    assert periods(result) == {("congress", "119"), ("congressional-record-date", "2025-01-28")}
    result = extract({"committeeMeeting": {"congress": 119, "eventId": "338384", "chamber": "Senate",
                      "date": "2026-05-12T19:00:00Z", "updateDate": "2026-05-11T22:08:26Z"}})
    assert result["native_id"] == "119:senate:338384"
    assert periods(result) == {("congress", "119"), ("meeting-date", "2026-05-12")}


def test_fr_thousand_records_keep_each_locator_without_completeness():
    result = extract({"count": 90000, "results": [{"document_number": f"2014-{i}", "type": "Notice",
                      "publication_date": "2014-11-03"} for i in range(1000)]})
    assert result["outcome"] == "ok" and result["native_id"] is None
    assert len(result["periods"]) == 1
    assert result["periods"][0]["field"] == "/results/0/publication_date"
    dates = [e for e in result["evidence"] if e["target"] == "period"]
    assert len(dates) == 1000 and dates[-1]["field"] == "/results/999/publication_date"
    assert "no completeness" in result["scope"]
    assert periods(result) == {("publication-date", "2014-11-03")}


def test_fr_correction_number_is_a_native_identity():
    # Observed in hash 08ef0c2ed57493e59d3d69c0784b25719c738c709b5affbf67b6fb9af80d0abe.
    result = extract({"results": [{"document_number": "C1-2024-22635", "type": "Correction",
                                   "publication_date": "2024-10-17"}]})
    assert result["outcome"] == "ok" and result["native_id"] == "C1-2024-22635"
    assert periods(result) == {("publication-date", "2024-10-17")}


def test_fr_uses_spicydocs_identity_rules_for_retained_split_documents():
    # Retained fr_1996.json, sha256:f7ed2d184707336a642a339ff4a66a2b04f12fef17b07957bf9bd049bddbdc91,
    # /results/475. The split suffix is literal publisher identity.
    result = extract({'results': [{'document_number': '96-32845-2', 'type': 'Notice',
                                  'publication_date': '1996-12-26'}]})
    assert result['outcome'] == 'ok' and result['native_id'] == '96-32845-2'
    assert periods(result) == {('publication-date', '1996-12-26')}


def test_fr_reader_dependency_tracks_the_source_owned_implementation():
    from pathlib import Path
    from spicy_docs.reading import xml_tree
    from spicy_docs.sources.federal_register import issues, native
    from spicy_docs.sources.govinfo import mods
    from spicy_regs.source_catalog.raw_content import reader_inputs
    assert {Path(module.__file__) for module in (native, issues, mods, xml_tree)} <= set(
        reader_inputs('family:federal-register'))


def test_fr_projected_results_use_existing_native_reader_with_explicit_family():
    # fr_OSH_quote.json, sha256:00e65962f7f30961e53ef2cade93a1e9f5608828e3ebe802bd3d7a42b40e782d,
    # retains these fields but omits the optional type. Signing is not publication.
    value = {'results': [{'document_number': '2026-15672', 'publication_date': '2026-08-03',
                          'signing_date': '2026-07-31'},
                         {'document_number': '2024-29614', 'publication_date': '2024-12-19'}]}
    result = extract(value, 'federal-register')
    assert result['outcome'] == 'ok' and result['primary_records'] == 2
    assert periods(result) == {('publication-date', '2026-08-03'), ('publication-date', '2024-12-19')}
    assert [p['field'] for p in result['periods']] == ['/results/0/publication_date',
                                                      '/results/1/publication_date']
    assert extract(value)['outcome'] == 'unsupported'
    assert extract(value, 'bill-family')['outcome'] == 'unsupported'


def test_fr_projected_results_refuse_invalid_or_partial_dates_without_partial_metadata():
    value = {'results': [{'document_number': '2026-15672', 'publication_date': '2026-08-03'},
                         {'document_number': '2026-15673', 'publication_date': '2026-02-30'}]}
    result = extract(value, 'federal-register')
    assert result['outcome'] == 'error' and result['periods'] == []
    del value['results'][1]['publication_date']
    assert extract(value, 'federal-register')['outcome'] == 'unsupported'


def fr_issue_mods(package='FR-1936-12-31', issued='1936-12-31'):
    # Minimal shape from verified FR-1936-12-31_mods.xml,
    # sha256:a6be5b0dc77b878eb728819ea12828678baca0ade7073cd7bd7a8dd5368a6c56.
    return (f'<mods xmlns="http://www.loc.gov/mods/v3"><originInfo><dateIssued>{issued}</dateIssued>'
            f'</originInfo><extension><collectionCode>FR</collectionCode><accessId>{package}</accessId>'
            '</extension></mods>').encode()


def test_fr_mods_reuses_issue_reader_and_keeps_metadata_only_scope():
    result = extract_metadata('family:federal-register', fr_issue_mods())
    assert result['outcome'] == 'ok' and result['native_id'] == 'FR-1936-12-31'
    assert result['primary_records'] == 1
    assert periods(result) == {('publication-date', '1936-12-31')}
    assert result['periods'][0]['field'] == '/mods/originInfo/dateIssued'
    assert 'metadata only' in result['scope'] and 'no rendition acquisition' in result['scope']
    assert extract_metadata(None, fr_issue_mods())['outcome'] == 'ok'
    assert extract_metadata('family:bill-family', fr_issue_mods())['outcome'] == 'unsupported'


@pytest.mark.parametrize('package,issued', [('FR-1936-12-31', '1936-12-30'),
                                          ('FR-1936-02-30', '1936-02-30'),
                                          ('FR-1936-12-31', '1936'),
                                          ('FR-1936-12-31', '1936-12-31/1937-01-01')])
def test_fr_issue_mods_refuses_date_or_package_disagreement(package, issued):
    result = extract_metadata('family:federal-register', fr_issue_mods(package, issued))
    assert result['outcome'] == 'error' and result['periods'] == []


def test_fr_granule_mods_does_not_promote_host_issue_date_to_primary_record():
    # Both retained granule MODS records have a host date and no primary dateIssued.
    raw = b'''<mods xmlns="http://www.loc.gov/mods/v3"><extension>
      <collectionCode>FR</collectionCode><accessId>94-8046-Filed</accessId></extension>
      <relatedItem type="host"><originInfo><dateIssued>1994-04-05</dateIssued></originInfo>
      <extension><accessId>FR-1994-04-05</accessId></extension></relatedItem></mods>'''
    result = extract_metadata('family:federal-register', raw)
    assert result['outcome'] == 'unsupported' and result['periods'] == []


@pytest.mark.parametrize("bad", [True, 119.0, None, "", "119.0", "-1", 0])
def test_invalid_congress_types_do_not_create_periods(bad):
    result = extract({"bill": {"congress": bad, "type": "HR", "number": 1}})
    assert result["outcome"] == "error" and not result["periods"]


@pytest.mark.parametrize("bad", ["2023-02-29", "2020-13-01", "2020-01-01junk", True,
                                  "2020-01-01T30:00:00Z", "2020-01-01T00:00:00"])
def test_invalid_native_dates_are_explicit_errors(bad):
    result = extract({"data": {"type": "comments", "id": "ACF-1", "attributes": {"postedDate": bad}}})
    assert result["outcome"] == "error" and not result["periods"]


def test_comment_uses_posted_date_not_id_or_modified_date():
    result = extract({"data": {"type": "comments", "id": "ACF-2023-0010-2523", "attributes": {
        "postedDate": "2024-02-29T23:30:00-04:00", "modifyDate": "2026-01-01T00:00:00Z"}},
        "included": [{"type": "comments", "id": "other", "attributes": {"postedDate": "1999-01-01"}}]})
    assert result["native_id"] == "ACF-2023-0010-2523"
    assert periods(result) == {("record-posted-date", "2024-02-29")}


def test_explicit_dataset_conflict_never_changes_mapping():
    result = extract({"bill": {"congress": 119, "type": "HR", "number": 1}}, "comments-mirror")
    assert result["outcome"] == "unsupported" and result["dataset_id"] is None
    assert not result["periods"]


def test_receipt_json_and_journal_are_not_source_periods_even_for_unknown_family():
    receipt = {"body_path": "capture.body", "byte_size": 1773, "requested_url": "https://api.congress.gov/v3/committee-meeting/119/senate/338384",
               "observed_at": "2026-09-22T17:42:34.394125Z", "sha256": "sha256:" + "a" * 64, "status_code": 200}
    for raw in [json.dumps(receipt).encode(), (json.dumps(receipt) + "\n" + json.dumps(receipt)).encode()]:
        result = extract_metadata("family:unknown", raw)
        assert result["role"] == "acquisition-receipt"
        assert result["outcome"] == "ok" and result["dataset_id"] is None
        assert not result["periods"] and result["native_id"] is None
    assert "role" not in extract({"sha256": "a" * 64, "bytes": 1, "title": "source document"})


@pytest.mark.parametrize("value", [{"error": "rate limited", "bill": {"congress": 119}},
                                   {"results": []}, {"count": 1, "results": [{"date": "2026-01-01"}]},
                                   {"capture": {"bill": {"congress": 119, "type": "HR", "number": 1}}}])
def test_error_or_unrecognized_json_is_unsupported(value):
    result = extract(value)
    assert result["outcome"] == "unsupported" and not result["periods"]


@pytest.mark.parametrize("raw", [b'{"bill":', b'{"bill":{"congress":119,"congress":118}}', b'{"bill":{"congress":NaN}}'])
def test_malformed_or_ambiguous_json_reports_error(raw):
    assert extract_metadata("family:bill-family", raw)["outcome"] == "error"


def test_bill_xml_accepts_external_dtd_name_without_fetching_and_ignores_references():
    raw = b'''<?xml version="1.0"?><!DOCTYPE bill PUBLIC "-//US Congress//DTDs/bill.dtd//EN" "file:///does/not/exist">
      <bill bill-stage="Introduced-in-Senate"><form><congress>109th CONGRESS</congress><legis-num>S. 1642</legis-num></form>
      <legis-body><congress>119</congress><legis-num>H.R. 2</legis-num></legis-body></bill>'''
    result = extract_metadata(None, raw)
    assert (result["native_id"], result["source_version"]) == ("s1642-109", "Introduced-in-Senate")
    assert periods(result) == {("congress", "109")}


def test_resolution_and_bill_status_xml():
    result = extract_metadata(None, b'<resolution><form><congress>111th CONGRESS</congress><legis-num>H. RES. 278</legis-num></form></resolution>')
    assert result["native_id"] == "hres278-111"
    result = extract_metadata(None, b'<billStatus version="3"><bill><congress>119</congress><type>HR</type><number>2</number></bill></billStatus>')
    assert result["native_id"] == "hr2-119" and result["source_version"] is None


def test_mods_primary_extension_and_actual_field_locators():
    raw = b'''<mods xmlns="http://www.loc.gov/mods/v3"><extension><collectionCode>BILLS</collectionCode></extension>
      <extension><accessId>BILLS-111hr3671rfs</accessId><docClass>hr</docClass><congress>111</congress>
      <billNumber>3671</billNumber><billVersion>rfs</billVersion><law congress="109" number="54"/></extension></mods>'''
    result = extract_metadata(None, raw)
    assert result["native_id"] == "BILLS-111hr3671rfs" and result["source_version"] == "rfs"
    assert periods(result) == {("congress", "111")}
    assert any(e["field"] == "/mods/extension[2]/accessId" for e in result["evidence"])


def test_both_vote_xml_formats_use_primary_vote_not_referenced_bill():
    house = b'''<rollcall-vote><vote-metadata><congress>105</congress><session>2nd</session><rollcall-num>164</rollcall-num>
      <legis-num>H R 3809</legis-num><action-date>19-May-1998</action-date></vote-metadata></rollcall-vote>'''
    senate = b'''<roll_call_vote><congress>111</congress><session>1</session><vote_number>116</vote_number>
      <vote_date>March 31, 2009,  02:33 PM</vote_date><modify_date>April 1, 2009</modify_date></roll_call_vote>'''
    result = extract_metadata(None, house)
    assert result["native_id"] == "105:2:house:164"
    assert periods(result) == {("congress", "105"), ("vote-date", "1998-05-19")}
    result = extract_metadata(None, senate)
    assert result["native_id"] == "111:1:senate:116"
    assert periods(result) == {("congress", "111"), ("vote-date", "2009-03-31")}


@pytest.mark.parametrize("raw", [
    b'<!DOCTYPE bill [<!ENTITY leak SYSTEM "file:///etc/passwd">]><bill>&leak;</bill>',
    b'<!DOCTYPE bill [<!ENTITY a "ha"><!ENTITY b "&a;&a;&a;">]><bill>&b;</bill>',
    b'<bill><form><congress>119th CONGRESS</congress><congress>118th CONGRESS</congress><legis-num>H.R. 2</legis-num></form></bill>',
    b'<bill>' + b'<x>' * 130 + b'</x>' * 130 + b'</bill>',
])
def test_xml_unsafe_or_ambiguous_sources_fail_closed(raw):
    result = extract_metadata(None, raw)
    assert result["outcome"] == "error" and not result["periods"]


@pytest.mark.parametrize("raw", [b'%PDF-1.5', b'<html><head><title>Federal Register, 2026-01-01</title></head>', b'WEBVTT', b''])
def test_opaque_files_are_not_mined_for_dates(raw):
    assert extract_metadata(None, raw)["outcome"] == "unsupported"


def test_register_html_uses_only_the_exact_issue_title_date():
    raw = (b'<html><head><title>Federal Register, Volume 61 Issue 216 '
           b'(Wednesday, November 6, 1996)</title></head><body>2026-10-15</body></html>')
    result = extract_metadata("family:federal-register", raw)
    assert result["outcome"] == "ok"
    assert result["dataset_id"] == "family:federal-register"
    assert periods(result) == {("publication-date", "1996-11-06")}
    assert result["periods"][0]["field"] == "/title"
    assert extract_metadata(None, raw)["outcome"] == "unsupported"
    assert extract_metadata("family:bill-family", raw)["outcome"] == "unsupported"
    assert extract_metadata("family:federal-register", raw.replace(b"Wednesday", b"Tuesday"))["outcome"] == "unsupported"
    assert extract_metadata("family:federal-register", raw.replace(b"November 6", b"November 31"))["outcome"] == "unsupported"
