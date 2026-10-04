"""Native registration and filing identity must not be inferred from names/images."""

from datetime import date
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.transforms import fec_identity_observations as identity
from spicy_regs.transforms.fec_query import CollectionSelection

PIN = "sha256:" + "a" * 64
SELECTION = CollectionSelection("collection", PIN, PIN, "official-fec", 2024, "snapshot", PIN)


def source(native, path, profile="document"):
    return dict(
        collection_id="collection",
        source_record_id="row/1",
        source_sha256=PIN,
        source_locator_json=json.dumps(dict(collection_id="collection", source_record_id="row/1", ordinal=1)),
        observed_at="2026-09-12T13:43:15.058319+00:00",  # when FEC's Form1Filer_2024.csv was captured
        source_url="https://api.open.fec.gov" + path if "/v1/" in path else "https://www.fec.gov" + path,
        metadata_json=json.dumps(native),
        profile=profile,
    )


def registry(mapping, **values):
    native = {key: "" for _, key in mapping.text_fields}
    native[mapping.native_date_field] = "11-SEP-24"
    native.update(values)
    paths = {
        identity.FORM1: "/bulk-downloads/2024/Form1Filer_2024.csv",
        identity.FORM2: "/bulk-downloads/2024/Form2Filer_2024.csv",
        identity.LOBBYIST: "/bulk-downloads/data.fec.gov/lobbyist.csv",
        identity.QUALITY_NOTICE: "/bulk-downloads/data.fec.gov/FalseFictitiousFilings.csv",
    }
    return source(native, paths[mapping], profile="positional")


def api(native, wrapped=True, path="/v1/filings/"):
    return source(
        dict(kind="api-record-observation", metadata=native) if wrapped else native,
        path,
        profile="document" if wrapped else "filing",
    )


def test_statement_image_and_source_row_id_are_not_filing_identifiers():
    row, _ = identity.map_registry(
        registry(
            identity.FORM1,
            COMMITTEE_ID="C00000001",
            COMMITTEE_NAME="A",
            AFFILIATED_COMMITTEE_NAME="NONE",
            BEGIN_IMAGE_NUMBER="202412319740048774",
            RECEIPT_DATE="31-DEC-24",
        ),
        SELECTION,
        identity.FORM1,
    )
    assert row["committee_id"] == "C00000001" and row["committee_id_status"] == "source_id_shape"
    assert "filing_key" not in row and "filing_link_status" not in row
    assert row["beginning_image_number"] == "202412319740048774" and row["affiliation_status"] == "source_reported_none"
    # The image number's leading digits are the day FEC received the filing (2024-12-31).
    assert row["receipt_date"] == date(2024, 12, 31) and row["receipt_date_status"] == "exact_with_year_bounds"
    assert row["current_record_status"] == "unqualified" and row["mapping_status"] == "mapped"


def test_form2_uses_report_year_not_election_cycle_for_receipt_date():
    row, _ = identity.map_registry(
        registry(
            identity.FORM2, CANDIDATE_ID="H4MI05180", RECEIPT_DATE="13-NOV-25", REPORT_YEAR="2025", ELECTION_YEAR="2024"
        ),
        SELECTION,
        identity.FORM2,
    )
    assert row["receipt_date"] == date(2025, 11, 13) and row["receipt_date_status"] == "exact_with_year_bounds"
    assert row["election_year"] == 2024 and row["source_cycle"] == 2024
    bad, _ = identity.map_registry(
        registry(identity.FORM2, RECEIPT_DATE="13-NOV-25", REPORT_YEAR="2024"), SELECTION, identity.FORM2
    )
    assert bad["receipt_date"] is None and bad["receipt_date_status"] == "outside_selected_year_bounds"


def test_lobbyist_blank_is_not_false_and_names_do_not_create_links():
    for value, expected, status in [
        ("Y", True, "reported_yes"),
        ("N", False, "reported_no"),
        ("", None, "source_empty"),
        (None, None, "source_null"),
    ]:
        row, _ = identity.map_registry(
            registry(identity.LOBBYIST, Is_Lobbyist=value, Committee_Name="Same name", Committee_Id=""),
            SELECTION,
            identity.LOBBYIST,
        )
        assert row["is_lobbyist"] is expected and row["lobbyist_status"] == status
        assert row["committee_id_status"] == "not_reported" and "filing_key" not in row
    bad, _ = identity.map_registry(registry(identity.LOBBYIST, Is_Lobbyist="UNKNOWN"), SELECTION, identity.LOBBYIST)
    assert bad["mapping_status"] == "partial" and bad["lobbyist_status"] == "unsupported_source_code"


def test_quality_notice_never_excludes_and_does_not_force_recent_century():
    row, _ = identity.map_registry(
        registry(identity.QUALITY_NOTICE, committee_id="P20000055", first_receipt_dt="31-DEC-76"),
        SELECTION,
        identity.QUALITY_NOTICE,
    )
    assert row["notice_scope"] == "source_listed_committee" and row["exclusion_status"] == "no_automatic_exclusion"
    # `76` is 1976, FEC's first cycle, not 2076: the bounds end at the year the list was captured.
    assert row["first_receipt_date"] == date(1976, 12, 31) and row["first_receipt_date_raw"] == "31-DEC-76"
    assert row["first_receipt_date_status"] == "exact_with_year_bounds"


def test_lobbyist_filed_date_is_typed_within_the_capture_year():
    row, _ = identity.map_registry(
        registry(
            identity.LOBBYIST,
            Committee_Id="C00141218",
            Is_Lobbyist="Y",
            Date_Filed="11-SEP-26",
            Link_Image="http://docquery.fec.gov/cgi-bin/fecimg/?_202609119904193523+0",
        ),
        SELECTION,
        identity.LOBBYIST,
    )
    assert row["filed_date"] == date(2026, 9, 11) and row["filed_date_status"] == "exact_with_year_bounds"


def test_registry_header_controls_are_not_statements_and_wrong_layout_refuses():
    row = registry(identity.FORM1)
    native = json.loads(row["metadata_json"])
    row["metadata_json"] = json.dumps(dict(kind="row", fields=list(native)))
    assert identity.map_registry(row, SELECTION, identity.FORM1) == (None, [])
    row["metadata_json"] = json.dumps(dict(kind="row", fields=["COMMITTEE_ID"]))
    with pytest.raises(ValueError, match="header differs"):
        identity.map_registry(row, SELECTION, identity.FORM1)
    with pytest.raises(ValueError, match="mapping differs"):
        identity.map_registry(registry(identity.FORM1), SELECTION, identity.FORM2)


def test_file_number_namespace_keeps_negative_paper_ids_and_rejects_guesses():
    assert identity.filing_key(-9529656).endswith(":openfec-file-number:-9529656")
    assert identity.filing_key(0) is None and identity.filing_key(None) is None
    for wrong in ["FEC-123", "1e4", "12.0", True, 12.0]:
        with pytest.raises(ValueError, match="exact signed integer"):
            identity.filing_key(wrong)
    tables, _ = identity.map_filing_metadata(
        api(dict(file_number=0, sub_id="3040720021023388732", beginning_image_number="123")), SELECTION
    )
    assert tables[identity.FILINGS][0]["filing_key"] is None


def test_candidate_filer_is_not_reported_committee_and_references_are_not_replacements():
    n = dict(
        file_number=1848391,
        sub_id="4110820241067704014",
        committee_id="H2AK01158",
        candidate_id="H2AK01158",
        form_type="F2",
        amendment_chain=[1577416, 1848391],
        previous_file_number=1577416,
        most_recent_file_number=1848391,
        receipt_date="2024-11-08T00:00:00",
    )
    tables, evidence = identity.map_filing_metadata(api(n), SELECTION)
    row = tables[identity.FILINGS][0]
    assert row["filer_entity_type"] == "candidate" and row["reporting_committee_id"] is None
    assert row["native_filer_id"] == "H2AK01158" and row["source_record_identifier"] == "4110820241067704014"
    assert row["filing_key"].endswith(":1848391") and row["receipt_date"] == date(2024, 11, 8)
    links = tables[identity.LINKS]
    assert len(links) == 4 and len({link["record_id"] for link in links}) == 4
    assert sum(link["reference_status"] == "self_reference" for link in links) == 2
    assert all(
        link["replacement_mode"] == "unknown" and link["target_resolution_status"] == "native_reference_unresolved"
        for link in links
    )
    assert len(evidence) == 5 and all(e["endpoint_kind"] == "source_record" for e in evidence)
    assert len({e["witness_locator_json"] for e in evidence}) == 1


def test_source_null_missing_and_empty_filing_fields_remain_distinct():
    tables, _ = identity.map_filing_metadata(api(dict(file_number=None, candidate_id="", pdf_url=None)), SELECTION)
    row = tables[identity.FILINGS][0]
    assert "native_field_states_json" not in row
    assert row["candidate_id"] == "" and row["source_record_identifier"] is None
    assert row["filing_link_status"] == "unresolved_no_file_number"
    assert row["receipt_date_status"] == "source_missing" and row["pdf_body_status"] == "source_null"


def test_api_controls_and_native_records_are_explicit():
    control = api({})
    control["metadata_json"] = json.dumps(dict(kind="api-response-field", field="results", value=[dict(file_number=1)]))
    assert identity.map_filing_metadata(control, SELECTION) == ({identity.FILINGS: [], identity.LINKS: []}, [])
    tables, _ = identity.map_filing_metadata(api(dict(file_number=1, form_type="F13"), wrapped=False), SELECTION)
    assert tables[identity.FILINGS][0]["source_pointer"] == ""
    with pytest.raises(ValueError, match="Unsupported filing metadata API route"):
        identity.map_filing_metadata(api({}, path="/v1/committees/"), SELECTION)
    with pytest.raises(ValueError, match="pinned collection"):
        identity.map_filing_metadata({**api(dict(file_number=1)), "source_sha256": "sha256:" + "b" * 64}, SELECTION)


def test_output_schemas_roundtrip(tmp_path):
    tables, evidence = identity.map_filing_metadata(
        api(
            dict(
                file_number=2011530,
                form_type="F99",
                amendment_chain=[2011530],
                pdf_url="https://docquery.fec.gov/x.pdf",
            )
        ),
        SELECTION,
    )
    for mapping in identity.REGISTRY_MAPPINGS:
        row, _ = identity.map_registry(registry(mapping), SELECTION, mapping)
        tables.setdefault(mapping.table, []).append(row)
    for name, rows in tables.items():
        arrow = pa.Table.from_pylist(rows, schema=identity.SCHEMAS[name])
        path = tmp_path / (name + ".parquet")
        pq.write_table(arrow, path)
        assert pq.read_table(path).equals(arrow)
    assert tables[identity.FILINGS][0]["pdf_body_status"] == "deferred_pdf"


def test_filing_flags_counts_and_years_are_typed_in_producer():
    tables, _ = identity.map_filing_metadata(
        api(
            dict(
                file_number=1,
                pages=23,
                report_year=2026,
                cycle=2026,
                election_year="2026",
                is_amended=True,
                most_recent=False,
            )
        ),
        SELECTION,
    )
    row = tables[identity.FILINGS][0]
    assert row["pages"] == 23 and row["report_year"] == 2026 and row["reported_cycle"] == 2026
    assert row["is_amended"] is True and row["most_recent"] is False
    bad, _ = identity.map_filing_metadata(
        api(dict(file_number=1, pages=2.5, report_year="26", is_amended=1)), SELECTION
    )
    invalid = bad[identity.FILINGS][0]
    assert (
        invalid["pages"] is None and invalid["pages_raw"] == "2.5" and invalid["pages_status"] == "unsupported_integer"
    )
    assert invalid["report_year"] is None and invalid["is_amended"] is None and invalid["mapping_status"] == "partial"
