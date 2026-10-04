"""Filing associations preserve rows and require native identity witnesses."""

from copy import deepcopy
import hashlib
import json

import duckdb
import pyarrow as pa
import pytest

from spicy_regs.relationship_views.fec_filing_associations import (
    financial_header_association_sql,
    financial_number_association_sql,
)
from spicy_regs.transforms.fec_filing_associations import (
    ASSOCIATION_SCHEMA,
    associate_filing_headers,
    associate_filing_numbers,
)
from spicy_regs.transforms.fec_identity_observations import filing_key
from spicy_regs.transforms.fec_typed_batch import typed_batch


def pin(text):
    return "sha256:" + hashlib.sha256(text.encode()).hexdigest()


GEN, SHA, EVIDENCE = pin("generation"), pin("original"), pin("definitions")
URL = "https://docquery.fec.gov/dcdev/posted/123.fec"
NAMESPACE = "fec-bulk-individual-contributions"


def filing(rid="metadata-1", number="123"):
    return dict(
        record_id=rid,
        identity_version="fec-typed-observation/1",
        mapping_version="fec-identity-observations/2",
        filing_key=filing_key(number),
        report_number=number,
        source_authority="official-fec",
        source_namespace="fec-openfec-file-number",
        native_filer_id="C00000001",
        form_type="F3",
        fec_url=URL,
    )


def financial(**changes):
    row = dict(
        record_id=pin("financial"),
        collection_id="c",
        source_record_id="native-row",
        source_sha256=SHA,
        source_authority="official-fec",
        selection_evidence_sha256=EVIDENCE,
        source_namespace=NAMESPACE,
        mapping_version="fec-bulk-individual-receipt/2",
        report_number="123",
        reporting_committee_id="C00000001",
        source_locator_json=json.dumps(dict(collection_id="c", source_record_id="native-row", member=None)),
    )
    return {**row, **changes}


def numbers(rows=None, filings=None, proofs=None):
    return associate_filing_numbers(
        rows or [financial()],
        table="fec_receipts",
        filings=filings if filings is not None else [filing()],
        source_generation_pin=GEN,
        namespace_evidence=proofs if proofs is not None else {NAMESPACE: EVIDENCE},
    )


def header_inputs():
    loc = dict(collection_id="c", source_record_id="header", sha256=SHA, ordinal=0, member=None)
    headers = [
        dict(
            collection_id="c",
            source_record_id="header",
            source_sha256=SHA,
            source_locator_json=json.dumps(loc, sort_keys=True, separators=(",", ":")),
            metadata_json=json.dumps(dict(kind="header", format_version="8.5", fields=["HDR", "FEC", "8.5", "123"])),
        )
    ]
    scopes = {"c": dict(capture=dict(requestUrl=URL, responseSha256=SHA), member=None)}
    return headers, scopes


def headers(hs=None, scopes=None, filings=None, authority="official-fec"):
    default_hs, default_scopes = header_inputs()
    return associate_filing_headers(
        hs if hs is not None else default_hs,
        scopes=scopes if scopes is not None else default_scopes,
        authorities={"c": authority},
        filings=filings if filings is not None else [filing()],
        source_generation_pin=GEN,
        selection_evidence_sha256=EVIDENCE,
    )


def test_native_number_keeps_all_metadata_witnesses_without_multiplying_money():
    rows = numbers(filings=[filing(), filing("metadata-2")])
    assert len(rows) == 1 and rows[0]["filing_key"] == filing_key("123")
    assert rows[0]["filing_observation_ids"] == ["metadata-1", "metadata-2"]
    assert rows[0]["filing_observation_count"] == 2
    assert rows[0]["current_record_status"] == "unqualified"
    assert typed_batch(rows, ASSOCIATION_SCHEMA).to_pylist() == rows


@pytest.mark.parametrize(
    "version,status",
    [
        ("fec-bulk-individual-receipt/1", "unresolved_native_namespace"),
        ("fec-bulk-individual-receipt/2", "resolved_native_filing_key"),
    ],
)
def test_individual_number_association_requires_current_mapper_in_python_and_sql(version, status):
    row = financial(mapping_version=version)
    expected = numbers([row])[0]
    assert expected["association_status"] == status
    with duckdb.connect(config={"threads": 1, "memory_limit": "64MB"}) as con:
        con.register("fec_receipts", pa.Table.from_pylist([row]))
        con.register("fec_filings", pa.Table.from_pylist([filing()]))
        result = con.sql(financial_number_association_sql(
            "fec_receipts", GEN, {NAMESPACE: EVIDENCE}, columns=tuple(row),
        )).to_arrow_table().to_pylist()[0]
    assert result["association_status"] == status
    assert result["filing_key"] == (filing_key("123") if status == "resolved_native_filing_key" else None)


@pytest.mark.parametrize(
    ("value", "status"),
    [
        (None, "unresolved_no_file_number"),
        ("", "unresolved_no_file_number"),
        ("0", "unresolved_no_file_number"),
        ("00123", "unresolved_invalid_file_number"),
        ("FEC-123", "unresolved_invalid_file_number"),
        ("123.0", "unresolved_invalid_file_number"),
        (" 123", "unresolved_invalid_file_number"),
        ("124", "unresolved_target_not_retained"),
    ],
)
def test_number_absent_invalid_or_unretained_never_uses_other_identifiers(value, status):
    row = numbers([financial(report_number=value, source_record_identifier="123", image_number="123")])[0]
    assert row["association_status"] == status and row["filing_key"] is None
    assert row["report_number_raw"] == value


def test_negative_paper_file_number_keeps_native_namespace():
    row = numbers([financial(report_number="-123")], filings=[filing(number="-123")])[0]
    assert row["filing_key"] == filing_key("-123")


@pytest.mark.parametrize(
    ("changes", "status"),
    [
        ({"source_authority": "unofficial-senate"}, "unresolved_source_authority"),
        ({"source_namespace": "fec-electronic-filing-schedule"}, "unresolved_native_namespace"),
        ({"mapping_version": "new/2"}, "unresolved_native_namespace"),
        ({"reporting_committee_id": "C00000002"}, "unresolved_filer_identity_conflict"),
    ],
)
def test_namespace_authority_mapping_and_filer_conflicts_refuse(changes, status):
    assert numbers([financial(**changes)])[0]["association_status"] == status


def test_namespace_requires_explicit_definition_evidence():
    assert numbers(proofs={})[0]["association_status"] == "unresolved_namespace_evidence"


@pytest.mark.parametrize(
    ("namespace", "filer_field"),
    [("independent-expenditure-csv", "spender_native_id"), ("communication-cost-csv", "reporting_entity_id")],
)
def test_union_schema_null_committee_field_does_not_hide_native_filer_conflict(namespace, filer_field):
    ns = "fec-bulk-" + namespace
    row = financial(
        source_namespace=ns, mapping_version=ns + "/1", reporting_committee_id=None, **{filer_field: "C00000002"}
    )
    assert numbers([row], proofs={ns: EVIDENCE})[0]["association_status"] == "unresolved_filer_identity_conflict"


@pytest.mark.parametrize(
    "change", ["duplicate", "key", "authority", "namespace", "filer", "form", "mapper", "identity"]
)
def test_conflicting_metadata_never_selects_first_or_latest(change):
    fs = [filing(), filing("metadata-2")]
    if change == "duplicate":
        fs[1] = deepcopy(fs[0])
    elif change == "key":
        fs[1]["report_number"] = "124"
    else:
        fs[1][
            {
                "authority": "source_authority",
                "namespace": "source_namespace",
                "filer": "native_filer_id",
                "form": "form_type",
                "mapper": "mapping_version",
                "identity": "identity_version",
            }[change]
        ] = "conflict"
    row = numbers(filings=fs)[0]
    assert row["filing_key"] is None and row["association_status"].startswith("unresolved_target_")
    assert row["filing_observation_count"] == 2


def test_exact_native_api_url_resolves_physical_header_without_filename_parsing():
    row = headers(filings=[filing(), filing("metadata-2")])[0]
    assert row["filing_key"] == filing_key("123") and row["header_record_id"] == "header"
    assert row["replacement_mode"] == "unknown" and row["membership_completeness"] == "unqualified"
    assert typed_batch([row], ASSOCIATION_SCHEMA).to_pylist() == [row]


def test_same_original_header_in_distinct_alias_collections_has_distinct_association_identity():
    hs, scopes = header_inputs()
    original = hs[0]
    alias = {**original, "collection_id": "alias"}
    locator = json.loads(alias["source_locator_json"])
    locator["collection_id"] = "alias"
    alias["source_locator_json"] = json.dumps(locator)
    rows = associate_filing_headers(
        [original, alias],
        scopes={**scopes, "alias": scopes["c"]},
        authorities={"c": "official-fec", "alias": "official-fec"},
        filings=[filing()],
        source_generation_pin=GEN,
        selection_evidence_sha256=EVIDENCE,
    )
    assert len({r["record_id"] for r in rows}) == 2
    assert rows[0]["header_record_id"] == rows[1]["header_record_id"]
    assert rows[0]["source_sha256"] == rows[1]["source_sha256"]
    assert rows[0]["filing_key"] == rows[1]["filing_key"]


def test_filename_and_header_report_number_do_not_resolve_without_api_url():
    assert headers(filings=[])[0]["association_status"] == "unresolved_no_native_url_witness"


def test_archive_member_identity_is_not_inferred_from_name():
    hs, scopes = header_inputs()
    member = dict(ordinal=0, name="123.fec")
    scopes["c"]["member"] = member
    loc = json.loads(hs[0]["source_locator_json"])
    loc["member"] = member
    hs[0]["source_locator_json"] = json.dumps(loc)
    assert headers(hs, scopes)[0]["association_status"] == "unresolved_archive_member_identity"


def test_header_capture_and_locator_must_agree():
    hs, scopes = header_inputs()
    scopes["c"]["capture"]["responseSha256"] = pin("different")
    with pytest.raises(ValueError, match="exact original/member"):
        headers(hs, scopes)
    hs, scopes = header_inputs()
    hs[0]["collection_id"] = "other"
    with pytest.raises(KeyError):
        headers(hs, scopes)


def test_unqualified_redirect_and_authority_do_not_assert_identity():
    hs, scopes = header_inputs()
    scopes["c"]["capture"]["resolvedUrl"] = URL + "?other"
    assert headers(hs, scopes)[0]["association_status"] == "unresolved_redirect_identity"
    assert headers(authority="unofficial-senate")[0]["association_status"] == "unresolved_source_authority"


def test_physical_header_join_keeps_each_financial_row_and_refuses_duplicates():
    hs, _ = header_inputs()
    associations = headers()
    loc = hs[0]["source_locator_json"]
    row = financial(filing_header_record_id="header", filing_header_locator_json=loc)
    with duckdb.connect(config={"threads": 1, "memory_limit": "64MB"}) as db:
        db.register(
            "fec_receipts",
            pa.Table.from_pylist([row, {**row, "record_id": pin("second"), "source_authority": "unofficial-senate"}]),
        )

        def run(values):
            db.register("fec_filing_header_associations", pa.Table.from_pylist(values, schema=ASSOCIATION_SCHEMA))
            return db.sql(financial_header_association_sql("fec_receipts", GEN)).to_arrow_table().to_pylist()

        result = run(associations)
        assert len(result) == 2
        assert {r["association_status"] for r in result} == {
            "resolved_native_filing_key",
            "unresolved_header_association_absent",
        }
        result = run(associations * 2)
        assert len(result) == 2 and any(
            r["association_status"] == "unresolved_header_association_ambiguous" for r in result
        )
        result = run([{**associations[0], "source_generation_pin": pin("other-generation")}])
        assert all(r["filing_key"] is None for r in result)


@pytest.mark.parametrize(
    ("changes", "status"),
    [
        ({"policy_version": "future/2"}, "unresolved_header_policy_version"),
        ({"policy_version": None}, "unresolved_header_policy_version"),
        ({"association_status": "qualified_current"}, "unresolved_header_policy_semantics"),
        ({"association_status": None}, "unresolved_header_policy_semantics"),
        ({"association_basis": "guessed-filename"}, "unresolved_header_policy_semantics"),
        ({"filing_key": None}, "unresolved_header_policy_semantics"),
        ({"referenced_filing_key": filing_key("999")}, "unresolved_header_policy_semantics"),
        ({"filing_key": "invented", "referenced_filing_key": "invented"}, "unresolved_header_policy_semantics"),
        ({"association_status": "unresolved_no_native_url_witness"}, "unresolved_header_policy_semantics"),
        ({"header_locator_json": "{}"}, "unresolved_header_locator_mismatch"),
    ],
)
def test_header_view_refuses_unsupported_or_inconsistent_policy_metadata(changes, status):
    hs, _ = header_inputs()
    row = financial(filing_header_record_id="header", filing_header_locator_json=hs[0]["source_locator_json"])
    association = {**headers()[0], **changes}
    with duckdb.connect(config={"threads": 1, "memory_limit": "64MB"}) as db:
        db.register("fec_receipts", pa.Table.from_pylist([row]))
        db.register("fec_filing_header_associations", pa.Table.from_pylist([association], schema=ASSOCIATION_SCHEMA))
        (result,) = db.sql(financial_header_association_sql("fec_receipts", GEN)).to_arrow_table().to_pylist()
    assert result["association_status"] == status and result["filing_key"] is None
    assert result["header_association_policy_version"] == association["policy_version"]
    assert result["header_association_status"] == association["association_status"]


def test_unknown_policy_cannot_be_filtered_away_before_duplicate_detection():
    hs, _ = header_inputs()
    row = financial(filing_header_record_id="header", filing_header_locator_json=hs[0]["source_locator_json"])
    association = headers()[0]
    with duckdb.connect(config={"threads": 1, "memory_limit": "64MB"}) as db:
        db.register("fec_receipts", pa.Table.from_pylist([row]))
        db.register(
            "fec_filing_header_associations",
            pa.Table.from_pylist(
                [association, {**association, "policy_version": "future/2"}], schema=ASSOCIATION_SCHEMA
            ),
        )
        (result,) = db.sql(financial_header_association_sql("fec_receipts", GEN)).to_arrow_table().to_pylist()
    assert result["association_status"] == "unresolved_header_association_ambiguous"
    assert result["filing_key"] is None


@pytest.mark.parametrize("version", ["fec-identity-observations/1", "fec-identity-observations/3", "unknown"])
def test_only_rebuilt_v2_filing_metadata_can_establish_associations(version):
    target = {**filing(), "mapping_version": version}
    for result in (numbers(filings=[target])[0], headers(filings=[target])[0]):
        assert result["filing_key"] is None
        assert result["association_status"] == "unresolved_target_identity_conflict"
    assert numbers()[0]["policy_version"] == "fec-retained-filing-association/2"
