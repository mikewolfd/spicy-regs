"""Metadata failures cannot hide data or invent relationships or source claims."""

from copy import deepcopy
import importlib.util
from pathlib import Path

import pytest

from spicy_regs.explorer_metadata import build_bundle, public_url, same_metadata


def publisher_script():
    path = Path(__file__).parents[1] / "scripts/publish_explorer_metadata.py"
    spec = importlib.util.spec_from_file_location("publish_explorer_metadata", path)
    assert spec is not None and spec.loader is not None
    script = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(script)
    return script


def descriptor(columns):
    return {"columns": [[name, "VARCHAR"] for name in columns], "rows": 1, "byteSize": 100}


def index():
    return {"format": "spicy-regs-publication", "version": 2, "families": {
        "example": {"artifactDigest": "sha256:" + "a" * 64,
                    "tables": {"child.parquet": descriptor(["parent_id", "edition"]),
                               "parent.parquet": descriptor(["id", "edition"]),
                               "brand_new.parquet": descriptor(["id"])}}}}


def join():
    return {"child": "child", "child_columns": ["parent_id", "edition"], "parent": "parent",
            "parent_columns": ["id", "edition"], "kind": "complete", "expected_cardinality": "one"}


def args():
    return {"descriptions": {"child": {"label": "Child", "columns": []},
                             "parent": {"label": "Parent", "columns": []}},
            "registry": {"sources": {"original": {"id": "original", "name": "Publisher",
                                                   "url": "https://example.org/", "kind": "government"}},
                         "families": {}, "tables": {"child": {"sources": ["original"], "inputs": ["parent"]}}},
            "join_record": {"joins": [join()]}, "audit": {},
            "generated_at": "2026-10-03T00:00:00Z"}


def test_new_table_remains_discoverable_but_never_borrowed_description_or_join():
    result = build_bundle(index(), **args())
    assert set(result["tables"]) == {"child", "parent", "brand_new"}
    assert result["tables"]["brand_new"]["metadataStatus"] == "unknown"
    assert result["tables"]["brand_new"]["sourceStatus"] == "unknown"
    assert len(result["joins"]) == 1
    assert result["tables"]["child"]["inputs"] == ["parent"]
    assert result["tables"]["parent"]["metadataStatus"] == "documented"
    assert result["tables"]["parent"]["sourceStatus"] == "unknown"


@pytest.mark.parametrize("identity,missing", [
    (["id", "native_edition"], ["native_edition"]),
    (["native_id", "edition"], ["native_id"]),
    (["native_id", "native_edition"], ["native_edition", "native_id"]),
    (["native_id"], ["native_id"]),
    (["id", "edition"], []),
    (["id"], []),
])
def test_selected_schema_exposes_a_complete_identity_or_reports_it_unavailable(identity, missing):
    options = args()
    options["descriptions"]["parent"]["identity_columns"] = identity
    before = deepcopy(options)
    parent = build_bundle(index(), **options)["tables"]["parent"]
    if missing:
        assert "identity_columns" not in parent
        assert parent["unavailableIdentity"] == {
            "columns": identity,
            "missing_columns": missing,
            "reason": "Selected published schema does not expose the complete declared identity.",
        }
    else:
        assert parent["identity_columns"] == identity
        assert "unavailableIdentity" not in parent
    assert parent["publicationSchema"] == [["id", "VARCHAR"], ["edition", "VARCHAR"]]
    assert options == before


def test_still_published_processing_tables_use_the_canonical_dictionary(monkeypatch):
    from spicy_regs import data_dictionary, explorer_metadata
    options = args()
    known = options.pop("descriptions")
    monkeypatch.setattr(explorer_metadata, "read_json", lambda _: deepcopy(known))
    def load_descriptions():
        return {"brand_new": {"label": "Saved collection log", "summary": "Records the requests made.",
                              "category": "processing_evidence",
                              "coverage": "Only saved requests.", "columns": {"id": "Request ID."}},
                "unpublished": {"label": "Not published"}}
    monkeypatch.setattr(data_dictionary, "load_curated_descriptions", load_descriptions)
    result = build_bundle(index(), **options)
    table = result["tables"]["brand_new"]
    assert table["metadataStatus"] == "documented"
    assert table["label"] == "Saved collection log"
    assert table["category"] == "processing_evidence"
    assert table["columns"] == [{"column_name": "id", "column_type": "VARCHAR", "description": "Request ID."}]
    assert "unpublished" not in result["tables"]
    assert len(result["joins"]) == 1


def test_bill_collection_checkpoints_have_an_explicit_evidence_category():
    from spicy_regs.data_dictionary import load_curated_descriptions

    descriptions = load_curated_descriptions()
    for name in ("bill_family_archives", "bill_family_backfills", "bill_family_backfill_walks"):
        assert descriptions[name]["category"] == "processing_evidence"
    # Campaign-finance receipts are substantive financial observations.
    assert descriptions["fec_receipts"].get("category") != "processing_evidence"


def test_published_fec_roles_and_count_units_survive_dictionary_fallback(monkeypatch):
    from spicy_regs import explorer_metadata

    # These published outputs are absent from the installed schema metadata.
    monkeypatch.setattr(explorer_metadata, "read_json", lambda _: {})
    expected = {
        "fec_api_response_controls": ("diagnostics", "response fields"),
        "fec_agency_mapping_dispositions": ("diagnostics", "mapping results"),
        "fec_research_context_dispositions": ("diagnostics", "processing results"),
        "fec_research_response_outcomes": ("diagnostics", "response outcomes"),
        "fec_collections": ("processing_evidence", "collections"),
        "fec_collection_selection": ("processing_evidence", "selection decisions"),
        "fec_record_evidence": ("source_evidence", "evidence links"),
        "fec_filing_header_associations": ("source_evidence", "association decisions"),
        "fec_filing_definition_evidence": ("source_evidence", "source references"),
        "fec_filing_definitions": ("reference_data", "filing layouts"),
        "fec_source_catalog": ("reference_data", "sources"),
        "fec_source_records": ("source_data", "source records"),
    }
    live = {name: {"schema": [["record_id", "VARCHAR"]]} for name in expected}
    result = explorer_metadata.publication_descriptions(live)
    for name, (category, unit) in expected.items():
        assert result[name]["category"] == category
        assert result[name]["row_unit"] == unit
        assert "record_id" in {column["column_name"] for column in result[name]["columns"]}
    assert "committee_report_reads" not in result  # Documentation cannot invent publication.


def test_packaged_fec_metadata_uses_the_same_roles_as_the_fallback(monkeypatch):
    from spicy_regs import explorer_metadata

    monkeypatch.setattr(explorer_metadata, "read_json", lambda _: {
        "fec_api_response_controls": {"label": "Known table", "category": "query_data"},
        "fec_receipts": {"category": "query_data"},
    })
    live = {name: {"schema": []} for name in ("fec_api_response_controls", "fec_receipts")}
    result = explorer_metadata.publication_descriptions(live)
    assert result["fec_api_response_controls"]["category"] == "diagnostics"
    assert result["fec_receipts"]["category"] == "query_data"


@pytest.mark.parametrize("change", [
    {"child_columns": ["missing"]}, {"child_columns": ["parent_id"]},
    {"child_columns": []}, {"child_columns": ["parent_id", "parent_id"]},
    {"parent": "unregistered"}, {"expected_cardinality": "sure"}, {"kind": "inferred"},
])
def test_rejects_invalid_keys_and_cardinality(change):
    options = args()
    options["join_record"]["joins"][0].update(change)
    with pytest.raises(ValueError):
        build_bundle(index(), **options)


def test_schema_changes_publish_descriptions_but_disable_old_connections():
    changed, options = index(), args()
    options["descriptions"]["parent"]["columns"] = [
        {"column_name": "id", "column_type": "VARCHAR"},
        {"column_name": "edition", "column_type": "VARCHAR"},
    ]
    changed["families"]["example"]["tables"]["parent.parquet"]["columns"] = [["new_id", "VARCHAR"]]
    result = build_bundle(changed, **options)
    assert result["tables"]["parent"]["publicationSchema"] == [["new_id", "VARCHAR"]]
    assert result["joins"] == []
    assert result["omittedJoins"] == [{"child": "child", "parent": "parent",
                                       "reason": "Fields no longer published: parent.id, parent.edition."}]
    assert result["tables"]["child"]["joinAudit"]["status"] == "missing"
    assert result["tables"]["child"]["unavailableJoins"] == result["omittedJoins"]


def test_curated_fallback_preserves_removed_key_validation_without_publishing_it(monkeypatch):
    from spicy_regs import data_dictionary, explorer_metadata
    changed, options = index(), args()
    known = options.pop("descriptions")
    del known["parent"]
    monkeypatch.setattr(explorer_metadata, "read_json", lambda _: deepcopy(known))
    monkeypatch.setattr(data_dictionary, "load_curated_descriptions", lambda: {
        "parent": {"label": "Parent", "columns": {"id": "Recorded ID.", "edition": "Edition."}}})
    changed["families"]["example"]["tables"]["parent.parquet"]["columns"] = [["new_id", "VARCHAR"]]
    result = build_bundle(changed, **options)
    assert result["joins"] == []
    assert result["tables"]["parent"]["publicationSchema"] == [["new_id", "VARCHAR"]]
    assert result["tables"]["parent"]["columns"] == [
        {"column_name": "new_id", "column_type": "VARCHAR", "description": ""}]
    assert "parent.id, parent.edition" in result["omittedJoins"][0]["reason"]
    options["join_record"]["joins"][0]["parent_columns"][0] = "never_documented"
    with pytest.raises(ValueError, match="missing columns"):
        build_bundle(changed, **options)


def test_new_current_key_can_be_declared_without_borrowing_old_fields():
    changed, options = index(), args()
    changed["families"]["example"]["tables"]["parent.parquet"]["columns"][0][0] = "new_id"
    options["join_record"]["joins"][0]["parent_columns"][0] = "new_id"
    result = build_bundle(changed, **options)
    assert all(result["joins"][0][k] == v for k, v in options["join_record"]["joins"][0].items())
    assert result["omittedJoins"] == []


def test_unmeasured_declaration_preserves_its_kind_and_uncertainty():
    options = args()
    options["join_record"]["joins"][0].update(kind="unmeasured", reason="No measured baseline yet.")
    result = build_bundle(index(), **options)
    assert result["joins"][0]["kind"] == "unmeasured"
    assert result["joins"][0]["reason"] == "No measured baseline yet."


def test_missing_live_parent_is_explicitly_omitted_but_input_is_preserved():
    changed, options = index(), args()
    del changed["families"]["example"]["tables"]["parent.parquet"]
    options["descriptions"]["parent"]["columns"] = [
        {"column_name": "id", "column_type": "VARCHAR"}, {"column_name": "edition", "column_type": "VARCHAR"}]
    options["audit"] = {"child": {"status": "connected", "reason": "Connected when reviewed."}}
    result = build_bundle(changed, **options)
    assert result["joins"] == []
    assert result["omittedJoins"][0]["parent"] == "parent"
    assert result["tables"]["child"]["inputs"] == ["parent"]
    assert result["tables"]["child"]["joinAudit"]["status"] == "missing"
    assert "unavailable" in result["tables"]["child"]["joinAudit"]["reason"]
    assert result["tables"]["child"]["unavailableJoins"][0]["parent"] == "parent"


def test_duplicate_canonical_join_is_rejected():
    options = args()
    options["join_record"]["joins"].append(join())
    with pytest.raises(ValueError, match="Duplicate join declaration"):
        build_bundle(index(), **options)


def test_timer_does_not_refresh_freshness_but_data_or_metadata_change_does():
    before = build_bundle(index(), **args())
    after = deepcopy(before)
    after["generatedAt"] = "2026-10-04T00:00:00Z"
    assert same_metadata(before, after)
    after["publication"]["families"]["example"] = "sha256:" + "b" * 64
    assert not same_metadata(before, after)


def test_invalid_source_and_undocumented_input_refused():
    options = args()
    options["registry"]["sources"]["original"]["url"] = "javascript:alert(1)"
    with pytest.raises(ValueError, match="source attribution"):
        build_bundle(index(), **options)
    options = args()
    options["registry"]["tables"]["child"]["inputs"] = ["imagined_table"]
    with pytest.raises(ValueError, match="Unknown"):
        build_bundle(index(), **options)


def test_scorecard_publishers_come_from_current_rows_not_a_hardcoded_list():
    options, data = args(), index()
    data["families"]["scorecards"] = data["families"].pop("example")
    options["scorecard_publishers"] = [{"id": "new-publisher", "name": "New Publisher",
                                        "url": "https://new.example/scorecard", "kind": "publisher"}]
    result = build_bundle(data, **options)
    assert result["tables"]["parent"]["sources"][0]["name"] == "New Publisher"
    assert result["tables"]["parent"]["sourceStatus"] == "documented"


def test_repeated_table_is_rejected():
    data = index()
    data["families"]["duplicate"] = deepcopy(data["families"]["example"])
    with pytest.raises(ValueError, match="repeated table"):
        build_bundle(data, **args())


def test_source_registry_distinguishes_deterministic_attributes_and_model_output():
    import json
    registry = json.loads((Path(__file__).parents[1] / "src/spicy_regs/explorer_sources.json").read_text())
    assert not registry["tables"]["document_attributes"]["modelGenerated"]
    assert registry["tables"]["document_attributes"]["inputs"] == []
    assert registry["tables"]["bill_summaries"]["modelGenerated"]
    assert registry["tables"]["member_vote_terms"]["inputs"] == ["member_votes", "member_terms"]


@pytest.mark.parametrize("url", [
    "https://example.org/scorecard",
    "http://www.citizen.org/vchart00/map.htm",
    "http://www.drugpolicyaction.org/voter-guide/",
    "http://globalsolutions.org/capitol-hill/reportcard/2010",
])
def test_scorecard_reader_uses_immutable_member_path_and_checks_digest(monkeypatch, url):
    import hashlib
    import io
    import pyarrow as pa
    import pyarrow.parquet as pq
    from spicy_regs.sources.publication import Member
    script = publisher_script()
    rows = [{"publisher_id": "new", "name": "New Publisher", "scorecard_index_url": url,
             "homepage_url": "https://example.org/"}]
    buffer = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows), buffer)
    raw = buffer.getvalue()
    member = Member("generations/scorecards/" + "a" * 64 + "/scorecard_publishers.parquet",
                    "sha256:" + hashlib.sha256(raw).hexdigest(), len(raw), 1)
    monkeypatch.setattr(script, "table_members", lambda *_: [member])
    seen = []
    monkeypatch.setattr(script, "get_public", lambda url, limit: seen.append(url) or raw)
    data = {"families": {"scorecards": {"tables": {"scorecard_publishers.parquet": {}}}}}
    publishers = script.scorecard_sources(data, "https://data.example.org")
    assert publishers[0]["name"] == "New Publisher"
    assert publishers[0]["url"] == url
    options, live = args(), index()
    live["families"]["scorecards"] = live["families"].pop("example")
    options["scorecard_publishers"] = publishers
    assert build_bundle(live, **options)["tables"]["parent"]["sources"][0]["url"] == url
    assert seen == ["https://data.example.org/" + member.path]
    monkeypatch.setattr(script, "get_public", lambda *_: b"wrong bytes")
    with pytest.raises(ValueError, match="differ"):
        script.scorecard_sources(data, "https://data.example.org")


def test_historical_http_links_are_only_allowed_for_attribution():
    assert public_url("https://example.org/")
    assert not public_url("http://example.org/")
    assert public_url("http://example.org/", allow_http=True)
    for url in ["javascript:alert(1)", "file:///tmp/source", "//example.org/",
                "http://user:password@example.org/", "https://user@example.org/", "http://"]:
        assert not public_url(url, allow_http=True)


def test_metadata_data_base_still_requires_https(monkeypatch):
    script = publisher_script()
    monkeypatch.setattr("sys.argv", ["publish_explorer_metadata.py", "--base-url", "http://data.example.org"])
    monkeypatch.setattr(script, "get_public", lambda *_: pytest.fail("Insecure data base must not be read"))
    with pytest.raises(SystemExit) as error:
        script.main()
    assert error.value.code == 2


def test_failed_write_preserves_previous_object_and_only_targets_metadata():
    import io
    script = publisher_script()
    bundle = build_bundle(index(), **args())

    class Store:
        data = b'{"old":"metadata"}'
        fail = True
        def get_object(self, **kwargs):
            assert kwargs["Key"] == script.KEY
            return {"Body": io.BytesIO(self.data)}
        def put_object(self, **kwargs):
            assert kwargs["Key"] == script.KEY
            if self.fail:
                raise RuntimeError("R2 unavailable")
            self.data = kwargs["Body"]

    client = Store()
    before = client.data
    with pytest.raises(RuntimeError, match="unavailable"):
        script.publish_bundle(client, "test", bundle)
    assert client.data == before
    client.fail = False
    assert script.publish_bundle(client, "test", bundle) == "published"
    client.fail = True
    assert script.publish_bundle(client, "test", bundle) == "unchanged"


def test_failed_readback_never_reports_success(monkeypatch):
    script = publisher_script()
    writes = []
    class Store:
        def put_object(self, **kwargs):
            writes.append(kwargs["Key"])
    monkeypatch.setattr(script, "read_object", lambda *a, **kw: b'{}')
    with pytest.raises(ValueError, match="readback differs"):
        script.publish_bundle(Store(), "test", build_bundle(index(), **args()))
    assert writes == [script.KEY]


@pytest.mark.parametrize("previous", [b'invalid JSON', b'[]', b'null', b'{}', b'\xff'])
def test_invalid_prior_metadata_is_replaced_after_validated_build(previous):
    import io
    script = publisher_script()
    class Store:
        data = previous
        writes = 0
        def get_object(self, **kwargs):
            return {"Body": io.BytesIO(self.data)}
        def put_object(self, **kwargs):
            assert kwargs["Key"] == script.KEY
            self.data = kwargs["Body"]
            self.writes += 1
    client = Store()
    bundle = build_bundle(index(), **args())
    assert script.publish_bundle(client, "test", bundle) == "published"
    assert client.writes == 1
    assert client.data == script.canonical_bytes(bundle)


def test_declared_record_identity_and_directional_scope_do_not_infer_uniqueness():
    options = args()
    options['descriptions']['parent']['identity_columns'] = ['id', 'edition']
    options['descriptions']['child']['identity_columns'] = ['parent_id', 'edition', 'row_index']
    result = build_bundle(index(), **options)
    assert result['tables']['parent']['recordIdentity'] == {
        'columns': ['id', 'edition'], 'basis': 'declared_main_key', 'uniqueness': 'unknown'}
    assert 'recordIdentity' not in result['tables']['child']
    declared = result['joins'][0]
    assert declared['completeKey'] == {'child': False, 'parent': True}
    assert declared['directions']['forward']['measurement']['status'] == 'unknown'
    # expected_cardinality is a declared CI policy, not a current measurement.
    assert declared['expected_cardinality'] == 'one'


def test_retired_relationships_remain_accounted_without_reappearing_as_joins():
    options = args()
    options['join_record']['processing_joins'] = [join()]
    result = build_bundle(index(), **options)
    retired = result['retiredJoins'][0]
    assert retired['status'] == 'retired_processing_relationship'
    assert not retired['directions']['forward']['available']
    assert not retired['directions']['reverse']['available']
    assert len(result['joins']) == 1


def test_main_policy_identity_is_admitted_but_receipt_identity_never_is(monkeypatch):
    from spicy_regs import subject_catalog
    monkeypatch.setattr(subject_catalog, 'descriptors', lambda: {
        'parent': {'identity_fields':['id','edition'], 'receipt_only':False},
        'child': {'identity_fields':['parent_id','edition'], 'receipt_only':True}})
    result = build_bundle(index(), **args())
    assert result['tables']['parent']['recordIdentity']['columns'] == ['id','edition']
    assert 'recordIdentity' not in result['tables']['child']
    assert result['joins'][0]['completeKey'] == {'child': False, 'parent': True}
