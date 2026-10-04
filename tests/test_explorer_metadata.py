"""Metadata failures cannot hide data or invent relationships or source claims."""

from copy import deepcopy
import importlib.util
from pathlib import Path

import pytest

from spicy_regs.explorer_metadata import build_bundle, same_metadata


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


def test_schema_changes_are_pinned_and_invalidated_before_publication():
    changed = index()
    changed["families"]["example"]["tables"]["parent.parquet"]["columns"] = [["new_id", "VARCHAR"]]
    with pytest.raises(ValueError, match="missing columns"):
        build_bundle(changed, **args())


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


def test_scorecard_reader_uses_immutable_member_path_and_checks_digest(monkeypatch):
    import hashlib
    import io
    import pyarrow as pa
    import pyarrow.parquet as pq
    from spicy_regs.sources.publication import Member
    script = publisher_script()
    rows = [{"publisher_id": "new", "name": "New Publisher", "scorecard_index_url": "https://example.org/scorecard",
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
    assert script.scorecard_sources(data, "https://data.example.org")[0]["name"] == "New Publisher"
    assert seen == ["https://data.example.org/" + member.path]
    monkeypatch.setattr(script, "get_public", lambda *_: b"wrong bytes")
    with pytest.raises(ValueError, match="differ"):
        script.scorecard_sources(data, "https://data.example.org")


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
