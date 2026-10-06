"""Audited scalar links retain full identity and publication-specific evidence."""

import json
from pathlib import Path

import pytest

from spicy_regs import data_dictionary, table_joins
from spicy_regs.explorer_metadata import build_bundle
from spicy_regs.explorer_publications import identity_digest

RECEIPT = json.loads((Path(__file__).parents[1] / "docs/evidence/column-navigation-2026-10-06.json").read_text())


def test_full_audit_links_use_the_canonical_registry_and_enforced_baselines():
    current = {j.name: j for j in table_joins.JOINS}
    assert len(RECEIPT["results"]) == 13
    for result in RECEIPT["results"]:
        join = current[result["join"]]
        assert join.measurement == result
        assert join.expected_cardinality == "one" and result["parent_duplicate_keys"] == 0
        assert (join.baseline_keys, join.baseline_missing) == (result["keys"], result["missing"])
        assert join.kind == ("scope" if result["missing"] else "complete")
    vote = next(j for j in current.values() if j.child == "bill_vote_references" and j.parent == "roll_call_votes")
    assert vote.child_columns == vote.parent_columns == ("congress", "chamber", "session", "roll_number")
    assert not any(j.child == "hearing_bill_links" and j.parent == "committee_meetings" for j in current.values())


def extra():
    descriptor = {
        "kind": "comments",
        "family": "comments",
        "members": [
            {
                "path": "comments_index.parquet",
                "sha256": "sha256:" + "a" * 64,
                "byteSize": 100,
                "rows": 10,
                "etag": '"held"',
            }
        ],
    }
    return {
        "family": "comments",
        "publicationSchema": [["agency_code", "VARCHAR"]],
        "publicationIdentity": identity_digest(descriptor),
        "descriptor": descriptor,
    }


def test_separate_schema_and_identity_activate_links_without_inventing_main_index_tables():
    index = {
        "format": "spicy-regs-publication",
        "version": 2,
        "families": {
            "agency": {
                "artifactDigest": "sha256:" + "b" * 64,
                "tables": {"agency_stats.parquet": {"columns": [["agency_code", "VARCHAR"]]}},
            }
        },
    }
    descriptions = {
        name: {"columns": [{"column_name": "agency_code", "column_type": "VARCHAR"}]}
        for name in ["agency_stats", "comments_index"]
    }
    reg = {"sources": {}, "tables": {}, "families": {}}
    j = table_joins.record(
        next(j for j in table_joins.JOINS if j.child == "comments_index" and j.parent == "agency_stats")
    )
    bundle = build_bundle(
        index,
        descriptions=descriptions,
        registry=reg,
        join_record={"joins": [j]},
        audit={},
        extra_tables={"comments_index": extra()},
    )
    declared = bundle["joins"][0]
    assert {k: v for k, v in declared.items() if k not in {"directions", "completeKey", "requiredFields"}} == j
    assert declared['directions']['forward']['measurement']['status'] == 'unknown'
    assert declared['requiredFields'] == {'child': [{'path': 'agency_code', 'status': 'published'}], 'parent': [{'path': 'agency_code', 'status': 'published'}]}
    assert bundle["tables"]["comments_index"]["publicationIdentity"] == extra()["publicationIdentity"]
    assert bundle["extra_tables"]["comments_index"] == extra()
    assert set(bundle["publication"]["families"]) == {"agency"}
    assert bundle["tables"]["comments_index"]["joinAudit"]["status"] == "connected"


@pytest.mark.parametrize("change", ["etag", "rows", "path", "sha256", "family"])
def test_stale_or_invalid_separate_descriptors_are_refused(change):
    entry = extra()
    if change == "family":
        entry["descriptor"]["family"] = "other"
    else:
        entry["descriptor"]["members"][0][change] = {"rows": 11}.get(change, "changed")
    with pytest.raises(ValueError):
        build_bundle(
            {"format": "spicy-regs-publication", "version": 2, "families": {}},
            descriptions={},
            registry={"sources": {}, "families": {}, "tables": {}},
            join_record={"joins": []},
            audit={},
            extra_tables={"comments_index": entry},
        )


def test_an_unmeasured_native_migration_is_explicitly_unavailable_on_old_fields():
    from tests.test_explorer_metadata import args, index

    options = args()
    options["join_record"]["joins"][0]["kind"] = "unmeasured"
    options["descriptions"]["parent"]["columns"] = [
        {"column_name": "id", "column_type": "VARCHAR"},
        {"column_name": "edition", "column_type": "VARCHAR"},
    ]
    options["descriptions"]["child"]["columns"] = [
        {"column_name": "parent_id", "column_type": "VARCHAR"},
        {"column_name": "edition", "column_type": "VARCHAR"},
    ]
    changed = index()
    changed["families"]["example"]["tables"]["parent.parquet"]["columns"] = [["other", "VARCHAR"]]
    bundle = build_bundle(changed, **options)
    assert bundle["joins"] == []
    assert "Fields no longer published" in bundle["omittedJoins"][0]["reason"]


def test_historical_publisher_links_retain_source_urls_without_weakening_data_base_validation():
    from spicy_regs.explorer_metadata import public_url

    assert public_url("http://example.org/scorecard", allow_http=True)
    assert not public_url("http://example.org/data")
    for url in ["javascript:alert(1)", "http://name:password@example.org", "file:///local"]:
        assert not public_url(url, allow_http=True)


def test_promoted_fec_source_tables_reuse_dictionary_prose():
    source = data_dictionary.load_curated_descriptions()
    native = json.loads((Path(__file__).parents[1] / "src/spicy_regs/table_metadata.json").read_text())
    assert "fec_collections" in source and "fec_collections" in native
    columns = [["collection_id", "VARCHAR"]]
    index = {
        "format": "spicy-regs-publication",
        "version": 2,
        "families": {
            "fec-source-catalog": {
                "artifactDigest": "sha256:" + "b" * 64,
                "tables": {"fec_collections.parquet": {"columns": columns}},
            }
        },
    }
    bundle = build_bundle(index, join_record={"joins": []}, audit={})
    assert set(bundle["tables"]) == {"fec_collections"}
    table = bundle["tables"]["fec_collections"]
    assert table["columns"][0]["description"] == source["fec_collections"]["columns"]["collection_id"]
    assert table["sourceStatus"] == table["metadataStatus"] == "documented"
    assert table["publicationSchema"] == columns


def test_main_detail_attempt_navigation_requires_complete_partition_identity():
    from spicy_regs.explorer_navigation import declarations, target_keys, validate_navigation
    specs=[spec for spec in declarations() if spec['id']=='nominations_detail_attempts']
    target=specs[0]['targets'][0]
    assert target_keys(target, {}, {'congress':'119','citation':'PN129-10'}) == ['119','PN129-10']
    assert target['table'] == 'nominations_detail_reads'
    assert target_keys(target, {}, {'congress':'119'}) is None
    malformed=json.loads(json.dumps(specs))
    malformed[0]['targets'][0]['keys'][0]['parts'][0]['literal']=42
    with pytest.raises(ValueError,match='literal'):
        validate_navigation(malformed)
