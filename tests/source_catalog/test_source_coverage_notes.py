"""Source inventory reviews survive import and carry only across unchanged route and schema/policy."""
import json

import pytest

from spicy_regs.source_catalog.exporter import export, resolve_source_coverage_note, source_coverage_note
from spicy_regs.source_catalog.importer import Importer
from spicy_regs.source_catalog.publication import import_publications
from spicy_regs.source_catalog.selection import public_members
from spicy_regs.source_catalog.storage import Inputs, connect, digest, schema


VERSION = "sha256:" + "a" * 64
NEXT_VERSION = "sha256:" + "c" * 64
THIRD_VERSION = "sha256:" + "d" * 64
COLUMNS = [["id", "VARCHAR"], ["updated_at", "VARCHAR"]]
ROUTE = {
    "id": "source:api",
    "collection_id": "source",
    "label": "Publisher API",
    "delivery_method": "api",
    "lifecycle": "active",
    "custodian": "publisher",
    "content_behavior": "snapshot",
    "access_url": "https://api.example/v1/items",
    "periods": [],
}


def observation(**changes):
    return {"id": "source-coverage:example", "collection_id": "source",
            "metadata_kind": "source-coverage", "table": "example",
            "table_version": VERSION, "status": "unknown",
            "note": "Source coverage: unknown; expected versions are not fully enumerated.",
            **changes}


def bound_observation(**changes):
    return {
        **observation(),
        "family": "family",
        "recorded_at": "2026-10-27T12:00:00Z",
        "publication_member": {"columns": COLUMNS, "rows": 10, "byteSize": 100, "sha256": "sha256:" + "1" * 64},
        "source_routes": [{"source_id": "family:api", "publisher": "Publisher", "locator": "https://api.example/v1/items"}],
        "source_identifiers": ["publisher-api"],
        "parent_tables": [],
        **changes,
    }


def bound_table(**changes):
    return {
        "id": "example",
        "family": "family",
        "status": "publication-pinned",
        "artifact_digest": NEXT_VERSION,
        "published_at": "2026-11-27T04:00:00Z",
        "publication_descriptor": {"columns": COLUMNS, "rows": 12, "byteSize": 120, "sha256": "sha256:" + "2" * 64},
        **changes,
    }


def _export_single_table(tmp_path, *, config: dict, table_row: dict, name: str = "run"):
    index = tmp_path / f"{name}.sqlite"
    with connect(index) as db:
        schema(db)
        Importer(db, Inputs(tmp_path), config)
        db.executemany("INSERT INTO metadata VALUES(?,?)", [
            ("generation", json.dumps("sha256:" + "b" * 64)),
            ("generatedAt", json.dumps("2026-11-27T00:00:00Z")),
        ])
        db.execute(
            "INSERT INTO tables VALUES(?,?,?,?)",
            (table_row["id"], "source", table_row["family"], json.dumps(table_row)),
        )
        db.commit()
    output = tmp_path / f"public-{name}"
    manifest = export(index, output)
    detail = json.loads((output / manifest["collections"][0]["detailPath"]).read_text())
    return output, manifest, detail


def test_registry_review_replays_and_exports_without_processing_qualification(tmp_path):
    config = {"collections": [{"id": "source"}], "observations": [observation()]}
    for attempt, version in enumerate([VERSION, VERSION, NEXT_VERSION]):
        _, _, detail = _export_single_table(
            tmp_path,
            config=config,
            table_row={"id": "example", "family": "family", "status": "publication-pinned", "artifact_digest": version},
            name=f"legacy-{attempt}",
        )
        if version == VERSION:
            assert detail["tables"][0]["coverageNote"] == observation()["note"]
            assert "coverageNoteMatch" not in detail["tables"][0]
            assert "coverageNoteReviewedVersion" not in detail["tables"][0]
        else:
            assert "coverageNote" not in detail["tables"][0]
        assert detail["observations"] == []
        assert "publication" not in detail["tables"][0]
        assert detail["coverage"]["completenessDenominator"] is None


def test_review_for_another_table_version_does_not_transfer():
    evidence = [("observations", observation())]
    assert source_coverage_note({"id": "example", "artifact_digest": NEXT_VERSION}, evidence) is None
    assert source_coverage_note({"id": "other", "artifact_digest": VERSION}, evidence) is None


@pytest.mark.parametrize("version", [None, "unversioned", "sha256:abc"])
def test_unversioned_or_malformed_table_cannot_select_a_review(version):
    item = observation(table_version=version)
    assert source_coverage_note({"id": "example", "artifact_digest": version}, [("observations", item)]) is None
    item.pop("table_version")
    assert source_coverage_note({"id": "example"}, [("observations", item)]) is None


def test_exact_digest_match_is_not_carried_and_wins_over_other_reviews(tmp_path):
    older = bound_observation(
        id="source-coverage:example:old",
        table_version=VERSION,
        recorded_at="2026-10-20T00:00:00Z",
        note="Older reviewed coverage note.",
    )
    exact = bound_observation(
        id="source-coverage:example:exact",
        table_version=NEXT_VERSION,
        recorded_at="2026-11-27T04:00:00Z",
        prior_review=older["id"],
        note="Exact version reviewed coverage note.",
    )
    resolved = resolve_source_coverage_note(bound_table(artifact_digest=NEXT_VERSION), [("observations", older), ("observations", exact)])
    assert resolved == {
        "note": "Exact version reviewed coverage note.",
        "match": "exact",
        "carried": False,
        "reviewed_version": NEXT_VERSION,
    }
    output, manifest, detail = _export_single_table(
        tmp_path,
        config={"collections": [{"id": "source"}], "routes": [ROUTE], "observations": [older, exact]},
        table_row=bound_table(artifact_digest=NEXT_VERSION),
        name="exact-match",
    )
    exported_table = detail["tables"][0]
    assert exported_table["coverageNote"] == "Exact version reviewed coverage note."
    assert "coverageNoteMatch" not in exported_table
    assert "coverageNoteReviewedVersion" not in exported_table
    public_members(output, manifest["generation"])


def test_routine_data_only_republish_carries_note_and_flags_carried_in_export(tmp_path):
    obs = bound_observation(route_ids=[ROUTE["id"]], policy_version="family-policy/1")
    table = bound_table(policy_version="family-policy/1")
    resolved = resolve_source_coverage_note(table, [("observations", obs)], routes=[ROUTE])
    assert resolved == {
        "note": obs["note"],
        "match": "carried",
        "carried": True,
        "reviewed_version": VERSION,
    }
    output, manifest, detail = _export_single_table(
        tmp_path,
        config={"collections": [{"id": "source"}], "routes": [ROUTE], "observations": [obs]},
        table_row=table,
        name="carried-match",
    )
    exported_table = detail["tables"][0]
    assert exported_table["coverageNote"] == obs["note"]
    assert exported_table["coverageNoteMatch"] == "carried"
    assert exported_table["coverageNoteReviewedVersion"] == VERSION
    assert exported_table["artifactDigest"] == NEXT_VERSION
    assert detail["observations"] == []
    public_members(output, manifest["generation"])


def test_carry_selects_latest_non_superseded_review_at_or_before_live_generation():
    first = bound_observation(
        id="source-coverage:example:v1",
        table_version=VERSION,
        recorded_at="2026-10-27T12:00:00Z",
        note="Initial October review.",
    )
    second = bound_observation(
        id="source-coverage:example:v2",
        table_version=NEXT_VERSION,
        recorded_at="2026-11-11T12:00:00Z",
        prior_review=first["id"],
        note="Refreshed November review.",
    )
    future = bound_observation(
        id="source-coverage:example:v3",
        table_version="sha256:" + "e" * 64,
        recorded_at="2026-12-01T00:00:00Z",
        prior_review=second["id"],
        note="Future December review.",
    )
    evidence = [("observations", first), ("observations", second), ("observations", future)]
    live = bound_table(artifact_digest=THIRD_VERSION, published_at="2026-11-27T04:00:00Z")
    resolved = resolve_source_coverage_note(live, evidence)
    assert resolved == {
        "note": "Refreshed November review.",
        "match": "carried",
        "carried": True,
        "reviewed_version": NEXT_VERSION,
    }
    # A historical generation predating November only sees October's review.
    historical = bound_table(artifact_digest=THIRD_VERSION, published_at="2026-11-01T00:00:00Z")
    assert resolve_source_coverage_note(historical, evidence) == {
        "note": "Initial October review.",
        "match": "carried",
        "carried": True,
        "reviewed_version": VERSION,
    }
    # A generation predating every review carries nothing.
    too_old = bound_table(artifact_digest=THIRD_VERSION, published_at="2026-10-01T00:00:00Z")
    assert resolve_source_coverage_note(too_old, evidence) is None


def test_acquisition_route_changes_invalidate_carry_forward(tmp_path):
    # 1. Collection route ID renamed or added/removed in SQLite routes
    obs_route_id = bound_observation(route_id=ROUTE["id"])
    assert resolve_source_coverage_note(bound_table(), [("observations", obs_route_id)], routes=[ROUTE]) is not None
    renamed_route = {**ROUTE, "id": "source:bulk"}
    assert resolve_source_coverage_note(bound_table(), [("observations", obs_route_id)], routes=[renamed_route]) is None
    assert resolve_source_coverage_note(bound_table(), [("observations", obs_route_id)], routes=[ROUTE, renamed_route]) is None

    # 2. Route definition changed (delivery_method / access_url / custodian) on same route ID
    obs_route_def = bound_observation(routes=[ROUTE])
    assert resolve_source_coverage_note(bound_table(), [("observations", obs_route_def)], routes=[ROUTE]) is not None
    changed_delivery = {**ROUTE, "delivery_method": "bulk"}
    changed_url = {**ROUTE, "access_url": "https://bulk.example/v2/archive.zip"}
    assert resolve_source_coverage_note(bound_table(), [("observations", obs_route_def)], routes=[changed_delivery]) is None
    assert resolve_source_coverage_note(bound_table(), [("observations", obs_route_def)], routes=[changed_url]) is None

    # 3. Table explicit route_id, source_routes, source_identifiers, parent_tables, or derived-from change
    assert resolve_source_coverage_note(bound_table(route_id="source:bulk"), [("observations", obs_route_id)], routes=[ROUTE, renamed_route]) is None
    assert resolve_source_coverage_note(
        bound_table(source_routes=[{"source_id": "family:bulk", "publisher": "Publisher", "locator": "https://bulk.example/archive.zip"}]),
        [("observations", bound_observation())],
    ) is None
    assert resolve_source_coverage_note(
        bound_table(source_identifiers=["different-source"]),
        [("observations", bound_observation())],
    ) is None
    assert resolve_source_coverage_note(
        bound_table(parent_tables=["upstream_table"]),
        [("observations", bound_observation(parent_tables=[]))],
    ) is None
    assert resolve_source_coverage_note(
        bound_table(family_parent_tables=["upstream_table"]),
        [("observations", bound_observation(recorded_family_parent_tables=[]))],
    ) is None
    assert resolve_source_coverage_note(
        bound_table(),
        [
            ("observations", bound_observation(parent_tables=[])),
            ("relationships", {"kind": "derived-from", "from_id": "example", "to_id": "upstream_table"}),
        ],
    ) is None

    # 4. Family change or missing route declaration on observation fails closed
    assert resolve_source_coverage_note(bound_table(family="other-family"), [("observations", bound_observation())]) is None
    no_routes_obs = bound_observation()
    for key in ("source_routes", "source_identifiers", "parent_tables"):
        no_routes_obs.pop(key)
    assert resolve_source_coverage_note(bound_table(), [("observations", no_routes_obs)]) is None

    # 5. End-to-end export omits coverageNote when route changed in SQLite
    _, _, detail = _export_single_table(
        tmp_path,
        config={"collections": [{"id": "source"}], "routes": [changed_delivery], "observations": [obs_route_def]},
        table_row=bound_table(),
        name="route-invalidated",
    )
    assert "coverageNote" not in detail["tables"][0]
    assert "coverageNoteMatch" not in detail["tables"][0]


def test_schema_and_policy_changes_invalidate_carry_forward(tmp_path):
    obs = bound_observation(policy_version="policy/v1")
    matching_table = bound_table(policy_version="policy/v1")
    assert resolve_source_coverage_note(matching_table, [("observations", obs)]) is not None

    # Column added, removed, retyped, reordered, or partitionColumns changed
    schema_variants = [
        {"columns": [*COLUMNS, ["extra_col", "VARCHAR"]]},
        {"columns": [COLUMNS[0]]},
        {"columns": [["id", "BIGINT"], COLUMNS[1]]},
        {"columns": [COLUMNS[1], COLUMNS[0]]},
        {"columns": COLUMNS, "partitionColumns": ["id"]},
    ]
    for desc in schema_variants:
        table = bound_table(policy_version="policy/v1", publication_descriptor=desc)
        assert resolve_source_coverage_note(table, [("observations", obs)]) is None

    # Partitioned table carries when partitionColumns match, invalidates when changed
    part_obs = bound_observation(
        publication_member={"columns": COLUMNS, "partitionColumns": ["id"], "rows": 10, "byteSize": 100},
    )
    part_table = bound_table(
        publication_descriptor={"columns": COLUMNS, "partitionColumns": ["id"], "rows": 20, "byteSize": 200},
    )
    assert resolve_source_coverage_note(part_table, [("observations", part_obs)]) is not None
    assert resolve_source_coverage_note(
        bound_table(publication_descriptor={"columns": COLUMNS, "partitionColumns": ["updated_at"], "rows": 20, "byteSize": 200}),
        [("observations", part_obs)],
    ) is None

    # Policy version / structured policy change or one-sided omission invalidates
    assert resolve_source_coverage_note(bound_table(policy_version="policy/v2"), [("observations", obs)]) is None
    assert resolve_source_coverage_note(bound_table(), [("observations", obs)]) is None
    assert resolve_source_coverage_note(bound_table(policy_version="policy/v1"), [("observations", bound_observation())]) is None
    structured_obs = bound_observation(policy={"policy_version": "v1", "identity_fields": ["id"]})
    assert resolve_source_coverage_note(
        bound_table(policy={"policy_version": "v1", "identity_fields": ["id"]}),
        [("observations", structured_obs)],
    ) is not None
    assert resolve_source_coverage_note(
        bound_table(policy={"policy_version": "v1", "identity_fields": ["id", "updated_at"]}),
        [("observations", structured_obs)],
    ) is None

    # End-to-end export omits coverageNote on schema change
    _, _, detail = _export_single_table(
        tmp_path,
        config={"collections": [{"id": "source"}], "routes": [ROUTE], "observations": [obs]},
        table_row=bound_table(policy_version="policy/v1", publication_descriptor={"columns": [*COLUMNS, ["extra", "VARCHAR"]]}),
        name="schema-invalidated",
    )
    assert "coverageNote" not in detail["tables"][0]


def test_conflicting_reviews_refuse_export_selection():
    # Conflicting exact-digest reviews refuse
    evidence = [("observations", observation()), ("observations", observation(note="Different source coverage."))]
    with pytest.raises(ValueError, match="Conflicting source coverage"):
        source_coverage_note({"id": "example", "artifact_digest": VERSION}, evidence)

    # Conflicting carried reviews tied at the same timestamp refuse
    tied_a = bound_observation(id="source-coverage:example:a", note="First coverage claim.")
    tied_b = bound_observation(id="source-coverage:example:b", note="Second conflicting claim.")
    with pytest.raises(ValueError, match="Conflicting source coverage"):
        resolve_source_coverage_note(bound_table(), [("observations", tied_a), ("observations", tied_b)])

    # Conflicting carried reviews without timestamps or prior_review refuse
    undated_a = bound_observation(id="source-coverage:example:ua", note="Undated claim A.")
    undated_a.pop("recorded_at")
    undated_b = bound_observation(id="source-coverage:example:ub", table_version=THIRD_VERSION, note="Undated claim A.")
    undated_b.pop("recorded_at")
    with pytest.raises(ValueError, match="Conflicting source coverage"):
        resolve_source_coverage_note(bound_table(), [("observations", undated_a), ("observations", undated_b)])

    # Exact duplicate review (same note and same reviewed version) resolves unambiguously
    dup = bound_observation(id="source-coverage:example:dup")
    assert resolve_source_coverage_note(bound_table(), [("observations", bound_observation()), ("observations", dup)]) == {
        "note": bound_observation()["note"],
        "match": "carried",
        "carried": True,
        "reviewed_version": VERSION,
    }


def test_publication_import_persists_timestamp_and_family_parents_for_carry_checks(tmp_path):
    pub_path = tmp_path / "publication.v2.json"
    pub = {
        "format": "spicy-regs-publication",
        "version": 2,
        "families": {
            "family": {
                "prefix": "generations/family/" + NEXT_VERSION.removeprefix("sha256:"),
                "logicalId": "urn:family",
                "artifactDigest": NEXT_VERSION,
                "publishedAt": "2026-11-27T04:00:00Z",
                "tables": {
                    "example.parquet": {
                        "sha256": "sha256:" + "3" * 64,
                        "byteSize": 100,
                        "rows": 15,
                        "columns": COLUMNS,
                    }
                },
            }
        },
    }
    pub_path.write_text(json.dumps(pub))
    selection_path = tmp_path / "selection.json"
    selection_path.write_text(json.dumps({
        "format": "spicy-publication-reconciliation",
        "version": 1,
        "publication": {"path": pub_path.name, "sha256": digest(pub_path), "bytes": pub_path.stat().st_size},
        "receipts": [],
    }))
    obs = bound_observation(recorded_family_parent_tables=[])
    index = tmp_path / "catalog.sqlite"
    with connect(index) as db:
        schema(db)
        Importer(db, Inputs(tmp_path), {
            "collections": [{"id": "source", "families": ["family"]}],
            "routes": [ROUTE],
            "observations": [obs],
        })
        db.executemany("INSERT INTO metadata VALUES(?,?)", [
            ("generation", json.dumps("sha256:" + "b" * 64)),
            ("generatedAt", json.dumps("2026-11-27T04:05:00Z")),
        ])
        import_publications(db, Inputs(tmp_path), selection_path)
        db.commit()
    output = tmp_path / "public"
    manifest = export(index, output)
    detail = json.loads((output / manifest["collections"][0]["detailPath"]).read_text())
    exported_table = detail["tables"][0]
    assert exported_table["coverageNote"] == obs["note"]
    assert exported_table["coverageNoteMatch"] == "carried"
    assert exported_table["coverageNoteReviewedVersion"] == VERSION
    public_members(output, manifest["generation"])


@pytest.mark.parametrize("tamper", [
    {"coverageNoteMatch": "guessed"},
    {"coverageNoteMatch": "carried", "coverageNoteReviewedVersion": None},
    {"coverageNoteMatch": "carried", "coverageNoteReviewedVersion": NEXT_VERSION},
    {"coverageNoteMatch": None, "coverageNoteReviewedVersion": VERSION},
])
def test_selection_public_members_rejects_invalid_carried_note_metadata(tmp_path, tamper):
    obs = bound_observation()
    output, manifest, detail = _export_single_table(
        tmp_path,
        config={"collections": [{"id": "source"}], "routes": [ROUTE], "observations": [obs]},
        table_row=bound_table(),
        name="tamper",
    )
    table = detail["tables"][0]
    for key, value in tamper.items():
        if value is None:
            table.pop(key, None)
        else:
            table[key] = value
    detail_path = output / manifest["collections"][0]["detailPath"]
    detail_path.write_text(json.dumps(detail, separators=(",", ":")) + "\n")
    with pytest.raises(ValueError):
        public_members(output, manifest["generation"])
