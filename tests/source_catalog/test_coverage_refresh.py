"""Coverage authority, partial traversal and immutable refresh checks."""

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from spicy_regs.source_catalog.coverage import coverage
from spicy_regs.source_catalog.exporter import public_coverage, validate_public
from spicy_regs.source_catalog.refresh import compare, scope_key, write_report
from spicy_regs.source_catalog.storage import connect, digest, schema


def write(path, value):
    path.write_text(json.dumps(value))
    return path


@pytest.fixture
def index(tmp_path):
    db = connect(tmp_path / "index.sqlite")
    schema(db)
    db.execute("INSERT INTO metadata VALUES('generation','\"sha256:index\"')")
    db.execute("INSERT INTO collections VALUES('code','Code','{}')")
    yield db
    db.close()


def test_unknown_denominators_and_candidate_not_publication(index):
    index.execute(
        "INSERT INTO evidence VALUES(?,?,?,?)",
        (
            "proof",
            "code",
            "planned",
            json.dumps(
                {
                    "qualification_input": {"sha256": "proofhash"},
                    "qualification_scope": "selected structure only",
                    "local_candidate_outputs": [{"table": "structure", "sha256": "candidatehash"}],
                }
            ),
        ),
    )
    result = coverage(index)["collections"][0]
    assert result["completeness_denominator"] is None
    assert result["stages"]["expected"]["status"] == "unknown"
    assert result["stages"]["published"]["status"] == "unknown"
    processed = result["stages"]["processed"]["measurements"][0]
    assert processed["version"] == "candidatehash"
    assert processed["scope"]["qualification_scope"] == "selected structure only"
    assert processed["period"] is None


def test_publication_requires_capture_and_exact_table_artifact(index):
    index.execute(
        "INSERT INTO tables VALUES(?,?,?,?)",
        (
            "structure",
            "code",
            "code",
            json.dumps({"artifact_digest": "sha256:published"}),
        ),
    )
    assert coverage(index)["collections"][0]["stages"]["published"]["status"] == "unknown"
    index.execute(
        "INSERT INTO evidence VALUES(?,?,?,?)",
        (
            "capture",
            None,
            "corpus_catalog",
            json.dumps(
                {
                    "publication_capture": {
                        "at": "2026-10-09T00:00:00Z",
                        "sha256": "sha256:capture",
                    }
                }
            ),
        ),
    )
    published = coverage(index)["collections"][0]["stages"]["published"]["measurements"][0]
    assert published["version"] == "sha256:published"
    assert published["scope"]["table"] == "structure"
    assert published["scope"]["captured_at"] == "2026-10-09T00:00:00Z"
    assert published["evidence_ids"] == ["capture"]


def test_selected_candidate_record_count_keeps_scope_output_pin_and_unpublished_state(index):
    index.execute(
        "INSERT INTO evidence VALUES(?,?,?,?)",
        (
            "metadata-proof",
            "code",
            "planned",
            json.dumps(
                {
                    "qualification_input": {"sha256": "proofhash"},
                    "qualification_scope": "complete selected metadata; archive-wide coverage unknown",
                    "local_candidate_outputs": [{"table": "statements", "sha256": "outputhash", "rows": 5656}],
                }
            ),
        ),
    )
    result = coverage(index)["collections"][0]
    measured = result["stages"]["processed"]["measurements"][0]
    assert measured["count"] == 5656
    assert measured["unit"] == "qualified-candidate-table-record"
    assert measured["version"] == "outputhash"
    assert "archive-wide coverage unknown" in measured["scope"]["qualification_scope"]
    assert result["stages"]["published"]["status"] == "unknown"


def test_observation_cannot_bypass_publication_authority(index):
    index.execute(
        "INSERT INTO evidence VALUES(?,?,?,?)",
        (
            "asserted",
            "code",
            "observations",
            json.dumps(
                {
                    "coverage_measurement": {
                        "stage": "published",
                        "count": 99,
                        "unit": "sections",
                        "scope": {"table": "fiction"},
                        "basis": "claimed",
                        "version": "fiction",
                    }
                }
            ),
        ),
    )
    with pytest.raises(ValueError, match="Incomplete coverage measurement"):
        coverage(index)


def test_public_coverage_excludes_arbitrary_nested_fields_and_internal_paths():
    value = public_coverage(
        {
            "stages": {
                "expected": {
                    "status": "evidence-recorded",
                    "measurements": [
                        {
                            "count": 7,
                            "unit": "sections",
                            "version": "v1",
                            "verification": "basis",
                            "scope": {
                                "table": "safe",
                                "raw": {"token": "private"},
                                "qualification_scope": {"unknown": "private"},
                            },
                            "evidence_ids": ["/Users/private/proof"],
                            "period": {"label": "/Users/private/period", "raw": {"private": "value"}},
                        }
                    ],
                }
            }
        }
    )
    validate_public(value)
    assert value["stages"]["expected"]["measurements"] == []
    assert value["stages"]["expected"]["status"] == "scope-withheld"
    assert "private" not in json.dumps(value)


def test_public_scope_retains_supported_selected_meaning_and_withholds_other_queries():
    measurements = [
        {
            "count": 7,
            "unit": "sections",
            "scope": {"table": "structure", "qualification_scope": "Title 1 only"},
            "verification": "pinned proof",
        },
        {"count": 100, "unit": "items", "scope": {"collection_id": "code", "query": {"agency": "X"}}},
        {"count": 200, "unit": "items", "scope": {"collection_id": "code"}, "period": {"query": "selected"}},
        {"count": 300, "unit": "items", "scope": {"collection_id": None}},
    ]
    value = public_coverage({"stages": {"expected": {"status": "evidence-recorded", "measurements": measurements}}})
    validate_public(value)
    result = value["stages"]["expected"]["measurements"]
    assert len(result) == 1
    assert result[0]["scope"]["qualificationScope"] == "Title 1 only"
    assert result[0]["evidenceBasis"] == "pinned proof"


@pytest.mark.parametrize(
    "field,value",
    [
        ("scope", {"table": "structure", "qualification_scope": "/Users/private/selection"}),
        ("period", {"label": "/Users/private/selection"}),
        ("version", "https://alice:password@example.org/version"),
        ("verification", "https://example.org/proof?signature=secret"),
        ("scope", {"table": ""}),
    ],
)
def test_public_measurement_requires_safe_complete_selected_meaning(field, value):
    measured = {
        "count": 7,
        "unit": "sections",
        "scope": {"table": "structure"},
        "period": None,
        "version": "v1",
        "verification": "pinned proof",
    }
    measured[field] = value
    result = public_coverage(
        {
            "stages": {
                "expected": {
                    "status": "evidence-recorded",
                    "measurements": [measured],
                }
            }
        }
    )
    assert result["stages"]["expected"] == {"status": "scope-withheld", "measurements": []}
    validate_public(result)


def test_acquisition_requires_manifest_hash_and_presence(index):
    index.execute("INSERT INTO objects VALUES('sha256:body',3)")
    for ident, status, facts in [
        ("present", "held", {"encoded_bytes": 3}),
        ("receipt", "receipt-only", {"encoded_bytes": 3}),
        ("missing", "missing-bytes", {}),
    ]:
        index.execute(
            "INSERT INTO files VALUES(?,?,?,?,?,?,?,?,?)",
            (
                ident,
                "code",
                None,
                ident,
                status,
                "sha256:body",
                3,
                None,
                json.dumps(facts),
            ),
        )
    acquired = coverage(index)["collections"][0]["stages"]["acquired"]["measurements"]
    assert acquired[0]["count"] == 1
    assert acquired[0]["evidence_ids"] == ["present"]
    assert acquired[0]["route_id"] is None


def snapshot(path, names, *, observation="obs", scope="same", errors=None):
    return write(
        path,
        {
            "format": "spicy-source-discovery",
            "version": 1,
            "observations": [
                {
                    "id": observation,
                    "capture_url": "https://example.org/catalog",
                    "scope": {"selection": scope},
                    "metadata_kind": "dcat",
                    "observed_at": "2026-10-01T00:00:00Z",
                    "enumeration_complete": False,
                }
            ],
            "items": [
                {
                    "id": name,
                    "native_id": name,
                    "observation_id": observation,
                    "version_id": observation + name,
                    "raw": {"title": title},
                }
                for name, title in names
            ],
            "errors": errors or [],
        },
    )


def compare_now(old, new, **kwargs):
    return compare(old, new, now=datetime(2026, 10, 10, tzinfo=UTC), max_age_days=7, owner="B", **kwargs)


def test_partial_inventory_missing_items_are_not_removed_and_metadata_versions_ignored(tmp_path):
    old = snapshot(tmp_path / "old.json", [("same", "Same"), ("missing", "Missing"), ("changed", "Old")])
    new = snapshot(
        tmp_path / "new.json", [("same", "Same"), ("changed", "New"), ("added", "Added")], observation="fresh"
    )
    report = compare_now(old, new)
    assert report["new"] == ["added"]
    assert report["metadata_changed"] == ["changed"]
    assert report["removed"] == []
    assert report["not_seen_unconfirmed"] == ["missing"]
    assert report["freshness"][0]["stale"] is True


def completion(path, selected):
    qualification = write(path.parent / (path.stem + ".qualification.json"), {"checked": "stable traversal"})
    value = json.loads(selected.read_text())
    return write(
        path,
        {
            "format": "spicy-source-traversal-completion",
            "version": 1,
            "status": "complete",
            "snapshot_sha256": digest(selected),
            "scope_key": scope_key(value),
            "inventory_stable": True,
            "observation_ids": [o["id"] for o in value["observations"]],
            "qualification_input": {"path": str(qualification), "sha256": digest(qualification)},
        },
    )


def test_removal_requires_two_bound_complete_proofs_and_no_errors(tmp_path):
    old = snapshot(tmp_path / "old.json", [("gone", "Gone")])
    new = snapshot(tmp_path / "new.json", [], observation="fresh")
    old_proof = completion(tmp_path / "old-proof.json", old)
    new_proof = completion(tmp_path / "new-proof.json", new)
    report = compare_now(old, new, previous_completion=old_proof, current_completion=new_proof)
    assert report["removed"] == ["gone"]
    assert compare_now(old, new, current_completion=new_proof)["removed"] == []
    snapshot(new, [], observation="fresh", errors=[{"reason": "interrupted"}])
    completion(new_proof, new)
    assert compare_now(old, new, previous_completion=old_proof, current_completion=new_proof)["removed"] == []


def test_reject_scope_changes_duplicate_identity_and_proof_tampering(tmp_path):
    old = snapshot(tmp_path / "old.json", [("one", "One")])
    new = snapshot(tmp_path / "new.json", [], scope="different")
    with pytest.raises(ValueError, match="scopes"):
        compare_now(old, new)
    snapshot(new, [("one", "One"), ("one", "Other")])
    with pytest.raises(ValueError, match="unique identity"):
        compare_now(old, new)
    snapshot(new, [])
    proof = completion(tmp_path / "proof.json", new)
    value = json.loads(proof.read_text())
    Path(value["qualification_input"]["path"]).write_text("changed")
    with pytest.raises(ValueError, match="differs from its pin"):
        compare_now(old, new, current_completion=proof)


def test_reports_refuse_overwrite(tmp_path):
    path = tmp_path / "report.json"
    write_report(path, {"new": []})
    with pytest.raises(FileExistsError):
        write_report(path, {"new": ["unsafe"]})
    assert json.loads(path.read_text()) == {"new": []}


def test_declared_code_links_preserve_structure_only_scope():
    registry = json.loads(
        (Path(__file__).parents[2] / "src/spicy_regs/source_catalog/config/registry.v1.json").read_text()
    )
    declared = next(p for p in registry["planned"] if p["id"] == "declared:uscode:destination")
    destinations = {p["id"]: p for p in registry["planned"]}
    assert declared["status"] == "implemented-scoped-local"
    assert set(declared["tables"]) == {"uscode_editions", "uscode_edition_structure"}
    for ident in declared["destination_ids"]:
        assert destinations[ident]["qualification_input"] == declared["qualification_input"]
    assert declared["published"] is False
    assert "no normalized section-body extraction" in declared["qualification_scope"]
