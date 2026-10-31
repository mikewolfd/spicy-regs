"""FEC source navigation distinguishes records from retained collection context."""

import pytest

from spicy_regs.explorer_navigation import declarations, published_navigation, target_keys, validate_navigation
from spicy_regs.table_joins import RETIRED_PROCESSING_JOINS


def recipe(name):
    return next(s for s in declarations(RETIRED_PROCESSING_JOINS) if s["id"] == name)


def witness(**changes):
    return {
        "target_table": "fec_receipts",
        "target_record_id": "sha256:" + "a" * 64,
        "target_generation_scope": "self",
        "witness_generation_scope": "external",
        "witness_generation_pin": "sha256:" + "b" * 64,
        "endpoint_kind": "source_record",
        "collection_id": "collection",
        "source_record_id": "original",
        "witness_sha256": "sha256:" + "c" * 64,
        "context_column": None,
        "context_pointer": None,
        **changes,
    }


CONTEXT_ONLY = (
    "fec_collection_selection",
    "fec_historical_ie_statistics",
    "fec_reference_filing_feed_items",
    "fec_research_meeting_observations",
    "fec_reference_source_pages",
    "fec_filing_definition_evidence",
)


def test_context_only_sources_keep_collection_routes_without_fictional_source_routes():
    specs = validate_navigation(declarations(RETIRED_PROCESSING_JOINS))
    for source in CONTEXT_ONLY:
        assert not any(s["id"] == "fec_source_row_" + source for s in specs)
        anchor = recipe("receipt_" + source + "_collection_id_fec_collections")
        assert anchor["targets"][0]["table"] == "fec_collections"
        assert all(p["path"] != ["source_record_id"] for k in anchor["targets"][0]["keys"] for p in k["parts"])


def test_record_and_context_witnesses_have_separate_routes_and_digests():
    record = recipe("fec_source_row_fec_record_evidence")["targets"][0]
    context = recipe("receipt_fec_record_evidence_collection_id_fec_collections")["targets"][0]
    row = witness()
    assert target_keys(record, {}, row) == ["collection", "original", row["witness_sha256"]]
    assert target_keys(context, {}, row) is None
    collection = witness(endpoint_kind="collection_context", source_record_id=None,
                         context_column="collection_outcome_json", context_pointer="/tableFieldDefinitions")
    assert target_keys(record, {}, collection) is None
    assert target_keys(context, {}, collection) == ["collection"]
    assert target_keys(context, {}, {**collection, "source_sha256": "unrelated"}) == ["collection"]


def test_literal_witness_guards_cannot_qualify_a_selected_generation():
    # Reproduces the remaining reader gate: target_keys has no publication
    # index. A valid but different external pin passes these literal guards.
    # Release preparation must refuse serving this route until metadata/reader
    # freshness compares the row's pin with the selected source generation.
    target = recipe("fec_source_row_fec_record_evidence")["targets"][0]
    row = witness()
    stale = row | {"witness_generation_pin": "sha256:" + "f" * 64}
    assert stale["witness_generation_pin"] != row["witness_generation_pin"]
    assert target_keys(target, {}, stale) == target_keys(target, {}, row)


def selected_witness_route(name="fec_source_row_fec_record_evidence", *, pins=True):
    spec = recipe(name)
    target_table = spec["targets"][0]["table"]
    schemas = {spec["source"]: [(field, "VARCHAR") for field in witness()],
               target_table: [(field, "VARCHAR") for field in spec["targets"][0]["columns"]],
               "fec_receipts": [("record_id", "VARCHAR")]}
    generations = {spec["source"]: "sha256:" + "a" * 64,
                   target_table: "sha256:" + "b" * 64,
                   "fec_receipts": "sha256:" + "a" * 64} if pins else {}
    return published_navigation([spec], schemas, generation_pins=generations)[0], schemas, generations


@pytest.mark.parametrize("name", [
    "fec_source_row_fec_record_evidence",
    "receipt_fec_record_evidence_collection_id_fec_collections",
    "receipt_fec_filing_definition_evidence_collection_id_fec_collections",
])
def test_selected_witness_generation_guards_reject_stale_and_wrong_target_scope(name):
    spec, schemas, generations = selected_witness_route(name)
    target = spec["targets"][0]
    row = witness()
    if target["table"] == "fec_collections":
        row.update(endpoint_kind="collection_context", source_record_id=None,
                   context_column="collection_outcome_json", context_pointer="/facts")
    assert target["directions"]["forward"]["available"]
    assert target_keys(target, {}, row) is not None
    assert target_keys(target, {}, row | {"witness_generation_pin": "sha256:" + "f" * 64}) is None
    assert target_keys(target, {}, row | {"target_table": "fec_source_records"}) is None
    # Rebuilding selected metadata replaces prior dynamic guards rather than
    # retaining old values alongside the fresh snapshot.
    generations[target["table"]] = "sha256:" + "f" * 64
    fresh = published_navigation([spec], schemas, generation_pins=generations)[0]["targets"][0]
    assert target_keys(fresh, {}, row) is None
    assert target_keys(fresh, {}, row | {"witness_generation_pin": "sha256:" + "f" * 64}) is not None


def test_unselected_witness_generations_disable_both_navigation_directions():
    spec, _, _ = selected_witness_route(pins=False)
    target = spec["targets"][0]
    assert not target["sourceAvailable"]
    assert "generation scope" in target["sourceUnavailableReason"]
    assert all(not direction["available"] for direction in target["directions"].values())


def test_metadata_builder_binds_witness_to_exact_index_and_changes_with_parent():
    from spicy_regs.explorer_metadata import build_bundle
    from tests.test_explorer_metadata import descriptor

    spec = recipe("fec_source_row_fec_record_evidence")
    row = witness()
    index = {"format": "spicy-regs-publication", "version": 2, "families": {
        "fec-query": {"artifactDigest": "sha256:" + "a" * 64, "tables": {
            "fec_record_evidence.parquet": descriptor(row), "fec_receipts.parquet": descriptor(["record_id"])}},
        "fec-observations": {"artifactDigest": row["witness_generation_pin"], "tables": {
            "fec_source_records.parquet": descriptor(["collection_id", "source_record_id", "source_sha256"])}}}}
    def bundle():
        return build_bundle(index, descriptions={},
                            registry={"sources": {}, "tables": {}, "families": {}},
                            join_record={"joins": [], "navigation": [spec]}, audit={})

    before = bundle()
    target = before["navigation"][0]["targets"][0]
    assert target_keys(target, {}, row) is not None
    assert target["generationScope"]["target"] == index["families"]["fec-query"]["artifactDigest"]
    index["families"]["fec-observations"]["artifactDigest"] = "sha256:" + "f" * 64
    fresh = bundle()["navigation"][0]["targets"][0]
    assert target_keys(fresh, {}, row) is None
    assert target_keys(fresh, {}, row | {"witness_generation_pin": "sha256:" + "f" * 64}) is not None
    index["families"]["fec-observations"]["artifactDigest"] = None
    unavailable = bundle()["navigation"][0]["targets"][0]
    assert not unavailable["directions"]["forward"]["available"]


def test_witness_measurement_and_proof_attachment_use_identical_generation_guards(tmp_path):
    import pyarrow as pa
    from spicy_regs.navigation_measurements import MeasurementCache, attached_directions
    from tests.test_remaining_main_navigation import selected

    row = witness(witness_generation_pin="sha256:" + "a" * 64)
    index, paths = selected(tmp_path, [
        ("fec_record_evidence", [row, row | {"witness_generation_pin": "sha256:" + "f" * 64}],
         pa.schema([(field, pa.string()) for field in row])),
        ("fec_source_records", [{"collection_id": "collection", "source_record_id": "original",
                                  "source_sha256": row["witness_sha256"]}], None),
        ("fec_receipts", [{"record_id": row["target_record_id"]}], None),
    ])
    cache = MeasurementCache(tmp_path / "cache")
    spec = recipe("fec_source_row_fec_record_evidence")
    proof = cache.measure(index, paths, spec)
    assert proof["eligible"] == proof["matched"] == 1
    assert proof["unsupportedReferences"] == 1
    assert attached_directions(index, spec, 0, proof) is not None
    from spicy_regs.navigation_measurement_runner import run_measurements
    census = run_measurements(index, paths, cache, all_routes=True, specs=[spec], metadata={}, policies={})
    assert census["census"][0]["status"] == "complete"
    assert census["results"][0]["binding"]["recipe"] == proof["binding"]["recipe"]
    assert census["results"][0]["eligible"] == census["results"][0]["matched"] == 1
    index["families"]["fixture"]["artifactDigest"] = "sha256:" + "f" * 64
    index["families"]["fixture"]["prefix"] = "generations/fixture/" + "f" * 64
    assert attached_directions(index, spec, 0, proof) is None


@pytest.mark.parametrize("scope", ["external", "self"])
def test_filing_navigation_checks_observations_pin_and_target_scope(scope):
    spec = recipe("native_filing_fec_receipts")
    row = {"filing_key": "urn:fec:filing:official-fec:openfec-file-number:123",
           "filing_association_status": "resolved_native_filing_key",
           "filing_association_target_generation_scope": scope,
           "filing_association_source_generation_pin": "sha256:" + "c" * 64,
           "filing_association_target_generation_pin": None if scope == "self" else "sha256:" + "b" * 64}
    schemas = {"fec_receipts": [(field, "VARCHAR") for field in row],
               "fec_filings": [("filing_key", "VARCHAR")], "fec_source_records": [("source_record_id", "VARCHAR")]}
    pins = {"fec_receipts": "sha256:" + "a" * 64,
            "fec_filings": "sha256:" + ("a" if scope == "self" else "b") * 64,
            "fec_source_records": "sha256:" + "c" * 64}
    bound = published_navigation([spec], schemas, generation_pins=pins)[0]
    target = bound["targets"][0 if scope == "external" else 1]
    assert target["sourceAvailable"]
    assert target_keys(target, {}, row) == [row["filing_key"]]
    assert target_keys(target, {}, row | {"filing_association_source_generation_pin": "sha256:" + "f" * 64}) is None
    if scope == "external":
        assert target_keys(target, {}, row | {"filing_association_target_generation_pin": "sha256:" + "f" * 64}) is None
    else:
        pins["fec_filings"] = "sha256:" + "f" * 64
        wrong_scope = published_navigation([spec], schemas, generation_pins=pins)[0]["targets"][1]
        assert not wrong_scope["sourceAvailable"]


def test_every_evidence_to_logical_target_route_requires_self_selected_generation():
    from spicy_regs.table_joins import Join
    # No such processing joins are currently declared. Cover the maintained
    # generator branch so a future logical target cannot bypass scope checks.
    logical_joins = tuple(Join("fec_record_evidence", ("target_record_id",), table, ("record_id",), 0, 0)
                          for table in ("fec_receipts", "fec_filings"))
    routes = [s for s in declarations((*RETIRED_PROCESSING_JOINS, *logical_joins))
              if s["id"].startswith("receipt_fec_record_evidence_target_record_id_")]
    assert routes
    for spec in routes:
        table = spec["targets"][0]["table"]
        row = witness(target_table=table)
        schemas = {"fec_record_evidence": [(field, "VARCHAR") for field in row],
                   table: [("record_id", "VARCHAR")]}
        pins = {"fec_record_evidence": "sha256:" + "a" * 64, table: "sha256:" + "a" * 64}
        target = published_navigation([spec], schemas, generation_pins=pins)[0]["targets"][0]
        assert target["sourceAvailable"]
        assert target_keys(target, {}, row) == [row["target_record_id"]]
        assert target_keys(target, {}, row | {"target_generation_scope": "external"}) is None
        pins[table] = "sha256:" + "f" * 64
        unavailable = published_navigation([spec], schemas, generation_pins=pins)[0]["targets"][0]
        assert not unavailable["directions"]["forward"]["available"]


@pytest.mark.parametrize("change", [
    {"target_generation_scope": "external"}, {"target_record_id": None}, {"target_table": None},
    {"witness_generation_scope": "self"}, {"witness_generation_pin": None},
    {"witness_generation_pin": "bad"}, {"witness_sha256": None},
    {"collection_id": None}, {"endpoint_kind": "unknown"},
])
def test_unavailable_witness_coordinates_and_scopes_refuse_both_branches(change):
    record = recipe("fec_source_row_fec_record_evidence")["targets"][0]
    context = recipe("receipt_fec_record_evidence_collection_id_fec_collections")["targets"][0]
    assert target_keys(record, {}, witness(**change)) is None
    assert target_keys(context, {}, witness(endpoint_kind="collection_context", source_record_id=None,
                                           context_column="collection_outcome_json", context_pointer="/facts") | change) is None


@pytest.mark.parametrize("route", [
    "receipt_fec_record_evidence_collection_id_fec_collections",
    "receipt_fec_filing_definition_evidence_collection_id_fec_collections",
])
@pytest.mark.parametrize("change", [{"context_pointer": None}, {"context_pointer": "facts"},
                                    {"context_column": "source_record_json"}])
def test_context_anchors_require_existing_column_and_pointer(route, change):
    row = witness(endpoint_kind="collection_context", source_record_id=None,
                  context_column="collection_outcome_json", context_pointer="/facts")
    assert target_keys(recipe(route)["targets"][0], {}, row | change) is None


def test_rebuilt_observation_anchor_requires_its_original_digest_without_record_identity():
    """Tables rebuilt from originals (ss-j4vz B7-B9) anchor to their collection by the original's digest."""
    target = recipe("receipt_fec_research_meeting_observations_collection_id_fec_collections")["targets"][0]
    row = {"collection_id": "capture", "source_sha256": "sha256:" + "d" * 64}
    assert target_keys(target, {}, row) == ["capture"]
    assert target_keys(target, {}, row | {"source_sha256": None}) is None
    assert target_keys(target, {}, row | {"source_sha256": "not-a-digest"}) is None


@pytest.mark.parametrize("side", ["subject", "object"])
def test_typed_relationship_routes_require_cycle_native_id_type_and_status(side):
    candidate, committee = recipe("fec_relationship_" + side)["targets"]
    row = {side + "_id": "H4NC05146", side + "_type": "candidate",
           side + "_id_status": "source_id_shape", side + "_endpoint_status": "lookup_eligible",
           "value_status": "reported", "cycle": 2024}
    assert target_keys(candidate, {}, row) == ["H4NC05146", "2024"]
    assert target_keys(committee, {}, row) is None
    for change in ({"cycle": None}, {side + "_id": None}, {side + "_type": "organization"},
                   {side + "_id_status": "invalid_source_id_shape"}, {side + "_endpoint_status": "name_only"}):
        assert target_keys(candidate, {}, row | change) is None
    if side == "object":
        assert target_keys(candidate, {}, row | {"value_status": "reported_none"}) is None
