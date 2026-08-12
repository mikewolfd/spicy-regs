"""The SourceCatalogRelease v1 producer: one fixture universe, and its refusals.

No network. The Mirrulations leg drives the real draw and fetch machinery
against an in-memory fake S3 resource, so the adapter is exercised through the
same code path a production run takes.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from spicy_regs.corpora import mirrulations_document_corpus as corpus
from spicy_regs.document_release_v3 import canonical_json_bytes
from spicy_regs.source_catalog import (
    CandidateRendition,
    DiscoveredItem,
    NormalizationPolicy,
    NormalizedDraft,
    Observation,
    ObservedTopic,
    SourceCatalogError,
    SourceOutcome,
    UniverseScope,
    UniverseSpec,
    build_source_catalog_release,
    pinned_schemas,
    publish_source_catalog_release,
    schema_set_identity,
    select_source_items,
    validate_bundle_records,
    verify_bundle_directory,
)
from spicy_regs.source_catalog import mirrulations as mirrulations_adapter
from spicy_regs.source_catalog.records import ROOT_OBJECT_KEY, SOURCE_ITEMS_OBJECT_KEY, release_identity
from spicy_regs.source_catalog.schema_pins import PINNED_SCHEMA_DIR, PINS_FILE

PUBLISHED_AT = "2026-08-12T00:00:00Z"

# The universe is configuration. These identifiers name a test fixture and
# nothing in the product; the corpus universe is the product owner's to name.
FIXTURE_UNIVERSE = "urn:spicy-regs:source-universe:fixture-universe"
FIXTURE_CATALOG = "urn:spicy-regs:source-catalog:fixture-universe"
FIXTURE_POLICY = "urn:spicy-regs:selection-policy:fixture-universe-rules-only"


def _spec(**overrides: Any) -> UniverseSpec:
    defaults: dict[str, Any] = {
        "universe_id": FIXTURE_UNIVERSE,
        "catalog_id": FIXTURE_CATALOG,
        "policy_id": FIXTURE_POLICY,
        "policy_version": "1.0",
        "source_system_id": "https://mirrulations.s3.amazonaws.com",
        "source_system_version": "raw-data-v1",
        "normalization": NormalizationPolicy(language="en", agency_names={"SEC": "Securities and Exchange Commission"}),
        "scope": UniverseScope(document_types=("Rule",)),
    }
    return UniverseSpec(**(defaults | overrides))


def _rendition(name: str, *, digest: str | None = None, size: int | None = 512) -> CandidateRendition:
    return CandidateRendition(
        rendition_id=name,
        media_type="text/html",
        locator=f"https://example.invalid/{name}.htm",
        expected_sha256=digest,
        expected_byte_size=size,
    )


def _draft(**overrides: Any) -> NormalizedDraft:
    defaults: dict[str, Any] = {
        "title": "A fixture rule",
        "agency_ids": ("SEC",),
        "document_type": "Rule",
        "publication_date": "2026-01-05",
        "last_updated_date": "2026-01-06",
        "docket_ids": ("SEC-2026-0001",),
        "regulation_identifier_numbers": ("3235-AN19",),
        "comment_close_date": None,
        "source_url": "https://api.regulations.gov/v4/documents/SEC-2026-0001-0001",
    }
    return NormalizedDraft(**(defaults | overrides))


def _hand_built_universe() -> tuple[DiscoveredItem, ...]:
    """Five discovery records: one per disposition the contract admits."""

    return (
        DiscoveredItem(
            source_item_id="regulations.gov/SEC-2026-0001-0001",
            document_id="SEC-2026-0001-0001",
            source_issued_version="2026-01-06T05:00:00Z",
            source_native_metadata={"id": "SEC-2026-0001-0001", "type": "documents"},
            normalized=_draft(),
            observed_topics=(
                ObservedTopic(
                    observed_topic_id="market-structure",
                    observed_topic_scheme="regulations.gov/topics",
                    label="Market structure",
                ),
            ),
            observations=(Observation(observation_key="frDocNum", observation_value="2026-00042"),),
            renditions=(_rendition("mirrulations-mirror", digest="sha256:" + "a" * 64),),
        ),
        DiscoveredItem(
            source_item_id="regulations.gov/SEC-2026-0002-0001",
            document_id="SEC-2026-0002-0001",
            source_issued_version="2026-01-07T05:00:00Z",
            source_native_metadata={"id": "SEC-2026-0002-0001", "type": "documents"},
            normalized=_draft(document_type="Notice", title="A fixture notice"),
            renditions=(_rendition("mirrulations-mirror"),),
        ),
        DiscoveredItem(
            source_item_id="regulations.gov/SEC-2026-0003-0001",
            document_id="SEC-2026-0003-0001",
            source_issued_version="2026-01-08T05:00:00Z",
            source_native_metadata={"id": "SEC-2026-0003-0001", "type": "documents"},
            normalized=_draft(title="A withdrawn rule"),
            outcome=SourceOutcome.DELETED,
            outcome_reason_code="source.withdrawn-after-publication",
            outcome_reason="The source marks this document withdrawn.",
        ),
        DiscoveredItem(
            source_item_id="regulations.gov/SEC-2026-0004-0001",
            document_id="SEC-2026-0004-0001",
            source_issued_version="2026-01-09T05:00:00Z",
            source_native_metadata={"id": "SEC-2026-0004-0001", "type": "documents"},
            normalized=_draft(title="A rule with nothing to capture"),
            renditions=(),
        ),
        DiscoveredItem(
            source_item_id="regulations.gov/SEC-2026-0005-0001",
            document_id="SEC-2026-0005-0001",
            source_issued_version="2026-01-10T05:00:00Z",
            source_native_metadata={"id": "SEC-2026-0005-0001", "raw": "502 Bad Gateway"},
            normalized=None,
            outcome=SourceOutcome.FAILED,
            outcome_reason_code="source.metadata-unparsable",
            outcome_reason="The source metadata record could not be parsed.",
        ),
    )


def _dispositions(items: list[dict[str, Any]]) -> dict[str, str]:
    return {item["sourceItemId"]: item["selection"]["disposition"] for item in items}


# ─── the pinned schema bytes ───────────────────────────────────────────


def test_pinned_schemas_match_their_recorded_digests_and_schema_set_identity() -> None:
    pins = json.loads(PINS_FILE.read_text(encoding="utf-8"))
    for entry in pins["schemas"]:
        payload = (PINNED_SCHEMA_DIR / entry["fileName"]).read_bytes()
        assert hashlib.sha256(payload).hexdigest() == entry["sha256"]
        assert len(payload) == entry["byteSize"]
        assert json.loads(payload)["$id"] == entry["schemaId"]
    assert schema_set_identity() == pins["schemaSetId"]
    # The identity Rulespec's own sealed fixture carries over the same bytes.
    assert pins["schemaSetId"] == (
        "urn:spicy:schema-set:v1:cffb8f62a70754beb07aa46886649506487712f7a5efea4e0dcc2021b99f02d6"
    )
    assert set(pinned_schemas()) == {"release-root", "member-manifest", "source-items"}


# ─── the universe specification ────────────────────────────────────────


def test_policy_digest_is_stable_across_spellings_and_moves_with_the_rules() -> None:
    first = _spec(scope=UniverseScope(document_types=("Rule", "Proposed Rule"), agency_ids=("SEC", "EPA")))
    second = _spec(scope=UniverseScope(document_types=("Proposed Rule", "Rule"), agency_ids=("EPA", "SEC", "EPA")))
    assert first.policy_sha256() == second.policy_sha256()

    widened = _spec(scope=UniverseScope(document_types=("Rule",)))
    assert widened.policy_sha256() != first.policy_sha256()

    # Renaming the policy leaves the digest alone: the digest states what the
    # policy is, and policyId states what it is called.
    renamed = _spec(policy_id="urn:spicy-regs:selection-policy:renamed", scope=first.scope)
    assert renamed.policy_sha256() == first.policy_sha256()
    assert renamed.selection_policy_record()["policyId"] != first.selection_policy_record()["policyId"]


def test_universe_specification_refuses_an_undeclared_language() -> None:
    with pytest.raises(SourceCatalogError, match="normalization.language is required"):
        UniverseSpec.from_mapping(
            {
                "universeId": FIXTURE_UNIVERSE,
                "catalogId": FIXTURE_CATALOG,
                "selectionPolicy": {"policyId": FIXTURE_POLICY, "policyVersion": "1.0"},
                "sourceSystem": {"sourceSystemId": "https://example.invalid", "sourceSystemVersion": "1"},
                "normalization": {"agencyNames": {}},
            }
        )


def test_universe_specification_round_trips_through_configuration(tmp_path: Path) -> None:
    config = {
        "universeId": FIXTURE_UNIVERSE,
        "catalogId": FIXTURE_CATALOG,
        "selectionPolicy": {"policyId": FIXTURE_POLICY, "policyVersion": "1.0"},
        "sourceSystem": {
            "sourceSystemId": "https://mirrulations.s3.amazonaws.com",
            "sourceSystemVersion": "raw-data-v1",
        },
        "scope": {
            "documentTypes": ["Rule"],
            "locationPrefixes": ["raw-data/SEC/"],
            "publicationWindow": {"from": "2026-01-01", "to": "2026-12-31"},
            "maxItems": 25,
        },
        "normalization": {"language": "en", "agencyNames": {"SEC": "Securities and Exchange Commission"}},
    }
    path = tmp_path / "universe.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    from spicy_regs.source_catalog import load_universe_spec

    spec = load_universe_spec(path)
    assert spec.universe_id == FIXTURE_UNIVERSE
    assert spec.scope.max_items == 25
    assert spec.policy_document()["scope"]["publicationWindow"] == {"from": "2026-01-01", "to": "2026-12-31"}
    assert spec.policy_sha256() == UniverseSpec.from_mapping(config).policy_sha256()


# ─── the produced bundle ───────────────────────────────────────────────


def test_fixture_universe_produces_one_row_per_disposition() -> None:
    rows = select_source_items(_spec(), _hand_built_universe())
    assert _dispositions(rows) == {
        "regulations.gov/SEC-2026-0001-0001": "selected",
        "regulations.gov/SEC-2026-0002-0001": "excluded",
        "regulations.gov/SEC-2026-0003-0001": "deleted",
        "regulations.gov/SEC-2026-0004-0001": "unavailable",
        "regulations.gov/SEC-2026-0005-0001": "failed",
    }
    by_id = {row["sourceItemId"]: row for row in rows}
    assert by_id["regulations.gov/SEC-2026-0002-0001"]["selection"]["reasonCode"] == (
        "policy.document-type-out-of-scope"
    )
    assert by_id["regulations.gov/SEC-2026-0004-0001"]["selection"]["reasonCode"] == ("source.no-candidate-rendition")
    # A selected row carries all ten normalized fields; the two the source never
    # states come from the declared policy, not from a placeholder.
    normalized = by_id["regulations.gov/SEC-2026-0001-0001"]["normalizedMetadata"]
    assert normalized["language"] == "en"
    assert normalized["agencies"] == [{"agencyId": "SEC", "agencyName": "Securities and Exchange Commission"}]
    assert set(normalized) == {
        "agencies",
        "commentCloseDate",
        "docketIds",
        "documentType",
        "language",
        "lastUpdatedDate",
        "publicationDate",
        "regulationIdentifierNumbers",
        "sourceUrl",
        "title",
    }
    # The failed row carries a null normalized view, which is the schema's own
    # answer for a non-selected item whose source served nothing usable.
    assert by_id["regulations.gov/SEC-2026-0005-0001"]["normalizedMetadata"] is None


def test_an_exhausted_item_budget_excludes_rather_than_drops() -> None:
    items = _hand_built_universe()
    second_rule = DiscoveredItem(
        source_item_id="regulations.gov/SEC-2026-0006-0001",
        document_id="SEC-2026-0006-0001",
        source_issued_version="2026-01-12T05:00:00Z",
        source_native_metadata={"id": "SEC-2026-0006-0001"},
        normalized=_draft(title="A second fixture rule"),
        renditions=(_rendition("mirrulations-mirror"),),
    )
    rows = select_source_items(_spec(scope=UniverseScope(document_types=("Rule",), max_items=1)), (*items, second_rule))
    # Every discovered item still appears exactly once; the budget changes the
    # disposition, never the membership of U.
    assert len(rows) == 6
    assert _dispositions(rows)["regulations.gov/SEC-2026-0006-0001"] == "excluded"
    budgeted = next(row for row in rows if row["sourceItemId"] == "regulations.gov/SEC-2026-0006-0001")
    assert budgeted["selection"]["reasonCode"] == "policy.item-budget-exhausted"


def test_built_bundle_validates_writes_and_round_trips(tmp_path: Path) -> None:
    bundle = build_source_catalog_release(
        _spec(), _hand_built_universe(), published_at=PUBLISHED_AT, release_status="fixture"
    )
    assert bundle.release_id.startswith("urn:spicy-regs:source-catalog-release:v1:")
    content = bundle.root["content"]
    assert content["counts"] == {
        "deletedCount": 1,
        "discoveredCount": 5,
        "excludedCount": 1,
        "failedCount": 1,
        "memberCount": 4,
        "selectedCount": 1,
        "totalMemberByteSize": sum(member["byteSize"] for member in bundle.manifest["members"]),
        "unavailableCount": 1,
    }
    assert content["coverage"] == {
        "accountedCount": 5,
        "distinctSelectedDocumentIdCount": 1,
        "selectedWithCandidateRenditionCount": 1,
        "unaccountedCount": 0,
    }
    assert content["selectionPolicy"]["policySha256"] == _spec().policy_sha256()

    output = tmp_path / "catalog"
    written = bundle.write(output)
    assert sorted(path.relative_to(written).as_posix() for path in written.rglob("*") if path.is_file()) == [
        "data/source-items.json",
        "manifests/global.json",
        "release.json",
        "schemas/member-manifest-v1.schema.json",
        "schemas/source-catalog-release-v1.schema.json",
        "schemas/source-items-v1.schema.json",
    ]
    root = verify_bundle_directory(written)
    assert root["releaseId"] == bundle.release_id
    assert root["annotations"] == {"publishedAt": PUBLISHED_AT, "releaseStatus": "fixture"}
    # The bundle carries the schema bytes, so a consumer needs no Rulespec checkout.
    for role, schema in pinned_schemas().items():
        assert (written / "schemas" / schema.file_name).read_bytes() == schema.payload, role


def test_publication_refuses_to_replace_an_existing_bundle(tmp_path: Path) -> None:
    output = tmp_path / "catalog"
    receipt = publish_source_catalog_release(
        _spec(), _hand_built_universe(), output, published_at=PUBLISHED_AT, build_run_id="fixture-run"
    )
    assert receipt["releaseId"].startswith("urn:spicy-regs:source-catalog-release:v1:")
    assert receipt["counts"]["selectedCount"] == 1
    with pytest.raises(SourceCatalogError, match="refusing to replace"):
        publish_source_catalog_release(_spec(), _hand_built_universe(), output, published_at=PUBLISHED_AT)


# ─── determinism ───────────────────────────────────────────────────────


def test_identical_inputs_produce_identical_bytes_and_publishedat_stays_out_of_identity() -> None:
    first = build_source_catalog_release(_spec(), _hand_built_universe(), published_at=PUBLISHED_AT)
    second = build_source_catalog_release(_spec(), _hand_built_universe(), published_at=PUBLISHED_AT)
    assert dict(first.files) == dict(second.files)

    later = build_source_catalog_release(
        _spec(), _hand_built_universe(), published_at="2027-03-04T09:15:00Z", build_run_id="second-run"
    )
    assert later.release_id == first.release_id
    assert later.files[ROOT_OBJECT_KEY] != first.files[ROOT_OBJECT_KEY]
    assert later.files[SOURCE_ITEMS_OBJECT_KEY] == first.files[SOURCE_ITEMS_OBJECT_KEY]

    # Discovery order does not move the bundle: rows are sorted by identity.
    shuffled = tuple(reversed(_hand_built_universe()))
    assert dict(build_source_catalog_release(_spec(), shuffled, published_at=PUBLISHED_AT).files) == dict(first.files)


# ─── refusals ──────────────────────────────────────────────────────────


def test_duplicate_source_item_id_is_refused() -> None:
    items = _hand_built_universe()
    duplicate = DiscoveredItem(
        source_item_id=items[0].source_item_id,
        document_id="SEC-2026-0009-0001",
        source_issued_version="2026-01-11T05:00:00Z",
        source_native_metadata={"id": "SEC-2026-0009-0001"},
        normalized=_draft(),
        renditions=(_rendition("mirrulations-mirror"),),
    )
    with pytest.raises(SourceCatalogError, match="duplicate sourceItemId"):
        build_source_catalog_release(_spec(), (*items, duplicate), published_at=PUBLISHED_AT)


def test_two_selected_items_may_not_claim_one_document_id() -> None:
    first, *_ = _hand_built_universe()
    second = DiscoveredItem(
        source_item_id="regulations.gov/SEC-2026-0001-0002",
        document_id=first.document_id,
        source_issued_version="2026-01-11T05:00:00Z",
        source_native_metadata={"id": "SEC-2026-0001-0002"},
        normalized=_draft(),
        renditions=(_rendition("mirrulations-mirror"),),
    )
    with pytest.raises(SourceCatalogError, match="already claimed by a selected item"):
        build_source_catalog_release(_spec(), (first, second), published_at=PUBLISHED_AT)


def test_a_non_selected_row_without_a_reason_is_refused() -> None:
    bundle = build_source_catalog_release(_spec(), _hand_built_universe(), published_at=PUBLISHED_AT)
    items = json.loads(json.dumps(list(bundle.items)))
    excluded = next(item for item in items if item["selection"]["disposition"] == "excluded")
    del excluded["selection"]["reason"]
    with pytest.raises(SourceCatalogError, match="selection/reason is required"):
        validate_bundle_records(root=bundle.root, manifest=bundle.manifest, items=items)

    del excluded["selection"]["reasonCode"]
    with pytest.raises(SourceCatalogError, match="selection/reasonCode is required"):
        validate_bundle_records(root=bundle.root, manifest=bundle.manifest, items=items)


def test_a_selected_row_without_a_candidate_rendition_is_refused() -> None:
    bundle = build_source_catalog_release(_spec(), _hand_built_universe(), published_at=PUBLISHED_AT)
    items = json.loads(json.dumps(list(bundle.items)))
    selected = next(item for item in items if item["selection"]["disposition"] == "selected")
    selected["candidateRenditions"] = []
    with pytest.raises(SourceCatalogError, match="a selected item requires at least one"):
        validate_bundle_records(root=bundle.root, manifest=bundle.manifest, items=items)


def test_a_source_observed_topic_may_not_carry_a_refspec_concept_identifier() -> None:
    first, *rest = _hand_built_universe()
    tainted = DiscoveredItem(
        source_item_id=first.source_item_id,
        document_id=first.document_id,
        source_issued_version=first.source_issued_version,
        source_native_metadata=first.source_native_metadata,
        normalized=first.normalized,
        observed_topics=(
            ObservedTopic(
                observed_topic_id="urn:ref:concept:federal-register-thesaurus:market-structure",
                observed_topic_scheme="regulations.gov/topics",
                label="Market structure",
            ),
        ),
        renditions=first.renditions,
    )
    with pytest.raises(SourceCatalogError, match="must not carry a RefSpec concept"):
        build_source_catalog_release(_spec(), (tainted, *rest), published_at=PUBLISHED_AT)


def test_a_tampered_set_digest_is_refused_on_read_back(tmp_path: Path) -> None:
    output = tmp_path / "catalog"
    build_source_catalog_release(_spec(), _hand_built_universe(), published_at=PUBLISHED_AT).write(output)

    root = json.loads((output / ROOT_OBJECT_KEY).read_text(encoding="utf-8"))
    root["content"]["selectedSourceSetDigest"] = "sha256:" + "0" * 64
    # Restamp the identity, so the bundle fails for the set digest and nothing
    # else — an unrestamped root would be refused for its identity first.
    root["releaseId"] = release_identity(root)
    (output / ROOT_OBJECT_KEY).write_bytes(canonical_json_bytes(root))

    with pytest.raises(SourceCatalogError, match="selectedSourceSetDigest"):
        verify_bundle_directory(output)


def test_an_undeclared_file_in_the_bundle_is_refused(tmp_path: Path) -> None:
    output = tmp_path / "catalog"
    build_source_catalog_release(_spec(), _hand_built_universe(), published_at=PUBLISHED_AT).write(output)
    (output / "notes.txt").write_text("stowaway", encoding="utf-8")
    with pytest.raises(SourceCatalogError, match="undeclared files"):
        verify_bundle_directory(output)


def test_a_rewritten_member_is_refused_on_read_back(tmp_path: Path) -> None:
    output = tmp_path / "catalog"
    build_source_catalog_release(_spec(), _hand_built_universe(), published_at=PUBLISHED_AT).write(output)
    path = output / SOURCE_ITEMS_OBJECT_KEY
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(SourceCatalogError, match="differs from its descriptor"):
        verify_bundle_directory(output)


def test_an_unpublishable_instant_is_refused() -> None:
    with pytest.raises(SourceCatalogError, match="publishedAt must be a UTC instant"):
        build_source_catalog_release(_spec(), _hand_built_universe(), published_at="2026-08-12")


# ─── the Mirrulations discovery adapter ────────────────────────────────

BUCKET = "mirrulations"
PREFIX = "raw-data/SEC/SEC-202"
STAMP = datetime(2026, 8, 5, 12, 0, tzinfo=timezone.utc)


class _Body:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def read(self, amount: int | None = None) -> bytes:
        return self.payload if amount is None else self.payload[:amount]

    def close(self) -> None:
        return None


class _Object:
    def __init__(self, store: dict[str, bytes], metadata: dict[str, dict[str, Any]], key: str) -> None:
        self.store = store
        self.metadata = metadata
        self.key = key

    def get(self, **kwargs: Any) -> dict[str, Any]:
        payload = self.store[self.key]
        return {
            "Body": _Body(payload),
            "ETag": self.metadata[self.key]["ETag"],
            "LastModified": self.metadata[self.key]["LastModified"],
            "ContentLength": len(payload),
        }


class _Resource:
    def __init__(self, store: dict[str, bytes], metadata: dict[str, dict[str, Any]]) -> None:
        self.store = store
        self.metadata = metadata

    def Object(self, bucket: str, key: str) -> _Object:  # noqa: N802 - boto3 API
        assert bucket == BUCKET
        return _Object(self.store, self.metadata, key)


def _document_payload(document_id: str, html: bytes, *, agency: str, document_type: str, withdrawn: bool) -> bytes:
    return json.dumps(
        {
            "data": {
                "id": document_id,
                "type": "documents",
                "links": {"self": f"https://api.regulations.gov/v4/documents/{document_id}"},
                "attributes": {
                    "additionalRins": ["3235-AN19", "not-a-rin"],
                    "agencyId": agency,
                    "commentEndDate": "2026-03-01T23:59:59Z",
                    "docketId": "-".join(document_id.split("-")[:3]),
                    "documentType": document_type,
                    "frDocNum": "2026-00042",
                    "modifyDate": "2026-01-06T05:00:00Z",
                    "postedDate": "2026-01-05T05:00:00Z",
                    "title": f"Title {document_id}",
                    "withdrawn": withdrawn,
                    "fileFormats": [
                        {
                            "fileUrl": f"https://downloads.regulations.gov/{document_id}/content.htm",
                            "format": "htm",
                            "size": len(html),
                        }
                    ],
                },
            }
        },
        sort_keys=True,
    ).encode()


def _mirrulations_cache(tmp_path: Path) -> tuple[Path, Path]:
    """Draw and fetch four documents through the real corpus machinery."""

    plan = [
        ("SEC-2026-0001-0001", "SEC", "Rule", False),
        ("SEC-2026-0002-0001", "SEC", "Notice", False),
        ("SEC-2026-0003-0001", "SEC", "Rule", True),
        ("SEC-2026-0004-0001", "XYZ", "Rule", False),
    ]
    store: dict[str, bytes] = {}
    for document_id, agency, document_type, withdrawn in plan:
        docket = "-".join(document_id.split("-")[:3])
        base = f"{PREFIX}/{docket}/text-{docket}/documents"
        html = f"<html><body>{document_id}</body></html>".encode()
        store[f"{base}/{document_id}.json"] = _document_payload(
            document_id, html, agency=agency, document_type=document_type, withdrawn=withdrawn
        )
        store[f"{base}/{document_id}_content.htm"] = html
    metadata = {
        key: {
            "Key": key,
            "Size": len(payload),
            "ETag": f'"{hashlib.md5(payload, usedforsecurity=False).hexdigest()}"',  # noqa: S324 - fake ETag
            "LastModified": STAMP,
        }
        for key, payload in store.items()
    }
    manifest = corpus.build_draw(list(metadata.values()), max_documents=len(plan))
    draw = tmp_path / "draw.json"
    corpus.write_draw(draw, manifest)
    cache = tmp_path / "cache"
    corpus.fetch_pairs(draw, cache, resource=_Resource(store, metadata), workers=2, retrieved_at="2026-08-12T00:00:00Z")
    return draw, cache


def test_mirrulations_discovery_maps_the_draw_into_a_conformant_release(tmp_path: Path) -> None:
    draw, cache = _mirrulations_cache(tmp_path)
    discovered = mirrulations_adapter.load_discovery(draw, cache)
    assert [item.source_item_id for item in discovered] == [
        "regulations.gov/SEC-2026-0001-0001",
        "regulations.gov/SEC-2026-0002-0001",
        "regulations.gov/SEC-2026-0003-0001",
        "regulations.gov/SEC-2026-0004-0001",
    ]

    rows = select_source_items(_spec(), discovered)
    assert _dispositions(rows) == {
        "regulations.gov/SEC-2026-0001-0001": "selected",
        # A Notice under a Rule-only universe.
        "regulations.gov/SEC-2026-0002-0001": "excluded",
        # The source itself withdrew this one.
        "regulations.gov/SEC-2026-0003-0001": "deleted",
        # The universe declares no name for agency code XYZ, and this producer
        # does not invent one.
        "regulations.gov/SEC-2026-0004-0001": "failed",
    }
    by_id = {row["sourceItemId"]: row for row in rows}
    assert by_id["regulations.gov/SEC-2026-0004-0001"]["selection"]["reasonCode"] == ("policy.agency-name-undeclared")

    selected = by_id["regulations.gov/SEC-2026-0001-0001"]
    normalized = selected["normalizedMetadata"]
    assert normalized["title"] == "Title SEC-2026-0001-0001"
    assert normalized["publicationDate"] == "2026-01-05"
    assert normalized["lastUpdatedDate"] == "2026-01-06"
    assert normalized["commentCloseDate"] == "2026-03-01"
    assert normalized["docketIds"] == ["SEC-2026-0001"]
    assert normalized["regulationIdentifierNumbers"] == ["3235-AN19"]
    assert normalized["sourceUrl"] == "https://api.regulations.gov/v4/documents/SEC-2026-0001-0001"
    assert normalized["agencies"] == [{"agencyId": "SEC", "agencyName": "Securities and Exchange Commission"}]
    # A source-stated RIN that is not in RIN form stays an observation rather
    # than being reshaped into one.
    assert {"observationKey": "unnormalizedRegulationIdentifierNumber", "observationValue": "not-a-rin"} in (
        selected["sourceObservations"]
    )
    # A Regulations.gov document record states no topic vocabulary.
    assert selected["sourceObservedTopics"] == []
    # The source-issued version is the source's own revision label, never the
    # digest of any capture.
    assert selected["sourceIssuedVersion"] == "2026-01-06T05:00:00Z"

    renditions = {item["renditionId"]: item for item in selected["candidateRenditions"]}
    assert set(renditions) == {"mirrulations-mirror", "source-declared"}
    mirror = renditions["mirrulations-mirror"]
    assert mirror["locator"].startswith("https://mirrulations.s3.amazonaws.com/raw-data/SEC/")
    assert mirror["expectedSha256"].startswith("sha256:")
    assert renditions["source-declared"]["expectedSha256"] is None

    output = tmp_path / "catalog"
    receipt = publish_source_catalog_release(
        _spec(), discovered, output, published_at=PUBLISHED_AT, release_status="candidate"
    )
    assert receipt["counts"] == {
        "deletedCount": 1,
        "discoveredCount": 4,
        "excludedCount": 1,
        "failedCount": 1,
        "memberCount": 4,
        "selectedCount": 1,
        "totalMemberByteSize": receipt["counts"]["totalMemberByteSize"],
        "unavailableCount": 0,
    }
    verify_bundle_directory(output)


def test_a_drawn_document_with_no_captured_pair_is_unavailable(tmp_path: Path) -> None:
    draw, cache = _mirrulations_cache(tmp_path)
    (cache / "receipts" / "SEC-2026-0001-0001.json").unlink()
    discovered = mirrulations_adapter.load_discovery(draw, cache)
    missing = next(item for item in discovered if item.document_id == "SEC-2026-0001-0001")
    assert missing.outcome is SourceOutcome.UNAVAILABLE
    assert missing.outcome_reason_code == "source.pair-not-captured"
    assert missing.normalized is None

    rows = select_source_items(_spec(), discovered)
    assert _dispositions(rows)["regulations.gov/SEC-2026-0001-0001"] == "unavailable"


def test_the_location_scope_facet_reads_the_mirror_object_key(tmp_path: Path) -> None:
    draw, cache = _mirrulations_cache(tmp_path)
    discovered = mirrulations_adapter.load_discovery(draw, cache)
    spec = _spec(scope=UniverseScope(document_types=("Rule",), location_prefixes=("raw-data/EPA/",)))
    rows = select_source_items(spec, discovered)
    assert {row["selection"]["disposition"] for row in rows} == {"excluded", "deleted"}
    excluded = next(row for row in rows if row["selection"]["disposition"] == "excluded")
    assert excluded["selection"]["reasonCode"] == "policy.location-out-of-scope"
