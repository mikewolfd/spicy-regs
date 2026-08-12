"""The named universe over the published catalog: its spec, sampler, and adapter.

No network and no 55 MB corpus. The published-catalog adapter is driven through
a small Parquet table written here, so the same code path a production run takes
is exercised over rows chosen to hit each edge the real table holds: a sentinel
posted date, a withdrawn document, an agency the crosswalk does not name, a row
with nothing to capture, and a row that offers both a file and attachments.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.source_catalog import (
    NormalizationPolicy,
    PublicationWindow,
    SampleCandidate,
    SamplePolicy,
    SourceCatalogError,
    UniverseScope,
    UniverseSpec,
    build_source_catalog_release,
    load_universe_spec,
    select_source_items,
    verify_bundle_directory,
)
from spicy_regs.source_catalog.published_catalog import (
    CATALOG_COLUMNS,
    discover_published_catalog,
    discovered_item,
)

PUBLISHED_AT = "2026-08-12T00:00:00Z"

TRACKED_UNIVERSE = (
    Path(__file__).resolve().parents[1] / "src/spicy_regs/universes/regulations-gov-published-catalog-2021-2025.json"
)


# ─── rows, exactly as the published catalog spells them ────────────────


def _row(**overrides: Any) -> dict[str, Any]:
    defaults: dict[str, Any] = dict.fromkeys(CATALOG_COLUMNS)
    defaults |= {
        "document_id": "EPA-2023-0001-0001",
        "docket_id": "EPA-2023-0001",
        "agency_code": "EPA",
        "title": "A published rule",
        "document_type": "Rule",
        "posted_date": "2023-04-05T04:00:00Z",
        "modify_date": "2023-04-06T04:00:00Z",
        "file_url": "https://downloads.regulations.gov/EPA-2023-0001-0001/content.pdf",
    }
    return defaults | overrides


def _write_catalog(path: Path, rows: list[dict[str, Any]]) -> Path:
    table = pa.table({column: [row.get(column) for row in rows] for column in CATALOG_COLUMNS})
    pq.write_table(table, path)
    return path


def _spec(**overrides: Any) -> UniverseSpec:
    defaults: dict[str, Any] = {
        "universe_id": "urn:spicy-regs:source-universe:fixture-published-catalog",
        "catalog_id": "urn:spicy-regs:source-catalog:fixture-published-catalog",
        "policy_id": "urn:spicy-regs:selection-policy:fixture-published-catalog",
        "policy_version": "1.0",
        "source_system_id": "https://data.spicy-regs.dev/documents.parquet",
        "source_system_version": "sha256:" + "0" * 64,
        "normalization": NormalizationPolicy(
            language="en",
            agency_names={"EPA": "environmental-protection-agency", "FAA": "federal-aviation-administration"},
            source_url_template="https://www.regulations.gov/document/{documentId}",
        ),
        "scope": UniverseScope(
            document_types=("Notice", "Rule"),
            publication_window=PublicationWindow(start="2021-01-01", end="2025-12-31"),
        ),
    }
    return UniverseSpec(**(defaults | overrides))


def _dispositions(rows: Sequence[Mapping[str, Any]]) -> dict[str, tuple[str, str | None]]:
    return {row["sourceItemId"]: (row["selection"]["disposition"], row["selection"].get("reasonCode")) for row in rows}


# ─── what the published catalog states, and what it does not ───────────


def test_the_adapter_reports_a_row_exactly_as_the_catalog_states_it() -> None:
    item = discovered_item(
        _row(
            comment_end_date="2023-05-05T04:59:59Z",
            additional_rins='["2060-AV12", "not a rin"]',
            fr_doc_num="2023-00042",
            attachments_json=json.dumps(
                [
                    {"url": "https://downloads.regulations.gov/EPA-2023-0001-0001/content.pdf", "size": 1234},
                    {"url": "https://downloads.regulations.gov/EPA-2023-0001-0001/attachment_1.docx", "size": 99},
                ]
            ),
        )
    )

    assert item.source_item_id == "regulations.gov/EPA-2023-0001-0001"
    # The version label is the one the SOURCE issued, never a capture digest.
    assert item.source_issued_version == "2023-04-06T04:00:00Z"
    assert item.normalized is not None
    assert item.normalized.publication_date == "2023-04-05"
    assert item.normalized.comment_close_date == "2023-05-05"
    assert item.normalized.regulation_identifier_numbers == ("2060-AV12",)
    # The catalog states no per-item address; the universe declares its form.
    assert item.normalized.source_url is None
    # A Regulations.gov record carries no topic vocabulary.
    assert item.observed_topics == ()
    assert dict(item.source_native_metadata)["fr_doc_num"] == "2023-00042"
    # An unstated column is not carried as a null.
    assert "reason_withdrawn" not in item.source_native_metadata

    observations = {observation.observation_key: observation.observation_value for observation in item.observations}
    assert observations["unnormalizedRegulationIdentifierNumber"] == "not a rin"

    renditions = {rendition.rendition_id: rendition for rendition in item.renditions}
    assert renditions["source-file-url"].media_type == "application/pdf"
    # The file URL also appears in the attachment list; it is one rendition, and
    # it takes the size the source declares there.
    assert renditions["source-file-url"].expected_byte_size == 1234
    assert len(renditions) == 2
    assert renditions["source-attachment-0"].locator.endswith("attachment_1.docx")
    # The catalog declares sizes and never hashes, so no digest is guessed.
    assert all(rendition.expected_sha256 is None for rendition in item.renditions)


@pytest.mark.parametrize(
    "posted",
    ["0000-12-30T00:00:00Z", "2023-02-30T00:00:00Z", "2023-04-05", "", None],
)
def test_a_posted_date_the_policy_cannot_read_is_carried_rather_than_coerced(posted: str | None) -> None:
    item = discovered_item(_row(posted_date=posted, modify_date="2023-04-06T04:00:00Z"))

    assert item.normalized is not None
    assert item.normalized.publication_date is None
    observations = {observation.observation_key: observation.observation_value for observation in item.observations}
    if posted:
        assert observations["unparsablePostedDate"] == posted
    else:
        # Nothing was said, so there is nothing to report as unreadable.
        assert "unparsablePostedDate" not in observations


def test_a_withdrawn_document_is_the_sources_own_settlement() -> None:
    item = discovered_item(_row(withdrawn="true", reason_withdrawn="Issued in error"))

    assert str(item.outcome) == "deleted"
    assert item.outcome_reason_code == "source.withdrawn-after-publication"
    assert "Issued in error" in str(item.outcome_reason)
    # A withdrawn document offers nothing to capture.
    assert item.renditions == ()


def test_a_row_stating_no_version_at_all_stops_the_build() -> None:
    with pytest.raises(SourceCatalogError, match="neither a modify nor a posted date"):
        discovered_item(_row(posted_date=None, modify_date=None))


def test_the_adapter_streams_every_row_of_a_parquet_catalog(tmp_path: Path) -> None:
    rows = [_row(document_id=f"EPA-2023-0001-{index:04d}") for index in range(5)]
    catalog = _write_catalog(tmp_path / "documents.parquet", rows)

    items = list(discover_published_catalog(catalog, batch_size=2))

    assert [item.document_id for item in items] == [row["document_id"] for row in rows]


def test_a_catalog_missing_a_column_this_adapter_reads_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "documents.parquet"
    pq.write_table(pa.table({"document_id": ["EPA-2023-0001-0001"]}), path)

    with pytest.raises(SourceCatalogError, match="missing columns"):
        list(discover_published_catalog(path))


# ─── the declared facts no record states ───────────────────────────────


def test_a_declared_source_url_fills_the_gap_and_never_overwrites_a_stated_one() -> None:
    policy = NormalizationPolicy(language="en", source_url_template="https://www.regulations.gov/document/{documentId}")

    assert policy.source_url(None, "EPA-2023-0001-0001") == "https://www.regulations.gov/document/EPA-2023-0001-0001"
    # The identifier is percent-encoded, so a source-stated oddity cannot forge a path.
    assert policy.source_url(None, "a/b?c") == "https://www.regulations.gov/document/a%2Fb%3Fc"
    stated = "https://api.regulations.gov/v4/documents/EPA-2023-0001-0001"
    assert policy.source_url(stated, "EPA-2023-0001-0001") == stated
    # A universe that declares no form leaves the gap where it is.
    assert NormalizationPolicy(language="en").source_url(None, "EPA-2023-0001-0001") is None


@pytest.mark.parametrize(
    "template",
    ["https://example.invalid/document", "https://example.invalid/{documentId}/{documentId}", "not-a-url/{documentId}"],
)
def test_a_source_url_template_that_cannot_address_one_item_is_refused(template: str) -> None:
    with pytest.raises(SourceCatalogError, match="sourceUrlTemplate"):
        NormalizationPolicy(language="en", source_url_template=template)


def test_an_item_whose_agency_the_universe_does_not_name_fails_rather_than_borrowing_its_code() -> None:
    rows = select_source_items(_spec(), (discovered_item(_row(agency_code="ABMC")),))

    assert _dispositions(rows) == {"regulations.gov/EPA-2023-0001-0001": ("failed", "policy.agency-name-undeclared")}


# ─── the window, and the dates it cannot place ─────────────────────────


def test_a_date_the_window_cannot_place_is_excluded_apart_from_one_outside_it() -> None:
    items = (
        discovered_item(_row(document_id="EPA-2018-0001-0001", posted_date="2018-04-05T04:00:00Z")),
        discovered_item(_row(document_id="EPA-0000-0001-0001", posted_date="0000-12-30T00:00:00Z")),
    )

    assert _dispositions(select_source_items(_spec(), items)) == {
        "regulations.gov/EPA-2018-0001-0001": ("excluded", "policy.publication-window-out-of-scope"),
        "regulations.gov/EPA-0000-0001-0001": ("excluded", "policy.publication-date-unusable"),
    }


def test_a_universe_declaring_no_window_does_not_refuse_an_undated_item_for_its_date() -> None:
    item = discovered_item(_row(posted_date="0000-12-30T00:00:00Z"))
    scope = UniverseScope(document_types=("Rule",))

    rows = select_source_items(_spec(scope=scope), (item,))

    # No window to place it in, so the refusal names the missing field instead.
    assert _dispositions(rows) == {"regulations.gov/EPA-2023-0001-0001": ("failed", "source.normalized-field-missing")}


# ─── the sampler, which is the selection policy ────────────────────────


def _candidates(count: int, *, document_type: str = "Rule", agencies: int = 4) -> list[SampleCandidate]:
    return [
        SampleCandidate(
            source_item_id=f"regulations.gov/DOC-{index:05d}",
            document_id=f"DOC-{index:05d}",
            document_type=document_type,
            agency_ids=(f"AG{index % agencies}",),
            publication_date=f"202{index % 5}-04-05",
        )
        for index in range(count)
    ]


def test_the_draw_is_deterministic_and_moves_only_with_the_seed() -> None:
    frame = _candidates(400)
    policy = SamplePolicy(seed="a-seed", per_partition_limit=50)

    first = policy.draw(frame)
    assert first == policy.draw(list(reversed(frame)))
    assert len(first) == 50
    assert first != SamplePolicy(seed="another-seed", per_partition_limit=50).draw(frame)
    # A cap the frame cannot reach draws the whole frame.
    assert len(SamplePolicy(seed="a-seed", per_partition_limit=10_000).draw(frame)) == 400


def test_the_cap_is_per_document_type_and_no_stratum_is_shut_out() -> None:
    rules = _candidates(300, document_type="Rule")
    notices = [
        SampleCandidate(
            source_item_id=candidate.source_item_id + "-n",
            document_id=candidate.document_id + "-n",
            document_type="Notice",
            agency_ids=candidate.agency_ids,
            publication_date=candidate.publication_date,
        )
        for candidate in _candidates(300)
    ]
    by_id = {candidate.source_item_id: candidate for candidate in (*rules, *notices)}
    policy = SamplePolicy(seed="a-seed", per_partition_limit=40)

    drawn = [by_id[source_item_id] for source_item_id in policy.draw(by_id.values())]

    # The cap binds per document type, not across the frame.
    assert len(drawn) == 80
    assert Counter(candidate.document_type for candidate in drawn) == {"Rule": 40, "Notice": 40}
    # A sqrt-proportional allocation reaches every stratum the frame holds.
    assert {policy.stratum(candidate) for candidate in drawn} == {
        policy.stratum(candidate) for candidate in by_id.values()
    }


def test_a_sampled_universe_excludes_the_frame_it_did_not_draw() -> None:
    rows = select_source_items(
        _spec(sample=SamplePolicy(seed="a-seed", per_partition_limit=1)),
        tuple(
            discovered_item(_row(document_id=f"EPA-2023-0001-{index:04d}", docket_id="EPA-2023-0001"))
            for index in range(4)
        ),
    )

    dispositions = _dispositions(rows)
    assert sorted(dispositions.values()).count(("excluded", "policy.sample-not-drawn")) == 3
    assert list(dispositions.values()).count(("selected", None)) == 1


def test_the_draw_reads_stated_facts_alone_and_not_what_can_be_captured() -> None:
    """An item without a rendition still occupies its place in the frame."""

    items = tuple(
        discovered_item(_row(document_id=f"EPA-2023-0001-{index:04d}", file_url=None if index else _row()["file_url"]))
        for index in range(4)
    )

    rows = select_source_items(_spec(sample=SamplePolicy(seed="a-seed", per_partition_limit=4)), items)

    dispositions = _dispositions(rows)
    assert list(dispositions.values()).count(("unavailable", "source.no-candidate-rendition")) == 3
    assert list(dispositions.values()).count(("selected", None)) == 1


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("partitionBy", "agencyId"),
        ("stratifyBy", ["agencyId"]),
        ("orderHash", "sha256(documentId:seed)"),
        ("allocation", "proportional"),
        ("perPartitionLimit", 0),
    ],
)
def test_a_sample_naming_mechanics_this_module_cannot_run_is_refused(field: str, value: Any) -> None:
    document = {"seed": "a-seed", "perPartitionLimit": 10, field: value}

    with pytest.raises(SourceCatalogError, match="sample"):
        SamplePolicy.from_mapping(document)


def test_the_sample_rides_inside_the_policy_digest() -> None:
    unsampled = _spec()
    sampled = _spec(sample=SamplePolicy(seed="a-seed", per_partition_limit=10))
    reseeded = _spec(sample=SamplePolicy(seed="another-seed", per_partition_limit=10))

    assert unsampled.policy_sha256() != sampled.policy_sha256()
    assert sampled.policy_sha256() != reseeded.policy_sha256()
    assert sampled.policy_sha256() == _spec(sample=SamplePolicy(seed="a-seed", per_partition_limit=10)).policy_sha256()


# ─── the tracked universe, end to end ──────────────────────────────────


def test_the_tracked_universe_loads_and_declares_what_no_record_states() -> None:
    spec = load_universe_spec(TRACKED_UNIVERSE)

    assert spec.scope.publication_window == PublicationWindow(start="2021-01-01", end="2025-12-31")
    assert len(spec.scope.document_types) == 6
    assert spec.normalization.language == "en"
    assert spec.normalization.source_url_template == "https://www.regulations.gov/document/{documentId}"
    assert spec.sample is not None
    assert spec.sample.per_partition_limit == 100_000
    # The crosswalk is declared, not guessed: a code it does not name has none.
    assert spec.normalization.agency_names["EPA"] == "environmental-protection-agency"
    assert "ABMC" not in spec.normalization.agency_names
    # The universe names the exact source bytes it was written for.
    assert spec.source_system_version.startswith("sha256:")


def test_the_tracked_universe_produces_a_bundle_that_verifies(tmp_path: Path) -> None:
    spec = load_universe_spec(TRACKED_UNIVERSE)
    rows = [
        _row(document_id="EPA-2023-0001-0001"),
        _row(document_id="FAA-2022-0002-0001", agency_code="FAA", docket_id="FAA-2022-0002", document_type="Notice"),
        _row(document_id="EPA-2018-0003-0001", posted_date="2018-04-05T04:00:00Z"),
        _row(document_id="EPA-0000-0004-0001", posted_date="0000-12-30T00:00:00Z"),
        _row(document_id="ABMC-2023-0005-0001", agency_code="ABMC", docket_id="ABMC-2023-0005"),
        _row(document_id="EPA-2023-0006-0001", file_url=None),
        _row(document_id="EPA-2023-0007-0001", withdrawn="true"),
    ]
    catalog = _write_catalog(tmp_path / "documents.parquet", rows)

    bundle = build_source_catalog_release(
        spec, discover_published_catalog(catalog), published_at=PUBLISHED_AT, release_status="fixture"
    )
    output = tmp_path / "release"
    bundle.write(output)
    verify_bundle_directory(output)

    dispositions = _dispositions(list(bundle.items))
    assert dispositions == {
        "regulations.gov/EPA-2023-0001-0001": ("selected", None),
        "regulations.gov/FAA-2022-0002-0001": ("selected", None),
        "regulations.gov/EPA-2018-0003-0001": ("excluded", "policy.publication-window-out-of-scope"),
        "regulations.gov/EPA-0000-0004-0001": ("excluded", "policy.publication-date-unusable"),
        "regulations.gov/ABMC-2023-0005-0001": ("failed", "policy.agency-name-undeclared"),
        "regulations.gov/EPA-2023-0006-0001": ("unavailable", "source.no-candidate-rendition"),
        "regulations.gov/EPA-2023-0007-0001": ("deleted", "source.withdrawn-after-publication"),
    }
    selected = next(item for item in bundle.items if item["selection"]["disposition"] == "selected")
    normalized = selected["normalizedMetadata"]
    assert normalized["language"] == "en"
    assert normalized["sourceUrl"] == "https://www.regulations.gov/document/EPA-2023-0001-0001"
    assert normalized["agencies"] == [{"agencyId": "EPA", "agencyName": "environmental-protection-agency"}]

    content = bundle.root["content"]
    assert content["counts"]["discoveredCount"] == len(rows)
    assert content["coverage"]["unaccountedCount"] == 0
    assert content["selectionPolicy"]["policySha256"] == spec.policy_sha256()


def test_the_tracked_universe_round_trips_through_its_own_canonical_form() -> None:
    document = json.loads(TRACKED_UNIVERSE.read_text(encoding="utf-8"))
    spec = load_universe_spec(TRACKED_UNIVERSE)
    assert spec.sample is not None

    assert UniverseSpec.from_mapping(document).policy_sha256() == spec.policy_sha256()
    # The tracked file spells every mechanic out rather than leaning on a
    # default, so the digest states the whole method and not part of it.
    assert document["sample"] == {
        "allocation": "sqrt-proportional",
        "orderHash": "md5(documentId:seed)",
        "partitionBy": "documentType",
        "perPartitionLimit": 100_000,
        "seed": spec.sample.seed,
        "stratifyBy": ["agencyId", "publicationYear"],
    }
