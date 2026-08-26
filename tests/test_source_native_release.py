"""Focused Federal Register source-native publication and reader checks."""

from __future__ import annotations

import json
import tomllib
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from urllib.parse import parse_qs, urlparse

import pytest
from rulespec_artifacts import ArtifactVerificationError, LocalMemberSource, Producer

from spicy_regs import source_native
from spicy_regs.publication import ImmutablePublicationError
from spicy_regs.federal_register_source_native import (
    DOCUMENT_FIELDS,
    FederalRegisterPage,
    FederalRegisterSourceError,
    federal_register_documents_url,
    iter_federal_register_pages,
    parse_page_response,
)
from spicy_regs.source_native import (
    SourceNativeReleaseBuild,
    SourceNativeReleaseError,
    SourceNativeReleasePublisher,
    SourceNativeReleaseReader,
    installed_release_schema_bundle,
    release_schema_bundle,
)
from spicy_regs.source_native_profiles import FEDERAL_REGISTER_PROFILE

IMPLEMENTATION_ID = "git+https://example.test/spicy-regs@" + "a" * 40
PRODUCER = Producer(
    product="spicy-regs",
    implementation_id=IMPLEMENTATION_ID,
    verifier_id="urn:spicy-regs:source-native-release-verifier",
    verifier_version="1.0",
    verifier_implementation_id=IMPLEMENTATION_ID,
)
QUERY_SCOPE = {"publishedFrom": "2026-08-25", "publishedThrough": "2026-08-25"}


def _document(number: str = "2026-00001", **changes: object) -> dict[str, object]:
    value: dict[str, object] = {
        "agencies": [],
        "body_html_url": None,
        "document_number": number,
        "html_url": f"https://www.federalregister.gov/d/{number}",
        "pdf_url": None,
        "publication_date": "2026-08-25",
        "regulation_id_numbers": ["not-a-rin"],
        "title": "A source-native rule",
        "topics": [],
        "type": "Rule",
    }
    value.update(changes)
    return value


def _response(
    *documents: dict[str, object],
    next_page_url: str | None = None,
    count: int | None = None,
    total_pages: int | None = None,
) -> bytes:
    return json.dumps(
        {
            "count": len(documents) if count is None else count,
            "next_page_url": next_page_url,
            "results": list(documents),
            "total_pages": (1 if next_page_url is None else 2) if total_pages is None else total_pages,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _page(
    traversal: int,
    page: int,
    response: bytes,
    *,
    cursor: str | None = None,
) -> FederalRegisterPage:
    return FederalRegisterPage(
        traversal_index=traversal,
        page_index=page,
        request_key=cursor or federal_register_documents_url(QUERY_SCOPE),
        source_cursor=cursor,
        response_bytes=response,
        window_index=0,
        window_page_index=page,
    )


def _build(*, query_scope: dict[str, str] | None = None) -> SourceNativeReleaseBuild:
    return SourceNativeReleaseBuild(
        query_scope=query_scope or QUERY_SCOPE,
        producer=PRODUCER,
        started_at="2026-08-25T00:00:00Z",
        completed_at="2026-08-25T00:00:01Z",
    )


def _stable_pages(*documents: dict[str, object]) -> list[FederalRegisterPage]:
    response = _response(*documents)
    return [_page(0, 0, response), _page(1, 0, response)]


def _publish(tmp_path: Path, pages: list[FederalRegisterPage]):
    return SourceNativeReleasePublisher(FEDERAL_REGISTER_PROFILE).publish(
        pages,
        build=_build(),
        destination=tmp_path / "release",
    )


def _reader(root: Path, pin):
    return SourceNativeReleaseReader(
        LocalMemberSource(root),
        profile=FEDERAL_REGISTER_PROFILE,
        expected_pin=pin,
        accepted_verifier_implementation_ids=frozenset({IMPLEMENTATION_ID}),
    )


def test_stable_release_preserves_source_value_and_streams(tmp_path: Path) -> None:
    published = _publish(tmp_path, _stable_pages(_document()))
    reader = _reader(published.root, published.artifact.pin)

    records = list(reader.iter_records())
    assert reader.pin == published.artifact.pin
    assert reader.source_state_scope == "observed-crawl"
    assert reader.source_system_id == "https://www.federalregister.gov/api/v1"
    assert reader.source_system_version == "v1"
    assert reader.source_state_digest.startswith("sha256:")
    assert reader.source_native_schema_set_digest.startswith("sha256:")
    assert records[0]["record"]["agencies"] == []
    assert records[0]["record"]["topics"] == []
    assert records[0]["record"]["regulation_id_numbers"] == ["not-a-rin"]
    assert records[0]["fieldDiagnostics"] == [
        {
            "code": "malformed-rin",
            "field": "regulation_id_numbers",
            "value": "not-a-rin",
        }
    ]

    renditions = list(reader.iter_renditions())
    assert [(row["sourceField"], row["locator"], row["mediaType"]) for row in renditions] == [
        ("body_html_url", None, "text/html"),
        ("html_url", "https://www.federalregister.gov/d/2026-00001", "text/html"),
        ("pdf_url", None, "application/pdf"),
    ]


def test_source_native_record_preserves_predecessor_source_facts(tmp_path: Path) -> None:
    document = _document(
        agencies=[
            {
                "name": "Environmental Protection Agency",
                "raw_name": "ENVIRONMENTAL PROTECTION AGENCY",
                "slug": "environmental-protection-agency",
            }
        ],
        body_html_url="https://www.federalregister.gov/documents/full_text/html/2026-00001.html",
        docket_ids=["EPA-HQ-OAR-2026-0001"],
        pdf_url="https://www.govinfo.gov/content/pkg/FR-2026-08-25/pdf/2026-00001.pdf",
        regulation_id_numbers=["2060-AV12"],
        title="Native Federal Register title",
        topics=["Air pollution control"],
        type="Notice",
    )

    published = _publish(tmp_path, _stable_pages(document))
    reader = _reader(published.root, published.artifact.pin)

    assert list(reader.iter_records())[0]["record"] == document
    assert [(row["sourceField"], row["locator"]) for row in reader.iter_renditions()] == [
        ("body_html_url", document["body_html_url"]),
        ("html_url", document["html_url"]),
        ("pdf_url", document["pdf_url"]),
    ]


@pytest.mark.parametrize("publication_date", [None, "", "not-a-date", "2026-08-25T00:00:00Z"])
def test_source_issued_publication_date_is_required(tmp_path: Path, publication_date: object) -> None:
    with pytest.raises(FederalRegisterSourceError, match="publication_date"):
        _publish(tmp_path, _stable_pages(_document(publication_date=publication_date)))


def test_acquisition_exception_cannot_publish_partial_release(tmp_path: Path) -> None:
    destination = tmp_path / "interrupted"

    def interrupted_pages() -> Iterator[FederalRegisterPage]:
        yield _page(0, 0, _response(_document()))
        raise RuntimeError("source stopped")

    with pytest.raises(RuntimeError, match="source stopped"):
        SourceNativeReleasePublisher(FEDERAL_REGISTER_PROFILE).publish(
            interrupted_pages(),
            build=_build(),
            destination=destination,
        )

    assert not destination.exists()


def test_concurrent_publishers_never_replace_the_winner(tmp_path: Path) -> None:
    barrier = Barrier(2)
    destination = tmp_path / "release"

    def publish():
        def synchronized_pages() -> Iterator[FederalRegisterPage]:
            barrier.wait()
            yield from _stable_pages(_document())

        return SourceNativeReleasePublisher(FEDERAL_REGISTER_PROFILE).publish(
            synchronized_pages(),
            build=_build(),
            destination=destination,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(publish) for _ in range(2)]
    successes = [future.result() for future in futures if future.exception() is None]
    failures = [future.exception() for future in futures if future.exception() is not None]

    assert len(successes) == 1
    assert len(failures) == 1
    assert isinstance(failures[0], (FileExistsError, ImmutablePublicationError))
    assert _reader(destination, successes[0].artifact.pin).pin == successes[0].artifact.pin


def test_observed_crawl_refuses_one_unreconciled_traversal(tmp_path: Path) -> None:
    with pytest.raises(SourceNativeReleaseError, match="two stable consecutive traversals"):
        _publish(tmp_path, [_page(0, 0, _response(_document()))])


def test_complete_snapshot_refuses_different_reconciliation_passes(tmp_path: Path) -> None:
    pages = [
        _page(0, 0, _response(_document("2026-00001"))),
        _page(1, 0, _response(_document("2026-00002"))),
    ]

    with pytest.raises(SourceNativeReleaseError, match="stable consecutive traversals"):
        _publish(tmp_path, pages)


def test_stable_federal_reconciliation_exposes_observed_crawl_scope(tmp_path: Path) -> None:
    published = _publish(tmp_path, _stable_pages(_document()))

    assert _reader(published.root, published.artifact.pin).source_state_scope == "observed-crawl"


def test_unclassified_source_field_fails_publication(tmp_path: Path) -> None:
    drifted = _document(new_upstream_field="unclassified")

    with pytest.raises(FederalRegisterSourceError, match="unclassified.*document fields"):
        _publish(tmp_path, _stable_pages(drifted))


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"title": 7}, "title must be text or null"),
        ({"volume": True}, "volume must be an integer or null"),
        ({"docket_ids": [7]}, "docket_ids must be a text array or null"),
        ({"topics": [7]}, "topics must be a text array or null"),
    ],
)
def test_source_field_type_drift_fails_publication(
    tmp_path: Path,
    changes: dict[str, object],
    message: str,
) -> None:
    document = _document()
    document.update(changes)
    with pytest.raises(FederalRegisterSourceError, match=message):
        _publish(tmp_path, _stable_pages(document))


def test_missing_or_forked_page_chain_fails(tmp_path: Path) -> None:
    next_url = "https://www.federalregister.gov/api/v1/documents?format=json&page=2&cursor=stable"
    first = _page(0, 0, _response(_document(), next_page_url=next_url))
    second = _page(0, 1, _response(_document("2026-00002")), cursor="wrong")

    with pytest.raises(SourceNativeReleaseError, match="missing or forked"):
        _publish(tmp_path, [first, second])


def test_publisher_refuses_a_cyclic_source_cursor(tmp_path: Path) -> None:
    initial = federal_register_documents_url(QUERY_SCOPE)
    second = "https://www.federalregister.gov/api/v1/documents?format=json&page=2"
    pages = [
        FederalRegisterPage(
            0,
            0,
            initial,
            None,
            _response(_document("2026-00001"), next_page_url=second, count=3, total_pages=3),
        ),
        FederalRegisterPage(
            0,
            1,
            second,
            second,
            _response(_document("2026-00002"), next_page_url=initial, count=3, total_pages=3),
            window_page_index=1,
        ),
    ]

    with pytest.raises(FederalRegisterSourceError, match="cyclic page cursor"):
        _publish(tmp_path, pages)


def test_changed_member_fails_before_reader_yields(tmp_path: Path) -> None:
    published = _publish(tmp_path, _stable_pages(_document()))
    records_path = published.root / "records/federal-register-000000.jsonl"
    records_path.write_bytes(records_path.read_bytes().replace(b"source-native rule", b"changed source rule"))

    with pytest.raises(ArtifactVerificationError, match="invalid.member-digest"):
        _reader(published.root, published.artifact.pin)


def test_records_and_renditions_stream_across_bounded_partitions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(source_native, "MAX_RECORDS_PER_MEMBER", 2)
    documents = [_document(f"2026-{number:05d}") for number in range(3, 0, -1)]
    published = _publish(tmp_path, _stable_pages(*documents))
    reader = _reader(published.root, published.artifact.pin)

    assert [row["sourceRecordId"] for row in reader.iter_records()] == [
        "2026-00001",
        "2026-00002",
        "2026-00003",
    ]
    assert len(list(reader.iter_renditions())) == 9
    assert len(list(published.root.glob("records/federal-register-*.jsonl"))) == 2
    assert len(list(published.root.glob("records/renditions-*.jsonl"))) == 5


def test_reader_requires_explicit_verifier_allowlist(tmp_path: Path) -> None:
    published = _publish(tmp_path, _stable_pages(_document()))

    with pytest.raises(SourceNativeReleaseError, match="at least one verifier"):
        SourceNativeReleaseReader(
            LocalMemberSource(published.root),
            profile=FEDERAL_REGISTER_PROFILE,
            expected_pin=published.artifact.pin,
            accepted_verifier_implementation_ids=frozenset(),
        )
    with pytest.raises(SourceNativeReleaseError, match="not accepted"):
        SourceNativeReleaseReader(
            LocalMemberSource(published.root),
            profile=FEDERAL_REGISTER_PROFILE,
            expected_pin=published.artifact.pin,
            accepted_verifier_implementation_ids=frozenset({"git+https://example.test/other@" + "b" * 40}),
        )


def test_new_path_uses_only_shared_artifact_implementation() -> None:
    source_native = Path(__file__).parents[1] / "src/spicy_regs/source_native.py"
    profile = Path(__file__).parents[1] / "src/spicy_regs/federal_register_source_native.py"
    text = source_native.read_text() + profile.read_text()

    assert "rulespec_conformance" not in text
    assert "import hashlib" not in text
    assert "from hashlib" not in text
    assert "docspec" not in text.lower()
    assert "refspec" not in text.lower()


def test_base_package_keeps_legacy_platform_dependencies_out_of_source_native_path() -> None:
    project = tomllib.loads(
        (Path(__file__).parents[1] / "pyproject.toml").read_text(encoding="utf-8")
    )["project"]

    dependencies = project["dependencies"]
    assert "rulespec-artifacts==1.0.0" in dependencies
    assert not any(
        dependency.startswith(("refspec", "rdflib", "rulespec-conformance"))
        for dependency in dependencies
    )
    assert "build-source-catalog" not in project["scripts"]


def test_installed_schema_bundle_is_the_exact_generated_bundle() -> None:
    assert installed_release_schema_bundle() == release_schema_bundle()


def test_injected_page_fetcher_builds_the_closed_query_and_reconciles() -> None:
    scope = {"publishedFrom": "2026-04-13", "publishedThrough": "2026-04-13"}
    initial = federal_register_documents_url(scope, per_page=1000)
    next_url = "https://www.federalregister.gov/api/v1/documents?format=json&page=2&cursor=stable"
    responses = {
        initial: _response(
            _document("2026-00002"),
            next_page_url=next_url,
            count=2,
            total_pages=2,
        ),
        next_url: _response(
            _document("2026-00001"),
            count=2,
            total_pages=2,
        ),
    }
    requests: list[str] = []

    def fetch(url: str) -> bytes:
        requests.append(url)
        return responses[url]

    pages = list(iter_federal_register_pages(fetch, query_scope=scope))

    assert requests == [initial, next_url, initial, next_url]
    assert [(page.traversal_index, page.page_index, page.source_cursor) for page in pages] == [
        (0, 0, None),
        (0, 1, next_url),
        (1, 0, None),
        (1, 1, next_url),
    ]
    query = parse_qs(urlparse(initial).query)
    assert query["conditions[publication_date][gte]"] == ["2026-04-13"]
    assert query["conditions[publication_date][lte]"] == ["2026-04-13"]
    assert query["fields[]"] == sorted(DOCUMENT_FIELDS)


def test_injected_page_fetcher_refuses_incomplete_or_cyclic_inventory() -> None:
    scope = {"publishedFrom": "2026-04-13", "publishedThrough": "2026-04-13"}
    initial = federal_register_documents_url(scope)

    with pytest.raises(FederalRegisterSourceError, match="declared and observed record counts"):
        list(
            iter_federal_register_pages(
                lambda _url: _response(_document(), count=2, total_pages=1),
                query_scope=scope,
            )
        )

    with pytest.raises(FederalRegisterSourceError, match="cyclic page cursor"):
        list(
            iter_federal_register_pages(
                lambda _url: _response(
                    _document(),
                    next_page_url=initial,
                    count=2,
                    total_pages=2,
                ),
                query_scope=scope,
                traversals=1,
            )
        )


def test_capped_interval_splits_into_exact_ordered_leaf_evidence(tmp_path: Path) -> None:
    scope = {"publishedFrom": "2026-04-13", "publishedThrough": "2026-04-14"}
    requests: list[tuple[str, str]] = []

    def fetch(url: str) -> bytes:
        query = parse_qs(urlparse(url).query)
        window = (
            query["conditions[publication_date][gte]"][0],
            query["conditions[publication_date][lte]"][0],
        )
        requests.append(window)
        if window[0] != window[1]:
            return _response(
                _document("2026-cap-probe", publication_date=window[0]),
                count=10_000,
                total_pages=10,
            )
        number = "2026-00013" if window[0].endswith("13") else "2026-00014"
        return _response(_document(number, publication_date=window[0]))

    destination = tmp_path / "split-release"
    published = SourceNativeReleasePublisher(FEDERAL_REGISTER_PROFILE).publish(
        iter_federal_register_pages(fetch, query_scope=scope),
        build=_build(query_scope=scope),
        destination=destination,
    )
    reader = _reader(published.root, published.artifact.pin)

    assert [row["sourceRecordId"] for row in reader.iter_records()] == ["2026-00013", "2026-00014"]
    assert requests == [
        ("2026-04-13", "2026-04-14"),
        ("2026-04-13", "2026-04-13"),
        ("2026-04-14", "2026-04-14"),
    ] * 2
    pages = [json.loads(line) for line in (destination / "acquisition/pages.jsonl").read_text().splitlines()]
    assert [
        (
            row["traversalIndex"],
            row["windowIndex"],
            row["windowPageIndex"],
            row["recordsIncluded"],
        )
        for row in pages
    ] == [
        (0, 0, 0, False),
        (0, 1, 0, True),
        (0, 2, 0, True),
        (1, 0, 0, False),
        (1, 1, 0, True),
        (1, 2, 0, True),
    ]
    evidence = sorted(destination.glob("evidence/traversal-*/page-*.json"))
    assert len(evidence) == 6
    assert {parse_page_response(path.read_bytes())["count"] for path in evidence} == {1, 10_000}


def test_capped_single_day_refuses_ambiguous_source_state() -> None:
    scope = {"publishedFrom": "2026-04-13", "publishedThrough": "2026-04-13"}
    pages = iter_federal_register_pages(
        lambda _url: _response(
            _document("2026-cap", publication_date="2026-04-13"),
            count=10_000,
            total_pages=10,
        ),
        query_scope=scope,
    )

    probe = next(pages)
    assert parse_page_response(probe.response_bytes)["count"] == 10_000
    with pytest.raises(FederalRegisterSourceError, match="result cap is ambiguous.*2026-04-13"):
        next(pages)


@pytest.mark.parametrize(
    "days",
    [
        ["2026-04-13"],
        ["2026-04-14", "2026-04-13"],
    ],
)
def test_publisher_refuses_missing_or_reordered_date_windows(tmp_path: Path, days: list[str]) -> None:
    scope = {"publishedFrom": "2026-04-13", "publishedThrough": "2026-04-14"}
    pages = [
        FederalRegisterPage(
            traversal_index=0,
            page_index=index,
            request_key=federal_register_documents_url(
                {"publishedFrom": day, "publishedThrough": day}
            ),
            source_cursor=None,
            response_bytes=_response(
                _document(f"2026-{day[-2:]}", publication_date=day)
            ),
            window_index=index,
            window_page_index=0,
        )
        for index, day in enumerate(days)
    ]

    with pytest.raises(FederalRegisterSourceError, match="split windows"):
        SourceNativeReleasePublisher(FEDERAL_REGISTER_PROFILE).publish(
            pages,
            build=_build(query_scope=scope),
            destination=tmp_path / "invalid-windows",
        )


def test_publisher_independently_refuses_a_false_source_count(tmp_path: Path) -> None:
    response = _response(_document(), count=2, total_pages=1)

    with pytest.raises(FederalRegisterSourceError, match="declared and observed record counts"):
        _publish(tmp_path, [_page(0, 0, response)])
