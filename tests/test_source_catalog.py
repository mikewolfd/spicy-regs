from __future__ import annotations

import json
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import pytest
from rulespec_conformance.platform_artifact import LocalMemberSource, admit_artifact

from spicy_regs.publication import ImmutablePublicationError
from spicy_regs import publication
from spicy_regs.schemas.regulations import DOCUMENT
from spicy_regs.source_catalog import (
    MAX_SOURCE_ITEM_BYTES,
    SOURCE_ITEMS_OBJECT_KEY,
    SourceCatalogBuild,
    SourceCatalogError,
    SourceCatalogIncompleteError,
    SourceCatalogItem,
    SourceCatalogPublisher,
    federal_register_items,
    regulations_gov_items,
    verify_source_catalog_semantics,
)
from spicy_regs.sources.base import Reader


class FakeReader(Reader):
    def __init__(self, records: list[dict]) -> None:
        self.records = records
        self.failed_keys: list[str] = []
        self.parse_failed_keys: list[str] = []

    def iter_records(self) -> Iterator[dict]:
        yield from self.records


def build(source: str) -> SourceCatalogBuild:
    return SourceCatalogBuild(
        catalog_id=f"urn:spicy-regs:catalog:{source}",
        source_system_id=f"urn:spicy-regs:source:{source}",
        source_system_version="2026-08-24",
        selection_policy_id="urn:spicy-regs:selection:complete-metadata",
        selection_policy_version="1",
        selection_policy_digest="sha256:" + "1" * 64,
    )


def regulations_raw(identity: str = "EPA-2026-0001") -> dict:
    return {
        "data": {
            "id": identity,
            "attributes": {
                "additionalRins": ["2060-AV12"],
                "agencyId": "EPA",
                "docketId": "EPA-HQ-OAR-2026-0001",
                "documentType": "Notice",
                "fileFormats": [
                    {
                        "fileUrl": f"https://downloads.regulations.gov/{identity}.pdf",
                        "format": "pdf",
                        "size": 123,
                    }
                ],
                "modifyDate": "2026-08-24T00:00:00Z",
                "postedDate": "2026-08-23T00:00:00Z",
                "title": "Air quality notice",
                "topics": ["Air quality"],
            },
        }
    }


def federal_raw(identity: str = "2026-12345") -> dict:
    return {
        "agencies": [
            {
                "name": "Environmental Protection Agency",
                "slug": "environmental-protection-agency",
            }
        ],
        "docket_ids": ["EPA-HQ-OAR-2026-0001"],
        "document_number": identity,
        "html_url": f"https://www.federalregister.gov/d/{identity}",
        "pdf_url": f"https://www.govinfo.gov/content/pkg/FR-{identity}/pdf/{identity}.pdf",
        "publication_date": "2026-08-24",
        "regulation_id_numbers": ["2060-AV12"],
        "title": "Federal Register notice",
        "topics": [{"name": "Air pollution control", "slug": "air-pollution-control"}],
        "type": "Notice",
    }


def test_both_existing_source_mappings_use_one_publisher(tmp_path: Path) -> None:
    publisher = SourceCatalogPublisher()
    regulations = publisher.publish(
        regulations_gov_items(FakeReader([regulations_raw()]), DOCUMENT),
        build=build("regulations-gov"),
        destination=tmp_path / "regulations",
    )
    federal = publisher.publish(
        federal_register_items(FakeReader([federal_raw()])),
        build=build("federal-register"),
        destination=tmp_path / "federal-register",
    )

    assert regulations.artifact.root["kind"] == "source-catalog"
    assert federal.artifact.root["kind"] == "source-catalog"
    assert regulations.artifact.pin != federal.artifact.pin
    for published in (regulations, federal):
        admitted = admit_artifact(
            LocalMemberSource(published.root),
            semantic_verifier=verify_source_catalog_semantics,
        )
        assert admitted.pin == published.artifact.pin
        row = json.loads((published.root / SOURCE_ITEMS_OBJECT_KEY).read_text().splitlines()[0])
        assert row["selection"] == {"disposition": "selected"}
        assert row["candidateRenditions"]
        assert row["normalizedMetadata"]["agencies"]


def test_source_mapping_preserves_non_selected_item_with_reason() -> None:
    raw = regulations_raw()
    raw["data"]["attributes"]["fileFormats"] = []
    item = next(regulations_gov_items(FakeReader([raw]), DOCUMENT)).as_dict()

    assert item["normalizedMetadata"] is not None
    assert item["candidateRenditions"] == []
    assert item["selection"]["disposition"] == "excluded"
    assert item["selection"]["reasonCode"] == "selection.missing-required-data"


def test_reader_exception_cannot_publish_partial_catalog(tmp_path: Path) -> None:
    class FailingReader(FakeReader):
        def iter_records(self) -> Iterator[dict]:
            yield self.records[0]
            raise RuntimeError("source stopped")

    destination = tmp_path / "catalog"
    with pytest.raises(RuntimeError, match="source stopped"):
        SourceCatalogPublisher().publish(
            federal_register_items(FailingReader([federal_raw()])),
            build=build("federal-register"),
            destination=destination,
        )
    assert not destination.exists()


@pytest.mark.parametrize("attribute", ["failed_keys", "parse_failed_keys"])
def test_reader_soft_failure_cannot_publish_partial_catalog(
    tmp_path: Path,
    attribute: str,
) -> None:
    reader = FakeReader([regulations_raw()])
    setattr(reader, attribute, ["source/key.json"])
    destination = tmp_path / attribute

    with pytest.raises(SourceCatalogIncompleteError, match="source/key.json"):
        SourceCatalogPublisher().publish(
            regulations_gov_items(reader, DOCUMENT),
            build=build("regulations-gov"),
            destination=destination,
        )
    assert not destination.exists()


def test_selected_item_without_rendition_is_rejected() -> None:
    value = next(federal_register_items(FakeReader([federal_raw()]))).as_dict()
    value["candidateRenditions"] = []

    with pytest.raises(SourceCatalogError, match="at least one"):
        SourceCatalogItem(value).as_dict()


def test_source_item_row_limit_is_enforced_before_publication(tmp_path: Path) -> None:
    value = next(federal_register_items(FakeReader([federal_raw()]))).as_dict()
    value["sourceNativeMetadata"] = {"body": "x" * MAX_SOURCE_ITEM_BYTES}
    destination = tmp_path / "oversized"

    with pytest.raises(SourceCatalogError, match="row limit"):
        SourceCatalogPublisher().publish(
            [SourceCatalogItem(value)],
            build=build("federal-register"),
            destination=destination,
        )
    assert not destination.exists()


def test_large_single_pass_stream_is_sorted_and_admitted(tmp_path: Path) -> None:
    class OnePass:
        def __init__(self) -> None:
            self.started = False

        def __iter__(self) -> Iterator[SourceCatalogItem]:
            assert not self.started
            self.started = True
            for index in range(1_000, 0, -1):
                yield from federal_register_items(FakeReader([federal_raw(f"2026-{index:05d}")]))

    published = SourceCatalogPublisher().publish(
        OnePass(),
        build=build("federal-register"),
        destination=tmp_path / "large",
    )

    lines = (published.root / SOURCE_ITEMS_OBJECT_KEY).read_text().splitlines()
    assert len(lines) == 1_000
    assert json.loads(lines[0])["sourceItemId"] == "2026-00001"
    assert json.loads(lines[-1])["sourceItemId"] == "2026-01000"


def test_concurrent_publishers_never_replace_the_winner(tmp_path: Path) -> None:
    barrier = Barrier(2)
    destination = tmp_path / "catalog"

    def publish() -> object:
        def synchronized_items() -> Iterator[SourceCatalogItem]:
            barrier.wait()
            yield from federal_register_items(FakeReader([federal_raw()]))

        return SourceCatalogPublisher().publish(
            synchronized_items(),
            build=build("federal-register"),
            destination=destination,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(publish) for _ in range(2)]
    successes = [future.result() for future in futures if future.exception() is None]
    failures = [future.exception() for future in futures if future.exception() is not None]

    assert len(successes) == 1
    assert len(failures) == 1
    assert isinstance(failures[0], (FileExistsError, ImmutablePublicationError))
    admit_artifact(
        LocalMemberSource(destination),
        semantic_verifier=verify_source_catalog_semantics,
    )


def test_native_publication_rename_refuses_an_existing_empty_directory(tmp_path: Path) -> None:
    working = tmp_path / "working"
    destination = tmp_path / "destination"
    working.mkdir()
    destination.mkdir()

    with pytest.raises(ImmutablePublicationError, match="refusing to replace"):
        publication._rename_directory_noreplace(working, destination)

    assert working.is_dir()
    assert destination.is_dir()


def test_cli_constructs_the_existing_federal_reader_and_publishes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from spicy_regs import source_catalog_cli

    monkeypatch.setattr(
        source_catalog_cli,
        "FederalRegisterReader",
        lambda **_kwargs: FakeReader([federal_raw()]),
    )
    destination = tmp_path / "cli-catalog"

    assert source_catalog_cli.main(
        [
            "--catalog-id",
            "urn:spicy-regs:catalog:cli-test",
            "--destination",
            str(destination),
            "federal-register",
            "--since",
            "2026-08-24",
            "--until",
            "2026-08-24",
        ]
    ) == 0
    output = capsys.readouterr().out.splitlines()
    assert output[0].startswith("urn:spicy:artifact:source-catalog:")
    assert output[1].startswith("sha256:")
    admit_artifact(
        LocalMemberSource(destination),
        semantic_verifier=verify_source_catalog_semantics,
    )


def test_cli_requests_the_bounded_existing_regulations_reader_factory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from spicy_regs import source_catalog_cli

    calls: dict[str, object] = {}

    def fake_factory(record_types, **options):
        calls["record_types"] = record_types
        calls.update(options)
        return lambda agency, record_type: FakeReader([regulations_raw(f"{agency}-2026-0001")])

    monkeypatch.setattr(source_catalog_cli.mirrulations, "reader_factory", fake_factory)
    destination = tmp_path / "regulations-cli"

    assert source_catalog_cli.main(
        [
            "--catalog-id",
            "urn:spicy-regs:catalog:regulations-cli-test",
            "--destination",
            str(destination),
            "regulations-gov",
            "--agency",
            "EPA",
        ]
    ) == 0
    assert calls["record_types"] == [DOCUMENT]
    assert calls["bounded"] is True
    admit_artifact(
        LocalMemberSource(destination),
        semantic_verifier=verify_source_catalog_semantics,
    )
