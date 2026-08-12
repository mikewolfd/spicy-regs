"""Hermetic failure-path checks for the SourceCatalog release tools."""

from __future__ import annotations

import importlib.util
import json
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from spicy_regs.source_catalog import (
    NormalizationPolicy,
    PinnedSource,
    SourceCatalogError,
    UniverseSpec,
    composite_source_version,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
TOOLS = REPO_ROOT / "tools"


def _load(name: str):
    if str(TOOLS) not in sys.path:
        sys.path.insert(0, str(TOOLS))
    spec = importlib.util.spec_from_file_location(name, TOOLS / f"{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


mirror_tool = _load("build_mirrulations_mirror_index")
publish_tool = _load("publish_source_catalog_release")


def _manifest(*records: dict[str, Any]) -> dict[str, Any]:
    return {
        "documents": {"EPA-2023-0001-0001": list(records)},
        "schemaVersion": mirror_tool.INDEX_SCHEMA_VERSION,
        "source": {"bucket": "mirrulations", "prefix": "raw-data"},
        "window": {"from": "2021-01-01", "to": "2025-12-31"},
    }


def _draw_record(*, size: int = 12) -> dict[str, Any]:
    return {
        "etag": '"abc123"',
        "key": "raw-data/EPA/EPA-2023-0001/text-EPA-2023-0001/documents/EPA-2023-0001-0001_content.htm",
        "lastModified": "2026-08-12T00:00:00Z",
        "size": size,
    }


def _write_json(path: Path, value: Any) -> Path:
    path.write_text(json.dumps(value), encoding="utf-8")
    return path


def test_draw_refuses_to_publish_a_manifest_when_a_docket_listing_fails(monkeypatch, tmp_path: Path) -> None:
    from spicy_regs.sources import mirrulations

    class FailingPaginator:
        def paginate(self, **_: Any):
            raise TimeoutError("listing timed out")

    class Client:
        def get_paginator(self, _: str) -> FailingPaginator:
            return FailingPaginator()

    monkeypatch.setattr(
        mirror_tool,
        "_document_ids",
        lambda _catalog, _window: {"EPA-2023-0001-0001": ("EPA", "EPA-2023-0001")},
    )
    monkeypatch.setattr(mirrulations, "s3_client", Client)
    output = tmp_path / "draw.json"

    with pytest.raises(mirror_tool.MirrorIndexError, match="1 of 1 docket listings failed"):
        mirror_tool.draw(tmp_path / "catalog.parquet", ("2021-01-01", "2025-12-31"), output, workers=1)

    assert not output.exists()


def test_failed_fetch_receipt_remains_eligible_for_retry(tmp_path: Path) -> None:
    receipts = tmp_path / "receipts.jsonl"
    receipts.write_text(json.dumps({"error": "timeout", "key": _draw_record()["key"]}) + "\n", encoding="utf-8")

    assert mirror_tool._already_fetched(receipts) == set()


def test_success_receipt_for_the_wrong_document_remains_eligible_for_retry(tmp_path: Path) -> None:
    record = _draw_record()
    receipts = tmp_path / "receipts.jsonl"
    receipts.write_text(
        json.dumps(
            {
                "documentId": "EPA-WRONG-DOCUMENT",
                "key": record["key"],
                "sha256": "a" * 64,
                "size": record["size"],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    expected = {record["key"]: ("EPA-2023-0001-0001", record["size"])}
    assert mirror_tool._already_fetched(receipts, expected) == set()


def test_fetch_refuses_to_silently_skip_an_oversized_drawn_object(tmp_path: Path) -> None:
    draw = _write_json(tmp_path / "draw.json", _manifest(_draw_record(size=mirror_tool.MAX_OBJECT_BYTES + 1)))

    with pytest.raises(mirror_tool.MirrorIndexError, match="larger than"):
        mirror_tool.fetch(draw, tmp_path / "receipts.jsonl", workers=1)


def test_seal_refuses_a_missing_receipt(tmp_path: Path) -> None:
    draw = _write_json(tmp_path / "draw.json", _manifest(_draw_record()))
    receipts = tmp_path / "receipts.jsonl"
    receipts.write_text("", encoding="utf-8")

    with pytest.raises(mirror_tool.MirrorIndexError, match="no valid successful receipt"):
        mirror_tool.seal(draw, receipts, tmp_path / "index.json")


def test_seal_refuses_a_receipt_whose_size_differs_from_the_draw(tmp_path: Path) -> None:
    record = _draw_record(size=12)
    draw = _write_json(tmp_path / "draw.json", _manifest(record))
    receipts = tmp_path / "receipts.jsonl"
    receipts.write_text(
        json.dumps(
            {
                "documentId": "EPA-2023-0001-0001",
                "key": record["key"],
                "sha256": "a" * 64,
                "size": 11,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(mirror_tool.MirrorIndexError, match="not the drawn size"):
        mirror_tool.seal(draw, receipts, tmp_path / "index.json")


def _universe(*, sources: tuple[PinnedSource, ...] = (), source_id: str | None = None) -> UniverseSpec:
    version = composite_source_version(sources) if sources else "sha256:" + "0" * 64
    return UniverseSpec(
        universe_id="urn:test:universe",
        catalog_id="urn:test:catalog",
        policy_id="urn:test:policy",
        policy_version="1",
        source_system_id=source_id or publish_tool.CATALOG_SOURCE_ID,
        source_system_version=version,
        normalization=NormalizationPolicy(language="en"),
        sources=sources,
    )


def test_publish_refuses_an_input_the_universe_does_not_declare(tmp_path: Path) -> None:
    catalog = tmp_path / "catalog.parquet"
    mirror = tmp_path / "mirror.json"
    catalog.write_bytes(b"catalog")
    mirror.write_bytes(b"mirror")
    spec = replace(_universe(), source_system_version=publish_tool.file_digest(catalog))

    with pytest.raises(SourceCatalogError, match="supplies s3://mirrulations/raw-data, but the universe does not declare"):
        publish_tool._validate_source_inputs(
            spec,
            catalog=catalog,
            mirror_index=mirror,
            federal_register=None,
        )


def test_publish_refuses_a_universe_that_does_not_declare_its_catalog_input(tmp_path: Path) -> None:
    catalog = tmp_path / "catalog.parquet"
    mirror = tmp_path / "mirror.json"
    catalog.write_bytes(b"catalog")
    mirror.write_bytes(b"mirror")
    sources = (
        PinnedSource(
            source_system_id=publish_tool.MIRROR_SOURCE_ID,
            source_system_version=publish_tool.file_digest(mirror),
            role="rendition",
        ),
    )

    with pytest.raises(SourceCatalogError, match="universe does not declare it"):
        publish_tool._validate_source_inputs(
            _universe(sources=sources, source_id="urn:test:composite"),
            catalog=catalog,
            mirror_index=mirror,
            federal_register=None,
        )


def test_publish_accepts_exactly_declared_and_pinned_inputs(tmp_path: Path) -> None:
    catalog = tmp_path / "catalog.parquet"
    mirror = tmp_path / "mirror.json"
    federal_register = tmp_path / "federal-register.parquet"
    catalog.write_bytes(b"catalog")
    mirror.write_bytes(b"mirror")
    federal_register.write_bytes(b"federal-register")
    sources = (
        PinnedSource(
            source_system_id=publish_tool.MIRROR_SOURCE_ID,
            source_system_version=publish_tool.file_digest(mirror),
            role="rendition",
        ),
        PinnedSource(
            source_system_id=publish_tool.CATALOG_SOURCE_ID,
            source_system_version=publish_tool.file_digest(catalog),
            role="metadata",
        ),
        PinnedSource(
            source_system_id=publish_tool.FEDERAL_REGISTER_SOURCE_ID,
            source_system_version=publish_tool.file_digest(federal_register),
            role="rendition",
        ),
    )

    publish_tool._validate_source_inputs(
        _universe(sources=sources, source_id="urn:test:composite"),
        catalog=catalog,
        mirror_index=mirror,
        federal_register=federal_register,
    )
