"""Bounded pinned real-input gate for the Federal Register source profile."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from rulespec_artifacts import LocalMemberSource, Producer

from spicy_regs.federal_register_source_native import iter_federal_register_pages
from spicy_regs.source_native import (
    SourceNativeReleaseBuild,
    SourceNativeReleasePublisher,
    SourceNativeReleaseReader,
)
from spicy_regs.source_native_profiles import FEDERAL_REGISTER_PROFILE
from spicy_regs.source_native_store import LocalSourceNativeBlobStore

_SCOPE = {"publishedFrom": "2026-04-13", "publishedThrough": "2026-04-13"}
_IMPLEMENTATION_ID = "pkg:pypi/spicy-regs@0.1.7?checksum=sha256:" + "a" * 64
_SOURCE_STATE_DIGEST = "sha256:e170cf3ddf2819b0f33ced07e050e18cef0adc43f57c5dfde51e04535ebf13bc"


def _completed_at() -> datetime:
    return datetime(2026, 8, 25, 0, 0, 1, tzinfo=UTC)


@pytest.mark.integration
def test_pinned_federal_register_day_publishes_and_replays_exactly(tmp_path: Path) -> None:
    producer = Producer(
        product="spicy-regs",
        implementation_id=_IMPLEMENTATION_ID,
        verifier_id="urn:spicy-regs:source-native-release-verifier",
        verifier_version="1.0",
        verifier_implementation_id=_IMPLEMENTATION_ID,
    )
    with httpx.Client(
        headers={"User-Agent": "spicy-regs-source-native/1.0 (https://github.com/civictechdc/spicy-regs)"},
        timeout=60.0,
        follow_redirects=True,
    ) as client:

        def fetch(url: str) -> bytes:
            response = client.get(url)
            response.raise_for_status()
            return response.content

        published = SourceNativeReleasePublisher(
            FEDERAL_REGISTER_PROFILE,
            blob_store=LocalSourceNativeBlobStore(tmp_path / "blobs"),
            clock=_completed_at,
        ).publish(
            iter_federal_register_pages(fetch, query_scope=_SCOPE),
            build=SourceNativeReleaseBuild(
                query_scope=_SCOPE,
                producer=producer,
                started_at="2026-08-25T00:00:00Z",
            ),
            destination=tmp_path / "release",
        )

    reader = SourceNativeReleaseReader(
        LocalMemberSource(published.root),
        blob_source=LocalSourceNativeBlobStore(tmp_path / "blobs"),
        profile=FEDERAL_REGISTER_PROFILE,
        expected_pin=published.artifact.pin,
        accepted_verifier_implementation_ids=frozenset({_IMPLEMENTATION_ID}),
    )
    records = list(reader.iter_records())
    renditions = list(reader.iter_renditions())

    assert reader.source_state_digest == _SOURCE_STATE_DIGEST
    assert len(records) == 93
    assert len(renditions) == 279
    assert records[0]["sourceRecordId"] == "2026-07034"
    assert records[-1]["sourceRecordId"] == "2026-07143"
    assert sum(not record["record"].get("topics") for record in records) == 84
