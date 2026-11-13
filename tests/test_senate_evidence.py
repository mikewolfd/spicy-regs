"""The serving-rung event has one meaning across actual Senate reader paths."""

import gzip
import json
from typing import cast

import httpx
import pytest
from spicy_docs.sources.congress.committee_rosters import SENATE_CVC_URL
from spicy_docs.sources.congress.votes import VoteSourceError
from spicy_docs.sources.walled_fetch import ProxyFetchers, ZyteHttpFetcher
from spicy_docs.storage.raw_archive import iter_acquisition_receipts

from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.sources.senate_evidence import record_serving_rung
from spicy_regs.sources.senate_roster_ladder import LadderCommitteeRosterAcquirer
from spicy_regs.sources.senate_vote_ladder import LadderVoteAcquirer
from tests.test_senate_roster_ladder import BUDGET as ROSTER_BUDGET, XML
from tests.test_senate_vote_ladder import BUDGET, SENATE_1, VOTE, Evidence, Fetcher, digest, replay
from tests.test_source_evidence import capture, journal


@pytest.mark.parametrize("reader", ["vote", "roster"])
@pytest.mark.parametrize("rung", ["direct", "zyte_http", "internet_archive"])
def test_same_event_fields_for_each_successful_reader_path(reader, rung, monkeypatch):
    monkeypatch.setattr("spicy_regs.sources.senate_vote_ladder.ARCHIVE_INTERVAL_SECONDS", 0)
    body = VOTE if reader == "vote" else XML
    original = SENATE_1.url() if reader == "vote" else SENATE_CVC_URL
    timestamp = "20260719010444"
    archived = gzip.compress(body, mtime=0)

    def respond(request):
        if request.url.host == "web.archive.org":
            if request.url.path == "/cdx/search/cdx":
                index = json.dumps([["timestamp", "original", "digest"],
                                    [timestamp, original, digest(archived)]]).encode()
                return httpx.Response(200, headers={"content-type": "application/json"},
                                      stream=httpx.ByteStream(index))
            assert str(request.url) == replay(timestamp, original)
            return httpx.Response(200, headers={"content-type": "text/xml", "content-encoding": "gzip"},
                                  stream=httpx.ByteStream(archived))
        return httpx.Response(200 if rung == "direct" else 403, headers={"content-type": "text/xml"},
                              stream=httpx.ByteStream(body if rung == "direct" else b"Forbidden"))

    evidence = Evidence()
    cls, budget = (LadderVoteAcquirer, BUDGET) if reader == "vote" else (LadderCommitteeRosterAcquirer, ROSTER_BUDGET)
    with cls(budget=budget, transport=httpx.MockTransport(respond), evidence=cast(CaptureEvidence, evidence)) as source:
        source._proxies = ProxyFetchers(cast(ZyteHttpFetcher, Fetcher(body)) if rung == "zyte_http" else "disabled", "disabled")
        acquired = source.acquire(SENATE_1) if isinstance(source, LadderVoteAcquirer) else source.acquire_senate(
            archived_at=timestamp if rung == "internet_archive" else None
        )
    [event] = [fields for name, fields in evidence.events if name == "source-serving-rung"]
    assert event == {
        "operation": "roll-call-vote" if reader == "vote" else "senate-cvc",
        "original_url": original,
        "transport": rung,
        "provider_request_id": "req-1" if rung == "zyte_http" else None,
        "requested_url": replay(timestamp, original) if rung == "internet_archive" else original,
        "resolved_url": acquired.capture.resolved_url,
        "observed_at": acquired.capture.observed_at,
        "sha256": acquired.capture.sha256,
    }


def test_failed_archive_validation_has_no_successful_serving_event(monkeypatch):
    from tests.test_senate_vote_ladder import Archive, no_proxies

    monkeypatch.setattr("spicy_regs.sources.senate_vote_ladder.ARCHIVE_INTERVAL_SECONDS", 0)
    timestamp, evidence = "20260719010444", Evidence()
    archive = Archive(
        {replay(timestamp, SENATE_1.url()): (VOTE + b"changed", {"content-type": "text/xml"})},
        [[timestamp, SENATE_1.url(), digest(VOTE)]],
    )
    with no_proxies(archive, evidence) as source, pytest.raises(VoteSourceError):
        source.acquire(SENATE_1)
    assert evidence.events == []


def test_shared_event_survives_in_the_central_receipt_archive(tmp_path):
    evidence, raw = CaptureEvidence(tmp_path, "test"), capture()
    record_serving_rung(evidence, raw, operation="roll-call-vote", original_url=raw.requested_url, transport="direct")
    [event] = [row for row in journal(evidence) if row["event"] == "source-serving-rung"]
    [(_, retained)] = iter_acquisition_receipts(evidence.store.root)
    assert retained["receipt"] == {**event, "run_id": evidence.run_id, "family": "test"}
    assert retained["receipt"]["event"] == "source-serving-rung"
    assert retained["receipt"]["sha256"] == raw.sha256
    assert retained["files"] == [], "a rung event must not duplicate or invent a body capture"
