"""Older-bill child dispositions and resumable membership."""

import json

import httpx
import pytest
from spicy_docs.sources.congress.bill_status import BillIdentity
from spicy_docs.transport.credentials import CredentialRefusedError

from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.sources.bill_children import OlderBillChildSource

IDENTITY = BillIdentity(107, "hr", 3162)
DETAIL = "https://api.congress.gov/v3/bill/107/hr/3162"
PARENT = {
    "summaries": {"count": 0, "url": DETAIL + "/summaries"},
    "actions": {"count": 2, "url": DETAIL + "/actions"},
    "cosponsors": {"count": 9, "url": DETAIL + "/cosponsors"},
}
PARENT_TIME = "2026-01-01T00:00:00Z"
CHILD_TIME = "2026-02-02T00:00:00Z"


def reply(route, records, *, count, next_url=None, status=200):
    body = json.dumps(
        {
            "request": {"congress": "107", "billType": "hr", "billNumber": "3162"},
            route: records,
            "pagination": {"count": count, "next": next_url},
        }
    ).encode()
    return httpx.Response(
        status,
        headers={"Content-Type": "application/json"},
        stream=httpx.ByteStream(body),
        extensions={"spicy_docs_retained_observed_at": CHILD_TIME},
    )


def source(tmp_path, name, send):
    return OlderBillChildSource(
        tmp_path / "checkpoints",
        api_key="test-api-key",
        action_page_size=1,
        evidence=CaptureEvidence(tmp_path / name, "bill-family", archive_root=tmp_path / "raw"),
        transport=lambda: httpx.MockTransport(send),
    )


def test_truncated_action_walk_preserves_completed_prefix_and_summary(tmp_path):
    calls = []
    second = DETAIL + "/actions?format=json&limit=1&offset=1"
    one = {"actionDate": "2001-10-25", "text": "A repeated action"}

    def initial(request):
        calls.append(str(request.url))
        if request.url.path.endswith("/summaries"):
            return reply("summaries", [], count=0)
        if request.url.params.get("offset") is None:
            return reply("actions", [one], count=2, next_url=second)
        return reply("actions", [], count=2)  # terminal short page

    partial = source(tmp_path, "partial", initial).read(
        IDENTITY, PARENT, parent_observed_at=PARENT_TIME, charge=lambda: None
    )
    outcomes = {r["route"]: r for r in partial.routes}
    assert partial.actions is None and partial.summaries == ()
    assert outcomes["summaries"]["outcome"] == "requested-empty"
    assert outcomes["actions"]["outcome"] == "refused"
    assert outcomes["actions"]["observed_count"] is None
    assert len(outcomes["actions"]["page_pins"]) == 1
    assert outcomes["cosponsors"]["outcome"] == "unrequested"
    assert outcomes["cosponsors"]["parent_declared_count"] == 9
    assert outcomes["cosponsors"]["observed_count"] is None
    assert len(calls) == 3

    def resume(request):
        assert str(request.url) == second
        calls.append(str(request.url))
        return reply("actions", [one], count=2)  # literal duplicates are separate source occurrences

    resumed = source(tmp_path, "resume", resume).read(
        IDENTITY, PARENT, parent_observed_at=PARENT_TIME, charge=lambda: None
    )
    assert [a.text for a in resumed.actions] == [one["text"], one["text"]]
    assert all(a.child_witness.observed_at == CHILD_TIME for a in resumed.actions)
    assert len(calls) == 4

    def unexpected(_request):
        pytest.fail("a completed child read made another HTTP request")

    again = source(tmp_path, "again", unexpected).read(
        IDENTITY, PARENT, parent_observed_at=PARENT_TIME, charge=lambda: None
    )
    assert again == resumed


def test_unavailable_and_credential_refused_differ(tmp_path):
    calls = []

    def send(request):
        calls.append(str(request.url))
        if request.url.path.endswith("/summaries"):
            return reply("summaries", [], count=0, status=404)
        return reply("actions", [], count=0, status=403)

    acquired = source(tmp_path, "attempt", send)
    summary_only = {name: link for name, link in PARENT.items() if name != "actions"}
    result = acquired.read(IDENTITY, summary_only, parent_observed_at=PARENT_TIME, charge=lambda: None)
    inventory = {row["route"]: row for row in result.routes}
    assert inventory["summaries"]["outcome"] == "unavailable"
    assert inventory["summaries"]["http_status"] == 404
    assert inventory["actions"]["outcome"] == "unrequested"
    with pytest.raises(CredentialRefusedError):
        acquired.read(IDENTITY, PARENT, parent_observed_at=PARENT_TIME, charge=lambda: None)
    journal = (acquired.evidence.artifact_dir / "journal.jsonl").read_text()
    assert "test-api-key" not in journal
    assert '"credential_refused":true' in journal
    assert len(calls) == 3
