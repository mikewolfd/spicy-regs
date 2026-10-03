"""Explicit proxy selection preserves source bytes, policy and refusal boundaries."""

import json

import pytest

from spicy_docs.sources.zyte import BROWSER_HTML, HTTP_RESPONSE_BODY, ZyteHttpResponse
from spicy_docs.transport.credentials import CredentialRefusedError
from spicy_regs.scorecards.acquisition import ScorecardTransportError, fetch_for_publishers, zyte_fetch
from spicy_regs.source_evidence import CaptureEvidence, verify_evidence
from spicy_regs.transforms.build_scorecards import _fatal
from tests.test_source_evidence import journal

URL = "https://scorecard.ijm.org/"
BODY = b"<html>original publisher bytes &amp; literal</html>"


class Fetcher:
    def __init__(self, status=200, error=None):
        self.calls = []
        self.status = status
        self.error = error

    def fetch(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if self.error:
            raise self.error
        return ZyteHttpResponse(
            url, url, self.status, "text/html", BODY, mode=kwargs["mode"], request_id="provider-request"
        )


def scope(tmp_path, monkeypatch, policy="hash_only"):
    monkeypatch.setenv("ZYTE_TOKEN", "synthetic-secret")
    root = CaptureEvidence(tmp_path, "scorecards")
    return root.for_source("ijm", policy, parser_version="test/1", policy_decision_id="test-policy")


@pytest.mark.parametrize("policy", ["hash_only", "metadata_only"])
def test_proxy_preserves_raw_bytes_and_records_representation_under_selected_policy(tmp_path, monkeypatch, policy):
    source, fetcher = scope(tmp_path, monkeypatch, policy), Fetcher()
    with zyte_fetch(source, fetcher=fetcher) as fetch:
        captured = fetch(URL)
        assert captured.body == BODY
        source.capture(captured, stage="index")
    assert len(fetcher.calls) == 1
    assert fetcher.calls[0][1]["mode"] == HTTP_RESPONSE_BODY
    proxy = next(row for row in journal(source.root) if row["event"] == "scorecard-proxy")
    assert proxy["body_is_publisher_bytes"] is True
    assert proxy["zyte_request_id"] == "provider-request"
    assert proxy["publisher_id"] == "ijm" and proxy["body_retained"] is False
    assert ("sha256" in proxy) == (policy == "hash_only")
    source.root.seal(outcome="build-complete")
    verify_evidence(source.root.artifact_dir)
    raw = b"\n".join(p.read_bytes() for p in source.root.artifact_dir.rglob("*") if p.is_file())
    assert BODY not in raw and b"synthetic-secret" not in raw


@pytest.mark.parametrize("status", [401, 403])
def test_target_access_refusal_aborts_without_retry_or_raw_retention(tmp_path, monkeypatch, status):
    source, fetcher = scope(tmp_path, monkeypatch), Fetcher(status=status)
    with zyte_fetch(source, fetcher=fetcher) as fetch, pytest.raises(CredentialRefusedError):
        fetch(URL)
    assert len(fetcher.calls) == 1
    assert not any(row["event"] == "capture" for row in journal(source.root))


def test_provider_failure_is_fatal_and_never_logs_secret_or_retries(tmp_path, monkeypatch):
    source = scope(tmp_path, monkeypatch)
    fetcher = Fetcher(error=RuntimeError("synthetic-secret reflected by provider"))
    with zyte_fetch(source, fetcher=fetcher) as fetch, pytest.raises(ScorecardTransportError) as caught:
        fetch(URL)
    assert len(fetcher.calls) == 1
    assert "synthetic-secret" not in str(caught.value)
    assert "synthetic-secret" not in json.dumps(journal(source.root))
    wrapped = ValueError("publisher wrapper")
    wrapped.__cause__ = caught.value
    with pytest.raises(ScorecardTransportError):
        _fatal(wrapped)


def test_request_budget_stops_before_second_paid_call(tmp_path, monkeypatch):
    source, fetcher = scope(tmp_path, monkeypatch), Fetcher()
    with zyte_fetch(source, max_requests=1, fetcher=fetcher) as fetch:
        fetch(URL)
        with pytest.raises(ScorecardTransportError):
            fetch(URL)
    assert len(fetcher.calls) == 1


def test_missing_token_refuses_without_issuing_a_request(tmp_path, monkeypatch):
    source, fetcher = scope(tmp_path, monkeypatch), Fetcher()
    monkeypatch.delenv("ZYTE_TOKEN")
    with pytest.raises(ScorecardTransportError, match="configured credential"), zyte_fetch(source, fetcher=fetcher):
        pytest.fail("Missing credential must refuse first")
    assert not fetcher.calls


def test_only_explicit_publisher_uses_proxy(tmp_path, monkeypatch):
    from contextlib import contextmanager
    from spicy_regs.scorecards import acquisition
    from spicy_regs.transforms import build_scorecards

    used = []

    @contextmanager
    def selected(source):
        used.append("proxy")
        yield "proxy"

    @contextmanager
    def direct(source):
        used.append("direct")
        yield "direct"

    monkeypatch.setattr(acquisition, "zyte_fetch", selected)
    monkeypatch.setattr(build_scorecards, "bounded_fetch", direct)
    factory = fetch_for_publishers(["ijm"])
    source = scope(tmp_path, monkeypatch)
    with factory(source) as fetch:
        assert fetch == "proxy"
    afp = source.root.for_source("afp", "hash_only", parser_version="test/1", policy_decision_id="test-policy")
    with factory(afp) as fetch:
        assert fetch == "direct"
    assert used == ["proxy", "direct"]
    with pytest.raises(ValueError, match="Unknown publisher"):
        fetch_for_publishers(["typo"])


def test_browser_api_rendition_has_distinct_provenance_and_shared_budget(tmp_path, monkeypatch):
    source, fetcher = scope(tmp_path, monkeypatch), Fetcher()
    api = URL + "wp-json/rds-bt50-scorecard/v1/legislators/detail/?legislatorId=20777&scorecardId=3995"
    with zyte_fetch(source, browser_api=True, max_requests=2, fetcher=fetcher) as fetch:
        fetch(URL)
        captured = fetch(api)
        assert captured.body == BODY
        source.capture(captured, stage="browser-api")
        with pytest.raises(ScorecardTransportError):
            fetch(URL)
    assert [call[1]["mode"] for call in fetcher.calls] == [HTTP_RESPONSE_BODY, BROWSER_HTML]
    records = [row for row in journal(source.root) if row["event"] == "scorecard-proxy"]
    assert [row["body_is_publisher_bytes"] for row in records] == [True, False]
    assert [row["mode"] for row in records] == [HTTP_RESPONSE_BODY, BROWSER_HTML]
    source.root.seal(outcome="build-complete")
    verify_evidence(source.root.artifact_dir)


@pytest.mark.parametrize(
    "url", [URL + "app.js", URL + "wp-json/another-route", "https://example.test/wp-json/rds-bt50-scorecard/v1/x"]
)
def test_browser_selection_is_limited_to_original_ijm_api_routes(tmp_path, monkeypatch, url):
    source, fetcher = scope(tmp_path, monkeypatch), Fetcher()
    with zyte_fetch(source, browser_api=True, fetcher=fetcher) as fetch:
        fetch(url)
    assert fetcher.calls[0][1]["mode"] == HTTP_RESPONSE_BODY


def test_browser_selection_rejects_unqualified_or_conflicting_publishers():
    with pytest.raises(ValueError, match="only supported for IJM"):
        fetch_for_publishers([], browser_publishers=["afp"])
    with pytest.raises(ValueError, match="one Zyte rendition"):
        fetch_for_publishers(["ijm"], browser_publishers=["ijm"])


def test_rollup_rejects_custom_fetch_combined_with_browser_selection():
    from spicy_regs.pipelines.rollups.scorecards import ScorecardsRollup

    with pytest.raises(ValueError, match="injected fetch factory"):
        ScorecardsRollup(fetch_factory=object(), zyte_browser_publishers=["ijm"])
