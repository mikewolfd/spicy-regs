"""Replay refusals protect source identity, exact queries, hashes and completeness."""

from hashlib import sha256
import json

import pytest

from spicy_regs.scorecards.replay import RetainedScorecardSequence, ScorecardReplayError

URL = "https://source.example/scorecard"


def retained(tmp_path, *, post=False, **changes):
    body = b'{"members":[{"name":"Example","score":"94%"}]}'
    (tmp_path / "response.body").write_bytes(body)
    record = dict(
        requested_url=URL,
        resolved_url=URL,
        http_status=200,
        content_type="application/json",
        observed_at="2026-10-04T00:00:00Z",
        byte_size=len(body),
        sha256=sha256(body).hexdigest(),
        body_file="response.body",
    )
    if post:
        query = b'{"query":"{ ratings { score } }"}'
        (tmp_path / "query.body").write_bytes(query)
        record.update(method="POST", request_body_file="query.body", request_body_sha256=sha256(query).hexdigest())
    record.update(changes)
    manifest = json.dumps([record]).encode()
    (tmp_path / "captures.json").write_bytes(manifest)
    return RetainedScorecardSequence(tmp_path, expected_sha256=sha256(manifest).hexdigest())


def test_exact_get_replay_preserves_original_bytes_and_metadata(tmp_path):
    replay = retained(tmp_path)
    response = replay(URL)
    assert response.body == (tmp_path / "response.body").read_bytes()
    assert response.observed_at == "2026-10-04T00:00:00Z"
    replay.complete()
    with pytest.raises(ScorecardReplayError, match="absent"):
        replay(URL)


def test_post_replay_preserves_exact_publisher_query(tmp_path):
    replay = retained(tmp_path, post=True)
    query = (tmp_path / "query.body").read_bytes()
    response = replay.request(URL, method="POST", content=query)
    assert response.method == "POST"
    assert response.request_body == query
    replay.complete()


def test_private_app_header_pin_reconciles_without_retaining_values(tmp_path):
    headers = {"X-API-Key": "synthetic-app-key"}
    pin = sha256(json.dumps(headers, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    replay = retained(tmp_path, request_headers_sha256=pin)
    with pytest.raises(ScorecardReplayError, match="headers"):
        replay.request(URL, method="GET", content=None, request_headers={"X-API-Key": "different"})
    assert replay.position == 0
    replay.request(URL, method="GET", content=None, request_headers=headers)
    replay.complete()
    assert "synthetic-app-key" not in (tmp_path / "captures.json").read_text()


@pytest.mark.parametrize("change", [{"body_file": "../outside.body"}, {"sha256": "0" * 64}, {"byte_size": 1}])
def test_invalid_source_pin_or_locator_refuses_without_advancing(tmp_path, change):
    replay = retained(tmp_path, **change)
    with pytest.raises(ScorecardReplayError):
        replay(URL)
    assert replay.position == 0


def test_changed_manifest_refuses(tmp_path):
    retained(tmp_path)
    with pytest.raises(ScorecardReplayError, match="manifest"):
        RetainedScorecardSequence(tmp_path, expected_sha256="0" * 64)


def test_unconsumed_capture_is_incomplete(tmp_path):
    replay = retained(tmp_path)
    with pytest.raises(ScorecardReplayError, match="entire"):
        replay.complete()


@pytest.mark.parametrize("method,content", [("GET", None), ("POST", b"changed query")])
def test_query_method_or_body_change_refuses(tmp_path, method, content):
    replay = retained(tmp_path, post=True)
    with pytest.raises(ScorecardReplayError):
        replay.request(URL, method=method, content=content)
    assert replay.position == 0


def test_selected_byte_limit_applies_to_the_original_body(tmp_path):
    retained(tmp_path)
    manifest = (tmp_path / "captures.json").read_bytes()
    replay = RetainedScorecardSequence(tmp_path, expected_sha256=sha256(manifest).hexdigest(), max_bytes=len(manifest))
    replay.max_bytes = 10
    with pytest.raises(ScorecardReplayError, match="byte bound"):
        replay(URL)
    assert replay.position == 0


@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
def test_unbounded_or_noninteger_replay_limit_refuses_before_input_read(tmp_path, limit):
    with pytest.raises(ValueError, match="positive integers"):
        RetainedScorecardSequence(tmp_path, expected_sha256="0" * 64, max_bytes=limit)
