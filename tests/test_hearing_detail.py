"""Dated hearing joins keep exact independent source observations and reusable text."""

import json
from importlib.metadata import version
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow.parquet as pq
import pytest
from spicy_docs.sources.govinfo.bodies import parse_package_id
from spicy_docs.transport.captured import CapturedBodyResponse

from spicy_regs.sources.hearing_detail import hearing_detail_facts, hearing_detail_urls
from spicy_regs.transforms.build_committee_reports import build_committee_reports, _reuse_hearing_text
from spicy_regs.transforms.committee_report_reads import RULE_VERSIONS
from tests.test_committee_reports import (
    HearingBodyAcquirer, JACKET_64431, OBSERVED_AT, StubHearings, TwoHearingsDiscovery, _Page, _no_prior,
)

MISSING = object()


@pytest.mark.parametrize(("meeting", "state"), [
    (MISSING, "missing"), (None, "null"), ({}, "empty"),
    ({"eventId": None}, "null-event-id"), ({"url": "https://example.test"}, "missing-event-id"),
    ({"eventId": "119003"}, "meeting"),
])
def test_native_meeting_states_keep_the_original_value_and_capture(meeting, state):
    identity = parse_package_id(JACKET_64431)
    (url,) = hearing_detail_urls(identity)
    record = {"congress": 119, "jacketNumber": 64431, "chamber": "House"}
    if meeting is not MISSING:
        record["associatedMeeting"] = meeting
    capture = CapturedBodyResponse(url, url, 200, "application/json", OBSERVED_AT,
                                   json.dumps({"hearing": record}).encode())
    result = hearing_detail_facts(identity, [record], capture)
    assert result["state"] == state
    assert result["associated_meeting"] == (None if meeting is MISSING else meeting)
    assert (result["sha256"], result["source_url"], result["observed_at"]) == (capture.sha256, url, OBSERVED_AT)


@pytest.mark.parametrize("meeting", [MISSING, None, {}])
def test_current_only_success_cannot_retract_a_held_dated_join_or_text(tmp_path, meeting):
    from tests.test_govinfo_revisions import prior_download

    first = tmp_path / "first"
    first.mkdir()
    prior = build_committee_reports(first, reader=TwoHearingsDiscovery(), acquirer=HearingBodyAcquirer(),
                                     hearings=StubHearings(), download_prior=_no_prior)
    class Current(StubHearings):
        def records(self, route, url, **kwargs):
            for page in super().records(route, url, **kwargs):
                record = dict(page.records[0])
                record.pop("associatedMeeting", None)
                if meeting is not MISSING:
                    record["associatedMeeting"] = meeting
                capture = CapturedBodyResponse(url, url, 200, "application/json", "2026-11-14T12:00:00Z",
                                               json.dumps({"hearing": record}).encode())
                self.observation = hearing_detail_facts(parse_package_id(JACKET_64431), [record], capture)
                page = _Page((record,))
                page.capture = capture
                yield page
    current = Current(details={"64431": "hearing-119-house-64431.json"})
    retry = tmp_path / "retry"
    retry.mkdir()
    with patch("spicy_regs.transforms.build_committee_reports.body_text", side_effect=AssertionError("re-extracted")):
        outputs = build_committee_reports(retry, reader=TwoHearingsDiscovery(), acquirer=HearingBodyAcquirer(),
                                          hearings=current, download_prior=prior_download(first), reread=(JACKET_64431,))
    assert pq.read_table(outputs[2]).to_pylist() == pq.read_table(prior[2]).to_pylist()
    assert pq.read_table(outputs[3]).to_pylist() == pq.read_table(prior[3]).to_pylist()
    assert current.observation["event_id"] is None
    assert current.observation["observed_at"] == "2026-11-14T12:00:00Z"
    assert current.observation["state"] == ("missing" if meeting is MISSING else "null" if meeting is None else "empty")


def test_reuse_depends_on_text_rules_and_exact_body_not_unrelated_provider_code():
    import hashlib
    text = "retained hearing"
    body = SimpleNamespace(identity=SimpleNamespace(package_id="CHRG-119hhrg1"), format="pdf",
                           body_capture=SimpleNamespace(sha256="sha256:body"))
    row = {"sha256": "sha256:body", "format": "pdf", "body_text": text, "body_completeness": "pdf_extracted",
           "text_sha256": "sha256:" + hashlib.sha256(text.encode()).hexdigest(),
           "text_derivation": "pdf-extraction-gpo-normalized", "page_count": "1"}
    prior = {(body.identity.package_id, "stamp"): row}
    checkpoint = {"last_modified": "stamp", "rule_version": RULE_VERSIONS["CHRG"] + ";code=unrelated"}
    reused = _reuse_hearing_text(body, prior, checkpoint)
    assert reused is not None
    assert reused.text == text
    old = dict(checkpoint, rule_version=checkpoint["rule_version"].replace("pymupdf=" + version("pymupdf"), "pymupdf=old"))
    assert _reuse_hearing_text(body, prior, old) is None
    assert _reuse_hearing_text(body, prior, dict(checkpoint, rule_version=None)) is None
    body.body_capture.sha256 = "sha256:different"
    assert _reuse_hearing_text(body, prior, checkpoint) is None


def test_legacy_unqualified_event_id_is_not_protected_as_a_dated_join():
    from spicy_regs.transforms.build_committee_reports import _qualified_prior_event
    body = SimpleNamespace(mods=SimpleNamespace(event_ids=()))
    assert not _qualified_prior_event({"event_id": "119003"}, body)
    body.mods.event_ids = ("119003",)
    assert _qualified_prior_event({"event_id": "119003"}, body)
    body.mods.event_ids = ()
    assert _qualified_prior_event({"event_id": "119003", "mods_event_ids_json": '["119003"]',
        "hearing_detail_json": '{"relationship":"mods-corroborated","event_id":"119003"}'}, body)


def test_duplicate_prior_dated_keys_are_refused(tmp_path):
    import pyarrow as pa
    from spicy_regs.transforms.build_committee_reports import _prior_hearing_rows
    path = tmp_path / "prior.parquet"
    pq.write_table(pa.Table.from_pylist([{"package_id": "a", "last_modified": "stamp"}] * 2), path)
    with pytest.raises(ValueError, match="repeat a dated key"):
        _prior_hearing_rows(path, {"a"})


def test_hearing_v3_receipts_keep_their_original_shape_under_v4():
    from spicy_regs.legislative_documents import map_subject, recorded_subject
    from spicy_regs.legislative_receipts import policy
    from spicy_regs.policy_migrations import earlier_policy

    old = earlier_policy("hearing_transcripts", "legislative-documents/3")
    raw = {"package_id": "CHRG-119hhrg64431", "last_modified": "2026-09-01T00:00:00Z",
           "event_id": "legacy-detail-id", "mods_event_id": None}
    current = map_subject("hearing_transcripts", raw)
    assert current is not None
    subject = {name: current[name] for name in old.subject_schema.names}
    assert recorded_subject("hearing_transcripts", raw, subject, policy_version=old.policy_version) == subject
    assert policy("hearing_transcripts").policy_version == "legislative-documents/4"
