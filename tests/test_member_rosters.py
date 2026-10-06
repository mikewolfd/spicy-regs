"""Selected fork bytes keep their own source identity and source refusals."""

from copy import deepcopy
from hashlib import sha256
import json

import httpx
import pytest
from spicy_docs.sources.legislators import LegislatorsAcquirer, LegislatorsBudget, LegislatorsSourceError
from spicy_docs.transport.captured import attached_capture

from spicy_regs.sources.member_rosters import ReviewedMemberRosters, SELECTION


def capture(*, lis=True):
    return json.dumps([{
        "id": {"bioguide": "A000001", **({"lis": "S001"} if lis else {})},
        "name": {"first": "Example", "last": "Person", "nickname": "  Literal  "},
        "terms": [{"type": "sen", "start": "2025-01-03", "state": "VT", "class": 1}],
    }]).encode()


def reader(raw, *, expected=None, content_type="text/plain; charset=utf-8"):
    selection = json.loads(SELECTION.read_bytes())
    for member in selection.values():
        member.update(sha256=sha256(raw if expected is None else expected).hexdigest(),
                      bytes=len(raw if expected is None else expected))
    requests = []

    def respond(request):
        requests.append(str(request.url))
        return httpx.Response(200, stream=httpx.ByteStream(raw), headers={"content-type": content_type})

    return ReviewedMemberRosters(
        budget=LegislatorsBudget(4, 1024 * 1024, 10.0, 0.0), selection=selection,
        transport=httpx.MockTransport(respond),
    ), requests, selection


@pytest.mark.parametrize("content_type", ["application/json", "text/plain; charset=utf-8"])
def test_selected_rosters_request_and_capture_the_immutable_fork_urls(content_type):
    owner, requests, selection = reader(capture(), content_type=content_type)
    with owner:
        current = owner.acquire_current()
        historical = owner.acquire_historical()
    assert requests == [selection[name]["url"] for name in ("current", "historical")]
    assert current.capture.requested_url == requests[0]
    assert historical.capture.requested_url == requests[1]
    assert current.capture.content_type == content_type
    assert historical.capture.content_type == content_type
    assert current.file.records[0].name_nickname == "  Literal  "


def test_plain_text_allowance_does_not_change_the_maintained_upstream_reader():
    raw = capture()
    owner = LegislatorsAcquirer(
        budget=LegislatorsBudget(4, 1024 * 1024, 10.0, 0.0),
        transport=httpx.MockTransport(lambda request: httpx.Response(
            200, stream=httpx.ByteStream(raw), headers={"content-type": "text/plain; charset=utf-8"},
        )),
    )
    with owner, pytest.raises(LegislatorsSourceError, match="Content-Type"):
        owner.acquire_current()


def test_plain_text_allowance_does_not_apply_to_unselected_adapter_routes():
    owner, _, _ = reader(capture())
    with owner, pytest.raises(LegislatorsSourceError, match="Content-Type"):
        owner.capture_validated(
            "https://example.com/other.json", media_types=("application/json",),
            parse=lambda response, allowance: response.body, max_bytes=1024 * 1024,
            unavailable=lambda response: LegislatorsSourceError("unavailable"), context={},
        )


@pytest.mark.parametrize("content_type", ["text/html", "application/octet-stream"])
def test_other_response_media_types_still_refuse(content_type):
    owner, _, _ = reader(capture(), content_type=content_type)
    with owner, pytest.raises(LegislatorsSourceError, match="Content-Type") as failure:
        owner.acquire_current()
    response = attached_capture(failure.value)
    assert response is not None
    assert response.content_type == content_type


@pytest.mark.parametrize("raw", [b"not JSON", b"{}"])
def test_pinned_plain_text_still_requires_valid_roster_json(raw):
    owner, _, _ = reader(raw)
    with owner, pytest.raises(LegislatorsSourceError) as failure:
        owner.acquire_current()
    response = attached_capture(failure.value)
    assert response is not None
    assert response.body == raw


def test_plain_text_route_still_obeys_the_requested_byte_limit():
    raw = capture()
    owner, _, _ = reader(raw)
    with owner, pytest.raises(LegislatorsSourceError):
        owner.acquire_current(max_bytes=len(raw) - 1)


def test_changed_fork_bytes_refuse_before_the_member_producer_uses_them():
    owner, _, _ = reader(capture(), expected=capture() + b" ")
    with owner, pytest.raises(LegislatorsSourceError, match="differ from the approved input") as failure:
        owner.acquire_current()
    response = attached_capture(failure.value)
    assert response is not None
    assert response.requested_url.startswith("https://raw.githubusercontent.com/")


def test_current_roster_still_requires_the_source_readers_lis_postcondition():
    owner, _, selection = reader(capture(lis=False))
    with owner, pytest.raises(LegislatorsSourceError, match="no id.lis") as failure:
        owner.acquire_current()
    response = attached_capture(failure.value)
    assert response is not None
    assert getattr(failure.value, "legislators_acquisition")["url"] == selection["current"]["url"]
    assert response.requested_url == selection["current"]["url"]


@pytest.mark.parametrize("change", ["branch", "mixed_commit", "missing_roster"])
def test_unreviewed_or_mixed_roster_selection_refuses_before_acquisition(change):
    selection = deepcopy(json.loads(SELECTION.read_bytes()))
    if change == "missing_roster":
        del selection["historical"]
    else:
        url = selection["historical"]["url"]
        selection["historical"]["url"] = url.replace(url.split("/")[-2], "main" if change == "branch" else "a" * 40)
    with pytest.raises(ValueError):
        ReviewedMemberRosters(budget=LegislatorsBudget(4, 1024 * 1024, 10.0, 0.0), selection=selection)


@pytest.mark.parametrize("selection", [[], 42, {"current": []}, {"current": "invalid"}])
def test_malformed_selection_refuses_with_a_controlled_error(selection):
    with pytest.raises(ValueError, match="roster mappings"):
        ReviewedMemberRosters(budget=LegislatorsBudget(4, 1024 * 1024, 10.0, 0.0), selection=selection)


def test_consumed_selection_is_copied_and_immutable():
    owner, _, supplied = reader(capture())
    original = owner.selection["current"]["sha256"]
    supplied["current"]["sha256"] = "0" * 64
    assert owner.selection["current"]["sha256"] == original
    with pytest.raises(TypeError):
        owner.selection["current"]["sha256"] = "0" * 64
