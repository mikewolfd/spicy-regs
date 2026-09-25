"""Hermetic tests for the OpenFEC committees ingest (no network).

Exercises the real SpicyDocs transport and pagination with supplied responses,
raw capture evidence, refusal/cleanup paths, and the existing table merge.
"""

from __future__ import annotations

import json
import importlib
import time
from itertools import pairwise
from types import SimpleNamespace

import httpx

import pyarrow.parquet as pq
import pytest

from spicy_regs.transforms.build_fec_committees import (
    API_KEY_ENV_VARS,
    COLUMNS,
    _resolve_api_key,
    _shape,
    iter_fec_committee_records,
)
from spicy_regs.transforms.build_fec_committees import write_fec_committee_rows

_KEY = "private-api-test-credential"


@pytest.fixture(autouse=True)
def clock(monkeypatch):
    """A fake monotonic clock that sleeping advances: pacing and 429 waits never really sleep.

    SpicyDocs' transport, its retries and this builder's deadline all read ``time``.
    """
    state = SimpleNamespace(now=1000.0, slept=[])

    def sleep(seconds):
        state.slept.append(seconds)
        state.now += seconds

    monkeypatch.setattr(time, "monotonic", lambda: state.now)
    monkeypatch.setattr(time, "sleep", sleep)
    return state


_RAW_COMMITTEE = {
    "committee_id": "C00684373",
    "name": "EXAMPLE FOR PRESIDENT",
    "committee_type": "P",
    "committee_type_full": "Presidential",
    "designation": "P",
    "designation_full": "Principal campaign committee",
    "party": "DEM",
    "party_full": "DEMOCRATIC PARTY",
    "state": "MA",
    "treasurer_name": "DOE, JANE",
    "organization_type": None,
    "organization_type_full": None,
    "filing_frequency": "A",
    "first_file_date": "2018-08-03",
    "last_file_date": "2021-02-01",
    "cycles": [2018, 2020, 2022],
    "candidate_ids": ["P00008052"],
    # Fields present in the payload but intentionally not published:
    "affiliated_committee_name": "NONE",
    "designated_agent_name": "DOE, JANE",
}


def test_retained_rows_use_bounded_batches_and_existing_shape(tmp_path, monkeypatch):
    module = importlib.import_module("spicy_regs.transforms.build_fec_committees")
    real_writer = module.pq.ParquetWriter
    written = []

    class ObservedWriter:
        def __init__(self, *args, **kwargs):
            self.writer = real_writer(*args, **kwargs)

        def __enter__(self):
            return self

        def __exit__(self, *error):
            self.writer.close()

        def write_table(self, table):
            written.append(table.num_rows)
            self.writer.write_table(table)

    monkeypatch.setattr(module.pq, "ParquetWriter", ObservedWriter)

    def records():
        for index in range(5):
            if index >= 2:
                assert sum(written) >= (index // 2) * 2
            yield {**_RAW_COMMITTEE, "committee_id": f"C{index:08}"}

    target = write_fec_committee_rows(records(), tmp_path / "selected.parquet", batch_size=2)
    assert written == [2, 2, 1]
    table = pq.read_table(target)
    assert table.column_names == list(COLUMNS)
    assert table.to_pylist() == [_shape({**_RAW_COMMITTEE, "committee_id": f"C{i:08}"}) for i in range(5)]


def test_failed_retained_input_preserves_previous_output(tmp_path):
    target = write_fec_committee_rows([_RAW_COMMITTEE], tmp_path / "selected.parquet")
    prior = target.read_bytes()

    def failed():
        yield {**_RAW_COMMITTEE, "name": "CHANGED BEFORE FAILURE"}
        raise RuntimeError("retained input failed verification")

    with pytest.raises(RuntimeError, match="failed verification"):
        write_fec_committee_rows(failed(), target, batch_size=1)
    assert target.read_bytes() == prior
    assert list(tmp_path.iterdir()) == [target]


def test_empty_retained_input_keeps_schema_and_duplicate_rows_survive(tmp_path):
    target = write_fec_committee_rows(iter(()), tmp_path / "empty.parquet")
    assert pq.read_table(target).column_names == list(COLUMNS)
    assert pq.ParquetFile(target).metadata.num_rows == 0
    write_fec_committee_rows([_RAW_COMMITTEE, _RAW_COMMITTEE], target)
    assert pq.read_table(target).to_pylist() == [_shape(_RAW_COMMITTEE)] * 2


@pytest.mark.parametrize("value", [0, -1, True])
def test_retained_input_rejects_invalid_batch_bound(tmp_path, value):
    with pytest.raises(ValueError, match="batch_size"):
        write_fec_committee_rows([], tmp_path / "unused.parquet", batch_size=value)


def test_default_builder_uses_the_shared_row_writer(tmp_path, monkeypatch):
    module = importlib.import_module("spicy_regs.transforms.build_fec_committees")
    monkeypatch.setattr(module.r2, "download", lambda *a: False)
    monkeypatch.setattr(
        module, "iter_fec_committee_records", lambda **kwargs: (row for row in [_RAW_COMMITTEE])
    )
    real_write = module.write_fec_committee_rows
    calls = []

    def write(records, destination):
        calls.append(destination.name)
        return real_write(records, destination, batch_size=1)

    monkeypatch.setattr(module, "write_fec_committee_rows", write)
    result = module.build_fec_committees(tmp_path)
    assert calls == ["_fec_new.parquet"]
    assert pq.read_table(result).to_pylist() == [_shape(_RAW_COMMITTEE)]


def test_shape_produces_exact_schema():
    row = _shape(_RAW_COMMITTEE)
    # Every published column present, and nothing extra (16-column schema).
    assert set(row) == set(COLUMNS)
    assert len(COLUMNS) == 16


def test_shape_maps_and_serializes_fields():
    row = _shape(_RAW_COMMITTEE)
    assert row["committee_id"] == "C00684373"
    assert row["name"] == "EXAMPLE FOR PRESIDENT"
    assert row["committee_type"] == "P"
    assert row["committee_type_full"] == "Presidential"
    assert row["designation_full"] == "Principal campaign committee"
    assert row["party_full"] == "DEMOCRATIC PARTY"
    assert row["state"] == "MA"
    assert row["treasurer_name"] == "DOE, JANE"
    assert row["first_file_date"] == "2018-08-03"
    assert row["last_file_date"] == "2021-02-01"
    # Array fields are serialized to JSON strings.
    assert json.loads(row["cycles_json"]) == [2018, 2020, 2022]
    assert json.loads(row["candidate_ids_json"]) == ["P00008052"]


def test_shape_handles_missing_fields_and_null_arrays():
    row = _shape({"committee_id": "C99999999"})
    assert row["committee_id"] == "C99999999"
    # Missing scalars degrade to null, not KeyError.
    assert row["name"] is None
    assert row["organization_type_full"] is None
    assert row["party_full"] is None
    # Missing / null array fields serialize to an empty JSON array, not null.
    assert row["cycles_json"] == "[]"
    assert row["candidate_ids_json"] == "[]"


def test_shape_omits_unpublished_organization_type_code():
    # organization_type (the bare code) is not in the pinned schema; only
    # organization_type_full is published.
    assert "organization_type" not in _shape(_RAW_COMMITTEE)


# -- API-key resolution ------------------------------------------------------


def test_resolve_api_key_prefers_first_env_var(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("DATA_GOV_API_KEY", "data-gov-key")
    monkeypatch.setenv("FEC_API_KEY", "fec-key")
    assert _resolve_api_key() == "data-gov-key"


def test_resolve_api_key_falls_back_in_order(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    # Only the last one set — the fallback chain should still find it.
    monkeypatch.setenv("REGULATIONS_GOV_API_KEY", "regs-key")
    assert _resolve_api_key() == "regs-key"


def test_resolve_api_key_returns_none_when_unset(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    assert _resolve_api_key() is None


def test_walker_refuses_missing_key(monkeypatch, tmp_path):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(ValueError, match="require an API key"):
        list(iter_fec_committee_records(capture_dir=tmp_path))
    assert list(tmp_path.iterdir()) == []


def _page(records, number, *, pages=9, count=0, exact=False):
    """One OpenFEC response page for ``number`` with the given declared pagination."""
    return {
        "results": records,
        "pagination": {
            "page": number,
            "pages": pages,
            "count": count,
            "is_count_exact": exact,
            "last_indexes": None,
        },
    }


def _records(tmp_path, payloads, **kwargs):
    """A walker over a mock transport serving ``payloads`` by page number, recording requests."""
    requests = []

    class ObservedTransport(httpx.MockTransport):
        closed = False

        def close(self):
            self.closed = True
            super().close()

    def respond(request):
        requests.append(request)
        number = int(request.url.params["page"])
        value = payloads[number - 1]
        if isinstance(value, int):
            return httpx.Response(value)
        return httpx.Response(200, json=value)

    transport = ObservedTransport(respond)
    records = iter_fec_committee_records(
        api_key=_KEY,
        capture_dir=tmp_path,
        transport=transport,
        min_interval=0,
        **kwargs,
    )
    return records, requests, transport


def _in_order(tmp_path, responses):
    """A default-paced walker whose mock OpenFEC answers ``responses`` in order, recording request times."""
    served = iter(responses)
    starts = []

    def respond(_request):
        starts.append(time.monotonic())
        return next(served)

    records = iter_fec_committee_records(api_key=_KEY, capture_dir=tmp_path, transport=httpx.MockTransport(respond))
    return records, starts


def _quota(**headers):
    """OpenFEC's rate-limit headers as observed 2026-09-25, plus any extras."""
    return {"X-RateLimit-Limit": "60", "X-RateLimit-Remaining": "59", **headers}


def _assert_credential_free(tmp_path, *texts):
    """Neither retained evidence nor reported text carries the API key."""
    for text in texts:
        assert _KEY not in text
    for path in _run_path(tmp_path).rglob("*"):
        if path.is_file():
            assert _KEY.encode() not in path.read_bytes()


def _run_path(tmp_path):
    """The walker's per-attempt capture directory."""
    (path,) = tmp_path.glob("committees-*")
    return path


def _run_state(tmp_path):
    """The walker's captured ``run.json`` state for the last attempt."""
    return json.loads((_run_path(tmp_path) / "run.json").read_text())


def test_provider_walks_past_short_pages_and_retains_credential_free_evidence(tmp_path):
    payloads = [
        _page([{"committee_id": "C1"}], 1),
        _page([{"committee_id": "C2"}, {"committee_id": "C3"}], 2),
        _page([], 3),
    ]
    records, requests, transport = _records(tmp_path, payloads, per_page=2)
    assert list(records) == [{"committee_id": c} for c in ("C1", "C2", "C3")]
    assert len(requests) == 3
    for request in requests:
        assert request.url.path == "/v1/committees/"
        assert set(request.url.params) == {"sort", "per_page", "page"}
        assert request.url.params["sort"] == "committee_id"
        assert request.headers["X-Api-Key"] == _KEY
    state = _run_state(tmp_path)
    assert state["status"] == "complete"
    assert state["records"] == 3
    assert transport.closed
    run_path = _run_path(tmp_path)
    index = [json.loads(line) for line in (run_path / "pages.jsonl").read_text().splitlines()]
    import hashlib

    for page, expected in zip(index, payloads, strict=True):
        evidence = page["evidence"]
        raw = (run_path / "blobs" / evidence["blob_path"]).read_bytes()
        assert json.loads(raw) == expected
        assert "sha256:" + hashlib.sha256(raw).hexdigest() == evidence["sha256"]
        assert len(raw) == evidence["bytes"]
    for path in run_path.rglob("*"):
        if path.is_file():
            assert _KEY.encode() not in path.read_bytes()


def test_provider_exact_count_terminates_without_unnecessary_empty_request(tmp_path):
    records, requests, transport = _records(
        tmp_path,
        [
            _page([{"committee_id": "C1"}], 1, pages=1, count=1, exact=True),
        ],
    )
    assert len(list(records)) == 1
    assert len(requests) == 1
    assert _run_state(tmp_path)["declared_exact_count"] == 1
    assert transport.closed


@pytest.mark.parametrize(
    ("payloads", "match"),
    [
        ([_page([{"committee_id": "C1"}], 1), 404], "HTTP 404"),
        ([_page([{"committee_id": "C1"}], 1), _page([{"committee_id": "C1"}], 2)], "repeated"),
        ([_page([{"committee_id": "C2"}], 1), _page([{"committee_id": "C1"}], 2)], "ceased increasing"),
        ([_page([{}], 1)], "identifier"),
        ([_page(["invalid"], 1)], "object"),
        ([_page([{"committee_id": "C1"}], 1, pages=1, count=2, exact=True)], "disagree"),
        (
            [
                _page([{"committee_id": "C1"}], 1, pages=2, count=2, exact=True),
                _page([{"committee_id": "C2"}], 2, pages=2, count=3, exact=True),
            ],
            "changed",
        ),
        ([_page([], 1, pages=2, count=2, exact=True)], "empty page"),
        ([{"results": []}], "pagination"),
    ],
)
def test_refusals_keep_attempt_incomplete_and_close_transport(tmp_path, payloads, match):
    records, _, transport = _records(tmp_path, payloads)
    with pytest.raises(ValueError, match=match):
        list(records)
    assert _run_state(tmp_path)["status"] == "incomplete"
    assert transport.closed


def test_page_bound_refuses_partial_population(tmp_path):
    records, _, transport = _records(tmp_path, [_page([{"committee_id": "C1"}], 1)], max_pages=1)
    destination = write_fec_committee_rows([_RAW_COMMITTEE], tmp_path / "previous.parquet")
    prior = destination.read_bytes()
    with pytest.raises(ValueError, match="page bound"):
        write_fec_committee_rows(records, destination, batch_size=1)
    assert destination.read_bytes() == prior
    assert _run_state(tmp_path)["status"] == "incomplete"
    assert transport.closed


def test_consumer_closing_iterator_marks_attempt_incomplete(tmp_path):
    records, _, transport = _records(tmp_path, [_page([{"committee_id": "C1"}], 1)])
    next(records)
    records.close()
    assert _run_state(tmp_path)["status"] == "incomplete"
    assert transport.closed


def test_default_merge_fresh_whole_row_wins_and_prior_only_survives(tmp_path, monkeypatch):
    module = importlib.import_module("spicy_regs.transforms.build_fec_committees")
    write_fec_committee_rows(
        [
            _RAW_COMMITTEE,
            {**_RAW_COMMITTEE, "committee_id": "C99999999", "name": "PRIOR ONLY"},
        ],
        tmp_path / "_fec_prior.parquet",
    )
    fresh = {**_RAW_COMMITTEE, "name": "FRESH", "state": None}
    monkeypatch.setattr(module, "iter_fec_committee_records", lambda **kwargs: (row for row in [fresh]))
    result = module.build_fec_committees(tmp_path)
    rows = pq.read_table(result).to_pylist()
    assert rows == [_shape(fresh), _shape({**_RAW_COMMITTEE, "committee_id": "C99999999", "name": "PRIOR ONLY"})]
    assert not (tmp_path / "_fec_prior.parquet").exists()
    assert not (tmp_path / "_fec_new.parquet").exists()


def test_builder_closes_walker_when_shaping_fails_and_preserves_output(tmp_path, monkeypatch):
    module = importlib.import_module("spicy_regs.transforms.build_fec_committees")
    target = write_fec_committee_rows([_RAW_COMMITTEE], tmp_path / module.OUTPUT)
    previous = target.read_bytes()
    closed = []

    def invalid_rows(**kwargs):
        try:
            yield {**_RAW_COMMITTEE, "name": {"not": "a scalar"}}
        finally:
            closed.append(True)

    monkeypatch.setattr(module.r2, "download", lambda *args: False)
    monkeypatch.setattr(module, "iter_fec_committee_records", invalid_rows)
    with pytest.raises((TypeError, ValueError)):
        module.build_fec_committees(tmp_path)
    assert closed == [True]
    assert target.read_bytes() == previous


@pytest.mark.parametrize("option", [{"max_pages": 0}, {"max_pages": True}, {"per_page": 0}])
def test_walker_rejects_invalid_bounds(tmp_path, option):
    with pytest.raises(ValueError, match="positive integer"):
        iter_fec_committee_records(capture_dir=tmp_path, **option)


# -- OpenFEC quota ------------------------------------------------------------


def test_default_pace_stays_under_the_openfec_quota(tmp_path):
    pages = [
        httpx.Response(200, headers=_quota(), json=_page([{"committee_id": f"C{n}"}], n, pages=6, count=6, exact=True))
        for n in range(1, 7)
    ]
    records, starts = _in_order(tmp_path, pages)
    assert len(list(records)) == 6
    gaps = [round(b - a, 3) for a, b in pairwise(starts)]
    # 1.1 s apart is at most 55 request starts in any 60 s, under the stated 60.
    assert gaps == [1.1] * 5
    assert _run_state(tmp_path)["status"] == "complete"


def test_transient_429_with_retry_after_is_waited_out_and_walk_completes(tmp_path, capsys):
    responses = [
        httpx.Response(200, headers=_quota(), json=_page([{"committee_id": "C1"}], 1, pages=2, count=2, exact=True)),
        httpx.Response(429, headers=_quota(**{"Retry-After": "30", "X-RateLimit-Remaining": "0"})),
        httpx.Response(200, headers=_quota(), json=_page([{"committee_id": "C2"}], 2, pages=2, count=2, exact=True)),
    ]
    records, starts = _in_order(tmp_path, responses)
    assert list(records) == [{"committee_id": "C1"}, {"committee_id": "C2"}]
    assert len(starts) == 3
    state = _run_state(tmp_path)
    assert (state["status"], state["pages"], state["records"]) == ("complete", 2, 2)
    _assert_credential_free(tmp_path, capsys.readouterr().err)


def test_persistent_429_refuses_and_keeps_prior_output(tmp_path, capsys):
    first = httpx.Response(200, headers=_quota(), json=_page([{"committee_id": "C1"}], 1, pages=2))
    refusals = [httpx.Response(429, headers=_quota(**{"Retry-After": "61"})) for _ in range(20)]
    records, starts = _in_order(tmp_path, [first, *refusals])
    destination = write_fec_committee_rows([_RAW_COMMITTEE], tmp_path / "previous.parquet")
    prior = destination.read_bytes()
    with pytest.raises(ValueError, match="HTTP 429") as refused:
        write_fec_committee_rows(records, destination, batch_size=1)
    assert destination.read_bytes() == prior
    assert 2 < len(starts) < 21  # retried within a bound, never through every refusal
    state = _run_state(tmp_path)
    assert (state["status"], state["pages"]) == ("incomplete", 1)
    _assert_credential_free(tmp_path, str(refused.value), capsys.readouterr().err)


def test_traversal_past_its_deadline_refuses_as_incomplete(tmp_path, clock):
    def slow_page(request):
        clock.now += 700  # three slow pages pass the 25-minute deadline
        n = int(request.url.params["page"])
        return httpx.Response(200, headers=_quota(), json=_page([{"committee_id": f"C{n}"}], n, pages=9))

    records = iter_fec_committee_records(api_key=_KEY, capture_dir=tmp_path, transport=httpx.MockTransport(slow_page))
    destination = write_fec_committee_rows([_RAW_COMMITTEE], tmp_path / "previous.parquet")
    prior = destination.read_bytes()
    with pytest.raises(TimeoutError, match="25-minute deadline after 3 pages") as refused:
        write_fec_committee_rows(records, destination, batch_size=1)
    assert destination.read_bytes() == prior
    state = _run_state(tmp_path)
    assert (state["status"], state["pages"], state["records"]) == ("incomplete", 3, 3)
    _assert_credential_free(tmp_path, str(refused.value))
