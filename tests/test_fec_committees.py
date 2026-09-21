"""Hermetic tests for the OpenFEC committees ingest (no network).

Exercises the real SpicyDocs transport and pagination with supplied responses,
raw capture evidence, refusal/cleanup paths, and the existing table merge.
"""

from __future__ import annotations

import json
import importlib

import httpx

import pyarrow.parquet as pq
import pytest

from spicy_regs.sources.fec_committees import (
    API_KEY_ENV_VARS,
    FecCommitteesReader,
    _resolve_api_key,
)
from spicy_regs.transforms.build_fec_committees import COLUMNS, _shape
from spicy_regs.transforms.build_fec_committees import write_fec_committee_rows

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
    monkeypatch.setattr(module.FecCommitteesReader, "iter_records", lambda self: (row for row in [_RAW_COMMITTEE]))
    real_write = module.write_fec_committee_rows
    calls = []

    def write(records, destination):
        calls.append(destination.name)
        return real_write(records, destination, batch_size=1)

    monkeypatch.setattr(module, "write_fec_committee_rows", write)
    monkeypatch.setenv("FEC_API_KEY", "test-key")
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


def test_reader_refuses_missing_key(monkeypatch, tmp_path):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    reader = FecCommitteesReader(capture_dir=tmp_path)
    with pytest.raises(ValueError, match="require an API key"):
        list(reader.iter_records())
    assert list(tmp_path.iterdir()) == []


def _page(records, number, *, pages=9, count=0, exact=False):
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


def _reader(tmp_path, payloads, **kwargs):
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
    reader = FecCommitteesReader(
        api_key="private-api-test-credential",
        capture_dir=tmp_path,
        transport=transport,
        min_interval=0,
        **kwargs,
    )
    return reader, requests, transport


def _run_state(reader):
    return json.loads((reader.last_run_path / "run.json").read_text())


def test_provider_walks_past_short_pages_and_retains_credential_free_evidence(tmp_path):
    payloads = [
        _page([{"committee_id": "C1"}], 1),
        _page([{"committee_id": "C2"}, {"committee_id": "C3"}], 2),
        _page([], 3),
    ]
    reader, requests, transport = _reader(tmp_path, payloads, per_page=2)
    assert list(reader.iter_records()) == [{"committee_id": c} for c in ("C1", "C2", "C3")]
    assert len(requests) == 3
    for request in requests:
        assert request.url.path == "/v1/committees/"
        assert set(request.url.params) == {"sort", "per_page", "page"}
        assert request.url.params["sort"] == "committee_id"
        assert request.headers["X-Api-Key"] == "private-api-test-credential"
    state = _run_state(reader)
    assert state["status"] == "complete"
    assert state["records"] == 3
    assert transport.closed
    index = [json.loads(line) for line in (reader.last_run_path / "pages.jsonl").read_text().splitlines()]
    import hashlib

    for page, expected in zip(index, payloads, strict=True):
        evidence = page["evidence"]
        raw = (reader.last_run_path / "blobs" / evidence["blob_path"]).read_bytes()
        assert json.loads(raw) == expected
        assert "sha256:" + hashlib.sha256(raw).hexdigest() == evidence["sha256"]
        assert len(raw) == evidence["bytes"]
    for path in reader.last_run_path.rglob("*"):
        if path.is_file():
            assert b"private-api-test-credential" not in path.read_bytes()


def test_provider_exact_count_terminates_without_unnecessary_empty_request(tmp_path):
    reader, requests, transport = _reader(
        tmp_path,
        [
            _page([{"committee_id": "C1"}], 1, pages=1, count=1, exact=True),
        ],
    )
    assert len(list(reader.iter_records())) == 1
    assert len(requests) == 1
    assert _run_state(reader)["declared_exact_count"] == 1
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
    reader, _, transport = _reader(tmp_path, payloads)
    with pytest.raises(ValueError, match=match):
        list(reader.iter_records())
    assert _run_state(reader)["status"] == "incomplete"
    assert transport.closed


def test_page_bound_refuses_partial_population(tmp_path):
    reader, _, transport = _reader(tmp_path, [_page([{"committee_id": "C1"}], 1)], max_pages=1)
    destination = write_fec_committee_rows([_RAW_COMMITTEE], tmp_path / "previous.parquet")
    prior = destination.read_bytes()
    with pytest.raises(ValueError, match="page bound"):
        write_fec_committee_rows(reader.iter_records(), destination, batch_size=1)
    assert destination.read_bytes() == prior
    assert _run_state(reader)["status"] == "incomplete"
    assert transport.closed


def test_consumer_closing_iterator_marks_attempt_incomplete(tmp_path):
    reader, _, transport = _reader(tmp_path, [_page([{"committee_id": "C1"}], 1)])
    records = reader.iter_records()
    next(records)
    records.close()
    assert _run_state(reader)["status"] == "incomplete"
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
    monkeypatch.setattr(module.FecCommitteesReader, "iter_records", lambda self: (row for row in [fresh]))
    result = module.build_fec_committees(tmp_path)
    rows = pq.read_table(result).to_pylist()
    assert rows == [_shape(fresh), _shape({**_RAW_COMMITTEE, "committee_id": "C99999999", "name": "PRIOR ONLY"})]
    assert not (tmp_path / "_fec_prior.parquet").exists()
    assert not (tmp_path / "_fec_new.parquet").exists()


def test_builder_closes_reader_when_shaping_fails_and_preserves_output(tmp_path, monkeypatch):
    module = importlib.import_module("spicy_regs.transforms.build_fec_committees")
    target = write_fec_committee_rows([_RAW_COMMITTEE], tmp_path / module.OUTPUT)
    previous = target.read_bytes()
    closed = []

    def invalid_rows(self):
        try:
            yield {**_RAW_COMMITTEE, "name": {"not": "a scalar"}}
        finally:
            closed.append(True)

    monkeypatch.setattr(module.r2, "download", lambda *args: False)
    monkeypatch.setattr(module.FecCommitteesReader, "iter_records", invalid_rows)
    with pytest.raises((TypeError, ValueError)):
        module.build_fec_committees(tmp_path)
    assert closed == [True]
    assert target.read_bytes() == previous


@pytest.mark.parametrize("option", [{"max_pages": 0}, {"max_pages": True}, {"per_page": 0}])
def test_reader_rejects_invalid_bounds(tmp_path, option):
    with pytest.raises(ValueError, match="positive integer"):
        FecCommitteesReader(capture_dir=tmp_path, **option)
