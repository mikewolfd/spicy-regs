"""Source failures must stop before SAM tables or publication pointers change."""

import importlib
import json
from datetime import date
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.sources import sam_entities as sam

build = importlib.import_module("spicy_regs.transforms.build_sam_entities")


def entity(uei="SOURCE-UEI"):
    return {"entityRegistration": {"ueiSAM": uei, "legalBusinessName": "Literal source name"}}


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"garbage",
        b"{}",
        b'{"error":"refused"}',
        b"null",
        b"[null]",
        b'{"entityData":[{}]}',
        b'{"entityData":{}}',
        b"\xff",
        b"\x1f\x8bbroken",
        b"PKbroken",
        json.dumps(entity()).encode() + b"\nnot json\n",
        json.dumps({"entityData": [entity()], "totalRecords": 2}).encode(),
        b'{"entityData":[],"totalRecords":0,"error":"denied"}',
    ],
)
def test_extract_refuses_invalid_or_incomplete_population(payload):
    with pytest.raises(sam.SamEntitiesError):
        list(sam._parse_extract_bytes(payload))


@pytest.mark.parametrize("payload", [b"[]", b'{"entityData":[],"totalRecords":0}'])
def test_extract_accepts_explicit_empty_population(payload):
    assert list(sam._parse_extract_bytes(payload)) == []


def test_zip_does_not_silently_drop_additional_members():
    import io
    import zipfile

    raw = io.BytesIO()
    with zipfile.ZipFile(raw, "w") as archive:
        archive.writestr("first.json", json.dumps([entity("FIRST")]))
        archive.writestr("second.json", json.dumps([entity("SECOND")]))
    with pytest.raises(sam.SamEntitiesError, match="exactly one"):
        list(sam._parse_extract_bytes(raw.getvalue()))


@pytest.mark.parametrize("status", [401, 403, 404, 429, 500])
def test_http_failure_never_returns_empty_or_exposes_key(monkeypatch, status):
    monkeypatch.setattr(sam, "_MAX_RETRIES", 2)
    monkeypatch.setattr(sam.time, "sleep", lambda *_: None)
    calls = []
    secret = "test-credential-never-in-errors"
    reader = sam.SamEntitiesReader(api_key=secret)

    def serve(request):
        calls.append(request)
        return httpx.Response(status, json={"error": str(request.url)})

    with httpx.Client(transport=httpx.MockTransport(serve)) as reader._client:
        with pytest.raises(sam.SamEntitiesError) as refused:
            reader._get(f"{sam.API_BASE}/entities", {"api_key": secret})
    assert secret not in str(refused.value)
    assert len(calls) == (2 if status in {429, 500} else 1)


@pytest.mark.parametrize("operation", ["request", "extract"])
def test_transport_exhaustion_refuses_without_echoing_credential(monkeypatch, operation):
    import traceback

    monkeypatch.setattr(sam, "_MAX_RETRIES", 2)
    monkeypatch.setattr(sam, "_EXTRACT_POLL_MAX", 2)
    monkeypatch.setattr(sam.time, "sleep", lambda *_: None)
    reader = sam.SamEntitiesReader(api_key="test-secret")

    def serve(request):
        raise httpx.ConnectError(f"failed {request.url}", request=request)

    with httpx.Client(transport=httpx.MockTransport(serve)) as reader._client:
        with pytest.raises(sam.SamEntitiesError) as refused:
            if operation == "request":
                reader._get(f"{sam.API_BASE}/entities", {"api_key": reader.api_key})
            else:
                list(reader._download_extract("https://api.sam.gov/extract/download"))
    assert "test-secret" not in "".join(traceback.format_exception(refused.value))


@pytest.mark.parametrize(
    "payload",
    [{}, {"totalRecords": 0}, {"totalRecords": True, "entityData": []}, {"totalRecords": 2, "entityData": [entity()]}],
)
def test_extract_trigger_requires_complete_declared_population(monkeypatch, payload):
    reader = sam.SamEntitiesReader(api_key="key")
    monkeypatch.setattr(reader, "_get", lambda *args: payload)
    with pytest.raises(sam.SamEntitiesError):
        list(reader._extract_window(2020))


def test_download_count_mismatch_refuses_even_with_smaller_record_selection(monkeypatch):
    reader = sam.SamEntitiesReader(api_key="key", max_records=1)
    monkeypatch.setattr(
        reader,
        "_get",
        lambda *args: {
            "totalRecords": 2,
            "download": "https://api.sam.gov/extract/download",
        },
    )
    monkeypatch.setattr(reader, "_download_extract", lambda *_: iter([entity()]))
    with pytest.raises(sam.SamEntitiesError, match="record count"):
        list(reader._extract_window(2020))
    assert reader._seen == 0


def test_self_link_is_not_an_extract_download():
    assert (
        sam._find_download_url(
            {
                "totalRecords": 0,
                "entityData": [],
                "links": {
                    "selfLink": f"{sam.API_BASE}/entities?api_key=REPLACE_WITH_API_KEY",
                },
            }
        )
        is None
    )


@pytest.mark.parametrize(
    "link",
    ["https://elsewhere.invalid/extract", "http://api.sam.gov/extract", "https://name:password@api.sam.gov/extract"],
)
def test_source_link_cannot_receive_key_on_another_host(link):
    with pytest.raises(sam.SamEntitiesError, match="authorized API host"):
        sam.SamEntitiesReader(api_key="key")._reinject_key(link)


@pytest.mark.parametrize(
    "failure", ["missing-continuation", "empty-page", "changed-total", "duplicate-id", "page-budget"]
)
def test_pagination_refuses_incomplete_or_inconsistent_selection(monkeypatch, failure):
    reader = sam.SamEntitiesReader(api_key="key", per_page=1)
    url = f"{sam.API_BASE}/entities?page=1"
    first = {"totalRecords": 2, "entityData": [entity("FIRST")], "links": {"nextLink": url}}
    second = {"totalRecords": 2, "entityData": [entity("SECOND")], "links": {}}
    if failure == "missing-continuation":
        first["links"] = {}
    elif failure == "empty-page":
        second["entityData"] = []
    elif failure == "changed-total":
        second["totalRecords"] = 3
    elif failure == "duplicate-id":
        second["entityData"] = [entity("FIRST")]
    else:
        monkeypatch.setattr(sam, "_MAX_PAGES", 1)
    responses = iter([first, second])
    monkeypatch.setattr(reader, "_get", lambda *args: next(responses))
    with pytest.raises(sam.SamEntitiesError):
        list(reader._page_window(date(2020, 1, 1), date(2020, 12, 31)))


def test_short_page_with_continuation_is_not_terminal(monkeypatch):
    reader = sam.SamEntitiesReader(api_key="key", per_page=10)
    responses = iter(
        [
            {
                "totalRecords": 2,
                "entityData": [entity("FIRST")],
                "links": {"nextLink": f"{sam.API_BASE}/entities?page=1"},
            },
            {"totalRecords": 2, "entityData": [entity("SECOND")], "links": {}},
        ]
    )
    monkeypatch.setattr(reader, "_get", lambda *args: next(responses))
    assert len(list(reader._page_window(date(2020, 1, 1), date(2020, 12, 31)))) == 2


def test_explicit_record_limit_completes_its_selection_without_next_page(monkeypatch):
    reader = sam.SamEntitiesReader(api_key="key", per_page=1, max_records=1)
    monkeypatch.setattr(reader, "_get", lambda *args: {"totalRecords": 2, "entityData": [entity()], "links": {}})
    assert len(list(reader._page_window(date(2020, 1, 1), date(2020, 12, 31)))) == 1


@pytest.mark.parametrize(
    "scope", [{"SAM_SINCE_YEAR": "2020"}, {"SAM_MAX_RECORDS": "-1"}, {"SAM_UNTIL_YEAR": "not-a-year"}]
)
def test_rollup_scope_errors_do_not_start_build(monkeypatch, tmp_path, scope):
    from spicy_regs.pipelines.rollups import sam_entities as rollup

    for name in ("SAM_INGEST_MODE", "SAM_SINCE_YEAR", "SAM_UNTIL_YEAR", "SAM_MAX_RECORDS"):
        monkeypatch.delenv(name, raising=False)
    for name, value in scope.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(rollup, "build_sam_entities", lambda *args, **kwargs: pytest.fail("must not acquire"))
    with pytest.raises(ValueError):
        rollup.SamEntitiesRollup().build(tmp_path)


@pytest.mark.parametrize("failure", ["missing-key", "failed-after-row", "unrecognized-row"])
def test_rollup_failure_preserves_prior_bytes_and_never_calls_publication(monkeypatch, tmp_path, failure):
    from spicy_regs.pipelines.rollups.sam_entities import SamEntitiesRollup
    from spicy_regs.sources import publication

    for name in sam.API_KEY_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    prior = tmp_path / "sam_entities.parquet"
    pq.write_table(pa.Table.from_pylist([build._shape(entity("PRIOR"))], schema=build._SCHEMA), prior)
    before = prior.read_bytes()
    monkeypatch.setattr(build.r2, "download", lambda *args: False)
    monkeypatch.setattr(publication, "publish_generation", lambda *args, **kwargs: pytest.fail("must not publish"))
    if failure != "missing-key":

        def read(_self):
            yield entity()
            if failure == "failed-after-row":
                raise sam.SamEntitiesError("source selection failed after the first row")
            yield {"error": "unrecognized"}

        monkeypatch.setattr(sam.SamEntitiesReader, "iter_records", read)
    with pytest.raises(sam.SamEntitiesError):
        SamEntitiesRollup(output_dir=tmp_path, skip_upload=False).run()
    assert prior.read_bytes() == before
    assert not (tmp_path / "generations").exists()
    assert not (tmp_path / "_sam_new.parquet").exists()


def test_successful_explicit_empty_selection_remains_valid(monkeypatch, tmp_path):
    monkeypatch.setattr(build.r2, "download", lambda *args: False)
    reader = sam.SamEntitiesReader(api_key="key")
    monkeypatch.setattr(reader, "_get", lambda *args: {"totalRecords": 0, "entityData": []})
    monkeypatch.setattr(reader, "iter_records", lambda: reader._extract_window(2020))
    monkeypatch.setattr(build, "SamEntitiesReader", lambda **kwargs: reader)
    result = build.build_sam_entities(tmp_path, since_year=2020, until_year=2020)
    assert pq.ParquetFile(result).metadata.num_rows == 0


@pytest.mark.parametrize(
    "options",
    [
        {"since_year": 2024, "until_year": 2020},
        {"since_year": 0},
        {"max_records": 0},
        {"max_records": -1},
        {"per_page": 0},
    ],
)
def test_invalid_scope_refuses_before_acquisition(options):
    with pytest.raises(ValueError):
        sam.SamEntitiesReader(api_key="key", **options)


def test_sam_dispatch_forwards_its_explicit_selection_controls():
    import yaml

    workflow = yaml.load(Path(".github/workflows/rollup-sam-entities.yml").read_text(), Loader=yaml.BaseLoader)
    names = {"sam_ingest_mode", "sam_since_year", "sam_until_year", "sam_max_records"}
    dispatch = workflow["on"]["workflow_dispatch"]["inputs"]
    forwarded = workflow["jobs"]["run"]["with"]
    assert names <= dispatch.keys()
    for name in names:
        assert f"inputs.{name}" in forwarded[name]
    assert dispatch["sam_ingest_mode"]["options"] == ["extract", "partition"]
