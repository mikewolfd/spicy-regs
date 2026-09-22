"""Source refusals preserve prior CRS, GAO, FCC and USAspending output bytes."""

import importlib
import json
from datetime import date
from decimal import Decimal

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_docs.reading.paged_json import PagedJsonSourceError
from spicy_docs.sources.gao.rss import GaoFeedSourceError
from spicy_docs.transport.credentials import CredentialRefusedError
from spicy_docs.transport import retry
from spicy_regs.sources import crs_reports as crs, fcc_ecfs as fcc, gao_reports as gao

usa = importlib.import_module("spicy_regs.transforms.build_usaspending_recipients")

KEY = "fixture-key-never-in-url"
DAY = date(2026, 9, 8)
CRS = {"id": "R1", "updateDate": "2026-09-08T00:00:00Z", "title": "One"}
FCC_FILING = {"id_submission": "f1", "date_received": "2026-09-08T23:56:03Z"}
FCC_PROCEEDING = {"name": "17-108", "id_proceeding": 1, "date_proceeding_created": "2026-09-08T10:00:00Z"}
RECIPIENT = {"id": "r1-R", "name": "ONE", "amount": 1.25}
FEED = b'<rss version="2.0"><channel><title>Reports</title><item><title>One</title><link>https://www.gao.gov/products/gao-26-107974</link><description>Native text</description></item></channel></rss>'
EMPTY_FEED = b'<rss version="2.0"><channel><title>Reports</title></channel></rss>'
REFUSALS = (ValueError, ConnectionError, httpx.HTTPError, CredentialRefusedError)


class Transport(httpx.MockTransport):
    def __init__(self, *responses):
        self.responses = iter(responses)
        self.calls = []
        super().__init__(self.handle)

    def handle(self, request):
        self.calls.append(request)
        value = next(self.responses)
        if isinstance(value, Exception):
            raise value
        status = value if isinstance(value, int) else 200
        raw = value if isinstance(value, bytes) else json.dumps(value).encode()
        content_type = "application/rss+xml" if request.url.path.endswith(".xml") else "application/json"
        return httpx.Response(status, stream=httpx.ByteStream(raw), headers={"content-type": content_type})


@pytest.fixture(autouse=True)
def bounded_requests(monkeypatch):
    monkeypatch.setattr(retry.random, "uniform", lambda *_: 0)
    for module in (crs, fcc, usa):
        monkeypatch.setattr(module, "_MAX_REQUESTS_PER_PAGE", 1)
    monkeypatch.setattr(gao, "_MAX_REQUESTS", 1)


def crs_page(records, *, total=None, next_url=None):
    return {"CRSReports": records, "pagination": {"count": len(records) if total is None else total, "next": next_url}}


def usa_page(records, *, total=None, next_page=None):
    return {
        "results": records,
        "page_metadata": {
            "total": len(records) if total is None else total,
            "hasNext": next_page is not None,
            "next": next_page,
        },
    }


def selected(kind, transport, **kwargs):
    if kind == "crs":
        return crs.CrsReportsReader(api_key=KEY, transport=transport, **kwargs)
    if kind == "gao":
        return gao.GaoReportsReader(transport=transport, **kwargs)
    if kind == "usa":
        return usa._iter_recipient_rows(transport=transport, **kwargs)
    cls = fcc.FccEcfsFilingsReader if kind == "fcc-filings" else fcc.FccEcfsProceedingsReader
    return cls(since=DAY, until=DAY, api_key=KEY, transport=transport, **kwargs)


def records(kind, transport, **kwargs):
    source = selected(kind, transport, **kwargs)
    return iter(source) if kind == "usa" else source.iter_records()


@pytest.mark.parametrize("kind", ["crs", "gao", "usa", "fcc-filings", "fcc-proceedings"])
@pytest.mark.parametrize("status", [401, 403, 404, 429, 500])
def test_http_refusal_is_not_empty_success(kind, status):
    transport = Transport(status)
    with pytest.raises(REFUSALS) as error:
        list(records(kind, transport))
    assert KEY not in str(error.value)
    assert len(transport.calls) == 1


@pytest.mark.parametrize("kind", ["crs", "gao", "usa", "fcc-filings", "fcc-proceedings"])
def test_transport_failure_is_not_empty_success(kind):
    with pytest.raises(REFUSALS):
        list(records(kind, Transport(httpx.ReadTimeout("fixture interruption"))))


@pytest.mark.parametrize(
    "kind,payload",
    [
        ("crs", {}),
        ("crs", {"CRSReports": []}),
        ("crs", crs_page([{}])),
        ("crs", crs_page([CRS, CRS])),
        ("crs", crs_page([], total=1)),
        ("usa", {}),
        ("usa", {"results": []}),
        ("usa", usa_page([{}])),
        ("usa", usa_page([RECIPIENT, RECIPIENT])),
        ("usa", usa_page([], total=1)),
        ("usa", {"results": [], "page_metadata": {"total": 0, "hasNext": True, "next": None}}),
        ("fcc-filings", {}),
        ("fcc-filings", {"filing": [None]}),
        ("fcc-filings", {"filing": [{}]}),
        ("fcc-proceedings", {"proceeding": [{}]}),
        ("fcc-filings", {"filing": [FCC_FILING, FCC_FILING]}),
        ("gao", b"<html><body>Denied</body></html>"),
        ("gao", b"<rss>"),
        ("gao", b'<!DOCTYPE rss [<!ENTITY x "boom">]><rss version="2.0"><channel/></rss>'),
        ("gao", b'<rss version="2.0"><channel><item><title>No identity</title></item></channel></rss>'),
    ],
)
def test_malformed_or_incomplete_source_is_refused(kind, payload):
    with pytest.raises(REFUSALS):
        list(records(kind, Transport(payload)))


@pytest.mark.parametrize(
    "kind,payload",
    [
        ("crs", crs_page([])),
        ("usa", usa_page([])),
        ("gao", EMPTY_FEED),
        ("fcc-filings", {"filing": []}),
        ("fcc-proceedings", {"proceeding": []}),
    ],
)
def test_confirmed_empty_selection_is_valid(kind, payload):
    assert list(records(kind, Transport(payload))) == []


def test_crs_follows_continuation_and_never_stops_at_an_old_row():
    next_url = f"{crs.API_BASE}/crsreport?offset=2&limit=2&format=json"
    older = {**CRS, "id": "R-old", "updateDate": "2026-01-01T00:00:00Z"}
    newer = {**CRS, "id": "R-new"}
    transport = Transport(crs_page([older, CRS], total=3, next_url=next_url), crs_page([newer], total=3))
    rows = list(selected("crs", transport, since=DAY, per_page=2).iter_records())
    assert [x["id"] for x in rows] == ["R1", "R-new"]
    assert transport.calls[0].url.params["fromDateTime"] == "2026-09-08T00:00:00Z"
    assert str(transport.calls[1].url) == next_url
    assert all(x.headers["x-api-key"] == KEY and "api_key" not in x.url.params for x in transport.calls)


def test_crs_page_bound_refuses_partial_walk(monkeypatch):
    monkeypatch.setattr(crs, "_MAX_PAGES", 1)
    next_url = f"{crs.API_BASE}/crsreport?offset=1&limit=1"
    with pytest.raises(PagedJsonSourceError, match="page bound"):
        list(selected("crs", Transport(crs_page([CRS], total=2, next_url=next_url)), per_page=1).iter_records())


def test_fcc_provider_window_includes_end_day_and_credentials_stay_in_header():
    transport = Transport({"filing": [FCC_FILING]})
    assert list(selected("fcc-filings", transport).iter_records()) == [FCC_FILING]
    request = transport.calls[0]
    assert request.url.params["date_received"] == "[gte]2026-09-08[lte]2026-09-09"
    assert request.headers["x-api-key"] == KEY and KEY not in str(request.url)


def test_fcc_subdivides_full_window_but_refuses_full_single_day(monkeypatch):
    monkeypatch.setattr(fcc, "MAX_RESULT_WINDOW", 2)
    transport = Transport(
        {"filing": [FCC_FILING, {**FCC_FILING, "id_submission": "f2"}]},
        {"filing": [FCC_FILING]},
        {"filing": [{**FCC_FILING, "id_submission": "f2"}]},
    )
    reader = fcc.FccEcfsFilingsReader(since=DAY, until=date(2026, 9, 9), api_key=KEY, per_page=2, transport=transport)
    assert len(list(reader.iter_records())) == 2
    assert len(transport.calls) == 3
    with pytest.raises(fcc.FccEcfsError, match="result ceiling"):
        list(
            selected(
                "fcc-filings", Transport({"filing": [FCC_FILING, {**FCC_FILING, "id_submission": "f2"}]}), per_page=2
            ).iter_records()
        )


def test_usa_short_nonterminal_page_continues_and_preserves_amount():
    transport = Transport(usa_page([RECIPIENT], total=2, next_page=2), usa_page([{**RECIPIENT, "id": "r2-R"}], total=2))
    rows = list(records("usa", transport, per_page=2))
    assert [x["id"] for x in rows] == ["r1-R", "r2-R"]
    assert rows[0]["amount"] == Decimal("1.25")
    assert json.loads(transport.calls[1].content)["page"] == 2


def test_usa_top_page_bound_is_an_explicit_selection():
    transport = Transport(usa_page([RECIPIENT], total=20, next_page=2))
    assert len(list(records("usa", transport, per_page=1, max_pages=1))) == 1
    assert len(transport.calls) == 1


@pytest.mark.parametrize(
    "second_page",
    [
        usa_page([{**RECIPIENT, "id": "r2-R"}], total=3),  # count changed
        usa_page([RECIPIENT], total=2),  # repeated identity across pages
        usa_page([], total=2),  # terminal count is short
        usa_page([], total=2, next_page=3),  # empty nonterminal page
        usa_page([{**RECIPIENT, "id": "r2-R"}], total=2, next_page=4),  # skipped page
    ],
)
def test_usa_refuses_cross_page_inconsistency(second_page):
    transport = Transport(usa_page([RECIPIENT], total=2, next_page=2), second_page)
    rows = records("usa", transport, per_page=1)
    assert next(rows)["id"] == RECIPIENT["id"]
    with pytest.raises(REFUSALS):
        list(rows)
    assert len(transport.calls) == 2


def test_fcc_named_proceedings_keep_each_selection_and_shared_filing():
    # A filing can legitimately appear in both selected proceedings. The reader
    # preserves both observations; table merging owns identity deduplication.
    shared = {**FCC_FILING, "proceedings": [{"name": "17-108"}, {"name": "23-320"}]}
    second = {**FCC_FILING, "id_submission": "f2"}
    transport = Transport({"filing": [shared]}, {"filing": [shared, second]})
    reader = selected("fcc-filings", transport, proceedings=("17-108", "23-320"))
    assert list(reader.iter_records()) == [shared, shared, second]
    assert [r.url.params["proceedings.name"] for r in transport.calls] == ["17-108", "23-320"]
    assert reader._current_proceeding is None
    assert reader._reader is None


def test_fcc_second_proceeding_failure_preserves_prior_output(monkeypatch, tmp_path):
    module = importlib.import_module("spicy_regs.transforms.build_fcc_ecfs")
    table = pa.Table.from_pylist([module._shape_filing(FCC_FILING)], schema=module._FILING_SCHEMA)
    prior = tmp_path / "_fcc_filings_prior.parquet"
    output = tmp_path / "fcc_filings.parquet"
    pq.write_table(table, prior)
    pq.write_table(table, output)
    before = prior.read_bytes(), output.read_bytes()
    transport = Transport({"filing": [FCC_FILING]}, 500)
    reader = selected("fcc-filings", transport, proceedings=("17-108", "23-320"))
    monkeypatch.setattr(module, "FccEcfsFilingsReader", lambda **kwargs: reader)

    with pytest.raises(REFUSALS):
        module.build_fcc_filings(tmp_path)

    assert (prior.read_bytes(), output.read_bytes()) == before
    assert [r.url.params["proceedings.name"] for r in transport.calls] == ["17-108", "23-320"]
    assert reader._current_proceeding is None
    assert reader._reader is None


def test_gao_validates_whole_feed_before_applying_selected_item_bound():
    assert list(selected("gao", Transport(FEED), max_records=1).iter_records()) == [
        {
            "title": "One",
            "link": "https://www.gao.gov/products/gao-26-107974",
            "description": "Native text",
            "pub_date": None,
        }
    ]
    bad = FEED.replace(b"</channel>", b"<item><title>Missing identity</title></item></channel>")
    with pytest.raises(GaoFeedSourceError):
        list(selected("gao", Transport(bad), max_records=1).iter_records())


CASES = {
    "crs": (
        "build_crs_reports",
        "build_crs_reports",
        "CrsReportsReader",
        "_crs_prior.parquet",
        "crs_reports.parquet",
        "_SCHEMA",
        "_shape",
        CRS,
    ),
    "gao": (
        "build_gao_reports",
        "build_gao_reports",
        "GaoReportsReader",
        "_gao_prior.parquet",
        "gao_reports.parquet",
        "_SCHEMA",
        "_shape",
        {"link": "https://www.gao.gov/products/gao-26-107974", "title": "Prior"},
    ),
    "usa": (
        "build_usaspending_recipients",
        "build_usaspending_recipients",
        "_iter_recipient_rows",
        "_usaspending_prior.parquet",
        "usaspending_recipients.parquet",
        "_SCHEMA",
        "_shape",
        RECIPIENT,
    ),
    "fcc-filings": (
        "build_fcc_ecfs",
        "build_fcc_filings",
        "FccEcfsFilingsReader",
        "_fcc_filings_prior.parquet",
        "fcc_filings.parquet",
        "_FILING_SCHEMA",
        "_shape_filing",
        FCC_FILING,
    ),
    "fcc-proceedings": (
        "build_fcc_ecfs",
        "build_fcc_proceedings",
        "FccEcfsProceedingsReader",
        "_fcc_proceedings_prior.parquet",
        "fcc_proceedings.parquet",
        "_PROCEEDING_SCHEMA",
        "_shape_proceeding",
        FCC_PROCEEDING,
    ),
}


@pytest.mark.parametrize("kind", CASES)
@pytest.mark.parametrize("failure", ["http", "malformed", "partial", "missing-key"])
def test_failed_build_preserves_prior_and_existing_output_bytes(monkeypatch, tmp_path, kind, failure):
    if failure == "missing-key" and kind not in ("crs", "fcc-filings", "fcc-proceedings"):
        pytest.skip("source is keyless")
    module_name, builder_name, reader_name, prior_name, output_name, schema_name, shape_name, native = CASES[kind]
    module = importlib.import_module("spicy_regs.transforms." + module_name)
    table = pa.Table.from_pylist([getattr(module, shape_name)(native)], schema=getattr(module, schema_name))
    prior, output = tmp_path / prior_name, tmp_path / output_name
    pq.write_table(table, prior)
    pq.write_table(table, output)
    before = (prior.read_bytes(), output.read_bytes())
    if failure == "partial":
        if kind == "crs":
            source = selected(
                kind,
                Transport(crs_page([CRS], total=2, next_url=f"{crs.API_BASE}/crsreport?offset=1&limit=1"), 500),
                per_page=1,
            )
        elif kind == "usa":
            source = selected(kind, Transport(usa_page([RECIPIENT], total=2, next_page=2), 500), per_page=1)
        elif kind.startswith("fcc"):
            source = selected(
                kind, Transport({("filing" if kind == "fcc-filings" else "proceeding"): [native]}, 500), per_page=1
            )
        else:
            source = selected(
                kind, Transport(FEED.replace(b"</channel>", b"<item><title>No identity</title></item></channel>"))
            )
    elif failure == "missing-key":
        source = selected(kind, Transport())
        source.api_key = ""
    else:
        source = selected(kind, Transport(500 if failure == "http" else b"not a source response"))
    monkeypatch.setattr(module, reader_name, lambda **_: source)
    with pytest.raises(REFUSALS):
        getattr(module, builder_name)(tmp_path)
    assert (prior.read_bytes(), output.read_bytes()) == before
    assert not list(tmp_path.glob("*_new.parquet"))


@pytest.mark.parametrize(
    "kind,payload",
    [
        ("crs", crs_page([])),
        ("usa", usa_page([])),
        ("gao", EMPTY_FEED),
        ("fcc-filings", {"filing": []}),
        ("fcc-proceedings", {"proceeding": []}),
    ],
)
@pytest.mark.parametrize("have_prior", [False, True])
def test_empty_success_keeps_prior_rows_or_builds_zero(monkeypatch, tmp_path, kind, payload, have_prior):
    module_name, builder_name, reader_name, prior_name, output_name, schema_name, shape_name, native = CASES[kind]
    module = importlib.import_module("spicy_regs.transforms." + module_name)
    expected = [getattr(module, shape_name)(native)] if have_prior else []
    if have_prior:
        pq.write_table(pa.Table.from_pylist(expected, schema=getattr(module, schema_name)), tmp_path / prior_name)
    source = selected(kind, Transport(payload))
    monkeypatch.setattr(module, reader_name, lambda **_: source)
    monkeypatch.setattr(module.r2, "download", lambda *_: False)
    out = getattr(module, builder_name)(tmp_path)
    assert out.name == output_name
    result = pq.read_table(out)
    assert result.to_pylist() == expected
    assert result.schema == getattr(module, schema_name)
