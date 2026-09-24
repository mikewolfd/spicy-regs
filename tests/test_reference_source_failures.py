"""Source refusals preserve prior CRS, GAO, FCC and USAspending output bytes."""

import importlib
import json
from datetime import date
from decimal import Decimal

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_docs.reading.paged_json import DEFAULT_POOL_PASSES, IncompleteWalkError, PagedJsonSourceError
from spicy_docs.sources.gao.rss import GaoFeedSourceError
from spicy_docs.transport.credentials import CredentialRefusedError
from spicy_docs.transport import retry
from spicy_regs.sources import crs_reports as crs, gao_reports as gao
from spicy_regs.transforms import build_fcc_ecfs as fcc

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


def fcc_page(kind, records, *, counted=0):
    """An ECFS page with the aggregation ECFS answers beside every response; ``counted`` records carry its field."""
    key, field = ("filing", "express_comment") if kind == "fcc-filings" else ("proceeding", "bureau_name")
    buckets = [{"key": 1, "doc_count": counted}] if counted else []
    return {
        key: records,
        "aggregations": {field: {"doc_count_error_upper_bound": 0, "sum_other_doc_count": 0, "buckets": buckets}},
    }


def usa_page(records, *, total=None, next_page=None):
    return {
        "results": records,
        "page_metadata": {
            "total": len(records) if total is None else total,
            "hasNext": next_page is not None,
            "next": next_page,
        },
    }


class FccFetch:
    """One FCC selection against the transform's adopted fetch; ``api_key`` stays mutable for the missing-key case.

    The fetch function is captured at construction so tests that monkeypatch
    ``build_fcc_ecfs._fetch_fcc`` still reach the real walk through this handle.
    """

    def __init__(self, *, endpoint: str, **kwargs):
        self.endpoint = endpoint
        self.kwargs = kwargs
        self._fetch = fcc._fetch_fcc

    @property
    def api_key(self) -> str | None:
        return self.kwargs.get("api_key")

    @api_key.setter
    def api_key(self, value: str | None) -> None:
        self.kwargs["api_key"] = value

    def iter_records(self):
        return self._fetch(self.endpoint, **self.kwargs)


def selected(kind, transport, **kwargs):
    if kind == "crs":
        return crs.CrsReportsReader(api_key=KEY, transport=transport, **kwargs)
    if kind == "gao":
        return gao.GaoReportsReader(transport=transport, **kwargs)
    if kind == "usa":
        return usa._iter_recipient_rows(transport=transport, **kwargs)
    endpoint = "filings" if kind == "fcc-filings" else "proceedings"
    return FccFetch(endpoint=endpoint, since=DAY, until=DAY, api_key=KEY, transport=transport, **kwargs)


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
        ("crs", crs_page([{"id": "R1", "title": "No update date"}])),
        ("usa", {}),
        ("usa", {"results": []}),
        ("usa", usa_page([{}])),
        ("usa", usa_page([RECIPIENT, RECIPIENT])),
        ("usa", usa_page([], total=1)),
        ("usa", {"results": [], "page_metadata": {"total": 0, "hasNext": True, "next": None}}),
        ("fcc-filings", fcc_page("fcc-filings", [FCC_FILING, FCC_FILING])),
        ("fcc-filings", {"filing": [FCC_FILING]}),
        ("fcc-filings", fcc_page("fcc-filings", [{**FCC_FILING, "id_submission": ""}])),
        ("fcc-filings", fcc_page("fcc-filings", [{**FCC_FILING, "express_comment": 1}], counted=2)),
        ("fcc-proceedings", {"proceeding": [FCC_PROCEEDING]}),
        ("gao", b"<html><body>Denied</body></html>"),
        ("gao", b"<rss>"),
        ("gao", b'<!DOCTYPE rss [<!ENTITY x "boom">]><rss version="2.0"><channel/></rss>'),
        ("gao", b'<rss version="2.0"><channel><item><title>No identity</title></item></channel></rss>'),
    ],
)
def test_malformed_or_incomplete_source_is_refused(kind, payload):
    # A walk short of its count is pooled over further passes before it refuses.
    with pytest.raises(REFUSALS):
        list(records(kind, Transport(*[payload] * DEFAULT_POOL_PASSES)))


@pytest.mark.parametrize(
    "kind,payload",
    [
        ("crs", crs_page([])),
        ("usa", usa_page([])),
        ("gao", EMPTY_FEED),
        ("fcc-filings", fcc_page("fcc-filings", [])),
        ("fcc-proceedings", fcc_page("fcc-proceedings", [])),
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


def test_fcc_subdivides_full_window_but_refuses_full_single_day(monkeypatch):
    monkeypatch.setattr(fcc, "MAX_RESULT_WINDOW", 2)
    transport = Transport(
        {"filing": [FCC_FILING, {**FCC_FILING, "id_submission": "f2"}]},
        fcc_page("fcc-filings", [FCC_FILING]),
        fcc_page("fcc-filings", [{**FCC_FILING, "id_submission": "f2"}]),
    )
    records = fcc._fetch_fcc("filings", since=DAY, until=date(2026, 9, 9), api_key=KEY, per_page=2, transport=transport)
    assert len(list(records)) == 2
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
    # A filing can legitimately appear in both selected proceedings. The fetch
    # preserves both observations; table merging owns identity deduplication.
    shared = {**FCC_FILING, "proceedings": [{"name": "17-108"}, {"name": "23-320"}]}
    second = {**FCC_FILING, "id_submission": "f2"}
    transport = Transport(fcc_page("fcc-filings", [shared]), fcc_page("fcc-filings", [shared, second]))
    reader = selected("fcc-filings", transport, proceedings=("17-108", "23-320"))
    assert list(reader.iter_records()) == [shared, shared, second]
    assert [r.url.params["proceedings.name"] for r in transport.calls] == ["17-108", "23-320"]


def test_fcc_second_proceeding_failure_preserves_prior_output(monkeypatch, tmp_path):
    module = importlib.import_module("spicy_regs.transforms.build_fcc_ecfs")
    table = pa.Table.from_pylist([module._shape_filing(FCC_FILING)], schema=module._FILING_SCHEMA)
    prior = tmp_path / "_fcc_filings_prior.parquet"
    output = tmp_path / "fcc_filings.parquet"
    pq.write_table(table, prior)
    pq.write_table(table, output)
    before = prior.read_bytes(), output.read_bytes()
    transport = Transport(fcc_page("fcc-filings", [FCC_FILING]), 500)
    reader = selected("fcc-filings", transport, proceedings=("17-108", "23-320"))
    monkeypatch.setattr(module, "_fetch_fcc", lambda *args, **kwargs: reader.iter_records())

    with pytest.raises(REFUSALS):
        module.build_fcc_filings(tmp_path)

    assert (prior.read_bytes(), output.read_bytes()) == before
    assert [r.url.params["proceedings.name"] for r in transport.calls] == ["17-108", "23-320"]


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
        "_fetch_fcc",
        "_fcc_filings_prior.parquet",
        "fcc_filings.parquet",
        "_FILING_SCHEMA",
        "_shape_filing",
        FCC_FILING,
    ),
    "fcc-proceedings": (
        "build_fcc_ecfs",
        "build_fcc_proceedings",
        "_fetch_fcc",
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
    if kind.startswith("fcc"):
        monkeypatch.setattr(module, reader_name, lambda *args, **kwargs: source.iter_records())
    else:
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
        ("fcc-filings", fcc_page("fcc-filings", [])),
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
    if kind.startswith("fcc"):
        monkeypatch.setattr(module, reader_name, lambda *args, **kwargs: source.iter_records())
    else:
        monkeypatch.setattr(module, reader_name, lambda **_: source)
    monkeypatch.setattr(module.r2, "download", lambda *_: False)
    out = getattr(module, builder_name)(tmp_path)
    assert out.name == output_name
    result = pq.read_table(out)
    assert result.to_pylist() == expected
    assert result.schema == getattr(module, schema_name)


def test_fcc_proceedings_refuse_an_empty_whole_walk_and_keep_the_output(monkeypatch, tmp_path):
    output = tmp_path / fcc.PROCEEDINGS_OUTPUT
    output.write_bytes(b"previous generation")
    source = selected("fcc-proceedings", Transport(fcc_page("fcc-proceedings", [])))
    monkeypatch.setattr(fcc, "_fetch_fcc", lambda *args, **kwargs: source.iter_records())
    with pytest.raises(fcc.FccEcfsError, match="empty whole walk"):
        fcc.build_fcc_proceedings(tmp_path)
    assert output.read_bytes() == b"previous generation"


def test_crs_pools_a_shifted_walk_until_its_declared_count():
    """A pass that repeats one report and skips another still serves the declared rows; pooling catches it.

    CRS ignores ``sort``, so the second walk moves its page boundaries by page size alone.
    """
    second = {**CRS, "id": "R2"}
    transport = Transport(crs_page([CRS, CRS], total=2), crs_page([second, CRS], total=2))
    assert sorted(x["id"] for x in selected("crs", transport).iter_records()) == ["R1", "R2"]
    assert [call.url.params["limit"] for call in transport.calls] == ["250", "237"]


def test_fcc_pools_a_window_until_its_aggregate_count():
    """ECFS states no total; the aggregation it answers beside the rows counts the window."""
    second = {**FCC_FILING, "id_submission": "f2", "express_comment": 0}
    first = {**FCC_FILING, "express_comment": 1}
    transport = Transport(
        fcc_page("fcc-filings", [first, first], counted=2), fcc_page("fcc-filings", [second, first], counted=2)
    )
    assert sorted(x["id_submission"] for x in selected("fcc-filings", transport).iter_records()) == ["f1", "f2"]
    assert len(transport.calls) == 2


def _proceeding(name, **fields):
    """An ECFS proceeding document carrying the bureau its window's count is aggregated over."""
    return {
        **FCC_PROCEEDING,
        "name": name,
        "bureau": {"code": "WCB", "name": "Wireline"},
        "total_filing_count": 1,
        **fields,
    }


def test_fcc_proceedings_pool_by_document_not_by_filing_activity():
    """A proceeding's filing counters move between walks while its document does not; the pool keys the document."""
    held, other, skipped = _proceeding("17-108"), _proceeding("23-320"), _proceeding("24-1")
    busier = {**held, "total_filing_count": 2, "last_30_days": "2026-09-09T00:00:00Z"}
    transport = Transport(
        fcc_page("fcc-proceedings", [held, held, other], counted=3),
        fcc_page("fcc-proceedings", [skipped, skipped, busier], counted=3),
    )
    rows = list(records("fcc-proceedings", transport))
    assert sorted(row["name"] for row in rows) == ["17-108", "23-320", "24-1"]
    assert next(row for row in rows if row["name"] == "17-108")["total_filing_count"] == 2, "the latest observation"
    assert len(transport.calls) == 2


def test_fcc_proceedings_edited_between_walks_refuse_rather_than_settle_on_a_surplus():
    """Any other change reads as another document: the pool overfills its count and only a clean walk could settle it."""
    held, other, skipped = _proceeding("17-108"), _proceeding("23-320"), _proceeding("24-1")
    closed = {**held, "date_closed": "2026-09-09T00:00:00Z"}
    dirty = fcc_page("fcc-proceedings", [held, held, other], counted=3)
    transport = Transport(
        dirty, fcc_page("fcc-proceedings", [skipped, skipped, closed], counted=3), *[dirty] * (DEFAULT_POOL_PASSES - 2)
    )
    with pytest.raises(IncompleteWalkError, match="pooled 4 records, more than the 3 declared"):
        list(records("fcc-proceedings", transport))


def test_fcc_proceedings_publish_one_row_per_docket_name(monkeypatch, tmp_path):
    """ECFS holds more than one document for some dockets; the original (or last edited) is published."""
    original = {
        "name": "13-84",
        "id_proceeding": 1012202662,
        "date_proceeding_created": "2013-03-27T15:50:47.000-04:00",
        "description": "RF exposure",
        "total_filing_count": 994,
    }
    recreated = {
        "name": "13-84",
        "id_proceeding": 1789995656032,
        "date_proceeding_created": "2026-09-21T13:00:56.032Z",
        "filingStatus": "OPENALL",
    }
    edited = {
        "name": "24-89",
        "id_proceeding": 1710787523643,
        "date_proceeding_created": "2024-03-18T16:24:02.000Z",
        "date_closed": "2024-12-09T05:00:00.000Z",
        "date_last_modified": "2024-12-09T19:33:20.191Z",
    }
    unedited = {
        "name": "24-89",
        "id_proceeding": 1710787523643,
        "date_proceeding_created": "2024-03-18T16:24:02.000Z",
        "filingStatus": "OPENALL",
    }
    nameless = {"id_proceeding": "0621042517382", "date_proceeding_created": "2017-06-21T21:12:23.075Z"}
    documents = [recreated, unedited, nameless, original, edited]
    monkeypatch.setattr(fcc, "_fetch_fcc", lambda *args, **kwargs: iter(documents))
    rows = pq.read_table(fcc.build_fcc_proceedings(tmp_path)).to_pylist()
    assert [(row["name"], row["id_proceeding"], row["date_closed"]) for row in rows] == [
        ("24-89", "1710787523643", "2024-12-09T05:00:00.000Z"),
        ("13-84", "1012202662", None),
    ]
    assert not list(tmp_path.glob(".*partial"))
