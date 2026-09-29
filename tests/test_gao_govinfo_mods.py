"""GovInfo MODS for GAO history rows: parsing real records, keyless paced reads, and the resumable fill.

The fixtures are the exact bytes GovInfo served on 2026-09-28 at
``https://www.govinfo.gov/metadata/pkg/<packageId>/mods.xml``: a report, a
testimony, and a product whose GovInfo title is a placeholder.
"""

from __future__ import annotations

import json
from importlib import import_module
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.sources.gao_govinfo import GaoGovInfoError
from spicy_regs.sources.gao_govinfo_mods import (
    MODS_URL,
    GaoModsAcquirer,
    GaoModsUnavailableError,
    ModsFacts,
    mods_facts,
)

FIXTURES = Path(__file__).parent / "fixtures" / "gao_govinfo_mods"
REPORT, TESTIMONY, PLACEHOLDER = "GAOREPORTS-GAO-08-919R", "GAOREPORTS-T-RCED-94-121", "GAOREPORTS-GAO-HEHS-00-73"
module = import_module("spicy_regs.transforms.build_gao_reports")


def _mods(package_id: str) -> bytes:
    return (FIXTURES / f"{package_id}.xml").read_bytes()


def test_a_report_states_its_number_type_abstract_and_topics():
    facts = mods_facts(_mods(REPORT), REPORT)
    assert (facts.report_number, facts.product_type) == ("GAO-08-919R", "Correspondence")
    abstract = facts.abstract or ""
    assert abstract.startswith("The terrorist attacks of September 11, 2001, on the World Trade Center")
    assert "  " not in abstract and "\n" not in abstract and "\t" not in abstract
    assert len(facts.topics) == 22
    # The publisher's own split of one program name stays as stated.
    assert facts.topics[:2] == ("Brokerage industry", "Deductibles and Coinsurance")
    assert facts.topics[-2:] == ("Treasury Terrorism Risk Insurance", "Program")


def test_a_testimony_is_typed_testimony_by_its_record():
    facts = mods_facts(_mods(TESTIMONY), TESTIMONY)
    assert (facts.report_number, facts.product_type) == ("T-RCED-94-121", "Testimony")
    assert (facts.abstract or "").startswith("Centralizing service for the Farmers Home Administration's (FmHA) single-family")
    assert len(facts.topics) == 12


def test_a_placeholder_titled_product_keeps_its_slashed_number_and_states_no_abstract_or_topics():
    assert mods_facts(_mods(PLACEHOLDER), PLACEHOLDER) == ModsFacts(
        report_number="GAO/HEHS-00-73", product_type="Other Written Product", abstract=None, topics=())


def test_a_record_naming_another_package_or_unreadable_bytes_refuse():
    with pytest.raises(GaoGovInfoError, match="identifies"):
        mods_facts(_mods(REPORT), TESTIMONY)
    with pytest.raises(GaoGovInfoError, match="unreadable"):
        mods_facts(b"<html>Page Not Found</html>", REPORT)


def test_the_acquirer_reads_keyless_retries_a_429_and_names_a_missing_record(monkeypatch):
    monkeypatch.setattr("spicy_docs.transport.retry.time.sleep", lambda _seconds: None)
    calls: list[httpx.Request] = []

    def serve(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        status, media, body = (
            (404, "text/html", b"Page Not Found") if request.url.path.endswith(f"{PLACEHOLDER}/mods.xml")
            else (429, "text/html", b"slow down") if len(calls) == 1
            else (200, "application/xml", _mods(REPORT))
        )
        return httpx.Response(status, headers={"content-type": media}, stream=httpx.ByteStream(body))

    with GaoModsAcquirer(transport=httpx.MockTransport(serve)) as acquirer:
        facts, capture = acquirer.capture(REPORT)
        assert facts.report_number == "GAO-08-919R" and capture.body == _mods(REPORT)
        assert [str(request.url) for request in calls] == [MODS_URL.format(REPORT)] * 2
        assert all("x-api-key" not in request.headers for request in calls)
        with pytest.raises(GaoModsUnavailableError):
            acquirer.capture(PLACEHOLDER)


def _history_row(package_id: str, report_id: str, **fields) -> dict:
    return {**dict.fromkeys(module.COLUMNS), "report_id": report_id, "title": f"Title {report_id}",
            "report_type": "Report", "published_date": "2000-03-31", "source": "govinfo",
            "url": f"https://www.govinfo.gov/app/details/{package_id}", **fields}


class FixtureMods:
    """Serves the fixtures; a package without one is unavailable, as a 404 would be."""

    def __init__(self):
        self.asked: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def capture(self, package_id):
        self.asked.append(package_id)
        if not (FIXTURES / f"{package_id}.xml").exists():
            raise GaoModsUnavailableError(package_id)
        return mods_facts(_mods(package_id), package_id), None


def _build(tmp_path, monkeypatch, prior_rows, mods):
    pq.write_table(pa.Table.from_pylist(prior_rows, schema=module._SCHEMA), tmp_path / "_gao_prior.parquet")

    class NoFeed:
        def __init__(self, **_):
            pass

        def iter_records(self):
            return iter(())

    monkeypatch.setattr(module, "GaoReportsReader", NoFeed)
    out, _ = module.build_gao_reports(tmp_path, govinfo_mods=True, mods=mods)
    rows = {row["report_id"]: row for row in pq.read_table(out).to_pylist()}
    out.rename(tmp_path / "_gao_prior.parquet")
    return rows


def test_unread_history_rows_fill_from_mods_in_batches_and_null_stays_null(tmp_path, monkeypatch):
    feed = {**dict.fromkeys(module.COLUMNS), "report_id": "gao-26-1", "title": "Feed", "report_type": "Report",
            "abstract": "What GAO Found.", "agencies_json": "[]", "topics_json": "[]", "source": "gao_rss",
            "url": "https://www.gao.gov/products/gao-26-1"}
    already = _history_row("GAOREPORTS-GGD-98-188", "ggd-98-188", report_number="GGD-98-188", abstract="Kept.")
    prior = [feed, already, _history_row(REPORT, "gao-08-919r"), _history_row(PLACEHOLDER, "hehs-00-73"),
             _history_row(TESTIMONY, "t-rced-94-121", report_type="Testimony"),
             _history_row("GAOREPORTS-AIMD-94-1", "aimd-94-1")]
    monkeypatch.setattr(module, "MODS_PER_RUN", 3)
    mods = FixtureMods()

    first = _build(tmp_path, monkeypatch, prior, mods)
    # report_id order; the row read before is not asked again, and a missing record stays unread.
    assert mods.asked == ["GAOREPORTS-AIMD-94-1", REPORT, PLACEHOLDER]
    assert first["aimd-94-1"] == _history_row("GAOREPORTS-AIMD-94-1", "aimd-94-1")
    assert first["gao-08-919r"]["product_type"] == "Correspondence"
    assert json.loads(first["gao-08-919r"]["topics_json"])[0] == "Brokerage industry"
    placeholder = first["hehs-00-73"]
    assert (placeholder["report_number"], placeholder["abstract"], placeholder["topics_json"]) == (
        "GAO/HEHS-00-73", None, None)
    assert placeholder["title"] == "Title hehs-00-73" and placeholder["agencies_json"] is None
    assert first["t-rced-94-121"]["report_number"] is None
    assert first["gao-26-1"] == feed and first["ggd-98-188"] == already

    second = _build(tmp_path, monkeypatch, list(first.values()), mods)
    assert mods.asked[3:] == ["GAOREPORTS-AIMD-94-1", TESTIMONY]
    assert second["t-rced-94-121"]["product_type"] == "Testimony"
    assert second["t-rced-94-121"]["report_type"] == "Testimony"
    assert second["gao-08-919r"] == first["gao-08-919r"]


def test_the_published_nine_column_prior_gains_the_mods_columns(tmp_path, monkeypatch):
    """The live table predates product_type and report_number; its first MODS run reads it without a migration."""
    nine = pa.schema([(c, pa.string()) for c in module.COLUMNS if c not in ("product_type", "report_number")])
    row = {c: v for c, v in _history_row(REPORT, "gao-08-919r").items() if c in nine.names}
    pq.write_table(pa.Table.from_pylist([row], schema=nine), tmp_path / "_gao_prior.parquet")

    class NoFeed:
        def __init__(self, **_):
            pass

        def iter_records(self):
            return iter(())

    monkeypatch.setattr(module, "GaoReportsReader", NoFeed)
    out, _ = module.build_gao_reports(tmp_path, govinfo_mods=True, mods=FixtureMods())
    (read,) = pq.read_table(out).to_pylist()
    assert pq.read_schema(out).names == list(module.COLUMNS)
    assert (read["report_number"], read["product_type"]) == ("GAO-08-919R", "Correspondence")
