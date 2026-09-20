"""Hermetic tests for the print-citations transform's own seam.

No network: the discovery reader, the body acquirer and the roster acquirer are
all stubbed, and the bodies are synthesized PDFs from ``tests/pdf_fixtures.py``
read by the same ``extraction.body_text`` the transform uses.

**What is established here and what deliberately is not.** Whether
``find_citations`` reads ``H.R. 1093`` correctly, whether a print's *ordered
reported* attaches to the right bill, and what a column means are all settled
in spicy-docs beside the code that decides them; re-asserting any of it here
would be a second copy of the same claim that can only drift. What this
repository owns, and what these tests hold, is the acquisition seam: which
packages are enumerated and which are dropped, which are fetched and which are
skipped, that a refusal is a counted row and a credential refusal is not, that
the per-run cap bounds the fetch and not the walk, and that the four tables
come out under their contracts' schemas with ``document_kind`` telling the two
families apart inside the shared link table.
"""

from __future__ import annotations

import pyarrow.parquet as pq
import pytest
from pathlib import Path

from spicy_docs.reading.paged_json import PagedJsonSourceError
from spicy_docs.sources.govinfo.activity_reports import is_activity_report
from spicy_docs.schemas import TABLE_CONTRACTS
from spicy_docs.sources.govinfo.bodies import (
    BODY_PREFERENCE,
    PackageBodyIdentity,
    PackageSummary,
    parse_package_id,
    validate_package_mods,
)
from spicy_docs.sources.govinfo.body_acquisition import GovInfoBodyBudget, GovInfoPackageBody
from spicy_docs.transport.captured import CapturedBodyResponse
from spicy_docs.transport.credentials import CredentialRefusedError

from spicy_regs.transforms.build_print_citations import (
    PRINT_BODY_PREFERENCE,
    ACTIVITY_REPORTS,
    BILL_ACTIONS,
    BUDGET_VOLUMES,
    CITATIONS,
    build_print_citations,
)
from tests.pdf_fixtures import make_multiline_pdf

MODS_DIR = Path(__file__).parent / "fixtures" / "govinfo_bodies"
BUDGET_DIR = Path(__file__).parent / "fixtures" / "budget_volumes"
OBSERVED_AT = "2026-09-20T00:00:00Z"

#: A real committee-report MODS stands in for an activity report's: the shape
#: the transform reads is the same, and which CRPT packages are activity
#: reports is the title rule's question, tested separately below.
CRPT_ID = "CRPT-119hrpt1"
BUDGET_ID = "BUDGET-2026-MSR"

#: Two pages whose text names keys of several kinds, so citation rows, a bill
#: action row and the page attribution are all reached. What each rule reads
#: out of it is spicy-docs' business; that rows arrive at all is this test's.
REPORT_PAGES = [
    ["ACTIVITY REPORT", "The Committee ordered H.R. 1093 reported on March 4, 2024.", "1"],
    ["Public Law No: 118-11 was enacted.", "See 5 U.S.C. 552 and RIN 1904-AF21.", "2"],
]

BUDGET_PAGES = [
    ["MID-SESSION REVIEW", "Enacted as Public Law 118-5, this account continues.", "1"],
    ["See 31 U.S.C. 1105 for the requirement.", "Funded by H.R. 7806 as reported.", "2"],
]

BUDGET = GovInfoBodyBudget(
    max_requests=8,
    max_body_bytes=1 << 20,
    max_metadata_bytes=1 << 18,
    timeout_seconds=10.0,
    min_request_interval_seconds=0.0,
)


def _mods_bytes(package_id: str) -> bytes:
    if package_id.startswith("BUDGET-"):
        return (BUDGET_DIR / f"mods-{package_id}.xml").read_bytes()
    return (MODS_DIR / f"mods-{package_id}.xml").read_bytes()


def _package(package_id: str, *, pages: list[list[str]], last_modified: str = "2026-09-18T12:00:00Z", pdf_pages=None):
    """A fetched package body, built the way the acquirer would have built one."""
    identity = parse_package_id(package_id)
    body = make_multiline_pdf(pdf_pages if pdf_pages is not None else pages)
    url = f"https://www.govinfo.gov/content/pkg/{package_id}/pdf/{package_id}.pdf"
    capture = CapturedBodyResponse(
        requested_url=url,
        resolved_url=url,
        status_code=200,
        content_type="application/pdf",
        observed_at=OBSERVED_AT,
        body=body,
    )
    mods_url = f"https://api.govinfo.gov/packages/{package_id}/mods"
    mods_capture = CapturedBodyResponse(
        requested_url=mods_url,
        resolved_url=mods_url,
        status_code=200,
        content_type="application/xml",
        observed_at=OBSERVED_AT,
        body=_mods_bytes(package_id),
    )
    mods = validate_package_mods(
        mods_capture.body, package=identity, final_url=mods_url, max_bytes=BUDGET.max_metadata_bytes
    )
    return GovInfoPackageBody(
        identity=identity,
        format="pdf",
        preference=("xml", "htm", "txt", "pdf"),
        offered_formats=("pdf",),
        summary=PackageSummary(
            identity=identity,
            collection_code=identity.collection,
            date_issued="2026-01-05",
            last_modified=last_modified,
            title=f"Report {package_id}",
            download_links=(),
            pages=str(len(pages)),
        ),
        mods=mods,
        body=PackageBodyIdentity(
            identity=identity,
            format="pdf",
            media_type="application/pdf",
            final_url=url,
            byte_size=len(body),
        ),
        summary_capture=capture,
        mods_capture=mods_capture,
        body_capture=capture,
        request_count=3,
        budget=BUDGET,
    )


class _Page:
    def __init__(self, records):
        self.records = tuple(records)
        self.declared_count = len(self.records)
        self.next_url = None


class _Reader:
    """A discovery reader that answers one page per collection and counts the walks."""

    def __init__(self, rows: dict[str, list[dict]]):
        self.rows = rows
        self.walked: list[str] = []

    def packages(self, url, *, max_pages=1):
        collection = url.rsplit("collection=", 1)[-1]
        self.walked.append(collection)
        return iter([_Page(self.rows.get(collection, []))])


class _Acquirer:
    """A body acquirer over prepared packages; anything else refuses, by name."""

    def __init__(self, packages: dict[str, object], refuse: dict[str, Exception] | None = None):
        self.packages = packages
        self.refuse = refuse or {}
        self.asked: list[str] = []
        self.preferences: list[tuple[str, ...]] = []

    def acquire(self, package_id, *, prefer=(), max_bytes=None):
        self.asked.append(package_id)
        self.preferences.append(tuple(prefer))
        if package_id in self.refuse:
            raise self.refuse[package_id]
        return self.packages[package_id]


class _NoRosters:
    """Both chamber files refuse, which is a degraded vocabulary and not a failure."""

    def acquire_house(self, *, congress, session=None, max_bytes=None):
        raise ConnectionError("no network in a hermetic test")

    def acquire_senate(self, *, max_bytes=None):
        raise ConnectionError("no network in a hermetic test")


def _no_download(remote_key: str, local_path: Path) -> bool:
    return False


def _listing(package_id: str, title: str, last_modified: str = "2026-09-18T12:00:00Z") -> dict:
    return {"packageId": package_id, "title": title, "lastModified": last_modified}


def _build(tmp_path, reader, acquirer, **kwargs):
    return build_print_citations(
        tmp_path, reader=reader, acquirer=acquirer, rosters=_NoRosters(), download_prior=_no_download, **kwargs
    )


def _rows(path: Path) -> list[dict]:
    return pq.read_table(path).to_pylist()


# --------------------------------------------------------------------------
# The title rule, which is the one selection rule copied rather than imported.
# --------------------------------------------------------------------------

#: Titles the live ``published`` CRPT window 2025-01-01..2025-03-31 served on
#: 2026-09-20, abbreviated. All five are activity reports.
REAL_ACTIVITY_TITLES = (
    "LEGISLATIVE REVIEW AND OVERSIGHT ACTIVITIES of the COMMITTEE ON FOREIGN AFFAIRS",
    "Summary of the Activities of the Committee on Transportation and Infrastructure",
    "ACTIVITIES OF THE COMMITTEE ON OVERSIGHT AND ACCOUNTABILITY ONE HUNDRED EIGHTEENTH CONGRESS",
    "REPORT ON THE ACTIVITIES OF THE COMMITTEE ON EDUCATION AND THE WORKFORCE",
    "ACTIVITY REPORT of the COMMITTEE ON ENERGY AND COMMERCE of the HOUSE OF REPRESENTATIVES",
)

#: The class a bare ``activit`` match takes in, which is why the rule matches a
#: phrase. spicy-docs measured two of these among its first eight matches.
MEASURED_FALSE_POSITIVES = (
    "DIRECTING THE SECRETARY OF STATE TO TRANSMIT DOCUMENTS RELATING TO ACTIVITIES IN A FOREIGN STATE",
    "PROVIDING FOR CONSIDERATION OF A BILL RELATING TO CERTAIN ACTIVITIES",
)


@pytest.mark.parametrize("title", REAL_ACTIVITY_TITLES)
def test_the_title_rule_matches_every_real_activity_report(title):
    assert is_activity_report(CRPT_ID, title)


@pytest.mark.parametrize("title", MEASURED_FALSE_POSITIVES)
def test_the_title_rule_refuses_the_measured_false_positive_class(title):
    """A bare ``activit`` matches these; the phrase rule must not.

    This is the half that makes the rule worth copying rather than widening:
    without it nothing here would notice the rule being loosened back to a
    word match.
    """
    assert "activit" in title.lower(), "the false positive must contain the word, or this proves nothing"
    assert not is_activity_report(CRPT_ID, title)


# --------------------------------------------------------------------------
# The pass itself.
# --------------------------------------------------------------------------


def test_both_families_publish_under_their_contracts_and_share_one_link_table(tmp_path):
    reader = _Reader(
        {
            "CRPT": [_listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE ON ENERGY AND COMMERCE")],
            "BUDGET": [_listing(BUDGET_ID, "Mid-Session Review")],
        }
    )
    acquirer = _Acquirer(
        {
            CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES),
            BUDGET_ID: _package(BUDGET_ID, pages=BUDGET_PAGES),
        }
    )
    activity, budget, actions, citations = _build(tmp_path, reader, acquirer)

    for path, name in (
        (activity, ACTIVITY_REPORTS),
        (budget, BUDGET_VOLUMES),
        (actions, BILL_ACTIONS),
        (citations, CITATIONS),
    ):
        assert path.name == f"{name}.parquet"
        assert pq.read_table(path).schema.names == list(TABLE_CONTRACTS[name].columns)

    assert [row["package_id"] for row in _rows(activity)] == [CRPT_ID]
    assert [row["package_id"] for row in _rows(budget)] == [BUDGET_ID]

    # One link table, two families, told apart by document_kind — and every
    # kind is one of spicy-docs' own two constants, not a spelling invented here.
    citation_rows = _rows(citations)
    assert citation_rows, "the prints state keys; the link table must carry them"
    by_document = {row["document_key"]: row["document_kind"] for row in citation_rows}
    assert by_document == {CRPT_ID: "govinfo_package", BUDGET_ID: "budget_volume"}


def test_a_budget_volumes_bill_citation_states_no_index_comparison(tmp_path):
    """NULL, never ``false``: a BUDGET package has no Congress to compare on.

    The rule is spicy-docs'; what is checked here is that this transform passes
    ``budget_index_stated_keys`` for a budget volume and ``index_stated_keys``
    for a report, which is the only way the distinction reaches a published row.
    """
    reader = _Reader({"CRPT": [], "BUDGET": [_listing(BUDGET_ID, "Mid-Session Review")]})
    acquirer = _Acquirer({BUDGET_ID: _package(BUDGET_ID, pages=BUDGET_PAGES)})
    _, _, _, citations = _build(tmp_path, reader, acquirer)

    bill_rows = [row for row in _rows(citations) if row["cite_kind"] == "bill_number"]
    # Without this the assertion below is satisfied by an empty list, which is
    # what a check that agrees with itself looks like.
    assert bill_rows, "the fixture's print names a bill; the link table must carry it"
    assert all(row["stated_by_index"] is None for row in bill_rows), (
        "a bill cite of a budget volume must say no comparison was possible"
    )


def test_bill_actions_come_only_from_the_activity_reports(tmp_path):
    """A budget volume is not a committee print and states no committee action."""
    reader = _Reader(
        {
            "CRPT": [_listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE ON ENERGY AND COMMERCE")],
            "BUDGET": [_listing(BUDGET_ID, "Mid-Session Review")],
        }
    )
    acquirer = _Acquirer(
        {CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES), BUDGET_ID: _package(BUDGET_ID, pages=BUDGET_PAGES)}
    )
    _, _, actions, _ = _build(tmp_path, reader, acquirer)
    action_rows = _rows(actions)
    # Both prints state an action phrase beside a bill; only the report's may
    # become a row, and an empty table would satisfy the subset assertion
    # without establishing that.
    assert action_rows, "the report states an action beside a bill"
    assert {row["document_key"] for row in action_rows} == {CRPT_ID}


def test_a_neighbouring_collections_id_is_dropped_by_name_and_never_fetched(tmp_path):
    """A CRPT walk serves ``ERP-``/``GPO-`` rows; the grammar refuses them before a request."""
    reader = _Reader(
        {
            "CRPT": [
                _listing("GPO-J6-REPORT", "ACTIVITY REPORT of the SELECT COMMITTEE"),
                _listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE ON ENERGY AND COMMERCE"),
            ],
            "BUDGET": [],
        }
    )
    acquirer = _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES)})
    _build(tmp_path, reader, acquirer)
    assert acquirer.asked == [CRPT_ID], "an id the grammar refuses must not cost a request"


def test_a_title_that_is_not_an_activity_report_is_never_fetched(tmp_path):
    reader = _Reader(
        {
            "CRPT": [
                _listing("CRPT-119hrpt2", "PROVIDING FOR CONSIDERATION OF A BILL RELATING TO CERTAIN ACTIVITIES"),
                _listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE ON ENERGY AND COMMERCE"),
            ],
            "BUDGET": [],
        }
    )
    acquirer = _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES)})
    _build(tmp_path, reader, acquirer)
    assert acquirer.asked == [CRPT_ID]


def test_an_unchanged_package_is_skipped_and_a_modified_one_is_re_read(tmp_path):
    """The publisher's own ``last_modified`` is the comparison, not the date."""
    listing = _listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE ON ENERGY AND COMMERCE", "2026-09-18T12:00:00Z")
    reader = _Reader({"CRPT": [listing], "BUDGET": []})
    acquirer = _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES)})
    activity, *_ = _build(tmp_path, reader, acquirer)
    assert acquirer.asked == [CRPT_ID]

    # The published table becomes the prior for the next run.
    activity.rename(tmp_path / f"_{ACTIVITY_REPORTS}_prior.parquet")
    acquirer.asked.clear()
    _build(tmp_path, _Reader({"CRPT": [listing], "BUDGET": []}), acquirer)
    assert acquirer.asked == [], "a package published at this last_modified costs nothing"

    moved = dict(listing, lastModified="2026-09-19T12:00:00Z")
    (tmp_path / f"{ACTIVITY_REPORTS}.parquet").rename(tmp_path / f"_{ACTIVITY_REPORTS}_prior.parquet")
    acquirer.asked.clear()
    acquirer.packages[CRPT_ID] = _package(CRPT_ID, pages=REPORT_PAGES, last_modified="2026-09-19T12:00:00Z")
    _build(tmp_path, _Reader({"CRPT": [moved], "BUDGET": []}), acquirer)
    assert acquirer.asked == [CRPT_ID], "a revised package must be re-read"


def test_a_refused_package_is_counted_and_enumerated_again_next_run(tmp_path):
    """AGENTS.md's resume rule, held by the enumeration rather than by a state table.

    The walk is the whole window every run, so a package that refused is
    offered again — which a last-modified watermark could not do once another
    package published past it.
    """
    rows = {
        "CRPT": [
            _listing("CRPT-119hrpt9", "ACTIVITY REPORT of the COMMITTEE ON RULES"),
            _listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE ON ENERGY AND COMMERCE"),
        ],
        "BUDGET": [],
    }
    acquirer = _Acquirer(
        {CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES)},
        refuse={"CRPT-119hrpt9": PagedJsonSourceError("the publisher served its error page")},
    )
    activity, *_ = _build(tmp_path, _Reader(rows), acquirer)
    assert acquirer.asked == ["CRPT-119hrpt9", CRPT_ID]
    assert [row["package_id"] for row in _rows(activity)] == [CRPT_ID], "a refusal publishes no row"

    activity.rename(tmp_path / f"_{ACTIVITY_REPORTS}_prior.parquet")
    acquirer.asked.clear()
    _build(tmp_path, _Reader(rows), acquirer)
    assert acquirer.asked == ["CRPT-119hrpt9"], "the refusal is retried and the published package is not"


def test_a_credential_refusal_aborts_the_run(tmp_path):
    """A 401/403 is not one bad row: every later package would refuse the same way."""
    acquirer = _Acquirer({}, refuse={CRPT_ID: CredentialRefusedError("the key was refused")})
    reader = _Reader({"CRPT": [_listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE")], "BUDGET": []})
    with pytest.raises(CredentialRefusedError):
        _build(tmp_path, reader, acquirer)


def test_the_per_run_cap_bounds_the_fetch_and_not_the_walk(tmp_path):
    """Both collections are still enumerated; only the fetching stops."""
    reader = _Reader(
        {
            "CRPT": [
                _listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE ON ENERGY AND COMMERCE"),
                _listing("CRPT-119hrpt9", "ACTIVITY REPORT of the COMMITTEE ON RULES"),
            ],
            "BUDGET": [_listing(BUDGET_ID, "Mid-Session Review")],
        }
    )
    acquirer = _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES)})
    _build(tmp_path, reader, acquirer, max_packages=1)
    assert acquirer.asked == [CRPT_ID]
    assert reader.walked == ["CRPT", "BUDGET"], "the window is walked whole so the rest is retried next run"


def test_every_body_is_asked_for_pdf_first(tmp_path):
    """The one rendition this transform names, and the reason it names one.

    Under the acquirer's sealed order HTML comes first, and a measured run
    (2026-09-20, retained as the receipt's first attempt) refused 10 of 41
    activity reports on HTML nesting depth and published no page attribution
    at all on the 31 it read. Four published columns state a page, so the
    order is this transform's to choose and the choice must not drift back.
    """
    reader = _Reader(
        {
            "CRPT": [_listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE ON ENERGY AND COMMERCE")],
            "BUDGET": [_listing(BUDGET_ID, "Mid-Session Review")],
        }
    )
    acquirer = _Acquirer(
        {CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES), BUDGET_ID: _package(BUDGET_ID, pages=BUDGET_PAGES)}
    )
    _build(tmp_path, reader, acquirer)
    assert acquirer.preferences == [PRINT_BODY_PREFERENCE, PRINT_BODY_PREFERENCE]
    # Equality with the derivation, not a spot check: a subset assertion would
    # pass on a transcription that had silently dropped `uslm`, and an
    # `[0] == "pdf"` assertion says nothing about the order behind it. This
    # fails the moment the constant stops being "PDF, then the sealed order",
    # including when spicy-docs adds a rendition to BODY_PREFERENCE.
    assert PRINT_BODY_PREFERENCE == ("pdf", *(fmt for fmt in BODY_PREFERENCE if fmt != "pdf"))
    assert set(PRINT_BODY_PREFERENCE) == set(BODY_PREFERENCE), "no rendition is dropped or invented"


def test_a_pdf_body_fills_the_page_columns_an_html_body_cannot(tmp_path):
    """The consequence the preference exists for, asserted on a published row."""
    reader = _Reader({"CRPT": [_listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE")], "BUDGET": []})
    acquirer = _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES)})
    activity, _, _, citations = _build(tmp_path, reader, acquirer)

    row = _rows(activity)[0]
    assert row["body_rendition"] == "pdf"
    assert row["pages_read"] == str(len(REPORT_PAGES))
    assert row["pages_capped"] == "false"
    assert all(r["evidence_page"] is not None for r in _rows(citations)), "a paginated body attributes every cite"


def test_a_cold_run_over_both_collections_publishes_rows_in_both(tmp_path):
    """Neither family may be starved by a shared cap, whatever the listing order.

    Walking CRPT to exhaustion first spends the whole cap on it whenever CRPT
    has more outstanding packages than the cap, and publishes an **empty**
    ``budget_volumes`` — which is what the first measured run did (40 activity
    reports, 0 budget volumes, 2026-09-20). A consumer cannot tell an empty
    contract table from a family with nothing in it, so a cold deploy must not
    produce one. The cap here is smaller than the CRPT listing on purpose.
    """
    reader = _Reader(
        {
            "CRPT": [
                _listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE ON ENERGY AND COMMERCE"),
                _listing("CRPT-119hrpt9", "ACTIVITY REPORT of the COMMITTEE ON RULES"),
                _listing("CRPT-119hrpt11", "ACTIVITY REPORT of the COMMITTEE ON THE BUDGET"),
            ],
            "BUDGET": [_listing(BUDGET_ID, "Mid-Session Review")],
        }
    )
    acquirer = _Acquirer(
        {
            CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES),
            "CRPT-119hrpt9": _package(CRPT_ID, pages=REPORT_PAGES),
            "CRPT-119hrpt11": _package(CRPT_ID, pages=REPORT_PAGES),
            BUDGET_ID: _package(BUDGET_ID, pages=BUDGET_PAGES),
        }
    )
    activity, budget, _, _ = _build(tmp_path, reader, acquirer, max_packages=2)

    assert _rows(activity), "the activity-report table must not be empty"
    assert _rows(budget), "nor may the budget table, which the listing order would starve"
    # The cap is still the cap: two packages, one from each family.
    assert len(acquirer.asked) == 2
