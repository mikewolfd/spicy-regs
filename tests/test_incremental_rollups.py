"""Hermetic tests that each incremental rollup actually re-fetches less.

The claim these pin is "a steady-state run does not pay for what it already
published". A docstring saying so is not evidence; each case here seeds a prior
published table, runs the transform with a counting stub, and asserts on what
was requested.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.schemas import TABLE_CONTRACTS

from spicy_regs.transforms.table_merge import prior_scratch_path


def seed(output_dir: Path, table: str, rows: list[dict]) -> None:
    """Write a prior published table where the transform will look for it."""
    contract = TABLE_CONTRACTS[table]
    filled = [{c: None for c in contract.columns} | row for row in rows]
    pq.write_table(
        pa.Table.from_pylist(filled, schema=pa.schema([(c, pa.string()) for c in contract.columns])),
        prior_scratch_path(output_dir, table),
    )


def no_download(remote_key: str, local_path: Path) -> bool:
    return False


# --------------------------------------------------------------------------- #
# Amendments: a window, not a full walk.
# --------------------------------------------------------------------------- #
class StubListingReader:
    """Records the URLs asked for and serves nothing."""

    def __init__(self):
        self.urls: list[str] = []

    def records(self, route, url, *, max_pages=100):
        self.urls.append(url)
        return iter(())


def test_amendments_windows_from_the_prior_watermark(tmp_path, monkeypatch):
    from spicy_regs.transforms.build_amendments import build_amendments

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    monkeypatch.delenv("AMENDMENTS_SINCE", raising=False)
    monkeypatch.delenv("AMENDMENTS_UNTIL", raising=False)
    seed(
        tmp_path,
        "amendments",
        [
            {
                "congress": "119",
                "amendment_type": "samdt",
                "amendment_number": "1",
                "update_date": "2026-09-10T00:00:00Z",
            }
        ],
    )

    reader = StubListingReader()
    build_amendments(tmp_path, reader=reader, download_prior=no_download)

    assert len(reader.urls) == 1
    url = reader.urls[0]
    # Watermark 2026-09-10 minus the 3-day overlap.
    assert "fromDateTime=2026-09-07" in url, url
    assert "toDateTime=" in url


def test_amendments_walks_everything_with_no_prior(tmp_path, monkeypatch):
    from spicy_regs.transforms.build_amendments import build_amendments

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    monkeypatch.delenv("AMENDMENTS_SINCE", raising=False)
    monkeypatch.delenv("AMENDMENTS_UNTIL", raising=False)

    reader = StubListingReader()
    build_amendments(tmp_path, reader=reader, download_prior=no_download)
    assert "fromDateTime" not in reader.urls[0], "a cold start must not bound the walk"


def test_amendments_caps_the_window(tmp_path, monkeypatch):
    """A deep backfill converges over runs instead of timing out in one."""
    from spicy_regs.transforms.build_amendments import MAX_WINDOW_DAYS, build_amendments

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    reader = StubListingReader()
    build_amendments(
        tmp_path, reader=reader, since=date(2020, 1, 1), until=date(2026, 1, 1), download_prior=no_download
    )
    url = reader.urls[0]
    assert "fromDateTime=2020-01-01" in url
    capped = date(2020, 1, 1).toordinal() + MAX_WINDOW_DAYS
    assert f"toDateTime={date.fromordinal(capped).isoformat()}" in url, url


def _amendment(number: int, update: str = "2026-01-01T00:00:00Z") -> dict:
    return {
        "congress": 119,
        "type": "SAMDT",
        "number": str(number),
        "updateDate": update,
        "url": f"https://api.congress.gov/v3/amendment/119/samdt/{number}?format=json",
    }


class ShiftingListingReader:
    """Each pass returns the declared count of rows, but repeats one amendment and skips another."""

    def __init__(self, passes):
        self.passes = list(passes)
        self.urls: list[str] = []

    def records(self, route, url, *, max_pages=100):
        if route.name == "amendment-detail":
            number = url.split("?")[0].rsplit("/", 1)[1]
            detail = {
                "sponsors": [{"bioguideId": f"S{number}", "fullName": f"Sen. {number}", "party": "D"}],
                "amendedBill": {"congress": 119, "type": "HR", "number": "1"},
                "chamber": "Senate",
            }
            return iter((SimpleNamespace(records=(detail,) if number != "404" else (), declared_count=None),))
        self.urls.append(url)
        records, declared = self.passes.pop(0)
        return iter((SimpleNamespace(records=tuple(records), declared_count=declared),))


def test_amendments_pool_opposite_sort_passes_until_the_declared_count(tmp_path, monkeypatch):
    """A single pass that repeats #2 and skips #3 still matches the declared count by rows; pooling catches it."""
    from spicy_regs.transforms.build_amendments import build_amendments

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    reader = ShiftingListingReader(
        [
            ([_amendment(1), _amendment(2), _amendment(2)], 3),
            ([_amendment(3), _amendment(2, "2026-02-01T00:00:00Z"), _amendment(1)], 3),
        ]
    )
    out = build_amendments(tmp_path, reader=reader, download_prior=no_download)
    rows = {row["amendment_number"]: row for row in pq.read_table(out).to_pylist()}
    assert sorted(rows) == ["1", "2", "3"]
    assert rows["2"]["update_date"] == "2026-02-01T00:00:00Z", "the later observation wins"
    assert "sort=updateDate+desc" in reader.urls[0] and "sort=updateDate+asc" in reader.urls[1]


def test_amendments_refuse_a_walk_that_never_reaches_its_declared_count(tmp_path, monkeypatch):
    from spicy_regs.sources.pooled_walk import IncompleteWalkError
    from spicy_regs.transforms.build_amendments import POOLED_SORTS, build_amendments

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    reader = ShiftingListingReader([([_amendment(1), _amendment(1)], 2)] * len(POOLED_SORTS))
    with pytest.raises(IncompleteWalkError, match="pooled 1 of 2 declared"):
        build_amendments(tmp_path, reader=reader, download_prior=no_download)
    assert len(reader.urls) == len(POOLED_SORTS)


# --------------------------------------------------------------------------- #
# Roll-call votes: held roll calls are not re-read from the Clerk.
# --------------------------------------------------------------------------- #
HOUSE_VOTE_RECORDS = [
    {
        "congress": 119,
        "sessionNumber": 1,
        "rollCallNumber": number,
        "legislationType": "HR",
        "legislationNumber": "3424",
        "sourceDataURL": f"https://clerk.house.gov/evs/2025/roll{number}.xml",
        "startDate": "2025-09-08T18:56:00-04:00",
        "updateDate": "2025-09-09T18:53:19-04:00",
    }
    for number in range(1, 41)
]


class StubVoteListingReader:
    def records(self, route, url, *, max_pages=100):
        class Page:
            records = tuple(HOUSE_VOTE_RECORDS)

        return iter((Page(),))


class CountingVoteAcquirer:
    def __init__(self):
        self.requested: list[int] = []

    def acquire(self, locator, *, crosswalk=None):
        self.requested.append(locator.roll_number)
        raise _Unavailable("stub: no Clerk file in a hermetic test")

    def list_senate_votes(self, congress, session):
        from types import SimpleNamespace

        return SimpleNamespace(menu=SimpleNamespace(votes=()))


class _Unavailable(Exception):
    pass


@pytest.fixture(autouse=True)
def _vote_error(monkeypatch):
    """Make the stub's refusal the type the transform counts rather than raises."""
    import spicy_regs.transforms.build_roll_call_votes as module

    monkeypatch.setattr(module, "VoteSourceError", _Unavailable)


def test_roll_call_votes_skips_what_it_already_published(tmp_path, monkeypatch):
    from spicy_regs.transforms.build_roll_call_votes import build_roll_call_votes

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    # Every listed roll call is already published with a real tally.
    seed(
        tmp_path,
        "roll_call_votes",
        [
            {
                "vote_id": f"119-house-1-{n}",
                "congress": "119",
                "chamber": "house",
                "session": "1",
                "roll_number": str(n),
                "yea": "220",
            }
            for n in range(1, 41)
        ],
    )

    acquirer = CountingVoteAcquirer()
    build_roll_call_votes(
        tmp_path,
        reader=StubVoteListingReader(),
        acquirer=acquirer,
        overlap=5,
        download_prior=no_download,
    )
    # Only the overlap is re-read; the other 35 are not fetched again.
    assert len(acquirer.requested) == 5
    assert sorted(acquirer.requested, reverse=True) == [40, 39, 38, 37, 36]


def test_roll_call_votes_fetches_everything_with_no_prior(tmp_path, monkeypatch):
    from spicy_regs.transforms.build_roll_call_votes import build_roll_call_votes

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    acquirer = CountingVoteAcquirer()
    build_roll_call_votes(
        tmp_path, reader=StubVoteListingReader(), acquirer=acquirer, overlap=5, download_prior=no_download
    )
    assert len(acquirer.requested) == 40


def test_a_linkage_only_row_is_not_treated_as_held(tmp_path, monkeypatch):
    """A row with no tally means the Clerk file was never read; try it again."""
    from spicy_regs.transforms.build_roll_call_votes import build_roll_call_votes

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    seed(
        tmp_path,
        "roll_call_votes",
        [
            {
                "vote_id": f"119-house-1-{n}",
                "congress": "119",
                "chamber": "house",
                "session": "1",
                "roll_number": str(n),
                "yea": None,
            }
            for n in range(1, 41)
        ],
    )
    acquirer = CountingVoteAcquirer()
    build_roll_call_votes(
        tmp_path, reader=StubVoteListingReader(), acquirer=acquirer, overlap=5, download_prior=no_download
    )
    assert len(acquirer.requested) == 40


# --------------------------------------------------------------------------- #
# Committee reports: watermark, and held packages are not re-fetched.
# --------------------------------------------------------------------------- #
def test_committee_reports_window_starts_at_the_prior_watermark(tmp_path, monkeypatch):
    from spicy_regs.transforms.build_committee_reports import _since

    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)
    seed(tmp_path, "committee_reports", [{"package_id": "CRPT-119hrpt1", "last_modified": "2026-09-10T12:00:00Z"}])
    since = _since(prior_scratch_path(tmp_path, "committee_reports"))
    # The watermark minus the 24-hour overlap.
    assert since.startswith("2026-09-09T12:00:00"), since


def test_committee_reports_cold_start_uses_the_default_window(tmp_path, monkeypatch):
    from spicy_regs.transforms.build_committee_reports import _since

    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)
    since = _since(None)
    assert since.endswith("Z")
    # Thirty days back, not a watermark.
    assert since < date.today().isoformat()


def test_committee_reports_env_override_wins(tmp_path, monkeypatch):
    from spicy_regs.transforms.build_committee_reports import _since

    monkeypatch.setenv("COMMITTEE_REPORTS_SINCE", "2020-01-01")
    seed(tmp_path, "committee_reports", [{"package_id": "CRPT-119hrpt1", "last_modified": "2026-09-10T12:00:00Z"}])
    assert _since(prior_scratch_path(tmp_path, "committee_reports")) == "2020-01-01T00:00:00Z"


def test_committee_reports_skips_packages_it_holds(tmp_path, monkeypatch):
    from spicy_regs.transforms import build_committee_reports as module

    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)
    seed(
        tmp_path,
        "committee_reports",
        [{"package_id": "CRPT-119hrpt1", "last_modified": "2026-09-10T12:00:00Z"}],
    )

    from spicy_regs.transforms.committee_report_reads import READS_TABLE, RULE_VERSIONS
    from spicy_regs.transforms.table_merge import prior_scratch_path

    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "package_id": "CRPT-119hrpt1",
                    "last_modified": "2026-09-10T12:00:00Z",
                    "outcome": "complete",
                    "rule_version": RULE_VERSIONS["CRPT"],
                    "observed_at": "2026-09-20",
                }
            ]
        ),
        prior_scratch_path(tmp_path, READS_TABLE),
    )

    class Reader:
        """Serves each collection its own packages, as GovInfo does."""

        def packages(self, url, *, max_pages=40):
            collection = "CRPT" if "CRPT" in url else "CHRG"

            class Page:
                records = (
                    {"packageId": f"{collection}-119hrpt1"},
                    {"packageId": f"{collection}-119hrpt2"},
                )

            return iter((Page(),))

    class CountingAcquirer:
        def __init__(self):
            self.requested: list[str] = []

        def acquire(self, package_id, *, prefer=(), max_bytes=None):
            self.requested.append(package_id)
            raise LookupError("stub: no body in a hermetic test")

    acquirer = CountingAcquirer()

    class NoHearings:
        def records(self, route, url, *, max_pages=1):
            raise AssertionError("no package body was read, so no hearing detail may be asked for")

    module.build_committee_reports(
        tmp_path, reader=Reader(), acquirer=acquirer, hearings=NoHearings(), download_prior=no_download
    )
    assert "CRPT-119hrpt1" not in acquirer.requested, "a held package must not be re-fetched"
    assert "CRPT-119hrpt2" in acquirer.requested


def test_amendments_carry_the_detail_routes_sponsor_and_amended_bill(tmp_path, monkeypatch):
    """The list route states neither; each amendment's detail record does."""
    from spicy_regs.transforms.build_amendments import build_amendments

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    reader = ShiftingListingReader([([_amendment(7)], 1)])
    rows = pq.read_table(build_amendments(tmp_path, reader=reader, download_prior=no_download)).to_pylist()
    assert [(r["sponsor_bioguide_id"], r["amended_bill_id"], r["chamber"], r["url"]) for r in rows] == [
        ("S7", "119-hr-1", "Senate", "https://api.congress.gov/v3/amendment/119/samdt/7?format=json")
    ]


def test_amendments_refuse_a_detail_that_answers_nothing(tmp_path, monkeypatch):
    from spicy_regs.transforms.build_amendments import build_amendments

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    reader = ShiftingListingReader([([_amendment(404)], 1)])
    with pytest.raises(RuntimeError, match="answered 0 records"):
        build_amendments(tmp_path, reader=reader, download_prior=no_download)
