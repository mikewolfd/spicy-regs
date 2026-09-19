"""Hermetic tests for the five Congress.gov index rollups' shared walk.

No network. Every page served here is a retained response from the bounded
live runs of 2026-09-19 (``tests/fixtures/congress_index/README.md``), the
list pages trimmed to a handful of records. What is owned here is the walk's
resume rule -- which listed records cost a detail request, in what order,
under what cap, and what a refusal leaves behind -- not any shaper's reading
of a field, which spicy-docs establishes beside the shaper.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest
from spicy_docs.reading.paged_json import PagedJsonSourceError
from spicy_docs.schemas import TABLE_CONTRACTS
from spicy_docs.transport.credentials import CredentialRefusedError

from spicy_regs.transforms.build_congress_index import INDEX_SPECS, build_index_table
from tests.test_incremental_rollups import no_download, seed

FIXTURES = Path(__file__).parent / "fixtures" / "congress_index"

#: Per table, the identities whose details the live run read (and so are here).
DETAILED = {
    "house_communications": {("119", "ec", "4752"), ("119", "ec", "4751"), ("119", "ml", "136")},
    "committee_meetings": {("119", "senate", "338774"), ("119", "senate", "338765"), ("119", "house", "119569")},
    "record_issues": {("172", "148"), ("172", "147"), ("172", "145")},
    "treaties": {("119", "2", ""), ("119", "1", "")},
}


def _path_of(url: str) -> str:
    return url.split("/v3/", 1)[1].split("?", 1)[0]


class FixtureReader:
    """Serves the fixture whose name the URL's path spells, keyed as the route spells its rows.

    ``refuse`` names paths answered with a 404-shaped refusal; ``serve_as``
    maps a path to another fixture, for the record-that-is-not-the-one-asked-for case.
    """

    def __init__(self, *, refuse: frozenset[str] = frozenset(), serve_as: dict[str, str] | None = None):
        self.urls: list[str] = []
        self.refuse = refuse
        self.serve_as = serve_as or {}

    def records(self, route, url: str, *, max_pages: int = 1):
        self.urls.append(url)
        path = _path_of(url)
        name = self.serve_as.get(path, path).replace("/", "-") + ".json"
        if path in self.refuse or not (FIXTURES / name).exists():
            raise PagedJsonSourceError(f"stub: list source answered HTTP 404 for {path}")
        document = json.loads((FIXTURES / name).read_text())
        rows = document[route.records_key]
        yield SimpleNamespace(
            records=(rows,) if isinstance(rows, dict) else tuple(rows),
            declared_count=(document.get("pagination") or {}).get("count"),
        )

    @property
    def details(self) -> list[str]:
        return [_path_of(url) for url in self.urls if "limit=1" in url]


def _run(tmp_path, table: str, reader=None, **kwargs) -> tuple[list[dict], FixtureReader]:
    reader = reader or FixtureReader()
    kwargs.setdefault("max_details", 3)
    out = build_index_table(
        tmp_path, INDEX_SPECS[table], reader=reader, congresses=[119], download_prior=no_download, **kwargs
    )
    return pq.read_table(out).to_pylist(), reader


def _key(table: str, row: dict) -> tuple:
    return tuple(row[column] for column in TABLE_CONTRACTS[table].identity)


# --------------------------------------------------------------------------- #
# The published shape, and what a list-only row looks like next to a read one.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("table", sorted(DETAILED))
def test_a_run_publishes_every_listed_record_and_details_only_the_sampled_ones(tmp_path, table):
    rows, reader = _run(tmp_path, table)
    contract = TABLE_CONTRACTS[table]
    spec = INDEX_SPECS[table]
    assert [column for column in rows[0]] == list(contract.columns)
    read = {_key(table, row) for row in rows if row[spec.detail_marker] is not None}
    assert read == DETAILED[table], "the marker is set on exactly the rows whose detail was read"
    for row in rows:
        assert row["url"], "the list row states the URL on every row"
        assert row["update_date"], "the resume stamp is on every row"
    assert len(reader.details) == len(DETAILED[table])


def test_nominations_are_complete_from_the_list_and_ask_for_no_detail(tmp_path):
    rows, reader = _run(tmp_path, "nominations")
    listed = json.loads((FIXTURES / "nomination-119.json").read_text())["nominations"]
    assert reader.details == []
    assert [row["citation"] for row in rows] == sorted(record["citation"] for record in listed)
    assert all(row["latest_action_text"] for row in rows)


def test_the_rin_is_read_at_shape_time_from_the_detail_only(tmp_path):
    rows, _ = _run(tmp_path, "house_communications")
    by_id = {row["communication_id"]: row for row in rows}
    rulemaking = by_id["119-ec-4752"]
    assert (rulemaking["is_rulemaking"], rulemaking["rin"], rulemaking["rin_rule"]) == (
        "true",
        "3133-AF97",
        "report_nature_rin_label",
    )
    assert rulemaking["rin_matched_text"] == "RIN: 3133-AF97"
    assert rulemaking["matching_requirement_number"] == "8070"
    # Read and found nothing is not the same as not run.
    assert (by_id["119-ec-4751"]["rin"], by_id["119-ec-4751"]["rin_rule"]) == (None, "unmatched")
    unread = [row for row in rows if row["committees_json"] is None]
    assert unread and all(row["rin_rule"] is None for row in unread)


def test_the_captured_edges_land_on_the_rows(tmp_path):
    """The map's edges, on the live-captured details: meeting->bill, record->legislative-day, treaty->cdoc."""
    meetings, _ = _run(tmp_path, "committee_meetings")
    by_event = {row["event_id"]: row for row in meetings}
    assert by_event["338765"]["chamber"] == "senate" and by_event["338765"]["bill_count"] == "5"
    assert by_event["119569"]["witness_document_count"] == "3"

    issues, _ = _run(tmp_path, "record_issues")
    by_issue = {(row["volume"], row["issue"]): row for row in issues}
    assert by_issue[("172", "148")]["chambers"] == "senate"
    assert by_issue[("172", "147")]["chambers"] == "house\x1fsenate"
    assert by_issue[("172", "148")]["package_id"] == "CREC-2026-09-18"
    assert all(row["chambers"] is None for row in issues if row["volume"] == "171"), "volume 171 is list-only"

    treaties, _ = _run(tmp_path, "treaties")
    assert {row["package_id"] for row in treaties} == {"CDOC-119tdoc1", "CDOC-119tdoc2"}


# --------------------------------------------------------------------------- #
# The resume rule: what costs a request, in what order, and what a refusal leaves.
# --------------------------------------------------------------------------- #
def _seed_communications(tmp_path, **states) -> None:
    """A prior with 4752, 4751 and ML 136 in the states named: ``held``, ``unread`` or ``stale``."""
    stamps = {"4752": "2026-09-18", "4751": "2026-09-17", "136": "2026-09-17"}
    rows = []
    for number, state in states.items():
        kind = "ml" if number == "136" else "ec"
        rows.append(
            {
                "communication_id": f"119-{kind}-{number}",
                "congress": "119",
                "communication_type": kind,
                "number": number,
                "update_date": "2026-01-01" if state == "stale" else stamps[number],
                "committees_json": None if state == "unread" else "[]",
                "abstract": f"prior {state}",
            }
        )
    seed(tmp_path, "house_communications", rows)


def test_a_held_row_costs_nothing_and_an_unread_or_stale_one_is_retried(tmp_path):
    _seed_communications(tmp_path, **{"4752": "held", "4751": "unread", "136": "stale"})
    rows, reader = _run(tmp_path, "house_communications", max_details=10)
    assert "house-communication/119/ec/4752" not in reader.details, "held at its stamp with a detail: no request"
    assert {"house-communication/119/ec/4751", "house-communication/119/ml/136"} <= set(reader.details)
    by_id = {row["communication_id"]: row for row in rows}
    assert by_id["119-ec-4752"]["abstract"] == "prior held", "the held row is the prior's, untouched"
    assert by_id["119-ml-136"]["update_date"] == "2026-09-17", "the stale row was re-read at the new stamp"
    assert by_id["119-ec-4751"]["committees_json"] is not None


def test_details_are_read_newest_first_under_the_cap(tmp_path):
    rows, reader = _run(tmp_path, "house_communications", max_details=2)
    # 4752 is 2026-09-18; the two 2026-09-17 rows tie and the identity breaks the tie.
    assert reader.details == ["house-communication/119/ec/4752", "house-communication/119/ml/136"]
    unread = {row["communication_id"] for row in rows if row["committees_json"] is None}
    assert "119-ec-4751" in unread, "beyond the cap, a new record is still indexed list-only"


def test_a_stale_row_beyond_the_cap_keeps_its_prior_detail(tmp_path):
    """A list-only replacement would erase a detail already held; the stale stamp queues it next run."""
    _seed_communications(tmp_path, **{"4751": "stale"})
    rows, reader = _run(tmp_path, "house_communications", max_details=1)
    assert reader.details == ["house-communication/119/ec/4752"]
    stale = next(row for row in rows if row["communication_id"] == "119-ec-4751")
    assert (stale["update_date"], stale["committees_json"], stale["abstract"]) == ("2026-01-01", "[]", "prior stale")


def test_a_refused_detail_indexes_a_new_record_list_only_and_leaves_a_held_one_alone(tmp_path):
    _seed_communications(tmp_path, **{"4751": "stale"})
    reader = FixtureReader(refuse=frozenset({"house-communication/119/ec/4752", "house-communication/119/ec/4751"}))
    rows, _ = _run(tmp_path, "house_communications", reader=reader, max_details=10)
    by_id = {row["communication_id"]: row for row in rows}
    assert by_id["119-ec-4752"]["committees_json"] is None, "new and refused: indexed, marker NULL, retried next run"
    assert by_id["119-ec-4751"]["abstract"] == "prior stale", "held and refused: the prior row stands"
    assert by_id["119-ml-136"]["committees_json"] is not None, "the run went on past the refusals"


def test_a_detail_naming_another_record_is_refused_not_shaped(tmp_path):
    reader = FixtureReader(serve_as={"house-communication/119/ec/4752": "house-communication/119/ec/4751"})
    rows, _ = _run(tmp_path, "house_communications", reader=reader)
    by_id = {row["communication_id"]: row for row in rows}
    assert by_id["119-ec-4752"]["committees_json"] is None
    assert by_id["119-ec-4752"]["abstract"] is None, "nothing of 4751's detail reached 4752's row"


def test_a_credential_refusal_aborts_the_run(tmp_path):
    class Refusing(FixtureReader):
        def records(self, route, url, *, max_pages=1):
            if "limit=1" in url:
                raise CredentialRefusedError("stub: 403")
            yield from super().records(route, url, max_pages=max_pages)

    with pytest.raises(CredentialRefusedError):
        _run(tmp_path, "house_communications", reader=Refusing())


def test_a_partitioned_treaty_is_published_list_only_and_never_asked_for(tmp_path):
    """Its detail has no route in LIST_ROUTES, so it is not a refusal to retry: it is the whole fact."""

    class WithAPart(FixtureReader):
        def records(self, route, url, *, max_pages=1):
            for page in super().records(route, url, max_pages=max_pages):
                if "limit=1" not in url:
                    part = dict(page.records[0], number=3, suffix="A", url="https://api.congress.gov/v3/treaty/119/3/A")
                    page = SimpleNamespace(records=(*page.records, part), declared_count=page.declared_count)
                yield page

    rows, reader = _run(tmp_path, "treaties", reader=WithAPart())
    assert "treaty/119/3" not in " ".join(reader.details)
    part = next(row for row in rows if row["treaty_id"] == "119-3-A")
    assert (part["titles_json"], part["package_id"]) == (None, None)


def test_a_repeated_list_record_is_published_once(tmp_path):
    """The publisher repeats a record across a page boundary (15 of 4,975 on the 119th, live)."""

    class Repeating(FixtureReader):
        def records(self, route, url, *, max_pages=1):
            for page in super().records(route, url, max_pages=max_pages):
                if "limit=1" not in url:
                    page = SimpleNamespace(records=(*page.records, page.records[0]), declared_count=page.declared_count)
                yield page

    rows, _ = _run(tmp_path, "nominations", reader=Repeating())
    assert len(rows) == 2


def test_every_spec_marks_a_detail_only_column_and_matches_its_rollup(tmp_path):
    """The marker must be NULL on a list-only row, or the resume would never ask; and each spec has one writer."""
    from spicy_regs.pipelines.rollups import congress_index as rollups

    outputs = {
        cls.output.removesuffix(".parquet")
        for cls in vars(rollups).values()
        if isinstance(cls, type) and issubclass(cls, rollups.RollupPipeline) and cls is not rollups.RollupPipeline
    }
    assert outputs == set(INDEX_SPECS)
    for table, spec in INDEX_SPECS.items():
        assert (spec.detail_route is None) == (spec.detail_marker is None) == (spec.detail_query is None)
        if spec.detail_marker is not None:
            assert spec.detail_marker in TABLE_CONTRACTS[table].columns
            assert spec.detail_marker not in TABLE_CONTRACTS[table].identity
