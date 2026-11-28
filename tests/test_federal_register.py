"""Hermetic tests for the Federal Register ingest (no network).

Covers the raw-doc → published-schema mapping (``_shape``), the observation
and merge semantics of ``build_federal_register`` and ``merge_federal_register``,
and the windowing contract the transform relies on from the SpicyDocs owner
(``iter_federal_register_pages``).
The owner's own tests pin its cap-split, single-day refusal and retry
semantics.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pytest

from spicy_docs.schemas.federal_register import FEDERAL_REGISTER_COLUMNS, project_federal_register_document

from spicy_regs.transforms.build_federal_register import build_federal_register, merge_federal_register, published_columns

COLUMNS = published_columns()

_RAW_DOC = {
    "document_number": "2024-00001",
    "title": "A Proposed Rule",
    "abstract": "Does a thing.",
    "type": "Proposed Rule",
    "publication_date": "2024-03-01",
    "effective_on": None,
    "docket_ids": ["EPA-HQ-OAR-2024-0001", "FRL-1234-01-OAR"],
    "regulation_id_numbers": ["2060-AV12"],
    "cfr_references": [{"title": 40, "part": 60, "chapter": None, "citation_url": None}],
    "agencies": [
        {
            "raw_name": "ENVIRONMENTAL PROTECTION AGENCY",
            "name": "Environmental Protection Agency",
            "slug": "environmental-protection-agency",
        },
        {"raw_name": "No slug agency", "name": "No slug agency"},
    ],
    "volume": 89,
    "start_page": 1234,
    "end_page": 1240,
    "executive_order_number": None,
    "html_url": "https://www.federalregister.gov/d/2024-00001",
}


def test_shape_produces_exact_schema():
    row = project_federal_register_document(_RAW_DOC)
    # Every fetched column present, and nothing extra; ``rin`` is derived in the merge.
    assert set(row) == set(FEDERAL_REGISTER_COLUMNS)
    # The contract orders them; SpicyDocs 0.66.6 lists ``rin`` before its newest fields. The builder's own page
    # pointer follows the contract's columns.
    assert sorted(COLUMNS[:-1]) == sorted((*FEDERAL_REGISTER_COLUMNS, "rin")) and len(COLUMNS) == len(set(COLUMNS))
    assert COLUMNS[-1] == "source_sha256"


def _collision_documents():
    return json.loads((Path(__file__).parent / "fixtures/federal_register/00-111.json").read_text())


def test_dated_native_records_survive_and_same_date_correction_replaces(tmp_path):
    """The merge into the published table (merge_federal_register): fresh wins on the dated key, nothing else moves."""
    import pyarrow.parquet as pq

    documents = _collision_documents()
    out = merge_federal_register(tmp_path, documents=lambda start: iter(documents), download_prior=lambda k, p: False)
    first = pq.read_table(out).to_pylist()
    assert {(r["document_number"], r["publication_date"]) for r in first} == {
        ("00-111", "2000-01-14"),
        ("00-111", "2000-01-18"),
    }
    # The correction is a controlled edit to one captured record, not a claim
    # that the publisher changed its title. The date-distinct record is untouched.
    correction = dict(documents[0], title="Corrected housing-credit title")
    merge_federal_register(tmp_path, documents=lambda start: iter([correction]), download_prior=lambda k, p: False)
    corrected = pq.read_table(out).to_pylist()
    assert len(corrected) == 2
    by_date = {r["publication_date"]: r for r in corrected}
    assert by_date["2000-01-14"]["title"] == correction["title"]
    assert by_date["2000-01-18"] == next(r for r in first if r["publication_date"] == "2000-01-18")
    merge_federal_register(tmp_path, documents=lambda start: iter([correction]), download_prior=lambda k, p: False)
    assert pq.read_table(out).to_pylist() == corrected


def test_unreviewed_number_collision_is_not_a_special_case(tmp_path):
    import pyarrow.parquet as pq

    records = [dict(_RAW_DOC, publication_date=day) for day in ("2024-03-01", "2024-03-02")]
    out = build_federal_register(
        tmp_path, documents=lambda start: iter(records)
    )
    assert pq.read_table(out).num_rows == 2


def test_identical_repeated_observations_collapse_within_the_dated_key(tmp_path):
    import pyarrow.parquet as pq

    out = build_federal_register(
        tmp_path, documents=lambda start: iter([_RAW_DOC, _RAW_DOC])
    )
    assert pq.read_table(out).num_rows == 1


def test_conflicting_same_date_fetch_refuses_before_output(tmp_path):
    records = [_RAW_DOC, dict(_RAW_DOC, title="Conflicting same-run observation")]
    out = tmp_path / "federal_register.parquet"
    out.write_bytes(b"previous output remains untouched")
    with pytest.raises(ValueError, match="conflicting.*same.*date"):
        build_federal_register(tmp_path, documents=lambda start: iter(records))
    assert out.read_bytes() == b"previous output remains untouched"


@pytest.mark.parametrize("date_value", [None, "", "2000-13-01", "20000114"])
def test_unknown_or_noncanonical_date_refuses_before_output(tmp_path, date_value):
    with pytest.raises(ValueError, match="identity"):
        build_federal_register(
            tmp_path,
            documents=lambda start: iter([dict(_RAW_DOC, publication_date=date_value)]),
        )
    assert not (tmp_path / "federal_register.parquet").exists()


def test_shape_maps_and_serializes_fields():
    row = project_federal_register_document(_RAW_DOC)
    assert row["document_number"] == "2024-00001"
    assert row["document_type"] == "Proposed Rule"  # API `type` -> document_type
    # Array fields serialize to JSON strings.
    assert json.loads(row["docket_ids_json"] or "null") == ["EPA-HQ-OAR-2024-0001", "FRL-1234-01-OAR"]
    assert json.loads(row["regulation_id_numbers_json"] or "null") == ["2060-AV12"]
    assert json.loads(row["cfr_references_json"] or "null")[0]["part"] == 60
    # agency_slugs is a comma-joined string of slugs, skipping agencies with none.
    assert row["agency_slugs"] == "environmental-protection-agency"
    # Integer scalars stringify (schema is all-VARCHAR).
    assert row["volume"] == "89"
    assert row["start_page"] == "1234"
    # Nulls pass through, and the REST API never reports modify_date.
    assert row["effective_on"] is None
    assert row["executive_order_number"] is None
    assert row["modify_date"] is None


def test_shape_handles_missing_arrays():
    row = project_federal_register_document({"document_number": "x"})
    assert row["docket_ids_json"] == "[]"
    assert row["regulation_id_numbers_json"] == "[]"
    assert row["cfr_references_json"] == "[]"
    assert row["agencies_json"] == "[]"
    assert row["agency_slugs"] is None


def test_archive_fetch_uses_bounded_top_level_windows():
    """Top-level windows must be capped in width *and* tile [since, until] exactly.

    Width alone missed an off-by-one: a stride of +2 days still yields narrow
    windows while dropping every 91st day of the archive. Only the gap-and-overlap
    assertion catches that. Exercised through the owner's acquisition, which the
    transform's live path consumes.
    """
    from urllib.parse import parse_qs, urlparse

    from spicy_docs.sources.federal_register import native as federal_register

    scope = {"publishedFrom": "2024-01-01", "publishedThrough": "2024-12-31"}
    windows: list[tuple[date, date]] = []

    def fetch(url: str) -> bytes:
        query = parse_qs(urlparse(url).query)
        windows.append(
            (
                date.fromisoformat(query["conditions[publication_date][gte]"][0]),
                date.fromisoformat(query["conditions[publication_date][lte]"][0]),
            )
        )
        return b'{"count": 0, "results": []}'

    list(federal_register.iter_federal_register_pages(fetch, query_scope=scope, traversals=1))
    assert len(windows) > 1
    assert all((lte - gte).days < federal_register.MAX_WINDOW_DAYS for gte, lte in windows)
    assert windows[0][0] == date.fromisoformat(scope["publishedFrom"])
    assert windows[-1][1] == date.fromisoformat(scope["publishedThrough"])
    assert all(nxt[0] == prev[1] + timedelta(days=1) for prev, nxt in zip(windows, windows[1:]))


# Round 6, M2: the Register's own regulations.gov link (spicy-docs 0.54.0+chaos.a1b91ce28a2a).

def _linked_records():
    """Seven real Register records: every shape of regulations_dot_gov_info, and its docket printed three ways."""
    return json.loads((Path(__file__).parent / "fixtures/federal_register/regulations-dot-gov-info.json").read_text())


def test_each_run_re_reads_the_last_120_days_so_the_relayed_comment_count_is_not_publication_weeks(tmp_path):
    """The Register re-checks regulations.gov until about the comment period's close (median 32 days after
    publication, 99th percentile about 91; impl-W/m2/overlap_days.out), so a week's overlap froze the count early."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from spicy_regs.transforms.regulations_shape import subject_schema

    schema = subject_schema("federal_register")
    row = dict.fromkeys(schema.names) | {"document_number": "2026-19936", "publication_date": "2026-09-30"}
    pq.write_table(pa.Table.from_pylist([row], schema=schema), tmp_path / "_fr_published.parquet")
    asked = []
    merge_federal_register(tmp_path, documents=lambda start: asked.append(start) or iter(()),
                           download_prior=lambda k, p: False)
    assert asked == [date(2026, 9, 30) - timedelta(days=120)]


def _prior(tmp_path, *, without=()):
    """A published prior of three rows, every one stating a stale ``rin``; ``without`` leaves columns out of it."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from spicy_regs.transforms.regulations_shape import subject_schema

    schema = subject_schema("federal_register")
    rows = [dict.fromkeys(schema.names) | {
        "document_number": number, "publication_date": "2024-01-01", "title": f"Prior {number}", "rin": "9999-ZZ99",
        "regulation_id_numbers": rins, "docket_ids": ["EPA-HQ-OAR-2024-0002"], "topics": ["Air pollution control"],
        "agencies": [{"id": 145, "name": "Environmental Protection Agency", "raw_name": "ENVIRONMENTAL PROTECTION AGENCY",
                      "parent_id": None, "slug": "environmental-protection-agency"}],
        "regulations_dot_gov_comments_count": 4, "significant": False,
        "html_url": f"https://www.federalregister.gov/documents/2024/01/01/{number}/x", "regulations_dot_gov_info_json": "{}",
    } for number, rins in (("P-two", ["1111-AA11", "2222-BB22"]), ("P-none", []), ("P-null", None))]
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table.drop_columns(list(without)), tmp_path / "_fr_published.parquet")
    return {row["document_number"]: row for row in rows}


@pytest.mark.parametrize("without", [(), ("rin", "topics")], ids=["published-width", "older-width"])
def test_rin_is_derived_for_every_row_whatever_the_prior_held(tmp_path, without):
    """The join key is a projection of the array, derived on carried rows and fresh rows alike, never read back.

    Carried rows keep the mapping of the night that last read them; ``rin`` is the exception, so a stale one (or none,
    in a prior from before the column) never survives the merge. A prior of another width is mapped again, its
    other values unchanged, and a column it lacks reads NULL until a re-read fills it.
    """
    import pyarrow.parquet as pq

    from spicy_regs.transforms.regulations_shape import subject_schema

    prior = _prior(tmp_path, without=without)
    fresh = [dict(_RAW_DOC, document_number="F-one", publication_date="2024-03-01")]
    journal = _Journal()
    out = merge_federal_register(tmp_path, since=date(2024, 3, 1), documents=lambda start: iter(fresh),
                                 download_prior=lambda k, p: False, evidence=journal)
    assert pq.read_schema(out).equals(subject_schema("federal_register"), check_metadata=False)
    # The journal says whether the prior was mapped again and which columns that added: a rename leaves a trace.
    [merge] = journal.events["federal-register-merge"]
    assert merge == {"prior_rows": 3, "read_rows": 1, "new_rows": 1, "rows": 4, "remapped": bool(without)} | (
        {"columns_added": ["topics", "rin"], "columns_dropped": [], "columns_retyped": []} if without else {})
    held = {row["document_number"]: row for row in pq.read_table(out).to_pylist()}
    assert held["F-one"]["rin"] == "2060-AV12"
    assert held["P-two"]["rin"] == "1111-AA11", "the first of several; the array keeps the rest"
    assert held["P-none"]["rin"] is None and held["P-null"]["rin"] is None
    for number, row in prior.items():
        assert {name: value for name, value in held[number].items() if name not in ("rin", *without)} == {
            name: value for name, value in row.items() if name not in ("rin", *without)}
        assert all(held[number][name] is None for name in without if name != "rin")


def test_a_re_map_that_loses_a_row_fails_the_merge(tmp_path, monkeypatch):
    """The count is the prior's as published, so a re-map that drops a row cannot pass as a smaller prior."""
    import pyarrow.parquet as pq

    from spicy_regs.transforms import regulations_shape

    mapped = regulations_shape.mapped_table

    def losing(dataset, text, destination, **options):
        mapped(dataset, text, destination, **options)
        if destination.name == "_fr_prior.parquet":
            table = pq.read_table(destination)
            pq.write_table(table.slice(1), destination)
        return destination

    monkeypatch.setattr(regulations_shape, "mapped_table", losing)
    _prior(tmp_path, without=("topics",))
    with pytest.raises(ValueError, match="lost or repeated a record.*'prior_rows': 3.*'rows': 2"):
        merge_federal_register(tmp_path, since=date(2024, 3, 1), documents=lambda start: iter(()),
                               download_prior=lambda k, p: False)
    assert not (tmp_path / "federal_register.parquet").exists()


class _Journal:
    """The run's journal as the merge uses it: named events with their fields."""

    def __init__(self):
        self.events = {}

    def event(self, name, **fields):
        self.events.setdefault(name, []).append(fields)


def test_a_prior_without_a_column_that_left_receipts_is_refused_before_anything_is_read(tmp_path):
    """Carried, it would publish NULL where its receipts held a value; nothing else stops it reaching the table."""
    asked = []
    _prior(tmp_path, without=("html_url",))
    with pytest.raises(ValueError, match=r"lacks \['html_url'\], which a generation before 2026-11-10 held only"):
        merge_federal_register(tmp_path, documents=lambda start: asked.append(start) or iter(()),
                               download_prior=lambda k, p: False)
    assert asked == [] and not (tmp_path / "federal_register.parquet").exists()


def _links(tmp_path, records):
    import pyarrow.parquet as pq

    from spicy_regs.transforms.build_fr_docket_links import build_fr_docket_links

    # The links read the Register's published (merged, native) table, as the plain route gives it them.
    merge_federal_register(tmp_path, documents=lambda start: iter(records), download_prior=lambda k, p: False)
    return pq.read_table(build_fr_docket_links(tmp_path)).to_pylist()


def test_each_document_docket_pair_is_one_row_that_says_which_statement_carries_it(tmp_path):
    """Printed labels, the Register's regulations.gov docket, or both: one row per pair, never one per statement.

    A printed label carries the Register's docket when it is that id or SpicyDocs' normalizer reads that id from
    it (E8-3813 prints "Docket No. FAA-2007-29334"). A docket is never read off a document id (2026-19200 states
    FERC-2026-5579-0001 and no docket).
    """
    rows = _links(tmp_path, _linked_records())
    got = sorted((r["document_number"], r["docket_id"], r["docket_source_ordinal"], r["link_source"]) for r in rows)
    assert got == [
        ("2016-04346", "NRC-2016-0040", 0, "both"),
        ("2026-12696", "FNA-2026-0034", None, "regulations_dot_gov_info"),
        ("2026-12696", "FNS-2026-0595", 0, "printed"),
        ("2026-19200", "Docket No. IC26-36-000", 0, "printed"),
        ("E8-3813", "AD 2008-05-04", 3, "printed"),
        ("E8-3813", "Amendment 39-15398", 2, "printed"),
        ("E8-3813", "Directorate Identifier 2006-NM-268-AD", 1, "printed"),
        ("E8-3813", "Docket No. FAA-2007-29334", 0, "both"),
        ("E8-4997", "Docket ID No. EPA-HQ-OW-2004-0015", 2, "both"),
        ("E8-4997", "EPA-HQ-OW-2004-0015", 0, "both"),
        ("E8-4997", "FRL-8541-7", 1, "printed"),
    ]
    fna = next(r for r in rows if r["docket_id"] == "FNA-2026-0034")
    assert (fna["publication_date"], fna["normalized_docket_candidates"]) == ("2026-06-24", ["FNA-2026-0034"])


def test_a_register_table_read_before_the_link_was_requested_links_its_printed_labels_only(tmp_path):
    """A held row whose regulations_dot_gov_info is NULL (not read) adds no row: absence of a read is not a link."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from spicy_regs.transforms.build_fr_docket_links import build_fr_docket_links

    merge_federal_register(tmp_path, documents=lambda start: iter(_linked_records()), download_prior=lambda k, p: False)
    table = pq.read_table(tmp_path / "federal_register.parquet")
    for name in [c for c in table.column_names if c.startswith("regulations_dot_gov")]:
        at = table.schema.get_field_index(name)
        table = table.set_column(at, table.schema.field(at), pa.nulls(table.num_rows, table.schema.field(at).type))
    pq.write_table(table, tmp_path / "federal_register.parquet")
    rows = pq.read_table(build_fr_docket_links(tmp_path)).to_pylist()
    assert {r["link_source"] for r in rows} == {"printed"} and len(rows) == 10


def test_the_rulemaking_stages_read_the_printed_statement_only(tmp_path):
    """Which dockets the rulemaking stages trust is unchanged: the Register's regulations.gov docket is a new
    statement, and whether the stages read it is a later decision (scout C, M2 owner call 2)."""
    from spicy_regs.ontology.federal_register import FederalRegisterIndex

    _links(tmp_path, _linked_records())
    index = FederalRegisterIndex(tmp_path / "federal_register.parquet")
    dockets = {(reference["document_number"], docket)
               for docket, reference in index.docket_links(tmp_path / "fr_docket_links.parquet")}
    assert ("2026-12696", "FNA-2026-0034") not in dockets
    assert {("2026-12696", "FNS-2026-0595"), ("2016-04346", "NRC-2016-0040")} <= dockets


def test_live_fetch_bounds_scope_with_until_and_preserves_31_fields(tmp_path, monkeypatch):
    """``until`` bounds ``publishedThrough`` in API requests, journal events, and retained evidence."""
    from contextlib import contextmanager
    from urllib.parse import parse_qs, urlparse

    from spicy_docs.sources.federal_register import native as federal_register
    from spicy_docs.transport import acquisition

    from spicy_regs.transforms.build_federal_register import _live_federal_register_documents

    requested_windows: list[tuple[date, date]] = []
    seen_field_sets: list[frozenset[str]] = []

    def fake_fetch(url: str) -> bytes:
        query = parse_qs(urlparse(url).query)
        gte = date.fromisoformat(query["conditions[publication_date][gte]"][0])
        lte = date.fromisoformat(query["conditions[publication_date][lte]"][0])
        requested_windows.append((gte, lte))
        seen_field_sets.append(frozenset(query["fields[]"]))
        doc = dict(_RAW_DOC, document_number=f"DOC-{gte.isoformat()}", publication_date=gte.isoformat())
        return json.dumps({"count": 1, "total_pages": 1, "results": [doc]}).encode()

    @contextmanager
    def fetcher(_):
        yield fake_fetch

    monkeypatch.setattr(acquisition, "federal_register_fetcher", fetcher)
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw-source-originals"))
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    rows = list(_live_federal_register_documents(date(2020, 1, 1), until=date(2020, 6, 30), output_dir=out_dir))
    assert len(requested_windows) == 3  # 182 days tiled into 90 + 90 + 2 day windows
    assert requested_windows[0][0] == date(2020, 1, 1)
    assert requested_windows[-1][1] == date(2020, 6, 30)
    assert all(nxt[0] == prev[1] + timedelta(days=1) for prev, nxt in zip(requested_windows, requested_windows[1:]))
    assert all(fields == federal_register.DOCUMENT_FIELDS for fields in seen_field_sets)
    assert len(rows) == 3
    assert all(page.startswith("sha256:") and len(page) == 71 for page, _ in rows)

    [journal_path] = list(out_dir.rglob("journal.jsonl"))
    events = [json.loads(line) for line in journal_path.read_text().splitlines()]
    [pages_event] = [e for e in events if e.get("event") == "federal-register-pages"]
    assert pages_event["requested_from"] == "2020-01-01"
    assert pages_event["requested_through"] == "2020-06-30"
    retained = [e for e in events if e.get("event") == "retained-file"]
    assert len(retained) == 3
    assert all(
        e["covered_period"] == {"requested_from": "2020-01-01", "requested_through": "2020-06-30"}
        for e in retained
    )


def test_merge_with_until_updates_window_and_preserves_outside_rows_and_composite_keys(tmp_path, monkeypatch):
    """A bounded ``[since, until]`` backfill replaces only keys in that window and preserves all outside rows."""
    import hashlib
    from contextlib import contextmanager
    from urllib.parse import parse_qs, urlparse

    import pyarrow as pa
    import pyarrow.parquet as pq
    from spicy_docs.transport import acquisition

    from spicy_regs.pipelines.rollups.federal_register import FederalRegisterRollup
    from spicy_regs.transforms.regulations_shape import subject_schema

    schema = subject_schema("federal_register")
    prior_rows = [
        # Pre-window row (1998) — untouched by 2020 batch, updated by 1998 batch
        dict.fromkeys(schema.names)
        | {
            "document_number": "98-100",
            "publication_date": "1998-05-01",
            "title": "Old 1998 notice",
            "html_url": "https://www.federalregister.gov/d/98-100",
            "body_html_url": "https://www.federalregister.gov/body/98-100.html",
            "regulations_dot_gov_info_json": "{}",
            "president_json": None,
            "source_sha256": None,
        },
        # In-window row (2020) — replaced with fresh 31-field data + source_sha256
        dict.fromkeys(schema.names)
        | {
            "document_number": "2020-00010",
            "publication_date": "2020-02-01",
            "title": "Old 2020 rule without president",
            "html_url": "https://www.federalregister.gov/d/2020-00010",
            "body_html_url": "https://www.federalregister.gov/body/2020-00010.html",
            "regulations_dot_gov_info_json": "{}",
            "president_json": None,
            "source_sha256": None,
        },
        # Composite-key doc spanning two dates: 2020 date inside batch, 2022 date outside batch
        dict.fromkeys(schema.names)
        | {
            "document_number": "00-111",
            "publication_date": "2020-03-10",
            "title": "Old 2020 printing of 00-111",
            "html_url": "https://www.federalregister.gov/documents/2020/03/10/00-111/old",
            "body_html_url": "https://www.federalregister.gov/body/2020-00-111.html",
            "regulations_dot_gov_info_json": "{}",
            "president_json": None,
            "source_sha256": None,
        },
        dict.fromkeys(schema.names)
        | {
            "document_number": "00-111",
            "publication_date": "2022-03-10",
            "title": "Later 2022 printing of 00-111",
            "html_url": "https://www.federalregister.gov/documents/2022/03/10/00-111/keep",
            "body_html_url": "https://www.federalregister.gov/body/2022-00-111.html",
            "regulations_dot_gov_info_json": "{}",
            "president_json": '{"identifier":"joseph-biden","name":"Joseph R. Biden Jr."}',
            "source_sha256": "sha256:" + "b" * 64,
        },
        # Post-window live 2026 row — untouched by historical batch
        dict.fromkeys(schema.names)
        | {
            "document_number": "2026-24001",
            "publication_date": "2026-08-01",
            "title": "Live 2026 row",
            "html_url": "https://www.federalregister.gov/d/2026-24001",
            "body_html_url": "https://www.federalregister.gov/body/2026-24001.html",
            "regulations_dot_gov_info_json": "{}",
            "president_json": '{"identifier":"donald-trump","name":"Donald J. Trump"}',
            "source_sha256": "sha256:" + "a" * 64,
        },
    ]
    out_dir = tmp_path / "output"
    out_dir.mkdir()
    pq.write_table(pa.Table.from_pylist(prior_rows, schema=schema), out_dir / "federal_register.parquet")

    fresh_2020 = [
        dict(
            _RAW_DOC,
            document_number="2020-00010",
            publication_date="2020-02-01",
            title="Refreshed 2020 rule",
            president={"identifier": "donald-trump", "name": "Donald J. Trump"},
            regulations_dot_gov_info={},
        ),
        dict(
            _RAW_DOC,
            document_number="00-111",
            publication_date="2020-03-10",
            title="Refreshed 2020 printing of 00-111",
            president={"identifier": "donald-trump", "name": "Donald J. Trump"},
            regulations_dot_gov_info={},
        ),
    ]
    fresh_1998 = [
        dict(
            _RAW_DOC,
            document_number="98-100",
            publication_date="1998-05-01",
            title="Refreshed 1998 notice",
            president={"identifier": "william-j-clinton", "name": "William J. Clinton"},
            regulations_dot_gov_info={},
        ),
    ]
    all_fresh = fresh_2020 + fresh_1998
    served_pages: dict[str, bytes] = {}

    def fake_fetch(url: str) -> bytes:
        query = parse_qs(urlparse(url).query)
        gte = query["conditions[publication_date][gte]"][0]
        lte = query["conditions[publication_date][lte]"][0]
        matched = [doc for doc in all_fresh if gte <= doc["publication_date"] <= lte]
        raw = json.dumps({"count": len(matched), "total_pages": 1, "results": matched}).encode()
        if matched:
            served_pages[matched[0]["document_number"]] = raw
        return raw

    @contextmanager
    def fetcher(_):
        yield fake_fetch

    monkeypatch.setattr(acquisition, "federal_register_fetcher", fetcher)
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw-source-originals"))
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    monkeypatch.setenv("FR_SINCE", "2020-01-01")
    monkeypatch.setenv("FR_UNTIL", "2020-03-31")
    monkeypatch.setenv("FR_REQUEST_INTERVAL_SECONDS", "0")

    FederalRegisterRollup(output_dir=out_dir, skip_upload=True).run()
    [gen_1] = list((out_dir / "generations").iterdir())
    assert pq.read_schema(gen_1 / "federal_register.parquet").equals(schema, check_metadata=False)
    after_2020 = {(r["document_number"], r["publication_date"]): r for r in pq.read_table(gen_1 / "federal_register.parquet").to_pylist()}
    assert len(after_2020) == 5

    # In-window rows updated with president_json and matching retained page SHA-256
    digest_2020 = "sha256:" + hashlib.sha256(served_pages["2020-00010"]).hexdigest()
    assert after_2020[("2020-00010", "2020-02-01")]["title"] == "Refreshed 2020 rule"
    assert json.loads(after_2020[("2020-00010", "2020-02-01")]["president_json"])["identifier"] == "donald-trump"
    assert after_2020[("2020-00010", "2020-02-01")]["source_sha256"] == digest_2020
    assert after_2020[("00-111", "2020-03-10")]["title"] == "Refreshed 2020 printing of 00-111"
    assert after_2020[("00-111", "2020-03-10")]["source_sha256"] == digest_2020

    # Outside rows (pre-window 1998, same-number 2022 printing, post-window 2026) preserved untouched
    assert after_2020[("98-100", "1998-05-01")]["title"] == "Old 1998 notice"
    assert after_2020[("98-100", "1998-05-01")]["source_sha256"] is None
    assert after_2020[("00-111", "2022-03-10")]["title"] == "Later 2022 printing of 00-111"
    assert after_2020[("00-111", "2022-03-10")]["source_sha256"] == "sha256:" + "b" * 64
    assert after_2020[("2026-24001", "2026-08-01")]["source_sha256"] == "sha256:" + "a" * 64

    # Second disjoint batch (1998) updates 1998 row while keeping 2020/2022/2026 intact
    monkeypatch.setenv("FR_SINCE", "1998-05-01")
    monkeypatch.setenv("FR_UNTIL", "1998-05-01")
    FederalRegisterRollup(output_dir=out_dir, skip_upload=True).run()
    [gen_2] = [p for p in (out_dir / "generations").iterdir() if p != gen_1]
    after_1998 = {(r["document_number"], r["publication_date"]): r for r in pq.read_table(gen_2 / "federal_register.parquet").to_pylist()}
    assert len(after_1998) == 5
    digest_1998 = "sha256:" + hashlib.sha256(served_pages["98-100"]).hexdigest()
    assert after_1998[("98-100", "1998-05-01")]["source_sha256"] == digest_1998
    assert json.loads(after_1998[("98-100", "1998-05-01")]["president_json"])["identifier"] == "william-j-clinton"
    assert after_1998[("2020-00010", "2020-02-01")] == after_2020[("2020-00010", "2020-02-01")]
    assert after_1998[("00-111", "2022-03-10")] == after_2020[("00-111", "2022-03-10")]
    assert after_1998[("2026-24001", "2026-08-01")] == after_2020[("2026-24001", "2026-08-01")]


def test_date_window_and_env_validation_refuses_malformed_or_reversed_dates(tmp_path, monkeypatch):
    """Malformed ISO dates, reversed ``since > until`` windows, and invalid pacing/batch params fail closed."""
    from datetime import datetime

    import pyarrow as pa
    import pyarrow.parquet as pq

    from spicy_regs.env_values import date_env, float_env
    from spicy_regs.transforms.build_federal_register import _live_federal_register_documents
    from spicy_regs.transforms.regulations_shape import subject_schema

    called = []
    with pytest.raises(ValueError, match="reversed"):
        build_federal_register(
            tmp_path,
            since=date(2020, 6, 1),
            until=date(2020, 5, 31),
            documents=lambda start: called.append(start) or iter(()),
        )
    assert called == []

    with pytest.raises(ValueError, match="reversed"):
        merge_federal_register(
            tmp_path,
            since=date(2020, 6, 1),
            until=date(2020, 5, 31),
            documents=lambda start: called.append(start) or iter(()),
            download_prior=lambda k, p: called.append("prior") or False,
        )
    assert called == []

    with pytest.raises(ValueError, match="reversed"):
        list(_live_federal_register_documents(date(2020, 6, 1), until=date(2020, 5, 31), output_dir=tmp_path))

    # Implicit since from prior_max (2026-09-30 - 120d = 2026-06-02) > explicit until (2020-12-31) refuses
    # without clobbering the local federal_register.parquet file.
    schema = subject_schema("federal_register")
    row = dict.fromkeys(schema.names) | {"document_number": "2026-19936", "publication_date": "2026-09-30"}
    local_out = tmp_path / "federal_register.parquet"
    pq.write_table(pa.Table.from_pylist([row], schema=schema), local_out)
    before_bytes = local_out.read_bytes()
    with pytest.raises(ValueError, match="reversed"):
        merge_federal_register(
            tmp_path,
            until=date(2020, 12, 31),
            documents=lambda start: called.append(start) or iter(()),
            download_prior=lambda k, p: False,
        )
    assert called == [] and local_out.read_bytes() == before_bytes

    for bad_date in ("20200601", "2020-13-01", "not-a-date", datetime(2020, 6, 1)):
        with pytest.raises(ValueError, match="until"):
            build_federal_register(tmp_path, since=date(2020, 1, 1), until=bad_date, documents=lambda s: iter(()))  # type: ignore[arg-type]

    for bad_env in ("20200601", "2020-02-30", "yesterday"):
        monkeypatch.setenv("FR_UNTIL", bad_env)
        with pytest.raises(ValueError, match="FR_UNTIL must be YYYY-MM-DD"):
            date_env("FR_UNTIL")
    monkeypatch.setenv("FR_UNTIL", "2020-06-30")
    assert date_env("FR_UNTIL") == date(2020, 6, 30)

    for bad_interval in (-0.1, float("nan"), float("inf"), True):
        with pytest.raises(ValueError, match="request_interval_seconds"):
            build_federal_register(
                tmp_path,
                since=date(2020, 1, 1),
                request_interval_seconds=bad_interval,  # type: ignore[arg-type]
                documents=lambda s: iter(()),
            )
    for bad_float_env in ("nan", "inf", "-inf", "abc"):
        monkeypatch.setenv("FR_REQUEST_INTERVAL_SECONDS", bad_float_env)
        with pytest.raises(ValueError, match="FR_REQUEST_INTERVAL_SECONDS must be a finite number"):
            float_env("FR_REQUEST_INTERVAL_SECONDS")
    monkeypatch.setenv("FR_REQUEST_INTERVAL_SECONDS", "0.25")
    assert float_env("FR_REQUEST_INTERVAL_SECONDS") == 0.25

    for bad_batch in (0, -10, True):
        with pytest.raises(ValueError, match="batch_size"):
            build_federal_register(
                tmp_path,
                since=date(2020, 1, 1),
                batch_size=bad_batch,  # type: ignore[arg-type]
                documents=lambda s: iter(()),
            )


def test_streaming_parquet_writer_matches_single_batch_and_flushes_incrementally(tmp_path, monkeypatch):
    """Streaming ``ParquetWriter`` flushes chunks incrementally and matches single-batch output exactly."""
    from contextlib import contextmanager

    import pyarrow.parquet as pq
    from spicy_docs.transport import acquisition

    from spicy_regs.transforms.build_federal_register import STREAM_BATCH_ROWS

    assert STREAM_BATCH_ROWS == 10_000

    # Two pages; doc "2020-DUP" repeats identically across pages (and chunks), while "2020-MULTI" spans two dates.
    p1_docs = [
        dict(_RAW_DOC, document_number="2020-001", publication_date="2020-01-01"),
        dict(_RAW_DOC, document_number="2020-DUP", publication_date="2020-01-02"),
        dict(_RAW_DOC, document_number="2020-MULTI", publication_date="2020-01-03", title="First printing"),
    ]
    next_url = (
        "https://www.federalregister.gov/api/v1/documents?"
        "conditions%5Bpublication_date%5D%5Bgte%5D=2020-01-01&conditions%5Bpublication_date%5D%5Blte%5D=2020-01-05"
        "&format=json&order=newest&page=2&per_page=1000"
    )
    p2_docs = [
        dict(_RAW_DOC, document_number="2020-004", publication_date="2020-01-04"),
        dict(_RAW_DOC, document_number="2020-DUP", publication_date="2020-01-02"),
        dict(_RAW_DOC, document_number="2020-MULTI", publication_date="2020-01-05", title="Second printing"),
    ]
    raw_p1 = json.dumps({"count": 6, "total_pages": 2, "next_page_url": next_url, "results": p1_docs}).encode()
    raw_p2 = json.dumps({"count": 6, "total_pages": 2, "results": p2_docs}).encode()

    @contextmanager
    def fetcher(_):
        yield lambda url: raw_p2 if url == next_url else raw_p1

    monkeypatch.setattr(acquisition, "federal_register_fetcher", fetcher)
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw-source-originals"))

    single_dir = tmp_path / "single"
    stream_dir = tmp_path / "stream"
    single_dir.mkdir()
    stream_dir.mkdir()

    single_out = build_federal_register(single_dir, since=date(2020, 1, 1), until=date(2020, 1, 5), batch_size=10_000)

    # Track write_table calls during streaming to prove chunks flush BEFORE the full stream ends.
    real_write_table = pq.ParquetWriter.write_table
    chunk_sizes: list[int] = []

    def spy_write_table(self, table, *args, **kwargs):
        chunk_sizes.append(table.num_rows)
        return real_write_table(self, table, *args, **kwargs)

    monkeypatch.setattr(pq.ParquetWriter, "write_table", spy_write_table)
    stream_out = build_federal_register(stream_dir, since=date(2020, 1, 1), until=date(2020, 1, 5), batch_size=2)

    assert chunk_sizes == [2, 2, 2]
    single_tbl = pq.read_table(single_out)
    stream_tbl = pq.read_table(stream_out)
    assert stream_tbl.schema.equals(single_tbl.schema, check_metadata=False)
    assert stream_tbl.to_pylist() == single_tbl.to_pylist()
    assert stream_tbl.num_rows == 5  # 6 rows - 1 duplicate ("2020-DUP", "2020-01-02")

    # First page's source_sha256 wins even when duplicate straddles Parquet chunks.
    import hashlib

    by_key = {(r["document_number"], r["publication_date"]): r for r in stream_tbl.to_pylist()}
    assert by_key[("2020-DUP", "2020-01-02")]["source_sha256"] == "sha256:" + hashlib.sha256(raw_p1).hexdigest()
    assert ("2020-MULTI", "2020-01-03") in by_key and ("2020-MULTI", "2020-01-05") in by_key

    # Conflicting same-date observations straddling chunk boundaries still refuse before replacing output.
    monkeypatch.setattr(pq.ParquetWriter, "write_table", real_write_table)
    conflict_dir = tmp_path / "conflict"
    conflict_dir.mkdir()
    conflict_records = [
        dict(_RAW_DOC, document_number="2020-A", publication_date="2020-01-01", title="Original"),
        dict(_RAW_DOC, document_number="2020-B", publication_date="2020-01-02"),
        dict(_RAW_DOC, document_number="2020-A", publication_date="2020-01-01", title="Conflicting"),
    ]
    with pytest.raises(ValueError, match="conflicting.*same.*date"):
        build_federal_register(conflict_dir, batch_size=2, documents=lambda s: iter(conflict_records))
    assert not (conflict_dir / "federal_register.parquet").exists()
    assert not (conflict_dir / "_fr_new.parquet").exists()

    # Empty stream produces a valid 0-row Parquet with exact schema.
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    empty_out = build_federal_register(empty_dir, batch_size=2, documents=lambda s: iter(()))
    assert pq.read_table(empty_out).num_rows == 0
    assert pq.read_schema(empty_out).names == list(COLUMNS)


def test_polite_pacing_sleeps_between_pages_not_before_first(tmp_path, monkeypatch):
    """``request_interval_seconds`` paces consecutive API fetches without delaying the first request."""
    import time
    from contextlib import contextmanager

    from spicy_docs.transport import acquisition

    from spicy_regs.transforms.build_federal_register import _live_federal_register_documents

    next_2 = (
        "https://www.federalregister.gov/api/v1/documents?"
        "conditions%5Bpublication_date%5D%5Bgte%5D=2020-01-01&conditions%5Bpublication_date%5D%5Blte%5D=2020-01-03"
        "&format=json&order=newest&page=2&per_page=1000"
    )
    next_3 = (
        "https://www.federalregister.gov/api/v1/documents?"
        "conditions%5Bpublication_date%5D%5Bgte%5D=2020-01-01&conditions%5Bpublication_date%5D%5Blte%5D=2020-01-03"
        "&format=json&order=newest&page=3&per_page=1000"
    )
    pages = {
        None: json.dumps({"count": 3, "total_pages": 3, "next_page_url": next_2, "results": [dict(_RAW_DOC, document_number="2020-1", publication_date="2020-01-01")]}).encode(),
        next_2: json.dumps({"count": 3, "total_pages": 3, "next_page_url": next_3, "results": [dict(_RAW_DOC, document_number="2020-2", publication_date="2020-01-02")]}).encode(),
        next_3: json.dumps({"count": 3, "total_pages": 3, "results": [dict(_RAW_DOC, document_number="2020-3", publication_date="2020-01-03")]}).encode(),
    }

    now = [100.0]
    sleeps: list[float] = []

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        now[0] += seconds

    @contextmanager
    def fetcher(_):
        def _get(url: str) -> bytes:
            return pages[url] if url in pages else pages[None]

        yield _get

    monkeypatch.setattr(acquisition, "federal_register_fetcher", fetcher)
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw-source-originals"))

    # 1. Explicit clock/sleep hooks: 3 instant pages -> 2 pauses of 0.25s, 0 before page 1.
    out_a = tmp_path / "a"
    out_a.mkdir()
    got = list(
        _live_federal_register_documents(
            date(2020, 1, 1),
            until=date(2020, 1, 3),
            request_interval_seconds=0.25,
            output_dir=out_a,
            sleep=fake_sleep,
            monotonic=lambda: now[0],
        )
    )
    assert len(got) == 3
    assert sleeps == [pytest.approx(0.25), pytest.approx(0.25)]

    # 2. Monkeypatching stdlib time.sleep/time.monotonic works through build_federal_register too.
    sleeps.clear()
    monkeypatch.setattr(time, "sleep", fake_sleep)
    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    out_b = tmp_path / "b"
    out_b.mkdir()
    build_federal_register(out_b, since=date(2020, 1, 1), until=date(2020, 1, 3), request_interval_seconds=0.25)
    assert sleeps == [pytest.approx(0.25), pytest.approx(0.25)]

    # 3. Interval of 0 never sleeps.
    sleeps.clear()
    out_c = tmp_path / "c"
    out_c.mkdir()
    build_federal_register(out_c, since=date(2020, 1, 1), until=date(2020, 1, 3), request_interval_seconds=0.0)
    assert sleeps == []
