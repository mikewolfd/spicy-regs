"""Hermetic tests for the Federal Register ingest (no network).

Covers the raw-doc → published-schema mapping (``_shape``), the merge
semantics of ``build_federal_register``, and the windowing contract the
transform relies on from the SpicyDocs owner (``iter_federal_register_pages``).
The owner's own tests pin its cap-split, single-day refusal and retry
semantics.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pytest

from spicy_regs.transforms.build_federal_register import COLUMNS, FETCHED_COLUMNS, _shape, build_federal_register

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
    row = _shape(_RAW_DOC)
    # Every fetched column present, and nothing extra; ``rin`` is derived in the merge.
    assert set(row) == set(FETCHED_COLUMNS)
    assert COLUMNS == (*FETCHED_COLUMNS, "rin")


def _collision_documents():
    return json.loads((Path(__file__).parent / "fixtures/federal_register/00-111.json").read_text())


def test_dated_native_records_survive_and_same_date_correction_replaces(tmp_path):
    import shutil

    import pyarrow.parquet as pq

    documents = _collision_documents()
    out = build_federal_register(
        tmp_path, documents=lambda start: iter(documents), download_prior=lambda key, path: False
    )
    first = pq.read_table(out).to_pylist()
    assert {(r["document_number"], r["publication_date"]) for r in first} == {
        ("00-111", "2000-01-14"),
        ("00-111", "2000-01-18"),
    }
    # The correction is a controlled edit to one captured record, not a claim
    # that the publisher changed its title. The date-distinct record is untouched.
    correction = dict(documents[0], title="Corrected housing-credit title")
    shutil.copyfile(out, tmp_path / "_fr_prior.parquet")
    build_federal_register(tmp_path, documents=lambda start: iter([correction]), download_prior=lambda key, path: False)
    corrected = pq.read_table(out).to_pylist()
    assert len(corrected) == 2
    by_date = {r["publication_date"]: r for r in corrected}
    assert by_date["2000-01-14"]["title"] == correction["title"]
    assert by_date["2000-01-18"] == next(r for r in first if r["publication_date"] == "2000-01-18")
    shutil.copyfile(out, tmp_path / "_fr_prior.parquet")
    build_federal_register(tmp_path, documents=lambda start: iter([correction]), download_prior=lambda key, path: False)
    assert pq.read_table(out).to_pylist() == corrected


def test_unreviewed_number_collision_is_not_a_special_case(tmp_path):
    import pyarrow.parquet as pq

    records = [dict(_RAW_DOC, publication_date=day) for day in ("2024-03-01", "2024-03-02")]
    out = build_federal_register(
        tmp_path, documents=lambda start: iter(records), download_prior=lambda key, path: False
    )
    assert pq.read_table(out).num_rows == 2


def test_identical_repeated_observations_collapse_within_the_dated_key(tmp_path):
    import pyarrow.parquet as pq

    out = build_federal_register(
        tmp_path, documents=lambda start: iter([_RAW_DOC, _RAW_DOC]), download_prior=lambda key, path: False
    )
    assert pq.read_table(out).num_rows == 1


def test_conflicting_same_date_fetch_refuses_before_output(tmp_path):
    records = [_RAW_DOC, dict(_RAW_DOC, title="Conflicting same-run observation")]
    out = tmp_path / "federal_register.parquet"
    out.write_bytes(b"previous output remains untouched")
    with pytest.raises(ValueError, match="conflicting.*same.*date"):
        build_federal_register(tmp_path, documents=lambda start: iter(records), download_prior=lambda key, path: False)
    assert out.read_bytes() == b"previous output remains untouched"


@pytest.mark.parametrize("date_value", [None, "", "2000-13-01", "20000114"])
def test_unknown_or_noncanonical_date_refuses_before_output(tmp_path, date_value):
    with pytest.raises(ValueError, match="identity"):
        build_federal_register(
            tmp_path,
            documents=lambda start: iter([dict(_RAW_DOC, publication_date=date_value)]),
            download_prior=lambda key, path: False,
        )
    assert not (tmp_path / "federal_register.parquet").exists()


def test_shape_maps_and_serializes_fields():
    row = _shape(_RAW_DOC)
    assert row["document_number"] == "2024-00001"
    assert row["document_type"] == "Proposed Rule"  # API `type` -> document_type
    # Array fields serialize to JSON strings.
    assert json.loads(row["docket_ids_json"]) == ["EPA-HQ-OAR-2024-0001", "FRL-1234-01-OAR"]
    assert json.loads(row["regulation_id_numbers_json"]) == ["2060-AV12"]
    assert json.loads(row["cfr_references_json"])[0]["part"] == 60
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
    row = _shape({"document_number": "x"})
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


@pytest.mark.parametrize("prior_width", ["fetched", "published"], ids=["migration-prior", "steady-state-prior"])
def test_rin_is_derived_for_every_row_whatever_the_priors_width(tmp_path, prior_width):
    """The join key is a projection of the array, filled on prior rows and fresh rows alike.

    Two priors: the pre-``rin`` width the live table had on 2026-09-19 (the
    migration case), and the published width carrying a ``rin`` that disagrees
    with its array (the steady-state case), which must be recomputed rather
    than read back -- the column is derived every run, never stored as a fact.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    columns = FETCHED_COLUMNS if prior_width == "fetched" else COLUMNS
    prior_rows = [
        {
            "document_number": "P-two",
            "publication_date": "2024-01-01",
            "regulation_id_numbers_json": '["1111-AA11", "2222-BB22"]',
        },
        {"document_number": "P-none", "publication_date": "2024-01-01", "regulation_id_numbers_json": "[]"},
        {"document_number": "P-null", "publication_date": "2024-01-01", "regulation_id_numbers_json": None},
    ]
    if prior_width == "published":
        for row in prior_rows:
            row["rin"] = "9999-ZZ99"  # stale: must not survive the merge
    schema = pa.schema([(c, pa.string()) for c in columns])
    pq.write_table(
        pa.Table.from_pylist([{c: None for c in columns} | r for r in prior_rows], schema=schema),
        tmp_path / "_fr_prior.parquet",
    )
    fresh = [dict(_RAW_DOC, document_number="F-one", publication_date="2024-03-01")]

    out = build_federal_register(
        tmp_path, since=date(2024, 3, 1), documents=lambda start: iter(fresh), download_prior=lambda k, p: False
    )
    table = pq.read_table(out)
    assert table.schema.names == list(COLUMNS)
    by_number = {row["document_number"]: row for row in table.to_pylist()}
    assert by_number["F-one"]["rin"] == "2060-AV12"
    assert by_number["P-two"]["rin"] == "1111-AA11", "the first of several; the array keeps the rest"
    assert by_number["P-none"]["rin"] is None
    assert by_number["P-null"]["rin"] is None
