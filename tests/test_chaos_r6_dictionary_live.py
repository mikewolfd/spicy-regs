"""Live checks of the round-6 dictionary sentences against the publisher the output ledger names (implementer B).

Integration tier (network), run locally: ``uv run --frozen pytest -m integration
tests/test_chaos_r6_dictionary_live.py``. Each test reads the sentence it holds to the data, so a rewrite that drops
the claim fails here as well as hermetically. Each states what it cannot see.
"""

from __future__ import annotations

import duckdb
import pytest

from scripts import check_status_constants as constants
from spicy_regs import data_dictionary as dd
from spicy_regs import output_ledger
from spicy_regs.duckdb_settings import load_public_http
from spicy_regs.sources import publication
from tests.test_chaos_r6_dictionary import MAP_TIME_FILING_LINK

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def urls() -> dict[str, list[str]]:
    return publication.published_urls(output_ledger.ledger_destination(output_ledger.LEDGER.read_text(encoding="utf-8")))


@pytest.fixture(scope="module")
def con() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    load_public_http(connection)
    return connection


def _scan(urls: dict[str, list[str]], table: str) -> str:
    return publication.parquet_scan(urls[table])


def _text(table: str, field: str = "data_quality") -> str:
    return " ".join((dd.load_descriptions()[table].get(field) or "").split())


def test_a_lobbyist_id_on_a_second_registrants_filing_is_confined_to_2020_2022(urls, con):
    """The registrant comes from the filing, not the lobbyist row. Cannot see: a person filed under one id by two
    registrants in a year when each also used its own id, which this counts once."""
    assert "all on filings of 2020-2022" in " ".join(dd.load_descriptions()["lobbying_activity_lobbyists"]["columns"]
                                                    ["lobbyist_id"].split())
    first, last = con.execute(f"""
        WITH r AS (SELECT l.lobbyist_id, f.registrant_id, f.filing_year
                   FROM {_scan(urls, 'lobbying_activity_lobbyists')} l JOIN {_scan(urls, 'lobbying_filings')} f
                   USING (filing_uuid) WHERE l.lobbyist_id IS NOT NULL),
             per AS (SELECT lobbyist_id, registrant_id, count(*) n, min(filing_year) y0, max(filing_year) y1
                     FROM r GROUP BY ALL),
             minor AS (SELECT * FROM per WHERE lobbyist_id IN (SELECT lobbyist_id FROM per GROUP BY 1 HAVING count(*) > 1)
                       QUALIFY row_number() OVER (PARTITION BY lobbyist_id ORDER BY n DESC, registrant_id) > 1)
        SELECT min(y0), max(y1) FROM minor""").fetchone()
    assert (first, last) == ("2020", "2022")


def test_filing_link_status_is_unresolved_with_no_filing_key_on_every_row_it_says(urls, con):
    """Read from the footers, not the rows. Cannot see: a row group without statistics (then the test fails)."""
    for table in MAP_TIME_FILING_LINK:
        stats = constants.footer_values(con, urls[table], ["filing_link_status", "filing_key"])
        link, key = stats["filing_link_status"], stats["filing_key"]
        assert link["complete"] and (link["min"], link["max"], link["nulls"]) == ("unresolved", "unresolved", 0), table
        assert key["nulls"] == key["values"], table


def test_each_original_filing_expenditure_has_exactly_one_cover_and_the_bulk_file_none(urls, con):
    """The summary's cover route. Cannot see: whether the cover's form type is the filing's latest version."""
    assert "`reported-form`" in _text("fec_independent_expenditures", "summary")
    rows = con.execute(f"""
        WITH covers AS (SELECT collection_id, count(*) n FROM {_scan(urls, 'fec_filing_report_observations')}
                        WHERE report_record_role = 'reported-form' GROUP BY 1)
        SELECT e.source_namespace, count(DISTINCT e.collection_id), count(DISTINCT e.collection_id) FILTER (WHERE c.n = 1),
               count(DISTINCT e.collection_id) FILTER (WHERE c.n IS NULL), count(e.source_cycle)
        FROM {_scan(urls, 'fec_independent_expenditures')} e LEFT JOIN covers c USING (collection_id) GROUP BY 1
        ORDER BY 1""").fetchall()
    bulk, electronic = rows
    assert bulk[0] == "fec-bulk-independent-expenditure-csv" and bulk[3] == bulk[1]
    assert electronic[0] == "fec-electronic-filing-schedule" and electronic[2] == electronic[1] and electronic[4] == 0


def test_senate_meetings_list_no_witnesses_while_their_printed_hearings_do(urls, con):
    """Cannot see: whether a Senate meeting with no printed hearing had witnesses at all."""
    assert "`hearing_transcripts.witnesses_json`" in _text("committee_meetings")
    conditions = {}
    for table in ("committee_meetings", "hearing_transcripts"):
        columns = {row[0]: row[1] for row in con.execute(f"DESCRIBE SELECT * FROM {_scan(urls, table)}").fetchall()}
        conditions[table] = _stated_witnesses(table, columns)
    meetings, transcripts = con.execute(f"""
        SELECT (SELECT count(*) FROM {_scan(urls, 'committee_meetings')} WHERE chamber IN ('senate', 'nochamber')
                AND {conditions['committee_meetings']}),
               (SELECT count(*) FROM {_scan(urls, 'hearing_transcripts')} WHERE chamber = 'senate'
                AND {conditions['hearing_transcripts']})""").fetchone()
    assert meetings == 0 and transcripts > 0


def _stated_witnesses(table, columns):
    """Count witnesses using exactly one declared native or legacy representation."""
    from spicy_regs.native_types import described_schema
    from spicy_regs.subject_catalog import policies

    native_type = dict(described_schema(policies()[table].subject_schema))["witnesses"]
    declared = {"witnesses": native_type, "witnesses_json": "VARCHAR"}
    present = set(declared) & columns.keys()
    if len(present) != 1:
        raise ValueError(f"{table}: expected one witness representation, found {sorted(present)}")
    column = present.pop()
    if columns[column] != declared[column]:
        raise ValueError(f"{table}: {column} has undeclared type {columns[column]}")
    return ("witnesses IS NOT NULL AND len(witnesses) > 0" if column == "witnesses"
            else "witnesses_json IS NOT NULL AND witnesses_json <> '[]'")


def test_table_iii_at_release_point_119_73_holds_no_record_of_119_70(urls, con):
    """A pinned measurement: a new release point fails this test, so the sentence is re-measured, not left standing."""
    assert "119-70" in _text("table3_records")
    points, held, classified = con.execute(f"""
        SELECT (SELECT list(DISTINCT release_point) FROM {_scan(urls, 'table3_records')}),
               (SELECT count(*) FROM {_scan(urls, 'table3_records')} WHERE act_key = '119-70'),
               (SELECT count(*) FROM {_scan(urls, 'law_code_sections')} WHERE law_id = '119-public-70')""").fetchone()
    assert points == ["119-73"] and held == 0 and classified > 0


def test_gaos_annual_reports_are_held_without_their_figures_and_its_label_sits_on_reports(urls, con):
    assert "their figures are not held" in _text("gao_decisions")
    reports, stating_figures = con.execute(f"""
        SELECT count(*), count(*) FILTER (WHERE abstract ILIKE '%sustain%' OR abstract ILIKE '%cases filed%')
        FROM {_scan(urls, 'gao_reports')} WHERE title LIKE 'GAO Bid Protest Annual Report to Congress for Fiscal Year%'
        """).fetchone()
    assert reports > 0 and stating_figures == 0
    labels = con.execute(f"""SELECT list(DISTINCT decision_type) FROM {_scan(urls, 'gao_decisions')}
                             WHERE decision_number IN ('B-423717', 'B-401197')""").fetchone()[0]
    assert labels == ["Bid Protest Decision"]
