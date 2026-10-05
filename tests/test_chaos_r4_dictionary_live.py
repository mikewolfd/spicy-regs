"""Live checks of the round-4 dictionary claims against the publisher the output ledger names (scout A).

Integration tier (network), run locally: ``uv run --frozen pytest -m integration
tests/test_chaos_r4_dictionary_live.py``. Each claim test reads the sentence the
dictionary states and holds it to the published data through a source other than
the column the sentence describes. Each states what it cannot see. The
"always NULL" checks are regression guards: they compare the data with prose the
same pipeline wrote, so they catch drift, not a wrong design.
"""

from __future__ import annotations

import re

import duckdb
import pytest

from spicy_regs import data_dictionary as dd
from spicy_regs import output_ledger
from spicy_regs.duckdb_settings import load_public_http
from spicy_regs.sources import publication

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


def _column(table: str, column: str) -> str:
    return dd.load_descriptions()[table]["columns"][column]


def test_w3_the_enactment_date_is_approved_date_or_signed_date_and_the_latest_action_never_precedes_it(urls, con):
    """`laws.latest_action_date` points enactment questions at `approved_date`, else `congress_bills.signed_date`.

    Two publisher records are compared: `approved_date` is the PLAW USLM file's (GovInfo), `signed_date` the
    coded became-law action of the bill's BILLSTATUS document. `latest_action_date`, the law list route's latest
    action, may follow enactment (119-public-68) but never precede it. Cannot see: a law whose PLAW is uncaptured
    and whose bill has no coded became-law action, where neither date exists.
    """
    # The sentence is the contract's own text for the column since the 0.54.0 build (the interim note that carried
    # it in data_quality left with that build).
    assert ("enactment date read approved_date where the PLAW was captured, else congress_bills.signed_date through "
            "bill_id") in _column("laws", "latest_action_date")
    disagree, undated_public, earlier = con.execute(f"""
        SELECT count(*) FILTER (WHERE l.approved_date IS NOT NULL AND b.signed_date IS NOT NULL
                                AND l.approved_date <> b.signed_date),
               count(*) FILTER (WHERE l.law_type = 'public' AND coalesce(l.approved_date, b.signed_date) IS NULL),
               count(*) FILTER (WHERE l.approved_date IS NOT NULL AND l.latest_action_date < l.approved_date)
        FROM {_scan(urls, 'laws')} l LEFT JOIN {_scan(urls, 'congress_bills')} b USING (bill_id)""").fetchone()
    assert (disagree, undated_public, earlier) == (0, 0, 0)


def test_w4_one_supreme_court_decision_can_be_several_clusters(urls, con):
    """`cluster_id` is one CourtListener record of a decision, not the decision.

    Groups Supreme Court clusters by the heuristic the dictionary names, `cl_docket_id` plus a U.S. Reports
    citation from `court_citations` (a separate publisher export), and requires a split decision to exist, with
    none of its clusters carrying `scdb_id`, the publisher's decision key, which the dictionary says is unset
    after 2019. Cannot see: a decision with no reporter citation yet, whose later print is a second cluster the
    grouping cannot pair, so it undercounts splits.
    """
    assert "not per decision" in _column("court_opinion_clusters", "cluster_id")
    assert "heuristic" in _column("court_opinion_clusters", "cluster_id")
    split, with_scdb = con.execute(f"""
        WITH c AS (SELECT cluster_id, volume, page FROM {_scan(urls, 'court_citations')} WHERE reporter = 'U.S.'),
             k AS (SELECT cluster_id, cl_docket_id, scdb_id FROM {_scan(urls, 'court_opinion_clusters')}
                   WHERE court_id = 'scotus' AND date_filed >= '2020-01-01'),
             g AS (SELECT cl_docket_id, volume, page, count(DISTINCT cluster_id) AS clusters, count(scdb_id) AS scdb
                   FROM c JOIN k USING (cluster_id) GROUP BY ALL)
        SELECT count(*) FILTER (WHERE clusters > 1), coalesce(sum(scdb), 0) FROM g""").fetchone()
    assert split > 0
    assert with_scdb == 0, "CourtListener now sets scdb_id after 2019: name it as the key for those decisions"


#: A column's description stating the column holds nothing: NULL in every row, or `[]` in every row.
_ALWAYS = re.compile(r"\b[Aa]lways (NULL|`\[\]`)")


def _always_claims() -> list[tuple[str, str, str]]:
    claims = []
    for table, entry in dd.load_descriptions().items():
        for column, text in (entry.get("columns") or {}).items():
            if match := _ALWAYS.search(text or ""):
                claims.append((table, column, match.group(1)))
    return claims


def test_the_always_claims_are_found():
    """The regression guard below sees every such sentence, so a reworded one cannot drop out of it unseen.

    It found ten on 2026-10-03. The native schemas (2026-10-04) dropped eight of those columns, seven lineage
    ``supersedes_id`` and ``hearing_transcripts.bill_id``, and with them their sentences; these two remain.
    """
    assert {("proceedings", "authority_refs", "`[]`"), ("federal_register", "modify_date", "NULL")} <= set(
        _always_claims()
    )


@pytest.mark.parametrize(("table", "column", "value"), _always_claims())
def test_a_column_described_as_always_empty_is_empty(urls, con, table, column, value):
    """Regression guard: the published column holds what its prose says it always holds."""
    scan = _scan(urls, table)
    condition = _empty_condition(con, scan, table, column, value)
    (found,) = con.execute(f"SELECT count(*) FROM {scan} WHERE {condition}").fetchone()
    assert found == 0, f"{table}.{column}: {found} rows contradict 'always {value}'"


def _empty_condition(con, scan, table, column, value):
    """Check the same claim on the published schema using only the maintained field mapping."""
    names = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {scan}").fetchall()}
    if column not in names:
        from spicy_regs.transforms.regulations_shape import NATIVE_FIELDS

        originals = [source for source, (native, _) in NATIVE_FIELDS.get(table, {}).items()
                     if native == column and source in names]
        if len(originals) != 1:
            raise ValueError(f"{table}.{column}: no published column or declared source field for the claim")
        column = originals[0]
    quoted = '"' + column.replace('"', '""') + '"'
    return f"{quoted} IS NOT NULL" if value == "NULL" else f"{quoted} IS DISTINCT FROM '[]'"
