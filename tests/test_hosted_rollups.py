"""Wiring tests for the rollups that publish the contract-hosted tables.

Nothing here runs a rollup: these assert the wiring that is easy to get subtly
wrong and that no other test would catch until a cron failed at 02:00 — the
declared outputs, the console scripts, the workflows, and the rule that every
hosted table has exactly one writer.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from spicy_regs import data_dictionary as dd
from spicy_regs.pipelines.rollups.amendments import AmendmentsRollup
from spicy_regs.pipelines.rollups.base import RollupPipeline
from spicy_regs.pipelines.rollups.bill_family import BillFamilyRollup
from spicy_regs.pipelines.rollups.committee_reports import CommitteeReportsRollup
from spicy_regs.pipelines.rollups.congress_index import (
    CommitteeMeetingsRollup,
    HouseCommunicationsRollup,
    NominationsRollup,
    RecordIssuesRollup,
    TreatiesRollup,
)
from spicy_regs.pipelines.rollups.members import MembersRollup
from spicy_regs.pipelines.rollups.print_citations import PrintCitationsRollup
from spicy_regs.pipelines.rollups.press_releases import PressReleasesRollup
from spicy_regs.pipelines.rollups.roll_call_votes import RollCallVotesRollup
from spicy_regs.pipelines.rollups.senate_expenditures import SenateExpendituresRollup

# A8/A9 (laws and rosters)
from spicy_regs.pipelines.rollups.committee_rosters import CommitteeRostersRollup
from spicy_regs.pipelines.rollups.laws import LawsRollup
from spicy_regs.transforms.build_bill_family import (
    ARCHIVE_COLUMNS,
    ARCHIVES_TABLE,
    BACKFILL_COLUMNS,
    BACKFILL_WALK_COLUMNS,
    BACKFILL_WALKS_TABLE,
    BACKFILLS_TABLE,
    VOTE_REFERENCE_COLUMNS,
    VOTE_REFERENCES_TABLE,
)

#: The bill family's published outputs that are not contract tables, each with
#: the column tuple its transform writes.
from spicy_regs.transforms.committee_report_reads import READS_TABLE, READ_COLUMNS

BILL_OWN_TABLES = {
    ARCHIVES_TABLE: ARCHIVE_COLUMNS,
    VOTE_REFERENCES_TABLE: VOTE_REFERENCE_COLUMNS,
    BACKFILLS_TABLE: BACKFILL_COLUMNS,
    BACKFILL_WALKS_TABLE: BACKFILL_WALK_COLUMNS,
}

OWN_TABLES = BILL_OWN_TABLES | {READS_TABLE: READ_COLUMNS}

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = REPO_ROOT / ".github" / "workflows"

HOSTED_ROLLUPS = (
    BillFamilyRollup,
    PressReleasesRollup,
    AmendmentsRollup,
    RollCallVotesRollup,
    MembersRollup,
    CommitteeReportsRollup,
    # A8/A9 (laws and rosters)
    LawsRollup,
    CommitteeRostersRollup,
    # The Congress.gov index tables (gaps A5, A7, A10).
    HouseCommunicationsRollup,
    CommitteeMeetingsRollup,
    RecordIssuesRollup,
    TreatiesRollup,
    NominationsRollup,
    # The two PDF-only families (spicy-docs 0.23.0), one rollup per
    # acquisition pass.
    PrintCitationsRollup,
    SenateExpendituresRollup,
)


def _declared_keys(rollup) -> tuple[str, ...]:
    return rollup.outputs or (rollup.output,)


@pytest.mark.parametrize("rollup", HOSTED_ROLLUPS, ids=lambda r: r.name)
def test_every_output_is_a_published_table(rollup):
    for key in _declared_keys(rollup):
        assert key.endswith(".parquet")
        assert key.removesuffix(".parquet") in dd.TABLES, key


@pytest.mark.parametrize("rollup", HOSTED_ROLLUPS, ids=lambda r: r.name)
def test_an_ingesting_rollup_primes_no_base_table(rollup):
    """These fetch from a publisher, so nothing is primed and nothing is fatal on absence.

    This says only that the rollup declares no hard ``inputs``. It deliberately
    does **not** stand in for "reads nothing else": two of these do read a
    published table at merge time, and they satisfy an ``inputs == ()``
    assertion by construction while doing it. What makes those reads safe is
    checked in ``test_a_soft_input_is_an_ingest_output_its_writer_produces_first``,
    against the ``soft_inputs`` they must declare.
    """
    assert rollup.inputs == ()


def _cron_minutes(workflow: Path) -> int:
    """The daily minute-of-day the workflow's schedule fires at."""
    crons = re.findall(r"cron: '(\d+) (\d+) \* \* \*'", workflow.read_text())
    assert len(crons) == 1, f"{workflow.name} declares {len(crons)} daily crons, expected 1"
    minute, hour = (int(part) for part in crons[0])
    return hour * 60 + minute


def _writers_of(remote_key: str) -> list[type[RollupPipeline]]:
    return [rollup for rollup in HOSTED_ROLLUPS if remote_key in (rollup.outputs or (rollup.output,))]


@pytest.mark.parametrize(
    ("rollup", "soft_input"),
    [(rollup, key) for rollup in HOSTED_ROLLUPS for key in rollup.soft_inputs],
    ids=lambda value: value if isinstance(value, str) else value.name,
)
def test_a_soft_input_is_an_ingest_output_its_writer_produces_first(rollup, soft_input):
    """The two things that make a merge-time read of a published table safe.

    The rollup contract (``pipelines/rollups/base.py``) permits reading an
    *ingest* rollup's output — one with no upstream dependency inside this
    repository, so a stale copy costs one cron's lag rather than racing — and
    forbids reading a derived rollup's. Both halves are checked here rather
    than asserted in prose: the writer ingests (declares no ``inputs``), and
    this rollup's cron **fires after** the writer's on the same day.

    Start order is all this asserts, and all a cron can give. It is not a
    guarantee that the writer has *finished*: the bill family's workflow
    budgets 180 minutes and these readers start 20 and 60 minutes after it, so
    a long family run is still writing when they begin, and they read the
    previous run's output. That is the ordering preference the rollup contract
    describes, not a barrier — which is exactly why both reads are
    ``soft_inputs``, tolerant of an absent or older table, rather than
    ``inputs``.
    """
    writers = _writers_of(soft_input)
    assert writers, f"{soft_input} is declared a soft input but no rollup publishes it"

    reader_at = _cron_minutes(WORKFLOWS / f"rollup-{rollup.name}.yml")
    earlier = []
    for writer in writers:
        assert writer.inputs == (), (
            f"{soft_input} is written by {writer.name}, which reads base tables — "
            "a derived rollup's output may not be a soft input"
        )
        writer_at = _cron_minutes(WORKFLOWS / f"rollup-{writer.name}.yml")
        if writer_at < reader_at:
            earlier.append((writer.name, reader_at - writer_at))

    assert earlier, (
        f"rollup-{rollup.name} reads {soft_input} but every writer of it "
        f"({[w.name for w in writers]}) fires later in the day, so it always reads yesterday's"
    )


def test_the_bill_family_declares_all_fourteen_plus_its_own_four():
    assert len(BillFamilyRollup.outputs) == 18
    assert BillFamilyRollup.outputs[0] == "congress_bills.parquet"
    assert set(BillFamilyRollup.outputs[-4:]) == {f"{name}.parquet" for name in BILL_OWN_TABLES}
    # The property the freshness checker uses resolves to the first key.
    assert BillFamilyRollup(output_dir=None).output == "congress_bills.parquet"


@pytest.mark.parametrize("table", sorted(OWN_TABLES), ids=str)
def test_a_non_contract_output_is_declared_once_in_each_place_that_needs_it(table):
    """Their columns are this repository's, so three lists state them and none may drift.

    The transform owns the tuple it writes; ``DERIVED_SCHEMAS`` is what the
    dictionary reconciles descriptions against; the MCP server lists it
    literally because it must stay installable without the source-readers
    group. Same shape as ``test_mcp_server_tables_match_dictionary``.
    """
    from spicy_regs import mcp_server

    assert [column for column, _ in dd.DERIVED_SCHEMAS[table]] == list(OWN_TABLES[table])
    assert all(kind == "VARCHAR" for _, kind in dd.DERIVED_SCHEMAS[table])
    assert table in dd.TABLES
    assert table in mcp_server.TABLES
    assert table not in dd.CONTRACT_TABLES, "it is this repository's own table, not a hosted contract"


def test_every_hosted_table_has_exactly_one_writer():
    """Two rollups writing one table would overwrite each other's columns.

    ``congress_bills`` was the exception until plan A1 retired its archive-wide
    list writer (decision 31): its run of 2026-09-23 replaced 3,033 BILLSTATUS
    ``update_date`` instants with same-day dates and 3,095 congress.gov page
    URLs with API resource URLs.
    """
    written: dict[str, list[str]] = {}
    for rollup in HOSTED_ROLLUPS:
        for key in _declared_keys(rollup):
            written.setdefault(key.removesuffix(".parquet"), []).append(rollup.name)

    assert set(written) == set(dd.CONTRACT_TABLES) | set(OWN_TABLES), (
        "every contract, plus the bill family's own two tables, must be published by exactly one rollup"
    )
    doubled = {table: names for table, names in written.items() if len(names) > 1}
    assert not doubled, f"tables with more than one writer: {doubled}"


def test_update_date_keeps_the_larger_value(tmp_path):
    """The contract's sentence for ``update_date``, as the merge applies it.

    Rows from the drift audit of 2026-09-23 (``drift-audit-2026-09-23/bill-family``):
    the retired list writer left ``119-hconres-100`` with the list route's
    same-day date and ``119-hr-10432`` with a later day than BILLSTATUS then
    stated. A BILLSTATUS re-read restores the first's instant, which sorts after
    the date; a re-read stating the older stamp never moves the second back.
    """
    from spicy_docs.schemas import TABLE_CONTRACTS

    from spicy_regs.transforms.table_merge import merge_contract_table, prior_scratch_path

    contract = TABLE_CONTRACTS["congress_bills"]

    def row(bill, update_date):
        return {c: None for c in contract.columns} | {"bill_id": bill, "congress": "119", "update_date": update_date}

    schema = pa.schema([(c, pa.string()) for c in contract.columns])
    prior = [row("119-hconres-100", "2026-09-19"), row("119-hr-10432", "2026-09-22"), row("119-hr-1", "2026-05-08")]
    pq.write_table(pa.Table.from_pylist(prior, schema=schema), prior_scratch_path(tmp_path, "congress_bills"))
    fresh = [row("119-hconres-100", "2026-09-19T23:35:29Z"), row("119-hr-10432", "2026-09-18T05:23:22Z")]
    out = merge_contract_table(tmp_path, "congress_bills", fresh, download_prior=lambda *_: False, prior_present=True)
    got = {r["bill_id"]: r["update_date"] for r in pq.read_table(out).to_pylist()}
    assert got == {
        "119-hconres-100": "2026-09-19T23:35:29Z",
        "119-hr-10432": "2026-09-22",
        "119-hr-1": "2026-05-08",
    }


def test_url_source_follows_the_url_the_merge_keeps(tmp_path):
    """Decision 1: an inherited url carries the label ``inherited``; a stated one carries its writer's."""
    from spicy_docs.schemas import TABLE_CONTRACTS

    from spicy_regs.transforms.table_merge import merge_contract_table, prior_scratch_path

    contract = TABLE_CONTRACTS["congress_bills"]

    def row(bill, **values):
        base = {c: None for c in contract.columns}
        number = bill.split("-")[-1]
        base.update(bill_id=bill, congress="118", bill_type="hr", bill_number=number, update_date="2026-01-01")
        return base | values

    api = "https://api.congress.gov/v3/bill/118/hr/{}?format=json"
    prior = [
        row("118-hr-1", url=api.format(1), url_source="congress_api_list"),  # fresh states none: inherited
        row("118-hr-2", url=api.format(2), url_source="congress_api_list"),  # fresh states its own
        row("118-hr-3", url=api.format(3), url_source="congress_api_list"),  # untouched: keeps its label
        row("118-hr-4", url=api.format(4)),  # predates the label: stays NULL when untouched
    ]
    schema = pa.schema([(c, pa.string()) for c in contract.columns])
    pq.write_table(pa.Table.from_pylist(prior, schema=schema), prior_scratch_path(tmp_path, "congress_bills"))
    page = "https://www.congress.gov/bill/118th-congress/house-bill/2"
    fresh = [
        row("118-hr-1", update_date="2026-02-01"),
        row("118-hr-2", update_date="2026-02-01", url=page, url_source="billstatus"),
        row("118-hr-5", update_date="2026-02-01"),  # no url anywhere: no label
    ]
    out = merge_contract_table(tmp_path, "congress_bills", fresh, download_prior=lambda *_: False, prior_present=True)
    got = {r["bill_id"]: (r["url"], r["url_source"]) for r in pq.read_table(out).to_pylist()}
    assert got == {
        "118-hr-1": (api.format(1), "inherited"),
        "118-hr-2": (page, "billstatus"),
        "118-hr-3": (api.format(3), "congress_api_list"),
        "118-hr-4": (api.format(4), None),
        "118-hr-5": (None, None),
    }


def test_only_congress_bills_merges_column_wise():
    """Coalescing is for rows from sources stating different columns; elsewhere a NULL is a value."""
    from spicy_regs.transforms.table_merge import COALESCED_TABLES

    assert COALESCED_TABLES == {"congress_bills"}


@pytest.mark.parametrize("rollup", HOSTED_ROLLUPS, ids=lambda r: r.name)
def test_each_rollup_has_a_console_script_and_a_workflow(rollup):
    scripts = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())["project"]["scripts"]
    command = f"run-rollup-{rollup.name}"
    assert command in scripts, f"{command} is not registered in pyproject.toml"

    workflow = WORKFLOWS / f"rollup-{rollup.name}.yml"
    assert workflow.exists(), f"missing {workflow.name}"
    document = yaml.safe_load(workflow.read_text())
    assert document["jobs"]["run"]["with"]["command"] == command
    assert document["jobs"]["run"]["uses"] == "./.github/workflows/_rollup.yml"


@pytest.mark.parametrize("rollup", HOSTED_ROLLUPS, ids=lambda r: r.name)
def test_each_workflow_has_its_own_cron_slot(rollup):
    workflow = WORKFLOWS / f"rollup-{rollup.name}.yml"
    text = workflow.read_text()
    assert re.search(r"cron: '[\d*/ ,-]+'", text), f"{workflow.name} declares no cron"


def test_no_two_rollup_workflows_share_a_cron_minute():
    """Spacing keeps six jobs from contending for a runner at the same instant."""
    slots: dict[str, list[str]] = {}
    for workflow in WORKFLOWS.glob("rollup-*.yml"):
        for cron in re.findall(r"cron: '([^']+)'", workflow.read_text()):
            slots.setdefault(cron, []).append(workflow.name)
    new = {f"rollup-{rollup.name}.yml" for rollup in HOSTED_ROLLUPS}
    for cron, names in slots.items():
        if len(names) > 1 and new.intersection(names):
            pytest.fail(f"new rollup shares cron {cron!r} with {names}")


def test_the_reusable_workflow_declares_every_input_the_callers_pass():
    """A caller passing an input the reusable workflow has not declared fails at run time."""
    reusable = yaml.safe_load((WORKFLOWS / "_rollup.yml").read_text())
    declared = set(reusable[True]["workflow_call"]["inputs"])
    for rollup in HOSTED_ROLLUPS:
        passed = set(yaml.safe_load((WORKFLOWS / f"rollup-{rollup.name}.yml").read_text())["jobs"]["run"]["with"])
        assert passed <= declared, f"rollup-{rollup.name}.yml passes undeclared {sorted(passed - declared)}"
