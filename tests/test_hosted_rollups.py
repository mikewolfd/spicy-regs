"""Wiring tests for the six rollups that publish the contract-hosted tables.

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
from spicy_regs.pipelines.rollups.bill_family import BillFamilyRollup
from spicy_regs.pipelines.rollups.committee_reports import CommitteeReportsRollup
from spicy_regs.pipelines.rollups.members import MembersRollup
from spicy_regs.pipelines.rollups.press_releases import PressReleasesRollup
from spicy_regs.pipelines.rollups.roll_call_votes import RollCallVotesRollup
from spicy_regs.transforms.build_bill_family import (
    ARCHIVE_COLUMNS,
    ARCHIVES_TABLE,
    VOTE_REFERENCE_COLUMNS,
    VOTE_REFERENCES_TABLE,
)

#: The bill family's two published outputs that are not contract tables, each
#: with the column tuple its transform writes.
OWN_TABLES = {ARCHIVES_TABLE: ARCHIVE_COLUMNS, VOTE_REFERENCES_TABLE: VOTE_REFERENCE_COLUMNS}

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = REPO_ROOT / ".github" / "workflows"

HOSTED_ROLLUPS = (
    BillFamilyRollup,
    PressReleasesRollup,
    AmendmentsRollup,
    RollCallVotesRollup,
    MembersRollup,
    CommitteeReportsRollup,
)


def _declared_keys(rollup) -> tuple[str, ...]:
    return rollup.outputs or (rollup.output,)


@pytest.mark.parametrize("rollup", HOSTED_ROLLUPS, ids=lambda r: r.name)
def test_every_output_is_a_published_table(rollup):
    for key in _declared_keys(rollup):
        assert key.endswith(".parquet")
        assert key.removesuffix(".parquet") in dd.TABLES, key


@pytest.mark.parametrize("rollup", HOSTED_ROLLUPS, ids=lambda r: r.name)
def test_an_ingesting_rollup_reads_no_base_table(rollup):
    """These fetch from a publisher; reading another rollup's output would race it."""
    assert rollup.inputs == ()


def test_the_bill_family_declares_all_thirteen_plus_its_own_two():
    assert len(BillFamilyRollup.outputs) == 15
    assert BillFamilyRollup.outputs[0] == "congress_bills.parquet"
    assert set(BillFamilyRollup.outputs[-2:]) == {f"{name}.parquet" for name in OWN_TABLES}
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

    ``congress_bills`` is the one deliberate exception and is asserted
    separately below, with the reason.
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


def test_congress_bills_has_a_second_narrow_writer():
    """The known exception, asserted so it stays deliberate rather than drifting.

    ``congress-bills`` walks the whole archive for the ten frozen columns;
    ``bill-family`` fills all forty-eight for the Congresses it is scoped to.
    """
    from spicy_regs.pipelines.rollups.congress_bills import CongressBillsRollup

    assert CongressBillsRollup.output == "congress_bills.parquet"
    assert "congress_bills.parquet" in BillFamilyRollup.outputs


def test_the_narrow_writer_does_not_drop_the_familys_columns(tmp_path):
    """The behavioral pin for the two-writer case, not a statement about it.

    Before this was fixed, the narrow writer published at its own ten-column
    width, which *deleted* the other thirty-eight from every published row —
    and at realistic scale the result was 96.6% of the prior bytes, well above
    the R2 shrink guard's 0.5, so nothing refused it. This seeds a prior the
    bill family would have written, runs the narrow writer's own merge, and
    asserts the published schema is still the contract's and that a value only
    the family sets is still there.
    """
    from spicy_docs.schemas import TABLE_CONTRACTS

    from spicy_regs.transforms.build_congress_bills import NAME, _shape
    from spicy_regs.transforms.table_merge import merge_contract_table, prior_scratch_path

    contract = TABLE_CONTRACTS["congress_bills"]
    prior = {c: None for c in contract.columns}
    prior.update(
        {
            "bill_id": "119-hr-6028",
            "congress": "119",
            "bill_type": "hr",
            "bill_number": "6028",
            "title": "A bill",
            "update_date": "2026-09-01",
            "stage": "passed_house",
            "stage_rule": "house_passage",
            "money_bill_kind": "appropriations",
        }
    )
    pq.write_table(
        pa.Table.from_pylist([prior], schema=pa.schema([(c, pa.string()) for c in contract.columns])),
        prior_scratch_path(tmp_path, NAME),
    )

    # Rows exactly as the narrow writer shapes them: ten keys, no more.
    fresh = _shape(
        {
            "congress": 119,
            "type": "HR",
            "number": 6028,
            "title": "A bill (updated)",
            "originChamber": "House",
            "updateDate": "2026-09-02",
            "latestAction": {"actionDate": "2026-09-02", "text": "Passed Senate."},
            "url": "https://api.congress.gov/v3/bill/119/hr/6028",
        }
    )
    assert len(fresh) == 10, "the narrow writer still shapes ten columns"

    out = merge_contract_table(tmp_path, NAME, [fresh], download_prior=lambda key, path: False, prior_present=True)
    published = pq.read_table(out)
    assert published.schema.names == list(contract.columns), "the published shape is the contract's"

    row = published.to_pylist()[0]
    # What only the family sets survives a narrow run...
    assert row["stage"] == "passed_house"
    assert row["stage_rule"] == "house_passage"
    assert row["money_bill_kind"] == "appropriations"
    # ...and what the narrow writer owns is still updated by it.
    assert row["title"] == "A bill (updated)"
    assert row["update_date"] == "2026-09-02"


def test_only_congress_bills_merges_column_wise():
    """Coalescing is for tables with two writers; elsewhere a NULL is a value."""
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
