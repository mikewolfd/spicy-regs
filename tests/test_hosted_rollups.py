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

import pytest
import yaml

from spicy_regs import data_dictionary as dd
from spicy_regs.pipelines.rollups.amendments import AmendmentsRollup
from spicy_regs.pipelines.rollups.bill_family import BillFamilyRollup
from spicy_regs.pipelines.rollups.committee_reports import CommitteeReportsRollup
from spicy_regs.pipelines.rollups.members import MembersRollup
from spicy_regs.pipelines.rollups.press_releases import PressReleasesRollup
from spicy_regs.pipelines.rollups.roll_call_votes import RollCallVotesRollup

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


def test_the_bill_family_declares_all_thirteen():
    assert len(BillFamilyRollup.outputs) == 13
    assert BillFamilyRollup.outputs[0] == "congress_bills.parquet"
    # The property the freshness checker uses resolves to the first key.
    assert BillFamilyRollup(output_dir=None).output == "congress_bills.parquet"


def test_every_hosted_table_has_exactly_one_writer():
    """Two rollups writing one table would overwrite each other's columns.

    ``congress_bills`` is the one deliberate exception and is asserted
    separately below, with the reason.
    """
    written: dict[str, list[str]] = {}
    for rollup in HOSTED_ROLLUPS:
        for key in _declared_keys(rollup):
            written.setdefault(key.removesuffix(".parquet"), []).append(rollup.name)

    assert set(written) == set(dd.CONTRACT_TABLES), "every contract must be published by exactly one rollup"
    doubled = {table: names for table, names in written.items() if len(names) > 1}
    assert not doubled, f"tables with more than one writer: {doubled}"


def test_congress_bills_has_a_second_narrow_writer():
    """The known exception, asserted so it stays deliberate rather than drifting.

    ``congress-bills`` walks the whole archive for the ten frozen columns;
    ``bill-family`` fills all forty-eight for the Congresses it is scoped to.
    Both write ``congress_bills.parquet``, and the merge prefers whichever ran
    most recently per ``bill_id``.
    """
    from spicy_regs.pipelines.rollups.congress_bills import CongressBillsRollup

    assert CongressBillsRollup.output == "congress_bills.parquet"
    assert "congress_bills.parquet" in BillFamilyRollup.outputs


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
