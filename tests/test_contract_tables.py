"""Hermetic tests for the contract-hosted tables.

No network. These cover the seam this repository actually owns — the
conversion from a contract's row shape to a published Parquet table — and
deliberately do not re-assert any rule. Whether ``stage`` is read correctly
from an action, or a version code is the right slug, is established in
spicy-docs beside the code that decides it; re-asserting it here would be a
second copy of the same claim that can only drift.

What is owned here: every hosted table publishes an all-VARCHAR schema in the
contract's column order, and the merge prefers the fresh row on a repeated
identity. Both are proved for every contract the wheel ships, hosted here or
not yet, by iterating ``TABLE_CONTRACTS`` rather than by listing tables, so a
table added upstream is covered the moment the wheel is adopted instead of
when someone remembers to add a case.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.schemas import TABLE_CONTRACTS

from spicy_regs import data_dictionary as dd
from spicy_regs.transforms.table_merge import merge_contract_table

CONTRACT_NAMES = sorted(TABLE_CONTRACTS)

#: How many contracts the adopted wheel ships (spicy-docs 0.23.0: 37 over 766
#: columns). See ``test_every_hosted_table_is_registered_everywhere`` for what
#: this catches that the hosted/unhosted partition below cannot.
ADOPTED_CONTRACT_COUNT = 37

#: Contracts spicy-docs 0.23.0 ships that no rollup here writes yet.
#: ``test_every_hosted_table_is_registered_everywhere`` asserts **exact**
#: equality against it, so the wheel's thirty-seven contracts are accounted for
#: one by one: a contract is either in ``data_dictionary.CONTRACT_TABLES`` — the
#: hosted surface, enumerated by hand on purpose — or named here as a decision
#: taken and not yet acted on. A contract a later wheel adds is in neither and
#: fails this test, which is the point.
#:
#: These five arrived with 0.23.0 and are the two PDF-family rollups' outputs.
#: Each name leaves this set in the commit that gives it a writer, so the set
#: is empty again once both rollups have landed.
UNHOSTED_CONTRACTS: frozenset[str] = frozenset(
    {
        "document_citations",
        "house_activity_reports",
        "budget_volumes",
        "senate_expenditures",
        "bill_committee_actions",
    }
)


def _no_download(remote_key: str, local_path: Path) -> bool:
    """Stand-in for R2: there is no prior table, and nothing may reach the network."""
    return False


def _row(contract, **overrides) -> dict:
    """A full row for ``contract``: every identity column set, everything else NULL."""
    row = {column: None for column in contract.columns}
    for index, column in enumerate(contract.identity):
        row[column] = f"id{index}"
    row.update(overrides)
    return row


@pytest.mark.parametrize("name", CONTRACT_NAMES)
def test_published_schema_is_the_contract_columns_as_varchar(tmp_path, name):
    contract = TABLE_CONTRACTS[name]
    out = merge_contract_table(tmp_path, name, [_row(contract)], download_prior=_no_download)

    assert out.name == f"{name}.parquet"
    schema = pq.read_table(out).schema
    assert list(zip(schema.names, schema.types)) == [(c, pa.string()) for c in contract.columns]


@pytest.mark.parametrize("name", CONTRACT_NAMES)
def test_merge_prefers_the_fresh_row_on_a_repeated_identity(tmp_path, name):
    contract = TABLE_CONTRACTS[name]
    # A column that is neither identity nor the version column, so the
    # preference is visible without disturbing either.
    spare = next(
        (c for c in contract.columns if c not in contract.identity and c != contract.version_column),
        None,
    )
    if spare is None:  # pragma: no cover - every contract has one today
        pytest.skip(f"{name} is all identity")

    first = merge_contract_table(tmp_path, name, [_row(contract, **{spare: "prior"})], download_prior=_no_download)
    # The published table becomes the prior for the next run.
    first.rename(tmp_path / f"_{name}_prior.parquet")

    out = merge_contract_table(tmp_path, name, [_row(contract, **{spare: "fresh"})], download_prior=_no_download)
    rows = pq.read_table(out).to_pylist()
    assert len(rows) == 1, f"{name}: a repeated identity must collapse to one row"
    assert rows[0][spare] == "fresh"


@pytest.mark.parametrize("name", CONTRACT_NAMES)
def test_a_row_missing_its_identity_is_dropped_not_published(tmp_path, name):
    """A null identity part cannot be keyed, so it must not reach the table."""
    contract = TABLE_CONTRACTS[name]
    good = _row(contract)
    bad = _row(contract)
    bad[contract.identity[0]] = None

    out = merge_contract_table(tmp_path, name, [good, bad], download_prior=_no_download)
    assert len(pq.read_table(out).to_pylist()) == 1


def test_every_hosted_table_is_registered_everywhere():
    """A hosted table is one of the wheel's contracts, and is published, queryable and described.

    The wheel ships more contracts than this repository hosts; the difference
    is ``UNHOSTED_CONTRACTS``, stated so a new upstream contract is a decision
    here rather than a silent omission from the hosted surface. The two sets
    partition the registry by name, so every contract the adopted wheel ships
    is accounted for individually rather than by a count.

    ``ADOPTED_CONTRACT_COUNT`` is the one place a number appears, and it is
    there because the partition alone cannot see one thing: adopting a wheel
    that adds a contract *and* adding that contract to ``CONTRACT_TABLES`` in
    the same edit keeps the partition exact while hosting a table nobody
    decided to host. The count fails in that case and the partition does not.
    """
    from spicy_regs import mcp_server

    hosted = set(dd.CONTRACT_TABLES)
    assert len(TABLE_CONTRACTS) == ADOPTED_CONTRACT_COUNT, (
        "the adopted wheel's contract count moved; adopting a release is a decision, "
        "so update this number in the same commit that decides what to do with the new contracts"
    )
    assert hosted <= set(TABLE_CONTRACTS), "every hosted table must be a wheel contract"
    assert set(TABLE_CONTRACTS) - hosted == UNHOSTED_CONTRACTS, "a wheel contract is hosted or named as not yet"
    assert UNHOSTED_CONTRACTS.isdisjoint(dd.TABLES), "an unhosted contract must not be half-listed"
    assert hosted <= set(dd.TABLES)
    assert hosted <= set(mcp_server.TABLES)
    assert hosted <= set(dd.load_descriptions())


def test_congress_bills_keeps_its_frozen_prefix():
    """Other repositories pin these ten columns by digest; the contract appends only."""
    from spicy_regs.transforms.build_congress_bills import COLUMNS

    contract = TABLE_CONTRACTS["congress_bills"]
    assert contract.columns[:10] == COLUMNS
    assert contract.identity == ("bill_id",)
    assert len(contract.columns) > len(COLUMNS), "the family appends columns; it does not replace them"
