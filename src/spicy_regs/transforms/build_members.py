"""Transform: build members, terms and source party-affiliation occurrences.

Related tables from one capture of the @unitedstates community crosswalk, through
``spicy_docs.sources.legislators``. The split is the point: a legislator's
``terms`` carries every term served, and one row cannot hold a chamber switch,
so ``members`` keeps the identity and the *current* term's facts while
``member_terms`` keeps one row per term in the crosswalk's own order.

Both rosters are captured — ``current`` and ``historical`` — because the
identity crosswalk (bioguide to LIS, FEC, ICPSR, GovTrack, Vote Smart, OpenSecrets,
Wikidata, previous Bioguide IDs and historical names) is useful for a member
who has left. ``roster`` records
which file a row came from. Row shapes come from
``spicy_docs.schemas.legislator_tables``; nothing here re-derives a column.

Keyless: both files are static JSON on unitedstates.github.io.
"""

from __future__ import annotations

from pathlib import Path

from loguru import logger
from spicy_docs.schemas.legislator_tables import shape_member, shape_member_term, shape_member_party_affiliation
from spicy_docs.sources.legislators import LegislatorsAcquirer, LegislatorsBudget

from spicy_regs.transforms.table_merge import merge_contract_table
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.sources.retained import RetainedLegislatorsAcquirer

# Two requests, paced. The historical file is the large one (~12 MB); the
# acquirer's own ``max_historical_bytes`` default covers it.
BUDGET = LegislatorsBudget(
    max_requests=4,
    max_bytes=8 * 1024 * 1024,
    timeout_seconds=120.0,
    min_request_interval_seconds=1.0,
)


def build_members(output_dir: Path, *, acquirer: LegislatorsAcquirer | None = None,
                  evidence: CaptureEvidence | None = None) -> tuple[Path, ...]:
    """Build members, terms and affiliations from both complete rosters."""
    acquirer = acquirer or (RetainedLegislatorsAcquirer(budget=BUDGET, evidence=evidence)
                           if evidence else LegislatorsAcquirer(budget=BUDGET))

    member_rows: list[dict] = []
    term_rows: list[dict] = []
    affiliation_rows: list[dict] = []
    for roster, acquire in (("current", acquirer.acquire_current), ("historical", acquirer.acquire_historical)):
        try:
            acquisition = acquire()
        except Exception as error:
            if evidence:
                evidence.refusal(error, stage=roster)
            raise
        if evidence:
            evidence.capture(acquisition.capture, stage=roster + ":used")
        observed_at = acquisition.capture.observed_at
        records = acquisition.file.records
        logger.info("Members: {} roster — {:,} legislators", roster, len(records))
        for legislator in records:
            member_rows.append(shape_member(legislator, roster=roster, observed_at=observed_at))
            for term_index, term in enumerate(legislator.terms):
                term_rows.append(
                    shape_member_term(
                        term,
                        bioguide_id=legislator.bioguide,
                        term_index=term_index,
                        observed_at=observed_at,
                    )
                )

                for affiliation_index, _ in enumerate(term.party_affiliations):
                    digest = acquisition.file.input_sha256
                    if digest is None:
                        raise ValueError("Member party affiliation occurrences require their exact roster input digest")
                    affiliation_rows.append(shape_member_party_affiliation(
                        legislator, term_index=term_index, affiliation_index=affiliation_index,
                        input_sha256=digest, observed_at=observed_at,
                    ))

    logger.info("Members: {:,} member rows, {:,} term rows", len(member_rows), len(term_rows))
    return (
        merge_contract_table(output_dir, "members", member_rows),
        merge_contract_table(output_dir, "member_terms", term_rows),
        # Both complete rosters were read before any output is written. This is
        # the current source history, not a union of earlier captured histories.
        merge_contract_table(output_dir, "member_party_affiliations", affiliation_rows, prior_present=False),
    )
