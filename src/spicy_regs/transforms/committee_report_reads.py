"""Own-output checkpoints for the committee-report rollups, including successful reads that produce no link.

``RULE_VERSIONS`` is the rule set that invalidates a prior read for each
collection, and ``complete`` is the completeness test over a checkpoint row.
"""

from pathlib import Path

import pyarrow.parquet as pq
from spicy_docs.interpretation.cbo_estimates import CBO_ESTIMATE_RULE_VERSION
from spicy_docs.interpretation.hearing_bill_links import HEARING_BILL_LINK_RULE_VERSION
from spicy_docs.schemas.committee_report_tables import REPORT_SECTION_READER_VERSION

from spicy_regs.generations import spicy_docs_code

READS_TABLE = "committee_report_reads"
READ_COLUMNS = ("package_id", "last_modified", "outcome", "rule_version", "observed_at")
#: ``parts`` is this repository's own rule for reading a report: every part
#: its record states, one row each, a package's rows replaced as a set
#: (decision 29). ``per-part-001`` replaced ``held-001``, which held out a
#: report read at its part 1's stem, so every CRPT checkpoint reads once more.
#: The re-read corrects what the backfill cannot: a row published before
#: ``part_id`` is kept as ``(package_id, package_id)``, right for a report in
#: one part, but CRPT-119hrpt811's Part-1 row (published 2026-09-24) becomes
#: ``(CRPT-119hrpt811-pt1, 1)``, and CRPT-119hrpt494 gains its Part 2 and part
#: numbers, only when read again.
#:
#: ``body`` is this repository's rule for a body's text, in both collections:
#: ``placeholder-pdf-001`` reads the PDF a part offers in place of the
#: publisher's placeholder text, publishes ``body_completeness`` and
#: ``text_derivation``, and gives a PDF-derived report's sections their pages.
#: ``placeholder-pdf-002`` (spicy-docs 0.33.2) also knows the notice spelled
#: without "IN", which CRPT-119hrpt649 and CHRG-119jhrg60491 print, so every
#: body read under 001 is read again.
#: ``code`` is the installed SpicyDocs' code and data (:func:`spicy_docs_code`),
#: which derives a report's text and sections; it replaced the release string,
#: under which every one of the stack's version-only releases re-read all 141
#: reports for rows differing only in ``observed_at`` (708f2bf4 against 95810b26).
#: CHRG also moves with ``HEARING_BILL_LINK_RULE_VERSION`` (the date correction
#: moved it). Admitting the two historical CHRG volumes to the package grammar
#: needs no token: no checkpoint names either, so there is nothing to re-read,
#: and only discovery can select them.
RULE_VERSIONS = {
    "CRPT": (
        f"code={spicy_docs_code()[:12]};cbo={CBO_ESTIMATE_RULE_VERSION};sections={REPORT_SECTION_READER_VERSION}"
        ";parts=per-part-001;body=placeholder-pdf-002"
    ),
    "CHRG": f"{HEARING_BILL_LINK_RULE_VERSION};body=placeholder-pdf-002",
}


def prior_reads(path: Path | None, priors: dict[str, Path | None]) -> dict[str, dict]:
    """Old package rows need enrichment once, even outside the discovery window."""
    rows = {} if path is None else {row["package_id"]: row for row in pq.read_table(path).to_pylist()}
    for prior in priors.values():
        if prior is not None:
            for row in pq.read_table(prior, columns=["package_id", "last_modified"]).to_pylist():
                rows.setdefault(row["package_id"], row | {"outcome": "pending", "rule_version": None})
    return rows


def complete(row: dict, collection: str, modified: str | None = None) -> bool:
    """Whether a recorded read is complete under this collection's rule version, optionally at a source stamp."""
    return (
        row.get("outcome") == "complete"
        and row.get("rule_version") == RULE_VERSIONS[collection]
        and (modified is None or modified == row.get("last_modified"))
    )
