"""Own-output checkpoints, including successful reads that produce no link."""

from pathlib import Path

import pyarrow.parquet as pq
from spicy_docs.interpretation.cbo_estimates import CBO_ESTIMATE_RULE_VERSION
from spicy_docs.interpretation.hearing_bill_links import HEARING_BILL_LINK_RULE_VERSION

READS_TABLE = "committee_report_reads"
READ_COLUMNS = ("package_id", "last_modified", "outcome", "rule_version", "observed_at")
RULE_VERSIONS = {"CRPT": CBO_ESTIMATE_RULE_VERSION, "CHRG": HEARING_BILL_LINK_RULE_VERSION}


def prior_reads(path: Path | None, priors: dict[str, Path | None]) -> dict[str, dict]:
    """Old package rows need enrichment once, even outside the discovery window."""
    rows = {} if path is None else {row["package_id"]: row for row in pq.read_table(path).to_pylist()}
    for prior in priors.values():
        if prior is not None:
            for row in pq.read_table(prior, columns=["package_id", "last_modified"]).to_pylist():
                rows.setdefault(row["package_id"], row | {"outcome": "pending", "rule_version": None})
    return rows


def complete(row: dict, collection: str, modified: str | None = None) -> bool:
    return (
        row.get("outcome") == "complete"
        and row.get("rule_version") == RULE_VERSIONS[collection]
        and (modified is None or modified == row.get("last_modified"))
    )
