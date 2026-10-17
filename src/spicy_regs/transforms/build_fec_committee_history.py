"""Build ``fec_committee_history`` from the FEC's bulk committee master (``cm_header_file.csv`` and ``cm<yy>.zip``).

Owner decision 53. The shape and its semantics live in :mod:`spicy_regs.transforms.fec_master_history`; this module
names the committee master's readers, projection and columns.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

from spicy_regs.transforms.fec_master_history import MasterFamily, build_master_history

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence

OUTPUT = "fec_committee_history.parquet"


def build_fec_committee_history(
    output_dir: Path,
    *,
    capture_dir: Path | None = None,
    selection: Path | None = None,
    evidence: CaptureEvidence | None = None,
    today: date | None = None,
) -> Path:
    """Refresh every cycle into ``capture_dir``, or rebuild from a retained ``selection``; write the whole table."""
    from spicy_docs.schemas.fec_committee_history import FEC_COMMITTEE_HISTORY, project_committee_master_row
    from spicy_docs.sources.fec import committee_master as source

    family = MasterFamily(
        "fec_committee_history",
        "committee",
        source.HEADER_URL,
        source.committee_master_cycles,
        source.committee_master_url,
        source.iter_committee_master_rows,
        project_committee_master_row,
        FEC_COMMITTEE_HISTORY.columns,
        "committee_id",
    )
    return build_master_history(
        family, output_dir, capture_dir=capture_dir, selection=selection, evidence=evidence, today=today
    )
