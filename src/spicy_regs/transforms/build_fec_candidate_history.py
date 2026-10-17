"""Build ``fec_candidate_history`` from the FEC's bulk candidate master (``cn_header_file.csv`` and ``cn<yy>.zip``).

The shape and its decision-53 semantics live in :mod:`spicy_regs.transforms.fec_master_history`; this module names
the candidate master's readers, projection and columns.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

from spicy_regs.transforms.fec_master_history import MasterFamily, build_master_history

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence

OUTPUT = "fec_candidate_history.parquet"


def build_fec_candidate_history(
    output_dir: Path,
    *,
    capture_dir: Path | None = None,
    selection: Path | None = None,
    evidence: CaptureEvidence | None = None,
    today: date | None = None,
) -> Path:
    """Refresh every cycle into ``capture_dir``, or rebuild from a retained ``selection``; write the whole table."""
    from spicy_docs.schemas.fec_candidate_history import FEC_CANDIDATE_HISTORY, project_candidate_master_row
    from spicy_docs.sources.fec import candidate_master as source

    family = MasterFamily(
        "fec_candidate_history",
        "candidate",
        source.HEADER_URL,
        source.candidate_master_cycles,
        source.candidate_master_url,
        source.iter_candidate_master_rows,
        project_candidate_master_row,
        FEC_CANDIDATE_HISTORY.columns,
        "candidate_id",
    )
    return build_master_history(
        family, output_dir, capture_dir=capture_dir, selection=selection, evidence=evidence, today=today
    )
