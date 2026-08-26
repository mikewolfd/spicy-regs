from spicy_regs.sources import iceberg, r2
from spicy_regs.sources.base import Reader, Writer
from spicy_regs.sources.bill_subjects import BillSubjectsFetcher
from spicy_regs.sources.cfr_sections import CfrSectionsReader
from spicy_regs.sources.congress_bills import CongressBillsReader
from spicy_regs.sources.derived_text import DerivedCommentText
from spicy_regs.sources.fcc_ecfs import FccEcfsFilingsReader, FccEcfsProceedingsReader
from spicy_regs.sources.federal_register import FederalRegisterReader
from spicy_regs.sources.mirrulations import MirrulationsReader
from spicy_regs.sources.pdf import fetch_pdf_bytes
from spicy_regs.sources.unified_agenda import UnifiedAgendaReader


def __getattr__(name: str):
    """Load the staging writer only when the predecessor pipeline requests it."""

    if name == "StagingWriter":
        from spicy_regs.sources.parquet import StagingWriter

        return StagingWriter
    raise AttributeError(name)

__all__ = [
    "Reader",
    "Writer",
    "MirrulationsReader",
    "CfrSectionsReader",
    "CongressBillsReader",
    "BillSubjectsFetcher",
    "UnifiedAgendaReader",
    "FederalRegisterReader",
    "FccEcfsProceedingsReader",
    "FccEcfsFilingsReader",
    "DerivedCommentText",
    "StagingWriter",
    "fetch_pdf_bytes",
    "r2",
    "iceberg",
]
