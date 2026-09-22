"""Source connectors; pipeline dependencies load only when a connector is used."""

from importlib import import_module
from typing import TYPE_CHECKING

from spicy_regs.sources.base import Reader, Writer

if TYPE_CHECKING:
    from spicy_regs.sources import iceberg, r2
    from spicy_regs.sources.cfr_sections import CfrSectionsReader
    from spicy_regs.sources.congress_bills import CongressBillsReader
    from spicy_regs.sources.derived_text import DerivedCommentText
    from spicy_regs.sources.fcc_ecfs import FccEcfsFilingsReader, FccEcfsProceedingsReader
    from spicy_regs.sources.federal_register import FederalRegisterReader
    from spicy_regs.sources.parquet import StagingWriter
    from spicy_regs.sources.unified_agenda import UnifiedAgendaReader

_CONNECTORS = {
    "CfrSectionsReader": "cfr_sections",
    "CongressBillsReader": "congress_bills",
    "DerivedCommentText": "derived_text",
    "FccEcfsFilingsReader": "fcc_ecfs",
    "FccEcfsProceedingsReader": "fcc_ecfs",
    "FederalRegisterReader": "federal_register",
    "StagingWriter": "parquet",
    "UnifiedAgendaReader": "unified_agenda",
}


def __getattr__(name: str):
    """Lazily import and cache a connector or submodule on first attribute access."""
    if name in {"r2", "iceberg"}:
        value = import_module(f"{__name__}.{name}")
    elif name in _CONNECTORS:
        value = getattr(import_module(f"{__name__}.{_CONNECTORS[name]}"), name)
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value

__all__ = [
    "Reader",
    "Writer",
    "CfrSectionsReader",
    "CongressBillsReader",
    "UnifiedAgendaReader",
    "FederalRegisterReader",
    "FccEcfsProceedingsReader",
    "FccEcfsFilingsReader",
    "DerivedCommentText",
    "StagingWriter",
    "r2",
    "iceberg",
]
