"""Source connectors; pipeline dependencies load only when a connector is used."""

from importlib import import_module
from typing import TYPE_CHECKING

from spicy_regs.sources.base import Reader, Writer

if TYPE_CHECKING:
    from spicy_regs.sources import iceberg, r2
    from spicy_regs.sources.cfr_sections import CfrSectionsReader
    from spicy_regs.sources.derived_text import DerivedCommentText
    from spicy_regs.sources.parquet import StagingWriter
    from spicy_regs.sources.unified_agenda import UnifiedAgendaReader

_CONNECTORS = {
    "CfrSectionsReader": "cfr_sections",
    "DerivedCommentText": "derived_text",
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
    "UnifiedAgendaReader",
    "DerivedCommentText",
    "StagingWriter",
    "r2",
    "iceberg",
]
