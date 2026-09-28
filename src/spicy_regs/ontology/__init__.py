"""Metadata and ontology primitives for the regulatory corpus.

The public tables are built by transforms under :mod:`spicy_regs.transforms`.
This package contains the identifier readers, Federal Register references,
provenance helpers and the day rule those transforms share.
"""

from spicy_regs.ontology.citations import (
    CfrCitation,
    canonical_cfr_iri,
    normalize_regsgov_identifier,
    parse_cfr_citation,
)


def __getattr__(name: str):
    # Serving needs the lightweight agency reader, not the optional table writers.
    if name in {"ATTESTATION_COLUMNS", "RunContext", "stable_id"}:
        from spicy_regs.ontology import common

        return getattr(common, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "ATTESTATION_COLUMNS",
    "CfrCitation",
    "RunContext",
    "canonical_cfr_iri",
    "normalize_regsgov_identifier",
    "parse_cfr_citation",
    "stable_id",
]
