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
from spicy_regs.ontology.common import ATTESTATION_COLUMNS, RunContext, stable_id

__all__ = [
    "ATTESTATION_COLUMNS",
    "CfrCitation",
    "RunContext",
    "canonical_cfr_iri",
    "normalize_regsgov_identifier",
    "parse_cfr_citation",
    "stable_id",
]
