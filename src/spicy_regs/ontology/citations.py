"""The identifier readers the rulemaking tables use: CFR objects, RINs and Regulations.gov ids.

The Federal Register states every CFR reference as a structured object (all 294,501 on
the 2026-09-23 parents), so no prose citation grammar is needed here; SpicyDocs owns the
data-side grammar (``spicy_docs.interpretation.citation_grammar``) and the label-aware
docket reader (see ``ontology.federal_register.linked_docket_id``).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import cast


def _digits(value: object) -> str | None:
    """Normalize a digit string, dropping leading zeros; ``None`` when not all digits."""
    if value is None:
        return None
    text = str(value).strip()
    return str(int(text)) if text.isdigit() else None


def _section(value: object) -> str | None:
    """Lowercase a section token and drop any parenthetical subsection detail."""
    if value is None:
        return None
    text = str(value).strip().lower()
    text = re.sub(r"\([^)]*\)", "", text)
    return text or None


def _cfr_section(value: object) -> str | None:
    """Normalize a Rulespec CFR section suffix, excluding subsection detail."""
    text = _section(value)
    if text is None:
        return None
    return text if re.fullmatch(r"\d+[a-z]{0,3}(?:-[0-9a-z]+)*", text) else None


@dataclass(frozen=True)
class CfrCitation:
    """A parsed CFR citation; ``section`` is ``None`` when the text named only a part."""

    title: str
    part: str
    section: str | None = None

    @property
    def cfr_ref(self) -> str:
        suffix = f".{self.section}" if self.section else ""
        return f"{self.title}-{self.part}{suffix}"


def canonical_cfr_iri(title: object, part: object, section: object = None) -> str:
    """Expand a CFR title/part/section to its IRI; malformed components raise ValueError."""
    title_number = _digits(title)
    part_number = _digits(part)
    section_number = _cfr_section(section)
    if not title_number or not part_number:
        raise ValueError(f"invalid CFR identifier components: title={title!r}, part={part!r}")
    if section is not None and section_number is None:
        raise ValueError(f"invalid CFR section: {section!r}")
    suffix = f".{section_number}" if section_number else ""
    return f"urn:rkaf:us:cfr:{title_number}:{part_number}{suffix}"


#: A Regulation Identifier Number: a four-digit agency code, then a two-letter
#: sub-agency code and a two-digit sequence. The one definition of the published key —
#: every reader of a RIN column goes through :func:`normalize_rin`. SpicyDocs' wider
#: shapes (``0648-A110``, ``2060-XXXX``) are for detection only; as a key they admit
#: only damage and placeholders (parsing survey 2026-09-23, section 5).
_RIN = re.compile(r"^\d{4}-[A-Z]{2}\d{2}$")


def normalize_rin(value: object) -> str | None:
    """Return the canonical RIN a value states, or ``None`` when it states none."""
    text = str(value or "").strip().upper()
    return text if _RIN.fullmatch(text) else None


def normalize_regsgov_identifier(identifier: object) -> str | None:
    """Return a canonical Regulations.gov identifier when syntax permits it."""
    value = str(identifier or "").strip().upper()
    return value if re.fullmatch(r"[A-Z0-9]+(?:[-_][A-Z0-9]+)*", value) else None


def parse_cfr_citation(value: object) -> list[CfrCitation]:
    """Read a Federal Register CFR reference object (``{"title": 40, "part": 60}``).

    Anything else reads as no citation: the Register states its references as objects,
    and a string would need a prose grammar this module does not carry.
    """
    if not isinstance(value, dict):
        return []
    components = cast(dict[str, object], value)
    title = _digits(components.get("title"))
    part = _digits(components.get("part"))
    section = _section(components.get("section"))
    return [CfrCitation(title, part, section)] if title and part else []
