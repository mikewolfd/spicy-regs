"""FEC's placeholder names: the one test of whether a filed name field states nothing.

Committees fill FEC's connected-organization field (``CONNECTED_ORG_NM``, Form 1's affiliated committee) with a
word rather than leave it blank. On FEC committee history ``4ed93047`` (298,435 committee-cycles, measured
2026-10-03) the spellings were NONE 56,551 (``NONE``, ``None``, ``none``, ``None.``, ``"NONE"``), BLANK 1,580
(``BLANK``, ``(BLANK)``), NA 700 (``N/A``, ``NA``, ``N A``), punctuation alone 215 (``-``, ``.``, ``$``, ``/``) and
``0`` once. Each is compared on its letters and digits alone, so a new spelling of the same word reads the same.
``SAME`` (41 rows) is a statement, not a placeholder, and is not here.
"""

from __future__ import annotations

import re

#: The letters and digits of a placeholder: empty (punctuation alone, or blank), ``NONE``, ``BLANK``, ``NA``, ``0``.
NOT_STATED_KEYS = frozenset({"", "NONE", "BLANK", "NA", "0"})


def not_stated(value: str | None) -> bool:
    """Whether a filed name states nothing: NULL, blank, or one of FEC's placeholder words."""
    return value is None or re.sub(r"[^A-Z0-9]", "", value.upper()) in NOT_STATED_KEYS


def not_stated_sql(expr: str) -> str:
    """:func:`not_stated` as a DuckDB predicate over the SQL expression ``expr`` (an application constant)."""
    keys = ", ".join(f"'{key}'" for key in sorted(NOT_STATED_KEYS))
    return f"(({expr}) IS NULL OR regexp_replace(upper({expr}), '[^A-Z0-9]', '', 'g') IN ({keys}))"
