"""Published identifier readers shared by lightweight serving and source transforms."""

from __future__ import annotations

import re


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


#: X-pattern codes (``0648-XC39``, ``0660-XC00``): agencies spell a non-regulatory action's
#: identifier exactly like a RIN. On the 2026-09-26 parents NOAA held 2,195 distinct
#: (0648-X…) and 215 others, nearly all Commerce bureaus (NTIA, BIS, ITA, Census, BEA,
#: NIST) or mistyped NOAA prefixes, on 537 Register rows; none of the 2,410 is on the
#: Unified Agenda. They name no rulemaking of their own (owner decision 61, extended from
#: NOAA's to every agency's).
_X_CODE = re.compile(r"^\d{4}-X[A-Z]\d{2}$")


def action_evidence_rin(value: object) -> str | None:
    """The RIN a value states when a RIN decides action evidence, or ``None``.

    Every RIN :func:`normalize_rin` admits stays a recorded value — on its row, in a
    proceeding's rins, in rule_targets edges; an X-pattern code (:data:`_X_CODE`) alone
    never makes a Register row, a Regulations.gov document or a docket action evidence,
    nor a RIN one proceeding holds specific (owner decision 61).
    """
    rin = normalize_rin(value)
    return None if rin is None or _X_CODE.fullmatch(rin) else rin


#: Every dash a U.S. Code section is printed with: spicy-docs ``schemas.tables.DASH_SPELLINGS`` (hyphen, non-breaking
#: hyphen, figure dash, en and em dash, horizontal bar, minus, and the Windows-1252 en/em bytes), restated because the
#: serving modules cannot import spicy-docs; ``tests/test_citation_parity.py`` holds the copy equal to its owner.
USC_DASH_SPELLINGS = "‐‑‒–—―−\x96\x97"


def usc_section_key_sql(expression: str) -> str:
    """SQL for spicy-docs ``usc_section_key`` over a trusted ``expression``: trimmed, lower-cased, dashes as hyphens."""
    return f"lower(translate(trim({expression}), '{USC_DASH_SPELLINGS}', '{'-' * len(USC_DASH_SPELLINGS)}'))"


#: A U.S. Code section's parts in Code order: integer stem, letters, the number after a dash, then any remainder
#: (1857f-5a).
_SECTION_PARTS = r"^([0-9]+)([a-z]*)(?:-([0-9]+))?(.*)$"


def section_order_sql(section: str) -> str:
    """SQL for a folded section's place in Code order: stem, letters length, letters, dash number, rest.

    Each number compares as a number and the letters by length first: 34c < 34d < 35, 698v < 698v-4 < 698v-10, and
    2689r < 2689z < 2689aa. RefSpec ``_section_key`` compares the part after the stem as text, which misorders the
    last two; the release point holds range stubs of both shapes (``698v...698v-10``, ``2689r...2689aa``).
    """
    stem, letters, number, rest = (f"regexp_extract({section}, '{_SECTION_PARTS}', {group})" for group in range(1, 5))
    return (f"struct_pack(stem := TRY_CAST({stem} AS BIGINT), letters_length := length({letters}), "
            f"letters := {letters}, number := coalesce(TRY_CAST({number} AS BIGINT), -1), rest := {rest})")
