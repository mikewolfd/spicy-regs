"""Deterministic LDA lobbyist-to-CT-witness candidate generation and precision-read batching.

Implements the joint Congressional Archive / spicy-regs person-identity matching contract
(``person-identity-review/1``):

* **Name normalization** (``contract-review.md:21-26,113-123``; ``pc-contract-draft.md`` §2):
  NFKC + casefold + accent folding (``José`` == ``Jose``), parenthetical removal, apostrophe
  elision (``O'Reilly`` == ``OReilly``), explicit hyphen-to-space conversion *before* general
  punctuation-to-space (``Holtz-Eakin`` == ``Holtz Eakin``), surname-first reordering
  (``Williams, James A.``, ``Van Hollen, Chris``), compound surname preservation
  (``Chris Van Hollen``), and strict conflict blocking on differing stated generational
  suffixes (``Jr.`` / ``Sr.`` / ``II`` / ``III`` / ``IV``) and differing stated middle
  initials.
* **Organization normalization**: **never** cuts at the first comma (so ``OIG, DoD`` and
  ``OIG, HHS`` remain distinct, and ``University of California, Berkeley`` stays distinct
  from ``University of California, Los Angeles``); strips only trailing place names cleanly
  (e.g., ``, Washington, DC``, ``, Cambridge, Massachusetts``), drops leading ``the``, and
  refuses empty or bare-title/private-citizen organizations.
* **Date-overlap scoring**: compares CT witness hearing dates against matching LDA filing
  periods (``first_quarter``..``fourth_quarter``, ``mid_year``, ``year_end``).
* **Privacy boundary**: emits only public hearing witness appearances matched to registered
  Senate LDA lobbyists (``lda_lobbyist:<id>``) and validates every emitted candidate through
  :func:`spicy_regs.identity_review.person_candidate_digest` (strictly excluding commenters,
  FEC donors under 52 U.S.C. 30111(a)(4), and third-party/LinkedIn profiles).
"""
from __future__ import annotations

import argparse
import json
import random
import re
import unicodedata
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import httpx

from spicy_regs import archive_release
from spicy_regs.identity_review import (
    PERSON_RULE,
    person_candidate_digest,
    render_person_batch_sheet,
)
from spicy_regs.public_url import resolve_r2_base_url

#: Explicit hyphen policy executed BEFORE general punctuation-to-space normalization.
HYPHEN_POLICY = "space_before_punctuation"
#: Default User-Agent sent to Cloudflare-fronted spicygov endpoints and DuckDB httpfs.
DEFAULT_USER_AGENT = "spicy-regs-person-identity/1.0 (+https://spicygov.ai)"

#: Generational suffixes extracted aside; differing stated suffixes block a merge.
SUFFIXES = frozenset({"jr", "sr", "ii", "iii", "iv"})

#: Compound-surname particles preserved intact inside the surname.
SURNAME_PARTICLES = frozenset({
    "van", "von", "de", "del", "della", "la", "le", "di", "da", "du", "st", "mac", "mc", "dos", "das",
})

#: Unambiguous honorifics, abbreviations, and credentials stripped anywhere in a name
#: (synchronized with CT ``model/sql/appearance-people.sql:18-23`` and ``witness-person-facts.sql``).
HONORIFICS_AND_CREDENTIALS = frozenset({
    "mr", "mrs", "ms", "miss", "mx", "dr", "hon", "honorable", "the", "rev",
    "sen", "rep", "gen", "lt", "col", "maj", "adm", "vadm", "radm", "ltg", "mg", "bg", "brig",
    "capt", "sgt", "prof",
    "phd", "md", "mph", "jd", "rn", "mba", "dvm", "esq", "cpa", "edd", "dds", "mpa", "mpp", "msw",
    "ret", "retired", "usa", "usn", "usaf", "usmc", "uscg", "ussf",
})

#: Full-word ranks and titles stripped only when leading/non-final so real surnames such as
#: "John Major", "Mary Justice", or "Tom Judge" are preserved intact.
LEADING_TITLES = frozenset({
    "reverend", "senator", "representative", "general", "lieutenant", "colonel", "major",
    "admiral", "vice", "rear", "captain", "sergeant", "brigadier", "chief", "ambassador",
    "secretary", "judge", "justice", "governor", "mayor", "professor", "commissioner",
    "chairman", "chairwoman", "chair", "ranking", "member", "congressman", "congresswoman",
    "delegate",
})

#: Non-institutional / bare-title / private-citizen organizations that never merge.
NON_MERGEABLE_ORGS = frozenset({
    "", "self", "none", "na", "n a", "unknown", "tbd", "witness", "witnesses", "individual",
    "private citizen", "private individual", "citizen",
    "inc", "llc", "llp", "corp", "ltd", "pc", "plc", "pllc",
    "senator", "u s senator", "united states senator", "representative",
    "u s representative", "member of congress",
    "director", "president", "chairman", "chair", "member", "chief", "partner",
    "founder", "owner", "attorney", "consultant", "professor", "ceo", "executive director",
    "commissioner", "administrator", "secretary", "senior fellow", "vice president",
})

#: Non-person CT witness_type values excluded from candidate generation (matching CT witness-people.sql).
EXCLUDED_WITNESS_TYPES = frozenset({
    "statement_for_record", "member_statement", "record_material", "member_appearance",
})

#: US states, District of Columbia, and territories accepted as trailing place-name states.
US_STATE_ABBREVS = frozenset({
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "hi", "id", "il", "in", "ia",
    "ks", "ky", "la", "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj",
    "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", "vt",
    "va", "wa", "wv", "wi", "wy", "dc", "pr", "vi", "gu", "as", "mp",
})

US_STATE_NAMES = frozenset({
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado", "connecticut",
    "delaware", "florida", "georgia", "hawaii", "idaho", "illinois", "indiana", "iowa",
    "kansas", "kentucky", "louisiana", "maine", "maryland", "massachusetts", "michigan",
    "minnesota", "mississippi", "missouri", "montana", "nebraska", "nevada", "new hampshire",
    "new jersey", "new mexico", "new york", "north carolina", "north dakota", "ohio",
    "oklahoma", "oregon", "pennsylvania", "rhode island", "south carolina", "south dakota",
    "tennessee", "texas", "utah", "vermont", "virginia", "washington", "west virginia",
    "wisconsin", "wyoming", "district of columbia", "puerto rico", "virgin islands", "guam",
})

#: Words that signal an organizational unit rather than a city name before a trailing state.
_NON_CITY_WORDS = frozenset({
    "department", "dept", "office", "bureau", "agency", "administration", "commission",
    "institute", "university", "college", "school", "center", "centre", "association",
    "council", "committee", "board", "authority", "service", "services", "division",
    "corp", "corporation", "inc", "incorporated", "llc", "llp", "pllc", "ltd", "limited",
    "group", "partners", "foundation", "union", "federation", "society", "hospital",
    "bank", "trust", "fund", "coalition", "alliance", "network", "system", "systems",
    "national", "american", "united", "federal", "international", "general", "inspector",
    "ministry", "program", "project", "policy", "research", "law", "legal", "defense",
    "health", "human", "veterans", "affairs", "security", "homeland", "justice", "energy",
    "commerce", "interior", "agriculture", "treasury", "transportation", "education", "labor",
    "housing", "urban", "development", "environmental", "protection", "state", "senate",
    "house", "congress", "congressional", "oig", "dod", "hhs", "doj", "dhs", "epa", "ftc",
    "sec", "fcc", "fda", "cdc", "irs", "gao", "crs", "cbo",
})

#: Trailing tokens on the remaining org prefix that indicate a trailing state is part of the
#: organization's own name (e.g. "University of Virginia", "State of Texas") rather than an address.
_ORG_FRAGMENT_TAILS = frozenset({
    "of", "for", "in", "at", "state", "university", "department", "office", "bureau",
    "agency", "commission", "board", "committee", "district", "county", "city", "town",
    "village", "commonwealth", "port", "authority", "chapter", "branch", "section",
    "division", "council", "association", "federation", "bar", "chamber",
})

_EXTRA_TRANSLIT = str.maketrans({
    "ø": "o",
    "ł": "l",
    "đ": "d",
    "ð": "d",
    "þ": "th",
    "æ": "ae",
    "œ": "oe",
})
_APOSTROPHE_RE = re.compile(r"['’`\u02bc]")
_HYPHEN_RE = re.compile(r"[\-\u2010-\u2015]")
_NON_ALNUM_RE = re.compile(r"[^\w]+|_")
_PAREN_RE = re.compile(r"\([^)]*\)")
_DOTTED_CRED_RE = re.compile(
    r"\b(?:ph\.d|m\.d|j\.d|m\.p\.h|m\.b\.a|d\.v\.m|c\.p\.a|ed\.d|d\.d\.s|m\.p\.a|m\.p\.p|m\.s\.w|r\.n|"
    r"u\.s\.a\.f|u\.s\.m\.c|u\.s\.c\.g|u\.s\.s\.f|u\.s\.a|u\.s\.n)\.?",
    re.IGNORECASE,
)
_ZIP_TAIL_RE = re.compile(r"(?:\s+\d{5}(?:-\d{4})?)?\s*$")

#: Exact calendar bounds for Senate LDA filing periods.
LDA_FILING_PERIOD_BOUNDS: dict[str, tuple[tuple[int, int], tuple[int, int]]] = {
    "first_quarter": ((1, 1), (3, 31)),
    "second_quarter": ((4, 1), (6, 30)),
    "third_quarter": ((7, 1), (9, 30)),
    "fourth_quarter": ((10, 1), (12, 31)),
    "mid_year": ((1, 1), (6, 30)),
    "year_end": ((7, 1), (12, 31)),
}


@dataclass(frozen=True)
class NormalizedName:
    """Normalized personal name components under ``person-identity-review/1``."""

    name_key: str | None
    middle_initial: str
    suffix: str
    surname_first: bool
    tokens: tuple[str, ...]


@dataclass(frozen=True)
class DateOverlap:
    """Temporal comparison between a witness hearing date and matching LDA filing periods."""

    status: str
    score: float
    min_abs_days_gap: int | None
    lda_min_date: str | None
    lda_max_date: str | None


@dataclass
class _LobbyistBucket:
    """Aggregated LDA filing evidence for one ``(name_key, org_key, lobbyist_id)``."""

    lobbyist_id: str
    name_key: str
    org_key: str
    middle_initials: set[str]
    suffixes: set[str]
    stated_names: Counter[str]
    matched_orgs: Counter[str]
    matched_roles: set[str]
    filing_periods: set[tuple[int, str | None]]
    filing_uuids: set[str]
    extra_filing_count: int
    covered_positions: Counter[str]


def fold_text(raw: str | None) -> str:
    """Apply NFKC normalization, casefolding, and Unicode accent folding (``José`` == ``Jose``)."""
    if not raw:
        return ""
    folded = unicodedata.normalize("NFKC", raw).casefold()
    folded = "".join(ch for ch in unicodedata.normalize("NFKD", folded) if unicodedata.category(ch) != "Mn")
    return folded.translate(_EXTRA_TRANSLIT)


def _collapse_dotted_credentials(text: str) -> str:
    return _DOTTED_CRED_RE.sub(lambda m: m.group(0).replace(".", ""), text)


def _tokenize_with_explicit_hyphens(text: str) -> list[str]:
    """Elide apostrophes, convert hyphens to spaces BEFORE punctuation-to-space, and tokenize."""
    cleaned = _collapse_dotted_credentials(text)
    cleaned = _APOSTROPHE_RE.sub("", cleaned)
    # Explicit hyphen handling BEFORE general punctuation-to-space (contract-review.md:23,118).
    cleaned = _HYPHEN_RE.sub(" ", cleaned)
    cleaned = _NON_ALNUM_RE.sub(" ", cleaned)
    return [tok for tok in cleaned.split() if tok]


def _raw_hyphen_units(text: str) -> list[str]:
    """Split text on whitespace while keeping hyphenated surnames like ``holtz-eakin`` as one unit."""
    cleaned = _APOSTROPHE_RE.sub("", text)
    cleaned = re.sub(r"[^\w\-\u2010-\u2015]+|_", " ", cleaned)
    return [unit.strip("-\u2010\u2011\u2012\u2013\u2014\u2015") for unit in cleaned.split() if unit.strip("-\u2010\u2011\u2012\u2013\u2014\u2015")]


def normalize_person_name(raw: str | None) -> NormalizedName:
    """Normalize a personal name string into ``(name_key, middle_initial, suffix)``.

    Handles accent folding, parentheticals, apostrophes, explicit hyphen-to-space,
    surname-first (``Williams, James A.``, ``Van Hollen, Chris``, ``Holtz-Eakin, Douglas``),
    compound surnames (``Chris Van Hollen``), honorific/rank/credential stripping,
    generational suffix extraction, and middle-initial extraction.
    """
    folded = _PAREN_RE.sub(" ", fold_text(raw)).strip()
    if not folded:
        return NormalizedName(None, "", "", False, ())

    surname_first = False
    if "," in folded:
        before_c, after_c = folded.split(",", 1)
        extra_suffixes: list[str] = []
        after_main = after_c
        if "," in after_c:
            first_after, rest_after = after_c.split(",", 1)
            rest_toks = _tokenize_with_explicit_hyphens(rest_after)
            if rest_toks and all(tok in SUFFIXES | HONORIFICS_AND_CREDENTIALS for tok in rest_toks):
                extra_suffixes = [tok for tok in rest_toks if tok in SUFFIXES]
                after_main = first_after

        b_units = _raw_hyphen_units(before_c)
        b_toks = _tokenize_with_explicit_hyphens(before_c)
        a_toks = _tokenize_with_explicit_hyphens(after_main)
        looks_like_surname = (
            len(b_units) == 1
            or (len(b_toks) == 2 and b_toks[0] in SURNAME_PARTICLES)
        )
        blocked_after_head = SUFFIXES | HONORIFICS_AND_CREDENTIALS | LEADING_TITLES
        if (
            looks_like_surname
            and 1 <= len(a_toks) <= 3
            and a_toks[0] not in blocked_after_head
        ):
            working = f"{after_main} {before_c} {' '.join(extra_suffixes)}"
            surname_first = True
        else:
            all_after_toks = _tokenize_with_explicit_hyphens(after_c)
            kept_suffixes = [tok for tok in all_after_toks if tok in SUFFIXES]
            working = f"{before_c} {' '.join(kept_suffixes)}"
    else:
        working = folded

    tokens = [
        tok for tok in _tokenize_with_explicit_hyphens(working)
        if tok not in HONORIFICS_AND_CREDENTIALS and not tok.isdigit()
    ]
    found_suffixes = sorted({tok for tok in tokens if tok in SUFFIXES})
    suffix = " ".join(found_suffixes)
    tokens = [tok for tok in tokens if tok not in SUFFIXES]

    # Strip leading rank/title words (never the surname at the end of the name).
    while len(tokens) >= 2 and tokens[0] in LEADING_TITLES:
        tokens = tokens[1:]

    if len(tokens) < 2:
        return NormalizedName(None, "", suffix, surname_first, tuple(tokens))

    middle_initial = "".join(tok for tok in tokens[1:-1] if len(tok) == 1)
    core = [tokens[0], *(tok for tok in tokens[1:] if len(tok) > 1)]
    if len(core) < 2:
        return NormalizedName(None, middle_initial, suffix, surname_first, tuple(tokens))

    return NormalizedName(
        name_key=" ".join(core),
        middle_initial=middle_initial,
        suffix=suffix,
        surname_first=surname_first,
        tokens=tuple(core),
    )


def normalize_lda_lobbyist_name(
    *,
    first_name: str | None,
    middle_name: str | None,
    last_name: str | None,
    suffix: str | None = None,
    prefix: str | None = None,
) -> NormalizedName:
    """Normalize structured Senate LDA lobbyist name fields to the same representation."""
    first_toks = [
        tok for tok in _tokenize_with_explicit_hyphens(_PAREN_RE.sub(" ", fold_text(first_name)))
        if tok not in HONORIFICS_AND_CREDENTIALS and not tok.isdigit()
    ]
    mid_toks = [
        tok for tok in _tokenize_with_explicit_hyphens(_PAREN_RE.sub(" ", fold_text(middle_name)))
        if tok not in HONORIFICS_AND_CREDENTIALS and not tok.isdigit()
    ]
    last_raw = _PAREN_RE.sub(" ", fold_text(last_name))
    # Strip trailing credential clause after comma in last_name while keeping suffixes.
    if "," in last_raw:
        lb, la = last_raw.split(",", 1)
        la_sufs = [tok for tok in _tokenize_with_explicit_hyphens(la) if tok in SUFFIXES]
        last_raw = f"{lb} {' '.join(la_sufs)}"
    last_toks = [
        tok for tok in _tokenize_with_explicit_hyphens(last_raw)
        if tok not in HONORIFICS_AND_CREDENTIALS and not tok.isdigit()
    ]
    suf_toks = _tokenize_with_explicit_hyphens(fold_text(suffix))

    # Extract any generational suffix embedded in first/middle/last/suffix fields.
    all_suffixes = {
        tok for tok in (*first_toks, *mid_toks, *last_toks, *suf_toks)
        if tok in SUFFIXES
    }
    first_toks = [tok for tok in first_toks if tok not in SUFFIXES]
    mid_toks = [tok for tok in mid_toks if tok not in SUFFIXES]
    last_toks = [tok for tok in last_toks if tok not in SUFFIXES]

    while len(first_toks) >= 2 and first_toks[0] in LEADING_TITLES:
        first_toks = first_toks[1:]
    if len(first_toks) == 1 and first_toks[0] in LEADING_TITLES:
        first_toks = []

    if not first_toks or not last_toks:
        # Fall back to whole-string normalization when LDA puts full name into one field.
        combined = " ".join(part for part in (prefix, first_name, middle_name, last_name, suffix) if part)
        return normalize_person_name(combined)

    # LDA sometimes places a middle initial inside first_name (e.g. "JAMES A.").
    embedded_mid = [tok for tok in first_toks[1:] if len(tok) == 1]
    given_toks = [first_toks[0], *(tok for tok in first_toks[1:] if len(tok) > 1)]

    # Keep all multi-letter tokens in last_name (compound surnames like "Van Hollen", "Holtz-Eakin").
    family_toks = [tok for tok in last_toks if len(tok) > 1]
    if not family_toks:
        family_toks = last_toks

    mid_initial_chars: list[str] = list(embedded_mid)
    if mid_toks:
        mid_initial_chars.append(mid_toks[0][0])
    middle_initial = "".join(mid_initial_chars)

    core = [*given_toks, *family_toks]
    if len(core) < 2:
        return NormalizedName(None, middle_initial, " ".join(sorted(all_suffixes)), False, tuple(core))

    return NormalizedName(
        name_key=" ".join(core),
        middle_initial=middle_initial,
        suffix=" ".join(sorted(all_suffixes)),
        surname_first=False,
        tokens=tuple(core),
    )


def _is_us_state_token(segment: str) -> tuple[bool, bool]:
    """Return ``(is_state, is_abbrev_or_dc)`` for a comma-delimited trailing place segment."""
    cleaned = _ZIP_TAIL_RE.sub("", segment).strip().rstrip(".").strip()
    if not cleaned:
        return False, False
    # Handle "d.c." / "d.c" / "n.y." style dotted two-letter abbreviations.
    compact = re.sub(r"[\s.]+", "", cleaned)
    if compact in US_STATE_ABBREVS and len(cleaned) <= 5:
        return True, True
    spaced = re.sub(r"\s+", " ", re.sub(r"[^\w\s]+", " ", cleaned)).strip()
    if spaced == "district of columbia":
        return True, True
    if spaced in US_STATE_NAMES:
        return True, False
    return False, False


def _looks_like_city(segment: str) -> bool:
    """Return True when a comma-delimited segment looks like a city name (not an org unit)."""
    cleaned = re.sub(r"[^\w\s.\-]+", " ", segment).strip()
    if not cleaned or any(ch.isdigit() for ch in cleaned):
        return False
    tokens = _tokenize_with_explicit_hyphens(cleaned)
    if not 1 <= len(tokens) <= 3:
        return False
    return not any(tok in _NON_CITY_WORDS for tok in tokens)


def _strip_trailing_place_name(folded_org: str) -> str:
    """Strip ONLY trailing US place names (``, City, State`` or ``, ST``), never cutting at first comma.

    Preserves comma-separated institutional sub-units such as
    ``Office of Inspector General, Department of Defense`` and
    ``University of California, Berkeley``.
    """
    text = folded_org.strip().rstrip(":;").strip()
    if "," not in text:
        return text

    parts = [part.strip() for part in text.split(",")]
    if len(parts) < 2:
        return text

    is_state, is_abbrev_or_dc = _is_us_state_token(parts[-1])
    if not is_state:
        return text

    # Case 1: "... , <City>, <State/DC>" or bare "<City>, <State/DC>".
    if len(parts) >= 2 and _looks_like_city(parts[-2]):
        if len(parts) == 2:
            # Bare place name only (e.g. "Washington, DC", "New York, NY") -> empty/unmergeable.
            return ""
        prefix = ", ".join(parts[:-2]).strip()
        prefix_toks = _tokenize_with_explicit_hyphens(prefix)
        if prefix_toks and prefix_toks[-1] not in _ORG_FRAGMENT_TAILS:
            return prefix
        return text

    # Case 2: "<Organization>, <ST>" (2-letter postal code or District of Columbia).
    if is_abbrev_or_dc:
        prefix = ", ".join(parts[:-1]).strip()
        prefix_toks = _tokenize_with_explicit_hyphens(prefix)
        if prefix_toks and prefix_toks[-1] not in _ORG_FRAGMENT_TAILS:
            return prefix

    return text


def normalize_organization(raw: str | None) -> str:
    """Normalize an organization string without cutting at the first comma.

    Strips only trailing place names cleanly, folds accents, elides apostrophes, converts
    hyphens and punctuation to spaces, collapses whitespace, drops a leading ``the``, and
    returns ``""`` for empty or bare-title/private-citizen organizations.
    """
    folded = fold_text(raw)
    if not folded.strip():
        return ""
    stripped = _strip_trailing_place_name(folded)
    if not stripped:
        return ""
    tokens = _tokenize_with_explicit_hyphens(stripped)
    norm = " ".join(tokens)
    if norm.startswith("the "):
        norm = norm[4:].strip()
    if norm in NON_MERGEABLE_ORGS:
        return ""
    return norm


def check_middle_initial_guard(left_mid: str, right_mid: str) -> tuple[bool, str]:
    """Block matches where both sides state differing middle initials; allow one/both unstated."""
    if not left_mid and not right_mid:
        return True, "both_unstated"
    if not left_mid or not right_mid:
        return True, "one_side_unstated"
    if left_mid[0] == right_mid[0]:
        return True, "both_match"
    return False, f"blocked_mismatch:{left_mid}!={right_mid}"


def check_suffix_guard(left_suf: str, right_suf: str) -> tuple[bool, str]:
    """Block matches where both sides state differing generational suffixes (Jr/Sr/II/III/IV)."""
    if not left_suf and not right_suf:
        return True, "both_unstated"
    if not left_suf or not right_suf:
        return True, "one_side_unstated"
    if left_suf == right_suf:
        return True, "both_match"
    return False, f"blocked_mismatch:{left_suf}!={right_suf}"


def lda_period_interval(filing_year: int, filing_period: str | None) -> tuple[date, date]:
    """Return inclusive ``(start_date, end_date)`` for a Senate LDA ``(filing_year, filing_period)``."""
    bounds = LDA_FILING_PERIOD_BOUNDS.get((filing_period or "").strip().lower())
    if bounds is None:
        return date(filing_year, 1, 1), date(filing_year, 12, 31)
    (sm, sd), (em, ed) = bounds
    return date(filing_year, sm, sd), date(filing_year, em, ed)


def compute_date_overlap(
    hearing_date: date | None,
    filing_periods: Sequence[tuple[int, str | None]],
) -> DateOverlap:
    """Score temporal overlap between a CT hearing date and a lobbyist's matching LDA filings."""
    intervals = [lda_period_interval(year, period) for year, period in filing_periods if year is not None]
    if not intervals:
        return DateOverlap("no_lda_dates", 0.0, None, None, None)
    min_date = min(start for start, _ in intervals)
    max_date = max(end for _, end in intervals)
    if hearing_date is None:
        return DateOverlap("undated", 0.0, None, min_date.isoformat(), max_date.isoformat())

    gaps: list[int] = []
    exact_hit = False
    years = {year for year, _ in filing_periods if year is not None}
    for start, end in intervals:
        if start <= hearing_date <= end:
            exact_hit = True
            gaps.append(0)
        elif hearing_date < start:
            gaps.append((start - hearing_date).days)
        else:
            gaps.append((hearing_date - end).days)
    min_gap = min(gaps)

    if exact_hit:
        status, score = "exact_period_overlap", 1.0
    elif hearing_date.year in years:
        status, score = "same_year_overlap", 0.9
    elif min_date <= hearing_date <= max_date:
        status, score = "span_overlap", 0.85
    elif min_gap <= 365:
        status, score = "adjacent_1y", 0.7
    elif min_gap <= 1095:
        status, score = "near_3y", 0.4
    else:
        status, score = "distant", 0.1

    return DateOverlap(
        status=status,
        score=score,
        min_abs_days_gap=min_gap,
        lda_min_date=min_date.isoformat(),
        lda_max_date=max_date.isoformat(),
    )


def _format_lda_name(record: Mapping[str, Any]) -> str:
    parts = [
        str(record.get(col) or "").strip()
        for col in ("prefix", "first_name", "middle_name", "last_name", "suffix")
    ]
    return " ".join(part for part in parts if part)


def _format_body_sha256(raw_sha: Any) -> str:
    if isinstance(raw_sha, bytes | bytearray | memoryview):
        hex_digest = bytes(raw_sha).hex()
    elif isinstance(raw_sha, str):
        stripped = raw_sha.strip()
        hex_digest = stripped.removeprefix("sha256:").lower()
    else:
        raise ValueError("Witness record_sha must be 32 raw bytes or a 64-hex string")
    if len(hex_digest) != 64 or not re.fullmatch(r"[0-9a-f]{64}", hex_digest):
        raise ValueError(f"Invalid SHA-256 record digest: {raw_sha!r}")
    return f"sha256:{hex_digest}"


def _coerce_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value.strip():
        return date.fromisoformat(value.strip()[:10])
    return None


def match_lda_witness_candidates(
    witnesses: Iterable[Mapping[str, Any]],
    lda_rows: Iterable[Mapping[str, Any]],
    *,
    ct_release_id: str | None = None,
    lobbying_generation_digest: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Deterministically match CT witness appearances against LDA lobbyist filings.

    Joins on exact normalized name + full normalized organization (matching either the
    lobbyist's ``registrant_name`` or ``client_name``), blocking differing generational
    suffixes and differing middle initials, and computes content-only ``candidate_digest``
    plus date-overlap scores.
    """
    stats: Counter[str] = Counter()

    # Index LDA lobbyist-org records by (name_key, org_key).
    # Structure: (name_key, org_key) -> lobbyist_id -> aggregated lobbyist evidence.
    lda_index: dict[tuple[str, str], dict[str, _LobbyistBucket]] = defaultdict(dict)
    for row in lda_rows:
        stats["lda_rows_scanned"] += 1
        lobbyist_id = str(row.get("lobbyist_id") or "").strip()
        if not lobbyist_id:
            stats["lda_skipped_no_id"] += 1
            continue
        norm_name = normalize_lda_lobbyist_name(
            first_name=row.get("first_name"),
            middle_name=row.get("middle_name"),
            last_name=row.get("last_name"),
            suffix=row.get("suffix"),
            prefix=row.get("prefix"),
        )
        if not norm_name.name_key:
            stats["lda_skipped_no_name"] += 1
            continue

        year_raw = row.get("filing_year")
        filing_year = int(year_raw) if year_raw is not None else None
        filing_period = str(row.get("filing_period") or "").strip() or None
        filing_uuid = str(row.get("filing_uuid") or "").strip()
        sample_uuids_raw = row.get("sample_filing_uuids")
        filing_count_raw = row.get("filing_count")
        stated_lda_name = _format_lda_name(row)
        covered_position = str(row.get("covered_position") or "").strip() or None

        org_roles: list[tuple[tuple[str, ...], str, str]] = []
        reg_raw = str(row.get("registrant_name") or "").strip()
        reg_key = normalize_organization(reg_raw)
        cli_raw = str(row.get("client_name") or "").strip()
        cli_key = normalize_organization(cli_raw)
        if reg_key and cli_key and reg_key == cli_key:
            org_roles.append((("client", "registrant"), reg_raw, reg_key))
        else:
            if reg_key:
                org_roles.append((("registrant",), reg_raw, reg_key))
            if cli_key:
                org_roles.append((("client",), cli_raw, cli_key))

        if not org_roles:
            stats["lda_skipped_no_org"] += 1
            continue

        for role_kinds, org_raw, org_key in org_roles:
            bucket = lda_index[(norm_name.name_key, org_key)].get(lobbyist_id)
            if bucket is None:
                bucket = _LobbyistBucket(
                    lobbyist_id=lobbyist_id,
                    name_key=norm_name.name_key,
                    org_key=org_key,
                    middle_initials=set(),
                    suffixes=set(),
                    stated_names=Counter(),
                    matched_orgs=Counter(),
                    matched_roles=set(),
                    filing_periods=set(),
                    filing_uuids=set(),
                    extra_filing_count=0,
                    covered_positions=Counter(),
                )
                lda_index[(norm_name.name_key, org_key)][lobbyist_id] = bucket
            if norm_name.middle_initial:
                bucket.middle_initials.add(norm_name.middle_initial)
            if norm_name.suffix:
                bucket.suffixes.add(norm_name.suffix)
            if stated_lda_name:
                bucket.stated_names[stated_lda_name] += 1
            bucket.matched_orgs[org_raw] += 1
            bucket.matched_roles.update(role_kinds)
            if filing_year is not None:
                bucket.filing_periods.add((filing_year, filing_period))
            if filing_uuid:
                bucket.filing_uuids.add(filing_uuid)
            if isinstance(sample_uuids_raw, list | tuple):
                for u in sample_uuids_raw:
                    if isinstance(u, str) and u.strip():
                        bucket.filing_uuids.add(u.strip())
            if filing_count_raw is not None:
                bucket.extra_filing_count += int(filing_count_raw)
            if covered_position:
                bucket.covered_positions[covered_position] += 1

    candidates: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for w in witnesses:
        stats["witnesses_scanned"] += 1
        if w.get("superseded_by") is not None:
            stats["witnesses_skipped_superseded"] += 1
            continue
        if w.get("bioguide_id") or w.get("matched_bioguide_id"):
            stats["witnesses_skipped_member"] += 1
            continue
        witness_type = str(w.get("witness_type") or "").strip()
        if witness_type in EXCLUDED_WITNESS_TYPES:
            stats["witnesses_skipped_excluded_type"] += 1
            continue

        witness_key = str(w.get("witness_key") or "").strip()
        stated_name = str(w.get("name") or "").strip()
        stated_org = str(w.get("organization") or "").strip()
        if not witness_key or not stated_name or not stated_org:
            stats["witnesses_skipped_missing_fields"] += 1
            continue

        w_name = normalize_person_name(stated_name)
        w_org = normalize_organization(stated_org)
        if not w_name.name_key or not w_org:
            stats["witnesses_skipped_unmergeable"] += 1
            continue

        matches_by_lobbyist = lda_index.get((w_name.name_key, w_org))
        if not matches_by_lobbyist:
            continue

        body_sha256 = _format_body_sha256(w.get("record_sha") or w.get("body_sha256"))
        page_or_offset = str(w.get("source_locator") or w.get("page_or_offset") or "").strip()
        if not page_or_offset:
            stats["witnesses_skipped_missing_locator"] += 1
            continue
        hearing_date = _coerce_date(w.get("hearing_date") or w.get("meeting_date"))

        for lobbyist_id in sorted(matches_by_lobbyist):
            lob = matches_by_lobbyist[lobbyist_id]
            # Collapse single unambiguous stated middle/suffix on the LDA lobbyist side.
            l_mids = sorted(lob.middle_initials)
            l_sufs = sorted(lob.suffixes)
            l_mid = l_mids[0] if len(l_mids) == 1 else ""
            l_suf = l_sufs[0] if len(l_sufs) == 1 else ""

            mid_ok, mid_guard = check_middle_initial_guard(w_name.middle_initial, l_mid)
            if not mid_ok:
                stats["blocked_middle_initial"] += 1
                continue
            suf_ok, suf_guard = check_suffix_guard(w_name.suffix, l_suf)
            if not suf_ok:
                stats["blocked_suffix"] += 1
                continue

            external_native_id = f"lda_lobbyist:{lobbyist_id}"
            candidate_id = f"lda_witness:{witness_key}:{external_native_id}"
            if candidate_id in seen_ids:
                continue
            seen_ids.add(candidate_id)

            lda_stated_name = lob.stated_names.most_common(1)[0][0]
            lda_matched_org = lob.matched_orgs.most_common(1)[0][0]
            lda_matched_role = "+".join(sorted(lob.matched_roles))
            filing_periods = sorted(lob.filing_periods, key=lambda item: (item[0], item[1] or ""))
            filing_years = sorted({y for y, _ in filing_periods})
            sample_uuids = sorted(lob.filing_uuids)[:5]
            filing_count = max(len(lob.filing_uuids), lob.extra_filing_count)
            covered_pos = lob.covered_positions.most_common(1)[0][0] if lob.covered_positions else None

            overlap = compute_date_overlap(hearing_date, filing_periods)
            year_span = f"{filing_years[0]}-{filing_years[-1]}" if filing_years else "undated"
            quoted_span = (
                f"CT witness '{stated_name}' ({stated_org}) at {page_or_offset}"
                f" <-> LDA {external_native_id} '{lda_stated_name}' "
                f"({lda_matched_role}: '{lda_matched_org}', filings: {filing_count}, years: {year_span})"
            )

            candidate: dict[str, Any] = {
                "rule_version": PERSON_RULE,
                "candidate_id": candidate_id,
                "subject_kind": "ct_witness_appearance",
                "witness_key": witness_key,
                "relation": "same_person",
                "external_native_id": external_native_id,
                # Exact 7 content fields hashed into candidate_digest:
                "stated_name": stated_name,
                "stated_org": stated_org,
                "body_sha256": body_sha256,
                "page_or_offset": page_or_offset,
                "quoted_span": quoted_span,
                # Provenance & matching diagnostics (STRICTLY outside candidate_digest):
                "release_id": ct_release_id,
                "source_generation_digest": lobbying_generation_digest,
                "name_key": w_name.name_key,
                "org_key": w_org,
                "witness_middle_initial": w_name.middle_initial,
                "witness_suffix": w_name.suffix,
                "witness_position": str(w.get("position") or "").strip() or None,
                "source": str(w.get("source") or "").strip() or None,
                "event_id": int(w["event_id"]) if w.get("event_id") is not None else None,
                "package_id": str(w.get("package_id") or "").strip() or None,
                "hearing_date": hearing_date.isoformat() if hearing_date else None,
                "lda_stated_name": lda_stated_name,
                "lda_middle_initial": l_mid,
                "lda_suffix": l_suf,
                "lda_matched_org": lda_matched_org,
                "lda_matched_role": lda_matched_role,
                "lda_filing_years": filing_years,
                "lda_filing_count": filing_count,
                "sample_filing_uuids": sample_uuids,
                "lda_covered_position": covered_pos,
                "middle_guard": mid_guard,
                "suffix_guard": suf_guard,
                "date_overlap_status": overlap.status,
                "date_overlap_score": overlap.score,
                "min_abs_days_gap": overlap.min_abs_days_gap,
                "lda_min_date": overlap.lda_min_date,
                "lda_max_date": overlap.lda_max_date,
            }
            candidate["content_digest"] = person_candidate_digest(candidate)
            candidates.append(candidate)
            stats["candidates_emitted"] += 1

    candidates.sort(key=lambda c: (c["name_key"], c["org_key"], c["witness_key"], c["external_native_id"]))
    return candidates, dict(stats)


def build_planted_wrong_controls(
    candidates: Sequence[Mapping[str, Any]],
    *,
    seed: int = 20261127,
    count: int = 2,
) -> list[dict[str, Any]]:
    """Construct auditable planted-wrong negative-control pairs from real witness/LDA records.

    Guarantees at least one clearly tracked wrong pair (mismatched person/organization
    drawn from distinct real witnesses and real LDA lobbyists) so the owner can audit
    precision-read vigilance before approving the matching rule.
    """
    if count < 1 or len(candidates) < 2:
        return []
    rng = random.Random(seed ^ 0x5A5A5A5A)
    by_name: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for item in candidates:
        by_name[item["name_key"]].append(item)
    distinct_names = sorted(by_name)
    if len(distinct_names) < 2:
        return []

    picked_names = rng.sample(distinct_names, min(len(distinct_names), count * 2))
    controls: list[dict[str, Any]] = []
    for idx in range(count):
        left_name = picked_names[(idx * 2) % len(picked_names)]
        right_name = picked_names[(idx * 2 + 1) % len(picked_names)]
        if left_name == right_name:
            continue
        w_src = by_name[left_name][0]
        l_src = by_name[right_name][0]
        if w_src["org_key"] == l_src["org_key"]:
            continue

        control_id = f"planted_control_{idx + 1}:{w_src['witness_key']}:{l_src['external_native_id']}"
        quoted_span = (
            f"[PLANTED NEGATIVE CONTROL #{idx + 1}] CT witness '{w_src['stated_name']}' ({w_src['stated_org']}) "
            f"at {w_src['page_or_offset']} paired intentionally with unrelated LDA {l_src['external_native_id']} "
            f"'{l_src['lda_stated_name']}' ({l_src['lda_matched_role']}: '{l_src['lda_matched_org']}')"
        )
        control: dict[str, Any] = {
            "rule_version": PERSON_RULE,
            "candidate_id": control_id,
            "subject_kind": "ct_witness_appearance",
            "witness_key": w_src["witness_key"],
            "relation": "same_person",
            "external_native_id": l_src["external_native_id"],
            "stated_name": w_src["stated_name"],
            "stated_org": w_src["stated_org"],
            "body_sha256": w_src["body_sha256"],
            "page_or_offset": w_src["page_or_offset"],
            "quoted_span": quoted_span,
            "release_id": w_src.get("release_id"),
            "source_generation_digest": w_src.get("source_generation_digest"),
            "name_key": w_src["name_key"],
            "org_key": w_src["org_key"],
            "witness_middle_initial": w_src.get("witness_middle_initial", ""),
            "witness_suffix": w_src.get("witness_suffix", ""),
            "witness_position": w_src.get("witness_position"),
            "source": w_src.get("source"),
            "event_id": w_src.get("event_id"),
            "package_id": w_src.get("package_id"),
            "hearing_date": w_src.get("hearing_date"),
            "lda_stated_name": l_src["lda_stated_name"],
            "lda_middle_initial": l_src.get("lda_middle_initial", ""),
            "lda_suffix": l_src.get("lda_suffix", ""),
            "lda_matched_org": l_src["lda_matched_org"],
            "lda_matched_role": l_src["lda_matched_role"],
            "lda_filing_years": l_src.get("lda_filing_years", []),
            "lda_filing_count": l_src.get("lda_filing_count", 0),
            "sample_filing_uuids": l_src.get("sample_filing_uuids", []),
            "lda_covered_position": l_src.get("lda_covered_position"),
            "middle_guard": "planted_control",
            "suffix_guard": "planted_control",
            "date_overlap_status": "planted_mismatch",
            "date_overlap_score": 0.0,
            "min_abs_days_gap": None,
            "lda_min_date": l_src.get("lda_min_date"),
            "lda_max_date": l_src.get("lda_max_date"),
            "is_planted_control": True,
            "planted_control_kind": "unrelated_person_and_org",
            "expected_owner_answer": "different_person",
        }
        control["content_digest"] = person_candidate_digest(control)
        controls.append(control)
    return controls


def build_precision_read_batch(
    candidates: Sequence[Mapping[str, Any]],
    *,
    seed: int = 20261127,
    total_pairs: int = 50,
    planted_wrong_count: int = 2,
    ct_release_id: str | None = None,
    lobbying_generation_digest: str | None = None,
    stats: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    """Build a reproducible seeded precision-read batch (default 50 pairs incl. planted controls).

    Samples at most one appearance per normalized person name so the 50-pair audit measures
    precision across independent individuals rather than repeat appearances of one lobbyist,
    and deterministically interleaves the clearly tracked planted negative control(s).
    """
    if planted_wrong_count < 1:
        raise ValueError("Precision-read batch requires at least 1 planted wrong negative control")
    if total_pairs <= planted_wrong_count:
        raise ValueError("total_pairs must exceed planted_wrong_count")

    rng = random.Random(seed)
    # Group real candidates by normalized name_key so each sampled real pair tests a distinct person.
    by_name: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in candidates:
        by_name[item["name_key"]].append({**dict(item), "is_planted_control": False})

    # Choose one deterministic representative appearance per person (preferring dated appearances).
    representatives: list[dict[str, Any]] = []
    for name_key in sorted(by_name):
        group = sorted(
            by_name[name_key],
            key=lambda c: (
                0 if c.get("hearing_date") else 1,
                -float(c.get("date_overlap_score") or 0.0),
                c["witness_key"],
                c["external_native_id"],
            ),
        )
        representatives.append(group[0])

    real_target = min(total_pairs - planted_wrong_count, len(representatives))
    sampled_real = rng.sample(representatives, real_target)

    # Ensure planted controls do not collide with sampled_real witness_keys where possible.
    used_witnesses = {c["witness_key"] for c in sampled_real}
    pool_for_controls = [c for c in representatives if c["witness_key"] not in used_witnesses]
    if len(pool_for_controls) < planted_wrong_count * 2:
        pool_for_controls = representatives
    planted = build_planted_wrong_controls(pool_for_controls, seed=seed, count=planted_wrong_count)

    combined = [*sampled_real, *planted]
    rng.shuffle(combined)

    overlap_counts = Counter(c.get("date_overlap_status") or "unknown" for c in candidates)
    sample_overlap_counts = Counter(c.get("date_overlap_status") or "unknown" for c in sampled_real)
    summary = {
        "population_appearances": len(candidates),
        "population_distinct_names": len({c["name_key"] for c in candidates}),
        "population_distinct_witness_keys": len({c["witness_key"] for c in candidates}),
        "population_distinct_lobbyists": len({c["external_native_id"] for c in candidates}),
        "population_blocked_middle_initial": (stats or {}).get("blocked_middle_initial", 0),
        "population_blocked_suffix": (stats or {}).get("blocked_suffix", 0),
        "sample_total_pairs": len(combined),
        "sample_real_pairs": len(sampled_real),
        "sample_planted_wrong_controls": len(planted),
        **{f"population_overlap_{k}": v for k, v in sorted(overlap_counts.items())},
        **{f"sample_overlap_{k}": v for k, v in sorted(sample_overlap_counts.items())},
    }

    return {
        "metadata": {
            "title": "ss-39zl: Seeded 50-Pair LDA Lobbyist <-> CT Witness Precision Read (Owner Approval Gate)",
            "rule_version": PERSON_RULE,
            "publication_status": "awaiting-owner-approval",
            "seed": seed,
            "ct_release_id": ct_release_id,
            "lobbying_generation_digest": lobbying_generation_digest,
            "population_appearances": summary["population_appearances"],
            "population_distinct_names": summary["population_distinct_names"],
            "population_distinct_lobbyists": summary["population_distinct_lobbyists"],
            "real_pair_count": len(sampled_real),
            "planted_control_count": len(planted),
            "summary": summary,
        },
        "items": combined,
    }


def load_live_lda_witness_candidates(
    *,
    r2_base_url: str | None = None,
    archive_origin: str = archive_release.ORIGIN,
    ct_release_id: str | None = None,
    user_agent: str = DEFAULT_USER_AGENT,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Query live/bound CT witnesses and live R2 lobbying tables via DuckDB httpfs projection pushdown.

    Performs zero full-file downloads: DuckDB reads only the required columns over HTTPS with
    an explicit ``User-Agent`` header. First extracts normalized witness names, then pushes a
    SQL surname/first-name pre-filter into the 10.4M-row LDA lobbyist Parquet so only matching
    lobbyist filings transfer over the wire.
    """
    base_url = resolve_r2_base_url(r2_base_url or "https://data.spicygov.ai")
    release_id = ct_release_id or archive_release.load_pins()["bound"]
    ct_base = f"{archive_origin.rstrip('/')}/releases/{release_id}"

    pub = httpx.get(
        f"{base_url}/publication.v2.json",
        headers={"User-Agent": user_agent},
        timeout=30,
    )
    pub.raise_for_status()
    pub_index = pub.json()
    lf = pub_index["families"]["lobbying-filings"]
    lda_base = f"{base_url}/{lf['prefix']}"
    lobbying_digest = lf["artifactDigest"]

    con = duckdb.connect(config={"custom_user_agent": user_agent})
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        witness_rows = con.execute(
            f"""
            WITH pkg_dates AS (
                SELECT d.package_id, min(l.document_date) AS pkg_date
                FROM read_parquet('{ct_base}/detail.parquet', file_row_number=true) d
                JOIN read_parquet('{ct_base}/list.parquet', file_row_number=true) l
                  ON l.file_row_number = d.file_row_number
                WHERE d.package_id IS NOT NULL AND l.document_date IS NOT NULL
                GROUP BY 1
            )
            SELECT
                w.witness_key,
                w.source,
                w.record_sha,
                w.source_locator,
                w.event_id,
                w.package_id,
                w.name,
                w.position,
                w.organization,
                w.witness_type,
                w.bioguide_id,
                w.matched_bioguide_id,
                w.superseded_by,
                coalesce(m.meeting_date, pd.pkg_date) AS hearing_date
            FROM read_parquet('{ct_base}/witnesses.parquet') w
            LEFT JOIN read_parquet('{ct_base}/meetings.parquet') m ON m.event_id = w.event_id
            LEFT JOIN pkg_dates pd ON pd.package_id = w.package_id
            WHERE w.superseded_by IS NULL
              AND w.name IS NOT NULL
              AND w.organization IS NOT NULL
              AND trim(w.organization) <> ''
              AND w.bioguide_id IS NULL
              AND w.matched_bioguide_id IS NULL
              AND coalesce(w.witness_type, '') NOT IN (
                  'statement_for_record', 'member_statement', 'record_material', 'member_appearance'
              )
            """
        ).fetchall()

        witness_cols = (
            "witness_key", "source", "record_sha", "source_locator", "event_id", "package_id",
            "name", "position", "organization", "witness_type", "bioguide_id",
            "matched_bioguide_id", "superseded_by", "hearing_date",
        )
        witness_dicts = [dict(zip(witness_cols, row, strict=True)) for row in witness_rows]

        # Collect candidate (first_token, last_token) pairs to selectively pre-filter
        # the 10.4M LDA lobbyist rows in DuckDB and aggregate per filing period before Python.
        witness_name_pairs: set[tuple[str, str]] = set()
        for w in witness_dicts:
            norm_w = normalize_person_name(w["name"])
            norm_o = normalize_organization(w["organization"])
            if norm_w.name_key and norm_o and len(norm_w.tokens) >= 2:
                witness_name_pairs.add((norm_w.tokens[0], norm_w.tokens[-1]))

        con.execute("CREATE TEMP TABLE witness_name_pairs(first_tok VARCHAR, last_tok VARCHAR)")
        con.executemany(
            "INSERT INTO witness_name_pairs VALUES (?, ?)",
            sorted(witness_name_pairs),
        )

        # Push exact (first_tok, last_tok) join + GROUP BY inside DuckDB so only matched
        # lobbyist-filing-org summaries cross into Python (runs in seconds over 10.4M rows).
        lda_rows = con.execute(
            f"""
            WITH filtered_lobbyists AS (
                SELECT
                    l.filing_uuid,
                    l.lobbyist_id,
                    l.prefix,
                    l.first_name,
                    l.middle_name,
                    l.last_name,
                    l.suffix,
                    l.covered_position
                FROM read_parquet('{lda_base}/lobbying_activity_lobbyists.parquet') l
                JOIN witness_name_pairs wp
                  ON wp.first_tok = list_extract(
                      list_filter(
                          string_split(
                              trim(regexp_replace(
                                  lower(replace(replace(strip_accents(l.first_name), '''', ''), '’', '')),
                                  '[^a-z0-9]+', ' ', 'g'
                              )),
                              ' '
                          ),
                          x -> x <> ''
                      ),
                      1
                  )
                 AND wp.last_tok = list_extract(
                      list_filter(
                          string_split(
                              trim(regexp_replace(
                                  lower(replace(replace(strip_accents(l.last_name), '''', ''), '’', '')),
                                  '[^a-z0-9]+', ' ', 'g'
                              )),
                              ' '
                          ),
                          x -> x <> '' AND x NOT IN ('jr', 'sr', 'ii', 'iii', 'iv')
                      ),
                      -1
                  )
                WHERE l.lobbyist_id IS NOT NULL
                  AND l.first_name IS NOT NULL
                  AND l.last_name IS NOT NULL
            )
            SELECT
                fl.lobbyist_id,
                fl.prefix,
                fl.first_name,
                fl.middle_name,
                fl.last_name,
                fl.suffix,
                any_value(fl.covered_position) AS covered_position,
                f.filing_year,
                f.filing_period,
                f.registrant_name,
                f.client_name,
                count(DISTINCT fl.filing_uuid) AS filing_count,
                list_slice(list(DISTINCT fl.filing_uuid ORDER BY fl.filing_uuid), 1, 5) AS sample_filing_uuids
            FROM filtered_lobbyists fl
            JOIN read_parquet('{lda_base}/lobbying_filings.parquet') f
              ON f.filing_uuid = fl.filing_uuid
            GROUP BY
                fl.lobbyist_id,
                fl.prefix,
                fl.first_name,
                fl.middle_name,
                fl.last_name,
                fl.suffix,
                f.filing_year,
                f.filing_period,
                f.registrant_name,
                f.client_name
            """
        ).fetchall()

        lda_cols = (
            "lobbyist_id", "prefix", "first_name", "middle_name", "last_name",
            "suffix", "covered_position", "filing_year", "filing_period", "registrant_name",
            "client_name", "filing_count", "sample_filing_uuids",
        )
        lda_dicts = [dict(zip(lda_cols, row, strict=True)) for row in lda_rows]
    finally:
        con.close()

    candidates, stats = match_lda_witness_candidates(
        witness_dicts,
        lda_dicts,
        ct_release_id=release_id,
        lobbying_generation_digest=lobbying_digest,
    )
    meta = {
        "ct_release_id": release_id,
        "lobbying_generation_digest": lobbying_digest,
        "stats": stats,
    }
    return candidates, meta


def main(argv: Sequence[str] | None = None) -> int:
    """CLI to generate or ``--check`` the seeded 50-pair LDA-witness precision read sheet."""
    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument("--seed", type=int, default=20261127, help="Deterministic RNG seed")
    parser.add_argument("--total-pairs", type=int, default=50, help="Total pairs in the precision read sheet")
    parser.add_argument("--planted-wrong", type=int, default=2, help="Number of clearly tracked planted wrong controls")
    parser.add_argument("--json-out", type=Path, required=True, help="Companion precision-read JSON path")
    parser.add_argument("--md-out", type=Path, required=True, help="Rendered Markdown precision-read sheet path")
    parser.add_argument("--check", action="store_true", help="Verify existing Markdown sheet matches JSON companion")
    parser.add_argument("--r2-base-url", default="https://data.spicygov.ai", help="Public R2 data base URL")
    args = parser.parse_args(argv)

    if args.check:
        batch = json.loads(args.json_out.read_text(encoding="utf-8"))
        rendered = render_person_batch_sheet(batch)
        existing = args.md_out.read_text(encoding="utf-8")
        if existing != rendered:
            raise SystemExit("Precision-read Markdown sheet disagrees with JSON companion")
        print(json.dumps({"status": "ok", "checked": str(args.md_out), "items": len(batch["items"])}))
        return 0

    candidates, meta = load_live_lda_witness_candidates(r2_base_url=args.r2_base_url)
    batch = build_precision_read_batch(
        candidates,
        seed=args.seed,
        total_pairs=args.total_pairs,
        planted_wrong_count=args.planted_wrong,
        ct_release_id=meta["ct_release_id"],
        lobbying_generation_digest=meta["lobbying_generation_digest"],
        stats=meta["stats"],
    )
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.md_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(batch, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    args.md_out.write_text(render_person_batch_sheet(batch), encoding="utf-8")
    print(json.dumps({"status": "written", "metadata": batch["metadata"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
