"""Resolve act-relative citations through the pinned OLRC Popular Names, Table III, and source-credit tables.

Vendored and adapted from RefSpec's ``refspec.registry.act_resolution`` and
``refspec.registry.citation_grammar`` (composite ``(law, division, enclosing_section)``
identity rule ``composite-law-division-enclosing-section-v1``, ``REVIEWED_ACT_ALIASES``,
and ``SourceCreditIndex``).

Every vendored Parquet file is verified against its pinned SHA-256 at load time
and read via DuckDB so lightweight serving environments without ``pyarrow`` work
identically.
"""

from __future__ import annotations

import hashlib
import json
import re
from bisect import bisect_left, bisect_right
from collections import OrderedDict
from collections.abc import Container, Iterable, Iterator, Mapping
from dataclasses import asdict, dataclass, field, replace
from functools import cached_property, lru_cache
from importlib.resources import files
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, NamedTuple, cast

import duckdb

__all__ = [
    "ALIAS_PRECEDENCE_RULE",
    "ALIAS_YEAR_RULE",
    "ANSWERING_SOURCES",
    "DIVISION_RULE",
    "ENCLOSING_SOURCE_IDENTITY_RULE",
    "GENERIC_ACT_STEM_STOPLIST",
    "REVIEWED_ACT_ALIASES",
    "SOURCE_COMPOSITION_RULE",
    "SOURCE_CREDIT_STATUSES",
    "UNRESOLVED_REASONS",
    "VENDORED_ACT_INDEX_DIR",
    "ActIndex",
    "ActRelativeCitation",
    "ActRelativeCitationOccurrence",
    "ActResolution",
    "Classification",
    "PopularNameRecord",
    "SourceCreditAnswer",
    "SourceCreditIndex",
    "SourceCreditTarget",
    "StatutoryShortTitleMatch",
    "act_name_absence_reason",
    "act_name_with_trailing_year",
    "act_resolution_pins",
    "canonical_usc_iri",
    "find_act_relative_citations",
    "find_act_relative_occurrences",
    "find_statutory_short_titles",
    "load_act_index",
    "load_source_credit_index",
    "normalize_popular_name",
    "resolve_act_name",
    "resolve_act_relative_citation",
    "stated_name_chain",
]

VENDORED_ACT_INDEX_DIR = Path(str(files("spicy_regs").joinpath("reference/refspec/usc-act-index")))

_ARTIFACT_PINS: Mapping[str, Mapping[str, str]] = {
    "usc-act-index-2026-08-22": {
        "usc-popular-names.parquet": "sha256:603d5b072133d8fe6802736aeaa70b9fb9832e4fb996158a083fae3ce1026a9a",
        "usc-act-sections.parquet": "sha256:9040d4460e90704aef911b4d4f704e9f432cebd7b58717f31f090a3008ca6159",
        "quarantine.parquet": "sha256:a89305172a27e682488e05e8fc82d8b0521d20a8fcee7d97a00331e8527acfac",
    },
    "usc-source-credit-index-2026-08-02": {
        "usc-source-credits.parquet": "sha256:d377545fe60d592a120bda30dffba665380cf82f826ae06cb327a581f0af9d8a",
    },
}

ALIAS_YEAR_RULE = "supply-trailing-year-when-exactly-one-act-supplies-it"
ALIAS_PRECEDENCE_RULE = "stated-cross-reference-before-derived-year-v1"
ALIAS_MAX_DEPTH = 8
DIVISION_RULE = "act-division-complete-pages-exclusion-v2"
ENCLOSING_SOURCE_IDENTITY_RULE = "composite-law-division-enclosing-section-v1"

REVIEWED_ACT_ALIASES: Mapping[str, str] = {
    "cercla": "comprehensive environmental response, compensation, and liability act of 1980",
}

_ENCLOSING_ACT_SECTION = re.compile(r"^(?P<enclosing>[1-9]\d*[A-Za-z]*)\[(?P<inner>[^\[\]()][^\[\]]*)\]$")
_TRAILING_SUBSECTION_PARENS = re.compile(r"(?:\([^()]+\))+$")

SOURCE_COMPOSITION_RULE = "table3-and-source-credits-plurality-and-disagreement-refuse-v2"
SOURCE_CREDIT_STATUSES = ("not_consulted", "no_key", "absent", "multi_target", "resolved")
ANSWERING_SOURCES = ("table3", "source_credits", "both")

_YEAR_SUFFIX = re.compile(r"\s+of\s+(?:1[789]|20)\d{2}$")
_LEADING_ARTICLE = re.compile(r"^the\s+")
_PUBLIC_LAW_KEY = re.compile(r"^[1-9]\d{0,2}-[1-9]\d*$")
_RKAF_USC_IRI = re.compile(r"^urn:rkaf:us:usc:[1-9][0-9]*:[1-9][0-9]*[a-z]*(?:-[0-9a-z]+)*$")
_PAGE_RANGE_OPEN_END = 1 << 30

UNRESOLVED_REASONS = (
    "act_name_ambiguous",
    "act_not_in_index",
    "act_listed_without_classification",
    "act_alias_target_not_listed",
    "source_incomplete",
    "act_section_not_classified",
    "classification_not_current",
    "usc_section_not_expressible",
    "act_section_ambiguous",
    "act_section_outside_act",
    "act_division_conflict",
    "sources_disagree",
)

# --------------------------------------------------------------------------- #
# Act-relative citation grammar helpers (from RefSpec citation_grammar)
# --------------------------------------------------------------------------- #

_DASHES = str.maketrans(dict.fromkeys("‐‑‒–—―−\x96\x97", "-"))
_NAME_EDGE = re.compile(r"^[\s(\"'“”]+|[\s,;:.)\"'“”]+$")
_CURLY_APOSTROPHE = re.compile(r"[’‘`]")
_YEAR_PREFIXED_NAME = re.compile(r"^\s*(?:the\s+)?((?:18|19|20)\d{2})\s+(\S.*)$", re.IGNORECASE)
_ZERO_PADDED_SECTION = re.compile(r"(?:^|(?<=[a-z]-))0+(?=\d)")

_ACT_SECTION = re.compile(r"(?:sec(?:tion)?s?\.?|§{1,2})\s*(?P<section>\d+[A-Za-z]?)", re.IGNORECASE)
_CITED_DIVISION = re.compile(r"(?i:\bdiv(?:ision)?\.?)\s+(?P<division>[A-Z]{1,3})\b")
_ACT_SECTION_OF_THE = re.compile(r"\s*(?:of\s+(?:the\s+)?|,\s*(?:the\s+)?|\s+)", re.IGNORECASE)
_ACT_SPACE = re.compile(r"\s*")
_ACT_PARENTHETICAL = re.compile(r"\([^()]{1,12}\)")
_CFR_PINPOINT_LABEL = re.compile(r"\(\s*([0-9A-Za-z]{1,4})\s*\)")
_CITATION_PARAGRAPH_BREAK = re.compile(r"\r?\n[ \t]*\r?\n")
_ACT_DIVISION_OF = re.compile(r"\s+of\s+(?:the\s+)?", re.IGNORECASE)
_ACT_NAME_GAP = re.compile(r"[\s,:\"'”’)]*")
_MAX_ACT_NAME_WORDS = 24


def _normalize_dashes(text: str) -> str:
    return text.translate(_DASHES)


def _usc_section(value: str | None) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\([^)]*\)", "", value.strip().lower())
    return _ZERO_PADDED_SECTION.sub("", text) or None


def normalize_popular_name(name: object) -> str:
    """Normalize a popular act name to its case-folded, dash- and quote-straightened join key."""
    text = _CURLY_APOSTROPHE.sub("'", _normalize_dashes(str(name or "")))
    text = _NAME_EDGE.sub("", text)
    return re.sub(r"\s+", " ", text).strip().lower()


def act_name_with_trailing_year(name: str) -> str | None:
    """Reorder ``'2020 PIPES Act'`` -> ``'PIPES Act of 2020'`` without inventing a year."""
    match = _YEAR_PREFIXED_NAME.match(name)
    return f"{match.group(2)} of {match.group(1)}" if match else None


@dataclass(frozen=True)
class ActRelativeCitation:
    """A provision cited through the Act that created it."""

    act_name: str
    act_key: str
    section: str
    division: str | None = None
    enclosing_section: str | None = None


@dataclass(frozen=True)
class ActRelativeCitationOccurrence:
    """An exact act-section mention; its labels are not mapped USC subsections."""

    citation: ActRelativeCitation
    start: int
    end: int
    text: str
    pinpoint: tuple[str, ...] = ()


def _act_name_span(
    document: str, words: list[re.Match[str]], act_names: Container[str], *, before: bool
) -> tuple[int, int, str, str] | None:
    for length in range(len(words), 0, -1):
        selected = words[-length:] if before else words[:length]
        start, end = selected[0].start(), selected[-1].end()
        raw = document[start:end]
        if _CITATION_PARAGRAPH_BREAK.search(raw):
            continue
        candidate = " ".join(raw.split())
        key = normalize_popular_name(candidate)
        if key not in act_names:
            reordered = act_name_with_trailing_year(key)
            if reordered is None or reordered not in act_names:
                continue
            key = reordered
        edges = list(_NAME_EDGE.finditer(raw))
        left = edges[0].end() if edges and edges[0].start() == 0 else 0
        right = edges[-1].start() if edges and edges[-1].end() == len(raw) else len(raw)
        return start + left, start + right, _NAME_EDGE.sub("", candidate), key
    return None


def find_act_relative_occurrences(
    text: object, *, act_names: Container[str] | Mapping[str, str]
) -> tuple[ActRelativeCitationOccurrence, ...]:
    """Read indexed act names with exact spans, repeats, and source pinpoints."""
    document = "" if text is None else str(text)
    words = list(re.finditer(r"\S+", document))
    starts, ends = [w.start() for w in words], [w.end() for w in words]
    found: list[ActRelativeCitationOccurrence] = []
    for marker in _ACT_SECTION.finditer(document):
        section = _usc_section(marker.group("section"))
        if section is None:
            continue
        end, labels = marker.end(), []
        while True:
            space = _ACT_SPACE.match(document, end)
            position = space.end() if space is not None else end
            if _CITATION_PARAGRAPH_BREAK.search(document, end, position):
                break
            parenthetical = _ACT_PARENTHETICAL.match(document, position)
            if parenthetical is None:
                break
            label = _CFR_PINPOINT_LABEL.fullmatch(document, position, parenthetical.end())
            if label is not None and not (position > end and label.group(1).isdigit() and len(label.group(1)) == 4):
                labels.append(label.group(1))
            end = parenthetical.end()
        left = bisect_right(ends, marker.start())
        named = _act_name_span(document, words[max(0, left - _MAX_ACT_NAME_WORDS) : left], act_names, before=True)
        if named is not None and (
            not _ACT_NAME_GAP.fullmatch(document, named[1], marker.start())
            or _CITATION_PARAGRAPH_BREAK.search(document, named[1], marker.start())
        ):
            named = None
        if named is None:
            opening = _ACT_SECTION_OF_THE.match(document, end)
            if opening is None or _CITATION_PARAGRAPH_BREAK.search(document, end, opening.end()):
                continue
            right = bisect_left(starts, opening.end())
            named = _act_name_span(document, words[right : right + _MAX_ACT_NAME_WORDS], act_names, before=False)
        if named is None:
            continue
        name_start, name_end, name, key = named
        start, end = min(marker.start(), name_start), max(end, name_end)
        division = None
        if name_start == start:
            prefix = words[max(0, bisect_right(ends, start) - 4)].start()
            for stated in _CITED_DIVISION.finditer(document, prefix, start):
                if _ACT_DIVISION_OF.fullmatch(document, stated.end(), start):
                    division, start = stated.group("division"), stated.start()
                    break
        if isinstance(act_names, Mapping):
            key = cast(Mapping[str, str], act_names)[key]
        citation = ActRelativeCitation(name, key, section, division)
        found.append(ActRelativeCitationOccurrence(citation, start, end, document[start:end], tuple(labels)))
    return tuple(found)


def find_act_relative_citations(
    text: object, *, act_names: Container[str] | Mapping[str, str]
) -> tuple[ActRelativeCitation, ...]:
    """Deduplicated ``ActRelativeCitation`` values in source order."""
    return tuple(dict.fromkeys(item.citation for item in find_act_relative_occurrences(text, act_names=act_names)))


# --------------------------------------------------------------------------- #
# Pinned Parquet loading via DuckDB (no pyarrow dependency)
# --------------------------------------------------------------------------- #


def canonical_usc_iri(title: object, section: object) -> str:
    """Mint ``urn:rkaf:us:usc:{title}:{section}`` inside the ``rkaf:us-usc`` lexical space."""
    title_text = str(title or "").strip().lstrip("0")
    section_text = re.sub(r"\([^)]*\)", "", str(section or "").strip().lower())
    iri = f"urn:rkaf:us:usc:{title_text}:{section_text}"
    if not _RKAF_USC_IRI.fullmatch(iri):
        raise ValueError(f"invalid U.S.C. identifier components: title={title!r}, section={section!r}")
    return iri


def _pinned_digests(name: str) -> Mapping[str, str]:
    stated = {artifact: tables[name] for artifact, tables in _ARTIFACT_PINS.items() if name in tables}
    if not stated:
        raise KeyError(name)
    return stated


def _verify_pinned_bytes(path: Path) -> tuple[frozenset[str], bytes]:
    data = path.read_bytes()
    digest = f"sha256:{hashlib.sha256(data).hexdigest()}"
    matched = frozenset(a for a, pin in _pinned_digests(path.name).items() if pin == digest)
    if not matched:
        expected = sorted(set(_pinned_digests(path.name).values()))
        raise ValueError(f"pinned act artifact drifted: {path.name}; expected={expected}, observed={digest}")
    return matched, data


def _query_parquet_bytes(
    data: bytes, name: str, sql_template: str, params: list[Any] | None = None
) -> tuple[list[str], list[tuple[Any, ...]]]:
    """Execute a DuckDB query against SHA-256-verified Parquet bytes without importing pyarrow."""
    with TemporaryDirectory(prefix="spicy-regs-act-") as tmp_dir:
        admitted = Path(tmp_dir) / name
        admitted.write_bytes(data)
        with duckdb.connect() as con:
            cursor = con.execute(sql_template.format(path=str(admitted).replace("'", "''")), params or [])
            columns = [col[0] for col in cursor.description]
            return columns, cursor.fetchall()


def _read_pinned_parquet_rows(
    directory: Path, name: str, sql_template: str = "SELECT * FROM read_parquet('{path}')", params: list[Any] | None = None
) -> tuple[frozenset[str], list[str], list[tuple[Any, ...]]]:
    """Verify SHA-256 and read Parquet tuple rows through DuckDB without importing pyarrow."""
    matched, data = _verify_pinned_bytes(directory / name)
    columns, rows = _query_parquet_bytes(data, name, sql_template, params)
    return matched, columns, rows


def _read_pinned_parquet(directory: Path, name: str) -> tuple[frozenset[str], list[dict[str, Any]]]:
    """Verify SHA-256 and read Parquet rows through DuckDB without importing pyarrow."""
    matched, columns, rows = _read_pinned_parquet_rows(directory, name)
    return matched, [dict(zip(columns, row, strict=True)) for row in rows]


def act_resolution_pins() -> dict[str, str]:
    """Return pinned SHA-256 digests for all vendored act-index and source-credit tables."""
    return {
        **dict(_ARTIFACT_PINS["usc-act-index-2026-08-22"]),
        **dict(_ARTIFACT_PINS["usc-source-credit-index-2026-08-02"]),
    }


# --------------------------------------------------------------------------- #
# Core index and resolution types
# --------------------------------------------------------------------------- #


class Classification(NamedTuple):
    """One Table III row: where an act section landed in the U.S. Code."""

    usc_title: str | None
    usc_section: str | None
    status: str | None
    statutes_at_large_page: int | None


@dataclass(frozen=True)
class PopularNameRecord:
    """One popular name and one source statement; shared by builder and lookup."""

    name: str
    content_type: str
    table3_key: str | None = None
    usc_title: str | None = None
    usc_section: str | None = None
    see_also: str | None = None
    release_point: str | None = None
    division: str | None = None
    statutes_at_large_volume: str | None = None
    statutes_at_large_page: str | None = None
    statutes_at_large_witness: str | None = None
    refused_usc_anchor: str | None = None
    enclosing_section: str | None = None


@dataclass(frozen=True)
class SourceCreditTarget:
    """One U.S. Code section a source credit says an act section added."""

    usc_title: str
    usc_section: str
    statutes_at_large_volume: str | None = None
    statutes_at_large_page: str | None = None


@dataclass(frozen=True)
class SourceCreditAnswer:
    """What the second source had to say about one ``(law, division[, enclosing_section], section)``."""

    status: str
    usc_title: str | None = None
    usc_section: str | None = None
    statutes_at_large_volume: str | None = None
    statutes_at_large_page: str | None = None
    targets: tuple[SourceCreditTarget, ...] = ()

    def __post_init__(self) -> None:
        if self.status not in SOURCE_CREDIT_STATUSES:
            raise ValueError(f"undeclared source-credit status: {self.status!r}")


@dataclass(frozen=True)
class ActResolution:
    """What an act-relative citation resolved to, or why it did not."""

    citation: ActRelativeCitation
    act_key: str | None = None
    table3_key: str | None = None
    usc_title: str | None = None
    usc_section: str | None = None
    iri: str | None = None
    unresolved_reason: str | None = None
    answered_by: str | None = None
    table3_reason: str | None = None
    source_credit_status: str = "not_consulted"
    statutes_at_large_volume: str | None = None
    statutes_at_large_page: str | None = None
    source_credit_targets: tuple[SourceCreditTarget, ...] = ()
    conflicting_targets: Mapping[str, str] = field(default_factory=dict)
    table3_candidate_iri: str | None = None
    act_division: str | None = None
    enclosing_section: str | None = None
    name_sources: tuple[PopularNameRecord, ...] = ()
    candidate_resolutions: tuple[ActResolution, ...] = ()

    def __post_init__(self) -> None:
        if (self.iri is None) == (self.unresolved_reason is None):
            raise ValueError("a resolution states an identifier or a reason, never both or neither")
        if self.unresolved_reason is not None and self.unresolved_reason not in UNRESOLVED_REASONS:
            raise ValueError(f"undeclared unresolved reason: {self.unresolved_reason!r}")
        if (self.answered_by is None) != (self.iri is None):
            raise ValueError("an identifier names the source that produced it, and a refusal names none")
        if self.answered_by is not None and self.answered_by not in ANSWERING_SOURCES:
            raise ValueError(f"undeclared answering source: {self.answered_by!r}")
        if self.source_credit_status not in SOURCE_CREDIT_STATUSES:
            raise ValueError(f"undeclared source-credit status: {self.source_credit_status!r}")
        if self.table3_reason is not None and self.table3_reason not in UNRESOLVED_REASONS:
            raise ValueError(f"undeclared Table III reason: {self.table3_reason!r}")
        if self.conflicting_targets and (
            self.unresolved_reason != "sources_disagree"
            or set(self.conflicting_targets) != {"table3", "source_credits"}
            or len(set(self.conflicting_targets.values())) != 2
        ):
            raise ValueError("conflicting targets require two distinct source answers and a disagreement refusal")
        if self.table3_candidate_iri is not None and (
            self.unresolved_reason != "act_section_ambiguous" or self.source_credit_status != "multi_target"
        ):
            raise ValueError("a Table III candidate requires a multiple-target refusal")
        if self.candidate_resolutions and (
            self.unresolved_reason != "act_name_ambiguous"
            or len(self.candidate_resolutions) < 2
            or any(c.candidate_resolutions or c.citation != self.citation for c in self.candidate_resolutions)
        ):
            raise ValueError("name candidates require an ambiguity refusal and unnested lookups of the same citation")


def _extract_enclosing_sections(sections: Iterable[str]) -> frozenset[str]:
    """Return enclosing sections hosting an enacted Act inside a law's section set."""
    section_set = sections if isinstance(sections, (set, frozenset, Mapping)) else set(sections)
    by_enc: dict[str, set[str]] = {}
    for sec in section_set:
        match = _ENCLOSING_ACT_SECTION.fullmatch(sec)
        if match is not None:
            by_enc.setdefault(match.group("enclosing"), set()).add(match.group("inner"))
    return frozenset(
        enc
        for enc, inners in by_enc.items()
        if len(inners) >= 2 or any(inner in section_set for inner in inners)
    )


class _LazyActSections(Mapping[str, Mapping[str, tuple[Classification, ...]]]):
    """Lazy, bounded-memory Table III classification lookup backed by SHA-256-verified Parquet bytes."""

    def __init__(self, name: str, parquet_bytes: bytes, *, max_cached_laws: int = 512) -> None:
        self._name = name
        self._parquet_bytes = parquet_bytes
        self._max_cached_laws = max_cached_laws
        self._cache: OrderedDict[str, dict[str, tuple[Classification, ...]]] = OrderedDict()
        self._enclosing_cache: dict[str, frozenset[str]] | None = None

    def _load_law(self, table3_key: str) -> dict[str, tuple[Classification, ...]]:
        cached = self._cache.get(table3_key)
        if cached is not None:
            self._cache.move_to_end(table3_key)
            return cached
        _, rows = _query_parquet_bytes(
            self._parquet_bytes,
            self._name,
            "SELECT act_section, usc_title, usc_section, status, statutes_at_large_page "
            "FROM read_parquet('{path}') WHERE table3_key = ?",
            [table3_key],
        )
        if not rows:
            raise KeyError(table3_key)
        by_section: dict[str, tuple[Classification, ...]] = {}
        for act_section, usc_title, usc_section, status, page_val in rows:
            by_section[act_section] = (
                *by_section.get(act_section, ()),
                Classification(usc_title, usc_section, status, int(page_val) if page_val else None),
            )
        self._cache[table3_key] = by_section
        if len(self._cache) > self._max_cached_laws:
            self._cache.popitem(last=False)
        return by_section

    def __getitem__(self, table3_key: str) -> Mapping[str, tuple[Classification, ...]]:
        return self._load_law(table3_key)

    def __contains__(self, table3_key: object) -> bool:
        if not isinstance(table3_key, str):
            return False
        if table3_key in self._cache:
            return True
        try:
            self._load_law(table3_key)
            return True
        except KeyError:
            return False

    def __iter__(self) -> Iterator[str]:
        _, rows = _query_parquet_bytes(
            self._parquet_bytes,
            self._name,
            "SELECT DISTINCT table3_key FROM read_parquet('{path}') ORDER BY table3_key",
        )
        for (law,) in rows:
            yield law

    def __len__(self) -> int:
        _, rows = _query_parquet_bytes(
            self._parquet_bytes,
            self._name,
            "SELECT count(DISTINCT table3_key) FROM read_parquet('{path}')",
        )
        return int(rows[0][0])

    def enclosing_sections_by_law(self) -> Mapping[str, frozenset[str]]:
        if self._enclosing_cache is not None:
            return self._enclosing_cache
        _, rows = _query_parquet_bytes(
            self._parquet_bytes,
            self._name,
            "WITH bracket_laws AS ("
            "SELECT DISTINCT table3_key FROM read_parquet('{path}') WHERE strpos(act_section, '[') > 0"
            ") "
            "SELECT DISTINCT s.table3_key, s.act_section "
            "FROM read_parquet('{path}') s "
            "JOIN bracket_laws b ON s.table3_key = b.table3_key",
        )
        by_law: dict[str, set[str]] = {}
        for law, sec in rows:
            by_law.setdefault(law, set()).add(sec)
        found: dict[str, frozenset[str]] = {}
        for law, sections in by_law.items():
            enclosing = _extract_enclosing_sections(sections)
            if enclosing:
                found[law] = enclosing
        self._enclosing_cache = found
        return found


@dataclass(frozen=True)
class ActIndex:
    """The Popular Names and Table III joins, loaded from pinned Parquets or built for tests."""

    table3_key_by_name: Mapping[str, str | None] = field(default_factory=dict)
    name_candidates: Mapping[str, tuple[PopularNameRecord, ...]] = field(default_factory=dict)
    alias_by_name: Mapping[str, str] = field(default_factory=dict)
    classifications: Mapping[str, Mapping[str, tuple[Classification, ...]]] = field(default_factory=dict)
    incomplete_sources: frozenset[str] = frozenset()
    cited_names: frozenset[str] = frozenset()
    division_by_name: Mapping[str, tuple[str, int | None]] = field(default_factory=dict)
    division_starts: Mapping[str, tuple[tuple[str, int], ...]] = field(default_factory=dict)
    narrowed_page_sections: frozenset[tuple[str, str]] = frozenset()
    enclosing_section_by_name: Mapping[str, str] = field(default_factory=dict)

    @cached_property
    def act_names(self) -> frozenset[str]:
        """Every normalized popular name and stated/reviewed alias this index recognizes."""
        return (
            frozenset(self.table3_key_by_name)
            | frozenset(self.alias_by_name)
            | frozenset(alias for alias, target in REVIEWED_ACT_ALIASES.items() if target in self.table3_key_by_name)
        )

    @cached_property
    def enclosing_sections_by_law(self) -> Mapping[str, frozenset[str]]:
        """Table III key -> enclosing sections hosting an enacted Act inside that law."""
        if isinstance(self.classifications, _LazyActSections):
            return self.classifications.enclosing_sections_by_law()
        found: dict[str, frozenset[str]] = {}
        for law, sections in self.classifications.items():
            enclosing = _extract_enclosing_sections(sections)
            if enclosing:
                found[law] = enclosing
        return found

    @cached_property
    def acts_supplying_year(self) -> Mapping[str, tuple[str, ...]]:
        """Stem -> the listed acts that complete it with a trailing year."""
        by_stem: dict[str, list[str]] = {}
        for known in self.table3_key_by_name:
            stem = _YEAR_SUFFIX.sub("", known)
            if stem != known:
                by_stem.setdefault(stem, []).append(known)
        return {stem: tuple(acts) for stem, acts in by_stem.items()}

    def act_page_range(self, act_key: str) -> tuple[int, int] | None:
        """The Statutes at Large pages an act's division occupies, or ``None`` if unbounded."""
        if act_key in self.name_candidates:
            return None
        stated = self.division_by_name.get(act_key)
        if stated is None:
            return None
        division = stated[0]
        starts = self.division_starts.get(self.table3_key_by_name.get(act_key) or "", ())
        start = min((page for name, page in starts if name == division), default=None)
        if start is None:
            return None
        return (start, min((page for _, page in starts if page > start), default=_PAGE_RANGE_OPEN_END))

    def find_statutory_short_titles(
        self, text: str, *, deduplicate: bool = True
    ) -> tuple[StatutoryShortTitleMatch, ...]:
        """Extract enacted statutory short titles from unstructured prose via maximal-NP scanning."""
        return find_statutory_short_titles(text, self, deduplicate=deduplicate)

    def scan_text(self, text: str, *, deduplicate: bool = True) -> tuple[StatutoryShortTitleMatch, ...]:
        """Alias for :meth:`find_statutory_short_titles` over unstructured prose."""
        return find_statutory_short_titles(text, self, deduplicate=deduplicate)

    @classmethod
    def from_directory(cls, artifact_dir: Path | str, *, require_quarantine: bool = True) -> ActIndex:
        """Load from a pinned act-index directory, verifying every SHA-256."""
        directory = Path(artifact_dir)
        receipt_path = directory / "receipt.json"
        incomplete_sources: frozenset[str] = frozenset()
        if receipt_path.is_file():
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            incomplete_sources = frozenset(hole["table3_key"] for hole in receipt.get("source_incomplete", ()))

        sec_artifacts, sec_bytes = _verify_pinned_bytes(directory / "usc-act-sections.parquet")
        quarantine_path = directory / "quarantine.parquet"
        narrowed: frozenset[tuple[str, str]] = frozenset()
        if quarantine_path.is_file() or require_quarantine:
            q_artifacts, _, q_rows = _read_pinned_parquet_rows(
                directory,
                "quarantine.parquet",
                "SELECT table3_key, raw_value FROM read_parquet('{path}') "
                "WHERE reason = 'statutes_at_large_page_span_narrowed'",
            )
            if not (sec_artifacts & q_artifacts):
                raise ValueError("classifications and quarantine must belong to the same act artifact")
            narrowed = frozenset((table3_key, raw_value.partition(" -> ")[0]) for table3_key, raw_value in q_rows)

        _, pop_cols, pop_rows = _read_pinned_parquet_rows(directory, "usc-popular-names.parquet")
        col_idx = {col: idx for idx, col in enumerate(pop_cols)}
        idx_name = col_idx["name"]
        idx_name_key = col_idx["name_key"]
        idx_content_type = col_idx["content_type"]
        idx_table3_key = col_idx["table3_key"]
        idx_usc_title = col_idx.get("usc_title")
        idx_usc_section = col_idx.get("usc_section")
        idx_see_also = col_idx.get("see_also")
        idx_see_also_key = col_idx["see_also_key"]
        idx_release_point = col_idx.get("release_point")
        idx_division = col_idx.get("division")
        idx_sal_vol = col_idx.get("statutes_at_large_volume")
        idx_sal_page = col_idx.get("statutes_at_large_page")
        idx_enclosing = col_idx.get("enclosing_section")

        records_by_name: dict[str, set[PopularNameRecord]] = {}
        alias_by_name: dict[str, str] = {}
        cited_names: set[str] = set()
        starts: dict[str, dict[str, int]] = {}
        for row in pop_rows:
            name = normalize_popular_name(row[idx_name_key])
            if row[idx_see_also_key]:
                alias_by_name.setdefault(name, normalize_popular_name(row[idx_see_also_key]))
            if row[idx_content_type] != "cite":
                continue
            cited_names.add(name)
            t3_key = row[idx_table3_key]
            division_val = row[idx_division] if idx_division is not None else None
            sal_page_val = row[idx_sal_page] if idx_sal_page is not None else None
            if t3_key:
                record = PopularNameRecord(
                    name=row[idx_name],
                    content_type=row[idx_content_type],
                    table3_key=t3_key,
                    usc_title=row[idx_usc_title] if idx_usc_title is not None else None,
                    usc_section=row[idx_usc_section] if idx_usc_section is not None else None,
                    see_also=row[idx_see_also] if idx_see_also is not None else None,
                    release_point=row[idx_release_point] if idx_release_point is not None else None,
                    division=division_val,
                    statutes_at_large_volume=row[idx_sal_vol] if idx_sal_vol is not None else None,
                    statutes_at_large_page=sal_page_val,
                    enclosing_section=row[idx_enclosing] if idx_enclosing is not None else None,
                )
                records_by_name.setdefault(name, set()).add(record)
            if not (division_val and sal_page_val):
                continue
            page = int(sal_page_val)
            if t3_key:
                by_division = starts.setdefault(t3_key, {})
                by_division[division_val] = min(by_division.get(division_val, page), page)

        table3_key_by_name: dict[str, str | None] = {}
        division_by_name: dict[str, tuple[str, int | None]] = {}
        enclosing_section_by_name: dict[str, str] = {}
        name_candidates: dict[str, tuple[PopularNameRecord, ...]] = {}
        for name, records in records_by_name.items():
            laws = {r.table3_key for r in records}
            table3_key_by_name[name] = next(iter(laws)) if len(laws) == 1 else None
            identities = {(r.table3_key, r.division, r.enclosing_section) for r in records}
            if len(identities) > 1:
                name_candidates[name] = tuple(
                    sorted(records, key=lambda r: tuple(str(v or "") for v in asdict(r).values()))
                )
            else:
                _, division, enclosing = next(iter(identities))
                if division:
                    div_page = min(
                        (int(r.statutes_at_large_page) for r in records if r.statutes_at_large_page), default=None
                    )
                    division_by_name[name] = (division, div_page)
                if enclosing:
                    enclosing_section_by_name[name] = enclosing

        return cls(
            table3_key_by_name=table3_key_by_name,
            name_candidates=name_candidates,
            alias_by_name=alias_by_name,
            classifications=_LazyActSections("usc-act-sections.parquet", sec_bytes),
            incomplete_sources=incomplete_sources,
            cited_names=frozenset(cited_names),
            division_by_name=division_by_name,
            division_starts={
                key: tuple(sorted(by_division.items(), key=lambda item: (item[1], item[0])))
                for key, by_division in starts.items()
            },
            narrowed_page_sections=narrowed,
            enclosing_section_by_name=enclosing_section_by_name,
        )

    @classmethod
    def from_vendored(cls) -> ActIndex:
        """Load from the SHA-256-pinned vendored ``usc-act-index/`` directory."""
        return cls.from_directory(VENDORED_ACT_INDEX_DIR)


@dataclass(frozen=True)
class SourceCreditIndex:
    """The U.S. Code's own source credits, keyed by ``(law, division[, enclosing_section], section)``."""

    targets: Mapping[tuple[str, str, str], tuple[SourceCreditTarget, ...]] = field(default_factory=dict)
    enclosing_targets: Mapping[tuple[str, str | None, str, str], tuple[SourceCreditTarget, ...]] = field(
        default_factory=dict
    )

    @classmethod
    def from_rows(cls, rows: Any) -> SourceCreditIndex:
        collected: dict[tuple[str, str, str], list[SourceCreditTarget]] = {}
        enclosing_collected: dict[tuple[str, str | None, str, str], list[SourceCreditTarget]] = {}
        for row in rows:
            if len(row) == 8:
                law, division, enclosing_section, act_section, usc_title, usc_section, volume, page = row
            else:
                law, division, act_section, usc_title, usc_section, volume, page = row
                enclosing_section = None
                if match := _ENCLOSING_ACT_SECTION.fullmatch(str(act_section or "")):
                    enclosing_section = match.group("enclosing")
                    act_section = match.group("inner")
            target = SourceCreditTarget(usc_title, usc_section, volume, page)
            if enclosing_section:
                bucket = enclosing_collected.setdefault((law, division or None, enclosing_section, act_section), [])
            else:
                bucket = collected.setdefault((law, division, act_section), [])
            if target not in bucket:
                bucket.append(target)
        return cls(
            targets={k: tuple(v) for k, v in collected.items()},
            enclosing_targets={k: tuple(v) for k, v in enclosing_collected.items()},
        )

    @classmethod
    def from_directory(cls, artifact_dir: Path | str) -> SourceCreditIndex:
        """Load from a pinned directory carrying ``usc-source-credits.parquet``."""
        _, cols, rows = _read_pinned_parquet_rows(Path(artifact_dir), "usc-source-credits.parquet")
        col_idx = {col: idx for idx, col in enumerate(cols)}
        i_law = col_idx["public_law"]
        i_div = col_idx["division"]
        i_enc = col_idx.get("enclosing_section")
        i_sec = col_idx["act_section"]
        i_title = col_idx["usc_title"]
        i_usec = col_idx["usc_section"]
        i_vol = col_idx["statutes_at_large_volume"]
        i_page = col_idx["statutes_at_large_page"]
        return cls.from_rows(
            (
                row[i_law],
                row[i_div],
                row[i_enc] if i_enc is not None else None,
                row[i_sec],
                row[i_title],
                row[i_usec],
                row[i_vol],
                row[i_page],
            )
            for row in rows
        )

    @classmethod
    def from_vendored(cls) -> SourceCreditIndex:
        """Load from the SHA-256-pinned vendored ``usc-act-index/usc-source-credits.parquet``."""
        return cls.from_directory(VENDORED_ACT_INDEX_DIR)

    def targets_for(
        self,
        public_law: str,
        division: str | None,
        act_section: str,
        *,
        enclosing_section: str | None = None,
    ) -> tuple[SourceCreditTarget, ...]:
        if enclosing_section:
            return self.enclosing_targets.get((public_law, division or None, enclosing_section, act_section), ())
        if not division:
            return ()
        return self.targets.get((public_law, division, act_section), ())

    def lookup(
        self,
        public_law: str | None,
        division: str | None,
        act_section: str,
        *,
        enclosing_section: str | None = None,
    ) -> SourceCreditAnswer:
        """Consult the source credits with composite key discrimination."""
        if enclosing_section:
            if not public_law:
                return SourceCreditAnswer(status="no_key")
            found = self.targets_for(public_law, division, act_section, enclosing_section=enclosing_section)
        else:
            if not public_law or not division:
                return SourceCreditAnswer(status="no_key")
            found = self.targets_for(public_law, division, act_section)
        if not found:
            return SourceCreditAnswer(status="absent")
        if len({(t.usc_title, t.usc_section) for t in found}) > 1:
            return SourceCreditAnswer(status="multi_target", targets=found)
        target = found[0]
        return SourceCreditAnswer(
            status="resolved",
            usc_title=target.usc_title,
            usc_section=target.usc_section,
            statutes_at_large_volume=target.statutes_at_large_volume,
            statutes_at_large_page=target.statutes_at_large_page,
        )


def stated_name_chain(name: str, index: ActIndex) -> tuple[str, ...]:
    """The names the tool's own cross-references reach, the query first."""
    chain: list[str] = []
    seen: set[str] = set()
    current = normalize_popular_name(name)
    while current not in seen and len(chain) < ALIAS_MAX_DEPTH:
        chain.append(current)
        seen.add(current)
        stated = index.alias_by_name.get(current)
        if stated is None:
            break
        current = stated
    return tuple(chain)


def resolve_act_name(name: str, index: ActIndex) -> str | None:
    """Resolve a popular name to its indexed act key following ``ALIAS_PRECEDENCE_RULE``."""
    chain = stated_name_chain(name, index)
    for step in chain:
        if step in index.table3_key_by_name:
            return step
    for step in chain:
        stripped = _LEADING_ARTICLE.sub("", step)
        if stripped != step and stripped in index.table3_key_by_name:
            return stripped
    for step in chain:
        reviewed = REVIEWED_ACT_ALIASES.get(step)
        if reviewed is not None and reviewed in index.table3_key_by_name:
            return reviewed
    for step in chain:
        supplied = index.acts_supplying_year.get(step, ())
        if len(supplied) == 1:
            return supplied[0]
    return None


@dataclass(frozen=True)
class _Verdict:
    iri: str | None = None
    usc_title: str | None = None
    usc_section: str | None = None
    reason: str | None = None
    statutes_at_large_volume: str | None = None
    statutes_at_large_page: str | None = None

    def __post_init__(self) -> None:
        if self.iri is not None and self.reason is not None:
            raise ValueError("a source states an identifier or a reason, never both")


_SILENT = _Verdict()


def _lookup_table3_rows(
    law_sections: Mapping[str, tuple[Classification, ...]],
    section: str,
    *,
    enclosing_section: str | None,
    has_enclosing_structure: bool,
) -> tuple[str, tuple[Classification, ...], str | None]:
    """Resolve Table III classification rows respecting composite enclosing-section identity."""
    stripped = section.strip()
    explicit = _ENCLOSING_ACT_SECTION.fullmatch(stripped)
    if explicit is not None:
        enc, inner = explicit.group("enclosing"), explicit.group("inner")
        if enclosing_section is not None and enc != enclosing_section:
            return stripped, (), "act_section_ambiguous"
        rows = law_sections.get(stripped, ())
        if rows:
            return stripped, rows, None
        base_inner = _TRAILING_SUBSECTION_PARENS.sub("", inner)
        if base_inner != inner and not any(k.startswith(f"{enc}[{base_inner}(") for k in law_sections):
            fallback_key = f"{enc}[{base_inner}]"
            rows = law_sections.get(fallback_key, ())
            if rows:
                return fallback_key, rows, None
        return stripped, (), "act_section_not_classified"

    if enclosing_section is not None:
        enclosed_key = f"{enclosing_section}[{stripped}]"
        rows = law_sections.get(enclosed_key, ())
        if rows:
            return enclosed_key, rows, None
        base = _TRAILING_SUBSECTION_PARENS.sub("", stripped)
        if base != stripped and not any(k.startswith(f"{enclosing_section}[{base}(") for k in law_sections):
            fallback_key = f"{enclosing_section}[{base}]"
            rows = law_sections.get(fallback_key, ())
            if rows:
                return fallback_key, rows, None
        return enclosed_key, (), "act_section_not_classified"

    if has_enclosing_structure:
        return stripped, (), "act_section_ambiguous"

    rows = law_sections.get(section, ())
    if rows:
        return section, rows, None
    base = _TRAILING_SUBSECTION_PARENS.sub("", stripped)
    if base != stripped and not any(k.startswith(f"{base}(") for k in law_sections):
        rows = law_sections.get(base, ())
        if rows:
            return base, rows, None
    return section, (), "act_section_not_classified"


def _resolve_through_table3(
    citation: ActRelativeCitation,
    index: ActIndex,
    act_key: str,
    table3_key: str,
    *,
    enclosing_section: str | None = None,
) -> _Verdict:
    """Table III's verdict. Always an identifier or a reason, never silence."""
    if table3_key in index.incomplete_sources:
        return _Verdict(reason="source_incomplete")
    effective_enclosing = (
        enclosing_section if enclosing_section is not None else getattr(citation, "enclosing_section", None)
    )
    law_sections = index.classifications.get(table3_key, {})
    enclosing_by_law = getattr(index, "enclosing_sections_by_law", {})
    lookup_key, rows, lookup_reason = _lookup_table3_rows(
        law_sections,
        citation.section,
        enclosing_section=effective_enclosing,
        has_enclosing_structure=table3_key in enclosing_by_law,
    )
    if lookup_reason is not None:
        return _Verdict(reason=lookup_reason)
    page_range = index.act_page_range(act_key)
    if page_range is not None and (table3_key, lookup_key) not in index.narrowed_page_sections:
        low, high = page_range
        if all(page is not None and not low <= page <= high for *_, page in rows):
            return _Verdict(reason="act_section_outside_act")
    if len(rows) > 1:
        return _Verdict(reason="act_section_ambiguous")
    usc_title, usc_section, status, page = rows[0]
    if status:
        return _Verdict(reason="classification_not_current")
    if not (usc_title and usc_section):
        return _Verdict(reason="act_section_not_classified")
    try:
        iri = canonical_usc_iri(usc_title, usc_section)
    except ValueError:
        return _Verdict(usc_title=usc_title, usc_section=usc_section, reason="usc_section_not_expressible")
    return _Verdict(
        iri=iri,
        usc_title=usc_title,
        usc_section=usc_section,
        statutes_at_large_page=str(page) if page is not None else None,
    )


def _verdict_from_credits(credit: SourceCreditAnswer) -> _Verdict:
    if credit.status != "resolved":
        return _SILENT
    provenance = {
        "usc_title": credit.usc_title,
        "usc_section": credit.usc_section,
        "statutes_at_large_volume": credit.statutes_at_large_volume,
        "statutes_at_large_page": credit.statutes_at_large_page,
    }
    try:
        return _Verdict(iri=canonical_usc_iri(credit.usc_title, credit.usc_section), **provenance)
    except ValueError:
        return _Verdict(reason="usc_section_not_expressible", **provenance)


def act_name_absence_reason(name: str, index: ActIndex) -> str:
    """Classify why ``resolve_act_name`` returned ``None``."""
    chain = stated_name_chain(name, index)
    if any(step in index.cited_names for step in chain):
        return "act_listed_without_classification"
    if len(chain) > 1:
        return "act_alias_target_not_listed"
    return "act_not_in_index"


def resolve_act_relative_citation(
    citation: ActRelativeCitation,
    *,
    index: ActIndex,
    source_credits: SourceCreditIndex | None = None,
) -> ActResolution:
    """Resolve one act-relative citation to a U.S.C. identifier, or refuse cleanly with its reason."""
    act_key = resolve_act_name(citation.act_key, index)
    if act_key is None:
        return ActResolution(citation, unresolved_reason=act_name_absence_reason(citation.act_key, index))
    sources = index.name_candidates.get(act_key, ())
    if sources:
        groups: dict[tuple[str | None, str | None, str | None], list[PopularNameRecord]] = {}
        division_compatible = False
        for source in sources:
            if citation.division and source.division and citation.division != source.division:
                continue
            division_compatible = True
            if (
                citation.enclosing_section
                and source.enclosing_section
                and citation.enclosing_section != source.enclosing_section
            ):
                continue
            identity = (
                source.table3_key,
                source.division or citation.division,
                source.enclosing_section or citation.enclosing_section,
            )
            groups.setdefault(identity, []).append(source)
        candidates: list[ActResolution] = []
        for (law, division, enclosing), records in sorted(
            groups.items(), key=lambda item: (item[0][0] or "", item[0][1] or "", item[0][2] or "")
        ):
            page = min((int(r.statutes_at_large_page) for r in records if r.statutes_at_large_page), default=None)
            narrowed = replace(
                index,
                table3_key_by_name={act_key: law},
                name_candidates={},
                division_by_name={act_key: (division, page)} if division else {},
                enclosing_section_by_name={act_key: enclosing} if enclosing else {},
            )
            candidates.append(resolve_act_relative_citation(citation, index=narrowed, source_credits=source_credits))
        if len(candidates) == 1:
            return replace(candidates[0], name_sources=sources)
        empty_reason = "act_section_ambiguous" if division_compatible else "act_division_conflict"
        return ActResolution(
            citation,
            act_key=act_key,
            table3_key=index.table3_key_by_name[act_key],
            unresolved_reason="act_name_ambiguous" if candidates else empty_reason,
            name_sources=sources,
            candidate_resolutions=tuple(candidates),
        )

    table3_key = index.table3_key_by_name[act_key]
    if table3_key is None:
        return ActResolution(citation, act_key=act_key, unresolved_reason="act_name_ambiguous")

    stated = index.division_by_name.get(act_key)
    stated_enclosing = index.enclosing_section_by_name.get(act_key)
    effective_enclosing = stated_enclosing or citation.enclosing_section
    common: dict[str, Any] = {
        "act_key": act_key,
        "table3_key": table3_key,
        "act_division": stated[0] if stated else citation.division,
        "enclosing_section": effective_enclosing,
    }

    if citation.division and stated and citation.division != stated[0]:
        return ActResolution(citation, **common, unresolved_reason="act_division_conflict")
    if citation.enclosing_section and stated_enclosing and citation.enclosing_section != stated_enclosing:
        return ActResolution(
            citation,
            act_key=act_key,
            table3_key=table3_key,
            act_division=stated[0] if stated else citation.division,
            enclosing_section=stated_enclosing,
            unresolved_reason="act_section_ambiguous",
        )

    table3 = _resolve_through_table3(citation, index, act_key, table3_key, enclosing_section=effective_enclosing)

    explicit_sec = _ENCLOSING_ACT_SECTION.fullmatch(citation.section.strip())
    if source_credits is None or (
        effective_enclosing is None and explicit_sec is None and table3_key in index.enclosing_sections_by_law
    ):
        credit = SourceCreditAnswer(status="not_consulted")
    else:
        credit_enclosing = effective_enclosing or (explicit_sec.group("enclosing") if explicit_sec else None)
        credit_section = explicit_sec.group("inner") if explicit_sec else citation.section
        credit_law = table3_key if _PUBLIC_LAW_KEY.fullmatch(table3_key or "") else None
        credit_div = stated[0] if stated else citation.division
        credit = source_credits.lookup(
            credit_law,
            credit_div,
            credit_section,
            enclosing_section=credit_enclosing,
        )
        if credit.status == "absent":
            base_sec = _TRAILING_SUBSECTION_PARENS.sub("", credit_section.strip())
            law_secs = index.classifications.get(table3_key, {})
            sub_prefix = f"{credit_enclosing}[{base_sec}(" if credit_enclosing else f"{base_sec}("
            if base_sec != credit_section.strip() and not any(k.startswith(sub_prefix) for k in law_secs):
                credit = source_credits.lookup(
                    credit_law,
                    credit_div,
                    base_sec,
                    enclosing_section=credit_enclosing,
                )
    credits = _verdict_from_credits(credit)

    shared: dict[str, Any] = {
        **common,
        "table3_reason": table3.reason,
        "source_credit_status": credit.status,
        "source_credit_targets": credit.targets,
    }

    if table3.iri is not None and credit.status == "multi_target":
        return ActResolution(
            citation,
            **shared,
            unresolved_reason="act_section_ambiguous",
            table3_candidate_iri=table3.iri,
        )
    if table3.iri is not None and credits.iri is not None:
        if table3.iri != credits.iri:
            return ActResolution(
                citation,
                **shared,
                unresolved_reason="sources_disagree",
                conflicting_targets={"table3": table3.iri, "source_credits": credits.iri},
            )
        answer, answered_by = (
            _Verdict(
                iri=table3.iri,
                usc_title=table3.usc_title,
                usc_section=table3.usc_section,
                statutes_at_large_volume=credits.statutes_at_large_volume,
                statutes_at_large_page=credits.statutes_at_large_page,
            ),
            "both",
        )
    elif credits.iri is not None:
        answer, answered_by = credits, "source_credits"
    elif table3.iri is not None:
        answer, answered_by = table3, "table3"
    else:
        unseated = credits.reason is not None and table3.reason == "act_section_not_classified"
        refusal = credits if unseated else table3
        return ActResolution(
            citation,
            **shared,
            usc_title=refusal.usc_title,
            usc_section=refusal.usc_section,
            unresolved_reason=refusal.reason,
        )

    return ActResolution(
        citation,
        **shared,
        usc_title=answer.usc_title,
        usc_section=answer.usc_section,
        iri=answer.iri,
        answered_by=answered_by,
        statutes_at_large_volume=answer.statutes_at_large_volume,
        statutes_at_large_page=answer.statutes_at_large_page,
    )


# --------------------------------------------------------------------------- #
# Maximal-NP statutory short-title scanning over unstructured prose
# --------------------------------------------------------------------------- #

GENERIC_ACT_STEM_STOPLIST: frozenset[str] = frozenset(
    {"a", "an", "any", "each", "other", "present", "prior", "said", "same", "such", "that", "the", "this"}
)

_PROSE_CHAR_MAP = str.maketrans(
    {
        "‘": "'",
        "’": "'",
        "‛": "'",
        "′": "'",
        "“": '"',
        "”": '"',
        "‟": '"',
        "″": '"',
        "\u00a0": " ",
        **dict.fromkeys("‐‑‒–—―−\x96\x97", "-"),
    }
)
_PROSE_PARAGRAPH_BREAK_RE = re.compile(r"\r?\n[ \t]*\r?\n")
_PROSE_ACT_CONNECTORS = frozenset({"of", "for", "and", "or", "the", "to", "on", "in", "with", "&", "by", "against", "from"})
_PROSE_ACT_STOP_TOKENS = frozenset(
    {
        "ACTS", "USC", "USCA", "USCS", "CFR", "STAT", "FR", "PL", "EO", "PUB",
        "AMEND", "AMENDS", "AMENDED", "AMENDING", "UNDER", "PURSUANT", "ENACTED", "TITLED", "ENTITLED",
    }
)
_PROSE_SENTENCE_INITIAL_OPENERS = frozenset(
    {
        "all", "although", "anything", "because", "both", "but", "cf", "compare", "despite", "during",
        "each", "either", "even", "every", "everything", "except", "few", "following", "given", "hence",
        "however", "if", "including", "instead", "irrespective", "many", "meanwhile", "moreover", "most",
        "neither", "nevertheless", "no", "none", "nonetheless", "nor", "note", "nothing", "notwithstanding",
        "once", "only", "otherwise", "pending", "regarding", "regardless", "respecting", "save", "see",
        "several", "since", "so", "some", "something", "then", "therefore", "though", "throughout", "thus",
        "unless", "unlike", "until", "when", "whenever", "where", "whereas", "whether", "while", "within", "yet",
    }
)
_PROSE_ABBREV_DOTTED_RE = re.compile(
    r"(?:(?:[A-Za-z]\.){1,6}(?:-[A-Za-z0-9']+)?|\d+\.\d+|(?i:"
    r"Adm|Admin|App|Art|Assn|Brig|Bros|Capt|Cf|Ch|Cmdr|Co|Col|Coop|Corp|Cpl"
    r"|Dept|Dev|Div|Dr|Envt|Fed|Ft|Gen|Gov|Govt|Hon|Inc|Intl|Jr|Lt|Ltd|Maj"
    r"|Mgmt|Mr|Mrs|Ms|Mt|Natl|No|Nos|Para|Prev|Prof|Pt|Pub|Pvt|Rehab|Rep"
    r"|Rev|Ry|Sec|Secs|Sen|Sgt|Sr|St|Ste|Subch|Subpt|Subsec|Supp|Tech|Tit|Vet"
    r")\.)"
)
_PROSE_CAP_TOKEN_RE = re.compile(
    r"(?:(?:[A-Z]\.){1,6}(?:-[\w']+)?|\d+\.\d+|[A-Z0-9][\w'\-&]*(?:\([A-Za-z0-9.\-&]{1,12}\))*\.?)"
)
_PROSE_FWD_LEX_RE = re.compile(
    r"(?P<para>\r?\n[ \t]*\r?\n)"
    r"|(?P<ws>\s+)"
    r"|(?P<comma>,)"
    r"|(?P<quoted>\"[^\"\r\n]{1,32}\"|''[^'\r\n]{1,32}'')"
    r"|(?P<paren>\([^()\r\n]{1,40}\))"
    r"|(?P<word>"
    r"(?:[A-Za-z]\.){2,6}(?:-[\w']+)?"
    r"|\d+\.\d+"
    r"|[A-Za-z0-9][\w'\-&]*(?:\([A-Za-z0-9.\-&]{1,12}\))+"
    r"|[A-Za-z0-9][\w'\-&]*\.?"
    r"|§+"
    r"|&"
    r")"
    r"|(?P<other>.)"
)
_PROSE_LIST_MARKER_PAREN_RE = re.compile(r"^\([A-Za-z0-9]{1,4}\)$")
_PROSE_DESIGNATOR_RE = re.compile(
    r"^(?:[A-Z]{1,3}|[IVXLCDM]{1,8}|\d{1,5}[A-Za-z0-9.\-]*(?:\([A-Za-z0-9.\-]{1,6}\))*|\([A-Za-z0-9]{1,4}\)(?:\([A-Za-z0-9]{1,4}\))*)$"
)
_PROSE_DESIGNATOR_CONJ = frozenset({"and", "&", "or", "through", "to"})
_PROSE_STRUCTURAL_CONTAINERS = frozenset(
    {
        "article", "articles", "art", "ch", "chapter", "chapters", "clause", "clauses",
        "div", "division", "divisions", "item", "items", "para", "paragraph", "paragraphs",
        "part", "parts", "pt", "schedule", "sec", "secs", "section", "sections",
        "subch", "subchapter", "subchapters", "subparagraph", "subparagraphs",
        "subpart", "subparts", "subpt", "subsec", "subsection", "subsections",
        "subtitle", "subtitles", "tit", "title", "titles", "§", "§§",
    }
)
_PROSE_AMENDMENTS_TAIL_RE = re.compile(r"\s+Amendments?\b")
_PROSE_OF_YEAR_TAIL_RE = re.compile(r"\s*(?:,\s*)?of\s+(?P<year>(?:1[789]|20)\d{2})\b")
_PROSE_COMMA_YEAR_TAIL_RE = re.compile(r"\s*,\s*(?P<year>(?:1[789]|20)\d{2})\b")
_PROSE_FOUR_DIGIT_YEAR_RE = re.compile(r"^(?:1[789]|20)\d{2}$")
_PROSE_QUOTED_ACT_END_RE = re.compile(r"\bAct\s*[\"']+$")


class _ProseToken(NamedTuple):
    kind: str
    raw: str
    bare: str
    start: int
    has_comma: bool


@dataclass(frozen=True, slots=True)
class StatutoryShortTitleMatch:
    """One enacted statutory short title resolved from unstructured prose."""

    surface: str
    act_key: str
    table3_key: str | None
    start: int
    end: int

    @property
    def normalized_surface(self) -> str:
        return re.sub(r"\s+", " ", self.surface).strip()

    @property
    def resolved_act_key(self) -> str:
        return self.act_key


def _is_sentence_initial(norm: str, pos: int) -> bool:
    idx = pos
    while idx > 0 and norm[idx - 1] in " \t\"'([{-*•":
        idx -= 1
    return idx == 0 or norm[idx - 1] in ".!?;:\n\r"


def _tokenize_window_backward(norm: str, window_start: int, act_start: int) -> list[_ProseToken]:
    lex_items: list[tuple[str, str, int, bool]] = []
    for m in _PROSE_FWD_LEX_RE.finditer(norm, window_start, act_start):
        lkind = m.lastgroup
        if lkind == "ws" or lkind is None:
            continue
        raw = m.group()
        if lkind == "comma":
            if lex_items and lex_items[-1][0] != "barrier" and not lex_items[-1][3]:
                prev_k, prev_r, prev_s, _ = lex_items[-1]
                lex_items[-1] = (prev_k, prev_r, prev_s, True)
            else:
                lex_items.append(("barrier", ",", m.start(), False))
            continue
        if lkind in {"para", "other"}:
            lex_items.append(("barrier", raw, m.start(), False))
            continue
        if lkind == "quoted":
            if _PROSE_QUOTED_ACT_END_RE.search(raw) or not any(ch.isalnum() for ch in raw):
                lex_items.append(("barrier", raw, m.start(), False))
            else:
                lex_items.append(("quoted", raw, m.start(), False))
            continue
        lex_items.append((lkind, raw, m.start(), False))

    if not lex_items or lex_items[-1][0] == "barrier" or lex_items[-1][3]:
        return []

    tokens: list[_ProseToken] = []
    for k in range(len(lex_items) - 1, -1, -1):
        kind, raw_word, tok_start, has_comma = lex_items[k]
        if kind == "barrier":
            break
        if kind == "quoted":
            tokens.append(_ProseToken("quoted", raw_word, raw_word, tok_start, has_comma))
            continue
        if kind == "paren":
            has_left_np_neighbor = (
                k > 0
                and lex_items[k - 1][0] in {"word", "quoted", "paren"}
                and not lex_items[k - 1][1].endswith(".")
                and (lex_items[k - 1][1][:1].isupper() or lex_items[k - 1][1].lower() in {"the", "a", "an"})
            )
            if (
                any(ch.isupper() or ch.isdigit() for ch in raw_word)
                or _PROSE_LIST_MARKER_PAREN_RE.fullmatch(raw_word)
                or has_left_np_neighbor
            ):
                tokens.append(_ProseToken("paren", raw_word, raw_word, tok_start, has_comma))
                continue
            break

        bare_word = raw_word.rstrip(".")
        upper_word = bare_word.upper()
        lower_word = bare_word.lower()

        if raw_word.endswith("."):
            if _PROSE_ABBREV_DOTTED_RE.fullmatch(raw_word):
                prev_bare_upper = (
                    lex_items[k - 1][1].replace(".", "").upper()
                    if k > 0 and lex_items[k - 1][0] == "word"
                    else ""
                )
                if upper_word == "L" and prev_bare_upper in {"PUB", "P"}:
                    break
                if upper_word == "APP" and prev_bare_upper in {"USC", "USCA", "USCS", "C"}:
                    break
            else:
                preceded_by_article = (
                    raw_word[:1].isupper()
                    and k > 0
                    and lex_items[k - 1][0] == "word"
                    and not lex_items[k - 1][1].endswith(".")
                    and not lex_items[k - 1][3]
                    and lex_items[k - 1][1].lower() in {"the", "a", "an"}
                )
                if not preceded_by_article:
                    break

        if upper_word in _PROSE_ACT_STOP_TOKENS:
            break

        if (
            _PROSE_CAP_TOKEN_RE.fullmatch(raw_word)
            or lower_word in _PROSE_ACT_CONNECTORS
            or lower_word in _PROSE_STRUCTURAL_CONTAINERS
        ):
            tokens.append(_ProseToken("word", raw_word, bare_word, tok_start, has_comma))
            continue
        break

    return tokens


def _resolve_prose_act_candidate(
    norm: str,
    span_start: int,
    act_end: int,
    index: ActIndex,
    *,
    leading_the_start: int | None = None,
) -> tuple[int, int, str] | None:
    base_end = act_end
    amend_match = _PROSE_AMENDMENTS_TAIL_RE.match(norm, base_end)
    if amend_match is not None and not _PROSE_PARAGRAPH_BREAK_RE.search(norm, base_end, amend_match.end()):
        base_end = amend_match.end()

    def _try_surface(c_start: int, c_end: int, probe_text: str) -> tuple[int, int, str] | None:
        resolved = resolve_act_name(probe_text, index)
        if resolved is None:
            reordered = act_name_with_trailing_year(normalize_popular_name(probe_text))
            if reordered is not None:
                resolved = resolve_act_name(reordered, index)
        if resolved is not None:
            return c_start, c_end, resolved
        if leading_the_start is not None and c_start == span_start:
            resolved_the = resolve_act_name(f"the {probe_text}", index)
            if resolved_the is not None:
                return leading_the_start, c_end, resolved_the
        return None

    stem_surface = re.sub(r"\s+", " ", norm[span_start:base_end]).strip()
    of_year = _PROSE_OF_YEAR_TAIL_RE.match(norm, base_end)
    if of_year is not None and not _PROSE_PARAGRAPH_BREAK_RE.search(norm, base_end, of_year.end()):
        yr = of_year.group("year")
        yr_end = of_year.end()
        for candidate_str in (f"{stem_surface} of {yr}", f"{stem_surface}, {yr}"):
            hit = _try_surface(span_start, yr_end, candidate_str)
            if hit is not None:
                return hit
        return None

    comma_year = _PROSE_COMMA_YEAR_TAIL_RE.match(norm, base_end)
    if comma_year is not None and not _PROSE_PARAGRAPH_BREAK_RE.search(norm, base_end, comma_year.end()):
        yr = comma_year.group("year")
        yr_end = comma_year.end()
        for candidate_str in (f"{stem_surface}, {yr}", f"{stem_surface} of {yr}"):
            hit = _try_surface(span_start, yr_end, candidate_str)
            if hit is not None:
                return hit

    return _try_surface(span_start, base_end, stem_surface)


def _match_structural_prefix_before_of(trimmed: list[_ProseToken]) -> int | None:
    idx = len(trimmed) - 1
    if idx < 1:
        return None
    if trimmed[idx].kind == "word" and trimmed[idx].bare.lower() in _PROSE_STRUCTURAL_CONTAINERS:
        idx -= 1
    if idx < 1 or not _PROSE_DESIGNATOR_RE.fullmatch(trimmed[idx].bare):
        return None
    idx -= 1
    while idx >= 2 and trimmed[idx].bare.lower() in _PROSE_DESIGNATOR_CONJ:
        if not _PROSE_DESIGNATOR_RE.fullmatch(trimmed[idx - 1].bare):
            break
        idx -= 2
    if idx >= 1 and trimmed[idx].bare.lower() == "of":
        return idx
    return None


def _trim_and_resolve_token_slice(
    raw_slice: list[_ProseToken],
    norm: str,
    act_start: int,
    act_end: int,
    index: ActIndex,
) -> tuple[int, int, str] | None:
    toks = list(raw_slice)
    leading_the_start: int | None = None
    while toks and toks[-1].kind == "word" and toks[-1].bare.lower() in _PROSE_ACT_CONNECTORS:
        leading_the_start = toks[-1].start if toks[-1].bare.lower() == "the" else None
        toks.pop()
    if not toks:
        return None

    def _attempt(active: list[_ProseToken], the_start: int | None) -> tuple[int, int, str] | None:
        if not active or not any(t.raw[:1].isupper() or t.kind == "quoted" for t in active):
            return None
        c_start = active[-1].start
        stem_raw = re.sub(r"\s+", " ", norm[c_start:act_start]).strip()
        stem_norm = _LEADING_ARTICLE.sub("", normalize_popular_name(stem_raw)).strip()
        if not stem_norm or stem_norm in GENERIC_ACT_STEM_STOPLIST:
            return None
        return _resolve_prose_act_candidate(norm, c_start, act_end, index, leading_the_start=the_start)

    direct = _attempt(toks, leading_the_start)
    if direct is not None:
        return direct

    trimmed = list(toks)
    sub_the = leading_the_start
    changed = False
    while trimmed:
        if (trimmed[-1].kind == "paren" and _PROSE_LIST_MARKER_PAREN_RE.fullmatch(trimmed[-1].raw)) or (
            len(trimmed) >= 2
            and trimmed[-1].kind == "word"
            and not trimmed[-1].has_comma
            and _is_sentence_initial(norm, trimmed[-1].start)
            and (
                (trimmed[-2].kind == "word" and trimmed[-2].bare.lower() == "the")
                or (
                    trimmed[-1].bare.lower() in _PROSE_SENTENCE_INITIAL_OPENERS
                    and trimmed[-2].kind == "word"
                    and trimmed[-2].bare.lower() in _PROSE_ACT_CONNECTORS
                )
            )
        ):
            trimmed.pop()
            changed = True
        else:
            of_idx = _match_structural_prefix_before_of(trimmed)
            if of_idx is not None:
                trimmed = trimmed[:of_idx]
                changed = True
            else:
                break

        sub_the = None
        while trimmed and trimmed[-1].kind == "word" and trimmed[-1].bare.lower() in _PROSE_ACT_CONNECTORS:
            sub_the = trimmed[-1].start if trimmed[-1].bare.lower() == "the" else None
            trimmed.pop()

    if changed and trimmed:
        return _attempt(trimmed, sub_the)
    return None


def _build_candidate_slices(tokens: list[_ProseToken]) -> list[list[_ProseToken]]:
    slices: list[list[_ProseToken]] = [tokens]
    seen_lengths: set[int] = {len(tokens)}

    def _push(sub: list[_ProseToken]) -> None:
        if sub and len(sub) not in seen_lengths:
            seen_lengths.add(len(sub))
            slices.append(sub)

    for idx_tok in range(len(tokens) - 1, 0, -1):
        tok = tokens[idx_tok]
        if tok.kind == "word" and tok.raw == "Act":
            cur = idx_tok - 1
            if cur >= 0 and tokens[cur].kind == "word" and tokens[cur].bare in {"Amendment", "Amendments"}:
                cur -= 1
            tail_had_comma = tokens[cur + 1].has_comma
            if (
                cur >= 1
                and tokens[cur].kind == "word"
                and tokens[cur].bare.lower() == "of"
                and _PROSE_FOUR_DIGIT_YEAR_RE.fullmatch(tokens[cur - 1].bare)
            ):
                tail_had_comma = tokens[cur - 1].has_comma
                cur -= 2
            elif tail_had_comma and cur >= 0 and _PROSE_FOUR_DIGIT_YEAR_RE.fullmatch(tokens[cur].bare):
                tail_had_comma = tokens[cur].has_comma
                cur -= 1
            if cur >= 0 and tail_had_comma:
                if tokens[cur].bare.lower() in {"and", "&", "or"}:
                    cur -= 1
                if cur >= 0:
                    _push(tokens[: cur + 1])
            elif cur >= 1 and tokens[cur].bare.lower() in {"and", "&", "or"}:
                _push(tokens[:cur])

        if tok.has_comma:
            right_neighbor_bare = tokens[idx_tok - 1].bare.lower()
            if right_neighbor_bare not in {"and", "&", "or"} and not _PROSE_FOUR_DIGIT_YEAR_RE.fullmatch(
                right_neighbor_bare
            ):
                _push(tokens[:idx_tok])

    return slices


def find_statutory_short_titles(
    text: str,
    index: ActIndex,
    *,
    deduplicate: bool = True,
) -> tuple[StatutoryShortTitleMatch, ...]:
    """Deterministically extract enacted statutory short titles from unstructured prose."""
    if not text:
        return ()
    raw_text = str(text)
    norm = raw_text.translate(_PROSE_CHAR_MAP)
    raw_hits: list[StatutoryShortTitleMatch] = []

    for match in re.finditer(r"\bAct\b", norm):
        act_start, act_end = match.start(), match.end()
        window_start = max(0, act_start - 450)
        tokens = _tokenize_window_backward(norm, window_start, act_start)
        if not tokens:
            continue

        for cand_slice in _build_candidate_slices(tokens):
            resolved_hit = _trim_and_resolve_token_slice(cand_slice, norm, act_start, act_end, index)
            if resolved_hit is not None:
                hit_start, hit_end, resolved_key = resolved_hit
                raw_hits.append(
                    StatutoryShortTitleMatch(
                        surface=raw_text[hit_start:hit_end],
                        act_key=resolved_key,
                        table3_key=index.table3_key_by_name.get(resolved_key),
                        start=hit_start,
                        end=hit_end,
                    )
                )
                break

    if not raw_hits:
        return ()

    by_length = sorted(raw_hits, key=lambda item: (-(item.end - item.start), item.start))
    kept_starts: list[int] = []
    kept_ends: list[int] = []
    kept: list[StatutoryShortTitleMatch] = []
    for candidate in by_length:
        idx = bisect_right(kept_starts, candidate.start)
        if (idx > 0 and kept_ends[idx - 1] > candidate.start) or (
            idx < len(kept_starts) and kept_starts[idx] < candidate.end
        ):
            continue
        kept_starts.insert(idx, candidate.start)
        kept_ends.insert(idx, candidate.end)
        kept.append(candidate)
    kept.sort(key=lambda item: (item.start, item.end))

    if not deduplicate:
        return tuple(kept)
    seen_keys: set[str] = set()
    unique: list[StatutoryShortTitleMatch] = []
    for item in kept:
        if item.act_key not in seen_keys:
            seen_keys.add(item.act_key)
            unique.append(item)
    return tuple(unique)


@lru_cache(maxsize=1)
def load_act_index() -> ActIndex:
    """Load and cache the SHA-256-pinned vendored ``ActIndex`` singleton."""
    return ActIndex.from_vendored()


@lru_cache(maxsize=1)
def load_source_credit_index() -> SourceCreditIndex:
    """Load and cache the SHA-256-pinned vendored ``SourceCreditIndex`` singleton."""
    return SourceCreditIndex.from_vendored()
