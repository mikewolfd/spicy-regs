"""Track House and Senate Appropriations OWT, Member-Day, and member-request deadlines.

This module fetches House and Senate Appropriations guidance pages, subcommittee
pages, and Senate hearings listings in pure HTTP (no browser or JavaScript
execution, and never bypassing CAPTCHA challenges). Senate ColdFusion
``/download/*`` links are resolved via standard cookie-jar persistence and
HTTP ``Refresh`` / ``<meta http-equiv="refresh">`` redirects to the underlying
PDFs. Every fetched PDF is hashed with SHA-256 for change detection against an
optional prior JSON snapshot, and every subcommittee deadline is classified
deterministically relative to ``--as-of``.
"""

from __future__ import annotations

import argparse
from collections.abc import Iterable, Mapping, Sequence
import csv
from dataclasses import asdict, dataclass, replace
from datetime import date, timedelta
import hashlib
import io
import json
from pathlib import Path
import re
from typing import TypedDict
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from bs4.element import Tag
import httpx
import pypdf

HOUSE_GUIDANCE_URL = "https://appropriations.house.gov/fy27-information/fy27-member-requests-guidance"
HOUSE_DEM_PORTAL_URL = "https://democrats-appropriations.house.gov/"
SENATE_GUIDANCE_URL = (
    "https://www.appropriations.senate.gov/fy27-appropriations-requests-and-congressionally-directed-spending"
)
SENATE_HEARINGS_URL = "https://www.appropriations.senate.gov/hearings"

USER_AGENT = "Mozilla/5.0 (compatible; spicy-regs/1.0; +https://github.com/civictechdc/spicy-regs)"

VALID_STATUSES = frozenset({"closed", "open", "hearing_relative", "uncertain", "absent", "captcha_gated"})

CSV_COLUMNS: tuple[str, ...] = (
    "chamber",
    "fiscal_year",
    "subcommittee",
    "subcommittee_name",
    "track",
    "source_url",
    "link_url",
    "pdf_url",
    "pdf_sha256",
    "http_status",
    "as_of",
    "status",
    "deadline_kind",
    "deadline_date",
    "deadline_text",
    "hearing_link",
    "hearing_date",
    "updated",
)

_MONTH_MAP: dict[str, int] = {
    "january": 1,
    "jan": 1,
    "february": 2,
    "feb": 2,
    "march": 3,
    "mar": 3,
    "april": 4,
    "apr": 4,
    "may": 5,
    "june": 6,
    "jun": 6,
    "july": 7,
    "jul": 7,
    "august": 8,
    "aug": 8,
    "september": 9,
    "sep": 9,
    "sept": 9,
    "october": 10,
    "oct": 10,
    "november": 11,
    "nov": 11,
    "december": 12,
    "dec": 12,
}

_WEEKDAY_NAMES: dict[str, int] = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}

_WORD_NUMBERS: dict[str, int] = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
    "thirteen": 13,
    "fourteen": 14,
    "fifteen": 15,
    "sixteen": 16,
    "seventeen": 17,
    "eighteen": 18,
    "nineteen": 19,
    "twenty": 20,
    "twenty-one": 21,
    "twenty-eight": 28,
    "thirty": 30,
}

_MONTH_PATTERN = (
    r"(?:January|February|March|April|May|June|July|August|September|October|November|December"
    r"|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\.?"
)
_WEEKDAY_PATTERN = r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)"
_EXPLICIT_DATE_RE = re.compile(
    rf"(?:(?P<weekday>{_WEEKDAY_PATTERN})\s*,\s*)?"
    rf"(?P<month>{_MONTH_PATTERN})\s+"
    rf"(?P<day>\d{{1,2}})\s*(?:st|nd|rd|th)?"
    rf"(?:\s*,\s*(?P<year>20\d{{2}}))?",
    re.IGNORECASE,
)
_NUMERIC_DATE_RE = re.compile(r"\b(?P<month>\d{1,2})/(?P<day>\d{1,2})/(?P<year>\d{2,4})\b")
_FY_YEAR_RE = re.compile(r"\b(?:FY\s*|Fiscal\s+Year\s+)(?P<fy>(?:20)?\d{2})\b", re.IGNORECASE)
_CALENDAR_YEAR_RE = re.compile(r"\b(20\d{2})\b")
_REFRESH_URL_RE = re.compile(r"url\s*=\s*(?P<quote>['\"]?)(?P<url>[^'\"\s;>]+)(?P=quote)", re.IGNORECASE)

_DEADLINE_CUE_RE = re.compile(
    r"\b(?:dea\s*dline|due|no\s+later\s+than|must\s+be\s+received|must\s+be\s+submitted"
    r"|must\s+be\s+made|must\s+be\s+emailed|should\s+be\s+emailed|submitted\s+by|will\s+be\s+due)\b",
    re.IGNORECASE,
)
_HEARING_SCHEDULE_ONLY_RE = re.compile(
    r"\b(?:has\s+scheduled\s+a\s+hearing|hearing\s+will\s+be\s+held|public\s+witness\s+hearings\s+on)\b",
    re.IGNORECASE,
)
_HEARING_RELATIVE_RE = re.compile(
    r"(?:(?P<num>\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|"
    r"sixteen|seventeen|eighteen|nineteen|twenty|twenty-one|twenty-eight|thirty)\s+"
    r"(?P<biz>business\s+)?(?:calendar\s+)?days?\s+"
    r"(?P<dir>after|following|before|prior\s+to)|"
    r"within\s+(?P<win_num>\d+|one|two|three|four|five|six|seven|eight|nine|ten|fourteen|twenty-one|thirty)\s+"
    r"(?P<win_biz>business\s+)?(?:calendar\s+)?days?\s+(?:of|after|following))"
    r"[^.:\n]{0,120}\bhearings?\b",
    re.IGNORECASE,
)
_HEARING_RELATIVE_CUE_RE = re.compile(
    r"\b(?:days?\s+(?:after|following|before|prior\s+to)|following\s+(?:the|each|respective))\b"
    r"[^.:\n]{0,120}\bhearings?\b",
    re.IGNORECASE,
)
_UNCERTAIN_CUE_RE = re.compile(
    r"\b(?:to\s+be\s+determined|to\s+be\s+announced|tbd|tba|will\s+be\s+announced|"
    r"check\s+back|stay\s+tuned|forthcoming|not\s+yet\s+(?:announced|scheduled|posted))\b",
    re.IGNORECASE,
)
_CAPTCHA_MARKERS: tuple[tuple[str, str], ...] = (
    ("h-captcha", "hCaptcha widget"),
    ("captcha-type-challenge--hcaptcha", "hCaptcha form challenge"),
    ("g-recaptcha", "reCAPTCHA widget"),
    ("cf-turnstile", "Cloudflare Turnstile challenge"),
    ("just a moment...", "Cloudflare interstitial challenge"),
    ("attention required!", "Cloudflare challenge"),
    ("captcha: enabled to secure this form", "Drupal form CAPTCHA"),
)


@dataclass(frozen=True, slots=True)
class SubcommitteeSpec:
    """Canonical subcommittee metadata for House/Senate normalization and hearing matching."""

    slug: str
    name: str
    house_slugs: tuple[str, ...]
    senate_slugs: tuple[str, ...]
    name_aliases: tuple[str, ...]
    hearing_keywords: tuple[str, ...]


SUBCOMMITTEE_SPECS: tuple[SubcommitteeSpec, ...] = (
    SubcommitteeSpec(
        slug="agriculture",
        name="Agriculture, Rural Development, Food and Drug Administration, and Related Agencies",
        house_slugs=("agriculture-rural-development-food-and-drug-administration-and-related-agencies",),
        senate_slugs=("agriculture-rural-development-food-and-drug-administration-and-related-agencies",),
        name_aliases=("agriculture", "rural development", "food and drug administration"),
        hearing_keywords=("department of agriculture", "usda", "food and drug administration", "fda"),
    ),
    SubcommitteeSpec(
        slug="commerce_justice_science",
        name="Commerce, Justice, Science, and Related Agencies",
        house_slugs=("commerce-justice-science-and-related-agencies-0", "commerce-justice-science-and-related-agencies"),
        senate_slugs=("commerce-justice-science-and-related-agencies",),
        name_aliases=("commerce, justice", "commerce justice"),
        hearing_keywords=(
            "department of commerce",
            "department of justice",
            "federal bureau of investigation",
            "nasa",
            "national aeronautics and space",
            "national science foundation",
        ),
    ),
    SubcommitteeSpec(
        slug="defense",
        name="Defense",
        house_slugs=("defense-0", "defense"),
        senate_slugs=("defense",),
        name_aliases=("defense",),
        hearing_keywords=(
            "department of defense",
            "department of the air force",
            "air force",
            "navy",
            "army",
            "national guard",
        ),
    ),
    SubcommitteeSpec(
        slug="energy_water",
        name="Energy and Water Development",
        house_slugs=("energy-and-water-development-and-related-agencies-0", "energy-and-water-development"),
        senate_slugs=("energy-and-water-development",),
        name_aliases=("energy and water",),
        hearing_keywords=(
            "department of energy",
            "army corps of engineers",
            "bureau of reclamation",
            "national nuclear security administration",
        ),
    ),
    SubcommitteeSpec(
        slug="financial_services",
        name="Financial Services and General Government",
        house_slugs=("financial-services-and-general-government-0", "financial-services-and-general-government"),
        senate_slugs=("financial-services-and-general-government",),
        name_aliases=("financial services",),
        hearing_keywords=(
            "department of the treasury",
            "general services administration",
            "securities and exchange commission",
            "small business administration",
            "supreme court",
        ),
    ),
    SubcommitteeSpec(
        slug="homeland_security",
        name="Homeland Security",
        house_slugs=("homeland-security-0", "homeland-security"),
        senate_slugs=("homeland-security",),
        name_aliases=("homeland security",),
        hearing_keywords=("department of homeland security", "homeland security"),
    ),
    SubcommitteeSpec(
        slug="interior",
        name="Interior, Environment, and Related Agencies",
        house_slugs=("interior-environment-and-related-agencies-0", "interior-environment-and-related-agencies"),
        senate_slugs=("interior-environment-and-related-agencies",),
        name_aliases=("interior", "interior and environment"),
        hearing_keywords=(
            "department of the interior",
            "environmental protection agency",
            "united states forest service",
            "indian country",
        ),
    ),
    SubcommitteeSpec(
        slug="labor_hhs",
        name="Labor, Health and Human Services, Education, and Related Agencies",
        house_slugs=("labor-health-and-human-services-education-and-related-agencies",),
        senate_slugs=("labor-health-and-human-services-education-and-related-agencies",),
        name_aliases=("labor, health and human services", "labor hhs", "labor, hhs"),
        hearing_keywords=(
            "department of labor",
            "department of health and human services",
            "department of education",
            "national institutes of health",
        ),
    ),
    SubcommitteeSpec(
        slug="legislative_branch",
        name="Legislative Branch",
        house_slugs=("legislative-branch-0", "legislative-branch"),
        senate_slugs=("legislative-branch",),
        name_aliases=("legislative branch",),
        hearing_keywords=(
            "architect of the capitol",
            "library of congress",
            "sergeant at arms",
            "capitol police",
            "congressional budget office",
            "government accountability office",
            "government publishing office",
        ),
    ),
    SubcommitteeSpec(
        slug="milcon_va",
        name="Military Construction, Veterans Affairs, and Related Agencies",
        house_slugs=("military-construction-veterans-affairs-and-related-agencies",),
        senate_slugs=("military-construction-veterans-affairs-and-related-agencies",),
        name_aliases=("military construction", "milcon"),
        hearing_keywords=("military construction", "department of veterans affairs", "family housing"),
    ),
    SubcommitteeSpec(
        slug="state_foreign_ops",
        name="State, Foreign Operations, and Related Programs",
        house_slugs=(
            "national-security-department-state-and-related-programs",
            "state-foreign-operations-and-related-programs",
        ),
        senate_slugs=("state-foreign-operations-and-related-programs",),
        name_aliases=("national security and department of state", "state, foreign operations", "state-foreign operations"),
        hearing_keywords=("department of state", "usaid", "foreign assistance"),
    ),
    SubcommitteeSpec(
        slug="thud",
        name="Transportation, Housing and Urban Development, and Related Agencies",
        house_slugs=("transportation-housing-and-urban-development-and-related-agencies",),
        senate_slugs=("transportation-housing-and-urban-development-and-related-agencies",),
        name_aliases=("transportation, housing and urban development", "transportation and housing and urban development"),
        hearing_keywords=(
            "department of transportation",
            "housing and urban development",
            "hud",
        ),
    ),
)

_SPEC_BY_SLUG: dict[str, SubcommitteeSpec] = {spec.slug: spec for spec in SUBCOMMITTEE_SPECS}


@dataclass(frozen=True, slots=True)
class DeadlineRecord:
    """Normalized deadline observation for one subcommittee track or portal."""

    chamber: str
    fiscal_year: int
    subcommittee: str
    subcommittee_name: str
    track: str
    source_url: str
    link_url: str
    pdf_url: str
    pdf_sha256: str
    http_status: int | None
    as_of: str
    status: str
    deadline_kind: str
    deadline_date: str
    deadline_text: str
    hearing_link: str
    hearing_date: str
    updated: bool = False

    def sort_key(self) -> tuple[str, str, str, str, str]:
        return (self.chamber, self.subcommittee, self.track, self.link_url, self.source_url)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class PageProbe:
    """Diagnostic record for an HTTP fetch during a tracker run."""

    url: str
    final_url: str
    http_status: int | None
    content_type: str
    error: str = ""

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ResolvedDocument:
    """Result of resolving a document URL (including ColdFusion Refresh chains)."""

    link_url: str
    pdf_url: str
    pdf_bytes: bytes | None
    pdf_sha256: str
    http_status: int | None
    hops: tuple[str, ...]
    captcha_marker: str | None = None
    error: str = ""


@dataclass(frozen=True, slots=True)
class HearingEntry:
    """Single hearing row parsed from Senate Appropriations ``/hearings``."""

    title: str
    location: str
    hearing_date: date
    url: str
    is_postponed: bool


class TrackerReport(TypedDict):
    """Deterministic top-level JSON report produced by ``collect_appropriations_deadlines``."""

    as_of: str
    fiscal_year: int
    summary: dict[str, object]
    records: list[dict[str, object]]
    page_probes: list[dict[str, object]]


def build_http_client(*, transport: httpx.BaseTransport | None = None, timeout: float = 30.0) -> httpx.Client:
    """Construct an HTTP client with cookie persistence and explicit redirect control."""
    return httpx.Client(
        transport=transport,
        timeout=timeout,
        follow_redirects=False,
        headers={"User-Agent": USER_AGENT},
    )


def detect_captcha(html_text: str, *, portal_or_form: bool = False) -> str | None:
    """Detect common CAPTCHA/bot-challenge markers without attempting any bypass."""
    lowered = html_text.lower()
    for marker, label in _CAPTCHA_MARKERS:
        if marker in lowered:
            if marker == "captcha: enabled to secure this form" and not portal_or_form:
                continue
            return label
    if portal_or_form and "captcha" in lowered:
        return "CAPTCHA-gated form"
    return None


def parse_refresh_target(base_url: str, headers: httpx.Headers, html_text: str) -> str | None:
    """Extract a redirect target from HTTP ``Refresh`` or HTML ``<meta http-equiv="refresh">``."""
    refresh_header = headers.get("refresh")
    if refresh_header:
        match = _REFRESH_URL_RE.search(refresh_header)
        if match:
            return urljoin(base_url, match.group("url").strip())
    if html_text:
        soup = BeautifulSoup(html_text, "html.parser")
        meta = soup.find("meta", attrs={"http-equiv": re.compile(r"^refresh$", re.IGNORECASE)})
        if isinstance(meta, Tag):
            content = str(meta.get("content") or "")
            match = _REFRESH_URL_RE.search(content)
            if match:
                return urljoin(base_url, match.group("url").strip())
    return None


def _is_pdf_response(response: httpx.Response) -> bool:
    content_type = (response.headers.get("content-type") or "").lower()
    return response.content.startswith(b"%PDF-") or "application/pdf" in content_type


def fetch_html_page(
    client: httpx.Client,
    url: str,
    *,
    max_redirects: int = 8,
    probes: list[PageProbe] | None = None,
) -> tuple[str | None, str, int | None, str | None]:
    """Fetch an HTML page while following standard HTTP redirects and recording probe state."""
    current = url
    visited: set[str] = set()
    try:
        for _ in range(max_redirects):
            if current in visited:
                error = f"Redirect loop at {current}"
                if probes is not None:
                    probes.append(PageProbe(url=url, final_url=current, http_status=None, content_type="", error=error))
                return None, current, None, error
            visited.add(current)
            response = client.get(current)
            if response.status_code in {301, 302, 303, 307, 308} and response.headers.get("location"):
                current = urljoin(str(response.url), response.headers["location"])
                continue
            content_type = response.headers.get("content-type") or ""
            if probes is not None:
                probes.append(
                    PageProbe(
                        url=url,
                        final_url=str(response.url),
                        http_status=response.status_code,
                        content_type=content_type,
                        error="" if response.status_code < 400 else f"HTTP {response.status_code}",
                    )
                )
            if response.status_code >= 400:
                return response.text, str(response.url), response.status_code, f"HTTP {response.status_code}"
            return response.text, str(response.url), response.status_code, None
        error = f"Exceeded {max_redirects} redirects"
        if probes is not None:
            probes.append(PageProbe(url=url, final_url=current, http_status=None, content_type="", error=error))
        return None, current, None, error
    except httpx.HTTPError as exc:
        if probes is not None:
            probes.append(PageProbe(url=url, final_url=current, http_status=None, content_type="", error=str(exc)))
        return None, current, None, str(exc)


def resolve_document_url(
    client: httpx.Client,
    url: str,
    *,
    max_hops: int = 8,
    probes: list[PageProbe] | None = None,
) -> ResolvedDocument:
    """Resolve direct PDF links or Senate ColdFusion ``/download/*`` cookie+Refresh chains."""
    current = url
    hops: list[str] = []
    visited: set[str] = set()
    try:
        for _ in range(max_hops):
            if current in visited:
                return ResolvedDocument(
                    link_url=url,
                    pdf_url="",
                    pdf_bytes=None,
                    pdf_sha256="",
                    http_status=None,
                    hops=tuple(hops),
                    error=f"Redirect loop at {current}",
                )
            visited.add(current)
            response = client.get(current)
            hops.append(str(response.url))
            content_type = response.headers.get("content-type") or ""

            if response.status_code in {301, 302, 303, 307, 308} and response.headers.get("location"):
                current = urljoin(str(response.url), response.headers["location"])
                continue

            if response.status_code >= 400:
                captcha = detect_captcha(response.text, portal_or_form=True) if "html" in content_type.lower() else None
                if probes is not None:
                    probes.append(
                        PageProbe(
                            url=url,
                            final_url=str(response.url),
                            http_status=response.status_code,
                            content_type=content_type,
                            error=captcha or f"HTTP {response.status_code}",
                        )
                    )
                return ResolvedDocument(
                    link_url=url,
                    pdf_url="",
                    pdf_bytes=None,
                    pdf_sha256="",
                    http_status=response.status_code,
                    hops=tuple(hops),
                    captcha_marker=captcha,
                    error=captcha or f"HTTP {response.status_code}",
                )

            if _is_pdf_response(response):
                digest = hashlib.sha256(response.content).hexdigest()
                if probes is not None:
                    probes.append(
                        PageProbe(
                            url=url,
                            final_url=str(response.url),
                            http_status=response.status_code,
                            content_type=content_type,
                        )
                    )
                return ResolvedDocument(
                    link_url=url,
                    pdf_url=str(response.url),
                    pdf_bytes=response.content,
                    pdf_sha256=digest,
                    http_status=response.status_code,
                    hops=tuple(hops),
                )

            html_text = response.text if "html" in content_type.lower() or "<meta" in response.text.lower() else ""
            refresh_target = parse_refresh_target(str(response.url), response.headers, html_text)
            if refresh_target:
                current = refresh_target
                continue

            captcha = detect_captcha(response.text, portal_or_form=True)
            if probes is not None:
                probes.append(
                    PageProbe(
                        url=url,
                        final_url=str(response.url),
                        http_status=response.status_code,
                        content_type=content_type,
                        error=captcha or "Non-PDF response with no refresh redirect",
                    )
                )
            return ResolvedDocument(
                link_url=url,
                pdf_url="",
                pdf_bytes=None,
                pdf_sha256="",
                http_status=response.status_code,
                hops=tuple(hops),
                captcha_marker=captcha,
                error=captcha or "Non-PDF response with no refresh redirect",
            )

        return ResolvedDocument(
            link_url=url,
            pdf_url="",
            pdf_bytes=None,
            pdf_sha256="",
            http_status=None,
            hops=tuple(hops),
            error=f"Exceeded {max_hops} hops",
        )
    except httpx.HTTPError as exc:
        if probes is not None:
            probes.append(PageProbe(url=url, final_url=current, http_status=None, content_type="", error=str(exc)))
        return ResolvedDocument(
            link_url=url,
            pdf_url="",
            pdf_bytes=None,
            pdf_sha256="",
            http_status=None,
            hops=tuple(hops),
            error=str(exc),
        )


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pypdf, returning empty text on malformed streams."""
    try:
        reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
    except Exception:
        return ""
    pages: list[str] = []
    for page in reader.pages:
        try:
            text = page.extract_text() or ""
        except Exception:
            text = ""
        if text:
            pages.append(text)
    return "\n".join(pages)


def infer_calendar_year(text: str, *, default_fiscal_year: int) -> int:
    """Infer calendar year for omitted-year dates from explicit years or FY markers."""
    fy_match = _FY_YEAR_RE.search(text)
    if fy_match:
        fy_raw = int(fy_match.group("fy"))
        fy_full = fy_raw + 2000 if fy_raw < 100 else fy_raw
        return fy_full - 1
    years = [int(y) for y in _CALENDAR_YEAR_RE.findall(text)]
    if years:
        return max(years)
    return default_fiscal_year - 1


def parse_explicit_date(
    text: str,
    *,
    fallback_year: int | None = None,
    prefer_last: bool = False,
) -> date | None:
    """Parse an explicit calendar date such as ``May 1st, 2026`` or ``March 19 th``."""
    matches = list(_EXPLICIT_DATE_RE.finditer(text))
    if prefer_last:
        matches = list(reversed(matches))
    for match in matches:
        month_key = match.group("month").rstrip(".").lower()
        month = _MONTH_MAP.get(month_key)
        day = int(match.group("day"))
        year_raw = match.group("year")
        year = int(year_raw) if year_raw else fallback_year
        if month is None or year is None:
            continue
        try:
            candidate = date(year, month, day)
        except ValueError:
            continue
        weekday_raw = match.group("weekday")
        if weekday_raw and not year_raw:
            expected_wd = _WEEKDAY_NAMES[weekday_raw.lower()]
            if candidate.weekday() != expected_wd:
                for adj in (-1, 1):
                    try:
                        alt = date(year + adj, month, day)
                    except ValueError:
                        continue
                    if alt.weekday() == expected_wd:
                        candidate = alt
                        break
        return candidate

    num_matches = list(_NUMERIC_DATE_RE.finditer(text))
    if prefer_last:
        num_matches = list(reversed(num_matches))
    for match in num_matches:
        month = int(match.group("month"))
        day = int(match.group("day"))
        year = int(match.group("year"))
        if year < 100:
            year += 2000
        try:
            return date(year, month, day)
        except ValueError:
            continue
    return None


def _split_candidate_clauses(text: str) -> list[str]:
    normalized = re.sub(r"[ \t]+", " ", text)
    # Rejoin PDF soft wraps like "Thursday, March 27,\n2026." before splitting clauses.
    normalized = re.sub(r",\s*\n\s*(20\d{2}\b)", r", \1", normalized)
    raw_lines = [line.strip() for line in re.split(r"[\r\n•]+", normalized) if line.strip()]
    merged_lines: list[str] = []
    for line in raw_lines:
        if (
            merged_lines
            and not re.search(r"[.!?:]$", merged_lines[-1])
            and not re.match(r"^(?:deadline|due|for\s+members|note|guidelines|delivery)\b", line, re.IGNORECASE)
        ):
            merged_lines[-1] = f"{merged_lines[-1]} {line}"
        else:
            merged_lines.append(line)

    clauses: list[str] = []
    for line in merged_lines:
        for sentence in re.split(r"(?<=[.!?])\s+(?=[A-Z])", line):
            cleaned = sentence.strip()
            if cleaned:
                clauses.append(cleaned)
    return clauses


def extract_deadline_milestones(text: str, *, default_fiscal_year: int) -> list[tuple[date, str]]:
    """Extract submission-deadline dates and their source clauses from PDF/HTML text."""
    fallback_year = infer_calendar_year(text, default_fiscal_year=default_fiscal_year)
    milestones: list[tuple[date, str]] = []
    seen: set[tuple[date, str]] = set()
    clauses = _split_candidate_clauses(text)
    for idx, clause in enumerate(clauses):
        if _HEARING_SCHEDULE_ONLY_RE.search(clause) and not _DEADLINE_CUE_RE.search(clause):
            continue
        window = clause
        if _DEADLINE_CUE_RE.search(clause):
            parsed = parse_explicit_date(clause, fallback_year=fallback_year)
            if parsed is None and idx + 1 < len(clauses):
                combined = f"{clause} {clauses[idx + 1]}"
                parsed = parse_explicit_date(combined, fallback_year=fallback_year)
                if parsed is not None:
                    window = combined
            if parsed is not None and (parsed, window) not in seen:
                seen.add((parsed, window))
                milestones.append((parsed, window))
    return milestones


def parse_hearing_relative_rule(text: str) -> tuple[int, bool, str] | None:
    """Parse relative deadline wording like ``seven days after ... hearing``."""
    collapsed = " ".join(text.split())
    match = _HEARING_RELATIVE_RE.search(collapsed)
    if not match:
        return None
    raw_num = (match.group("num") or match.group("win_num") or "").lower()
    if raw_num.isdigit():
        days = int(raw_num)
    else:
        days = _WORD_NUMBERS.get(raw_num, 0)
    if days <= 0:
        return None
    direction = (match.group("dir") or "after").lower()
    signed_days = -days if ("before" in direction or "prior" in direction) else days
    business_days = bool(match.group("biz") or match.group("win_biz"))
    return signed_days, business_days, match.group(0).strip()


def has_hearing_relative_cue(text: str) -> bool:
    """Return True when text describes a hearing-dependent deadline."""
    collapsed = " ".join(text.split())
    return bool(_HEARING_RELATIVE_RE.search(collapsed) or _HEARING_RELATIVE_CUE_RE.search(collapsed))


def add_offset_days(start_date: date, offset_days: int, *, business_days: bool) -> date:
    """Add calendar or business days to ``start_date``."""
    if not business_days:
        return start_date + timedelta(days=offset_days)
    step = 1 if offset_days >= 0 else -1
    remaining = abs(offset_days)
    cursor = start_date
    while remaining > 0:
        cursor += timedelta(days=step)
        if cursor.weekday() < 5:
            remaining -= 1
    return cursor


def classify_deadline(
    *,
    as_of: date,
    deadline_date: date | None,
    is_hearing_relative: bool,
    hearing_matched: bool,
    is_absent: bool,
    is_captcha_gated: bool,
) -> tuple[str, str]:
    """Classify a deadline record into ``(status, deadline_kind)`` relative to ``as_of``."""
    if is_captcha_gated:
        return "captcha_gated", "captcha_gated"
    if is_absent:
        return "absent", "none"
    if is_hearing_relative:
        if not hearing_matched or deadline_date is None:
            return "uncertain", "hearing_relative"
        if deadline_date < as_of:
            return "closed", "hearing_relative"
        return "hearing_relative", "hearing_relative"
    if deadline_date is not None:
        if deadline_date < as_of:
            return "closed", "explicit"
        return "open", "explicit"
    return "uncertain", "ambiguous"


def match_subcommittee_by_slug(slug_or_url: str) -> SubcommitteeSpec | None:
    """Resolve a House or Senate subcommittee URL/slug to a canonical ``SubcommitteeSpec``."""
    path = urlparse(slug_or_url).path.rstrip("/")
    tail = path.rsplit("/", 1)[-1].lower()
    for spec in SUBCOMMITTEE_SPECS:
        if tail == spec.slug or tail in spec.house_slugs or tail in spec.senate_slugs:
            return spec
    return None


def match_subcommittee_by_name(text: str) -> SubcommitteeSpec | None:
    """Resolve a subcommittee heading or table label to a canonical ``SubcommitteeSpec``."""
    lowered = " ".join(text.lower().split())
    for spec in SUBCOMMITTEE_SPECS:
        if any(alias in lowered for alias in spec.name_aliases):
            return spec
    return None


def _classify_pdf_track(link_text: str, link_url: str) -> str:
    combined = f"{link_text} {link_url}".lower()
    has_owt = any(
        tok in combined
        for tok in ("outside witness", "owt", "public witness", "written public testimony", "testimony for the record")
    )
    has_member_day = "member day" in combined or "members' day" in combined or "members day" in combined
    if has_owt and has_member_day:
        return "owt_and_member_day"
    if has_member_day:
        return "member_day"
    if "ai-an" in combined or "american indian" in combined:
        return "owt_aian"
    if "all-groups" in combined or "all groups" in combined:
        return "owt_all_groups"
    if has_owt or "submit written testimony" in combined:
        return "owt"
    return "subcommittee_notice"


def _analyze_document_text(
    text: str,
    *,
    as_of: date,
    fiscal_year: int,
    subcommittee: SubcommitteeSpec,
    hearings: Sequence[HearingEntry],
) -> tuple[str, str, str, str, str, str]:
    """Analyze extracted notice text and return deadline classification and hearing match details."""
    milestones = extract_deadline_milestones(text, default_fiscal_year=fiscal_year)
    if milestones:
        latest_date, latest_clause = max(milestones, key=lambda item: item[0])
        summary_parts = [f"{d.isoformat()}: {clause}" for d, clause in milestones]
        deadline_text = " | ".join(summary_parts) if len(milestones) > 1 else latest_clause
        status, kind = classify_deadline(
            as_of=as_of,
            deadline_date=latest_date,
            is_hearing_relative=False,
            hearing_matched=False,
            is_absent=False,
            is_captcha_gated=False,
        )
        return status, kind, latest_date.isoformat(), deadline_text, "", ""

    rel_rule = parse_hearing_relative_rule(text)
    if rel_rule is not None or has_hearing_relative_cue(text):
        matched_hearings = match_subcommittee_hearings(subcommittee, hearings, fiscal_year=fiscal_year)
        if rel_rule is not None and matched_hearings:
            offset_days, business_days, phrase = rel_rule
            computed: list[tuple[date, HearingEntry]] = [
                (add_offset_days(h.hearing_date, offset_days, business_days=business_days), h)
                for h in matched_hearings
            ]
            chosen_deadline, chosen_hearing = max(computed, key=lambda item: item[0])
            status, kind = classify_deadline(
                as_of=as_of,
                deadline_date=chosen_deadline,
                is_hearing_relative=True,
                hearing_matched=True,
                is_absent=False,
                is_captcha_gated=False,
            )
            details = "; ".join(
                f"{h.title} ({h.hearing_date.isoformat()} -> {d.isoformat()})" for d, h in computed
            )
            return (
                status,
                kind,
                chosen_deadline.isoformat(),
                f"{phrase} [{details}]",
                chosen_hearing.url,
                chosen_hearing.hearing_date.isoformat(),
            )
        cue = rel_rule[2] if rel_rule is not None else "hearing-relative deadline without explicit day offset or matched hearing"
        status, kind = classify_deadline(
            as_of=as_of,
            deadline_date=None,
            is_hearing_relative=True,
            hearing_matched=False,
            is_absent=False,
            is_captcha_gated=False,
        )
        return status, kind, "", cue, "", ""

    collapsed = " ".join(text.split())
    if _UNCERTAIN_CUE_RE.search(collapsed):
        snippet = collapsed[:240]
    elif collapsed:
        snippet = f"No explicit deadline date found ({collapsed[:180]})"
    else:
        snippet = "Empty PDF or unextractable text"
    status, kind = classify_deadline(
        as_of=as_of,
        deadline_date=None,
        is_hearing_relative=False,
        hearing_matched=False,
        is_absent=False,
        is_captcha_gated=False,
    )
    return status, kind, "", snippet, "", ""


def parse_senate_hearings_html(html_text: str, base_url: str) -> list[HearingEntry]:
    """Parse Senate Appropriations ``/hearings`` HTML table rows into ``HearingEntry`` objects."""
    soup = BeautifulSoup(html_text, "html.parser")
    entries: list[HearingEntry] = []
    seen_urls: set[str] = set()
    for tr in soup.select("table tr"):
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue
        link_tag = tr.find("a", href=True)
        if not isinstance(link_tag, Tag):
            continue
        title = " ".join(tds[0].get_text(" ", strip=True).split())
        location = " ".join(tds[1].get_text(" ", strip=True).split())
        raw_date = " ".join(tds[2].get_text(" ", strip=True).split())
        parsed_date = parse_explicit_date(raw_date)
        if parsed_date is None:
            continue
        href = urljoin(base_url, str(link_tag["href"]))
        if href in seen_urls:
            continue
        seen_urls.add(href)
        lowered = title.lower()
        is_postponed = any(flag in lowered for flag in ("postponed", "to be rescheduled", "cancelled", "canceled"))
        entries.append(
            HearingEntry(
                title=title,
                location=location,
                hearing_date=parsed_date,
                url=href,
                is_postponed=is_postponed,
            )
        )
    return entries


def fetch_senate_hearings(
    client: httpx.Client,
    *,
    hearings_url: str = SENATE_HEARINGS_URL,
    max_pages: int = 2,
    probes: list[PageProbe] | None = None,
) -> list[HearingEntry]:
    """Fetch recent pages of Senate Appropriations ``/hearings``."""
    entries: list[HearingEntry] = []
    seen_urls: set[str] = set()
    for page_num in range(1, max_pages + 1):
        target = hearings_url if page_num == 1 else f"{hearings_url}?PageNum_rs={page_num}"
        html_text, final_url, _, err = fetch_html_page(client, target, probes=probes)
        if html_text is None or err:
            break
        page_entries = parse_senate_hearings_html(html_text, final_url)
        if not page_entries:
            break
        for entry in page_entries:
            if entry.url not in seen_urls:
                seen_urls.add(entry.url)
                entries.append(entry)
    return entries


def match_subcommittee_hearings(
    subcommittee: SubcommitteeSpec,
    hearings: Sequence[HearingEntry],
    *,
    fiscal_year: int,
) -> list[HearingEntry]:
    """Match active FY budget hearings for a given Senate subcommittee."""
    fy_tokens = (
        f"fiscal year {fiscal_year}",
        f"fy {fiscal_year}",
        f"fy{fiscal_year}",
        f"fy {fiscal_year % 100}",
        f"fy{fiscal_year % 100}",
    )
    matched: list[HearingEntry] = []
    for entry in hearings:
        if entry.is_postponed:
            continue
        title_lower = entry.title.lower()
        if "business meeting" in title_lower or "markup" in title_lower:
            continue
        if not any(tok in title_lower for tok in fy_tokens):
            continue
        if any(kw in title_lower for kw in subcommittee.hearing_keywords):
            matched.append(entry)
    return matched


def discover_subcommittee_urls(
    html_text: str,
    base_url: str,
    *,
    chamber: str,
) -> dict[str, str]:
    """Discover House or Senate subcommittee pages from guidance page links with canonical fallback."""
    soup = BeautifulSoup(html_text, "html.parser")
    discovered: dict[str, str] = {}
    parsed_base = urlparse(base_url)
    for anchor in soup.find_all("a", href=True):
        href = urljoin(base_url, str(anchor["href"]))
        parsed = urlparse(href)
        if parsed.netloc != parsed_base.netloc:
            continue
        parts = [p for p in parsed.path.split("/") if p]
        if len(parts) != 2 or parts[0] != "subcommittees":
            continue
        spec = match_subcommittee_by_slug(href)
        if spec is not None and spec.slug not in discovered:
            discovered[spec.slug] = f"{parsed.scheme}://{parsed.netloc}/subcommittees/{parts[1]}"

    default_host = "https://appropriations.house.gov" if chamber == "house" else "https://www.appropriations.senate.gov"
    for spec in SUBCOMMITTEE_SPECS:
        if spec.slug not in discovered:
            slug = spec.house_slugs[0] if chamber == "house" else spec.senate_slugs[0]
            discovered[spec.slug] = f"{default_host}/subcommittees/{slug}"
    return discovered


def _cached_resolve_pdf(
    client: httpx.Client,
    link_url: str,
    pdf_cache: dict[str, ResolvedDocument],
    probes: list[PageProbe] | None,
) -> ResolvedDocument:
    if link_url not in pdf_cache:
        pdf_cache[link_url] = resolve_document_url(client, link_url, probes=probes)
    return pdf_cache[link_url]


def parse_house_guidance_records(
    client: httpx.Client,
    html_text: str,
    source_url: str,
    *,
    as_of: date,
    fiscal_year: int,
    pdf_cache: dict[str, ResolvedDocument],
    probes: list[PageProbe] | None = None,
) -> list[DeadlineRecord]:
    """Parse House ``Subcommittee-Specific Guidance`` rows (programmatic/language + CPF)."""
    soup = BeautifulSoup(html_text, "html.parser")
    main = soup.find("main") or soup
    records: list[DeadlineRecord] = []

    anchor_heading = main.find(
        lambda tag: isinstance(tag, Tag)
        and tag.name in {"h2", "h3"}
        and "subcommittee-specific guidance" in tag.get_text(" ", strip=True).lower()
    )
    if not isinstance(anchor_heading, Tag):
        return records

    for sibling in anchor_heading.find_next_siblings():
        if not isinstance(sibling, Tag):
            continue
        if sibling.name == "h2":
            break
        if sibling.name != "h3":
            continue
        spec = match_subcommittee_by_name(sibling.get_text(" ", strip=True))
        ul = sibling.find_next_sibling("ul")
        if spec is None or not isinstance(ul, Tag):
            continue
        for li in ul.find_all("li", recursive=False):
            li_text = " ".join(li.get_text(" ", strip=True).split())
            lower_li = li_text.lower()
            if "programmatic and language" in lower_li:
                track = "member_programmatic_language"
            elif "community project funding" in lower_li:
                track = "member_cpf"
            else:
                continue

            pdf_links = [
                urljoin(source_url, str(a["href"]))
                for a in li.find_all("a", href=True)
                if ".pdf" in str(a["href"]).lower()
            ]
            deadline_date = parse_explicit_date(li_text, fallback_year=fiscal_year - 1)
            is_absent = "not applicable" in lower_li and not pdf_links and deadline_date is None

            if not pdf_links:
                status, kind = classify_deadline(
                    as_of=as_of,
                    deadline_date=deadline_date,
                    is_hearing_relative=False,
                    hearing_matched=False,
                    is_absent=is_absent,
                    is_captcha_gated=False,
                )
                records.append(
                    DeadlineRecord(
                        chamber="house",
                        fiscal_year=fiscal_year,
                        subcommittee=spec.slug,
                        subcommittee_name=spec.name,
                        track=track,
                        source_url=source_url,
                        link_url="",
                        pdf_url="",
                        pdf_sha256="",
                        http_status=200,
                        as_of=as_of.isoformat(),
                        status=status,
                        deadline_kind=kind,
                        deadline_date=deadline_date.isoformat() if deadline_date else "",
                        deadline_text="Not applicable" if is_absent else li_text,
                        hearing_link="",
                        hearing_date="",
                    )
                )
                continue

            for link_url in pdf_links:
                doc = _cached_resolve_pdf(client, link_url, pdf_cache, probes)
                resolved_date = deadline_date
                deadline_snippet = li_text
                if resolved_date is None and doc.pdf_bytes is not None:
                    pdf_text = extract_pdf_text(doc.pdf_bytes)
                    milestones = extract_deadline_milestones(pdf_text, default_fiscal_year=fiscal_year)
                    if milestones:
                        resolved_date, deadline_snippet = max(milestones, key=lambda item: item[0])
                status, kind = classify_deadline(
                    as_of=as_of,
                    deadline_date=resolved_date,
                    is_hearing_relative=False,
                    hearing_matched=False,
                    is_absent=False,
                    is_captcha_gated=doc.captcha_marker is not None,
                )
                records.append(
                    DeadlineRecord(
                        chamber="house",
                        fiscal_year=fiscal_year,
                        subcommittee=spec.slug,
                        subcommittee_name=spec.name,
                        track=track,
                        source_url=source_url,
                        link_url=link_url,
                        pdf_url=doc.pdf_url,
                        pdf_sha256=doc.pdf_sha256,
                        http_status=doc.http_status,
                        as_of=as_of.isoformat(),
                        status=status,
                        deadline_kind=kind,
                        deadline_date=resolved_date.isoformat() if resolved_date else "",
                        deadline_text=doc.error or deadline_snippet,
                        hearing_link="",
                        hearing_date="",
                    )
                )
    return records


def _empty_error_record(
    *,
    chamber: str,
    fiscal_year: int,
    spec: SubcommitteeSpec,
    track: str,
    source_url: str,
    link_url: str = "",
    pdf_url: str = "",
    pdf_sha256: str = "",
    http_status: int | None,
    as_of: date,
    status: str,
    kind: str,
    deadline_text: str,
) -> DeadlineRecord:
    return DeadlineRecord(
        chamber=chamber,
        fiscal_year=fiscal_year,
        subcommittee=spec.slug,
        subcommittee_name=spec.name,
        track=track,
        source_url=source_url,
        link_url=link_url,
        pdf_url=pdf_url,
        pdf_sha256=pdf_sha256,
        http_status=http_status,
        as_of=as_of.isoformat(),
        status=status,
        deadline_kind=kind,
        deadline_date="",
        deadline_text=deadline_text,
        hearing_link="",
        hearing_date="",
    )


def _extract_subcommittee_notice_links(
    soup: BeautifulSoup,
    base_url: str,
    *,
    scope_main: bool,
    allow_coldfusion_download: bool,
    link_keywords: tuple[str, ...],
) -> list[tuple[str, str]]:
    container: Tag | BeautifulSoup = (soup.find("main") or soup) if scope_main else soup
    links: list[tuple[str, str]] = []
    seen: set[str] = set()
    for anchor in container.find_all("a", href=True):
        href = urljoin(base_url, str(anchor["href"]))
        text = " ".join(anchor.get_text(" ", strip=True).split())
        combined = f"{text} {href}".lower()
        is_pdf = ".pdf" in href.lower()
        is_cf = allow_coldfusion_download and "/download/" in href.lower()
        if not (is_pdf or is_cf):
            continue
        if not any(kw in combined for kw in link_keywords):
            continue
        if href not in seen:
            seen.add(href)
            links.append((text, href))
    return links


def _scrape_subcommittee_page(
    client: httpx.Client,
    spec: SubcommitteeSpec,
    url: str,
    *,
    chamber: str,
    default_track: str,
    scope_main: bool,
    allow_coldfusion_download: bool,
    link_keywords: tuple[str, ...],
    check_inline_deadline: bool,
    empty_note: str,
    as_of: date,
    fiscal_year: int,
    hearings: Sequence[HearingEntry],
    pdf_cache: dict[str, ResolvedDocument],
    probes: list[PageProbe] | None = None,
) -> list[DeadlineRecord]:
    """Shared fetcher and PDF/HTML analyzer for House and Senate subcommittee pages."""
    html_text, final_url, http_status, err = fetch_html_page(client, url, probes=probes)
    if html_text is None or err:
        captcha = detect_captcha(html_text or "", portal_or_form=True)
        status, kind = classify_deadline(
            as_of=as_of,
            deadline_date=None,
            is_hearing_relative=False,
            hearing_matched=False,
            is_absent=http_status in {404, 410},
            is_captcha_gated=captcha is not None,
        )
        return [
            _empty_error_record(
                chamber=chamber,
                fiscal_year=fiscal_year,
                spec=spec,
                track=default_track,
                source_url=final_url,
                http_status=http_status,
                as_of=as_of,
                status=status,
                kind=kind,
                deadline_text=captcha or err or f"HTTP {http_status}",
            )
        ]

    soup = BeautifulSoup(html_text, "html.parser")
    notice_links = _extract_subcommittee_notice_links(
        soup,
        final_url,
        scope_main=scope_main,
        allow_coldfusion_download=allow_coldfusion_download,
        link_keywords=link_keywords,
    )
    if not notice_links:
        captcha = detect_captcha(html_text, portal_or_form=False)
        main_el = soup.find("main") or soup
        main_text = " ".join(main_el.get_text(" ", strip=True).split())
        if captcha is not None:
            status, kind, note = "captcha_gated", "captcha_gated", captcha
        elif check_inline_deadline and _DEADLINE_CUE_RE.search(main_text) and "witness" in main_text.lower():
            status, kind, deadline_date, note, _, _ = _analyze_document_text(
                main_text, as_of=as_of, fiscal_year=fiscal_year, subcommittee=spec, hearings=hearings
            )
            return [
                DeadlineRecord(
                    chamber=chamber,
                    fiscal_year=fiscal_year,
                    subcommittee=spec.slug,
                    subcommittee_name=spec.name,
                    track=default_track,
                    source_url=final_url,
                    link_url="",
                    pdf_url="",
                    pdf_sha256="",
                    http_status=http_status,
                    as_of=as_of.isoformat(),
                    status=status,
                    deadline_kind=kind,
                    deadline_date=deadline_date,
                    deadline_text=note,
                    hearing_link="",
                    hearing_date="",
                )
            ]
        else:
            status, kind, note = "absent", "none", empty_note
        return [
            _empty_error_record(
                chamber=chamber,
                fiscal_year=fiscal_year,
                spec=spec,
                track=default_track,
                source_url=final_url,
                http_status=http_status,
                as_of=as_of,
                status=status,
                kind=kind,
                deadline_text=note,
            )
        ]

    records: list[DeadlineRecord] = []
    for link_text, link_url in notice_links:
        track = _classify_pdf_track(link_text, link_url)
        doc = _cached_resolve_pdf(client, link_url, pdf_cache, probes)
        if doc.captcha_marker is not None or doc.pdf_bytes is None:
            status, kind = classify_deadline(
                as_of=as_of,
                deadline_date=None,
                is_hearing_relative=False,
                hearing_matched=False,
                is_absent=doc.http_status in {404, 410},
                is_captcha_gated=doc.captcha_marker is not None,
            )
            records.append(
                _empty_error_record(
                    chamber=chamber,
                    fiscal_year=fiscal_year,
                    spec=spec,
                    track=track,
                    source_url=final_url,
                    link_url=link_url,
                    pdf_url=doc.pdf_url,
                    pdf_sha256=doc.pdf_sha256,
                    http_status=doc.http_status,
                    as_of=as_of,
                    status=status,
                    kind=kind,
                    deadline_text=doc.captcha_marker or doc.error,
                )
            )
            continue

        pdf_text = extract_pdf_text(doc.pdf_bytes)
        status, kind, deadline_date, deadline_text, hearing_link, hearing_date = _analyze_document_text(
            pdf_text,
            as_of=as_of,
            fiscal_year=fiscal_year,
            subcommittee=spec,
            hearings=hearings,
        )
        records.append(
            DeadlineRecord(
                chamber=chamber,
                fiscal_year=fiscal_year,
                subcommittee=spec.slug,
                subcommittee_name=spec.name,
                track=track,
                source_url=final_url,
                link_url=link_url,
                pdf_url=doc.pdf_url,
                pdf_sha256=doc.pdf_sha256,
                http_status=doc.http_status,
                as_of=as_of.isoformat(),
                status=status,
                deadline_kind=kind,
                deadline_date=deadline_date,
                deadline_text=deadline_text,
                hearing_link=hearing_link,
                hearing_date=hearing_date,
            )
        )
    return records


def scrape_house_subcommittee_page(
    client: httpx.Client,
    spec: SubcommitteeSpec,
    url: str,
    *,
    as_of: date,
    fiscal_year: int,
    pdf_cache: dict[str, ResolvedDocument],
    probes: list[PageProbe] | None = None,
) -> list[DeadlineRecord]:
    """Fetch and parse OWT / Member-Day PDFs or empty/CAPTCHA state for one House subcommittee."""
    return _scrape_subcommittee_page(
        client,
        spec,
        url,
        chamber="house",
        default_track="subcommittee_notice",
        scope_main=True,
        allow_coldfusion_download=False,
        link_keywords=("witness", "testimony", "member day", "members day", "owt", "fy27", "fy-27"),
        check_inline_deadline=True,
        empty_note="No OWT or Member-Day deadline notice on subcommittee page",
        as_of=as_of,
        fiscal_year=fiscal_year,
        hearings=(),
        pdf_cache=pdf_cache,
        probes=probes,
    )


def probe_house_dem_portal(
    client: httpx.Client,
    url: str,
    *,
    as_of: date,
    fiscal_year: int,
    probes: list[PageProbe] | None = None,
) -> DeadlineRecord:
    """Probe the House Appropriations Democrats site/form and mark CAPTCHA-gated portals explicitly."""
    html_text, final_url, http_status, err = fetch_html_page(client, url, probes=probes)
    captcha = detect_captcha(html_text or "", portal_or_form=True)
    if captcha is not None:
        status, kind = "captcha_gated", "captcha_gated"
        note = f"{captcha} on House Democrats portal"
    elif err:
        status, kind = ("absent" if http_status in {404, 410} else "uncertain"), "none"
        note = err
    else:
        status, kind = "absent", "none"
        note = "No active FY submission deadline or CAPTCHA form detected"
    return DeadlineRecord(
        chamber="house",
        fiscal_year=fiscal_year,
        subcommittee="democrats_portal",
        subcommittee_name="House Appropriations Democrats Portal",
        track="portal",
        source_url=final_url,
        link_url=url,
        pdf_url="",
        pdf_sha256="",
        http_status=http_status,
        as_of=as_of.isoformat(),
        status=status,
        deadline_kind=kind,
        deadline_date="",
        deadline_text=note,
        hearing_link="",
        hearing_date="",
    )


def parse_senate_general_guidance_pdf(
    pdf_text: str,
    *,
    source_url: str,
    link_url: str,
    pdf_url: str,
    pdf_sha256: str,
    http_status: int | None,
    as_of: date,
    fiscal_year: int,
) -> list[DeadlineRecord]:
    """Parse the Senate full-committee general guidance PDF's 12-subcommittee deadline table."""
    collapsed = " ".join(pdf_text.split())
    start_idx = collapsed.find("Submission Deadlines")
    if start_idx == -1:
        return []
    end_idx = collapsed.find("Congressionally Directed Spending", start_idx)
    section = collapsed[start_idx : end_idx if end_idx != -1 else len(collapsed)]

    row_pattern = re.compile(
        r"(?P<label>.+?)\s+"
        rf"(?P<cds>N/A|{_MONTH_PATTERN}\s+\d{{1,2}}(?:st|nd|rd|th)?,\s+20\d{{2}})\s+"
        rf"(?P<prog>N/A|{_MONTH_PATTERN}\s+\d{{1,2}}(?:st|nd|rd|th)?,\s+20\d{{2}})",
        re.IGNORECASE,
    )
    records: list[DeadlineRecord] = []
    seen_pairs: set[tuple[str, str]] = set()
    for match in row_pattern.finditer(section):
        label = match.group("label").strip()
        spec = match_subcommittee_by_name(label)
        if spec is None:
            continue
        for track, raw_val in (("cds", match.group("cds")), ("member_programmatic_language", match.group("prog"))):
            if (spec.slug, track) in seen_pairs:
                continue
            seen_pairs.add((spec.slug, track))
            if raw_val.strip().upper() == "N/A":
                records.append(
                    DeadlineRecord(
                        chamber="senate",
                        fiscal_year=fiscal_year,
                        subcommittee=spec.slug,
                        subcommittee_name=spec.name,
                        track=track,
                        source_url=source_url,
                        link_url=link_url,
                        pdf_url=pdf_url,
                        pdf_sha256=pdf_sha256,
                        http_status=http_status,
                        as_of=as_of.isoformat(),
                        status="absent",
                        deadline_kind="none",
                        deadline_date="",
                        deadline_text="N/A (not accepting CDS requests)",
                        hearing_link="",
                        hearing_date="",
                    )
                )
            else:
                parsed = parse_explicit_date(raw_val, fallback_year=fiscal_year - 1)
                status, kind = classify_deadline(
                    as_of=as_of,
                    deadline_date=parsed,
                    is_hearing_relative=False,
                    hearing_matched=False,
                    is_absent=parsed is None,
                    is_captcha_gated=False,
                )
                records.append(
                    DeadlineRecord(
                        chamber="senate",
                        fiscal_year=fiscal_year,
                        subcommittee=spec.slug,
                        subcommittee_name=spec.name,
                        track=track,
                        source_url=source_url,
                        link_url=link_url,
                        pdf_url=pdf_url,
                        pdf_sha256=pdf_sha256,
                        http_status=http_status,
                        as_of=as_of.isoformat(),
                        status=status,
                        deadline_kind=kind,
                        deadline_date=parsed.isoformat() if parsed else "",
                        deadline_text=raw_val.strip(),
                        hearing_link="",
                        hearing_date="",
                    )
                )
    return records


def scrape_senate_guidance_records(
    client: httpx.Client,
    html_text: str,
    source_url: str,
    *,
    as_of: date,
    fiscal_year: int,
    pdf_cache: dict[str, ResolvedDocument],
    probes: list[PageProbe] | None = None,
) -> list[DeadlineRecord]:
    """Fetch and parse Senate full-committee FY guidance PDF deadline table."""
    soup = BeautifulSoup(html_text, "html.parser")
    guidance_links: list[str] = []
    for anchor in soup.find_all("a", href=True):
        href = urljoin(source_url, str(anchor["href"]))
        lowered = href.lower()
        if "general_guidance" in lowered or ("guidance" in lowered and (".pdf" in lowered or "/download/" in lowered)):
            if href not in guidance_links:
                guidance_links.append(href)
    records: list[DeadlineRecord] = []
    for link_url in guidance_links:
        doc = _cached_resolve_pdf(client, link_url, pdf_cache, probes)
        if doc.pdf_bytes is None:
            continue
        pdf_text = extract_pdf_text(doc.pdf_bytes)
        records.extend(
            parse_senate_general_guidance_pdf(
                pdf_text,
                source_url=source_url,
                link_url=link_url,
                pdf_url=doc.pdf_url,
                pdf_sha256=doc.pdf_sha256,
                http_status=doc.http_status,
                as_of=as_of,
                fiscal_year=fiscal_year,
            )
        )
    return records


def scrape_senate_subcommittee_page(
    client: httpx.Client,
    spec: SubcommitteeSpec,
    url: str,
    *,
    as_of: date,
    fiscal_year: int,
    hearings: Sequence[HearingEntry],
    pdf_cache: dict[str, ResolvedDocument],
    probes: list[PageProbe] | None = None,
) -> list[DeadlineRecord]:
    """Fetch and parse OWT PDFs (including ColdFusion ``/download/*``) for one Senate subcommittee."""
    return _scrape_subcommittee_page(
        client,
        spec,
        url,
        chamber="senate",
        default_track="owt",
        scope_main=False,
        allow_coldfusion_download=True,
        link_keywords=("testimony", "owt", "witness"),
        check_inline_deadline=False,
        empty_note="No OWT deadline notice on subcommittee page",
        as_of=as_of,
        fiscal_year=fiscal_year,
        hearings=hearings,
        pdf_cache=pdf_cache,
        probes=probes,
    )


def load_prior_records(prior_path: Path) -> list[dict[str, object]]:
    """Load prior records from a JSON snapshot (either ``{"records": [...]}`` or a bare list)."""
    payload = json.loads(prior_path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and isinstance(payload.get("records"), list):
        return [item for item in payload["records"] if isinstance(item, dict)]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    raise ValueError(f"Unsupported prior snapshot format in {prior_path}")


def apply_prior_diff(
    records: Sequence[DeadlineRecord],
    prior_records: Sequence[Mapping[str, object]] | None,
) -> list[DeadlineRecord]:
    """Compare current records against ``prior_records`` and set ``updated=True/False``."""
    if prior_records is None:
        return [replace(rec, updated=False) for rec in records]

    by_exact_key: dict[tuple[str, str, str, str], Mapping[str, object]] = {}
    by_bucket: dict[tuple[str, str, str], list[Mapping[str, object]]] = {}
    for item in prior_records:
        chamber = str(item.get("chamber") or "")
        subcommittee = str(item.get("subcommittee") or "")
        track = str(item.get("track") or "")
        link_url = str(item.get("link_url") or "")
        by_exact_key[(chamber, subcommittee, track, link_url)] = item
        by_bucket.setdefault((chamber, subcommittee, track), []).append(item)

    current_bucket_counts: dict[tuple[str, str, str], int] = {}
    for rec in records:
        key = (rec.chamber, rec.subcommittee, rec.track)
        current_bucket_counts[key] = current_bucket_counts.get(key, 0) + 1

    updated_records: list[DeadlineRecord] = []
    for rec in records:
        exact_key = (rec.chamber, rec.subcommittee, rec.track, rec.link_url)
        prior = by_exact_key.get(exact_key)
        bucket_key = (rec.chamber, rec.subcommittee, rec.track)
        if prior is None and current_bucket_counts.get(bucket_key) == 1 and len(by_bucket.get(bucket_key, [])) == 1:
            prior = by_bucket[bucket_key][0]

        if prior is None:
            is_updated = bool(rec.pdf_sha256 or rec.deadline_date)
        else:
            prior_sha = str(prior.get("pdf_sha256") or "")
            prior_deadline = str(prior.get("deadline_date") or "")
            prior_hearing = str(prior.get("hearing_date") or "")
            prior_status = str(prior.get("status") or "")
            is_updated = (
                (bool(rec.pdf_sha256) and rec.pdf_sha256 != prior_sha)
                or (rec.deadline_date != prior_deadline)
                or (rec.hearing_date != prior_hearing)
                or (rec.status != prior_status and rec.status not in {"closed", "open"})
            )
        updated_records.append(
            DeadlineRecord(
                chamber=rec.chamber,
                fiscal_year=rec.fiscal_year,
                subcommittee=rec.subcommittee,
                subcommittee_name=rec.subcommittee_name,
                track=rec.track,
                source_url=rec.source_url,
                link_url=rec.link_url,
                pdf_url=rec.pdf_url,
                pdf_sha256=rec.pdf_sha256,
                http_status=rec.http_status,
                as_of=rec.as_of,
                status=rec.status,
                deadline_kind=rec.deadline_kind,
                deadline_date=rec.deadline_date,
                deadline_text=rec.deadline_text,
                hearing_link=rec.hearing_link,
                hearing_date=rec.hearing_date,
                updated=is_updated,
            )
        )
    return updated_records


def summarize_records(records: Sequence[DeadlineRecord], probes: Sequence[PageProbe]) -> dict[str, object]:
    """Compute deterministic summary counts by status, chamber, updates, and HTTP errors."""
    by_status: dict[str, int] = {status: 0 for status in sorted(VALID_STATUSES)}
    by_chamber: dict[str, int] = {}
    updated_count = 0
    for rec in records:
        by_status[rec.status] = by_status.get(rec.status, 0) + 1
        by_chamber[rec.chamber] = by_chamber.get(rec.chamber, 0) + 1
        if rec.updated:
            updated_count += 1
    http_errors = sum(1 for p in probes if p.error)
    return {
        "total": len(records),
        "by_status": by_status,
        "by_chamber": dict(sorted(by_chamber.items())),
        "updated_count": updated_count,
        "http_errors": http_errors,
        "probe_count": len(probes),
    }


def collect_appropriations_deadlines(
    *,
    as_of: date,
    fiscal_year: int = 2027,
    prior_records: Sequence[Mapping[str, object]] | None = None,
    transport: httpx.BaseTransport | None = None,
    timeout: float = 30.0,
    max_hearing_pages: int = 2,
    house_guidance_url: str = HOUSE_GUIDANCE_URL,
    house_dem_portal_url: str = HOUSE_DEM_PORTAL_URL,
    senate_guidance_url: str = SENATE_GUIDANCE_URL,
    senate_hearings_url: str = SENATE_HEARINGS_URL,
) -> TrackerReport:
    """Run the full House + Senate Appropriations deadlines tracker and return a deterministic report."""
    probes: list[PageProbe] = []
    pdf_cache: dict[str, ResolvedDocument] = {}
    records: list[DeadlineRecord] = []

    with build_http_client(transport=transport, timeout=timeout) as client:
        house_html, house_final, _, _ = fetch_html_page(client, house_guidance_url, probes=probes)
        if house_html:
            records.extend(
                parse_house_guidance_records(
                    client,
                    house_html,
                    house_final,
                    as_of=as_of,
                    fiscal_year=fiscal_year,
                    pdf_cache=pdf_cache,
                    probes=probes,
                )
            )
            house_subs = discover_subcommittee_urls(house_html, house_final, chamber="house")
        else:
            house_subs = discover_subcommittee_urls("", house_guidance_url, chamber="house")

        for spec in SUBCOMMITTEE_SPECS:
            records.extend(
                scrape_house_subcommittee_page(
                    client,
                    spec,
                    house_subs[spec.slug],
                    as_of=as_of,
                    fiscal_year=fiscal_year,
                    pdf_cache=pdf_cache,
                    probes=probes,
                )
            )

        records.append(
            probe_house_dem_portal(
                client,
                house_dem_portal_url,
                as_of=as_of,
                fiscal_year=fiscal_year,
                probes=probes,
            )
        )

        senate_hearings = fetch_senate_hearings(
            client,
            hearings_url=senate_hearings_url,
            max_pages=max_hearing_pages,
            probes=probes,
        )

        senate_html, senate_final, _, _ = fetch_html_page(client, senate_guidance_url, probes=probes)
        if senate_html:
            records.extend(
                scrape_senate_guidance_records(
                    client,
                    senate_html,
                    senate_final,
                    as_of=as_of,
                    fiscal_year=fiscal_year,
                    pdf_cache=pdf_cache,
                    probes=probes,
                )
            )
            senate_subs = discover_subcommittee_urls(senate_html, senate_final, chamber="senate")
        else:
            senate_subs = discover_subcommittee_urls("", senate_guidance_url, chamber="senate")

        for spec in SUBCOMMITTEE_SPECS:
            records.extend(
                scrape_senate_subcommittee_page(
                    client,
                    spec,
                    senate_subs[spec.slug],
                    as_of=as_of,
                    fiscal_year=fiscal_year,
                    hearings=senate_hearings,
                    pdf_cache=pdf_cache,
                    probes=probes,
                )
            )

    sorted_records = sorted(records, key=lambda rec: rec.sort_key())
    diffed_records = apply_prior_diff(sorted_records, prior_records)
    sorted_probes = sorted(probes, key=lambda p: (p.url, p.final_url))
    return {
        "as_of": as_of.isoformat(),
        "fiscal_year": fiscal_year,
        "summary": summarize_records(diffed_records, sorted_probes),
        "records": [rec.to_dict() for rec in diffed_records],
        "page_probes": [probe.to_dict() for probe in sorted_probes],
    }


def write_json_report(report: TrackerReport | Mapping[str, object], path: Path) -> None:
    """Write deterministic formatted JSON report to ``path``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_csv_report(records: Iterable[Mapping[str, object]], path: Path) -> None:
    """Write deterministic CSV output with fixed column ordering to ``path``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(CSV_COLUMNS), lineterminator="\n", extrasaction="ignore")
        writer.writeheader()
        for record in records:
            writer.writerow(record)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the appropriations deadlines tracker."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--as-of",
        default=date.today().isoformat(),
        help="Reference date YYYY-MM-DD used to classify deadlines (default: today)",
    )
    parser.add_argument("--fiscal-year", type=int, default=2027, help="Target appropriations fiscal year (default: 2027)")
    parser.add_argument("--prior", type=Path, help="Optional prior JSON snapshot for SHA-256 / deadline diffing")
    parser.add_argument("--json-out", type=Path, help="Optional output path for deterministic JSON report")
    parser.add_argument("--csv-out", type=Path, help="Optional output path for deterministic CSV table")
    parser.add_argument(
        "--max-hearing-pages",
        type=int,
        default=2,
        help="Maximum Senate /hearings listing pages to inspect (default: 2)",
    )
    parser.add_argument("--timeout", type=float, default=30.0, help="Per-request HTTP timeout in seconds (default: 30)")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point for tracking House and Senate Appropriations deadlines."""
    args = parse_args(argv)
    as_of = date.fromisoformat(args.as_of)
    prior_records = load_prior_records(args.prior) if args.prior else None
    report = collect_appropriations_deadlines(
        as_of=as_of,
        fiscal_year=args.fiscal_year,
        prior_records=prior_records,
        timeout=args.timeout,
        max_hearing_pages=args.max_hearing_pages,
    )
    if args.json_out:
        write_json_report(report, args.json_out)
    if args.csv_out:
        write_csv_report(report["records"], args.csv_out)
    print(json.dumps({"as_of": report["as_of"], "fiscal_year": report["fiscal_year"], "summary": report["summary"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
