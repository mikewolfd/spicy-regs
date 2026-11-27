"""Exact scorecard links over already-published, immutable congressional inputs.

This module performs no acquisition and never changes source ratings. Its output
is all VARCHAR (JSON details are serialized text). Unknown or conflicting source
identifiers remain visible instead of falling through to a weaker name match.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
import json
import re
from typing import Any, Mapping, Sequence
import unicodedata

from spicy_docs.sources.congress.votes import vote_day

RULE_VERSION = "scorecard-resolution-v1.11"
SOURCE_TABLES = ("scorecards", "scorecard_members", "scorecard_items")
OFFICIAL_TABLES = ("members", "member_terms", "congress_bills", "amendments", "roll_call_votes")
INPUT_TABLES = SOURCE_TABLES + OFFICIAL_TABLES
OFFICIAL_COLUMNS = {
    "members": (
        "bioguide_id",
        "bioguide_previous_json",
        "lis_id",
        "fec_ids_json",
        "icpsr_id",
        "govtrack_id",
        "votesmart_id",
        "opensecrets_id",
        "wikidata_id",
        "name_first",
        "name_last",
        "other_names_json",
    ),
    "member_terms": (
        "bioguide_id",
        "term_index",
        "term_type",
        "term_start",
        "term_end",
        "term_state",
        "term_district",
        "term_party",
        "observed_at",
    ),
    "congress_bills": ("bill_id",),
    "amendments": ("amendment_id", "amended_bill_id"),
    "roll_call_votes": ("vote_id", "bill_id", "vote_date"),
}
COMMON_COLUMNS = (
    "source_snapshot_id",
    "resolution_status",
    "resolution_rule",
    "rule_version",
    "candidate_count",
    "candidates_json",
    "reason",
    "capture_id",
    "source_url",
    "source_path",
)
MEMBER_LINK_COLUMNS = (
    "scorecard_id",
    "publisher_member_key",
    "bioguide_id",
    "normalized_state",
    "state_status",
    "normalized_district",
    "district_status",
    "normalized_party",
    "party_status",
    "correction_evidence_json",
    "override_version",
) + COMMON_COLUMNS
ITEM_LINK_COLUMNS = (
    "scorecard_id",
    "item_id",
    "reference_id",
    "source_reference_id",
    "congress",
    "chamber",
    "session",
    "roll_number",
    "vote_id",
    "bill_id",
    "amendment_id",
) + COMMON_COLUMNS
LINK_COLUMNS = {"scorecard_member_links": MEMBER_LINK_COLUMNS, "scorecard_item_links": ITEM_LINK_COLUMNS}
LINK_KEYS = {
    "scorecard_member_links": ("scorecard_id", "publisher_member_key"),
    "scorecard_item_links": ("scorecard_id", "item_id", "reference_id"),
}

Row = Mapping[str, str | None]
Rows = Mapping[str, Sequence[Row]]
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_SCHEMES = {
    "bioguide": "bioguide_id",
    "bioguide_id": "bioguide_id",
    "lis": "lis_id",
    "lis_id": "lis_id",
    "govtrack": "govtrack_id",
    "govtrack_id": "govtrack_id",
    "votesmart": "votesmart_id",
    "votesmart_id": "votesmart_id",
    "icpsr": "icpsr_id",
    "icpsr_id": "icpsr_id",
    "opensecrets": "opensecrets_id",
    "opensecrets_id": "opensecrets_id",
    "wikidata": "wikidata_id",
    "wikidata_id": "wikidata_id",
    "fec": "fec_id",
    "fec_id": "fec_id",
}
_PUBLISHER_NATIVE_SCHEMES = frozenset({
    "cpac",
    "cpac_profile",
    "wix_people",
    "club_for_growth",
    "heritage_action",
    "aflcio",
    "niac",
    "agc",
    "freedomindex.us:legislator",
    "peaceaction.org:legislator",
    "peaceaction.org:wpc_id",
    "ocd-division",
    "limitedgov.org:person",
    "votervoice_occupancy_id",
    "votervoice_politician_id",
    "quorum",
    "numbersusa",
    "KnowWhoPersonID",
    "alaska_wilderness_action_sheet_uid",
    "ntu_slug",
    "cdf_wp_post_id",
    "legislatorKey",
    "legislatorKeySimple",
    "bipac.net:leg_id_num",
    "bipac.net:tenant",
})
_BIOGUIDE_SHAPE = re.compile(r"[A-Z][0-9]{6}\Z")
_PARTIES = {
    "d": "Democrat",
    "dem": "Democrat",
    "dem.": "Democrat",
    "democrat": "Democrat",
    "democrats": "Democrat",
    "democratic": "Democrat",
    "(d)": "Democrat",
    "r": "Republican",
    "rep": "Republican",
    "rep.": "Republican",
    "republican": "Republican",
    "republicans": "Republican",
    "(r)": "Republican",
    "i": "Independent",
    "ind": "Independent",
    "ind.": "Independent",
    "independent": "Independent",
    "(i)": "Independent",
    "l": "Libertarian",
    "lib": "Libertarian",
    "libertarian": "Libertarian",
    "id": "Independent Democrat",
    "ind. democrat": "Independent Democrat",
    "independent democrat": "Independent Democrat",
}
_STATE_NAMES = (
    "AL:Alabama|AK:Alaska|AZ:Arizona|AR:Arkansas|CA:California|CO:Colorado|CT:Connecticut|DE:Delaware|"
    "FL:Florida|GA:Georgia|HI:Hawaii|ID:Idaho|IL:Illinois|IN:Indiana|IA:Iowa|KS:Kansas|KY:Kentucky|"
    "LA:Louisiana|ME:Maine|MD:Maryland|MA:Massachusetts|MI:Michigan|MN:Minnesota|MS:Mississippi|"
    "MO:Missouri|MT:Montana|NE:Nebraska|NV:Nevada|NH:New Hampshire|NJ:New Jersey|NM:New Mexico|"
    "NY:New York|NC:North Carolina|ND:North Dakota|OH:Ohio|OK:Oklahoma|OR:Oregon|PA:Pennsylvania|"
    "RI:Rhode Island|SC:South Carolina|SD:South Dakota|TN:Tennessee|TX:Texas|UT:Utah|VT:Vermont|"
    "VA:Virginia|WA:Washington|WV:West Virginia|WI:Wisconsin|WY:Wyoming|DC:District of Columbia|"
    "AS:American Samoa|GU:Guam|MP:Northern Mariana Islands|PR:Puerto Rico|VI:Virgin Islands"
)
_STATES: dict[str, str] = {
    token.casefold(): code
    for entry in _STATE_NAMES.split("|")
    for code, name in [entry.split(":")]
    for token in (code, name)
}
_CHAMBERS = {
    "h": "house",
    "house": "house",
    "rep": "house",
    "representative": "house",
    "house of representatives": "house",
    "u.s. house of representatives": "house",
    "us house": "house",
    "u.s house": "house",
    "u.s. house": "house",
    "s": "senate",
    "senate": "senate",
    "sen": "senate",
    "senator": "senate",
    "us senate": "senate",
    "u.s. senate": "senate",
}


def classify_identifier_scheme(scheme: str | None) -> str:
    """Classify a source identifier scheme without conflating publisher keys with crosswalks."""
    if scheme in _SCHEMES:
        return "authoritative"
    if scheme in _PUBLISHER_NATIVE_SCHEMES:
        return "publisher_native"
    return "unrecognized"


def qualified_item_targets(status: str, targets: Mapping, candidates: Sequence[Mapping]) -> tuple[dict, list[dict]]:
    """Keep exact partial evidence among candidates; direct keys require a resolved association."""
    retained = [dict(candidate) for candidate in candidates]
    if status == "resolved":
        return dict(targets), retained
    for field, value in targets.items():
        if value is not None and not any(candidate.get(field) == value for candidate in retained):
            retained.append({field: value})
    return {}, retained


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def _array(value: str | None, label: str) -> list:
    result = json.loads(value) if value else []
    if not isinstance(result, list):
        raise ValueError(f"{label} must contain a JSON array")
    return result


def _historical_array(value: str | None, label: str) -> list:
    # An explicit source null remains distinct in the member table but adds no
    # identity candidates, just like an absent or empty list.
    return [] if value == "null" else _array(value, label)


def _alias_bounds(patch: dict) -> tuple[date, date] | None:
    fields = {"first", "middle", "last", "suffix", "nickname", "official_full", "start", "end"}
    if set(patch) - fields or not (patch.get("start") or patch.get("end")):
        return None
    bounds = []
    for field, default in (("start", date.min), ("end", date.max)):
        value = patch.get(field)
        if value is None:
            bounds.append(default)
        elif not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            return None
        else:
            try:
                bounds.append(date.fromisoformat(value))
            except ValueError:
                return None
    return (bounds[0], bounds[1]) if bounds[0] < bounds[1] else None


def _text(value: str | None) -> str:
    return (value or "").strip()


def _chamber(value: str | None) -> str | None:
    return _CHAMBERS.get(_text(value).casefold())


def _district(value: str | None) -> str | None:
    text = _text(value).casefold()
    if text in {"at-large", "at large", "al", "at-large congressional district"}:
        return "0"
    if text.isdecimal():
        return str(int(text))
    return _number(text, r"Congressional District")


def _number(value: str | None, suffix: str = "") -> str | None:
    match = re.fullmatch(r"\s*(\d+)(?:st|nd|rd|th)?\s*" + suffix + r"\s*", value or "", re.I)
    return str(int(match[1])) if match else None


def _congress(value: str | None) -> str | None:
    return _number(value, r"(?:Congress)?")


def _name(value: str | None) -> str:
    text = unicodedata.normalize("NFKC", _text(value)).casefold()
    if text.count(",") == 1:
        last, first = text.split(",")
        text = first + " " + last
    return " ".join("".join(" " if unicodedata.category(ch).startswith("P") else ch for ch in text).split())


def _folded_tokens(value: str | None) -> set[str]:
    """Accent-folded lowercase word tokens for name-conflict guards."""
    text = unicodedata.normalize("NFKD", _name(value))
    stripped = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    return {tok for tok in stripped.split() if len(tok) > 1}


def _party(value: str | None) -> str | None:
    text = _text(value)
    if not text:
        return None
    return _PARTIES.get(text.casefold(), text.casefold())


def _cpac_is_historical_lifetime(source: Row) -> bool:
    """True when a CPAC observation carries empty period history and no period-scoped state/district/party."""
    if any(_text(source.get(field)) for field in ("state", "district", "party")):
        return False
    notes = _text(source.get("notes_text"))
    if not notes:
        return False
    try:
        parsed = json.loads(notes)
    except ValueError:
        return False
    return isinstance(parsed, dict) and parsed.get("history") == []


def _index(rows: Sequence[Row], keys: tuple[str, ...], label: str) -> dict:
    result = {}
    for row in rows:
        if any(value is not None and not isinstance(value, str) for value in row.values()):
            raise ValueError(f"{label} contains a non-VARCHAR value")
        key = tuple(row.get(k) for k in keys)
        if any(not _text(value) for value in key) or key in result:
            raise ValueError(f"{label} contains a missing or repeated identity {key}")
        result[key] = row
    return result


def _pins(pins: Mapping[str, Mapping[str, Any]]) -> str:
    for table in INPUT_TABLES:
        pin = pins.get(table)
        if not isinstance(pin, Mapping):
            raise ValueError(f"Missing immutable input pin for {table}")
        digest_fields = {field for field in ("sha256", "tableDescriptorDigest") if field in pin}
        if len(digest_fields) != 1 or not all(_DIGEST.fullmatch(str(pin[field])) for field in digest_fields):
            raise ValueError(f"{table} requires exactly one sha256 or tableDescriptorDigest pin")
        if not _DIGEST.fullmatch(str(pin.get("artifactDigest", ""))):
            raise ValueError(f"{table} requires an artifactDigest pin")
        if not isinstance(pin.get("family"), str) or not pin["family"]:
            raise ValueError(f"{table} requires a family pin")
        if type(pin.get("byteSize")) is not int or pin["byteSize"] < 0:
            raise ValueError(f"{table} requires a nonnegative byteSize")
    return _json(pins)


def _window(text: str | None) -> tuple[date, date] | None:
    """Only exact source dates, ISO intervals, years or numbered Congresses."""
    text = _text(text)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        start = date.fromisoformat(text)
        return start, start + timedelta(days=1)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}/\d{4}-\d{2}-\d{2}", text):
        start, end = map(date.fromisoformat, text.split("/"))
        if start >= end:
            raise ValueError("Source period interval must have an end after its start")
        return start, end
    if (year_match := re.fullmatch(r"(\d{4})(?:\.0)?", text)) and 1789 <= int(year_match[1]) < 2100:
        year = int(year_match[1])
        return date(year, 1, 1), date(year + 1, 1, 1)
    match = re.fullmatch(r"(\d+)(?:st|nd|rd|th)?\s+Congress", text, re.I)
    if match and 1 <= int(match[1]) < 150:
        congress = int(match[1])
        year = 1789 + (congress - 1) * 2
        return date(year, 1 if congress >= 74 else 3, 3 if congress >= 74 else 4), date(
            year + 2, 1 if congress >= 73 else 3, 3 if congress >= 73 else 4
        )
    return None


def _context(member: Row, edition: Row) -> dict:
    notes = []
    state_text = member.get("state")
    state = _STATES.get(_text(state_text).casefold())
    if state_text and not state:
        notes.append(f"unknown_state_literal_ignored:{state_text}")
    chamber_text = member.get("chamber_text") or edition.get("chamber_scope_text")
    chamber = _chamber(chamber_text)
    if chamber_text and not chamber and _text(chamber_text).casefold() not in {"both", "house and senate"}:
        notes.append(f"unknown_chamber_literal_ignored:{chamber_text}")
    district = _district(member.get("district"))
    if chamber == "senate" and member.get("district"):
        notes.append(f"senate_district_not_applicable_literal:{member['district']}")
        district = None
    elif member.get("district") and district is None:
        notes.append(f"unknown_district_literal_ignored:{member['district']}")
    periods = []
    edition_periods = _array(edition.get("periods_json"), "periods_json")
    if member.get("period_text"):
        periods = [member["period_text"]]
    elif edition_periods:
        # Explicit coverage outranks an edition/release year. Unsupported text
        # must not fall through to a narrower year and silently lose history.
        periods = [entry.get("period_text") for entry in edition_periods]
    elif edition.get("year_text"):
        periods = [edition["year_text"]]
    elif congress := _congress(edition.get("congress_text")):
        periods = [congress + " Congress"]
    windows = [_window(period) for period in periods]
    if not windows or any(window is None for window in windows):
        windows = []
        notes.append("no_exact_historical_period")
    return {"state": state, "chamber": chamber, "district": district, "windows": windows, "notes": notes}


class _Members:
    def __init__(self, members: Sequence[Row], terms: Sequence[Row]):
        self.members = {key[0]: row for key, row in _index(members, ("bioguide_id",), "members").items()}
        self.terms = defaultdict(list)
        for term in _index(terms, ("bioguide_id", "term_index"), "member_terms").values():
            if term["bioguide_id"] not in self.members:
                raise ValueError("member_terms references a member absent from the pinned members table")
            self.terms[term["bioguide_id"]].append(term)
        self.ids = defaultdict(set)
        self.exact = defaultdict(set)
        self.normal = defaultdict(set)
        self.previous_ids = defaultdict(set)
        self.aliases = {}
        self.exact_aliases = defaultdict(lambda: defaultdict(list))
        self.normal_aliases = defaultdict(lambda: defaultdict(list))
        self.last_tokens: dict[str, set[str]] = {}
        for bioguide, row in self.members.items():
            for field in set(_SCHEMES.values()) - {"fec_id"}:
                if row.get(field):
                    self.ids[field, row[field]].add(bioguide)
            for value in _array(row.get("fec_ids_json"), "fec_ids_json"):
                if not isinstance(value, str):
                    raise ValueError("FEC identifiers must be strings")
                self.ids["fec_id", value].add(bioguide)
            for value in _historical_array(row.get("bioguide_previous_json"), "bioguide_previous_json"):
                if not isinstance(value, str) or not value.strip():
                    raise ValueError("Previous Bioguide identifiers must be nonempty strings")
                self.ids["bioguide_id", value].add(bioguide)
                self.previous_ids[value].add(bioguide)
            first, last = _text(row.get("name_first")), _text(row.get("name_last"))
            last_set = _folded_tokens(last)
            if first and last:
                for name in (first + " " + last, last + ", " + first, last + " " + first):
                    self.exact[name].add(bioguide)
                    self.normal[_name(name)].add(bioguide)
            if last:
                self.exact[last].add(bioguide)
            aliases = _historical_array(row.get("other_names_json"), "other_names_json")
            self.aliases[bioguide] = aliases
            for index, patch in enumerate(aliases):
                if not isinstance(patch, dict):
                    raise ValueError("other_names_json entries must be objects")
                for field in ("first", "middle", "last", "suffix", "nickname", "official_full", "start", "end"):
                    if patch.get(field) is not None and not isinstance(patch[field], str):
                        raise ValueError(f"other_names_json {field} must be a string or null")
                alias_first = _text(patch.get("first", first))
                alias_last = _text(patch.get("last", last))
                last_set |= _folded_tokens(alias_last)
                if alias_first and alias_last:
                    for name in (
                        alias_first + " " + alias_last,
                        alias_last + ", " + alias_first,
                        alias_last + " " + alias_first,
                    ):
                        self.exact_aliases[name][bioguide].append(index)
                        self.normal_aliases[_name(name)][bioguide].append(index)
                if alias_last:
                    self.exact_aliases[alias_last][bioguide].append(index)
            self.last_tokens[bioguide] = last_set

    def name_compatible(self, source_name: str | None, bioguide: str) -> bool:
        """Require at least one accent-folded surname token overlap with the authoritative member."""
        source_tokens = _folded_tokens(source_name)
        allowed = self.last_tokens.get(bioguide, set())
        return bool(source_tokens and allowed and (source_tokens & allowed))

    def prior_service_terms(self, bioguide: str, context: dict) -> list[Row]:
        """Authoritative terms in the observed chamber starting on or before the edition window."""
        if not context["windows"]:
            return []
        cutoff = max(right for _, right in context["windows"])
        matches = []
        for term in self.terms[bioguide]:
            if context["chamber"] and _chamber(term.get("term_type")) != context["chamber"]:
                continue
            start_text, end_text = term.get("term_start"), term.get("term_end")
            if not start_text or not end_text:
                continue
            start, end = date.fromisoformat(start_text), date.fromisoformat(end_text)
            if start >= end:
                raise ValueError("Pinned member term ends before it starts")
            if start < cutoff:
                matches.append(term)
        return sorted(matches, key=lambda row: (row.get("term_start") or "", row["term_index"]))

    def alias_terms(self, bioguide: str, index: int, context: dict, inclusive: bool = False) -> list[Row]:
        bounds = _alias_bounds(self.aliases[bioguide][index])
        if not bounds or not context["windows"]:
            return []
        matches = []
        for term in self.terms_for(bioguide, context, inclusive):
            start_text, end_text = term.get("term_start"), term.get("term_end")
            if not start_text or not end_text:
                continue
            start, end = date.fromisoformat(start_text), date.fromisoformat(end_text)
            for left, right in context["windows"]:
                term_end = (
                    end + timedelta(days=1) if inclusive and right - left == timedelta(days=1) and left == end else end
                )
                if max(start, bounds[0], left) < min(term_end, bounds[1], right):
                    matches.append(term)
                    break
        return matches

    def name_terms(
        self, key: str, context: dict, base_names: set, alias_names: Mapping, inclusive: bool = False
    ) -> list[Row]:
        terms = self.terms_for(key, context, inclusive) if key in base_names else []
        for index in set(alias_names.get(key, [])):
            terms.extend(self.alias_terms(key, index, context, inclusive))
        unique = {term["term_index"]: term for term in terms}
        return sorted(unique.values(), key=lambda row: (row.get("term_start") or "", row["term_index"]))

    def terms_for(self, bioguide: str, context: dict, inclusive: bool = False) -> list[Row]:
        matches = []
        for term in self.terms[bioguide]:
            if context["state"] and term.get("term_state") != context["state"]:
                continue
            if context["chamber"] and _chamber(term.get("term_type")) != context["chamber"]:
                continue
            if context["district"] is not None and _district(term.get("term_district")) != context["district"]:
                continue
            if context["windows"]:
                if not term.get("term_start") or not term.get("term_end"):
                    continue
                start, end = date.fromisoformat(term["term_start"]), date.fromisoformat(term["term_end"])
                if start >= end:
                    raise ValueError("Pinned member term ends before it starts")
                if not any(
                    start < right
                    and end > left
                    or (inclusive and right - left == timedelta(days=1) and start <= left == end)
                    for left, right in context["windows"]
                ):
                    continue
            matches.append(term)
        return sorted(matches, key=lambda row: (row.get("term_start") or "", row["term_index"]))

    def uncertain_geography_terms(self, bioguide: str, context: dict, inclusive: bool) -> list[Row]:
        """Identify terms that may overlap but cannot be assigned to the chamber and period."""
        uncertain = []
        for term in self.terms[bioguide]:
            chamber = _chamber(term.get("term_type"))
            if chamber and chamber != context["chamber"]:
                continue
            start_text, end_text = term.get("term_start"), term.get("term_end")
            if chamber and start_text and end_text:
                continue  # The normal term lookup checks these bounds.
            try:
                start = date.fromisoformat(start_text) if start_text else None
                end = date.fromisoformat(end_text) if end_text else None
            except (TypeError, ValueError):
                uncertain.append(term)
                continue
            for left, right in context["windows"]:
                ends_after_left = end is None or end > left or (
                    inclusive and right - left == timedelta(days=1) and end == left
                )
                if (start is None or start < right) and ends_after_left:
                    uncertain.append(term)
                    break
        return uncertain

    def resolve(self, source: Row, edition: Row, override: Row | None) -> dict:
        context = _context(source, edition)
        reasons = list(context["notes"])
        matches = defaultdict(set)
        alias_details = defaultdict(dict)
        id_sets = []
        cpac_keys: list[str] = []
        identifiers = _array(source.get("identifiers_json"), "identifiers_json")
        for field in _SCHEMES.values():
            if source.get(field):
                identifiers.append({"scheme": field, "value": source[field]})
        for identifier in identifiers:
            scheme = identifier.get("scheme", identifier.get("field"))
            value = identifier.get("value")
            classification = classify_identifier_scheme(scheme)
            if classification == "publisher_native":
                if (
                    scheme == "cpac"
                    and edition.get("publisher_id") == "cpac"
                    and isinstance(value, str)
                    and _BIOGUIDE_SHAPE.fullmatch(value)
                ):
                    cpac_keys.append(value)
                continue
            if classification != "authoritative":
                reasons.append(f"unrecognized_identifier_scheme:{scheme}")
                continue
            if not isinstance(value, str) or not value:
                reasons.append(f"invalid_explicit_identifier:{scheme}")
                id_sets.append(set())
                continue
            field = _SCHEMES[scheme]
            ids = self.ids[field, value]
            id_sets.append(ids)
            for bioguide in ids:
                matches[bioguide].add(field)
                if field == "bioguide_id" and bioguide in self.previous_ids[value]:
                    matches[bioguide].add("bioguide_previous")
            if not ids:
                reasons.append(f"unknown_explicit_identifier:{scheme}:{value}")
        method = "explicit_bioguide" if any("bioguide_id" in rules for rules in matches.values()) else "exact_crosswalk"
        status, chosen = "unresolved", None
        term_matches = {bioguide: self.terms_for(bioguide, context) for bioguide in matches}
        distinct_cpac = sorted(set(cpac_keys))
        if id_sets:
            agreed = set.intersection(*map(set, id_sets))
            if distinct_cpac:
                cpac_candidates = set.union(*(set(self.ids["bioguide_id", key]) for key in distinct_cpac))
                if len(distinct_cpac) > 1 or (agreed and cpac_candidates and not (agreed & cpac_candidates)):
                    agreed = set()
                    reasons.append(f"publisher_key_conflicts_with_authoritative_id:cpac:{','.join(distinct_cpac)}")
            if not agreed:
                status, method = "conflict", "conflicting_or_unknown_identifiers"
            else:
                eligible = {
                    key
                    for key in agreed
                    if term_matches[key]
                    or not any(
                        [context["state"], context["chamber"], context["district"] is not None, context["windows"]]
                    )
                }
                if not eligible:
                    inclusive = {key: self.terms_for(key, context, inclusive=True) for key in agreed}
                    if sum(bool(terms) for terms in inclusive.values()) == 1:
                        term_matches.update(inclusive)
                        eligible = {key for key in agreed if inclusive[key]}
                        reasons.append("unique_inclusive_term_end_fallback")
                if not eligible:
                    status, method = "conflict", "identifier_context_conflict"
                elif len(eligible) == 1:
                    status, chosen = "resolved", next(iter(eligible))
                else:
                    status = "ambiguous"
            # Known identifiers never fall through to name or override rules.
        elif distinct_cpac:
            # Qualified CPAC key-first matching against authoritative members/member_terms
            # with explicit name + period/historical-service conflict guards.
            if len(distinct_cpac) > 1:
                status, method = "conflict", "conflicting_publisher_keys"
                reasons.append(f"conflicting_publisher_keys:cpac:{','.join(distinct_cpac)}")
            else:
                cpac_key = distinct_cpac[0]
                key_ids = set(self.ids["bioguide_id", cpac_key])
                for bioguide in key_ids:
                    matches[bioguide].add("publisher_key:cpac")
                    if bioguide in self.previous_ids[cpac_key]:
                        matches[bioguide].add("bioguide_previous")
                    term_matches[bioguide] = self.terms_for(bioguide, context)
                if not key_ids:
                    status, method = "unresolved", "unknown_publisher_key"
                    reasons.append(f"unknown_publisher_key:cpac:{cpac_key}")
                else:
                    name_ok = {key for key in key_ids if self.name_compatible(source.get("member_name"), key)}
                    if not name_ok:
                        status, method = "conflict", "publisher_key_name_conflict"
                        reasons.append(f"publisher_key_name_conflict:cpac:{cpac_key}")
                    else:
                        has_context = any(
                            [context["state"], context["chamber"], context["district"] is not None, context["windows"]]
                        )
                        eligible = {key for key in name_ok if term_matches[key] or not has_context}
                        if not eligible:
                            inclusive = {key: self.terms_for(key, context, inclusive=True) for key in name_ok}
                            if sum(bool(terms) for terms in inclusive.values()) == 1:
                                term_matches.update(inclusive)
                                eligible = {key for key in name_ok if inclusive[key]}
                                reasons.append("unique_inclusive_term_end_fallback")
                        if len(eligible) == 1:
                            status, chosen, method = (
                                "resolved",
                                next(iter(eligible)),
                                "qualified_publisher_key_period_term",
                            )
                        elif len(eligible) > 1:
                            status, method = "ambiguous", "qualified_publisher_key_period_term"
                        elif _cpac_is_historical_lifetime(source) and context["windows"]:
                            prior = {key: self.prior_service_terms(key, context) for key in name_ok}
                            hist_eligible = {key for key in name_ok if prior[key]}
                            if len(hist_eligible) == 1:
                                chosen = next(iter(hist_eligible))
                                status, method = "resolved", "qualified_publisher_key_historical_service"
                                term_matches[chosen] = prior[chosen]
                                reasons.append("historical_lifetime_observation_prior_terms")
                            elif len(hist_eligible) > 1:
                                status, method = "ambiguous", "qualified_publisher_key_historical_service"
                            else:
                                status, method = "conflict", "publisher_key_context_conflict"
                                reasons.append(f"publisher_key_no_matching_historical_term:cpac:{cpac_key}")
                        else:
                            status, method = "conflict", "publisher_key_context_conflict"
                            reasons.append(f"publisher_key_context_conflict:cpac:{cpac_key}")
            # Qualified publisher keys with conflicts never fall through to name heuristics.
        else:
            exact_context = (
                context["state"]
                and context["chamber"]
                and (context["chamber"] == "senate" or context["district"] is not None)
            )
            if context["windows"]:
                rules = []
                if exact_context:
                    literal = _text(source.get("member_name"))
                    rules.append(("exact_historical_name_context", self.exact[literal], self.exact_aliases[literal]))
                normalized = _name(source.get("member_name"))
                rules.append(
                    ("unique_normalized_name_historical_term", self.normal[normalized], self.normal_aliases[normalized])
                )
                for rule, base_names, alias_names in rules:
                    names = base_names | set(alias_names)
                    for key in names:
                        matches[key].add(rule)
                        term_matches[key] = self.name_terms(key, context, base_names, alias_names)
                        for index in set(alias_names.get(key, [])):
                            matches[key].add("historical_alias")
                            alias_details[key][index] = {
                                "alias_index": index,
                                "source_patch": self.aliases[key][index],
                                "context_match": bool(self.alias_terms(key, index, context)),
                            }
                            note = f"historical_alias_outside_context_or_unqualified:{key}:{index}"
                            if not alias_details[key][index]["context_match"] and note not in reasons:
                                reasons.append(note)
                    eligible = {key for key in names if term_matches[key]}
                    if not eligible:
                        inclusive = {
                            key: self.name_terms(key, context, base_names, alias_names, inclusive=True) for key in names
                        }
                        eligible = {key for key in names if inclusive[key]}
                        if len(eligible) == 1:
                            term_matches.update(inclusive)
                            reasons.append("unique_inclusive_term_end_fallback")
                            for key in eligible:
                                for index in alias_details[key]:
                                    if self.alias_terms(key, index, context, inclusive=True):
                                        alias_details[key][index]["context_match"] = True
                                        note = f"historical_alias_outside_context_or_unqualified:{key}:{index}"
                                        if note in reasons:
                                            reasons.remove(note)
                        else:
                            eligible = set()
                    if len(eligible) == 1:
                        chosen, status, method = next(iter(eligible)), "resolved", rule
                        break
                    if eligible:
                        status, method = "ambiguous", rule
            if not chosen and override:
                key = _text(override["bioguide_id"])
                matches[key].add("versioned_override")
                term_matches[key] = self.terms_for(key, context)
                if (
                    any([context["state"], context["chamber"], context["district"] is not None, context["windows"]])
                    and not term_matches[key]
                ):
                    status, method = "conflict", "override_context_conflict"
                else:
                    chosen, status, method = key, "resolved", "versioned_override"
                    reasons.append(override["reason"])
            if status == "unresolved":
                method = "no_exact_member_match"
        candidates = [
            {
                "bioguide_id": key,
                "matched_by": sorted(matches[key]),
                "context_match": bool(term_matches.get(key)),
                "term_indexes": [term["term_index"] for term in term_matches.get(key, [])],
                **(
                    {"alias_matches": [alias_details[key][index] for index in sorted(alias_details[key])]}
                    if alias_details[key]
                    else {}
                ),
            }
            for key in sorted(matches)
        ]
        geography_terms = []
        uncertain_geography = False
        inclusive = "unique_inclusive_term_end_fallback" in reasons
        if status == "resolved" and chosen is not None:  # every resolution names its member
            # Identity matching may filter by publisher geography. Once the
            # person is known, inspect all terms in the chamber and period so
            # those filters cannot hide a geographic or party change.
            geography_context = {**context, "state": None, "district": None}
            inclusive = "unique_inclusive_term_end_fallback" in reasons
            geography_terms = self.terms_for(chosen, geography_context, inclusive=inclusive)
            if context["windows"] and context["chamber"]:
                uncertain_geography = bool(self.uncertain_geography_terms(chosen, context, inclusive))
                if uncertain_geography:
                    reasons.append("unqualified_geography_term")
        return {
            "bioguide_id": chosen,
            **_resolved_geography(source, context, status, geography_terms, uncertain_geography, inclusive=inclusive),
            "resolution_status": status,
            "resolution_rule": method,
            "candidate_count": str(len(candidates)),
            "candidates_json": _json(candidates),
            "override_version": override["version"] if override and method == "versioned_override" else None,
            "reason": ";".join(reasons) or method,
        }


def _geography_coverage(windows, terms, inclusive=False):
    """Record half-open source windows and their uncovered intervals."""
    result = []
    for left, right in windows:
        covered, gaps = left, []
        for term in sorted(terms, key=lambda row: row.get("term_start") or ""):
            if not term.get("term_start") or not term.get("term_end"):
                continue
            start, end = date.fromisoformat(term["term_start"]), date.fromisoformat(term["term_end"])
            if inclusive and right - left == timedelta(days=1) and left == end:
                end += timedelta(days=1)
            if end <= covered or start >= right:
                continue
            if start > covered:
                gaps.append([covered.isoformat(), min(start, right).isoformat()])
            covered = max(covered, min(end, right))
        if covered < right:
            gaps.append([covered.isoformat(), right.isoformat()])
        result.append({"start": left.isoformat(), "end": right.isoformat(), "uncovered": gaps})
    return result


def _resolved_geography(
    source: Row, context: Mapping, status: str, terms: Sequence[Row], uncertain: bool = False, *, inclusive: bool = False
) -> dict:
    """Expose accepted historical geography and party without changing identity matching or raw facts."""
    result = dict(
        normalized_state=None,
        state_status=status + "_identity",
        normalized_district=None,
        district_status=status + "_identity",
        normalized_party=None,
        party_status=status + "_identity",
    )
    if status != "resolved":
        return result
    if uncertain or not context["windows"] or not context["chamber"] or not terms:
        return result | dict(
            state_status="unqualified_term_context",
            district_status="unqualified_term_context",
            party_status="unqualified_term_context",
        )
    if any(window["uncovered"] for window in _geography_coverage(context["windows"], terms, inclusive)):
        return result | dict(
            state_status="unqualified_term_context",
            district_status="unqualified_term_context",
            party_status="unqualified_term_context",
        )
    states = {term.get("term_state") for term in terms}
    if None in states or "" in states:
        return result | dict(
            state_status="unqualified_term_state",
            district_status="unqualified_term_state",
            party_status="unqualified_term_state",
        )
    if len(states) != 1:
        return result | dict(
            state_status="ambiguous_term_state",
            district_status="ambiguous_term_state",
            party_status="ambiguous_term_state",
        )
    term_state = next(iter(states))
    if context["state"]:
        if context["state"] != term_state:
            return result | dict(
                state_status="unqualified_term_context",
                district_status="unqualified_term_context",
                party_status="unqualified_term_context",
            )
        state_status = "confirmed"
    else:
        state_status = "corrected_from_term" if _text(source.get("state")) else "supplied_from_term"
    result.update(normalized_state=term_state, state_status=state_status)

    parties = {_text(term.get("term_party")) for term in terms}
    if "" in parties:
        result["party_status"] = "unqualified_term_party"
    elif len(parties) != 1:
        result["party_status"] = "ambiguous_term_party"
    else:
        term_party = next(iter(parties))
        raw_party = _text(source.get("party"))
        if not raw_party:
            party_status = "supplied_from_term"
        elif _party(raw_party) == _party(term_party):
            party_status = "confirmed"
        else:
            party_status = "corrected_from_term"
        result.update(normalized_party=term_party, party_status=party_status)

    if context["chamber"] == "senate":
        return result | {"district_status": "not_applicable"}
    districts = {_district(term.get("term_district")) for term in terms}
    if None in districts:
        return result | {"district_status": "unqualified_term_district"}
    if len(districts) != 1:
        return result | {"district_status": "ambiguous_term_district"}
    district = next(iter(districts))
    if _district(source.get("district")) == district:
        district_status = "confirmed"
    else:
        district_status = "corrected_from_term" if source.get("district") else "supplied_from_term"
    return result | dict(normalized_district=district, district_status=district_status)


def _citation(text: str | None, amendment: bool = False) -> tuple[str | None, str, str] | None:
    if not text:
        return None
    natural = re.fullmatch(r"(\d+)-(hamdt|samdt|suamdt|hr|s|hjres|sjres|hconres|sconres|hres|sres)-(\d+)", text, re.I)
    # Exact type-number-Congress grammar from pinned unitedstates/congress
    # utils.py/build_amendment_id. Callers must supply an explicit citation.
    kinds_pattern = "hamdt|samdt|suamdt" if amendment else "hr|s|hjres|sjres|hconres|sconres|hres|sres"
    upstream = re.fullmatch(r"(" + kinds_pattern + r")(\d+)-(\d+)", text, re.I)
    if natural:
        congress, kind, number = natural.groups()
    elif upstream:
        kind, number, congress = upstream.groups()
    else:
        match = re.fullmatch(r"\s*([A-Za-z.\s]+?)\s*(\d+)\s*", text)
        if not match:
            return None
        kind, number = match.groups()
        congress = None
        kind = re.sub(r"[.\s]", "", kind)
    kind = kind.lower()
    kinds = (
        {"hamdt", "samdt", "suamdt"}
        if amendment
        else {"hr", "s", "hjres", "sjres", "hconres", "sconres", "hres", "sres"}
    )
    return (str(int(congress)) if congress else None, kind, str(int(number))) if kind in kinds else None


def _measure_window(text: str) -> tuple[date, date] | None:
    """Read an explicit source period only to constrain edition-context use."""
    try:
        window = _window(text)
        if window:
            return window
        match = re.fullmatch(r"([A-Za-z]+) (\d{1,2}), (\d{4})", _text(text))
        months = (
            "january",
            "february",
            "march",
            "april",
            "may",
            "june",
            "july",
            "august",
            "september",
            "october",
            "november",
            "december",
        )
        if match and match[1].casefold() in months:
            start = date(int(match[3]), months.index(match[1].casefold()) + 1, int(match[2]))
            return start, start + timedelta(days=1)
    except ValueError:
        pass
    return None


def _edition_measure_congress(reference: Row, item: Row, edition: Row) -> tuple[str | None, str | None]:
    """Qualify an otherwise unnumbered measure; never supply a floor-vote context."""
    congress = _congress(edition.get("congress_text"))
    window = _window(congress + " Congress") if congress else None
    if not window:
        return None, None
    if reference.get("kind") in {"roll_call", "roll_call_vote"}:
        return None, "edition_congress_not_used_for_vote_context"
    for source in (reference, item):
        # Even malformed asserted context must not fall through to the edition.
        if _text(source.get("congress_text")):
            return None, None
        if source.get("roll_number_text") or source.get("session_text"):
            return None, "edition_congress_not_used_for_vote_context"
        if source.get("chamber_text") and not _chamber(source["chamber_text"]):
            return None, "edition_congress_not_used_with_unqualified_chamber"
        for field, amendment in (("bill_citation_text", False), ("amendment_citation_text", True)):
            literal = source.get(field)
            parsed = _citation(literal, amendment) if literal else None
            if literal and (not parsed or parsed[0] is not None):
                return None, "edition_congress_not_used_with_stated_identifier_context"
    for occurrence in _array(item.get("references_json"), "references_json"):
        if occurrence.get("congress_text") and _congress(occurrence["congress_text"]) != congress:
            return None, "edition_congress_not_used_with_conflicting_reference_context"
        kind = occurrence.get("kind")
        if kind in {"roll_call", "roll_call_vote"}:
            return None, "edition_congress_not_used_with_sibling_vote_context"
        if kind in {"bill", "bill_citation", "amendment", "amendment_citation"}:
            field = "amendment_citation_text" if kind.startswith("amendment") else "bill_citation_text"
            literal = occurrence.get(field) or occurrence.get("citation_text")
            parsed = _citation(literal, kind.startswith("amendment")) if literal else None
            if not parsed or parsed[0] not in {None, congress}:
                return None, "edition_congress_not_used_with_conflicting_reference_context"
    periods = [
        value
        for source, fields in (
            (reference, ("item_date_text", "period_text", "year_text")),
            (item, ("item_date_text", "period_text", "year_text")),
            (edition, ("timespan_text", "year_text")),
        )
        for field in fields
        if (value := source.get(field))
    ]
    for period in _array(edition.get("periods_json"), "periods_json"):
        if not isinstance(period, dict) or period.get("kind") != "explicit":
            return None, "edition_congress_not_used_with_relative_or_unknown_period"
        if period.get("congress_text") and _congress(period["congress_text"]) != congress:
            return None, "edition_congress_not_used_with_conflicting_period"
        if not period.get("period_text"):
            return None, "edition_congress_not_used_with_relative_or_unknown_period"
        periods.append(period["period_text"])
        if period.get("year_text"):
            periods.append(period["year_text"])
    for text in periods:
        period_window = _measure_window(text)
        if not period_window or not (window[0] <= period_window[0] < period_window[1] <= window[1]):
            return None, "edition_congress_not_used_with_historical_or_unqualified_period"
    return congress, "explicit_edition_congress_for_measure"


class _Items:
    def __init__(self, official: Rows):
        self.bills = {
            key[0]: row for key, row in _index(official["congress_bills"], ("bill_id",), "congress_bills").items()
        }
        self.amendments = {
            key[0]: row for key, row in _index(official["amendments"], ("amendment_id",), "amendments").items()
        }
        self.votes = {
            key[0]: row for key, row in _index(official["roll_call_votes"], ("vote_id",), "roll_call_votes").items()
        }
        self.vote_years = defaultdict(list)
        self.vote_calendar = defaultdict(list)
        for key, vote in self.votes.items():
            canonical = re.fullmatch(r"(\d+)-(house|senate)-(\d+)-(\d+)", key)
            if not canonical or len(canonical[3]) == 4:
                continue
            congress, chamber, session, roll = canonical.groups()
            try:
                day = vote_day(chamber, vote.get("vote_date"))
            except ValueError:
                continue
            if day:
                self.vote_years[congress, chamber, roll, day[:4]].append((vote, session, day))
                self.vote_calendar[day[:4], chamber, roll].append((vote, congress, session, day))

    def resolve(self, reference: Row, item: Row, edition: Row) -> dict:
        reference = dict(reference)
        reference_kind = reference.get("kind")
        if reference_kind in {"bill", "bill_citation", "amendment", "amendment_citation"}:
            field = "amendment_citation_text" if reference_kind.startswith("amendment") else "bill_citation_text"
            if not reference.get(field):
                reference[field] = reference.get("citation_text")
        congress = _congress(reference.get("congress_text"))
        chamber = _chamber(reference.get("chamber_text"))
        session = _number(reference.get("session_text"), r"(?:session)?")
        roll = _number(reference.get("roll_number_text"))
        reasons, conflicts, unknown = [], [], []
        edition_measure_congress = None
        targets, candidates, ambiguous = {}, [], False
        if reference_kind in {"roll_call", "roll_call_vote"}:
            natural = re.fullmatch(r"(\d+)-(house|senate)-(\d+)-(\d+)", reference.get("citation_text") or "")
            upstream = re.fullmatch(r"([hs])(\d+)-(\d+)\.(\d{4})", reference.get("citation_text") or "")
            if natural:
                values = tuple(str(int(value)) if index != 1 else value for index, value in enumerate(natural.groups()))
                if any(old and old != new for old, new in zip((congress, chamber, session, roll), values)):
                    conflicts.append("contradictory_roll_identifiers")
                congress, chamber, session, roll = values
                reference["roll_number_text"] = roll
            elif upstream:
                letter, cited_roll, cited_congress, year = upstream.groups()
                if re.fullmatch(r"\d{4}", item.get("item_date_text") or "") and item["item_date_text"] != year:
                    conflicts.append("contradictory_source_vote_years")
                values = (str(int(cited_congress)), "house" if letter == "h" else "senate", str(int(cited_roll)))
                if any(old and old != new for old, new in zip((congress, chamber, roll), values)):
                    conflicts.append("contradictory_roll_identifiers")
                congress, chamber, roll = values
                reference["roll_number_text"] = roll
                found = self.vote_years[congress, chamber, roll, year]
                candidates.extend(
                    {"vote_id": row["vote_id"], "vote_date": row.get("vote_date"), "vote_day": day}
                    for row, _, day in sorted(found, key=lambda value: value[0]["vote_id"])
                )
                eligible = [entry for entry in found if not session or entry[1] == session]
                if len(eligible) == 1:
                    session = eligible[0][1]
                    reasons.append("upstream_vote_year_matched_official_date_and_session")
                elif len(eligible) > 1:
                    ambiguous = True
                    session = None
                    reasons.append("multiple_official_sessions_for_upstream_vote_year")
                elif found:
                    conflicts.append("source_session_conflicts_with_official_vote_year")
                else:
                    unknown.append("no_official_vote_with_qualified_source_year")
            elif reference.get("citation_text"):
                unknown.append("unsupported_roll_citation_text")
        if session and len(session) == 4:
            # Upstream calendar-year sessions are a different identifier
            # dialect. An unqualified year must never become our ordinal.
            unknown.append("calendar_year_session_not_ordinal")
            session = None
        for field, parsed in (
            ("congress_text", congress),
            ("chamber_text", chamber),
            ("session_text", session),
            ("roll_number_text", roll),
        ):
            if reference.get(field) and not parsed:
                unknown.append(f"unsupported_{field}")
        kind = _name(item.get("item_kind_text")).replace(" ", "_")
        bill_only = kind in {"cosponsorship", "co_sponsorship", "sponsorship", "bill"}
        committee = kind in {"committee_action", "committee_vote"}
        # A literal item year (for example LCV's CSV Year preamble) qualifies
        # the roll against actual official dates. Edition years are not evidence
        # for an individual item, and calendar arithmetic cannot supply a session.
        vote_candidates = {candidate["vote_id"] for candidate in candidates if "vote_id" in candidate}
        item_year = item.get("item_date_text") or ""
        if re.fullmatch(r"\d{4}", item_year) and chamber and roll and not bill_only and not committee:
            found = self.vote_calendar[item_year, chamber, roll]
            for vote, _, _, day in sorted(found, key=lambda value: value[0]["vote_id"]):
                if vote["vote_id"] not in vote_candidates:
                    candidates.append({"vote_id": vote["vote_id"], "vote_date": vote.get("vote_date"), "vote_day": day})
                    vote_candidates.add(vote["vote_id"])
            eligible = [
                entry
                for entry in found
                if (not congress or entry[1] == congress) and (not session or entry[2] == session)
            ]
            if len(eligible) == 1:
                congress, session = eligible[0][1:3]
                reasons.append("item_year_matched_official_date_congress_and_session")
            elif len(eligible) > 1:
                ambiguous = True
                session = None
                reasons.append("multiple_official_votes_for_source_item_year")
            elif found:
                conflicts.append("source_identifiers_conflict_with_official_item_year")
            else:
                unknown.append("no_official_vote_with_source_item_year")
                session = None
        for field, amendment, index, target in (
            ("bill_citation_text", False, self.bills, "bill_id"),
            ("amendment_citation_text", True, self.amendments, "amendment_id"),
        ):
            literal = reference.get(field)
            if not literal:
                continue
            parsed = _citation(literal, amendment)
            if not parsed:
                unknown.append(f"unsupported_{field}")
                continue
            cited_congress, kind, number = parsed
            if congress and cited_congress and congress != cited_congress:
                conflicts.append("contradictory_congress_identifiers")
            resolved_congress = cited_congress or congress
            if not resolved_congress:
                fallback, reason = _edition_measure_congress(reference, item, edition)
                if reason and reason not in reasons:
                    reasons.append(reason)
                if fallback:
                    resolved_congress = edition_measure_congress = fallback
            if not resolved_congress:
                unknown.append(f"missing_source_congress_for_{field}")
                continue
            key = f"{resolved_congress}-{kind}-{number}"
            if key not in index:
                unknown.append(f"unknown_exact_{target}:{key}")
                continue
            targets[target] = key
            candidates.append({target: key})
            congress = congress or cited_congress
        has_roll = bool(reference.get("roll_number_text"))
        if has_roll and bill_only:
            reasons.append("source_item_kind_links_measure_only_roll_not_selected")
        elif has_roll and committee:
            reasons.append("committee_action_not_mapped_to_floor_roll")
        elif has_roll:
            if all((congress, chamber, session, roll)):
                key = f"{congress}-{chamber}-{session}-{roll}"
                vote = self.votes.get(key)
                if vote:
                    targets["vote_id"] = key
                    if key not in vote_candidates:
                        candidates.append({"vote_id": key})
                        vote_candidates.add(key)
                    if targets.get("bill_id") and vote.get("bill_id") and targets["bill_id"] != vote["bill_id"]:
                        conflicts.append("source_bill_conflicts_with_exact_roll_bill")
                    elif vote.get("bill_id") in self.bills:
                        targets["bill_id"] = vote["bill_id"]
                else:
                    unknown.append(f"unknown_exact_vote_id:{key}")
            else:
                unknown.append("incomplete_roll_identifiers")
        if targets.get("amendment_id") and targets.get("bill_id"):
            amended = self.amendments[targets["amendment_id"]].get("amended_bill_id")
            if amended and amended != targets["bill_id"]:
                conflicts.append("source_bill_conflicts_with_exact_amendment_bill")
        if targets.get("amendment_id") and targets.get("vote_id"):
            reasons.append("amendment_roll_relationship_not_validated")
        status = (
            "conflict"
            if conflicts
            else "ambiguous"
            if ambiguous
            else "unresolved"
            if unknown or not targets
            else "resolved"
        )
        rule = (
            "identifier_conflict"
            if conflicts
            else "exact_roll_call"
            if targets.get("vote_id")
            else "exact_amendment"
            if targets.get("amendment_id")
            else "exact_bill"
            if targets.get("bill_id")
            else "no_exact_item_match"
        )
        if edition_measure_congress and not conflicts and (targets.get("bill_id") or targets.get("amendment_id")):
            rule += "_edition_congress"
        if unknown:
            rule += "_incomplete"
        if not has_roll:
            reasons.append("no_roll_selected_from_bill_or_amendment_alone")
        targets, candidates = qualified_item_targets(status, targets, candidates)
        if conflicts:
            congress = chamber = session = roll = None
        output = {
            "congress": None if conflicts else congress or edition_measure_congress,
            "chamber": chamber,
            "session": session if not bill_only and not committee else None,
            "roll_number": roll if not bill_only and not committee else None,
            "vote_id": targets.get("vote_id"),
            "bill_id": targets.get("bill_id"),
            "amendment_id": targets.get("amendment_id"),
            "resolution_status": status,
            "resolution_rule": rule,
            "candidate_count": str(len(candidates)),
            "candidates_json": _json(candidates),
            "reason": ";".join(conflicts + unknown + reasons) or rule,
        }
        return output


def resolve_scorecard_links(
    source_tables: Rows,
    official_tables: Rows,
    input_pins: Mapping[str, Mapping[str, Any]],
    *,
    member_overrides: Sequence[Row] = (),
    rule_version: str = RULE_VERSION,
) -> dict[str, list[dict[str, str | None]]]:
    """Resolve exact links without mutating inputs, files, source identities or ratings.

    Pins are RollupPipeline._prime records keyed by logical table name. Single
    files use sha256; split tables use tableDescriptorDigest over their immutable
    complete descriptors. The caller verifies bytes and descriptors before calling.
    Overrides require
    scorecard_id, publisher_member_key, bioguide_id, version and reason. A known
    identifier conflict cannot be repaired by an override or a name heuristic.
    """
    if not rule_version:
        raise ValueError("A resolver rule_version is required")
    for table in SOURCE_TABLES:
        if table not in source_tables:
            raise ValueError(f"Missing source input {table}")
    for table in OFFICIAL_TABLES:
        if table not in official_tables:
            raise ValueError(f"Missing official input {table}")
    _pins(input_pins)
    editions = _index(source_tables["scorecards"], ("scorecard_id",), "scorecards")
    members = _Members(official_tables["members"], official_tables["member_terms"])
    items = _Items(official_tables)
    overrides = _index(member_overrides, ("scorecard_id", "publisher_member_key"), "member_overrides")
    for override in overrides.values():
        if not all(_text(override.get(field)) for field in ("version", "reason", "bioguide_id")):
            raise ValueError("Member overrides require a version, reason and exact Bioguide")
        if override["bioguide_id"] not in members.members:
            raise ValueError("Member override names a Bioguide absent from the pinned input")
    output = {name: [] for name in LINK_COLUMNS}

    def base(source: Row, name: str) -> tuple[dict, Row]:
        edition = editions.get((source.get("scorecard_id"),))
        if not edition or not edition.get("snapshot_id") or source.get("snapshot_id") != edition["snapshot_id"]:
            raise ValueError("Source fact must match its edition's selected snapshot")
        if not all(_text(source.get(field)) for field in ("capture_id", "source_url", "source_path")):
            raise ValueError("Source fact lacks provenance")
        row = dict.fromkeys(LINK_COLUMNS[name])
        row.update(
            scorecard_id=source["scorecard_id"],
            source_snapshot_id=source["snapshot_id"],
            rule_version=rule_version,
            **{field: source[field] for field in ("capture_id", "source_url", "source_path")},
        )
        return row, edition

    from spicy_regs.scorecards.member_corrections import reviewed_member_context

    for key, source in sorted(
        _index(
            source_tables["scorecard_members"], ("scorecard_id", "publisher_member_key"), "scorecard_members"
        ).items()
    ):
        row, edition = base(source, "scorecard_member_links")
        context_source, correction = reviewed_member_context(source, edition)
        row.update(
            publisher_member_key=source["publisher_member_key"],
            **members.resolve(context_source, edition, overrides.get(key)),
            correction_evidence_json=_json(correction) if correction is not None else None,
        )
        if (
            correction is not None
            and correction.get("status") == "applied"
            and row["normalized_state"] is not None
        ):
            row["state_status"] = "corrected_from_reviewed_evidence"
        output["scorecard_member_links"].append(row)
    for _, source in sorted(
        _index(source_tables["scorecard_items"], ("scorecard_id", "item_id"), "scorecard_items").items()
    ):
        references = _array(source.get("references_json"), "references_json")
        occurrences = []
        direct_fields = (
            "congress_text",
            "chamber_text",
            "session_text",
            "roll_number_text",
            "bill_citation_text",
            "amendment_citation_text",
        )
        if any(source.get(field) for field in direct_fields) or not references:
            occurrences.append(("item:direct", None, source))
        seen = set()
        for reference in references:
            identity = reference.get("occurrence_id")
            if not isinstance(identity, str) or not identity or identity in seen:
                raise ValueError("Item references require distinct nonempty source occurrence IDs")
            seen.add(identity)
            occurrences.append(("reference:" + identity, identity, reference))
        for identity, raw_identity, reference in occurrences:
            row, edition = base(source, "scorecard_item_links")
            row.update(
                item_id=source["item_id"],
                reference_id=identity,
                source_reference_id=raw_identity,
                **items.resolve(reference, source, edition),
            )
            if raw_identity and reference.get("source_path"):
                row["source_path"] = reference["source_path"]
            output["scorecard_item_links"].append(row)
    return output
