"""Audit unresolved source names against retained originals; never update links."""

from __future__ import annotations

from collections import Counter
import csv
import io
import json
from pathlib import Path
import re
import unicodedata
from typing import Any

from bs4 import BeautifulSoup
import pyarrow.parquet as pq

from qualify_lcv import digest, write
from spicy_regs.scorecards.resolution import _STATES, _name

BASE = Path("/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts")
LCV = BASE / "scorecards-lcv-2026-10-03"
SAMPLES = BASE / "scorecards-2026-10-03-samples"
MEMBERS = BASE / "scorecards-members-2026-10-03"
OUT = Path(__file__).parent

PROPOSALS = [
    {
        "proposal": "Preserve primary name.middle, name.nickname and name.official_full from the already retained congressional identity source; index exact source-stated forms with historical term context and collision refusal.",
        "observed_group_count": 31,
        "limit": "A proposal, not an applied change. Names and raw observations must remain source-faithful; never generate common nickname dictionaries.",
    },
    {
        "proposal": "Consider explicit source-specific parsing of parenthetical name forms for Jim Moylan and Zach Nunn, with literal source fields retained.",
        "observed_group_count": 2,
        "limit": "Do not strip arbitrary parenthetical text or infer unrelated aliases.",
    },
    {
        "proposal": "Evaluate a versioned diacritic-folded exact comparison over the full pinned name corpus, requiring unique historical context and preserving the original accented values.",
        "observed_group_count": 7,
        "limit": "Measure candidate collisions before admission. This diagnostic group does not authorize automatic matching.",
    },
    {
        "proposal": "Parse a state-prefixed district only when the prefix exactly agrees with the independently stated source state; preserve the literal district.",
        "limit": "43 unresolved rows carry this diagnostic, but name or term failures coexist. No claim that parsing districts alone resolves 43 people.",
    },
    {
        "proposal": "Keep the four 2026-only members unresolved for the 2025 edition; investigate publisher roster-versus-rating period semantics before changing the temporal rule.",
        "observed_group_count": 4,
        "limit": "Do not assign a 2025 historical term to a later member.",
    },
    {
        "proposal": "For remaining name-component, unstated-name-variant and Ogles discrepancies, obtain explicit identifier or publisher-backed alias evidence before considering a versioned override.",
        "observed_group_count": 18,
        "limit": "A shared district or plausible nickname is not sufficient identity evidence.",
    },
]


def fold(value):
    return _name("".join(ch for ch in unicodedata.normalize("NFKD", value) if not unicodedata.combining(ch)))


def in_year(term):
    return term["term_start"] < "2026-01-01" and term["term_end"] > "2025-01-01"


def audit():
    inputs = json.loads((LCV / "official-inputs.json").read_bytes())
    links = json.loads((LCV / "integration-run-1/unresolved.private.json").read_bytes())
    member_rows = pq.ParquetFile(inputs["paths"]["members"][0]).read().to_pylist()
    member_index = {r["bioguide_id"]: r for r in member_rows}
    terms = pq.ParquetFile(inputs["paths"]["member_terms"][0]).read().to_pylist()
    raw_members = {}
    raw_sources = []
    for journal in MEMBERS.glob("source-evidence/*/artifact/journal.jsonl"):
        for line in journal.read_bytes().splitlines():
            event = json.loads(line)
            if event.get("event") != "capture" or any(r["url"] == event["requested_url"] for r in raw_sources):
                continue
            path = journal.parent / "blobs" / "sha256" / event["sha256"].removeprefix("sha256:")
            assert digest(path) == event["sha256"]
            raw_sources.append({"url": event["requested_url"], "sha256": event["sha256"], "path": str(path)})
            for index, record in enumerate(json.loads(path.read_bytes())):
                raw_members[record["id"]["bioguide"]] = (
                    record,
                    {"sha256": event["sha256"], "url": event["requested_url"], "json_pointer": f"/{index}"},
                )
    csv_path = SAMPLES / "lcv-2025-senate-csv.body"
    html_path = SAMPLES / "lcv-members-exact-html.body"
    original: dict[str, dict[str, Any]] = {}
    chamber = state = None
    for row_number, row in enumerate(csv.reader(io.StringIO(csv_path.read_bytes().decode("utf-8-sig"))), 1):
        if len(row) == 1:
            if row[0] in ("House", "Senate"):
                chamber = row[0]
            elif row[0]:
                state = row[0]
        if len(row) == 7 and row[-1].startswith("https://www.lcv.org/moc/"):
            original[row[-1]] = {
                "row_number": row_number,
                "first": row[0],
                "last": row[1],
                "party": row[2],
                "district": row[3],
                "annual": row[4],
                "lifetime": row[5],
                "url": row[6],
                "chamber": chamber,
                "state": state,
            }
    html = BeautifulSoup(html_path.read_bytes(), "html.parser")
    audited = []
    for link in links:
        source = json.loads(link["source_context_json"])
        raw = original[source["publisher_member_id"]]
        assert source["member_name"] == raw["first"] + " " + raw["last"]
        assert (
            source["state"] == raw["state"]
            and source["district"] == raw["district"]
            and source["chamber_text"] == raw["chamber"]
        )
        anchor = html.select_one(f'a.card-link[href="{raw["url"]}"]')
        assert anchor is not None
        card = anchor.find_parent(class_="congress-item")
        assert card is not None
        html_name = card.select_one(".congress-name")
        assert html_name is not None and _name(html_name.get_text(strip=True)) == _name(source["member_name"])
        state_code = _STATES[raw["state"].casefold()]
        district = raw["district"].split("-")[-1]
        district = "0" if district == "AL" else str(int(district)) if district.isdecimal() else None
        existing = json.loads(link["candidates_json"])
        if len(existing) == 1:
            ids = {existing[0]["bioguide_id"]}
            basis = "Existing exact-name candidate retained by resolver; historical term checked independently."
        else:
            ch = "sen" if raw["chamber"] == "Senate" else "rep"
            ids = {
                t["bioguide_id"]
                for t in terms
                if in_year(t)
                and t["term_type"] == ch
                and t["term_state"] == state_code
                and (ch == "sen" or t["term_district"] == district)
            }
            if len(ids) > 1:
                ids = {
                    key
                    for key in ids
                    if set(fold(raw["last"]).split()) <= set(fold(member_index[key]["name_last"]).split())
                }
            if len(ids) > 1:
                ids = {
                    key
                    for key in ids
                    if fold(source["member_name"])
                    == fold(member_index[key]["name_first"] + " " + member_index[key]["name_last"])
                }
            basis = "Diagnostic candidate from retained state/chamber/2025 term and explicit district, with surname filtering where needed; not an admitted identity link."
        assert len(ids) == 1, (source["member_name"], ids)
        key = next(iter(ids))
        candidate = member_index[key]
        upstream, locator = raw_members[key]
        name = upstream["name"]
        relevant = [t for t in terms if t["bioguide_id"] == key and in_year(t)]
        latest = [t for t in terms if t["bioguide_id"] == key]
        primary = candidate["name_first"] + " " + candidate["name_last"]
        forms = {"official_full": name.get("official_full", "")}
        if name.get("nickname"):
            forms["nickname + last"] = name["nickname"] + " " + name["last"]
        explicit = {field: value for field, value in forms.items() if _name(value) == _name(source["member_name"])}
        parenthetical = re.search(r"\(([^()]+)\)", name["first"])
        if not relevant:
            category = "term_outside_2025"
            reason = f"Exact-name candidate's first retained term starts {min(t['term_start'] for t in latest)}; no retained term overlaps 2025."
        elif fold(source["member_name"]) == fold(primary):
            category = "accent_difference"
            reason = f"LCV {source['member_name']!r} differs from retained primary name {primary!r} by diacritics; whitespace is already normalized."
        elif explicit:
            category = "explicit_retained_name_omitted"
            reason = (
                "LCV matches retained upstream "
                + ", ".join(f"{field}={value!r}" for field, value in explicit.items())
                + f"; current member index only uses primary first/last {primary!r}."
            )
        elif parenthetical and _name(parenthetical[1] + " " + name["last"]) == _name(source["member_name"]):
            category = "explicit_parenthetical_name"
            reason = f"Retained first name {name['first']!r} explicitly contains the LCV given name in parentheses; resolver does not index that form."
        elif source["publisher_member_key"] in {
            "amata-coleman-radewagen",
            "french-hill",
            "morgan-griffith",
            "pablo-jose-hernandez",
            "james-c-justice",
        }:
            category = "name_component_difference"
            reason = f"LCV name {source['member_name']!r} uses different or fewer name components than primary {primary!r}; retained full name is {name.get('official_full')!r}. No complete exact alias is currently indexed."
        elif source["publisher_member_key"] == "william-ogles":
            category = "source_name_discrepancy"
            reason = "LCV CSV and HTML both say William Ogles; the retained TN-05 candidate is Andrew Ogles, with no William alias in retained name fields. District occupancy does not prove identity."
        else:
            category = "name_variant_not_explicit_in_retained_fields"
            reason = f"LCV given name differs from primary {primary!r}; no complete matching official_full or nickname-plus-last form occurs in the retained upstream name fields. Candidate needs additional exact alias evidence."
        audited.append(
            {
                "publisher_member_key": source["publisher_member_key"],
                "source_name": source["member_name"],
                "category": category,
                "reason": reason,
                "source": raw
                | {
                    "csv_sha256": digest(csv_path),
                    "html_sha256": digest(html_path),
                    "html_name": html_name.get_text(strip=True),
                    "html_locator": f'a.card-link[href="{raw["url"]}"] / parent .congress-item / .congress-name',
                },
                "candidate": {
                    "bioguide_id": key,
                    "primary_indexed_name": primary,
                    "retained_name_fields": name,
                    "selection_basis": basis,
                    "upstream_locator": locator,
                    "terms_overlapping_2025": [
                        {
                            field: t[field]
                            for field in ("term_start", "term_end", "term_state", "term_type", "term_district")
                        }
                        for t in relevant
                    ],
                    "first_term_start": min(t["term_start"] for t in latest),
                },
                "resolver_status": link["resolution_status"],
                "resolver_candidate_count": link["candidate_count"],
                "resolver_reason": link["reason"],
                "diagnostic_candidate_only": True,
                "source_link_unchanged": True,
            }
        )
    assert len(audited) == len(links) == 62
    counts = dict(Counter(r["category"] for r in audited))
    report = {
        "status": "completed_read_only_audit",
        "scope": "All LCV2025 member rows unresolved under scorecard-resolution-v1.3; existing source and analysis artifacts unchanged.",
        "counts": counts,
        "total": len(audited),
        "source_member_csv": {"path": str(csv_path), "sha256": digest(csv_path)},
        "source_member_html": {"path": str(html_path), "sha256": digest(html_path)},
        "retained_member_json_sources": raw_sources,
        "input_pins": {name: inputs["input_pins"][name] for name in ("members", "member_terms")},
        "unresolved_artifact": {
            "path": str(LCV / "integration-run-1/unresolved.private.json"),
            "sha256": digest(LCV / "integration-run-1/unresolved.private.json"),
        },
        "notes": [
            "Raw congressional identity records are the retained unitedstates/congress-legislators community source, not a new federal agency acquisition.",
            "Every source name and context was reread from original CSV and corroborated against original HTML. Candidate name fields were read from original JSON and compared with pinned Parquet.",
            "District/surname-based diagnostic candidates are evidence for explaining a failed exact match, not verified identity links. No fuzzy matcher, override or resolver change was applied.",
            "No row is classified as an ambiguous exact match: existing resolver candidate_count is zero for 58 and one rejected historical candidate for four. Absence from the exact-name index does not mean the person is absent from congressional data.",
        ],
        "members": audited,
        "proposed_improvements": PROPOSALS,
    }
    write(OUT / "unresolved_members.json", report)
    print(json.dumps(counts, indent=2))


if __name__ == "__main__":
    audit()
