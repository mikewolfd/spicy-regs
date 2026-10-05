"""Resolve the Clerk's printed member labels of 1990-2002 to bioguide ids, and fill them on ``member_votes``.

A House roll-call file before 2003 names each member by a label, a party letter and a state
(``Jones (NC)``, ``R``, ``NC``) and by no id, so spicy-docs keys the row ``name:`` plus the label
with a NULL ``bioguide_id``. Owner decision, 2026-10-05: resolve what matches with certainty and
leave the rest unresolved with its reason, never guessed.

**The rule** (:data:`RULE_VERSION`), decided once per distinct (Congress, label, party, state):

1. The label reads as ``Surname[, First][ (ST)]``. The state is the row's; on a delegate's row
   (``XX``) it is the label's qualifier (``Norton (DC)``), or any delegate jurisdiction where the
   label prints none.
2. The candidates are the members with a ``rep`` term in that state during that Congress whose
   surname is the printed one once accents, spaces and punctuation are dropped
   (``Jackson-Lee`` is the crosswalk's ``Jackson Lee``). The surname is the crosswalk's
   ``name_last`` or a dated ``other_names`` surname in force during that term (``Lambert``,
   ``Bono``). Only where no whole surname matches is one part of a compound surname tried
   (``Chenoweth`` for ``Chenoweth-Hage``), and the resolution says so (``surname_part``).
3. The printed party must be one the crosswalk states for that term: ``term_party`` or one of the
   term's dated affiliations, so a member who changed party mid-term matches under both letters.
4. Where several members remain, a printed first name chooses one: equal to the crosswalk's
   ``name_first``, else equal in its first word (``Thomas M.``), else a printed initial that is its
   first letter (``E. B.``). A nickname is not read (``Tom`` is not ``Thomas``).
5. Exactly one member left resolves the key; none or several leave it unresolved.

A resolved key fills a row only where the vote's day lies inside one of that member's matched
terms, and two rows of one roll call that resolve to one member both stay NULL: the Clerk lists a
member once, so one of them is someone else.

**Measured** on the live tables of 2026-10-04 (roll-call-votes ``e053a851``, members ``a1955f7e``;
receipt ``~/Work/corpora/fork-execution-2026-09-21/votes-person-crosswalk-2026-10-05/``). From
2003 the Clerk prints the same labels beside its own ``name-id``, so the rule was run on those
rows with the id hidden: see ``validation.json`` there for the rows, coverage and every
disagreement, and ``coverage.json`` for the 101st-107th by Congress and reason. What that
comparison cannot see is a label form or a term record that only the years before 2003 hold; the
receipt's ``voteview.json`` compares each resolved member's positions with Voteview's record of
that member on that roll call instead.

Each distinct key is resolved once in Python and joined back in DuckDB; no row passes through
Python. The fill is derived, not coalesced: a ``name:`` row's id is whatever this rule gives
today, so a tightened rule or a corrected term withdraws an id as readily as it adds one. The
file is rewritten only when a row changes.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from loguru import logger
from spicy_docs.sources.congress.votes import VoteSourceError, vote_day

from spicy_regs.scorecards.resolution import _alias_bounds, _historical_array, _window
from spicy_regs.sources import r2
from spicy_regs.transforms.table_merge import _duckdb_session, _merge_order, published_table

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence

#: v1: surname, state and party within the Congress, a first name where several remain, the vote
#: day inside the matched term, one row per member per roll call.
RULE_VERSION = "house-name-crosswalk-v1"

#: The published tables the rule reads, all written by ``run-rollup-members`` in one generation.
SOURCES = ("members", "member_terms", "member_party_affiliations")

#: Why a key resolves to no member, and why a row of a resolved key is still left NULL: its day, or a printed date
#: spicy-docs cannot read as one, lies in none of the matched terms, or its roll call resolves two rows to the member.
UNREAD_LABEL, STATE_CONFLICT, NO_MEMBER, PARTY_DIFFERS, SEVERAL_MEMBERS = (
    "unread_label",
    "state_conflict",
    "no_member",
    "party_differs",
    "several_members",
)
OUTSIDE_TERM, LISTED_TWICE = "outside_term", "listed_twice"

#: The prefix spicy-docs' ``member_key`` gives a row whose file states neither a bioguide nor a LIS id.
_NAME_KEY = "name:"
_NAME_ROWS = f"chamber = 'house' AND starts_with(member_key, '{_NAME_KEY}')"
_KEY = ("congress", "member_name", "party", "state")

#: The Clerk's party letter and the crosswalk parties it stands for. The two hyphenated values are
#: the crosswalk's New York fusion labels for Scheuer and Lent in the 102nd, whom the Clerk prints D and R.
_PARTIES = {
    "D": frozenset({"Democrat", "Democrat-Liberal"}),
    "R": frozenset({"Republican", "Republican-Conservative"}),
    "I": frozenset({"Independent"}),
}
#: The Clerk's state for a delegate or the Resident Commissioner, and the jurisdictions that send one.
_DELEGATE = "XX"
_DELEGATE_STATES = frozenset({"AS", "DC", "GU", "MP", "PR", "VI"})

_LABEL = re.compile(r"(?P<surname>[^,(]+)(?:,(?P<first>[^(]+))?(?:\((?P<qualifier>[A-Z]{2})\))?")
_INITIAL = re.compile(r"[A-Z]\.")
#: The first-name rules, strongest first; the first that any candidate meets decides.
_FIRST_NAME_RULES = ("first_name", "first_word", "first_initial")


def _fold(text: str | None) -> str:
    """Letters only, accents dropped, so the Clerk's unaccented, hyphenated print meets the crosswalk's."""
    return "".join(ch for ch in unicodedata.normalize("NFKD", text or "") if ch.isalpha())


def _first_name_rule(printed: str, first: str) -> str | None:
    """How a printed first name agrees with the crosswalk's: whole, in its first word, or as a printed initial."""
    if _fold(printed) == _fold(first):
        return "first_name"
    printed_words, words = printed.split(), first.split()
    if not printed_words or not words:
        return None
    if _INITIAL.fullmatch(printed_words[0]):
        return "first_initial" if _fold(words[0]).startswith(_fold(printed_words[0])) else None
    return "first_word" if _fold(printed_words[0]) == _fold(words[0]) else None


def _house_terms(
    members: Iterable[Mapping[str, Any]], terms: Iterable[Mapping[str, Any]], affiliations: Iterable[Mapping[str, Any]]
) -> tuple[dict[str, list[dict]], dict[str, list[dict]]]:
    """Every dated ``rep`` term under its member's folded surname, and under each space- or hyphen-separated part of it.

    A term appears once per surname its member has carried: ``name_last`` for all of it, and a
    dated ``other_names`` surname for the days that patch bounds (scorecards' ``_alias_bounds``;
    an undated patch bounds nothing and is not read). O(members + terms).
    """
    surnames: dict[str, tuple[str, list[tuple[str, date, date]]]] = {}
    for member in members:
        carried = [(member["name_last"] or "", date.min, date.max)]
        for patch in _historical_array(member["other_names_json"], "other_names_json"):
            bounds = _alias_bounds(patch) if isinstance(patch, dict) and patch.get("last") else None
            if bounds:
                carried.append((patch["last"], *bounds))
        surnames[member["bioguide_id"]] = (member["name_first"] or "", carried)
    parties: dict[tuple[str, str], set[str]] = defaultdict(set)
    for affiliation in affiliations:
        parties[affiliation["bioguide_id"], affiliation["term_index"]].add(affiliation["party"])
    whole: dict[str, list[dict]] = defaultdict(list)
    parts: dict[str, list[dict]] = defaultdict(list)
    for term in terms:
        if term["term_type"] != "rep" or not term["term_start"] or not term["term_end"]:
            continue
        bioguide = term["bioguide_id"]
        first, carried = surnames.get(bioguide, ("", []))
        for surname, since, until in carried:
            entry = {
                "bioguide_id": bioguide,
                "first": first,
                "term_index": term["term_index"],
                "start": date.fromisoformat(term["term_start"]),
                "end": date.fromisoformat(term["term_end"]),
                "state": term["term_state"],
                "parties": parties[bioguide, term["term_index"]] | {term["term_party"]},
                "since": since,
                "until": until,
            }
            whole[_fold(surname)].append(entry)
            for piece in {_fold(piece) for piece in re.split(r"[\s-]+", surname)} - {""}:
                parts[piece].append(entry)
    return whole, parts


def _resolve(key: tuple[str, str, str, str], whole: Mapping[str, list[dict]], parts: Mapping[str, list[dict]]) -> dict:
    """One key's resolution: the member and matched terms, or the reason and the members it stopped at."""

    def unresolved(reason: str, candidates: Iterable[dict] = ()) -> dict:
        return {"bioguide_id": None, "rule": None, "reason": reason, "terms": [],
                "candidates": sorted({term["bioguide_id"] for term in candidates})}

    congress, label, party, state = key
    read = _LABEL.fullmatch(" ".join(label.split()))
    span = _window(f"{congress} Congress")
    if read is None or span is None:
        return unresolved(UNREAD_LABEL)
    convened, adjourned = span
    surname, printed_first, qualifier = read["surname"], (read["first"] or "").strip(), read["qualifier"]
    if state == _DELEGATE:
        states = {qualifier} if qualifier else _DELEGATE_STATES
    elif qualifier and qualifier != state:
        return unresolved(STATE_CONFLICT)
    else:
        states = {state}

    def seated(terms: Iterable[dict]) -> list[dict]:
        # In that state, during that Congress, and while the member carried this surname.
        return [
            term for term in terms
            if term["state"] in states
            and max(term["start"], term["since"], convened) < min(term["end"], term["until"], adjourned)
        ]

    rule, pool = "surname", seated(whole.get(_fold(surname), ()))
    if not pool:
        rule, pool = "surname_part", seated(parts.get(_fold(surname), ()))
    if not pool:
        return unresolved(NO_MEMBER)
    of_party = [term for term in pool if term["parties"] & _PARTIES.get(party, frozenset())]
    if not of_party:
        return unresolved(PARTY_DIFFERS, pool)
    chosen = {term["bioguide_id"] for term in of_party}
    if len(chosen) > 1 and printed_first:
        agreed = {term["bioguide_id"]: _first_name_rule(printed_first, term["first"]) for term in of_party}
        for first_rule in _FIRST_NAME_RULES:
            named = {bioguide for bioguide, how in agreed.items() if how == first_rule}
            if named:
                if len(named) == 1:
                    rule, chosen = f"{rule}+{first_rule}", named
                break
    if len(chosen) > 1:
        return unresolved(SEVERAL_MEMBERS, of_party)
    [bioguide] = chosen
    matched = sorted(
        {(int(term["term_index"]), term["start"], term["end"]) for term in of_party if term["bioguide_id"] == bioguide}
    )
    return {
        "bioguide_id": bioguide, "rule": rule, "reason": None, "candidates": [bioguide],
        "terms": [{"term_index": str(index), "term_start": start.isoformat(), "term_end": end.isoformat()}
                  for index, start, end in matched],
    }


def resolve_name_keys(
    keys: Iterable[tuple[str, str, str, str]],
    members: Iterable[Mapping[str, Any]],
    terms: Iterable[Mapping[str, Any]],
    affiliations: Iterable[Mapping[str, Any]] = (),
) -> dict[tuple[str, str, str, str], dict]:
    """Each distinct (congress, member_name, party, state) and its resolution under :data:`RULE_VERSION`.

    A resolution carries ``bioguide_id``, ``rule`` and the matched ``terms``, or NULL with its
    ``reason`` and the ``candidates`` it stopped at. ``members``, ``terms`` and ``affiliations``
    are the published tables' rows. O(members + terms + keys).
    """
    whole, parts = _house_terms(members, terms, affiliations)
    return {key: _resolve(key, whole, parts) for key in keys}


def _digest(path: Path) -> str:
    with path.open("rb") as stream:
        return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()


def fill_house_name_bioguide_ids(
    output_dir: Path,
    out_file: Path,
    download_prior: Callable[[str, Path], bool] = r2.download,
    *,
    evidence: CaptureEvidence | None = None,
) -> int:
    """Set ``bioguide_id`` on every House ``name:`` row of a merged ``member_votes`` from the published crosswalk.

    Held rows included: each run decides every ``name:`` row again and journals the rule version,
    the inputs, every key left unresolved with its reason, and each resolution that changed a row
    with its rule and matched terms (``member-name-crosswalk``). Best-effort like the Senate fill:
    with any of :data:`SOURCES` unpublished or unreadable the file stays as merged. Returns how
    many ``name:`` rows carry an id.
    """
    import duckdb
    from spicy_docs.schemas import TABLE_CONTRACTS
    from spicy_docs.transport.credentials import scrub_credential

    contract = TABLE_CONTRACTS["member_votes"]
    filled_file = output_dir / f"_{contract.name}_name_filled.parquet"
    sources: dict[str, Path | None] = {}
    con = None
    try:
        con = _duckdb_session(output_dir)

        def read(sql: str) -> list[dict]:
            cursor = con.execute(sql)
            names = [column[0] for column in cursor.description]
            return [dict(zip(names, row)) for row in cursor.fetchall()]

        keys = con.execute(
            f"SELECT DISTINCT {', '.join(_KEY)} FROM read_parquet('{out_file}') WHERE {_NAME_ROWS}"
        ).fetchall()
        if not keys:
            return 0
        sources = {name: published_table(output_dir, name, download_prior) for name in SOURCES}
        if missing := sorted(name for name, path in sources.items() if path is None):
            logger.info("{}: no published {} — name: rows left as merged", contract.name, ", ".join(missing))
            if evidence is not None:
                evidence.event("member-name-crosswalk", rule_version=RULE_VERSION, input_available=False, missing=missing)
            return 0
        resolutions = resolve_name_keys(
            keys,
            read(f"SELECT bioguide_id, name_first, name_last, other_names_json FROM read_parquet('{sources['members']}')"),
            read("SELECT bioguide_id, term_index, term_type, term_start, term_end, term_state, term_party "
                 f"FROM read_parquet('{sources['member_terms']}')"),
            read(f"SELECT bioguide_id, term_index, party FROM read_parquet('{sources['member_party_affiliations']}')"),
        )
        # Each printed day is read once, by spicy-docs' rule; one it refuses covers no term.
        days = []
        for (literal,) in con.execute(
            f"SELECT DISTINCT vote_date FROM read_parquet('{out_file}') WHERE {_NAME_ROWS}"
        ).fetchall():
            try:
                iso = vote_day("house", literal)
            except VoteSourceError:
                iso = None
            if iso is not None:
                days.append({"vote_date": literal, "day": date.fromisoformat(iso)})
        text = pa.string()
        con.register("days", pa.Table.from_pylist(days, schema=pa.schema([("vote_date", text), ("day", pa.date32())])))
        con.register("name_terms", pa.Table.from_pylist(
            [dict(zip(_KEY, key), bioguide_id=resolution["bioguide_id"], term_start=date.fromisoformat(term["term_start"]),
                  term_end=date.fromisoformat(term["term_end"]))
             for key, resolution in resolutions.items() for term in resolution["terms"]],
            schema=pa.schema([*((column, text) for column in _KEY), ("bioguide_id", text),
                              ("term_start", pa.date32()), ("term_end", pa.date32())]),
        ))
        # ``found`` is the key's member on a day one of the matched terms covers; ``listed`` counts the
        # roll call's rows that found the same member, and only a member found once is filled.
        con.execute(
            f"""
            CREATE TEMP TABLE name_fill AS
            WITH named AS (
                SELECT vote_id, member_key, {", ".join(_KEY)}, vote_date, bioguide_id AS held
                FROM read_parquet('{out_file}') WHERE {_NAME_ROWS}
            ),
            serving AS (
                SELECT DISTINCT {", ".join(f"t.{column}" for column in _KEY)}, d.vote_date, t.bioguide_id
                FROM name_terms t JOIN days d ON d.day BETWEEN t.term_start AND t.term_end
            ),
            found AS (
                SELECT n.*, s.bioguide_id AS found, count(*) OVER (PARTITION BY n.vote_id, s.bioguide_id) AS listed
                FROM named n LEFT JOIN serving s USING ({", ".join(_KEY)}, vote_date)
            )
            SELECT *, CASE WHEN listed = 1 THEN found END AS resolved FROM found
            """
        )
        outcomes = {
            tuple(row[: len(_KEY)]): row[len(_KEY):]
            for row in con.execute(
                f"""
                SELECT {", ".join(_KEY)}, count(*), count(resolved),
                       count(*) FILTER (WHERE found IS NULL),
                       count(*) FILTER (WHERE resolved IS DISTINCT FROM held)
                FROM name_fill GROUP BY ALL
                """
            ).fetchall()
        }
        changed = sum(outcome[3] for outcome in outcomes.values())
        if changed:
            con.execute(
                f"""
                COPY (
                    SELECT t.* REPLACE (CASE WHEN f.member_key IS NULL THEN t.bioguide_id ELSE f.resolved END AS bioguide_id)
                    FROM read_parquet('{out_file}') t
                    LEFT JOIN name_fill f ON f.vote_id = t.vote_id AND f.member_key = t.member_key
                    ORDER BY {_merge_order(contract.version_column, contract.identity)}
                ) TO '{filled_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
                """
            )
            filled_file.replace(out_file)
        digests = {name: _digest(path) for name, path in sources.items() if path is not None}
    except (duckdb.Error, OSError) as error:
        logger.warning(
            "{}: the name crosswalk failed — name: rows left as merged: {}", contract.name,
            scrub_credential(str(error), ""),
        )
        filled_file.unlink(missing_ok=True)
        return 0
    finally:
        if con is not None:
            con.close()
        for path in sources.values():
            if path is not None:
                path.unlink(missing_ok=True)

    resolved_keys, unresolved = [], []
    for key, resolution in sorted(resolutions.items()):
        rows, filled, outside, moved = outcomes[key]
        named = dict(zip(_KEY, key))
        if resolution["bioguide_id"] is None:
            unresolved.append({**named, "reason": resolution["reason"], "candidates": resolution["candidates"],
                               "rows": rows})
            continue
        # A resolved key's NULL rows: a day outside the matched terms, else a member the roll call lists twice.
        for reason, count in ((OUTSIDE_TERM, outside), (LISTED_TWICE, rows - filled - outside)):
            if count:
                unresolved.append({**named, "reason": reason, "candidates": resolution["candidates"], "rows": count})
        if moved:
            resolved_keys.append({**named, "bioguide_id": resolution["bioguide_id"], "rule": resolution["rule"],
                                  "terms": resolution["terms"], "rows": filled, "rows_changed": moved})
    filled = sum(outcome[1] for outcome in outcomes.values())
    total = sum(outcome[0] for outcome in outcomes.values())
    logger.info(
        "{}: {:,} of {:,} name: rows carry a bioguide_id under {} ({:,} changed this run, {:,} keys unresolved)",
        contract.name, filled, total, RULE_VERSION, changed, sum(1 for r in resolutions.values() if r["bioguide_id"] is None),
    )
    if evidence is not None:
        evidence.event(
            "member-name-crosswalk", rule_version=RULE_VERSION, input_available=True,
            inputs={name: {"sha256": digest, "generation": evidence.published_input(f"{name}.parquet", sha256=digest)}
                    for name, digest in digests.items()},
            keys=len(resolutions), rows=total, rows_resolved=filled, rows_changed=changed,
            resolved=resolved_keys, unchanged=sum(1 for r in resolutions.values() if r["bioguide_id"]) - len(resolved_keys),
            unresolved=unresolved,
        )
    return filled
