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
   ``name_last`` (``surname``) or a dated ``other_names`` surname in force during that term
   (``former_surname``: ``Lambert``, ``Bono``). Only where no whole surname matches is one part
   of a compound surname tried (``surname_part``: ``Chenoweth`` for ``Chenoweth-Hage``).
3. The printed party must be one the crosswalk states for that term: ``term_party`` or one of the
   term's dated affiliations, so a member who changed party mid-term matches under both letters.
4. Where several members remain, a printed first name chooses one if it can: equal to the
   crosswalk's ``name_first``, else equal in its first word (``Thomas M.``), else a printed
   initial that is its first letter (``E. B.``), else equal to the nickname the crosswalk states
   for the member (``Tom`` for Thomas Davis, ``Denny`` for Dennis Smith). Only a stated nickname
   is read, and only where ``members`` carries the column (:data:`_NICKNAME`); without it this
   step is as it was before the nickname existed.
5. No member left leaves the key unresolved, with its reason.

Then each row is decided on its vote's day, in two steps, the second asked only what the first
cannot answer:

- **The seating day.** One member left: the row is that member's on a day inside a matched term.
  Several left (``+seating_day``; owner decision, 2026-10-05): the row is the one of them a
  matched term seats that day, which separates a successor of the same surname, state and party
  from the member they followed (the 105th's Capps and Bono, the 107th's Shuster). This also requires a dated former surname to be in force on that day. It comes
  first because it reads nothing but the terms: a member not seated is on no roll call, whatever
  the Clerk printed.
- **The remaining member** (``+remaining_member``). A label that prints no first name, on a day
  that seats several of its members: where the same roll call lists all but one of them under a
  label that does print a first name, the plain label is the one left. The Clerk prints a first
  name only to tell one of them apart (the 101st's ``Smith, Denny (OR)`` beside ``Smith (OR)``),
  and lists a member once. It is read from the row's own roll call, never from the Congress at
  large: ``Miller (FL)`` was Dan Miller's label until Jeff Miller was seated in 2001, after which
  the Clerk printed both first names, and no roll call lists it beside either.

A day that seats none of a key's members stays NULL (``outside_term``), as does one that seats
several the roll call does not tell apart (``several_seated``), and so do two rows of one roll
call that resolve to one member (``listed_twice``).

**Historical v3 measurements** on the live tables of 2026-10-04 (roll-call-votes ``e053a851``, members ``a1955f7e``;
receipt ``~/Work/corpora/fork-execution-2026-09-21/votes-person-crosswalk-2026-10-05/``). From
2003 the Clerk prints the same labels beside its own ``name-id``, so the rule was run on those
rows with the id hidden: see ``validation.json`` there for the rows, coverage and every
disagreement, ``coverage.json`` for the 101st-107th by Congress and reason, and ``v1-to-v2.json``
and ``v2-to-v3.json`` for what each later step added. What that comparison cannot see is a label
form or a term record that only the years before 2003 hold; the receipt's ``voteview.json``
compares each resolved member's positions with Voteview's record of that member on that roll
call instead. No label of 2003 on needs the remaining-member step, so ``validation.json`` also
strips the first name from one label of each first-named pair in turn and checks the step
returns the Clerk's id for it; that shows the step finds the member left over, and the one
label of 1990-2002 it decides (``Smith (OR)``) rests on Voteview's positions. ``members`` did not
publish the nickname when this was measured: the receipt's ``nickname-input/`` appends the
source's own ``name.nickname`` to the published table, and without the column the fill is v2's
row for row.

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

#: v4 preserves dated surname bounds on each vote and refuses selected input read failures.
#: v3 (owner decision, 2026-10-05): a printed first name also agrees with the nickname the crosswalk states, and a
#: plain label seating several takes the member its roll call's first-named labels leave over. v2: where several
#: members still answer to a key, a row takes the one seated on its vote's day, and a resolution names a former
#: surname as such. v1: surname, state and party within the Congress, a first name where several remain, the vote
#: day inside the matched term, one row per member per roll call. Neither earlier version was published.
RULE_VERSION = "house-name-crosswalk-v4"

#: The published tables the rule reads, all written by ``run-rollup-members`` in one generation.
SOURCES = ("members", "member_terms", "member_party_affiliations")

#: Why a key answers to no member, and why a row of a key that does is still left NULL: its day (or a printed date
#: spicy-docs cannot read as one) lies in none of the matched terms, lies in the terms of two of the key's members,
#: or its roll call resolves two rows to the one member.
UNREAD_LABEL, STATE_CONFLICT, NO_MEMBER, PARTY_DIFFERS = "unread_label", "state_conflict", "no_member", "party_differs"
OUTSIDE_TERM, SEVERAL_SEATED, LISTED_TWICE = "outside_term", "several_seated", "listed_twice"
#: The step that gave a row its member where its key answers to several: appended to that member's rule.
SEATING_DAY, REMAINING_MEMBER = "seating_day", "remaining_member"

#: The prefix spicy-docs' ``member_key`` gives a row whose file states neither a bioguide nor a LIS id.
_NAME_KEY = "name:"
_NAME_ROWS = f"chamber = 'house' AND starts_with(member_key, '{_NAME_KEY}')"
_KEY = ("congress", "member_name", "party", "state")
#: The ``members`` column for the source's ``name.nickname``, read where the published table has it.
_NICKNAME = "name_nickname"

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
_FIRST_NAME_RULES = ("first_name", "first_word", "first_initial", "nickname")


def _fold(text: str | None) -> str:
    """Letters only, accents dropped, so the Clerk's unaccented, hyphenated print meets the crosswalk's."""
    return "".join(ch for ch in unicodedata.normalize("NFKD", text or "") if ch.isalpha())


def _first_name_rule(printed: str, first: str, nickname: str | None) -> str | None:
    """How a printed first name agrees with the crosswalk's: whole, in its first word, as a printed initial, or as
    the member's stated nickname."""
    if _fold(printed) == _fold(first):
        return "first_name"
    printed_words, words = printed.split(), first.split()
    if printed_words and words:
        if _INITIAL.fullmatch(printed_words[0]):
            if _fold(words[0]).startswith(_fold(printed_words[0])):
                return "first_initial"
        elif _fold(printed_words[0]) == _fold(words[0]):
            return "first_word"
    return "nickname" if _fold(nickname) and _fold(printed) == _fold(nickname) else None


def _house_terms(
    members: Iterable[Mapping[str, Any]], terms: Iterable[Mapping[str, Any]], affiliations: Iterable[Mapping[str, Any]]
) -> tuple[dict[str, list[dict]], dict[str, list[dict]]]:
    """Every dated ``rep`` term under its member's folded surname, and under each space- or hyphen-separated part of it.

    A term appears once per surname its member has carried: ``name_last`` for all of it, and a
    dated ``other_names`` surname for the days that patch bounds (scorecards' ``_alias_bounds``;
    an undated patch bounds nothing and is not read). O(members + terms).
    """
    surnames: dict[str, tuple[str, str | None, list[tuple[str, date, date]]]] = {}
    for member in members:
        carried = [(member["name_last"] or "", date.min, date.max)]
        for patch in _historical_array(member["other_names_json"], "other_names_json"):
            bounds = _alias_bounds(patch) if isinstance(patch, dict) and patch.get("last") else None
            if bounds:
                carried.append((patch["last"], *bounds))
        surnames[member["bioguide_id"]] = (member["name_first"] or "", member.get(_NICKNAME), carried)
    parties: dict[tuple[str, str], set[str]] = defaultdict(set)
    for affiliation in affiliations:
        parties[affiliation["bioguide_id"], affiliation["term_index"]].add(affiliation["party"])
    whole: dict[str, list[dict]] = defaultdict(list)
    parts: dict[str, list[dict]] = defaultdict(list)
    for term in terms:
        if term["term_type"] != "rep" or not term["term_start"] or not term["term_end"]:
            continue
        bioguide = term["bioguide_id"]
        first, nickname, carried = surnames.get(bioguide, ("", None, []))
        for surname, since, until in carried:
            entry = {
                "bioguide_id": bioguide,
                "first": first,
                "nickname": nickname,
                "term_index": term["term_index"],
                "start": date.fromisoformat(term["term_start"]),
                "end": date.fromisoformat(term["term_end"]),
                "state": term["term_state"],
                "parties": parties[bioguide, term["term_index"]] | {term["term_party"]},
                "since": since,
                "until": until,
                "former": (since, until) != (date.min, date.max),
            }
            whole[_fold(surname)].append(entry)
            for piece in {_fold(piece) for piece in re.split(r"[\s-]+", surname)} - {""}:
                parts[piece].append(entry)
    return whole, parts


def _resolve(key: tuple[str, str, str, str], whole: Mapping[str, list[dict]], parts: Mapping[str, list[dict]]) -> dict:
    """One key's resolution: each member it answers to with their rule and matched terms, or why it answers to none."""

    def unresolved(reason: str, candidates: Iterable[dict] = ()) -> dict:
        return {"reason": reason, "candidates": sorted({term["bioguide_id"] for term in candidates}), "members": {},
                "first_named": False}

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

    part, pool = False, seated(whole.get(_fold(surname), ()))
    if not pool:
        part, pool = True, seated(parts.get(_fold(surname), ()))
    if not pool:
        return unresolved(NO_MEMBER)
    of_party = [term for term in pool if term["parties"] & _PARTIES.get(party, frozenset())]
    if not of_party:
        return unresolved(PARTY_DIFFERS, pool)
    chosen, steps = {term["bioguide_id"] for term in of_party}, ""
    if len(chosen) > 1 and printed_first:
        agreed = {term["bioguide_id"]: _first_name_rule(printed_first, term["first"], term["nickname"])
                  for term in of_party}
        for first_rule in _FIRST_NAME_RULES:
            named = {bioguide for bioguide, how in agreed.items() if how == first_rule}
            if named:
                if len(named) == 1:
                    chosen, steps = named, f"+{first_rule}"
                break
    members = {}
    for bioguide in sorted(chosen):
        own = [term for term in of_party if term["bioguide_id"] == bioguide]
        named_by = "surname_part" if part else "former_surname" if all(term["former"] for term in own) else "surname"
        matched = sorted({(int(term["term_index"]), term["start"], term["end"],
                          max(term["start"], term["since"]), min(term["end"], term["until"])) for term in own})
        members[bioguide] = {
            "rule": named_by + steps,
            "terms": [{"term_index": str(index), "term_start": start.isoformat(), "term_end": end.isoformat()}
                      | ({"name_start": since.isoformat(), "name_end": until.isoformat()}
                         if (since, until) != (start, end) else {})
                      for index, start, end, since, until in matched],
        }
    return {"reason": None, "candidates": sorted(chosen), "members": members, "first_named": bool(printed_first)}


def resolve_name_keys(
    keys: Iterable[tuple[str, str, str, str]],
    members: Iterable[Mapping[str, Any]],
    terms: Iterable[Mapping[str, Any]],
    affiliations: Iterable[Mapping[str, Any]] = (),
) -> dict[tuple[str, str, str, str], dict]:
    """Each distinct (congress, member_name, party, state) and its resolution under :data:`RULE_VERSION`.

    A resolution's ``members`` maps each member the key answers to onto their ``rule`` and matched
    ``terms``; several mean each row is decided on its vote's day, and ``first_named`` says the
    label printed a first name. A key that answers to none has no members, a ``reason`` and the
    ``candidates`` it stopped at. ``members``, ``terms`` and ``affiliations`` are the published
    tables' rows; a ``members`` row without :data:`_NICKNAME` states no nickname.
    O(members + terms + keys).
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
    selected_input: Callable[[str], dict | None] | None = None,
) -> int:
    """Set ``bioguide_id`` on every House ``name:`` row of a merged ``member_votes`` from the published crosswalk.

    Held rows included: each run decides every ``name:`` row again and journals the rule version,
    the inputs and whether they state nicknames, the rows left unresolved by key and reason, and each
    key's member whose rows changed with their rule and matched terms (``member-name-crosswalk``).
    With any of :data:`SOURCES` unpublished the file stays as merged. A failure to read a selected
    input aborts the build, so an unverifiable held id cannot be published. Returns how
    many ``name:`` rows carry an id.
    """
    from spicy_docs.schemas import TABLE_CONTRACTS

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
        # The nickname column arrives with a later ``members``; until then no nickname is read.
        nicknames = any(column[0] == _NICKNAME for column in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{sources['members']}')").fetchall())
        resolutions = resolve_name_keys(
            keys,
            read(f"SELECT bioguide_id, name_first, name_last, other_names_json{f', {_NICKNAME}' if nicknames else ''} "
                 f"FROM read_parquet('{sources['members']}')"),
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
            [dict(zip(_KEY, key), bioguide_id=bioguide,
                  term_start=date.fromisoformat(term.get("name_start", term["term_start"])),
                  term_end=date.fromisoformat(term.get("name_end", term["term_end"])), first_named=resolution["first_named"],
                  members=len(resolution["members"]))
             for key, resolution in resolutions.items()
             for bioguide, member in resolution["members"].items() for term in member["terms"]],
            schema=pa.schema([*((column, text) for column in _KEY), ("bioguide_id", text),
                              ("term_start", pa.date32()), ("term_end", pa.date32()),
                              ("first_named", pa.bool_()), ("members", pa.int64())]),
        ))
        # Each key's members a matched term seats on each printed day.
        con.execute(
            f"""
            CREATE TEMP TABLE serving AS
            SELECT DISTINCT {", ".join(f"t.{column}" for column in _KEY)}, d.vote_date, t.bioguide_id, t.first_named, t.members
            FROM name_terms t JOIN days d ON d.day BETWEEN t.term_start AND t.term_end
            """
        )
        # The seating day: ``seated`` counts the key's members seated on the row's day, and ``found`` is the one
        # when it is one.
        con.execute(
            f"""
            CREATE TEMP TABLE seated AS
            SELECT n.*, s.first_named, s.members, coalesce(s.seated, 0) AS seated,
                   CASE WHEN s.seated = 1 THEN s.member END AS found
            FROM (SELECT vote_id, member_key, {", ".join(_KEY)}, vote_date, bioguide_id AS held
                  FROM read_parquet('{out_file}') WHERE {_NAME_ROWS}) n
            LEFT JOIN (SELECT {", ".join(_KEY)}, vote_date, any_value(first_named) AS first_named,
                              any_value(members) AS members, count(*) AS seated, min(bioguide_id) AS member
                       FROM serving GROUP BY ALL) s USING ({", ".join(_KEY)}, vote_date)
            """
        )
        # The remaining member: of a plain label's several seated members, those no first-named label of the same
        # roll call was found to be. One left over takes the row. Then only a member the roll call's rows gave once
        # is filled.
        con.execute(
            f"""
            CREATE TEMP TABLE name_fill AS
            WITH remaining AS (
                SELECT r.vote_id, r.member_key, count(*) AS unclaimed, min(s.bioguide_id) AS member
                FROM seated r JOIN serving s USING ({", ".join(_KEY)}, vote_date)
                WHERE r.seated > 1 AND NOT r.first_named AND NOT EXISTS (
                    SELECT 1 FROM seated o WHERE o.vote_id = r.vote_id AND o.first_named AND o.found = s.bioguide_id)
                GROUP BY ALL
            ),
            decided AS (
                SELECT r.*, coalesce(r.found, CASE WHEN u.unclaimed = 1 THEN u.member END) AS member,
                       CASE WHEN r.found IS NULL THEN '{REMAINING_MEMBER}' WHEN r.members > 1 THEN '{SEATING_DAY}' END AS step
                FROM seated r LEFT JOIN remaining u USING (vote_id, member_key)
            )
            SELECT *, CASE WHEN count(*) OVER (PARTITION BY vote_id, member) = 1 THEN member END AS resolved FROM decided
            """
        )
        # One row per key and outcome: a member, the step that gave them the rows, and the rows; or a reason and its rows.
        outcomes = con.execute(
            f"""
            SELECT {", ".join(_KEY)}, resolved, CASE WHEN resolved IS NOT NULL THEN step END,
                   CASE WHEN resolved IS NOT NULL THEN NULL WHEN seated = 0 THEN '{OUTSIDE_TERM}'
                        WHEN member IS NULL THEN '{SEVERAL_SEATED}' ELSE '{LISTED_TWICE}' END AS reason,
                   count(*), count(*) FILTER (WHERE resolved IS DISTINCT FROM held)
            FROM name_fill GROUP BY ALL ORDER BY ALL
            """
        ).fetchall()
        changed = sum(moved for *_, moved in outcomes)
        digests = {name: _digest(path) for name, path in sources.items() if path is not None}
        bindings = {} if selected_input is None else {name: selected_input(name) for name in SOURCES}
        for name, binding in bindings.items():
            if binding is None or binding["processing"]["sha256"] != digests[name]:
                raise ValueError(f"Crosswalk processing input differs from selected native input: {name}")
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
    finally:
        if con is not None:
            con.close()
        filled_file.unlink(missing_ok=True)
        for path in sources.values():
            if path is not None:
                path.unlink(missing_ok=True)

    resolved, unresolved, unchanged, filled, total = [], [], 0, 0, 0
    for congress, label, party, state, bioguide, step, reason, rows, moved in outcomes:
        key = (congress, label, party, state)
        resolution, named = resolutions[key], dict(zip(_KEY, key))
        total += rows
        if bioguide is None:
            # A key that answers to no member states why itself; otherwise the row's day or roll call does.
            unresolved.append({**named, "reason": resolution["reason"] or reason,
                               "candidates": resolution["candidates"], "rows": rows})
            continue
        filled += rows
        if moved:
            member = resolution["members"][bioguide]
            resolved.append({**named, "bioguide_id": bioguide, "rule": member["rule"] + (f"+{step}" if step else ""),
                             "terms": member["terms"], "rows": rows, "rows_changed": moved})
        else:
            unchanged += 1
    logger.info(
        "{}: {:,} of {:,} name: rows carry a bioguide_id under {} ({:,} changed this run, {:,} keys answer to no member)",
        contract.name, filled, total, RULE_VERSION, changed, sum(1 for r in resolutions.values() if not r["members"]),
    )
    if evidence is not None:
        evidence.event(
            "member-name-crosswalk", rule_version=RULE_VERSION, input_available=True,
            inputs={name: {"sha256": digest, "generation": evidence.published_input(f"{name}.parquet", sha256=digest)}
                    | ({"selection": bindings[name]} if name in bindings else {})
                    for name, digest in digests.items()},
            nicknames=nicknames, keys=len(resolutions), rows=total, rows_resolved=filled, rows_changed=changed,
            resolved=resolved, unchanged=unchanged, unresolved=unresolved,
        )
    return filled
