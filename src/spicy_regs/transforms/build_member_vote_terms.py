"""Transform: build ``member_vote_terms.parquet``, the term each member vote counts toward.

Delivery decision 2 (2026-09-22): a vote on ``vote_day`` counts toward the member's term with
``term_start <= vote_day < term_end`` (half-open, so the day one Congress ends and the next
begins belongs to the new term). Only where that finds no term is an inclusive end accepted, and
only if it yields exactly one term. The term's type follows the chamber (``rep`` for the House,
``sen`` for the Senate); a term with no end date is never treated as open-ended.

Measured on the frozen 1,573-vote selection (``votes-qualification/complete-member-join-replay``):
half-open terms leave 18 of 381,936 observations unmatched, the fallback resolves 15 of them with
no ambiguity, and the other three are native ``Not Voting`` rows dated after their member's
recorded term, which stay ``unmatched``. Inclusive ends alone would have made 1,855 House rows on
2025-01-03 ambiguous.

The day is spicy-docs' ``vote_day`` over ``member_votes.vote_date``, the chamber's printed
Eastern date; a spelling it cannot read refuses the build, and a vote whose file prints no date is
``undated`` with no term rather than matched against a guessed day. A House row carries its
Bioguide id; a Senate row carries only a LIS id, resolved through ``members``. Every vote row
appears exactly once, matched or not.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

OUTPUT = "member_vote_terms.parquet"
INPUTS = ("member_votes.parquet", "members.parquet", "member_terms.parquet")

COLUMNS = (
    "vote_id",
    "member_key",
    "chamber",
    "bioguide_id",
    "vote_day",
    "term_match",
    "term_index",
    "term_start",
    "term_end",
)
_SCHEMA = pa.schema([(column, pa.string()) for column in COLUMNS])

#: ``term_match`` values, in the order the rule decides them: a vote with no day is ``undated``
#: (its member is still resolved, but no term is chosen), then a member with no Bioguide id is
#: ``unresolved_member``, and only then is a term matched, half-open before inclusive-end.
UNDATED, UNRESOLVED_MEMBER, HALF_OPEN, INCLUSIVE_END, AMBIGUOUS, UNMATCHED = (
    "undated",
    "unresolved_member",
    "half_open",
    "inclusive_end",
    "ambiguous",
    "unmatched",
)

_TERM_TYPE = {"house": "rep", "senate": "sen"}


def _rows(path: Path, columns: list[str]) -> list[dict]:
    return pq.read_table(path, columns=columns).to_pylist()


def build_member_vote_terms(output_dir: Path) -> Path:
    """Build ``member_vote_terms.parquet`` from the three published inputs in ``output_dir``.

    Raises FileNotFoundError for a missing input, ValueError for a duplicated member
    vote, a LIS id naming two members, or a vote date outside its chamber's spelling.
    """
    # Local, like every spicy-docs use reachable from the transforms facade: a base
    # install imports this module without the source-readers group.
    from spicy_docs.sources.congress.votes import vote_day

    missing = [name for name in INPUTS if not (output_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"member_vote_terms needs {', '.join(missing)} in {output_dir}")

    bioguide_by_lis: dict[str, str] = {}
    for member in _rows(output_dir / "members.parquet", ["bioguide_id", "lis_id"]):
        lis, bioguide = member["lis_id"], member["bioguide_id"]
        if lis and bioguide:
            if bioguide_by_lis.setdefault(lis, bioguide) != bioguide:
                raise ValueError(f"LIS id {lis} names two members")

    terms: dict[tuple[str, str], list[tuple[date, date, dict]]] = defaultdict(list)
    for term in _rows(
        output_dir / "member_terms.parquet", ["bioguide_id", "term_index", "term_type", "term_start", "term_end"]
    ):
        if term["term_start"] and term["term_end"]:
            start, end = date.fromisoformat(term["term_start"]), date.fromisoformat(term["term_end"])
            terms[(term["bioguide_id"], term["term_type"])].append((start, end, term))

    rows, seen, counts = [], set(), defaultdict(int)
    # One roll call's literal repeats on every member row; read each once.
    days: dict[tuple[str, str | None], date | None] = {}
    votes = _rows(
        output_dir / "member_votes.parquet", ["vote_id", "member_key", "chamber", "bioguide_id", "lis_id", "vote_date"]
    )
    for vote in votes:
        identity = (vote["vote_id"], vote["member_key"])
        if identity in seen:
            raise ValueError(f"member vote {identity} appears twice")
        seen.add(identity)
        chamber = vote["chamber"]
        literal = (chamber, vote["vote_date"])
        if literal not in days:
            iso = vote_day(*literal)
            days[literal] = None if iso is None else date.fromisoformat(iso)
        day = days[literal]
        bioguide = vote["bioguide_id"] or bioguide_by_lis.get(vote["lis_id"] or "")
        term, match = None, UNRESOLVED_MEMBER
        if day is None:  # decided before the member: no day, no term, whoever voted
            match = UNDATED
        elif bioguide:
            candidates = terms.get((bioguide, _TERM_TYPE[chamber]), ())
            half_open = [t for start, end, t in candidates if start <= day < end]
            inclusive = [t for start, end, t in candidates if start <= day <= end]
            if len(half_open) == 1:
                term, match = half_open[0], HALF_OPEN
            elif half_open:
                match = AMBIGUOUS
            elif len(inclusive) == 1:
                term, match = inclusive[0], INCLUSIVE_END
            else:
                match = AMBIGUOUS if inclusive else UNMATCHED
        counts[match] += 1
        rows.append(
            {
                "vote_id": vote["vote_id"],
                "member_key": vote["member_key"],
                "chamber": chamber,
                "bioguide_id": bioguide,
                "vote_day": None if day is None else day.isoformat(),
                "term_match": match,
                "term_index": term["term_index"] if term else None,
                "term_start": term["term_start"] if term else None,
                "term_end": term["term_end"] if term else None,
            }
        )

    out_file = output_dir / OUTPUT
    staged = output_dir / f".{OUTPUT}.partial"
    pq.write_table(pa.Table.from_pylist(rows, schema=_SCHEMA), staged, compression="zstd", row_group_size=100_000)
    staged.replace(out_file)
    logger.info("Member vote terms: {:,} rows {}", len(rows), dict(sorted(counts.items())))
    return out_file
