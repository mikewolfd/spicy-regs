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
Bioguide id; a Senate row carries only a LIS id, which spicy-docs' ``match_member`` resolves
through ``members``. Every vote row appears exactly once, matched or not, in ``member_votes``'
order.

One DuckDB pass, streamed to Parquet in the same row groups: the day and the member are decided
once per distinct printed date and per distinct id pair in Python, and joined back. The per-row
Python loop this replaces held every row as a dict, so the daily rebuild's memory grew with every
Congress backfilled: 0.73 GB for the 119th's 382,536 rows and 9.46 GB (26 s) for 7.65 million,
about the 108th-119th. This pass took 0.41 GB and 2.52 GB (6 s), bounded by DuckDB's memory limit,
and wrote byte-identical files at both sizes (receipt
``~/Work/corpora/fork-execution-2026-09-21/votes-backfill-2026-09-26/mvt-bench/``).
"""

from __future__ import annotations

from collections import Counter
from datetime import date
from pathlib import Path
from types import SimpleNamespace

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
_ROW_GROUP = 100_000

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

#: One row per member vote, in file order: its day and resolved member joined on, then its
#: candidate terms (the chamber's type, ``term_start <= day <= term_end``) counted half-open and
#: inclusive. A term is taken only when exactly one qualifies under the first rule that finds any.
_MATCH_SQL = f"""
WITH votes AS (
    SELECT v.file_row_number AS row_number, v.vote_id, v.member_key, v.chamber,
           d.vote_day, d.day, d.term_type, m.bioguide
    FROM read_parquet(?, file_row_number = true) v
    JOIN days d ON d.chamber = v.chamber AND d.vote_date IS NOT DISTINCT FROM v.vote_date
    JOIN ids m ON m.bioguide_id IS NOT DISTINCT FROM v.bioguide_id AND m.lis_id IS NOT DISTINCT FROM v.lis_id
),
candidates AS (
    SELECT votes.row_number,
           count(*) FILTER (WHERE votes.day < t.term_end_day) AS half_open,
           any_value(t.term_row) FILTER (WHERE votes.day < t.term_end_day) AS half_open_term,
           count(*) AS inclusive,
           any_value(t.term_row) AS inclusive_term
    FROM votes
    JOIN terms t ON t.bioguide_id = votes.bioguide AND t.term_type = votes.term_type
        AND t.term_start_day <= votes.day AND votes.day <= t.term_end_day
    GROUP BY votes.row_number
),
decided AS (
    SELECT votes.*,
           CASE
               WHEN votes.day IS NULL THEN '{UNDATED}'
               WHEN coalesce(votes.bioguide, '') = '' THEN '{UNRESOLVED_MEMBER}'
               WHEN c.half_open = 1 THEN '{HALF_OPEN}'
               WHEN c.half_open > 1 THEN '{AMBIGUOUS}'
               WHEN c.inclusive = 1 THEN '{INCLUSIVE_END}'
               WHEN c.inclusive > 1 THEN '{AMBIGUOUS}'
               ELSE '{UNMATCHED}'
           END AS term_match,
           c.half_open_term, c.inclusive_term
    FROM votes LEFT JOIN candidates c USING (row_number)
)
SELECT d.vote_id, d.member_key, d.chamber, d.bioguide AS bioguide_id, d.vote_day, d.term_match,
       t.term_index, t.term_start, t.term_end
FROM decided d
LEFT JOIN terms t ON t.term_row = CASE d.term_match
    WHEN '{HALF_OPEN}' THEN d.half_open_term WHEN '{INCLUSIVE_END}' THEN d.inclusive_term END
ORDER BY d.row_number
"""


def _rows(path: Path, columns: list[str]) -> list[dict]:
    return pq.read_table(path, columns=columns).to_pylist()


def build_member_vote_terms(output_dir: Path) -> Path:
    """Build ``member_vote_terms.parquet`` from the three published inputs in ``output_dir``.

    Raises FileNotFoundError for a missing input, ValueError for a duplicated member
    vote, a LIS id naming two members, an unknown chamber, or a vote date outside its
    chamber's spelling.
    """
    # Local, like every spicy-docs use reachable from the transforms facade: a base
    # install imports this module without the source-readers group.
    import duckdb
    from spicy_docs.interpretation.member_matching import MemberQuery, match_member
    from spicy_docs.sources.congress.votes import vote_day

    missing = [name for name in INPUTS if not (output_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"member_vote_terms needs {', '.join(missing)} in {output_dir}")

    senators: dict[str, SimpleNamespace] = {}
    for member in _rows(output_dir / "members.parquet", ["bioguide_id", "lis_id"]):
        lis, bioguide = member["lis_id"], member["bioguide_id"]
        if lis and bioguide:
            if senators.setdefault(lis, SimpleNamespace(bioguide=bioguide)).bioguide != bioguide:
                raise ValueError(f"LIS id {lis} names two members")
    # A crosswalk with no ``by_bioguide``: the publisher's own Bioguide id stands as stated.
    crosswalk = SimpleNamespace(by_lis=senators)

    terms = [
        {**term, "term_row": row, "term_start_day": date.fromisoformat(term["term_start"]),
         "term_end_day": date.fromisoformat(term["term_end"])}
        for row, term in enumerate(_rows(
            output_dir / "member_terms.parquet", ["bioguide_id", "term_index", "term_type", "term_start", "term_end"]
        ))
        if term["term_start"] and term["term_end"]
    ]

    votes = str(output_dir / "member_votes.parquet")
    spill = output_dir / ".duckdb_tmp"
    spill.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill}'")
    repeated = con.execute(
        "SELECT vote_id, member_key FROM read_parquet(?) GROUP BY ALL HAVING count(*) > 1 LIMIT 1", [votes]
    ).fetchone()
    if repeated:
        raise ValueError(f"member vote {repeated} appears twice")
    # Each printed day and each id pair is decided once, by the same spicy-docs rules, however
    # many rows share it; a spelling ``vote_day`` refuses still refuses the build.
    days = []
    for chamber, literal in con.execute("SELECT DISTINCT chamber, vote_date FROM read_parquet(?)", [votes]).fetchall():
        if chamber not in _TERM_TYPE:
            raise ValueError(f"member vote chamber {chamber!r} has no term type")
        iso = vote_day(chamber, literal)
        days.append({"chamber": chamber, "vote_date": literal, "vote_day": iso, "term_type": _TERM_TYPE[chamber],
                     "day": None if iso is None else date.fromisoformat(iso)})
    ids = [
        {"bioguide_id": bioguide, "lis_id": lis,
         "bioguide": match_member(MemberQuery(bioguide, lis), crosswalk=crosswalk).bioguide}
        for bioguide, lis in con.execute("SELECT DISTINCT bioguide_id, lis_id FROM read_parquet(?)", [votes]).fetchall()
    ]
    text = pa.string()
    con.register("days", pa.Table.from_pylist(days, schema=pa.schema([
        ("chamber", text), ("vote_date", text), ("vote_day", text), ("term_type", text), ("day", pa.date32()),
    ])))
    con.register("ids", pa.Table.from_pylist(ids, schema=pa.schema([
        ("bioguide_id", text), ("lis_id", text), ("bioguide", text),
    ])))
    con.register("terms", pa.Table.from_pylist(terms, schema=pa.schema([
        ("bioguide_id", text), ("term_index", text), ("term_type", text), ("term_start", text), ("term_end", text),
        ("term_row", pa.int64()), ("term_start_day", pa.date32()), ("term_end_day", pa.date32()),
    ])))

    out_file = output_dir / OUTPUT
    staged = output_dir / f".{OUTPUT}.partial"
    counts: Counter[str] = Counter()
    with pq.ParquetWriter(staged, _SCHEMA, compression="zstd") as writer:
        buffered = _SCHEMA.empty_table()
        for batch in con.execute(_MATCH_SQL, [votes]).to_arrow_reader(_ROW_GROUP):
            counts.update(batch.column("term_match").to_pylist())
            buffered = pa.concat_tables([buffered, pa.Table.from_batches([batch]).cast(_SCHEMA)])
            # Whole row groups, cut where ``pq.write_table(row_group_size=_ROW_GROUP)`` cuts them.
            while buffered.num_rows >= _ROW_GROUP:
                writer.write_table(buffered.slice(0, _ROW_GROUP), row_group_size=_ROW_GROUP)
                buffered = buffered.slice(_ROW_GROUP)
        if buffered.num_rows:
            writer.write_table(buffered, row_group_size=_ROW_GROUP)
    staged.replace(out_file)
    logger.info("Member vote terms: {:,} rows {}", sum(counts.values()), dict(sorted(counts.items())))
    return out_file
