"""Transform: build ``org_committee_links.parquet``, the commenter-organization ↔ FEC committee name bridge.

Materializes the bridge ``docs/index.md`` describes but nothing implemented —
organization name links ``lobbying_filings``, ``fec_committees`` and comment
filers where no shared id exists — once, instead of every consumer re-inventing
its own normalization, and shows its work in ``match_method`` / ``confidence`` so
a consumer can pick a precision bar.

**Grain.** One row per (``organization`` string as filed on a comment,
``committee_id``); the org string is reused across comments and so is the unit
of resolution, and it joins straight back to ``comments.organization``.

**Matching.** Both sides are normalized the same way (uppercase, drop
parenthetical asides and apostrophes, ``&`` → ``AND``, punctuation → space),
then the committee side is stripped of PAC decorations (``... POLITICAL ACTION
COMMITTEE``, ``... PAC``, ...) to recover the sponsoring organization's name.
Three tiers, strongest first: ``exact`` (full normalized names equal), ``core``
(decoration-stripped cores equal), ``prefix`` (the committee core *starts with*
the whole organization core on a token boundary, matched by equality against
pre-generated committee token prefixes rather than a ``LIKE`` nested loop,
which would be ~1.3B comparisons).

**Fan-out is a signal, not an error.** One organization legitimately matching
many committees is usually a real affiliate network (*Planned Parenthood* hits
~90 state affiliate committees, SEIU ~16 locals, IBEW ~15), so rows are never
truncated: each carries ``committee_match_count`` and a ``confidence`` that
degrades with fan-out. ``confidence`` grades the match rule alone.

**Committees that are never an organization's own.** A candidate's committee
(type H, S or P) or a leadership PAC (designation D) is matched and counted in
the fan-out like any other committee, then left out of the table: a commenter
named like one is a person, a namesake or a slogan.

**The sponsor FEC states.** ``connected_organization_name`` is the committee's
connected organization as it last stated one in ``fec_committee_history``,
verbatim; FEC's placeholders (``NONE``, ``N/A``, ``BLANK``, ...) state none, as
spicy-docs' one predicate reads them (``is_placeholder_name``, which the build
registers as a DuckDB function of the same name). ``sponsor_name_match``
compares it with the organization string, both read by the same
normalization, and is evidence beside the grade, never part of it.

**Junk guards.** A core must be ≥ :data:`MIN_CORE_LENGTH` chars and ≥
:data:`MIN_CORE_TOKENS` tokens (which also blocks bare acronyms like ``NRDC``),
and must not be a :data:`GENERIC_ORG_CORES` entry; the blocklist matches the
*whole* core only, so blocking ``NEW MEXICO`` still leaves ``NEW MEXICO CATTLE
GROWERS ASSOCIATION`` free to match.

**Coverage is small by upstream reality, not a bug.** ``comments.organization``
holds what the publisher's record states (a submitter's entry or a campaign's
sponsor; EPA and FDA state none): 2,222,760 of 26,415,400 comments on the
2026-10-03 export (receipt ``sha256:77a08369…``). Most commenting organizations
run no federal PAC, so a low match rate is the correct answer rather than a
matcher to tune harder; ``name_source`` is stamped
on every row so text-derived names added later (comment title, letterhead,
signature block) can arrive as extra rows without breaking consumers.

``comments.parquet`` is read from ``output_dir`` when present, otherwise
straight from the public R2 URL over ``httpfs`` so Parquet projection pushdown
fetches only the five named columns rather than the multi-GB table. Comment
rows are deduplicated on ``comment_id`` (newest ``modify_date`` wins), matching
the MCP server's ``comments`` view so counts here agree with counts there.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.duckdb_settings import load_public_http
from spicy_regs.public_url import comments_source

OUTPUT = "org_committee_links.parquet"

# The published schema, in a fixed order.
COLUMNS: tuple[tuple[str, str], ...] = (
    ("organization", "VARCHAR"),
    ("organization_norm", "VARCHAR"),
    ("organization_core", "VARCHAR"),
    ("name_source", "VARCHAR"),
    ("committee_id", "VARCHAR"),
    ("committee_name", "VARCHAR"),
    ("committee_type_full", "VARCHAR"),
    ("designation_full", "VARCHAR"),
    ("party_full", "VARCHAR"),
    ("organization_type_full", "VARCHAR"),
    ("committee_state", "VARCHAR"),
    ("match_method", "VARCHAR"),
    ("confidence", "VARCHAR"),
    ("committee_match_count", "BIGINT"),
    ("comment_count", "BIGINT"),
    ("docket_count", "BIGINT"),
    ("agency_codes_json", "VARCHAR"),
    ("first_comment_date", "VARCHAR"),
    ("last_comment_date", "VARCHAR"),
    ("connected_organization_name", "VARCHAR"),
    ("sponsor_name_match", "VARCHAR"),
)

# Where a matched name came from. Only the structured field today; text-derived
# names (title, letterhead, signature block) would arrive as additional rows
# under their own source rather than displacing these.
NAME_SOURCE_ORGANIZATION_FIELD = "organization_field"

# An organization core shorter/thinner than this is too generic to match on.
# Two tokens also keeps bare acronyms ("NRDC", "NASW") out of the core/prefix
# tiers, where a three-letter string would match unrelated committees.
MIN_CORE_LENGTH = 8
MIN_CORE_TOKENS = 2

# Prefix tier: compare the first 2..8 tokens of a committee core. Beyond eight
# tokens the remaining suffix is long enough that the match is no longer a
# recognizable "<org> <decoration>" shape.
MAX_PREFIX_TOKENS = 8

# Fan-out above which a prefix match is demoted to "low". Real affiliate
# networks exceed it (Planned Parenthood ~90, SEIU ~16) and are kept — the
# confidence label is what tells them apart from an over-broad core.
PREFIX_FANOUT_MEDIUM_MAX = 5

# FEC committee kinds that are never an organization's own committee: a House, Senate or presidential candidate's
# committee (committee type), and a member's leadership PAC (designation). A commenter named like one is a person, a
# slogan or a namesake: 25 of 25 such links sampled on 2026-10-03 were false (round-6 scout C, M6).
CANDIDATE_COMMITTEE_TYPES = ("H", "S", "P")
LEADERSHIP_PAC_DESIGNATION = "D"

# Trailing committee decorations, stripped from the FEC side to recover the
# sponsoring organization's name. Longest phrases first so the alternation
# prefers them, and applied over several passes to peel stacked suffixes
# ("... POLITICAL ACTION COMMITTEE FUND").
_DECORATIONS = (
    "POLITICAL ACTION COMMITTEE",
    "POLITICAL ACTION CMTE",
    "POLITICAL PARTICIPATION FUND",
    "SEPARATE SEGREGATED FUND",
    "POLITICAL EDUCATION COMMITTEE",
    "GOOD GOVERNMENT COMMITTEE",
    "GOOD GOVERNMENT FUND",
    "POLITICAL ACTION FUND",
    "POLITICAL COMMITTEE",
    "POLITICAL ACTION",
    "POLITICAL FUND",
    "FEDERAL PAC",
    "PAC FUND",
    "PAC",
    "COMMITTEE",
    "FUND",
    "EMPLOYEES",
    "EMPLOYEE",
    "VOLUNTARY",
    "FEDERAL",
    "POLITICAL",
)
_DECORATION_PASSES = 3

# Corporate/legal suffixes stripped from both sides. Deliberately excludes the
# ambiguous short ones (CO, PC, PA, US) that also occur as real name tokens.
_LEGAL_SUFFIXES = (
    "INCORPORATED",
    "CORPORATION",
    "COMPANY",
    "LIMITED",
    "PLLC",
    "CORP",
    "INC",
    "LLC",
    "LLP",
    "LTD",
)
_LEGAL_PASSES = 2

# Cores that are never an organization. Matched against the *whole* core, so a
# blocked entry never suppresses a real name that merely starts with it.
# Single-token entries are already excluded by MIN_CORE_TOKENS; the multi-token
# ones are what matter (each was an observed false positive or is an obvious
# near-miss of one).
GENERIC_ORG_CORES: frozenset[str] = frozenset(
    {
        # Self-descriptions in place of an organization.
        "SELF EMPLOYED",
        "SELF ONLY",
        "MYSELF ONLY",
        "PRIVATE CITIZEN",
        "PRIVATE INDIVIDUAL",
        "PRIVATE PERSON",
        "PRIVATE PRACTICE",
        "PRIVATE SECTOR",
        "CONCERNED CITIZEN",
        "CONCERNED CITIZENS",
        "INDIVIDUAL CITIZEN",
        "SENIOR CITIZEN",
        "US CITIZEN",
        "GENERAL PUBLIC",
        "THE PUBLIC",
        "WE THE PEOPLE",
        "BUSINESS OWNER",
        "SMALL BUSINESS OWNER",
        "RETIRED TEACHER",
        "NO ORGANIZATION",
        "NOT AFFILIATED",
        "NOT APPLICABLE",
        "NONE OF THE ABOVE",
        # Government employers. A commenter naming their employer is not an
        # organization→PAC link, and these prefix-match aggressively.
        "UNITED STATES",
        "UNITED STATES SENATE",
        "UNITED STATES CONGRESS",
        "UNITED STATES GOVERNMENT",
        "UNITED STATES HOUSE OF REPRESENTATIVES",
        "US SENATE",
        "US CONGRESS",
        "US GOVERNMENT",
        "US HOUSE OF REPRESENTATIVES",
        "HOUSE OF REPRESENTATIVES",
        "FEDERAL GOVERNMENT",
        "STATE GOVERNMENT",
        "LOCAL GOVERNMENT",
        "US MILITARY",
        # Multi-token states/territories a commenter gives as their location.
        # (Single-token ones fall to MIN_CORE_TOKENS.)
        "NEW HAMPSHIRE",
        "NEW JERSEY",
        "NEW MEXICO",
        "NEW YORK",
        "NORTH CAROLINA",
        "NORTH DAKOTA",
        "RHODE ISLAND",
        "SOUTH CAROLINA",
        "SOUTH DAKOTA",
        "WEST VIRGINIA",
        "DISTRICT OF COLUMBIA",
        "PUERTO RICO",
        "VIRGIN ISLANDS",
        "AMERICAN SAMOA",
        "NORTHERN MARIANA ISLANDS",
    }
)


def _sql_str(value: str) -> str:
    """Render a Python string as a single-quoted SQL literal."""
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _normalize(expr: str) -> str:
    """SQL for the shared normalization applied to both sides of the join.

    Uppercase, drop parenthetical asides (``(BANKPAC)``, ``(A.K.A. ...)``) and
    apostrophes (straight and curly — the FEC data contains both, and one row
    even doubles them), expand ``&``, then collapse every other run of
    non-alphanumerics to a single space.
    """
    out = f"upper({expr})"
    out = f"regexp_replace({out}, '\\([^)]*\\)', ' ', 'g')"
    out = f"regexp_replace({out}, '''', '', 'g')"
    out = f"replace({out}, '’', '')"
    out = f"regexp_replace({out}, '&', ' AND ', 'g')"
    out = f"regexp_replace({out}, '[^A-Z0-9]+', ' ', 'g')"
    return f"trim({out})"


def _strip_suffixes(expr: str, suffixes: tuple[str, ...], passes: int) -> str:
    """SQL that peels trailing ``suffixes`` off ``expr``, ``passes`` times."""
    pattern = " (" + "|".join(suffixes) + ")$"
    out = expr
    for _ in range(passes):
        out = f"regexp_replace({out}, {_sql_str(pattern)}, '')"
    return out


def _core(expr: str, *, decorations: bool) -> str:
    """SQL for the comparable core of a normalized name.

    Strips legal suffixes and a leading ``THE`` from both sides; ``decorations``
    additionally peels the committee-only PAC suffixes.
    """
    out = _strip_suffixes(expr, _LEGAL_SUFFIXES, _LEGAL_PASSES)
    if decorations:
        out = _strip_suffixes(out, _DECORATIONS, _DECORATION_PASSES)
        # A decoration can uncover another legal suffix beneath it
        # ("... INC POLITICAL ACTION COMMITTEE" → "... INC").
        out = _strip_suffixes(out, _LEGAL_SUFFIXES, _LEGAL_PASSES)
    out = f"regexp_replace({out}, '^THE ', '')"
    return f"trim({out})"


def build_query(comments_source: str, committees_file: str, history_file: str, out_file: str) -> str:
    """Return the full COPY ... TO statement that materializes the link table.

    Split out from :func:`build_org_committee_links` so the exact published SQL
    can be inspected and exercised without a DuckDB connection or R2 access.
    """
    # Escape the interpolated paths the way `data_dictionary.discover_schemas`
    # does; the remote URL is separately validated in public_url.comments_source.
    comments_source = comments_source.replace("'", "''")
    committees_file = committees_file.replace("'", "''")
    history_file = history_file.replace("'", "''")

    org_norm = _normalize("r.organization")
    org_core = _core("o.organization_norm", decorations=False)
    cm_norm = _normalize("f.name")
    cm_core = _core("n.committee_norm", decorations=True)
    # A filer often writes its PAC's own name as the sponsor, so the sponsor is read both as an organization and, with
    # the PAC decorations peeled, as a committee; spaces are set aside ("DELTA AIRLINES" for "Delta Air Lines").
    sponsor_keys = [f"replace({_core('h.sponsor_norm', decorations=peel)}, ' ', '')" for peel in (False, True)]
    blocklist = ", ".join(_sql_str(entry) for entry in sorted(GENERIC_ORG_CORES))
    never_own_types = ", ".join(_sql_str(code) for code in CANDIDATE_COMMITTEE_TYPES)

    return f"""
    COPY (
        WITH comment_orgs AS (
            -- Dedup on comment_id the same way the MCP `comments` view does, so
            -- counts published here agree with counts computed there.
            SELECT
                c.organization AS organization,
                c.docket_id AS docket_id,
                c.agency_code AS agency_code,
                c.posted_date AS posted_date
            FROM read_parquet('{comments_source}') c
            WHERE c.organization IS NOT NULL
              AND trim(c.organization) <> ''
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY c.comment_id ORDER BY c.modify_date DESC NULLS LAST
            ) = 1
        ),
        orgs_rolled AS (
            SELECT
                organization,
                count(*)::BIGINT AS comment_count,
                count(DISTINCT docket_id)::BIGINT AS docket_count,
                to_json(list_sort(list(DISTINCT agency_code)
                    FILTER (WHERE agency_code IS NOT NULL))) AS agency_codes_json,
                min(posted_date) AS first_comment_date,
                max(posted_date) AS last_comment_date
            FROM comment_orgs
            GROUP BY organization
        ),
        orgs_normed AS (
            SELECT r.*, {org_norm} AS organization_norm
            FROM orgs_rolled r
        ),
        orgs AS (
            SELECT o.*, {org_core} AS organization_core
            FROM orgs_normed o
        ),
        orgs_eligible AS (
            SELECT * FROM orgs
            WHERE length(organization_core) >= {MIN_CORE_LENGTH}
              AND len(string_split(organization_core, ' ')) >= {MIN_CORE_TOKENS}
              AND organization_core NOT IN ({blocklist})
        ),
        committees_normed AS (
            SELECT
                f.committee_id,
                f.name AS committee_name,
                f.committee_type_full,
                f.designation_full,
                f.party_full,
                f.organization_type_full,
                f.state AS committee_state,
                -- A candidate's committee or a leadership PAC is never an organization's own committee. It still
                -- counts in the fan-out below, so a name it matches stays as generic as it was.
                coalesce(f.committee_type, '') NOT IN ({never_own_types})
                    AND coalesce(f.designation, '') <> {_sql_str(LEADERSHIP_PAC_DESIGNATION)} AS can_be_own,
                {cm_norm} AS committee_norm
            FROM read_parquet('{committees_file}') f
            WHERE f.committee_id IS NOT NULL AND f.name IS NOT NULL
        ),
        committees AS (
            SELECT n.*, {cm_core} AS committee_core
            FROM committees_normed n
        ),
        -- Token prefixes of each committee core, so the prefix tier is an
        -- equality (hash) join instead of a 14K x 89K LIKE nested loop.
        committee_prefixes AS (
            SELECT t.committee_id, array_to_string(t.tokens[1:g.k], ' ') AS prefix_key
            FROM (
                SELECT committee_id, string_split(committee_core, ' ') AS tokens
                FROM committees
            ) t,
                 generate_series({MIN_CORE_TOKENS}, {MAX_PREFIX_TOKENS}) AS g(k)
            WHERE g.k < len(t.tokens)
        ),
        matched AS (
            SELECT o.organization, c.committee_id, 'exact' AS match_method, 1 AS method_rank
            FROM orgs_eligible o
            JOIN committees c ON o.organization_norm = c.committee_norm
            UNION ALL
            SELECT o.organization, c.committee_id, 'core', 2
            FROM orgs_eligible o
            JOIN committees c ON o.organization_core = c.committee_core
            UNION ALL
            SELECT o.organization, p.committee_id, 'prefix', 3
            FROM orgs_eligible o
            JOIN committee_prefixes p ON o.organization_core = p.prefix_key
        ),
        -- One row per (organization, committee), keeping the strongest tier.
        best AS (
            SELECT organization, committee_id, match_method
            FROM matched
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY organization, committee_id ORDER BY method_rank
            ) = 1
        ),
        counted AS (
            SELECT b.*,
                   count(*) OVER (PARTITION BY b.organization)::BIGINT AS committee_match_count
            FROM best b
        ),
        -- The connected organization each committee last stated, in its latest filing year that states one; FEC's
        -- placeholders (NONE, N/A, BLANK, ...) are not a statement.
        sponsors AS (
            SELECT h.committee_id, h.connected_organization_name,
                   {sponsor_keys[0]} AS sponsor_key, {sponsor_keys[1]} AS sponsor_peeled_key
            FROM (
                SELECT f.*, {_normalize("f.connected_organization_name")} AS sponsor_norm
                FROM read_parquet('{history_file}') f
                WHERE f.connected_organization_name IS NOT NULL
                  AND NOT is_placeholder_name(f.connected_organization_name)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY f.committee_id ORDER BY TRY_CAST(f.cycle AS INTEGER) DESC NULLS LAST
                ) = 1
            ) h
        )
        -- Every column is cast explicitly so the published schema is pinned by
        -- this query rather than inherited from the input parquet (an all-null
        -- passthrough column would otherwise read back as INTEGER).
        SELECT
            o.organization::VARCHAR AS organization,
            o.organization_norm::VARCHAR AS organization_norm,
            o.organization_core::VARCHAR AS organization_core,
            {_sql_str(NAME_SOURCE_ORGANIZATION_FIELD)}::VARCHAR AS name_source,
            c.committee_id::VARCHAR AS committee_id,
            c.committee_name::VARCHAR AS committee_name,
            c.committee_type_full::VARCHAR AS committee_type_full,
            c.designation_full::VARCHAR AS designation_full,
            c.party_full::VARCHAR AS party_full,
            c.organization_type_full::VARCHAR AS organization_type_full,
            c.committee_state::VARCHAR AS committee_state,
            m.match_method::VARCHAR AS match_method,
            CASE
                WHEN m.match_method IN ('exact', 'core') THEN 'high'
                WHEN m.committee_match_count <= {PREFIX_FANOUT_MEDIUM_MAX} THEN 'medium'
                ELSE 'low'
            END::VARCHAR AS confidence,
            m.committee_match_count::BIGINT AS committee_match_count,
            o.comment_count::BIGINT AS comment_count,
            o.docket_count::BIGINT AS docket_count,
            o.agency_codes_json::VARCHAR AS agency_codes_json,
            o.first_comment_date::VARCHAR AS first_comment_date,
            o.last_comment_date::VARCHAR AS last_comment_date,
            s.connected_organization_name::VARCHAR AS connected_organization_name,
            CASE
                WHEN s.committee_id IS NULL THEN 'not_stated'
                WHEN replace(o.organization_core, ' ', '') IN (s.sponsor_key, s.sponsor_peeled_key) THEN 'agrees'
                ELSE 'differs'
            END::VARCHAR AS sponsor_name_match
        FROM counted m
        JOIN orgs_eligible o ON o.organization = m.organization
        JOIN committees c ON c.committee_id = m.committee_id
        LEFT JOIN sponsors s ON s.committee_id = m.committee_id
        WHERE c.can_be_own
        -- Sorted by organization so `WHERE organization = ?` prunes row groups.
        ORDER BY o.organization, m.match_method, c.committee_id
    ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
    """


def build_org_committee_links(output_dir: Path) -> Path:
    """Build ``org_committee_links.parquet`` (commenter org → FEC committee)."""
    import duckdb
    from spicy_docs.schemas.fec_committee_history import is_placeholder_name  # imported on use: spicy-docs is optional

    committees_file, history_file = output_dir / "fec_committees.parquet", output_dir / "fec_committee_history.parquet"
    for required in (committees_file, history_file):
        if not required.exists():
            raise FileNotFoundError(f"{required.name} not found in {output_dir}")

    comments = comments_source(output_dir)
    out_file = output_dir / OUTPUT

    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)

    con = duckdb.connect()
    # The query reads FEC's placeholders by spicy-docs' predicate under its own name; a NULL is no statement either.
    con.create_function("is_placeholder_name", is_placeholder_name, ["VARCHAR"], "BOOLEAN")
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")
    if comments.startswith("https://"):
        load_public_http(con)

    logger.info("Building org ↔ committee links via DuckDB...")
    con.execute(build_query(comments, str(committees_file), str(history_file), str(out_file)))
    con.close()

    rows = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("org_committee_links: {:,} rows", rows)
    return out_file
