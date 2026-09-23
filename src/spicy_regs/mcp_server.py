"""MCP server exposing the published Spicy Regs tables as read-only SQL tools.

The tools (``list_sources``, ``describe_table``, ``query_sql``) run over a
cached DuckDB connection whose views are pinned to one publication snapshot,
reading either the public R2 bucket or an explicitly configured local directory
(``SPICY_REGS_DATA_DIR``). The HTTP app also serves the human setup page at ``/``.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
import tempfile
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from functools import lru_cache
from importlib.resources import files
from pathlib import Path
from time import monotonic as _monotonic
from typing import Any
from uuid import UUID

import duckdb
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import Icon
from starlette.requests import Request
from starlette.responses import Response

from spicy_regs._icon import ICON_DATA_URI
from spicy_regs.public_url import resolve_r2_base_url

TABLES = (
    "dockets",
    "documents",
    "comments",
    "comments_index",
    "feed_summary",
    "agency_stats",
    "agency_monthly_volume",
    "rulemaking_lifecycles",
    "fr_docket_links",
    "discovery_signals",
    "cfr_sections",
    "congress_bills",
    "bill_subjects",
    "unified_agenda",
    "federal_register",
    "sam_entities",
    "lobbying_filings",
    "fec_committees",
    "fec_source_catalog",
    "fec_collections",
    "fec_source_records",
    "fec_relationships",
    "org_committee_links",
    "gao_reports",
    "crs_reports",
    "court_dockets",
    "court_docket_groups",
    "court_opinion_clusters",
    "court_citations",
    "court_citation_map",
    "court_parentheticals",
    "court_opinions",
    "usaspending_recipients",
    "fcc_proceedings",
    "fcc_filings",
    # The BillTrax-derived tables hosted from spicy-docs' table contracts.
    # Listed literally rather than imported from data_dictionary: that module
    # reads the contracts out of the spicy-docs wheel, and the MCP server is a
    # base install that must not require the source-readers group.
    # test_mcp_server_tables_match_dictionary keeps the two lists equal.
    "bill_actions",
    "bill_committees",
    "bill_publisher_summaries",
    "bill_versions",
    "bill_sections",
    "section_diffs",
    "section_diff_items",
    "financial_changes",
    "section_classifications",
    "bill_summaries",
    "diff_summaries",
    "public_activity_events",
    "amendments",
    "press_releases",
    "roll_call_votes",
    "member_votes",
    "members",
    "member_terms",
    "committee_reports",
    "report_sections",
    "hearing_transcripts",
    "hearing_bill_links",
    "cbo_cost_estimates",
    "house_activity_reports",
    "budget_volumes",
    "bill_committee_actions",
    "document_citations",
    "senate_expenditures",
    # A8/A9 (laws and rosters): the laws and committee-rosters rollups.
    "laws",
    "law_code_sections",
    "table3_records",
    "committees",
    "committee_assignments",
    # The Congress.gov index tables (gaps A5, A7, A10).
    "house_communications",
    "committee_meetings",
    "record_issues",
    "treaties",
    "nominations",
    # Processing state, not a contract table: the bill-family rollup retains one
    # BILLSTATUS folder listing entry per folder so the next run can prove a zip
    # unchanged without downloading it. Queryable for the same reason every
    # other published object is — it is on the public bucket either way, and a
    # reader asking "when did this pipeline last see the H.R. zip" should not
    # have to reach past the server to answer it.
    "bill_family_archives",
    "bill_vote_references",
    # The pre-BILLSTATUS backfill's retained state, published for the same
    # reason: it is on the bucket either way, and "which bills has the
    # backfill filled or refused, under what list stamp" is answerable
    # without reaching past the server.
    "bill_family_backfills",
    "bill_family_backfill_walks",
    "committee_report_reads",
)
STATEMENT_TIMEOUT = os.environ.get("SPICY_REGS_STATEMENT_TIMEOUT", "790s")

logger = logging.getLogger(__name__)

CATALOG_ALIAS = "reg_catalog"
DEFAULT_CATALOG_NAMESPACE = "default"


def _parse_timeout_seconds(raw: str) -> float | None:
    """Parse a ``ms``/``s``/``m`` duration; an invalid spelling raises RuntimeError, non-positive means no timeout."""
    text = raw.strip().lower()
    if not text:
        return None
    multiplier = 1.0
    for suffix, factor in (("ms", 0.001), ("s", 1.0), ("m", 60.0)):
        if text.endswith(suffix):
            multiplier = factor
            text = text[: -len(suffix)].strip()
            break
    try:
        value = float(text)
    except ValueError as exc:
        raise RuntimeError(f"SPICY_REGS_STATEMENT_TIMEOUT is not a valid duration: {raw!r}") from exc
    if value <= 0:
        return None
    return value * multiplier


STATEMENT_TIMEOUT_SECONDS = _parse_timeout_seconds(STATEMENT_TIMEOUT)

INSTRUCTIONS = (
    "Query Spicy Regs public datasets across government sources. Use list_sources "
    "to discover available tables and declared outputs, describe_table for actual "
    "schemas, field meanings, identifiers and coverage caveats, and query_sql for "
    "read-only queries and joins. A declared output or coverage measurement does "
    "not establish publication or freshness. Always LIMIT exploratory results. "
    "Cite source identifiers, evidence locators and dates from returned rows."
)

ICONS = [Icon(src=ICON_DATA_URI, mimeType="image/png", sizes=["512x512"])]


def _resolve_r2_base_url() -> str:
    return resolve_r2_base_url()


R2_BASE_URL = _resolve_r2_base_url()


def _resolve_data_dir() -> Path | None:
    """An explicit local source replaces remote tables for the whole connection."""
    raw = os.environ.get("SPICY_REGS_DATA_DIR")
    if raw is None:
        return None
    if not raw.strip():
        raise RuntimeError("SPICY_REGS_DATA_DIR must name a directory")
    # Resolve a `current` symlink when building each connection, not at import.
    path = Path(raw).expanduser().absolute()
    if not path.is_dir():
        raise RuntimeError(f"SPICY_REGS_DATA_DIR is not a directory: {path}")
    return path


DATA_DIR = _resolve_data_dir()


def _source_details(cursor: duckdb.DuckDBPyConnection | None = None) -> dict[str, str]:
    """Describe the active data source (local directory or R2) for tool replies."""
    if DATA_DIR is not None:
        selection = _connection_local_selection(cursor) if cursor is not None else None
        if selection is not None:
            return {"source": "local", "base_path": str(DATA_DIR), "selected_directory": selection["directory"]}
        return {"source": "local", "base_path": str(DATA_DIR)}
    return {"source": "r2", "base_url": R2_BASE_URL}


def _resolve_home_directory() -> str:
    raw = os.environ.get("SPICY_REGS_HOME_DIR", tempfile.gettempdir())
    if any(c in raw for c in ("\x00", "\n", "\r")):
        raise RuntimeError(f"SPICY_REGS_HOME_DIR contains illegal characters: {raw!r}")
    return raw


HOME_DIRECTORY = _resolve_home_directory()


def _resolve_memory_limit() -> str | None:
    """DuckDB memory ceiling from SPICY_REGS_MEMORY_LIMIT (e.g. '12GB', '75%').

    Unset => None => DuckDB's own default (~80% of detected RAM). Set it on hosts
    where DuckDB can't see the real allocation (containers detect host RAM, not
    the cgroup limit) so it spills/errors before the platform OOM-kills the process.
    Interpolated into a SET, so the value is format-validated.
    """
    raw = os.environ.get("SPICY_REGS_MEMORY_LIMIT", "").strip()
    if not raw:
        return None
    if not re.fullmatch(r"\d+(\.\d+)?\s*(%|[KMGT]?i?B)?", raw, re.IGNORECASE):
        raise RuntimeError(f"SPICY_REGS_MEMORY_LIMIT is not a valid size/percent: {raw!r}")
    return raw


def _resolve_temp_dir() -> str:
    """DuckDB spill directory from SPICY_REGS_TEMP_DIR.

    Default '' disables spilling — the safe serverless behavior, since DuckDB's
    default temp dir is a relative '.tmp' that is read-only on serverless hosts,
    so a spilling query would fail there anyway. Supply a writable path (a
    container with real disk, or a mounted volume) to let big GROUP BY/ORDER BY
    spill instead of erroring. Interpolated into a SET, so injection chars are
    rejected.
    """
    raw = os.environ.get("SPICY_REGS_TEMP_DIR", "")
    if any(c in raw for c in ("'", "\\", "\x00", "\n", "\r")):
        raise RuntimeError(f"SPICY_REGS_TEMP_DIR contains illegal characters: {raw!r}")
    return raw


MEMORY_LIMIT = _resolve_memory_limit()
TEMP_DIR = _resolve_temp_dir()


def _resolve_catalog_config() -> dict[str, str] | None:
    """R2 catalog config from the environment, or None when a credential is missing.

    Values are interpolated into SQL, so illegal characters raise RuntimeError.
    """
    uri = os.environ.get("R2_CATALOG_URI")
    warehouse = os.environ.get("R2_CATALOG_WAREHOUSE")
    token = os.environ.get("R2_CATALOG_TOKEN")
    if not (uri and warehouse and token):
        return None
    config = {
        "uri": uri,
        "warehouse": warehouse,
        "token": token,
        "namespace": os.environ.get("R2_CATALOG_NAMESPACE") or DEFAULT_CATALOG_NAMESPACE,
    }
    for key, value in config.items():
        if any(c in value for c in ("'", "\\", "\x00", "\n", "\r")):
            raise RuntimeError(f"R2 catalog {key} contains illegal characters")
    return config


def _jsonify(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, timedelta):
        return value.total_seconds()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonify(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonify(v) for k, v in value.items()}
    return str(value)


READ_ONLY_STATEMENT_TYPES = frozenset({"SELECT", "EXPLAIN"})


def _first_write_statement(cursor: duckdb.DuckDBPyConnection, sql: str) -> str | None:
    """Name of the first non-read-only statement in ``sql``, else None.

    The sandbox cannot express "no writes" on its own. ``disabled_filesystems``
    is the setting that would, but LocalFileSystem has to stay enabled for
    httpfs to read the CA bundle (see the security-settings notes), so
    ``COPY ... TO``, ``ATTACH``, and ``EXPORT DATABASE`` could all write to the
    container filesystem — on Cloud Run an in-memory one, where a large enough
    write evicts the instance. This is the gate that says no instead.

    Classification comes from DuckDB's own parser rather than a prefix regex,
    so leading comments, string literals, and stacked statements cannot smuggle
    a write past it. ``DESCRIBE``/``SHOW``/``SUMMARIZE``/``VALUES``/``TABLE``
    and the FROM-first shorthand all parse as SELECT, which is why a two-entry
    allowlist still admits every read form.

    Matching on ``StatementType.name`` rather than the enum member keeps this
    working against duckdb's incomplete type stubs, which do not declare the
    members, without a blanket type-ignore over the comparison.

    A ``ParserException`` propagates untouched: malformed SQL should surface
    DuckDB's own message, which names the offending token.
    """
    for statement in cursor.extract_statements(sql):
        name = statement.type.name
        if name not in READ_ONLY_STATEMENT_TYPES:
            return name
    return None


def _apply_security_settings(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET autoinstall_known_extensions=false")
    con.execute("SET autoload_known_extensions=false")
    con.execute("SET allow_unsigned_extensions=false")
    if MEMORY_LIMIT is not None:
        con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
    con.execute(f"SET temp_directory='{TEMP_DIR}'")
    con.execute("SET lock_configuration=true")


def _attach_catalog(con: duckdb.DuckDBPyConnection, config: dict[str, str]) -> bool:
    """Attach the R2 Iceberg catalog under CATALOG_ALIAS; returns False (logged) when the attach fails."""
    try:
        try:
            con.execute("INSTALL avro")
            con.execute("LOAD avro")
        except duckdb.Error as avro_exc:
            logger.info("avro not separately provisioned (%s); using iceberg's bundled path", avro_exc)
        con.execute("INSTALL iceberg")
        con.execute("LOAD iceberg")
        con.execute(f"CREATE OR REPLACE SECRET r2_catalog_secret (TYPE ICEBERG, TOKEN '{config['token']}');")
        con.execute(f"ATTACH '{config['warehouse']}' AS {CATALOG_ALIAS} (TYPE ICEBERG, ENDPOINT '{config['uri']}');")
        return True
    except duckdb.Error as exc:
        logger.warning("R2 catalog attach failed; comments fall back to monolith: %s", exc)
        return False


def _build_connection() -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection with one view per available table, pinned to one publication snapshot.

    Local managed bytes are rehashed before their views are created; a view
    whose schema differs from its admitted generation, or a managed member that
    cannot be read, raises RuntimeError.
    """
    from spicy_regs.sources.publication import load_index, table_location

    local = None
    signatures = {}
    if DATA_DIR is not None:
        from spicy_regs.local_data import local_selection, verify_local_members

        local = local_selection(DATA_DIR)
        signatures = verify_local_members(local)
    publication_index = local.publication if local is not None else load_index(R2_BASE_URL)
    con = duckdb.connect()
    con.execute(f"SET home_directory='{HOME_DIRECTORY.replace(chr(39), chr(39) * 2)}'")
    if DATA_DIR is None:
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")

    catalog = _resolve_catalog_config() if DATA_DIR is None else None
    catalog_attached = catalog is not None and _attach_catalog(con, catalog)

    _apply_security_settings(con)
    # Request cursors share regular in-memory tables, not connection-local
    # temporary tables. Keep the pin alongside the views they actually query.
    con.execute("CREATE TABLE _spicy_publication (snapshot VARCHAR)")
    con.execute("INSERT INTO _spicy_publication VALUES (?)", [json.dumps(publication_index)])
    if local is not None and local.is_download:
        con.execute("CREATE TABLE _spicy_local_selection (snapshot VARCHAR)")
        con.execute(
            "INSERT INTO _spicy_local_selection VALUES (?)",
            [
                json.dumps(
                    {
                        "directory": str(local.directory),
                        "signatures": signatures,
                        "selected_tables": list(local.files),
                    }
                )
            ],
        )
    managed_names = [
        key.removesuffix(".parquet") for e in publication_index["families"].values() for key in e["tables"]
    ]
    selected_names = list(local.files) if local is not None and local.is_download else []
    for name in dict.fromkeys((*TABLES, *managed_names, *selected_names)):
        key, published = table_location(publication_index, f"{name}.parquet")
        if name == "comments" and catalog_attached and published is None:
            namespace = catalog["namespace"]  # type: ignore[index]
            try:
                con.execute(
                    f"CREATE VIEW comments AS "
                    f'SELECT * FROM {CATALOG_ALIAS}."{namespace}"."comments" '
                    f"QUALIFY ROW_NUMBER() OVER "
                    f"(PARTITION BY comment_id ORDER BY modify_date DESC NULLS LAST) = 1"
                )
                continue
            except duckdb.Error as exc:
                logger.warning("comments not available in catalog; falling back to monolith: %s", exc)
        if local is not None:
            if name not in local.files:
                continue
            target = local.files[name][0]
            url = str(target).replace("'", "''")
        else:
            url = f"{R2_BASE_URL}/{key}"
        try:
            con.execute(f"CREATE VIEW \"{name}\" AS SELECT * FROM read_parquet('{url}')")
            if published is not None:
                actual = con.execute(f'DESCRIBE "{name}"').fetchall()
                if [[row[0], row[1]] for row in actual] != published["columns"]:
                    con.close()
                    raise RuntimeError(f"Published schema differs from admitted generation: {name}")
        except duckdb.Error as exc:
            if published is not None or (local is not None and local.is_download):
                con.close()
                raise RuntimeError(f"Published generation member unavailable: {name}") from exc
            logger.warning("table %s not available at %s; skipping view: %s", name, url, exc)
    return con


def _connection_index(cursor: duckdb.DuckDBPyConnection) -> dict:
    try:
        row = cursor.execute("SELECT snapshot FROM _spicy_publication").fetchone()
        if row is None:
            raise RuntimeError("Missing publication snapshot")
        return json.loads(row[0])
    except duckdb.CatalogException:
        # Explicitly injected local connections have no publication admission.
        return {"families": {}}


def _connection_local_selection(cursor: duckdb.DuckDBPyConnection) -> dict | None:
    try:
        row = cursor.execute("SELECT snapshot FROM _spicy_local_selection").fetchone()
        return json.loads(row[0]) if row is not None else None
    except duckdb.CatalogException:
        return None


def _publication_status(cursor: duckdb.DuckDBPyConnection) -> dict:
    """Describe actual availability and pins from this connection's snapshot."""
    index = _connection_index(cursor)
    available = _available_tables(cursor)
    local = _connection_local_selection(cursor) if DATA_DIR is not None else None
    managed = {
        key.removesuffix(".parquet"): {
            "status": "managed_download" if local is not None else "managed_generation",
            "family": family,
            "artifact_digest": entry["artifactDigest"],
            **(
                {
                    "verification": "Local bytes rehashed at connection creation; file changes checked around tool statements."
                }
                if local is not None
                else {}
            ),
        }
        for family, entry in index["families"].items()
        for key in entry["tables"]
    }
    fallback = "local_unversioned" if DATA_DIR is not None else "legacy_unversioned"
    return {
        "tables": available,
        "declared_tables": list(TABLES),
        "publication": {name: managed.get(name, {"status": fallback}) for name in available},
        "verification": (
            "Managed local bytes rehashed at connection creation; file changes checked around tool statements."
            if local is not None
            else "Managed remote bytes verified before publication; this reader pins URLs and checks schemas."
        ),
    }


# Building a connection is the expensive part of a tool call — install httpfs +
# iceberg, attach the R2 catalog over REST, and CREATE VIEW over every table in
# ``TABLES`` (each reads a parquet footer over HTTPS), ~35s on a cold serverless
# instance.
# The query that follows is milliseconds. So we build once and reuse: Fluid
# Compute keeps a warmed instance's module state across invocations, and the
# stdio server is a single long-lived process, so a module-level connection
# amortizes that cost across every request the instance serves.
#
# Concurrency: the cached connection is shared, but each request runs on its own
# `con.cursor()` — DuckDB's supported way to run overlapping queries on one
# connection — so the statement-timeout interrupt (below) hits only that cursor,
# never a sibling request.
#
# Staleness: the views hold parquet footers and the catalog attach pins an
# Iceberg snapshot, both taken at build time. The ETL republishes daily, so a
# connection older than the TTL is rebuilt to pick up new data. On rebuild we
# only drop the module reference to the old connection — never close it — so any
# cursor still mid-query keeps working (a cursor outlives its parent losing its
# last Python reference); the old connection is reclaimed once its cursors drain.
_CONNECTION_TTL_SECONDS = float(os.environ.get("SPICY_REGS_CONNECTION_TTL", "300"))
_connection_lock = threading.Lock()
_cached_connection: duckdb.DuckDBPyConnection | None = None
_cached_connection_at = 0.0


def _get_connection() -> duckdb.DuckDBPyConnection:
    """Return a shared, cached DuckDB connection, rebuilding past the TTL.

    Callers must run their query on ``.cursor()`` of the returned connection,
    not on the connection itself, so concurrent requests don't serialize and a
    per-request timeout interrupt stays scoped to that request.
    """
    global _cached_connection, _cached_connection_at
    with _connection_lock:
        age = _monotonic() - _cached_connection_at
        if _cached_connection is None or age >= _CONNECTION_TTL_SECONDS:
            # Drop (don't close) the previous connection: an in-flight cursor on
            # another thread still references it and must survive this swap.
            _cached_connection = _build_connection()
            _cached_connection_at = _monotonic()
        return _cached_connection


def _reset_connection_cache() -> None:
    """Forget the cached connection so the next call rebuilds. For tests."""
    global _cached_connection, _cached_connection_at
    with _connection_lock:
        _cached_connection = None
        _cached_connection_at = 0.0


@contextmanager
def _statement_timeout(cursor: duckdb.DuckDBPyConnection) -> Iterator[None]:
    """Bound one cursor's statement runtime, raising TimeoutError when its timer trips.

    Local member signatures are re-checked before and after the statement; with
    no configured timeout this is only the signature check.
    """
    local = _connection_local_selection(cursor) if DATA_DIR is not None else None
    if local is not None:
        from spicy_regs.local_data import assert_local_members_unchanged

        assert_local_members_unchanged(local["signatures"])
    if STATEMENT_TIMEOUT_SECONDS is None:
        yield
        if local is not None:
            assert_local_members_unchanged(local["signatures"])
        return
    tripped = threading.Event()

    def _interrupt() -> None:
        tripped.set()
        # Interrupt only this request's cursor, not the shared connection —
        # sibling requests run on their own cursors and must not be cancelled.
        cursor.interrupt()

    timer = threading.Timer(STATEMENT_TIMEOUT_SECONDS, _interrupt)
    timer.start()
    try:
        yield
        if local is not None:
            assert_local_members_unchanged(local["signatures"])
    except duckdb.InterruptException as exc:
        if tripped.is_set():
            raise TimeoutError(f"Query exceeded the {STATEMENT_TIMEOUT} statement timeout") from exc
        raise
    finally:
        timer.cancel()


@lru_cache(maxsize=1)
def _table_metadata() -> dict[str, dict[str, Any]]:
    """Generated dictionary resource; requires neither spicy-docs nor a checkout."""
    return json.loads(files("spicy_regs").joinpath("table_metadata.json").read_text(encoding="utf-8"))


def _available_tables(cursor: duckdb.DuckDBPyConnection) -> list[str]:
    """Tables/views registered in this connection, in declared display order."""
    rows = cursor.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_catalog = current_database()"
    ).fetchall()
    registered = {row[0] for row in rows}
    managed = [
        key.removesuffix(".parquet")
        for entry in _connection_index(cursor)["families"].values()
        for key in entry["tables"]
    ]
    local = _connection_local_selection(cursor) if DATA_DIR is not None else None
    selected = local["selected_tables"] if local is not None else []
    return [name for name in dict.fromkeys((*TABLES, *managed, *selected)) if name in registered]


def _register_tools(mcp: FastMCP) -> None:
    @mcp.tool()
    def list_sources() -> dict[str, Any]:
        """List available tables and distinguish declared outputs without a loaded view.

        Availability reflects the cached connection, rebuilt after its configured
        lifetime. It establishes that a view loaded, not full row validation.
        Use describe_table for meaning, coverage and schema differences.
        """
        cursor = _get_connection().cursor()
        with _statement_timeout(cursor):
            available = _available_tables(cursor)
        metadata = _table_metadata()
        return {
            **_source_details(cursor),
            "tables": available,
            "declared_tables": list(TABLES),
            "unavailable_tables": [name for name in TABLES if name not in available],
            "availability_basis": "Views loaded in the current connection; not a full data or freshness audit.",
            "connection_ttl_seconds": _CONNECTION_TTL_SECONDS,
            "publication": _publication_status(cursor)["publication"],
            "datasets": [
                {"table": name, "label": metadata.get(name, {}).get("label", name), "available": name in available}
                for name in dict.fromkeys((*TABLES, *available))
            ],
        }

    @mcp.tool()
    def describe_table(table: str) -> dict[str, Any]:
        """Return actual columns, field meanings, row identity and coverage caveats.

        Declared columns and coverage metadata describe supported output; they
        do not certify this connection's data population or freshness. An
        unavailable declared table still returns its dictionary description.
        """
        cursor = _get_connection().cursor()
        with _statement_timeout(cursor):
            status = _publication_status(cursor)
        if table not in TABLES and table not in status["tables"]:
            return {
                "error": f"Unknown table '{table}'",
                "declared_tables": list(TABLES),
            }
        entry = _table_metadata().get(table, {"table": table, "columns": []})
        declared = {column["column_name"]: column for column in entry["columns"]}
        with _statement_timeout(cursor):
            available = table in status["tables"]
            rows = cursor.execute(f'DESCRIBE "{table}"').fetchall() if available else []
        actual = {row[0]: row[1] for row in rows}
        differences = (
            {
                "missing_columns": [name for name in declared if name not in actual],
                "unexpected_columns": [name for name in actual if name not in declared],
                "type_differences": [
                    {"column": name, "declared": declared[name]["column_type"], "actual": dtype}
                    for name, dtype in actual.items()
                    if name in declared and dtype != declared[name]["column_type"]
                ],
            }
            if available and declared
            else None
        )
        return {
            "table": table,
            **_source_details(cursor),
            "available": available,
            "publication": status["publication"].get(table, {"status": "unavailable"}),
            "metadata": {key: value for key, value in entry.items() if key not in {"table", "columns"}},
            "metadata_basis": "Dictionary declarations and dated coverage notes; not live population measurements.",
            "declared_columns": entry["columns"],
            "schema_matches_declared": not any(differences.values()) if differences is not None else None,
            "schema_differences": differences,
            "columns": [
                {
                    "column_name": row[0],
                    "column_type": row[1],
                    "null": row[2],
                    "key": row[3],
                    "default": row[4],
                    "description": declared.get(row[0], {}).get("description"),
                }
                for row in rows
            ],
        }

    @mcp.tool()
    def query_sql(sql: str, max_rows: int = 25) -> dict[str, Any]:
        """Run read-only SQL against configured Spicy Regs tables, returning up to max_rows rows.

        Only SELECT and EXPLAIN run; DESCRIBE, SHOW, SUMMARIZE, VALUES and the
        FROM-first shorthand are accepted as SELECT. Statements that write
        (COPY TO, ATTACH, CREATE, INSERT, DROP, EXPORT, SET, ...) are refused.
        The connection reads either R2 or an explicitly configured local directory.
        Local mode never falls back to remote files. One view exists per
        table listed by list_sources. Always include a LIMIT in exploratory
        queries; results past max_rows are dropped.
        """
        if max_rows <= 0 or max_rows > 500:
            return {"error": "max_rows must be between 1 and 500"}

        cursor = _get_connection().cursor()
        write_statement = _first_write_statement(cursor, sql)
        if write_statement is not None:
            return {"error": f"query_sql is read-only; refusing {write_statement} statement"}

        with _statement_timeout(cursor):
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchmany(max_rows)
        result_rows = [{col: _jsonify(val) for col, val in zip(columns, row)} for row in rows]
        return {
            **_source_details(cursor),
            "columns": columns,
            "row_count_shown": len(result_rows),
            "max_rows": max_rows,
            "rows": result_rows,
            "connection_publication": _publication_status(cursor)["publication"],
        }


def build_server() -> FastMCP:
    mcp = FastMCP("spicy-regs", instructions=INSTRUCTIONS, icons=ICONS)
    _register_tools(mcp)
    return mcp


STATIC_DIR = Path(__file__).parent / "static"


@lru_cache(maxsize=1)
def _landing_page() -> bytes:
    """The setup page served at /, with its view list rendered from TABLES.

    The list used to be hand-maintained in the HTML and had drifted seven
    tables behind by the time this page was restored; substituting it here
    keeps the page honest as TABLES grows.
    """
    views = " ·\n        ".join(f'<code class="inline">{table}</code>' for table in TABLES)
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    return html.replace("<!--VIEWS-->", views).encode("utf-8")


@lru_cache(maxsize=1)
def _landing_icon() -> bytes:
    """Decoded from ICON_DATA_URI so the PNG has exactly one home in the package."""
    return base64.b64decode(ICON_DATA_URI.split(",", 1)[1])


def _register_landing_page(mcp: FastMCP) -> None:
    """Serve the human-facing setup page alongside the MCP endpoint.

    Vercel served this as a static file at the site root; when that deploy was
    retired the page went with it, leaving mcp.spicy-regs.dev/ a bare 404. It
    lives in the canonical server now so every host (Cloud Run, the Cloudflare
    container) gets it without host-specific static-file config.
    """

    @mcp.custom_route("/", methods=["GET"])
    async def landing(_request: Request) -> Response:
        return Response(
            _landing_page(),
            media_type="text/html; charset=utf-8",
            headers={"Cache-Control": "public, max-age=300"},
        )

    @mcp.custom_route("/icon.png", methods=["GET"])
    async def icon(_request: Request) -> Response:
        return Response(
            _landing_icon(),
            media_type="image/png",
            headers={"Cache-Control": "public, max-age=86400"},
        )


def build_app():
    """Build the stateless streamable-HTTP ASGI app (tools plus the landing page)."""
    mcp = FastMCP(
        "spicy-regs",
        instructions=INSTRUCTIONS,
        icons=ICONS,
        stateless_http=True,
        streamable_http_path="/mcp",
        transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
    )
    _register_tools(mcp)
    _register_landing_page(mcp)
    return mcp.streamable_http_app()


def main() -> None:
    build_server().run()


if __name__ == "__main__":
    main()
