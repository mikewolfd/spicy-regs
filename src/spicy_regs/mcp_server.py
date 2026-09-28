"""MCP server exposing the published Spicy Regs tables as read-only SQL tools.

The tools (``list_sources``, ``describe_table``, ``query_sql``) run over a
cached DuckDB connection whose views are pinned to one publication snapshot,
reading either the public R2 bucket or an explicitly configured local directory
(``SPICY_REGS_DATA_DIR``). The HTTP app also serves the human setup page at ``/``.
"""

from __future__ import annotations

import base64
import difflib
import json
import logging
import os
import tempfile
import threading
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from functools import lru_cache
from importlib.resources import files
from pathlib import Path
from time import monotonic as _monotonic
from typing import Annotated, Any
from uuid import UUID

import duckdb
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import Icon
from pydantic import Field
from starlette.requests import Request
from starlette.responses import Response

from spicy_regs._icon import ICON_DATA_URI
from spicy_regs.duckdb_settings import memory_limit
from spicy_regs.public_url import resolve_r2_base_url

TABLES = (
    "dockets",
    "documents",
    "comments",
    "comments_index",
    "feed_summary",
    "agency_stats",
    "agency_monthly_volume",
    "fr_docket_links",
    "discovery_signals",
    "cfr_sections",
    "congress_bills",
    "bill_subjects",
    "unified_agenda",
    "federal_register",
    # The materialized rulemaking dataset, read through its own snapshot pointer
    # (publication.SNAPSHOT_POINTER), not the publication index.
    "rule_targets",
    "proceedings",
    "regulatory_agenda_items",
    "agenda_item_proceedings",
    "comment_periods",
    "rulemaking_lifecycles",
    "lifecycle_events",
    "agency_lifecycle_stats",
    "sam_entities",
    "lobbying_filings",
    "lobbying_activities",
    "lobbying_activity_lobbyists",
    "fec_committees",
    "fec_committee_history",
    "document_attributes",
    "docket_attributes",
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
    "bill_cosponsors",
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
    "member_party_affiliations",
    "member_vote_terms",
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
    "native_legal_references",
    "native_legal_reference_reads",
    "court_opinion_pdf_extractions",
)
STATEMENT_TIMEOUT = os.environ.get("SPICY_REGS_STATEMENT_TIMEOUT", "790s")

logger = logging.getLogger(__name__)

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
    "not establish publication or freshness. qualification reports the output "
    "ledger's audit disposition for its own pin beside the live pin; it is not a "
    "verification flag for the live generation. Always LIMIT exploratory results. "
    "Cite source identifiers, evidence locators and dates from returned rows. "
    "Derived relationship views retain source occurrences separately from distinct pairs. "
    "Use resolve_document_citations for bounded target lookups; a normalized citation key alone does not prove existence."
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
    """DuckDB home directory from SPICY_REGS_HOME_DIR (default system temp); rejects control characters."""
    raw = os.environ.get("SPICY_REGS_HOME_DIR", tempfile.gettempdir())
    if any(c in raw for c in ("\x00", "\n", "\r")):
        raise RuntimeError(f"SPICY_REGS_HOME_DIR contains illegal characters: {raw!r}")
    return raw


HOME_DIRECTORY = _resolve_home_directory()


def _resolve_memory_limit() -> str | None:
    """DuckDB memory ceiling from SPICY_REGS_MEMORY_LIMIT (e.g. '12GB', '16GiB').

    Unset => None => DuckDB's own default (~80% of detected RAM). Set it on hosts
    where DuckDB can't see the real allocation (containers detect host RAM, not
    the cgroup limit) so it spills/errors before the platform OOM-kills the process.
    Interpolated into a SET, so the value is format-validated.
    """
    raw = os.environ.get("SPICY_REGS_MEMORY_LIMIT", "").strip()
    if not raw:
        return None
    return memory_limit(raw, "SPICY_REGS_MEMORY_LIMIT")


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
    """Convert a DuckDB result value (dates, Decimal, UUID, bytes, containers) to a JSON-safe value."""
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


READ_ONLY_STATEMENT_TYPES = frozenset({"SELECT"})


def _first_write_statement(cursor: duckdb.DuckDBPyConnection, sql: str) -> str | None:
    """Name of the first non-read-only statement in ``sql``, else None.

    Native file permissions constrain reads to selected dataset files; this
    additional gate prevents writes to those files and changes to the database
    or configuration. LocalFileSystem remains enabled for httpfs and spill.

    Classification comes from DuckDB's own parser rather than a prefix regex,
    so leading comments, string literals, and stacked statements cannot smuggle
    a write past it. ``DESCRIBE``/``SHOW``/``SUMMARIZE``/``VALUES``/``TABLE``
    and the FROM-first shorthand all parse as SELECT. EXPLAIN is refused because
    EXPLAIN ANALYZE executes its inner statement, including writes, and the
    exposed parser result does not identify that inner statement's type.

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


def _tables_named(cursor: duckdb.DuckDBPyConnection, sql: str) -> set[str]:
    """Table names ``sql`` references, from DuckDB's parse tree; CTE names included, views not expanded.

    ``cursor.get_table_names`` binds the query and expands each view to the
    tables under it, which for these ``read_parquet`` views is none, so the
    unbound tree from ``json_serialize_sql`` is walked instead. Callers
    intersect the result with the published tables, which drops CTE names.
    """
    [(serialized,)] = cursor.execute("SELECT json_serialize_sql(?)", [sql]).fetchall()
    tree = json.loads(serialized)
    names: set[str] = set()
    stack: list[Any] = [tree]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            if node.get("type") == "BASE_TABLE":
                names.add(node["table_name"])
            stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)
    return names


def _apply_security_settings(con: duckdb.DuckDBPyConnection, allowed_paths: list[str] | None = None) -> None:
    """Restrict file reads to the selected dataset members, then lock the session settings."""
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET autoinstall_known_extensions=false")
    con.execute("SET autoload_known_extensions=false")
    con.execute("SET allow_unsigned_extensions=false")
    if con.execute("SELECT current_setting('allow_persistent_secrets')").fetchone() != (False,):
        con.execute("SET allow_persistent_secrets=false")
    con.execute("SET allowed_paths=?", [allowed_paths or []])
    if MEMORY_LIMIT is not None:
        con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
    con.execute(f"SET temp_directory='{TEMP_DIR}'")
    con.execute("SET enable_external_access=false")
    con.execute("SET lock_configuration=true")


def _build_connection() -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection with one view per available table, pinned to one publication snapshot.

    A remote connection reads the publication index and the rulemaking pointer
    once each; a rulemaking table's view reads the snapshot the pointer names.
    Local managed bytes are rehashed before their views are created; a view
    whose schema differs from its admitted generation, or a managed or snapshot
    member that cannot be read, raises RuntimeError.
    """
    from spicy_regs.sources.publication import (
        load_index,
        load_rulemaking_snapshot,
        parquet_scan,
        table_descriptor,
        table_members,
    )

    if DATA_DIR is None and _resolve_catalog_config() is not None:
        raise RuntimeError(
            "MCP catalog reads require dynamic file access, which this restricted SQL server refuses; "
            "serve published Parquet or SPICY_REGS_DATA_DIR instead"
        )
    local = None
    signatures = {}
    if DATA_DIR is not None:
        from spicy_regs.local_data import local_selection, verify_local_members

        local = local_selection(DATA_DIR)
        signatures = verify_local_members(local)
    publication_index = local.publication if local is not None else load_index(R2_BASE_URL)
    rulemaking = None if local is not None else load_rulemaking_snapshot(R2_BASE_URL)
    con = duckdb.connect()
    con.execute("SET allow_persistent_secrets=false")
    con.execute(f"SET home_directory='{HOME_DIRECTORY.replace(chr(39), chr(39) * 2)}'")
    if DATA_DIR is None:
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")

    allowed_paths: list[str] = []
    # Request cursors share regular in-memory tables, not connection-local
    # temporary tables. Keep the pin alongside the views they actually query.
    con.execute("CREATE TABLE _spicy_publication (snapshot VARCHAR)")
    con.execute("INSERT INTO _spicy_publication VALUES (?)", [json.dumps(publication_index)])
    if rulemaking is not None:
        con.execute("CREATE TABLE _spicy_rulemaking (snapshot VARCHAR)")
        con.execute("INSERT INTO _spicy_rulemaking VALUES (?)", [json.dumps(rulemaking)])
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
    snapshot_names = [key.removesuffix(".parquet") for key in (rulemaking or {"tables": {}})["tables"]]
    selected_names = list(local.files) if local is not None and local.is_download else []
    for name in dict.fromkeys((*TABLES, *managed_names, *snapshot_names, *selected_names)):
        published = table_descriptor(publication_index, f"{name}.parquet")
        paths = [member.path for member in table_members(publication_index, f"{name}.parquet")]
        pinned = rulemaking["tables"].get(f"{name}.parquet") if rulemaking is not None and published is None else None
        if pinned is not None:
            paths = [pinned["remote_key"]]
        if local is not None:
            if name not in local.files:
                continue
            urls = [str(path) for path in local.paths(name)]
        else:
            urls = [f"{R2_BASE_URL}/{path}" for path in paths]
        try:
            con.execute(f'CREATE VIEW "{name}" AS SELECT * FROM {parquet_scan(urls)}')
            if published is not None:
                actual = con.execute(f'DESCRIBE "{name}"').fetchall()
                if [[row[0], row[1]] for row in actual] != published["columns"]:
                    con.close()
                    raise RuntimeError(f"Published schema differs from admitted generation: {name}")
            allowed_paths.extend(urls)
        except duckdb.Error as exc:
            if published is not None or pinned is not None or (local is not None and local.is_download):
                con.close()
                raise RuntimeError(f"Published generation member unavailable: {name}") from exc
            logger.warning("table %s not available at %s; skipping view: %s", name, urls, exc)
    _install_relationship_views(con)
    _apply_security_settings(con, allowed_paths)
    return con


@lru_cache(maxsize=16)
def _parsed_pin(raw: str) -> dict:
    """A pinned JSON record, parsed once per distinct document, so once per connection rather than per helper call.

    One tool call reads the same pins in several helpers; the index alone is tens of kilobytes. Callers must not
    mutate the shared result.
    """
    return json.loads(raw)


def _connection_index(cursor: duckdb.DuckDBPyConnection) -> dict:
    """The publication snapshot pinned in this connection; injected local connections report an empty family map."""
    try:
        row = cursor.execute("SELECT snapshot FROM _spicy_publication").fetchone()
        if row is None:
            raise RuntimeError("Missing publication snapshot")
        return _parsed_pin(row[0])
    except duckdb.CatalogException:
        # Explicitly injected local connections have no publication admission.
        return {"families": {}}


def _pinned_record(cursor: duckdb.DuckDBPyConnection, table: str) -> dict | None:
    """The JSON record this connection pinned in ``table`` when it was built, or None when it pinned none."""
    try:
        row = cursor.execute(f"SELECT snapshot FROM {table}").fetchone()
        return _parsed_pin(row[0]) if row is not None else None
    except duckdb.CatalogException:
        return None


def _connection_local_selection(cursor: duckdb.DuckDBPyConnection) -> dict | None:
    """The local-selection snapshot pinned in this connection, or None when it has none."""
    return _pinned_record(cursor, "_spicy_local_selection")


def _connection_rulemaking(cursor: duckdb.DuckDBPyConnection) -> dict:
    """The rulemaking snapshot pinned in this connection; empty when it read none (local, injected or unpublished)."""
    return _pinned_record(cursor, "_spicy_rulemaking") or {"snapshot_id": None, "tables": {}}


def _connection_relationships(cursor: duckdb.DuckDBPyConnection) -> dict:
    """Definitions and dependency availability bound alongside this connection's source views."""
    return _pinned_record(cursor, "_spicy_relationships") or {}


def _install_relationship_views(con: duckdb.DuckDBPyConnection) -> None:
    """Bind trusted SQL definitions before locking the connection; no source row scan."""
    from spicy_regs.relationship_views import install_relationship_views

    status = _publication_status(con)
    relationships = install_relationship_views(con, status["tables"], publication=status["publication"])
    con.execute("CREATE TABLE _spicy_relationships (snapshot VARCHAR)")
    con.execute("INSERT INTO _spicy_relationships VALUES (?)", [json.dumps(relationships)])


def _publication_status(cursor: duckdb.DuckDBPyConnection) -> dict:
    """Describe actual availability and pins from this connection's snapshot."""
    index = _connection_index(cursor)
    rulemaking = _connection_rulemaking(cursor)
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
    snapshot = {
        key.removesuffix(".parquet"): {
            "status": "rulemaking_snapshot",
            "snapshot_id": rulemaking["snapshot_id"],
            "sha256": f"sha256:{record['sha256']}",
        }
        for key, record in rulemaking["tables"].items()
    }
    fallback = "local_unversioned" if DATA_DIR is not None else "legacy_unversioned"
    derived = {
        name: {
            "status": "derived_view",
            "rule_version": value["metadata"]["rule_version"],
            "dependencies": value["dependencies"],
            "input_publications": value["metadata"]["input_publications"],
        }
        for name, value in _connection_relationships(cursor).items() if value["status"] == "available"
    }
    return {
        "tables": available,
        "declared_tables": list(TABLES),
        "publication": {
            name: derived.get(name) or managed.get(name) or snapshot.get(name, {"status": fallback})
            for name in available
        },
        "verification": (
            "Managed local bytes rehashed at connection creation; file changes checked around tool statements."
            if local is not None
            else "Managed remote bytes verified before publication; this reader pins URLs and checks schemas."
            + (
                " Rulemaking snapshot URLs are pinned by the pointer read at connection creation; their manifest"
                " states no columns, so describe_table compares their schemas with the dictionary."
                if snapshot
                else ""
            )
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
# Staleness: the publication index and the rulemaking pointer are each read
# once per build, the views hold parquet footers at the immutable URLs they
# name, and the catalog attach pins an Iceberg snapshot, all taken at build
# time. The ETL republishes daily, so a connection older than the TTL is
# rebuilt to pick up new data, a moved pointer included. On rebuild we
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


QUALIFICATION_BASIS = (
    "The output ledger's audits, bundled when the dictionary was generated. ledger_disposition is the "
    "ledger's word for ledger_pin only; generation compares that pin with this connection's live pin. "
    "'newer generation, not yet audited' means the live pin differs from every pin the ledger records "
    "for the table. A disposition covers the scope its ledger statement names; it does not verify "
    "relationships that metadata.data_quality calls heuristic or unresolved."
)


def _ledger_index(record: dict) -> tuple[dict, dict[str, list[dict]]]:
    """A qualification record and its ledger rows by table."""
    rows: dict[str, list[dict]] = {}
    for row in record["rows"]:
        for table in row["tables"]:
            rows.setdefault(table, []).append(row)
    return record, rows


@lru_cache(maxsize=1)
def _ledger() -> tuple[dict, dict[str, list[dict]]]:
    """The bundled qualification record (``output_ledger``), read and indexed once per process."""
    return _ledger_index(json.loads(files("spicy_regs").joinpath("table_qualification.json").read_text("utf-8")))


JOINS_BASIS = (
    "Declared cross-table joins, bundled when the dictionary was generated. baseline_keys and "
    "baseline_missing count distinct non-null child keys and those absent from the parent on the baseline "
    "date; floor_pct is the resolution rate scripts/check_table_joins.py holds the live tables to. A "
    "'scope' or 'design' join resolves partly for the stated reason; it is not a defect. "
    "This is not an exhaustive relationship catalog: JSON-array joins and other undeclared relationships "
    "may be described in the column meanings. An empty join list does not establish that no relationship exists."
)


@lru_cache(maxsize=1)
def _joins() -> dict:
    """The bundled join declarations (``table_joins``), read once per process."""
    return json.loads(files("spicy_regs").joinpath("table_joins.json").read_text("utf-8"))


def _table_joins(table: str) -> dict:
    """The declared joins where ``table`` is the child or the parent, with their baseline."""
    record = _joins()
    return {
        "basis": JOINS_BASIS,
        "baseline": record["baseline"],
        "outgoing": [join for join in record["joins"] if join["child"] == table],
        "incoming": [join for join in record["joins"] if join["parent"] == table],
    }


def _qualification(
    cursor: duckdb.DuckDBPyConnection, tables: list[str], *, statements: bool
) -> tuple[dict, dict[str, dict] | None]:
    """The ledger's scope fields and each table's audit beside its live pin; no tables for another publisher.

    Live pins come from this connection's publication snapshot: the family
    artifact digest, or the table digest for a base object's ``table`` pin,
    and for a rulemaking table the ``snapshot`` its pointer named. A table
    neither pointer names has no live pin to compare.
    """
    record, rows = _ledger()
    scope = {"ledger": record["ledger"], "ledger_destination": record["destination"], "basis": QUALIFICATION_BASIS}
    if DATA_DIR is not None or R2_BASE_URL != record["destination"]:
        reads = str(DATA_DIR) if DATA_DIR is not None else R2_BASE_URL
        reason = f"This server reads {reads}; the ledger records audits only for {record['destination']}."
        return {**scope, "status": "unknown_for_publisher", "reason": reason}, None
    # A split table has no single table digest, so only its artifact pin can match an audit.
    live = {
        key.removesuffix(".parquet"): {"artifact": entry["artifactDigest"][7:15],
                                       **({"table": table["sha256"][7:15]} if "sha256" in table else {})}
        for entry in _connection_index(cursor)["families"].values()
        for key, table in entry["tables"].items()
    }
    rulemaking = _connection_rulemaking(cursor)
    for key in rulemaking["tables"]:
        live.setdefault(key.removesuffix(".parquet"), {"snapshot": rulemaking["snapshot_id"][: len("snapshot_") + 8]})
    return scope, {name: _table_qualification(rows.get(name, []), live.get(name, {}), statements) for name in tables}


def _table_qualification(rows: list[dict], live: dict[str, str], statements: bool) -> dict:
    """The audit matching the live pin, else the ledger's latest, as separate fields; never one verified flag."""
    audits = [audit for row in rows for audit in row["audits"]]
    result: dict[str, Any]
    if not audits:
        result = {
            "status": "no_audit_recorded" if rows else "not_in_ledger",
            "live_pin": live.get("artifact", live.get("snapshot")),
        }
    else:
        matched = [audit for audit in audits if live.get(audit["pin_kind"]) == audit["pin"]]
        audit = max(matched or audits, key=lambda item: item["date"])
        live_pin = live.get(audit["pin_kind"])
        if matched:
            generation = "current generation audited"
        elif live_pin:
            generation = "newer generation, not yet audited"
        else:
            generation = "live generation not pinned by the publication index; cannot compare"
        result = {
            "status": "recorded",
            "generation": generation,
            "live_pin": live_pin,
            "ledger_pin": audit["pin"],
            "pin_kind": audit["pin_kind"],
            "ledger_date": audit["date"],
            "ledger_disposition": audit["disposition"],
        }
    if statements and rows:
        result["ledger_tasks"] = list(dict.fromkeys(row["task"] for row in rows))
        result["ledger_statements"] = [row["statement"] for row in rows]
    return result


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
    snapshot = [key.removesuffix(".parquet") for key in _connection_rulemaking(cursor)["tables"]]
    local = _connection_local_selection(cursor) if DATA_DIR is not None else None
    selected = local["selected_tables"] if local is not None else []
    relationships = _connection_relationships(cursor)
    return [
        name for name in dict.fromkeys((*TABLES, *managed, *snapshot, *selected, *relationships)) if name in registered
    ]


def _register_tools(mcp: FastMCP) -> None:
    @mcp.tool()
    def lookup_agency(
        namespace: Annotated[str, Field(min_length=1, max_length=256)],
        identifier: Annotated[str, Field(min_length=1, max_length=256)],
        on_date: Annotated[str | None, Field(pattern=r"^\d{4}-\d{2}-\d{2}$")] = None,
    ) -> dict[str, Any]:
        """Look up an exact agency identifier in the pinned reviewed RefSpec mapping.

        Namespaces are regulations.gov:agency (e.g. OPM) and
        federal_register_agency (e.g. 406). Labels are not identifiers.
        Return mapping evidence, publication pins and documented abstentions.
        Parent relationships and succession events are not identity. REF-072 bridge
        and event evidence stays separate from REF-038 candidates. Current-lineage
        lookup follows the owner policy without dates; on_date preserves evidence
        but cannot establish historical identity.
        No acquisition, new adjudication, or money attribution is performed.
        """
        from spicy_regs.vocabulary_mapping import lookup_agency as lookup

        return lookup(namespace, identifier, on_date=on_date)

    @mcp.tool()
    def list_sources() -> dict[str, Any]:
        """List the queryable tables: each one's label and coverage kind, with derived views grouped.

        coverage is the dictionary's kind: true_range, window, sampled or
        derived; a window or a sample does not hold the source's full history.
        A listed table loaded in this connection; that is not a data or
        freshness audit. Call describe_table before querying a table: it gives
        columns, coverage caveats, joins, the live data version and the output
        ledger's audit.
        """
        cursor = _get_connection().cursor()
        with _statement_timeout(cursor):
            available = _available_tables(cursor)
        metadata = _table_metadata()
        relationships = _connection_relationships(cursor)
        # A relationship family's occurrence, pair and field-state views share one summary; list it once.
        views: dict[str, list[str]] = {}
        for name, entry in relationships.items():
            if name in available:
                views.setdefault(entry["metadata"]["summary"], []).append(name)
        return {
            **_source_details(cursor),
            "tables": [
                {"table": name, "label": metadata.get(name, {}).get("label"), "coverage": metadata.get(name, {}).get("kind")}
                for name in available if name not in relationships
            ],
            "relationship_views": [{"views": names, "summary": summary} for summary, names in views.items()],
            "unavailable_tables": [name for name in (*TABLES, *relationships) if name not in available],
        }

    @mcp.tool()
    def describe_table(table: str) -> dict[str, Any]:
        """Return a table's columns with their meanings, row identity, coverage caveats and joins.

        Coverage metadata describes supported output; it does not certify this
        connection's data population or freshness. columns are the loaded
        view's, each with its dictionary meaning; an unavailable declared table
        returns its declared columns. schema_differences names any column or
        type the view does not share with the dictionary. publication is the
        live data version. qualification gives the live pin, the output
        ledger's audited pin, date and disposition, whether they match, and
        the ledger's own statement, as separate fields; it is reported only for
        the ledger's publisher. joins lists the declared joins this table makes
        (outgoing) and receives (incoming), each with its measured baseline.
        """
        cursor = _get_connection().cursor()
        with _statement_timeout(cursor):
            status = _publication_status(cursor)
        relationships = _connection_relationships(cursor)
        if table not in TABLES and table not in status["tables"] and table not in relationships:
            known = list(dict.fromkeys((*TABLES, *status["tables"], *relationships)))
            close = difflib.get_close_matches(table, known, n=5, cutoff=0.6)
            hint = f" Close names: {', '.join(close)}." if close else ""
            raise ValueError(f"Unknown table '{table}'.{hint} list_sources lists every table.")
        with _statement_timeout(cursor):
            available = table in status["tables"]
            rows = cursor.execute(f'DESCRIBE "{table}"').fetchall() if available else []
        entry: dict[str, Any]
        if table in relationships:
            from spicy_regs.relationship_views import view_columns

            # A derived view declares its bound schema; it is described here, not at connection build.
            entry = {"table": table, **relationships[table]["metadata"], "columns": view_columns(rows)}
        else:
            entry = _table_metadata().get(table, {"table": table, "columns": []})
        declared = {column["column_name"]: column for column in entry["columns"]}
        actual = {row[0]: row[1] for row in rows}
        scope, qualified = _qualification(cursor, [table], statements=True)
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
            **({"relationship": relationships[table]} if table in relationships else {}),
            "publication": status["publication"].get(table, {"status": "unavailable"}),
            "qualification": scope if qualified is None else {**scope, **qualified[table]},
            "joins": _table_joins(table),
            "metadata": {key: value for key, value in entry.items() if key not in {"table", "columns"}},
            "metadata_basis": "Dictionary declarations and dated coverage notes; not live population measurements.",
            "schema_matches_declared": not any(differences.values()) if differences is not None else None,
            "schema_differences": differences,
            "columns": [
                {"column_name": name, "column_type": dtype, "description": declared.get(name, {}).get("description")}
                for name, dtype in actual.items()
            ] if available else entry["columns"],
        }

    @mcp.tool()
    def query_sql(sql: str, max_rows: Annotated[int, Field(ge=1, le=500)] = 25) -> dict[str, Any]:
        """Run read-only SQL against configured Spicy Regs tables, returning up to max_rows rows.

        Only SELECT runs; DESCRIBE, SHOW, SUMMARIZE, VALUES and the
        FROM-first shorthand are accepted as SELECT. Statements that write
        (COPY TO, ATTACH, CREATE, INSERT, DROP, EXPORT, SET, ...) are refused.
        EXPLAIN is refused because its ANALYZE form can execute writes.
        The connection reads either R2 or an explicitly configured local directory.
        Local mode never falls back to remote files. One view exists per
        table listed by list_sources. Always include a LIMIT in exploratory
        queries. truncated reports whether rows beyond max_rows were omitted.
        Selected columns must have unique names; alias shared names in joins.
        publication gives the live data version of each table the query names.
        """
        cursor = _get_connection().cursor()
        write_statement = _first_write_statement(cursor, sql)
        if write_statement is not None:
            raise ValueError(f"query_sql is read-only; refusing {write_statement} statement")

        with _statement_timeout(cursor):
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            duplicates = [name for name, count in Counter(columns).items() if count > 1]
            if duplicates:
                raise ValueError(f"Duplicate result column names: {duplicates}; use AS aliases to give each a unique name")
            rows = cursor.fetchmany(max_rows + 1)
            publication = _publication_status(cursor)["publication"]
            named = _tables_named(cursor, sql)
        result_rows = [{col: _jsonify(val) for col, val in zip(columns, row)} for row in rows[:max_rows]]
        return {
            **_source_details(cursor),
            "columns": columns,
            "row_count_shown": len(result_rows),
            "max_rows": max_rows,
            "truncated": len(rows) > max_rows,
            "rows": result_rows,
            "publication": {name: pin for name, pin in publication.items() if name in named},
        }

    @mcp.tool()
    def resolve_document_citations(
        document_kind: str,
        document_key: str,
        max_occurrences: Annotated[int, Field(ge=1, le=500)] = 100,
    ) -> dict[str, Any]:
        """Resolve a bounded document's held citations against this connection's selected targets.

        Findings retain their spelling, text digest and extraction rule. Target
        lookup does not validate extraction precision or legal applicability.
        Missing, ambiguous, unsupported, unread and stale results stay explicit.
        A capped response sets truncated; it does not establish whole-document coverage.
        acquisition_queue plans qualified missing targets for retained-evidence
        inspection. It performs no acquisition or publication.
        Held-field kinds bill_section, report_section and lobbying_activity use
        compact JSON-list keys in their source table's full key order; comment_inline
        uses the literal comment_id. These scopes cover only the selected field.
        """
        from spicy_regs.acquisition_queue import build_missing_target_queue
        from spicy_regs.citation_resolution import SOURCE_TABLES, resolve_citations
        from spicy_regs.citation_sources import TEXT_SOURCES, source_digests as held_source_digests

        cursor = _get_connection().cursor()
        with _statement_timeout(cursor):
            status = _publication_status(cursor)
            if "document_citations" not in status["tables"]:
                raise ValueError("document_citations is not available in this connection")
            cursor.execute(
                "SELECT * FROM document_citations WHERE document_kind = ? AND document_key = ? "
                "ORDER BY cite_kind, span_start, rule_version LIMIT ?",
                [document_kind, document_key, max_occurrences + 1],
            )
            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()
            # This tool reads text findings. A legacy file without the digest
            # column must not fall through the resolver's native-field API.
            occurrences = [
                {"text_sha256": None, **dict(zip(columns, row, strict=True))}
                for row in rows[:max_occurrences]
            ]
            source_digests = {}
            parent = SOURCE_TABLES.get(document_kind)
            source_read = {"table": parent, "status": "unavailable" if parent else "unsupported"}
            if parent in status["tables"]:
                try:
                    if document_kind in TEXT_SOURCES:
                        values = held_source_digests(cursor, document_kind, document_key)
                    else:
                        values = cursor.execute(
                            f'SELECT DISTINCT text_sha256 FROM "{parent}" WHERE package_id = ? LIMIT 2',
                            [document_key],
                        ).fetchall()
                except duckdb.InterruptException:
                    raise
                except duckdb.Error as error:
                    source_read.update(status="read_failure", error_type=type(error).__name__)
                else:
                    source_read["status"] = "ambiguous" if len(values) > 1 else "missing_digest"
                    if len(values) == 1 and values[0][0]:
                        source_digests[(document_kind, document_key)] = values[0][0]
                        source_read["status"] = "read"
            result = resolve_citations(
                cursor, occurrences, status["publication"], source_digests=source_digests,
                max_target_keys=max_occurrences,
            )
            capped = len(rows) > max_occurrences
            result["coverage"]["occurrence_selection"] = {
                "status": "capped" if capped else "complete_held_selection",
                "max_occurrences": max_occurrences,
                "meaning": "Held citation rows for this document; not extraction recall or source completeness.",
            }
            if capped:
                result["coverage"]["partial"] = True
        return {
            **_source_details(cursor), **_jsonify(result),
            "document_kind": document_kind, "document_key": document_key,
            "max_occurrences": max_occurrences, "truncated": len(rows) > max_occurrences,
            "source_read": source_read,
            "acquisition_queue": build_missing_target_queue(
                result, input_snapshots={document_kind: status["publication"].get(parent, {})},
                intended_query="Resolve the cited target for this held document", max_items=max_occurrences,
            ),
            # Each resolved target carries its own target_snapshot; this names the tables the lookup read.
            "publication": {
                name: pin for name, pin in status["publication"].items() if name in {"document_citations", parent}
            },
        }


def build_server() -> FastMCP:
    """Build the stdio FastMCP server with discovery, read-only queries and citation lookup."""
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
