"""Tests for the data dictionary generator/checker.

Hermetic (no network): everything here runs against the in-code schema
(``RECORD_TYPES`` + ``DERIVED_SCHEMAS``) and the committed
``data_dictionary/descriptions.yaml``. The live-data reconciliation
(``check --source r2``) is exercised in the docs deploy workflow, not here.
"""

from __future__ import annotations

import json
from datetime import date

import pytest

from spicy_regs import data_dictionary as dd
from spicy_regs.schemas.regulations import RECORD_TYPES


def test_record_types_covered_by_expected_schemas():
    """Every core RecordType column flows into the expected schema."""
    expected = dd.expected_schemas()
    for name, rt in RECORD_TYPES.items():
        assert name in expected
        assert [c for c, _ in expected[name]] == list(rt.schema.keys())


def test_all_tables_have_a_schema():
    expected = dd.expected_schemas()
    assert set(expected) == set(dd.TABLES)
    for cols in expected.values():
        assert cols, "schema should be non-empty"


def test_descriptions_match_schema():
    """The shipped descriptions.yaml must fully cover the in-code schema."""
    descriptions = dd.load_descriptions()
    errors = dd.check_descriptions(dd.expected_schemas(), descriptions)
    assert errors == [], "\n".join(errors)


def test_check_detects_missing_description():
    descriptions = dd.load_descriptions()
    # Drop a real column's description -> should be flagged.
    broken = {t: {**v, "columns": dict(v.get("columns") or {})} for t, v in descriptions.items()}
    broken["dockets"]["columns"].pop("rin")
    errors = dd.check_descriptions(dd.expected_schemas(), broken)
    assert any("rin" in e for e in errors)


def test_check_detects_orphan_description():
    descriptions = dd.load_descriptions()
    broken = {t: {**v, "columns": dict(v.get("columns") or {})} for t, v in descriptions.items()}
    broken["dockets"]["columns"]["not_a_real_column"] = "x"
    errors = dd.check_descriptions(dd.expected_schemas(), broken)
    assert any("not_a_real_column" in e for e in errors)


def test_check_detects_missing_summary():
    descriptions = dd.load_descriptions()
    broken = {t: {**v} for t, v in descriptions.items()}
    broken["comments"] = {**broken["comments"], "summary": ""}
    errors = dd.check_descriptions(dd.expected_schemas(), broken)
    assert any("summary" in e for e in errors)


def test_generate_writes_a_page_per_table(tmp_path):
    descriptions = dd.load_descriptions()
    written = dd.generate(descriptions, dd.expected_schemas(), out_dir=tmp_path)
    assert {p.stem for p in written} == set(dd.TABLES)
    dockets = (tmp_path / "dockets.md").read_text(encoding="utf-8")
    assert "# `dockets`" in dockets
    assert "`docket_id`" in dockets
    # The primary key is flagged.
    assert "🔑" in dockets


def test_committed_pages_are_up_to_date(tmp_path):
    """The committed docs/tables/*.md must equal a fresh generation."""
    descriptions = dd.load_descriptions()
    dd.generate(descriptions, dd.expected_schemas(), out_dir=tmp_path)
    for table in dd.TABLES:
        committed = dd.DEFAULT_DOCS_TABLES_DIR / f"{table}.md"
        assert committed.exists(), f"missing committed page for {table}"
        fresh = (tmp_path / f"{table}.md").read_text(encoding="utf-8")
        assert committed.read_text(encoding="utf-8") == fresh, (
            f"{table}.md is stale; run 'uv run spicy-regs-dict generate'"
        )


def test_mcp_queryable_subset_of_tables():
    assert dd.MCP_QUERYABLE <= set(dd.TABLES)


def test_mcp_server_tables_match_dictionary():
    """The MCP server must expose exactly the dictionary's published tables."""
    from spicy_regs import mcp_server

    assert set(mcp_server.TABLES) == set(dd.TABLES)


def test_mcp_queryable_matches_mcp_server():
    """The docs' 'queryable via MCP' flag must track what the server serves."""
    from spicy_regs import mcp_server

    assert dd.MCP_QUERYABLE == set(mcp_server.TABLES)


def test_every_table_has_a_display_label():
    """The catalog names a class by this label, so no table may go unnamed."""
    descriptions = dd.load_descriptions()
    labels = dd.table_labels(descriptions)
    assert set(labels) == set(dd.TABLES)
    unnamed = [t for t, label in labels.items() if not label.strip()]
    assert not unnamed, f"tables with no display label: {unnamed}"


def test_check_detects_a_missing_label():
    descriptions = dd.load_descriptions()
    broken = {t: dict(entry) for t, entry in descriptions.items()}
    broken["congress_bills"].pop("label")
    errors = dd.check_descriptions(dd.expected_schemas(), broken)
    assert any("missing a 'label'" in e for e in errors)


def test_generated_page_carries_the_label():
    descriptions = dd.load_descriptions()
    page = dd.DEFAULT_DOCS_TABLES_DIR / "congress_bills.md"
    assert f"**{descriptions['congress_bills']['label']}**" in page.read_text()


def test_every_table_has_a_coverage_statement():
    """The boundary sentence a user reads comes from here, so none may be blank."""
    descriptions = dd.load_descriptions()
    coverage = dd.table_coverage(descriptions)
    assert set(coverage) == set(dd.TABLES)
    missing = [t for t, e in coverage.items() if not e["coverage"].strip()]
    assert not missing, f"tables with no coverage statement: {missing}"


def test_every_coverage_statement_names_its_kind():
    """A range, a window and a derived table are different claims; each says which."""
    bad = [t for t, e in dd.table_coverage(dd.load_descriptions()).items() if dd.coverage_kind(e["coverage"]) is None]
    assert not bad, f"coverage statements that do not open by naming their kind: {bad}"


def test_kind_is_readable_without_parsing_the_sentence():
    """The consumer branches on `kind`, never on the prose's first token.

    The prose opens with the kind followed by a period or a comma depending on
    the phrasing, so a whitespace split returns "Derived." or "Derived," and
    matches neither. This asserts the consumer's access pattern rather than the
    author's, which is why the earlier startswith test passed while a consumer
    would have failed.
    """
    document = dd.build_catalog(dd.load_descriptions(), dd.expected_schemas())
    for entry in document["classes"]:
        assert entry["kind"] in set(dd.COVERAGE_KINDS.values()), entry["table"]
        assert dd.coverage_kind(entry["coverage"]) == entry["kind"], entry["table"]
    assert {e["kind"] for e in document["classes"]} == set(dd.COVERAGE_KINDS.values())


def test_check_detects_a_missing_coverage_statement():
    descriptions = dd.load_descriptions()
    broken = {t: dict(entry) for t, entry in descriptions.items()}
    broken["gao_reports"].pop("coverage")
    errors = dd.check_descriptions(dd.expected_schemas(), broken)
    assert any("missing a 'coverage'" in e for e in errors)


def test_check_detects_an_empty_data_quality_note():
    descriptions = dd.load_descriptions()
    broken = {t: dict(entry) for t, entry in descriptions.items()}
    broken["documents"]["data_quality"] = "   "
    errors = dd.check_descriptions(dd.expected_schemas(), broken)
    assert any("empty 'data_quality'" in e for e in errors)


@pytest.mark.parametrize(
    ("prose", "refusal"),
    [
        ("Derived. `snapshot_91b19da7…` holds the periods.", "a snapshot id"),
        ("Window. Published as generation 9c87b600aa7b5847.", "a digest or generation pin"),
        ("Derived. Built from members generation `017366cc…`.", "a digest or generation pin"),
        ("Sampled. Local output only; not uploaded.", "a publication-status claim"),
        ("Window. The candidate is not yet published.", "a publication-status claim"),
        ("Window. Measured 319,501 rows on the candidate.", "a row count"),
        ("True range. 1,009,005 dated records published 1994 on.", "a row count"),
        ("Sampled. 13,717,161 provider records in selected collections.", "a row count"),
    ],
)
def test_coverage_prose_refuses_facts_that_decay_with_each_publication(prose, refusal):
    """Counts, snapshots, pins and publication claims go stale at the next publish; the server states them live."""
    errors = dd.coverage_prose_errors("gao_reports", prose)
    assert len(errors) == 1 and refusal in errors[0], errors


@pytest.mark.parametrize(
    "prose",
    [
        "Window: filings ECFS received on or after 2026-08-24, extended by a daily run.",
        "True range. Filing years 2010 to date, each read whole; spicy-regs `3bc0a97` built it.",
        "Sampled. One row per retained collection; rows from different cycles share the table.",
    ],
)
def test_coverage_prose_keeps_scope_dates_rules_and_commit_references(prose):
    assert dd.coverage_prose_errors("gao_reports", prose) == []


def test_a_coverage_exception_allows_exactly_its_phrase(monkeypatch):
    monkeypatch.setitem(dd.COVERAGE_PROSE_EXCEPTIONS, "gao_reports", {"about 3,693 rows": "caveat size"})
    assert dd.coverage_prose_errors("gao_reports", "Window. Names unread on about 3,693 rows.") == []
    errors = dd.coverage_prose_errors("gao_reports", "Window. Names unread on about 3,693 rows; 43 rows held.")
    assert len(errors) == 1 and "'43 rows'" in errors[0]


def test_a_coverage_exception_that_no_longer_matches_is_an_error(monkeypatch):
    """A rewritten note cannot leave a stale permission behind."""
    monkeypatch.setitem(dd.COVERAGE_PROSE_EXCEPTIONS, "gao_reports", {"about 3,693 rows": "caveat size"})
    errors = dd.coverage_prose_errors("gao_reports", "Window. Reports the feed has listed.")
    assert errors and "no longer matches" in errors[0]


def test_check_refuses_a_row_count_in_committed_style_prose():
    descriptions = dd.load_descriptions()
    broken = {t: dict(entry) for t, entry in descriptions.items()}
    broken["gao_reports"]["coverage"] = "Window. 43 rows on the measurement date."
    errors = dd.check_descriptions(dd.expected_schemas(), broken)
    assert any("[gao_reports] coverage states a row count" in e for e in errors)


def test_generated_page_carries_coverage_and_data_quality():
    page = (dd.DEFAULT_DOCS_TABLES_DIR / "documents.md").read_text()
    assert "**Coverage.**" in page
    assert "**Data quality.**" in page


def test_catalog_declares_every_published_class():
    """The consumer cannot import this package, so the file is the interface."""
    descriptions = dd.load_descriptions()
    document = dd.build_catalog(descriptions, dd.expected_schemas())
    assert document["format_version"] == dd.CATALOG_FORMAT_VERSION
    assert [c["table"] for c in document["classes"]] == list(dd.TABLES)
    for entry in document["classes"]:
        assert entry["label"].strip(), entry["table"]
        assert entry["coverage"].strip(), entry["table"]
        assert entry["columns"], entry["table"]


def test_catalog_does_not_claim_searchability():
    """Searchable is the serving side's fact; a catalog certifying itself is not a check."""
    document = dd.build_catalog(dd.load_descriptions(), dd.expected_schemas())
    blob = json.dumps(document).lower()
    for claim in ("queryable", "searchable", "indexed", "mcp_queryable"):
        assert claim not in blob, f"catalog must not assert {claim!r}"


def test_committed_catalog_is_up_to_date():
    """Same discipline as the table pages: the committed file equals a fresh build."""
    fresh = dd.build_catalog(dd.load_descriptions(), dd.expected_schemas())
    committed = json.loads(dd.DEFAULT_CATALOG_PATH.read_text(encoding="utf-8"))
    assert committed == fresh, "catalog.json is stale; run 'uv run spicy-regs-dict catalog'"


def test_bundled_mcp_metadata_matches_dictionary_and_provider():
    """The base MCP wheel carries the same meaning as the generated docs."""
    fresh = dd.build_mcp_metadata(dd.load_descriptions(), dd.expected_schemas())
    committed = json.loads(dd.DEFAULT_MCP_METADATA_PATH.read_text(encoding="utf-8"))
    assert committed == fresh, "table_metadata.json is stale; run 'uv run spicy-regs-dict generate'"
    assert committed["dockets"]["identity_columns"] == ["docket_id"]
    assert committed["member_votes"]["identity_columns"] == list(dd._contracts()["member_votes"].identity)
    assert committed["member_votes"]["grain"] == dd.contract_grain("member_votes")


def test_check_rejects_undeclared_identity_column():
    descriptions = dd.load_descriptions()
    descriptions["fec_committees"]["identity_columns"] = ["missing_column"]
    assert any("identity_columns" in error for error in dd.check_descriptions(dd.expected_schemas(), descriptions))


def test_catalog_digest_sidecar_matches_the_committed_bytes():
    """A vendored contract is pinned by digest, so the sidecar must track the file.

    The digest is a sidecar rather than a field inside the document on purpose:
    a digest carried by the thing it certifies proves nothing.
    """
    import hashlib

    payload = dd.DEFAULT_CATALOG_PATH.read_bytes()
    recorded = dd.DEFAULT_CATALOG_DIGEST_PATH.read_text(encoding="utf-8").split()[0]
    assert hashlib.sha256(payload).hexdigest() == recorded, (
        "catalog.json.sha256 is stale; run 'uv run spicy-regs-dict catalog'"
    )


def test_catalog_bytes_are_the_one_canonical_form():
    """One document, one byte string, or the digest pin is meaningless."""
    document = dd.build_catalog(dd.load_descriptions(), dd.expected_schemas())
    assert dd.catalog_bytes(document) == dd.catalog_bytes(document)
    assert dd.catalog_bytes(document) == dd.DEFAULT_CATALOG_PATH.read_bytes()


def test_every_coverage_statement_carries_a_measurement_date():
    """A statement with no date cannot be told from a fresh one by a reader."""
    coverage = dd.table_coverage(dd.load_descriptions())
    assert set(coverage) == set(dd.TABLES)
    for table, entry in coverage.items():
        assert entry["measured_on"].strip(), table
        date.fromisoformat(entry["measured_on"])


def test_check_detects_a_missing_or_malformed_measurement_date():
    descriptions = dd.load_descriptions()
    missing = {t: dict(e) for t, e in descriptions.items()}
    missing["gao_reports"].pop("measured_on")
    assert any("missing 'measured_on'" in e for e in dd.check_descriptions(dd.expected_schemas(), missing))

    malformed = {t: dict(e) for t, e in descriptions.items()}
    malformed["gao_reports"]["measured_on"] = "last Tuesday"
    assert any("not an ISO date" in e for e in dd.check_descriptions(dd.expected_schemas(), malformed))


def test_unreachable_source_is_not_reported_as_a_pass():
    """The gate must tell an outage from drift; collapsing them is why it could not fail."""
    parser = dd.build_parser()
    args = parser.parse_args(["check", "--source", "r2", "--base", "https://nonexistent.invalid"])
    assert args.func(args) == dd.EXIT_SOURCE_UNREACHABLE
    assert dd.EXIT_SOURCE_UNREACHABLE not in (0, 1)


def test_discovery_continues_after_an_unreadable_table(tmp_path, monkeypatch):
    import pyarrow as pa
    import pyarrow.parquet as pq

    monkeypatch.setattr(dd, 'TABLES', ('missing', 'present'))
    pq.write_table(pa.table({'id': ['one']}), tmp_path / 'present.parquet')
    with pytest.raises(dd.SchemaDiscoveryError) as error:
        dd.discover_schemas('local', str(tmp_path))
    assert error.value.schemas == {'present': [('id', 'VARCHAR')]}
    assert '[missing]' in str(error.value)


def test_live_check_accepts_supported_but_unpublished_tables(monkeypatch):
    schemas = dd.expected_schemas()
    monkeypatch.setattr(dd, 'discover_schemas', lambda *_: {'dockets': schemas['dockets']})
    args = dd.build_parser().parse_args(['check', '--source', 'r2'])
    assert args.func(args) == 0


def test_unknown_published_table_is_schema_drift(monkeypatch):
    monkeypatch.setattr(dd, 'discover_schemas', lambda *_: {'unknown_published': [('id', 'VARCHAR')]})
    args = dd.build_parser().parse_args(['check', '--source', 'r2'])
    assert args.func(args) == 1


def test_incomplete_live_check_is_not_a_pass_after_other_tables_match(monkeypatch):
    def partial(*_):
        raise dd.SchemaDiscoveryError({'dockets': dd.expected_schemas()['dockets']}, ['[documents] unreachable'])
    monkeypatch.setattr(dd, 'discover_schemas', partial)
    args = dd.build_parser().parse_args(['check', '--source', 'r2'])
    assert args.func(args) == dd.EXIT_SOURCE_UNREACHABLE


# --------------------------------------------------------------------------- #
# The contract-hosted tables: prose comes from spicy-docs, not from this repo.
# --------------------------------------------------------------------------- #
def test_every_hosted_column_has_prose():
    """All four hundred-odd columns carry a sentence, read from the contract."""
    descriptions = dd.load_descriptions()
    schemas = dd.expected_schemas()
    for table in dd.CONTRACT_TABLES:
        columns = descriptions[table]["columns"]
        assert set(columns) == {c for c, _ in schemas[table]}, table
        blank = [name for name, text in columns.items() if not (text or "").strip()]
        assert not blank, f"{table}: columns with no description: {blank}"


def test_hosted_prose_is_the_contract_prose_not_a_copy():
    """The sentences are read from the wheel, so there is only one copy of them."""
    from spicy_docs.schemas import TABLE_CONTRACTS

    descriptions = dd.load_descriptions()
    for table in dd.CONTRACT_TABLES:
        assert descriptions[table]["columns"] == dict(TABLE_CONTRACTS[table].descriptions), table


def test_hosted_entries_do_not_list_columns_inline():
    """The marker and an inline list would be two sources that can disagree."""
    import yaml

    raw = yaml.safe_load(dd.DEFAULT_DESCRIPTIONS.read_text(encoding="utf-8"))["tables"]
    for table in dd.CONTRACT_TABLES:
        assert raw[table].get("columns_from") == dd.COLUMNS_FROM_SPICY_DOCS, table
        assert "columns" not in raw[table], f"{table} lists columns inline as well as by marker"


def test_an_inline_column_list_beside_the_marker_is_refused(tmp_path):
    """Declaring both must fail loudly rather than silently preferring one."""
    import yaml

    path = tmp_path / "descriptions.yaml"
    path.write_text(
        yaml.safe_dump(
            {"tables": {"members": {"columns_from": "spicy_docs", "columns": {"bioguide_id": "x"}}}},
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="columns_from"):
        dd.load_descriptions(path)


def test_hosted_schemas_follow_the_contract_types_in_contract_order():
    """VARCHAR unless the contract types a column (decision 67), spelled as DuckDB describes the file."""
    from spicy_docs.schemas import TABLE_CONTRACTS

    from spicy_regs.contract_types import DESCRIBED

    for table, columns in dd.contract_schemas().items():
        contract = TABLE_CONTRACTS[table]
        assert columns == [(c, DESCRIBED[contract.column_type(c)]) for c in contract.columns], table
    typed = dict(dd.contract_schemas()["document_attributes"])
    assert (typed["page_count"], typed["receive_date"], typed["topics"]) == (
        "INTEGER", "TIMESTAMP WITH TIME ZONE", "VARCHAR[]")


# --------------------------------------------------------------------------- #
# The materialized rulemaking dataset: builders' own schemas, served from the snapshot pointer.
# --------------------------------------------------------------------------- #
RULEMAKING_IDENTITY = {
    "rule_targets": ["docket_id", "cfr_ref", "rin", "source"],
    "proceedings": ["proceeding_id"],
    "regulatory_agenda_items": ["agenda_item_id"],
    "agenda_item_proceedings": ["relationship_id"],
    "comment_periods": ["comment_period_id"],
    "rulemaking_lifecycles": ["proceeding_id"],
    "lifecycle_events": ["proceeding_id", "document_id"],
    "agency_lifecycle_stats": ["agency_code", "stratum"],
}


def test_each_rulemaking_table_names_its_identity_and_where_it_is_published():
    assert tuple(RULEMAKING_IDENTITY) == dd.RULEMAKING_TABLES
    metadata = json.loads(dd.DEFAULT_MCP_METADATA_PATH.read_text(encoding="utf-8"))
    for table, identity in RULEMAKING_IDENTITY.items():
        entry = metadata[table]
        assert (entry["identity_columns"], entry["kind"]) == (identity, "derived"), table
        assert entry["grain"] and entry["label"] and entry["measured_on"], table
        page = (dd.DEFAULT_DOCS_TABLES_DIR / f"{table}.md").read_text(encoding="utf-8")
        assert "in the snapshot that `materialized/rulemaking/latest.json` names" in page, table
        assert "**MCP `query_sql` support:** Configured" in page, table


def test_a_pointer_that_names_no_publication_is_unreadable_not_drift(monkeypatch):
    from spicy_regs.sources.publication import PublicationError

    def refuse(*_):
        raise PublicationError("Invalid rulemaking snapshot")

    monkeypatch.setattr(dd, "discover_schemas", refuse)
    args = dd.build_parser().parse_args(["check", "--source", "r2"])
    assert args.func(args) == dd.EXIT_SOURCE_UNREACHABLE


def test_live_check_holds_declared_types_to_the_published_file(monkeypatch, capsys):
    """A DATE column published as VARCHAR keeps its name, so only the type comparison can see it."""
    declared = dd.expected_schemas()["rulemaking_lifecycles"]
    drifted = [(column, "VARCHAR" if column == "proposal_date" else kind) for column, kind in declared]
    args = dd.build_parser().parse_args(["check", "--source", "r2"])
    monkeypatch.setattr(dd, "discover_schemas", lambda *_: {"rulemaking_lifecycles": drifted})
    assert args.func(args) == 1
    assert "[rulemaking_lifecycles.proposal_date] in-code type DATE but live parquet VARCHAR" in capsys.readouterr().err
    monkeypatch.setattr(dd, "discover_schemas", lambda *_: {"rulemaking_lifecycles": declared})
    assert args.func(args) == 0
