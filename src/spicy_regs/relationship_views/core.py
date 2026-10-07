"""Lazy SQL views over held source arrays; no acquisition or provider imports."""

from dataclasses import dataclass
import json
from typing import Any, Iterable, Mapping

from .lineage import column_lineage, table_columns, view_columns_from


def quoted(value: str) -> str:
    """Quote a trusted SQL identifier (including unusual names in a source schema)."""
    return '"' + value.replace('"', '""') + '"'


def literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


@dataclass(frozen=True)
class ArrayRelationship:
    """One independently expanded array and its deliberately narrower pair view.

    Expressions refer to `s` (source row) and `e` (json_each element). They are
    application constants, never SQL supplied by a client. ``details`` adds
    (column, expression, meaning) triples to the occurrence view; the meaning is
    what ``describe_table`` states for that column. ``detail_read_column`` names
    the source column that says a detail record was read (``'true'``), on a
    table whose array is only read with that detail: the field-state view then
    carries it and says whether an absent or empty array was ever read
    (:data:`DETAIL_STATES`).
    """

    name: str
    source_table: str
    source_keys: tuple[str, ...]
    source_field: str
    target_kind: str
    target_expression: str
    valid_expression: str
    meaning: str
    context_columns: tuple[str, ...] = ()
    details: tuple[tuple[str, str, str], ...] = ()
    rule_version: str = "held-array-v1"
    target_kind_expression: str | None = None
    detail_read_column: str | None = None

    @property
    def names(self) -> tuple[str, str, str]:
        return (self.name + "_occurrences", self.name + "_pairs", self.name + "_field_states")

    @property
    def required_columns(self) -> tuple[str, ...]:
        detail = (self.detail_read_column,) if self.detail_read_column else ()
        return (*self.source_keys, self.source_field, *self.context_columns, *detail)


#: A detail-backed field's states, where an absent array no longer reads as one state: ``unread`` (no detail read: the
#: field is NULL or an empty array), ``not_stated`` (read, field NULL), ``stated_empty`` (read, empty array),
#: ``stated`` (a populated array, read or not). Malformed and non-array values keep their literal state.
DETAIL_STATES = ("unread", "not_stated", "stated_empty", "stated")


def _selects(spec: ArrayRelationship, pin: object) -> tuple[str, str, str]:
    """The occurrence, pair and field-state SELECTs, in that order; pairs read the occurrence view."""
    occurrences = quoted(spec.names[0])
    source = quoted(spec.source_table)
    field = f"s.{quoted(spec.source_field)}"
    parsed = f"TRY_CAST({field} AS JSON)"
    array = f"CASE WHEN json_type({parsed}) = 'ARRAY' THEN {parsed} ELSE '[]'::JSON END"
    source_columns = ", ".join(f"s.{quoted(c)}" for c in (*spec.source_keys, *spec.context_columns))
    identity = ", ".join(quoted(c) for c in spec.source_keys)
    provenance = (
        f"{literal(spec.source_table)} AS source_table, "
        f"{literal(spec.source_field)} AS source_field, "
        f"{literal(json.dumps(pin, sort_keys=True))} AS source_publication_json, "
        f"{literal(spec.rule_version)} AS rule_version"
    )
    # Guard every derived key with the same validation as its disposition. Raw
    # malformed/null elements still get an occurrence and retain their position.
    valid = f"COALESCE(({spec.valid_expression}), FALSE)"
    details = "".join(f", {expr} AS {quoted(name)}" for name, expr, _ in spec.details)
    occurrence_sql = f"""SELECT {source_columns}, CAST(e.key AS BIGINT) AS source_ordinal,
            CAST(e.value AS VARCHAR) AS raw_value_json,
            {literal('/' + spec.source_field + '/')} || e.key AS source_pointer,
            CASE WHEN e.type = 'NULL' THEN 'null_element'
                 WHEN {valid} THEN 'valid' ELSE 'unsupported_element' END AS parsing_status,
            {spec.target_kind_expression or literal(spec.target_kind)} AS target_kind,
            CASE WHEN {valid} THEN {spec.target_expression} ELSE NULL END AS target_key,
            CASE WHEN {valid} AND ({spec.target_expression}) IS NOT NULL
                 THEN 'not_checked' ELSE 'unsupported' END AS target_status,
            {provenance}{details}
        FROM (SELECT s.*, unnest(json_extract({array}, '$[*]')) AS _relationship_element,
                generate_subscripts(json_extract({array}, '$[*]'),1)-1 AS _relationship_ordinal
              FROM {source} s) s"""
    # SELECT-list expansion retains duplicate/null positions without hashing
    # every source row in json_each's correlated delimiter join. These are the
    # only json_each fields the maintained array recipes consume.
    occurrence_sql = (occurrence_sql
                      .replace("e.key", "CAST(s._relationship_ordinal AS VARCHAR)")
                      .replace("e.value", "s._relationship_element")
                      .replace("e.type", "json_type(s._relationship_element)"))
    pair_sql = f"""SELECT DISTINCT {identity}, target_kind, target_key, target_status,
            source_table, source_field, source_publication_json, rule_version
        FROM {occurrences} WHERE parsing_status = 'valid' AND target_key IS NOT NULL"""
    states = ("'sql_null'", "'empty_array'", "'populated_array'")
    detail = ""
    if spec.detail_read_column:
        read = f"s.{quoted(spec.detail_read_column)}"
        detail = f", {read}"
        unread, not_stated, stated_empty, stated = (literal(state) for state in DETAIL_STATES)
        states = (f"CASE WHEN {read} = 'true' THEN {not_stated} ELSE {unread} END",
                  f"CASE WHEN {read} = 'true' THEN {stated_empty} ELSE {unread} END", stated)
    state_sql = f"""SELECT {source_columns}{detail}, {field} AS raw_field_value,
            CASE WHEN {field} IS NULL THEN {states[0]}
                 WHEN {parsed} IS NULL THEN 'malformed_json'
                 WHEN json_type({parsed}) = 'NULL' THEN 'json_null'
                 WHEN json_type({parsed}) <> 'ARRAY' THEN 'unsupported_shape'
                 WHEN json_array_length({parsed}) = 0 THEN {states[1]}
                 ELSE {states[2]} END AS field_state,
            CASE WHEN json_type({parsed}) = 'ARRAY'
                 THEN json_array_length({parsed}) ELSE NULL END AS array_length,
            {provenance}
        FROM {source} s"""
    return occurrence_sql, pair_sql, state_sql


def install_arrays(
    connection: Any,
    available_tables: Iterable[str],
    specs: Iterable[ArrayRelationship],
    publication: Mapping[str, object] | None = None,
) -> dict[str, dict[str, Any]]:
    """Bind views using schemas only; never scan source rows at installation.

    Missing tables/columns are reported. Malformed row values are visible in
    field-state and occurrence views when queried. Programming/SQL errors raise
    rather than being disguised as unavailable source data.
    """
    available = set(available_tables)
    publication = publication or {}
    schemas: dict[str, list[str]] = {}
    result: dict[str, dict[str, Any]] = {}
    for spec in specs:
        if spec.source_table in available and spec.source_table not in schemas:
            schemas[spec.source_table] = [
                row[0] for row in connection.execute(f"DESCRIBE {quoted(spec.source_table)}").fetchall()
            ]
        missing = sorted(set(spec.required_columns) - set(schemas.get(spec.source_table, [])))
        status = "available"
        reason = None
        if spec.source_table not in available:
            status, reason = "unavailable", "Source table is not loaded"
        elif missing:
            status, reason = "unsupported", "Missing source columns: " + ", ".join(missing)
        lineages: dict[str, dict[str, list[str]]] = {}
        if status == "available":
            relations = {spec.source_table: table_columns(spec.source_table, schemas[spec.source_table])}
            for name, select in zip(spec.names, _selects(spec, publication.get(spec.source_table)), strict=True):
                connection.execute(f"CREATE OR REPLACE VIEW {quoted(name)} AS {select}")
                lineages[name] = column_lineage(connection, select, relations)
                # The pair view reads the occurrence view; its columns keep the source table's lineage.
                relations[name] = view_columns_from(lineages[name])
        for name, kind in zip(spec.names, ("occurrences", "pairs", "field_states"), strict=True):
            result[name] = {
                "status": status,
                "reason": reason,
                "dependencies": [spec.source_table],
                "metadata": {
                    "label": name.replace("_", " "),
                    "summary": spec.meaning,
                    "kind": "derived",
                    "view_kind": kind,
                    "source_keys": list(spec.source_keys),
                    "source_field": spec.source_field,
                    "identity_columns": [*spec.source_keys, "source_ordinal"] if kind == "occurrences"
                    else [*spec.source_keys, "target_kind", "target_key"] if kind == "pairs"
                    else list(spec.source_keys),
                    "rule_version": spec.rule_version,
                    "input_publications": {spec.source_table: publication.get(spec.source_table)},
                    "coverage": "Bounded by the loaded source selection. Target existence is not checked. "
                    + ("A NULL or empty list is told apart by the source's "
                       f"{spec.detail_read_column}: never read, read with no list, or read with an empty one. "
                       if spec.detail_read_column else
                       "SQL NULL describes the held field, not the absent/null state of the original publisher field. ")
                    + "Pairs deduplicate navigation keys; occurrences preserve repeated source elements.",
                    **({"column_descriptions": {name: meaning for name, _, meaning in spec.details}}
                       if kind == "occurrences" and spec.details else {}),
                    **({"column_lineage": lineages[name]} if name in lineages else {}),
                },
            }
    return result
