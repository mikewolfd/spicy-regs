"""Lazy SQL views over held source arrays; no acquisition or provider imports."""

from dataclasses import dataclass
import json
from typing import Any, Iterable, Mapping


def quoted(value: str) -> str:
    """Quote a trusted SQL identifier (including unusual names in a source schema)."""
    return '"' + value.replace('"', '""') + '"'


def literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


@dataclass(frozen=True)
class ArrayRelationship:
    """One independently expanded array and its deliberately narrower pair view.

    Expressions refer to `s` (source row) and `e` (json_each element). They are
    application constants, never SQL supplied by a client.
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
    details: tuple[tuple[str, str], ...] = ()
    rule_version: str = "held-array-v1"
    target_kind_expression: str | None = None

    @property
    def names(self) -> tuple[str, str, str]:
        return (self.name + "_occurrences", self.name + "_pairs", self.name + "_field_states")

    @property
    def required_columns(self) -> tuple[str, ...]:
        return (*self.source_keys, self.source_field, *self.context_columns)


def _statements(spec: ArrayRelationship, pin: object) -> tuple[str, str, str]:
    occurrences, pairs, states = map(quoted, spec.names)
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
    details = "".join(f", {expr} AS {quoted(name)}" for name, expr in spec.details)
    occurrence_sql = f"""CREATE OR REPLACE VIEW {occurrences} AS
        SELECT {source_columns}, CAST(e.key AS BIGINT) AS source_ordinal,
            CAST(e.value AS VARCHAR) AS raw_value_json,
            {literal('/' + spec.source_field + '/')} || e.key AS source_pointer,
            CASE WHEN e.type = 'NULL' THEN 'null_element'
                 WHEN {valid} THEN 'valid' ELSE 'unsupported_element' END AS parsing_status,
            {spec.target_kind_expression or literal(spec.target_kind)} AS target_kind,
            CASE WHEN {valid} THEN {spec.target_expression} ELSE NULL END AS target_key,
            CASE WHEN {valid} AND ({spec.target_expression}) IS NOT NULL
                 THEN 'not_checked' ELSE 'unsupported' END AS target_status,
            {provenance}{details}
        FROM {source} s, json_each({array}) e"""
    pair_sql = f"""CREATE OR REPLACE VIEW {pairs} AS
        SELECT DISTINCT {identity}, target_kind, target_key, target_status,
            source_table, source_field, source_publication_json, rule_version
        FROM {occurrences} WHERE parsing_status = 'valid' AND target_key IS NOT NULL"""
    state_sql = f"""CREATE OR REPLACE VIEW {states} AS
        SELECT {source_columns}, {field} AS raw_field_value,
            CASE WHEN {field} IS NULL THEN 'sql_null'
                 WHEN {parsed} IS NULL THEN 'malformed_json'
                 WHEN json_type({parsed}) = 'NULL' THEN 'json_null'
                 WHEN json_type({parsed}) <> 'ARRAY' THEN 'unsupported_shape'
                 WHEN json_array_length({parsed}) = 0 THEN 'empty_array'
                 ELSE 'populated_array' END AS field_state,
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
    schemas: dict[str, set[str]] = {}
    result: dict[str, dict[str, Any]] = {}
    for spec in specs:
        if spec.source_table in available and spec.source_table not in schemas:
            schemas[spec.source_table] = {
                row[0] for row in connection.execute(f"DESCRIBE {quoted(spec.source_table)}").fetchall()
            }
        missing = sorted(set(spec.required_columns) - schemas.get(spec.source_table, set()))
        status = "available"
        reason = None
        if spec.source_table not in available:
            status, reason = "unavailable", "Source table is not loaded"
        elif missing:
            status, reason = "unsupported", "Missing source columns: " + ", ".join(missing)
        if status == "available":
            for sql in _statements(spec, publication.get(spec.source_table)):
                connection.execute(sql)
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
                    "SQL NULL describes the held field, not the absent/null state of the original publisher field. "
                    "Pairs deduplicate navigation keys; occurrences preserve repeated source elements.",
                },
            }
    return result
