"""Small registry for multi-table navigation that cannot be expressed as one array."""

from dataclasses import dataclass, field
import json
from typing import Any, Callable, Iterable, Mapping

from .core import literal, quoted
from .lineage import column_lineage, table_columns


@dataclass(frozen=True)
class SQLView:
    """One trusted SQL view over held tables.

    ``required`` lists each dependency with the columns the SQL reads. A column
    the SQL projects unchanged from a dependency (``alias.*`` or a bare column
    reference) carries that column's dictionary meaning in ``describe_table``,
    by lineage read from the parse tree at install (``lineage.column_lineage``);
    a column the view computes carries only its ``column_descriptions`` entry or
    the shared registry's, else None. ``tests/test_chaos_r3_server.py`` holds
    every view to that.
    """

    name: str
    required: Mapping[str, tuple[str, ...]]
    query: Callable[[Mapping[str, object]], str]
    meaning: str
    identity_columns: tuple[str, ...]
    rule_version: str = "held-navigation-v1"
    column_descriptions: Mapping[str, str] = field(default_factory=dict)


def pin(publication: Mapping[str, object], table: str) -> str:
    return literal(json.dumps(publication.get(table), sort_keys=True))


def install_sql_views(connection: Any, available_tables: Iterable[str], specs: Iterable[SQLView],
                      publication: Mapping[str, object] | None = None) -> dict[str, dict[str, Any]]:
    available, publication = set(available_tables), publication or {}
    result: dict[str, dict[str, Any]] = {}
    schemas: dict[str, list[str]] = {}
    for spec in specs:
        missing_tables = sorted(set(spec.required) - available)
        missing_columns = []
        for table, required in spec.required.items():
            if table not in available:
                continue
            if table not in schemas:
                schemas[table] = [row[0] for row in connection.execute(f'DESCRIBE {quoted(table)}').fetchall()]
            missing_columns.extend(f'{table}.{c}' for c in required if c not in schemas[table])
        status, reason = 'available', None
        if missing_tables:
            status, reason = 'unavailable', 'Source tables are not loaded: ' + ', '.join(missing_tables)
        elif missing_columns:
            status, reason = 'unsupported', 'Missing source columns: ' + ', '.join(missing_columns)
        lineage: dict[str, list[str]] = {}
        if status == 'available':
            select = spec.query(publication)
            connection.execute(f'CREATE OR REPLACE VIEW {quoted(spec.name)} AS {select}')
            lineage = column_lineage(connection, select, {t: table_columns(t, schemas[t]) for t in spec.required})
        result[spec.name] = {
            'status': status, 'reason': reason, 'dependencies': list(spec.required),
            'metadata': {
                'label': spec.name.replace('_', ' '), 'summary': spec.meaning, 'kind': 'derived',
                'rule_version': spec.rule_version, 'identity_columns': list(spec.identity_columns),
                'input_publications': {table: publication.get(table) for table in spec.required},
                **({'column_descriptions': dict(spec.column_descriptions)} if spec.column_descriptions else {}),
                **({'column_lineage': lineage} if lineage else {}),
                'coverage': 'Bounded by each selected input independently. A found target establishes a lookup, '
                            'not population completeness, legal applicability, or common-person identity.',
            },
        }
    return result


_COLUMN_DESCRIPTIONS = {
    'source_ordinal': 'Zero-based position in the held source array; repeated elements remain distinct.',
    'source_pointer': 'Location in the retained source field; see source_field and the source key.',
    'raw_value_json': 'The complete held element, including unsupported objects and nulls.',
    'raw_field_value': 'Literal held field value; retained so malformed input remains inspectable.',
    'target_key': 'Typed target reference; a key alone does not prove the target exists.',
    'target_status': 'Lookup disposition: found, missing, ambiguous, unsupported or not_checked.',
    'parsing_status': 'Source-value interpretation, separate from target existence.',
    'source_publication_json': 'Host-selected publication metadata for this source; JSON null means unprovided.',
    'target_publication_json': 'Host-selected target publication metadata; no atomic cross-table snapshot is implied.',
    'rule_version': 'Version of this relationship-view rule.',
    'target_count': 'Number of exact target rows under the complete key in this selected publication.',
    'source_table': 'The held table this row was derived from.',
    'source_field': 'The held column or native field name this row was derived from.',
    'target_kind': 'Namespace of target_key: the table or identifier kind it names.',
    'field_state': 'Literal state of the held field: sql_null, malformed_json, json_null, unsupported_shape, '
                   'empty_array or populated_array.',
    'array_length': 'Element count of the held array; NULL when the field is not an array.',
}


def view_columns(described: Iterable[tuple], descriptions: Mapping[str, str] | None = None,
                 inherited: Mapping[str, str] | None = None) -> list[dict[str, str | None]]:
    """Declare a bound view's columns from its ``DESCRIBE`` rows, each with its meaning.

    A column's meaning is the spec's ``column_descriptions`` entry, else the shared
    registry's, else ``inherited`` (the dependency tables' dictionary meanings,
    which the server supplies), else None: a column nothing describes is reported
    undescribed, never paraphrased from its name.

    Callers describe a view when asked (``describe_table``), not at
    installation: binding each of the registry's views again over remote
    Parquet doubled the serving connection's build time.
    """
    meanings = {**(inherited or {}), **_COLUMN_DESCRIPTIONS, **(descriptions or {})}
    return [{'column_name': row[0], 'column_type': row[1], 'description': meanings.get(row[0])} for row in described]


def annotate_views(views: dict[str, dict[str, Any]]) -> None:
    """Attach the shared availability and coverage semantics without evaluating or rebinding views."""
    for info in views.values():
        metadata = info['metadata']
        metadata['availability_basis'] = (
            'SQL bound against loaded schemas only; installation does not scan rows or verify source authority.'
        )
        metadata['coverage_semantics'] = {
            'serving_state': 'connection_view_only',
            'source_population': 'Inherited from each selected input; no complete-population claim.',
            'input_selection': 'Independent per-table publications, not an asserted atomic cross-table snapshot.',
            'measurement_status': 'not_measured_at_install',
            'occurrence_rows': 'Array elements preserve repetition; invalid elements retain a disposition.',
            'unique_pairs': 'Navigation pairs remove repeats and exclude unsupported target keys; not a participation count.',
            'legacy_schema': 'Missing dependency columns are unsupported, never a fabricated empty population.',
            'missingness': 'Held SQL NULL, JSON null, malformed input and empty arrays remain distinct. '
                           'Original absent/null states are known only where native field-state evidence was retained.',
            'publication_limit': 'A bound view is not a separately published or source-qualified artifact.',
        }
