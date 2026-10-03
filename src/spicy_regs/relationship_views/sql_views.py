"""Small registry for multi-table navigation that cannot be expressed as one array."""

from dataclasses import dataclass, field
import json
from typing import Any, Callable, Iterable, Mapping

from .core import literal, quoted


@dataclass(frozen=True)
class SQLView:
    name: str
    required: Mapping[str, tuple[str, ...]]
    query: Callable[[Mapping[str, object]], str]
    meaning: str
    identity_columns: tuple[str, ...]
    rule_version: str = "held-navigation-v1"
    role: str = "navigation"
    category: str = "derived"
    column_descriptions: Mapping[str, str] = field(default_factory=dict)


def pin(publication: Mapping[str, object], table: str) -> str:
    return literal(json.dumps(publication.get(table), sort_keys=True))


def install_sql_views(connection: Any, available_tables: Iterable[str], specs: Iterable[SQLView],
                      publication: Mapping[str, object] | None = None) -> dict[str, dict[str, Any]]:
    available, publication = set(available_tables), publication or {}
    result: dict[str, dict[str, Any]] = {}
    schemas: dict[str, set[str]] = {}
    for spec in specs:
        missing_tables = sorted(set(spec.required) - available)
        missing_columns = []
        for table, required in spec.required.items():
            if table not in available:
                continue
            if table not in schemas:
                schemas[table] = {row[0] for row in connection.execute(f'DESCRIBE {quoted(table)}').fetchall()}
            missing_columns.extend(f'{table}.{c}' for c in required if c not in schemas[table])
        status, reason = 'available', None
        if missing_tables:
            status, reason = 'unavailable', 'Source tables are not loaded: ' + ', '.join(missing_tables)
        elif missing_columns:
            status, reason = 'unsupported', 'Missing source columns: ' + ', '.join(missing_columns)
        if status == 'available':
            connection.execute(f'CREATE OR REPLACE VIEW {quoted(spec.name)} AS {spec.query(publication)}')
        result[spec.name] = {
            'status': status, 'reason': reason, 'dependencies': list(spec.required),
            'metadata': {
                'label': spec.name.replace('_', ' '), 'summary': spec.meaning, 'kind': 'derived',
                'rule_version': spec.rule_version, 'identity_columns': list(spec.identity_columns),
                'view_role': spec.role, 'category': spec.category,
                'column_descriptions': dict(spec.column_descriptions),
                'input_publications': {table: publication.get(table) for table in spec.required},
                'coverage': 'Bounded by each selected input independently. A found target establishes a lookup, '
                            'not population completeness, legal applicability, or common-person identity.',
            },
        }
    return result


_COLUMN_DESCRIPTIONS = {
    'source_ordinal': 'Zero-based position in the held source array; repeated elements remain distinct.',
    'source_pointer': 'Location in the retained source field; see source_field and the source key.',
    'raw_value_json': 'The complete held element, including unsupported objects and nulls.',
    'subject_attributes_json': 'Held subject attributes excluding children, which appear as separate rows; source_pointer locates the complete original element.',
    'raw_field_value': 'Literal held field value; retained so malformed input remains inspectable.',
    'target_key': 'Typed target reference; a key alone does not prove the target exists.',
    'target_status': 'Lookup disposition: found, missing, ambiguous, unsupported or not_checked.',
    'parsing_status': 'Source-value interpretation, separate from target existence.',
    'source_publication_json': 'Host-selected publication metadata for this source; JSON null means unprovided.',
    'target_publication_json': 'Host-selected target publication metadata; no atomic cross-table snapshot is implied.',
    'rule_version': 'Version of this relationship-view rule.',
    'target_count': 'Number of exact target rows under the complete key in this selected publication.',
}


def view_columns(described: Iterable[tuple], descriptions: Mapping[str, str] | None = None) -> list[dict[str, str]]:
    """Declare a bound view's columns from its ``DESCRIBE`` rows, with this registry's meanings.

    Callers describe a view when asked (``describe_table``), not at
    installation: binding each of the registry's views again over remote
    Parquet doubled the serving connection's build time.
    """
    return [
        {'column_name': row[0], 'column_type': row[1],
         'description': (descriptions or {}).get(row[0]) or _COLUMN_DESCRIPTIONS.get(row[0], row[0].replace('_', ' ').capitalize() +
                                                 '; its meaning and source grain are described by this view.')}
        for row in described
    ]


def annotate_views(views: dict[str, dict[str, Any]]) -> None:
    """Attach the shared availability and coverage semantics without evaluating or rebinding views."""
    for name, info in views.items():
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

        if metadata.get('view_role') == 'child_query':
            metadata['coverage'] = (
                'Inherited from the selected source tables. A cleaner shape does not expand source coverage '
                'or establish current records, complete populations, verified identities or financial totals.'
            )
            metadata['coverage_semantics'].pop('unique_pairs')
            metadata['coverage_semantics']['occurrence_rows'] = (
                'See the view summary for row grain, array ordering, filters and interpretation status. '
                'Source tables preserve original values and field-presence evidence.'
            )
            metadata['coverage_semantics']['missingness'] = (
                'Typed NULL does not establish source absence. Consult status columns and the original source row '
                'to distinguish unsupported values, source nulls and missing fields.'
            )
