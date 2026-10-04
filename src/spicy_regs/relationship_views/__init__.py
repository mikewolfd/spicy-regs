"""Source-occurrence and distinct navigation views, using only held tables.

Install before applying the serving connection's SQL restrictions. Installation
binds schemas but does not read source rows or look up targets. This module has
no pipeline or spicy_docs import dependency.
"""

from typing import Any, Iterable, Mapping
from dataclasses import replace

from .affiliations import AFFILIATION_VIEWS, NATIVE_AFFILIATION_VIEWS
from .agenda import AGENDA_VIEWS
from .artifacts_topics import ARTIFACT_SQL_VIEWS, ARTIFACT_TOPIC_RELATIONSHIPS, NATIVE_BILL_SUBJECTS
from .courts import COURT_VIEWS
from .legislative_diffs import NATIVE_DIFF_VIEWS as DIFF_VIEWS
from .diffs import DIFF_VIEWS as LEGACY_DIFF_VIEWS
from .entities import ENTITY_VIEWS
from .fec import FEC_VIEWS
from .fec_native_document_query import FEC_NATIVE_DOCUMENT_QUERY_VIEWS
from .fcc_native import FCC_NATIVE_VIEWS
from .lobbying_native import LOBBYING_NATIVE_VIEWS
from .regulations_native import ARRAYS as NATIVE_REGULATORY_ARRAYS, install_native_regulations_views
from .lifecycle_dates import LIFECYCLE_DATE_VIEWS
from .identity_candidates import IDENTITY_VIEWS, NATIVE_IDENTITY_VIEWS
from .sql_views import annotate_views, install_sql_views, view_columns
from .comments import install_comment_references
from .congress import CONGRESS_RELATIONSHIPS, NATIVE_CONGRESS_RELATIONSHIPS, NATIVE_COMMUNICATION_RINS
from .core import ArrayRelationship, install_arrays
from .regulatory import REGULATORY_RELATIONSHIPS

RELATIONSHIP_VIEWS = (*CONGRESS_RELATIONSHIPS, *REGULATORY_RELATIONSHIPS, *ARTIFACT_TOPIC_RELATIONSHIPS)
FEC_QUERY_VIEWS = tuple(
    replace(spec, role="child_query", category="source_evidence" if spec.name == "fec_api_responses" else "query_data")
    for spec in FEC_NATIVE_DOCUMENT_QUERY_VIEWS
)
SQL_RELATIONSHIP_VIEWS = (
    *ARTIFACT_SQL_VIEWS,
    *ENTITY_VIEWS,
    *COURT_VIEWS,
    *DIFF_VIEWS,
    *FEC_VIEWS,
    *AGENDA_VIEWS,
    *IDENTITY_VIEWS,
    *AFFILIATION_VIEWS,
    *FCC_NATIVE_VIEWS,
    *LOBBYING_NATIVE_VIEWS,
    *LIFECYCLE_DATE_VIEWS,
)


def install_relationship_views(
    connection: Any,
    available_tables: Iterable[str],
    publication: Mapping[str, object] | None = None,
) -> dict[str, dict[str, Any]]:
    """Return per-view availability, meaning, source identity and publication pins."""
    available = set(available_tables)
    from spicy_regs.subject_catalog import descriptors
    from importlib.resources import files
    import json
    declared = descriptors()
    metadata = json.loads(files("spicy_regs").joinpath("table_metadata.json").read_text())
    native = {table for table in available if table in declared and not declared[table]["receipt_only"]
              and {column["column_name"] for column in metadata[table]["columns"]} <=
              {row[0] for row in connection.execute(f'DESCRIBE "{table}"').fetchall()}}
    congress = tuple(spec for spec in CONGRESS_RELATIONSHIPS if spec.source_table not in native)
    congress += tuple(spec for spec in NATIVE_CONGRESS_RELATIONSHIPS if spec.source_table in native)
    native_arrays = tuple(spec for spec in (NATIVE_COMMUNICATION_RINS, NATIVE_BILL_SUBJECTS)
                          if spec.source_table in native)
    replacements = {spec.name for spec in native_arrays}
    arrays = tuple(spec for spec in (*congress, *REGULATORY_RELATIONSHIPS, *ARTIFACT_TOPIC_RELATIONSHIPS)
                   if spec.name not in replacements and not (spec.source_table in native and spec.source_table in {"fcc_filings", "lobbying_activities"}))
    arrays += native_arrays
    results = install_arrays(connection, available, arrays, publication)
    results.update(install_comment_references(connection, available, publication))
    sql = tuple(spec for spec in SQL_RELATIONSHIP_VIEWS if spec not in DIFF_VIEWS)
    native_sql = tuple(spec for spec in (*NATIVE_IDENTITY_VIEWS, *NATIVE_AFFILIATION_VIEWS)
                       if set(spec.required) <= native)
    replaced = {spec.name for spec in native_sql}
    sql = tuple(spec for spec in sql if spec.name not in replaced) + native_sql
    sql += tuple(spec for spec in DIFF_VIEWS if set(spec.required) <= native)
    sql += tuple(spec for spec in LEGACY_DIFF_VIEWS if not set(spec.required) <= native)
    results.update(install_sql_views(connection, available, sql, publication))
    results.update(install_sql_views(connection, available, FEC_QUERY_VIEWS, publication))
    native_results = install_native_regulations_views(connection, native)
    for name, state in native_results.items():
        if state == "receipt-only":
            results.pop(name, None)
            continue
        dependencies = next(([table] for base, table, *_ in NATIVE_REGULATORY_ARRAYS
                             if name in {base + "_occurrences", base + "_pairs"}), None)
        if dependencies is None:
            dependencies = (["regulatory_agenda_items", "unified_agenda"] if name == "agenda_item_editions"
                            else ["documents"] if name == "document_attachment_renditions" else ["comments"])
        metadata = dict(results.get(name, {}).get("metadata", {}))
        native_meanings = {
            "comment_native_references": ("Publisher document references retained on each comment, distinguished by reference kind.", ["comment_id", "target_kind", "target_key"]),
            "comment_document_references": ("Comments linked by their publisher-stated comment-on document identifier.", ["comment_id", "target_key"]),
        }
        meaning, identities = native_meanings.get(name, (metadata.get("summary", name.replace("_", " ")), metadata.get("identity_columns", [])))
        metadata.update(label=metadata.get("label", name.replace("_", " ")), summary=meaning,
                        kind="derived", rule_version="native-subject-navigation/1", identity_columns=identities,
                        input_publications={table: (publication or {}).get(table) for table in dependencies})
        results[name] = {"status": "available", "reason": None, "dependencies": dependencies, "metadata": metadata}
    annotate_views(results)
    return results


__all__ = ["ArrayRelationship", "RELATIONSHIP_VIEWS", "install_relationship_views", "view_columns"]
