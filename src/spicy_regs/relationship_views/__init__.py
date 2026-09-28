"""Source-occurrence and distinct navigation views, using only held tables.

Install before applying the serving connection's SQL restrictions. Installation
binds schemas but does not read source rows or look up targets. This module has
no pipeline or spicy_docs import dependency.
"""

from typing import Any, Iterable, Mapping

from .affiliations import AFFILIATION_VIEWS
from .agenda import AGENDA_VIEWS
from .artifacts_topics import ARTIFACT_SQL_VIEWS, ARTIFACT_TOPIC_RELATIONSHIPS
from .courts import COURT_VIEWS
from .diffs import DIFF_VIEWS
from .entities import ENTITY_VIEWS
from .fec import FEC_VIEWS
from .fcc_native import FCC_NATIVE_VIEWS
from .lifecycle_dates import LIFECYCLE_DATE_VIEWS
from .identity_candidates import IDENTITY_VIEWS
from .sql_views import column_metadata, install_sql_views
from .comments import install_comment_references
from .congress import CONGRESS_RELATIONSHIPS
from .core import ArrayRelationship, install_arrays
from .regulatory import REGULATORY_RELATIONSHIPS

RELATIONSHIP_VIEWS = (*CONGRESS_RELATIONSHIPS, *REGULATORY_RELATIONSHIPS, *ARTIFACT_TOPIC_RELATIONSHIPS)
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
    *LIFECYCLE_DATE_VIEWS,
)


def install_relationship_views(
    connection: Any,
    available_tables: Iterable[str],
    publication: Mapping[str, object] | None = None,
) -> dict[str, dict[str, Any]]:
    """Return per-view availability, meaning, source identity and publication pins."""
    available = set(available_tables)
    results = install_arrays(connection, available, RELATIONSHIP_VIEWS, publication)
    results.update(install_comment_references(connection, available, publication))
    results.update(install_sql_views(connection, available, SQL_RELATIONSHIP_VIEWS, publication))
    column_metadata(connection, results)
    return results


__all__ = ["ArrayRelationship", "RELATIONSHIP_VIEWS", "install_relationship_views"]
