"""LDA source relationships over ordered native arrays."""

from .sql_views import SQLView


def contacted_entities(publication):
    return """SELECT s.filing_uuid, s.activity_index, ordinality - 1 AS source_ordinal,
        v.id AS target_key, v.name AS native_label, 'lda_government_entity' AS source_namespace
        FROM lobbying_activities s,
        UNNEST(s.government_entities) WITH ORDINALITY AS entities(v, ordinality)"""


LOBBYING_NATIVE_VIEWS = (
    SQLView(
        "lobbying_contacted_entities_occurrences",
        {"lobbying_activities": ("filing_uuid", "activity_index", "government_entities")},
        contacted_entities,
        "Each agency/chamber occurrence in activity order; names establish no cross-source identity.",
        ("filing_uuid", "activity_index", "source_ordinal"),
        rule_version="lda-native-entities/1",
    ),
    SQLView(
        "lobbying_contacted_entities_pairs",
        {"lobbying_activities": ("filing_uuid", "activity_index", "government_entities")},
        lambda p: (
            "SELECT DISTINCT filing_uuid, activity_index, target_key, source_namespace FROM ("
            + contacted_entities(p)
            + ") WHERE target_key IS NOT NULL AND target_key <> ''"
        ),
        "Distinct stated agency/chamber identifiers for navigation; occurrence rows retain repetitions and nulls.",
        ("filing_uuid", "activity_index", "target_key"),
        rule_version="lda-native-entities/1",
    ),
)
