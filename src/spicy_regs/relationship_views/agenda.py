"""Agenda membership keeps each edition and source URL as a separate target."""

from .sql_views import SQLView, pin


def editions(p):
    return f"""WITH editions AS (
        SELECT rin,agenda_edition,url,count(*) AS target_count
        FROM unified_agenda GROUP BY rin,agenda_edition,url
    )
    SELECT i.agenda_item_id,i.rin,e.agenda_edition,e.url AS agenda_url,
        CASE WHEN e.rin IS NOT NULL THEN to_json(struct_pack(rin:=e.rin,agenda_edition:=e.agenda_edition,url:=e.url))::VARCHAR
             ELSE NULL END AS target_key,
        coalesce(e.target_count,0) AS target_count,
        CASE WHEN i.rin IS NULL OR i.rin='' THEN 'unsupported'
             WHEN e.rin IS NULL THEN 'missing'
             WHEN e.agenda_edition IS NULL OR e.url IS NULL THEN 'unsupported'
             WHEN e.target_count=1 THEN 'found' ELSE 'ambiguous' END AS target_status,
        'unified_agenda_edition' AS target_kind, 'many_editions' AS expected_cardinality,
        {pin(p, 'regulatory_agenda_items')} AS source_publication_json,
        {pin(p, 'unified_agenda')} AS target_publication_json,
        'agenda-rin-edition-v1' AS rule_version
    FROM regulatory_agenda_items i LEFT JOIN editions e ON i.rin=e.rin AND i.rin<>''"""


AGENDA_VIEWS = (
    SQLView('agenda_item_editions', {
        'regulatory_agenda_items': ('agenda_item_id','rin'),
        'unified_agenda': ('rin','agenda_edition','url'),
    }, editions, 'One regulatory agenda item may occur in many native Agenda editions. Each exact RIN/edition/URL '
       'target remains separate, with duplicate target-row counts. Edition membership does not merge rulemaking '
       'actions and an older edition is never replaced by a chosen latest row.',
       ('agenda_item_id','rin','agenda_edition','agenda_url'),
       column_descriptions={'expected_cardinality': 'Always many_editions: one agenda item may match several native '
                                                    'editions, so a row count is not an item count.'}),
)
