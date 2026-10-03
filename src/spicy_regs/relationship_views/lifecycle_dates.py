"""Route event date evidence by the source that supplied the date."""
from .sql_views import SQLView, pin


def date_evidence(p):
    return f"""WITH targets AS (
        SELECT 'regulations_gov' AS dated_by,document_id AS lookup_key,count(*) AS n
        FROM documents GROUP BY document_id
        UNION ALL SELECT 'federal_register',document_number||'@'||publication_date,count(*)
        FROM federal_register GROUP BY document_number,publication_date
        UNION ALL SELECT 'unified_agenda',agenda_item_id,count(*)
        FROM regulatory_agenda_items GROUP BY agenda_item_id
    ) SELECT e.*,CASE e.dated_by WHEN 'regulations_gov' THEN 'documents'
        WHEN 'federal_register' THEN 'federal_register' WHEN 'unified_agenda' THEN 'regulatory_agenda_items'
        END AS target_table,coalesce(t.n,0) AS target_count,
        CASE WHEN e.dated_by NOT IN ('regulations_gov','federal_register','unified_agenda') OR e.dated_by IS NULL
                  OR e.document_id IS NULL OR trim(e.document_id)='' THEN 'unsupported'
             WHEN t.n IS NULL THEN 'missing' WHEN t.n=1 THEN 'found' ELSE 'ambiguous' END AS target_status,
        {pin(p,'lifecycle_events')} AS source_publication_json,
        CASE e.dated_by WHEN 'regulations_gov' THEN {pin(p,'documents')}
            WHEN 'federal_register' THEN {pin(p,'federal_register')}
            WHEN 'unified_agenda' THEN {pin(p,'regulatory_agenda_items')} END AS target_publication_json,
        'lifecycle-date-source/1' AS rule_version
    FROM lifecycle_events e LEFT JOIN targets t ON e.dated_by=t.dated_by AND e.document_id=t.lookup_key"""


LIFECYCLE_DATE_VIEWS = (
    SQLView('lifecycle_date_evidence', {
        'lifecycle_events': ('proceeding_id','document_id','source','dated_by','event_date'),
        'documents': ('document_id',), 'federal_register': ('document_number','publication_date'),
        'regulatory_agenda_items': ('agenda_item_id',),
    },date_evidence,'Date evidence routes by dated_by, independently of the source that typed the stage. '
       'A Register key includes the native publication date; posting dates never manufacture that key. '
       'Counts report selected target existence, not date accuracy or legal applicability.',
       ('proceeding_id','document_id'),rule_version='lifecycle-date-source/1',
       column_descriptions={'target_table': 'The table dated_by routes the lookup to: documents, federal_register or '
                                            'regulatory_agenda_items; NULL for an unsupported dated_by.'}),
)
