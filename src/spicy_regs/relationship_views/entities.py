"""UEI navigation separates entity-level enrichment from SAM registration rows."""

from .sql_views import SQLView, pin

# Shape validation only. The exact retained spelling is used for every join;
# normalization and provider entity adjudication are deliberately not attempted.
UEI = "regexp_full_match(uei, '[A-Z0-9]{12}')"
SAM_REQUIRED = ('uei', 'entity_eft_indicator')
RECIPIENT_REQUIRED = ('recipient_id', 'uei', 'recipient_level', 'duns', 'total_award_amount')


def identifiers(p):
    return f"""SELECT uei, 'sam' AS source_namespace, {pin(p, 'sam_entities')} AS source_publication_json
        FROM sam_entities WHERE {UEI} GROUP BY uei
        UNION ALL
        SELECT uei, 'usaspending' AS source_namespace,
            {pin(p, 'usaspending_recipients')} AS source_publication_json
        FROM usaspending_recipients WHERE {UEI} GROUP BY uei"""


def recipient_entities(p):
    return f"""WITH registrations AS (
        SELECT uei, count(*) AS registration_count FROM sam_entities WHERE {UEI} GROUP BY uei)
        SELECT r.*, CASE WHEN NOT COALESCE(regexp_full_match(r.uei, '[A-Z0-9]{{12}}'), FALSE)
                        THEN 'unsupported' WHEN s.uei IS NULL THEN 'missing' ELSE 'found' END AS entity_status,
            s.registration_count,
            {pin(p, 'usaspending_recipients')} AS source_publication_json,
            {pin(p, 'sam_entities')} AS target_publication_json,
            'uei-exact-entity-v1' AS rule_version
        FROM usaspending_recipients r LEFT JOIN registrations s
            ON r.uei=s.uei AND regexp_full_match(r.uei, '[A-Z0-9]{{12}}')"""


def registrations(p):
    return f"""SELECT uei, entity_eft_indicator,
        CASE WHEN {UEI} THEN 'valid_shape' ELSE 'unsupported' END AS identifier_status,
        {pin(p, 'sam_entities')} AS source_publication_json, 'uei-exact-registration-v1' AS rule_version
        FROM sam_entities"""

ENTITY_VIEWS = (
    SQLView('uei_identifiers', {'sam_entities': SAM_REQUIRED, 'usaspending_recipients': RECIPIENT_REQUIRED},
            identifiers, 'Distinct literal UEIs per source namespace. Shape checks do not establish identity; '
            'the source remains visible rather than merging provider observations.', ('source_namespace', 'uei'),
            column_descriptions={
                'uei': 'Literal 12-character UEI as the named source spells it; one row per source namespace holding it.',
                'source_namespace': 'Which source holds the UEI: sam or usaspending.',
            }),
    SQLView('recipient_sam_entities', {'sam_entities': SAM_REQUIRED, 'usaspending_recipients': RECIPIENT_REQUIRED},
            recipient_entities, 'One row per held spending recipient, enriched only with the count of matching SAM '
            'registrations. Amounts, provider IDs, levels, DUNS and observation dates stay on the original recipient '
            'row. No first registration is selected and parent/child amounts must not be summed as disjoint.',
            ('recipient_id',),
            column_descriptions={
                'entity_status': 'found when at least one SAM registration carries the recipient UEI, missing when none, '
                                 'unsupported when the UEI is not 12 alphanumerics.',
                'registration_count': 'Number of sam_entities rows with this UEI; NULL when none.',
            }),
    SQLView('sam_uei_registrations', {'sam_entities': SAM_REQUIRED}, registrations,
            'Literal UEI to native EFT registration key. Multiple registrations remain separate; NULL EFT is not '
            'rewritten to an empty identifier. Join monetary rows to recipient_sam_entities instead.',
            ('uei', 'entity_eft_indicator'),
            column_descriptions={'identifier_status': 'valid_shape when the UEI is 12 alphanumerics, else unsupported; '
                                                      'a shape check, not registration validity.'}),
)
