"""Typed CourtListener endpoints: opinion IDs never stand in for cluster IDs."""

from .sql_views import SQLView, pin


def endpoint_query(source, roles):
    """Preserve every native edge while counting exact selected target rows."""
    def query(p):
        parts = []
        for role, field, target, target_field, kind in roles:
            valid = f"regexp_full_match(s.{field}, '[1-9][0-9]*')"
            parts.append(f"""SELECT s.*, '{role}' AS endpoint_role, '{kind}' AS target_kind,
                s.{field} AS target_key, coalesce(t.n,0) AS target_count,
                CASE WHEN NOT coalesce({valid},FALSE) THEN 'unsupported'
                     WHEN t.n IS NULL THEN 'missing' WHEN t.n=1 THEN 'found' ELSE 'ambiguous' END AS target_status,
                {pin(p, source)} AS source_publication_json,
                {pin(p, target)} AS target_publication_json, 'court-native-endpoint-v1' AS rule_version
                FROM {source} s LEFT JOIN
                    (SELECT {target_field}, count(*) AS n FROM {target} GROUP BY {target_field}) t
                ON {valid} AND s.{field}=t.{target_field}""")
        return ' UNION ALL '.join(parts)
    return query


def spec(name, source, source_keys, roles, extras=()):
    required = {source: tuple(dict.fromkeys((*source_keys, *(r[1] for r in roles), *extras)))}
    for _, _, target, target_field, _ in roles:
        required[target] = tuple(dict.fromkeys((*required.get(target, ()), target_field)))
    return SQLView(name, required, endpoint_query(source, roles),
                   'Native CourtListener endpoints and exact target-row counts in the selected publications. '
                   'Endpoint kinds are distinct; missing rows do not imply nonexistent cases. No case-name match, '
                   'opinion-body acquisition or Supreme Court crosswalk is inferred.', (*source_keys, 'endpoint_role'),
                   column_descriptions={'endpoint_role': 'Which end of the source edge this row looks up ('
                                        + ', '.join(role for role, *_ in roles) + '); each role is its own row of the same edge.'})


COURT_VIEWS = (
    spec('court_opinion_citation_endpoints', 'court_citation_map',
         ('citing_opinion_id','cited_opinion_id','dump_date'), (
             ('citing','citing_opinion_id','court_opinions','opinion_id','court_opinion'),
             ('cited','cited_opinion_id','court_opinions','opinion_id','court_opinion'),)),
    spec('court_opinion_cluster_endpoints', 'court_opinions', ('opinion_id',), (
        ('decision','cluster_id','court_opinion_clusters','cluster_id','court_cluster'),)),
    spec('court_cluster_docket_endpoints', 'court_opinion_clusters', ('cluster_id',), (
        ('case','cl_docket_id','court_dockets','cl_docket_id','court_docket'),)),
    spec('court_reporter_citation_endpoints', 'court_citations', ('citation_id','dump_date'), (
        ('decision','cluster_id','court_opinion_clusters','cluster_id','court_cluster'),)),
    spec('court_parenthetical_endpoints', 'court_parentheticals', ('parenthetical_id','dump_date'), (
        ('described','described_opinion_id','court_opinions','opinion_id','court_opinion'),
        ('describing','describing_opinion_id','court_opinions','opinion_id','court_opinion'),)),
)
