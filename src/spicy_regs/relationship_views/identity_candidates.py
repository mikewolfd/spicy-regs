"""Expose existing name matches as pending candidates, never accepted identity."""

from .core import literal
from .sql_views import SQLView, pin

SOURCE = 'org_committee_links'
FIELDS = ('organization','organization_norm','organization_core','name_source','committee_id','committee_name',
          'match_method','confidence','committee_match_count','first_comment_date','last_comment_date')


def candidates(p):
    selected = p.get(SOURCE)
    pinned = isinstance(selected, dict) and bool(selected.get('artifact_digest') or selected.get('sha256'))
    identity = f"""'org_candidate_' || sha256(to_json(struct_pack(
        source_publication:=CAST({pin(p, SOURCE)} AS JSON), organization:=organization,
        name_source:=name_source, committee_id:=committee_id)))""" if pinned else 'NULL::VARCHAR'
    return f"""SELECT *, {identity} AS candidate_id,
        {literal('publication_scoped' if pinned else 'unversioned_source')} AS candidate_id_status,
        organization AS observed_name, 'fec_committee' AS target_namespace,
        committee_id AS candidate_target_id, 'pending' AS decision, 'unknown' AS acting_role,
        to_json(struct_pack(match_method:=match_method,confidence:=confidence,
            organization_norm:=organization_norm,organization_core:=organization_core,
            committee_name:=committee_name))::VARCHAR AS matcher_evidence_json,
        to_json(struct_pack(committee_match_count:=committee_match_count,
            multiple_name_matches:=TRY_CAST(committee_match_count AS BIGINT)>1))::VARCHAR AS competing_candidates_json,
        NULL::VARCHAR AS accepted_identity_evidence, NULL::VARCHAR AS decision_at,
        {pin(p, SOURCE)} AS source_publication_json,
        'existing-name-candidates-v1' AS rule_version
        FROM org_committee_links"""


COLUMNS = {
    'candidate_id': "Stable candidate key: 'org_candidate_' plus a digest of the pinned source publication, "
                    'organization, name_source and committee_id; NULL when the source is unpinned.',
    'candidate_id_status': 'publication_scoped when candidate_id is bound to a pinned source digest, '
                           'unversioned_source when none is available.',
    'target_namespace': 'Always fec_committee: the identifier space of candidate_target_id.',
    'decision': 'Always pending: no acceptance, rejection or revocation workflow exists in this view.',
    'acting_role': 'Always unknown: no role (filer, funder, affiliate) is asserted.',
    'matcher_evidence_json': 'JSON of the recorded matcher features: match_method, confidence, normalized and core '
                             'names, committee_name.',
    'competing_candidates_json': 'JSON of committee_match_count and whether more than one committee name matched.',
    'accepted_identity_evidence': 'Always NULL: no identity has been accepted.',
    'decision_at': 'Always NULL: no decision has been recorded.',
}

IDENTITY_VIEWS = (
    SQLView('org_identity_candidates', {SOURCE: FIELDS}, candidates,
            'Existing organization-name matches are pending candidates. Matching names, confidence labels and '
            'multiple alternatives are recorded features, not adjudicated positive/conflicting identity evidence. '
            'Original comment date bounds are not validity dates. No common person/organization, affiliate network '
            'or funds attribution is established. Candidate IDs require a pinned source publication; no qualified '
            'acceptance, rejection or revocation workflow is implemented by this view.', ('candidate_id',),
            column_descriptions=COLUMNS),
)


def native_candidates(publication):
    selected = publication.get(SOURCE)
    pinned = isinstance(selected, dict) and bool(selected.get('artifact_digest') or selected.get('sha256'))
    identity = f"""'org_candidate_' || sha256(to_json(struct_pack(
        source_publication:=CAST({pin(publication, SOURCE)} AS JSON), organization:=organization,
        committee_id:=committee_id)))""" if pinned else 'NULL::VARCHAR'
    return f"""SELECT *, {identity} AS candidate_id,
        {literal('publication_scoped' if pinned else 'unversioned_source')} AS candidate_id_status,
        organization AS observed_name, 'fec_committee' AS target_namespace,
        committee_id AS candidate_target_id, 'pending' AS decision, 'unknown' AS acting_role,
        struct_pack(committee_match_count:=committee_match_count,
            multiple_name_matches:=committee_match_count>1) AS competing_candidates,
        NULL::VARCHAR AS accepted_identity_evidence, NULL::VARCHAR AS decision_at,
        {pin(publication, SOURCE)} AS source_publication_json,
        'native-name-candidates/2' AS rule_version
        FROM org_committee_links"""


NATIVE_IDENTITY_VIEWS = (
    SQLView('org_identity_candidates', {
        SOURCE: ('organization', 'committee_id', 'committee_name', 'committee_match_count',
                 'first_comment_date', 'last_comment_date', 'association_kind'),
    }, native_candidates,
       'Organization name associations remain pending candidates, not accepted identity. '
       'Candidate IDs bind the selected domain row and publication. The match method, confidence '
       'label and sponsor-name comparison are columns of the source row; date bounds '
       'describe comments and do not establish validity. Competing counts are native structs.',
       ('candidate_id',), rule_version='native-name-candidates/2', column_descriptions={
           **{name: text for name, text in COLUMNS.items() if name not in {'matcher_evidence_json', 'competing_candidates_json'}},
           'candidate_id': "Stable candidate key from the pinned publication, organization and committee_id; NULL when unpinned.",
           'competing_candidates': "Native struct of the recorded match count and whether several committee names matched.",
       }),
)
