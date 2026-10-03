"""Date-qualified native party intervals; no implicit term-party fallback."""

from .sql_views import SQLView, pin

AFFILIATION_FIELDS = ('bioguide_id','input_sha256','term_index','affiliation_index','party',
                      'affiliation_start','affiliation_end','start_status','end_status','term_party',
                      'source_path','source_json','observed_at')


def dated_parties(p):
    valid = """a.start_status='valid' AND a.end_status='valid'
        AND TRY_CAST(a.affiliation_start AS DATE) IS NOT NULL AND TRY_CAST(a.affiliation_end AS DATE) IS NOT NULL
        AND TRY_CAST(a.affiliation_start AS DATE)<TRY_CAST(a.affiliation_end AS DATE)"""
    matches = f"""{valid} AND TRY_CAST(a.affiliation_start AS DATE)<=TRY_CAST(v.vote_day AS DATE)
        AND TRY_CAST(v.vote_day AS DATE)<TRY_CAST(a.affiliation_end AS DATE)"""
    return f"""SELECT v.*,m.source_affiliation_count,m.unusable_interval_count,m.target_count,
        CASE WHEN m.target_count=1 THEN m.candidates[1].party ELSE NULL END AS dated_party,
        CASE WHEN TRY_CAST(v.vote_day AS DATE) IS NULL OR v.term_index IS NULL OR v.bioguide_id IS NULL
                  THEN 'unsupported' WHEN m.target_count=0 THEN 'missing'
             WHEN m.target_count=1 THEN 'found' ELSE 'ambiguous' END AS target_status,
        CASE WHEN m.target_count>1 THEN 'ambiguous'
             WHEN m.target_count=1 AND m.candidates[1].party IS NOT NULL THEN 'source_interval'
             ELSE 'unknown' END AS party_status,
        to_json(m.candidates)::VARCHAR AS affiliation_candidates_json,
        {pin(p, 'member_vote_terms')} AS source_publication_json,
        {pin(p, 'member_party_affiliations')} AS target_publication_json,
        'native-party-half-open-v1' AS rule_version
        FROM member_vote_terms v LEFT JOIN LATERAL (
            SELECT count(*) AS source_affiliation_count,
                count(*) FILTER (WHERE NOT coalesce(({valid}),FALSE)) AS unusable_interval_count,
                count(*) FILTER (WHERE {matches}) AS target_count,
                list(struct_pack(input_sha256:=a.input_sha256,term_index:=a.term_index,
                    affiliation_index:=a.affiliation_index,party:=a.party,
                    affiliation_start:=a.affiliation_start,affiliation_end:=a.affiliation_end,
                    source_path:=a.source_path,source_json:=a.source_json,observed_at:=a.observed_at))
                    FILTER (WHERE {matches}) AS candidates
            FROM member_party_affiliations a
            WHERE a.bioguide_id=v.bioguide_id AND a.term_index=v.term_index
        ) m ON TRUE"""


COLUMNS = {
    'source_affiliation_count': 'Number of member_party_affiliations intervals held for this member and term, usable or not.',
    'unusable_interval_count': 'Intervals with an absent, invalid or inverted start or end date; they never match a vote day.',
    'dated_party': 'The party of the single interval containing vote_day (start <= vote_day < end); NULL when none or several match.',
    'party_status': 'source_interval when exactly one interval states a party, ambiguous when several match, unknown '
                    'when no interval covers the day.',
    'affiliation_candidates_json': 'JSON list of every matching interval with its capture digest, positions, dates, '
                                   'source path and observed_at.',
}

AFFILIATION_VIEWS = (
    SQLView('member_vote_party_affiliations', {
        'member_vote_terms': ('vote_id','member_key','bioguide_id','term_index','vote_day'),
        'member_party_affiliations': AFFILIATION_FIELDS,
    }, dated_parties, 'One row per held vote/member position. Native party intervals use start <= vote_day < end; '
       'missing/invalid end dates never mean open-ended. Gaps stay unknown, overlaps or multiple captures stay '
       'ambiguous, and no latest-capture or term-level party fallback is chosen. The crosswalk records an interval '
       'only for a member whose party changed within a term, so most positions read unknown/missing here. For the '
       'party the roll-call file states on the vote day, join member_votes.party on (vote_id, member_key): the '
       "publisher's own statement, not an inference; members.current_term_party is the latest term's party, undated. "
       'Candidate JSON retains capture digests and source occurrence positions.', ('vote_id','member_key'),
       column_descriptions=COLUMNS),
)
