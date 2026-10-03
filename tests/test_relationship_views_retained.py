"""Replay native retained blocks through the held-array SQL, without provider imports."""

import hashlib
import json
from pathlib import Path
import xml.etree.ElementTree as ET

from typing import Any

import duckdb

from spicy_regs.relationship_views import install_relationship_views

FIXTURES = Path(__file__).parent / 'fixtures' / 'relationship_views'


def test_native_vote_keeps_all_document_and_amendment_occurrences():
    raw = (FIXTURES / 'senate-vote-119-1-00522.xml').read_bytes()
    assert hashlib.sha256(raw).hexdigest() == '418eb3d0635cf1f1f4e8565af24b794d24b1436dd282648742e3f1dbb752803e'
    vote = ET.fromstring(raw)
    documents = [{child.tag.removeprefix('document_'): child.text for child in node}
                 for node in vote.findall('document')]
    amendments = [{child.tag.removeprefix('amendment_'): child.text for child in node}
                  for node in vote.findall('amendment')]
    con: Any = duckdb.connect()
    con.execute('CREATE TABLE roll_call_votes(vote_id VARCHAR,documents_json VARCHAR,amendments_json VARCHAR,'
                'question VARCHAR,source_url VARCHAR)')
    con.execute('INSERT INTO roll_call_votes VALUES (?,?,?,?,?)', [
        '119-senate-1-522', json.dumps(documents), json.dumps(amendments), vote.findtext('vote_question_text'),
        'https://www.senate.gov/legislative/LIS/roll_call_votes/vote1191/vote_119_1_00522.xml',
    ])
    con.execute("ALTER TABLE roll_call_votes ADD COLUMN congress VARCHAR DEFAULT '119'")
    con.execute("ALTER TABLE roll_call_votes ADD COLUMN chamber VARCHAR DEFAULT 'senate'")
    install_relationship_views(con, ['roll_call_votes'], {'roll_call_votes': {'fixture_sha256': hashlib.sha256(raw).hexdigest()}})
    assert con.execute('SELECT count(*) FROM vote_documents_occurrences').fetchone()[0] == len(documents) == 48
    assert con.execute('SELECT count(*) FROM vote_amendments_occurrences').fetchone()[0] == len(amendments) == 48
    assert con.execute('SELECT count(*) FROM vote_amendments_pairs').fetchone()[0] == 0
    assert con.execute('SELECT target_key FROM vote_documents_occurrences WHERE source_ordinal=0').fetchone()[0] == '119:PN55-25'
    assert con.execute("SELECT count(*) FROM vote_documents_occurrences WHERE parsing_status<>'valid'").fetchone()[0] == 0
    con.execute("UPDATE roll_call_votes SET amendments_json='[]'")
    assert con.execute('SELECT count(*) FROM vote_documents_occurrences').fetchone()[0] == 48


def test_native_meeting_keeps_independent_jackets_witnesses_and_offered_documents():
    data = json.loads((FIXTURES / 'meeting-119-house-119003.json').read_text())
    con: Any = duckdb.connect()
    fields = ['congress','chamber','event_id','meeting_status','committees_json','hearing_jackets_json',
              'bill_ids_json','witnesses_json','witness_documents_json','meeting_documents_json','document_urls_json',
              'detail_read']
    con.execute('CREATE TABLE committee_meetings (' + ','.join(f'{f} VARCHAR' for f in fields) + ')')
    witness_docs = data['witnessDocuments']
    meeting_docs = data['meetingDocuments']
    con.execute('INSERT INTO committee_meetings VALUES (' + ','.join('?' for _ in fields) + ')', [
        str(data['congress']),data['chamber'].lower(),str(data['eventId']),data['meetingStatus'],
        json.dumps(data['committees']),json.dumps([str(j['jacketNumber']) for j in data['hearingTranscript']]),
        '[]',json.dumps(data['witnesses']),json.dumps(witness_docs),json.dumps(meeting_docs),
        json.dumps([d['url'] for d in witness_docs+meeting_docs]), 'true',
    ])
    install_relationship_views(con, ['committee_meetings'])
    for view, expected in [('meeting_committees',len(data['committees'])),
                           ('meeting_hearing_jackets',len(data['hearingTranscript'])),
                           ('meeting_witnesses',len(data['witnesses'])),
                           ('meeting_witness_documents',len(witness_docs)),
                           ('meeting_publications',len(meeting_docs))]:
        assert con.execute(f'SELECT count(*) FROM {view}_occurrences').fetchone()[0] == expected
    assert con.execute('SELECT count(*) FROM meeting_bills_occurrences').fetchone()[0] == 0
    assert con.execute('SELECT count(*) FROM meeting_witnesses_pairs').fetchone()[0] == 0
    saved = con.execute('SELECT raw_value_json FROM meeting_witness_documents_occurrences ORDER BY source_ordinal').fetchall()
    assert [json.loads(row[0]) for row in saved] == witness_docs


def test_native_amendment_and_treaty_use_independent_source_congress():
    con: Any = duckdb.connect()
    con.execute('CREATE TABLE roll_call_votes(vote_id VARCHAR,congress VARCHAR,chamber VARCHAR,'
                'documents_json VARCHAR,amendments_json VARCHAR,question VARCHAR,source_url VARCHAR)')
    receipts = json.loads((FIXTURES / 'amendment-treaty-provenance-2026-09-27.json').read_text())
    for filename, kind, key, congress in [
        ('senate-vote-108-2-00172.xml', 'amendment', '108-senate-2-172', '108'),
        ('senate-vote-109-1-00244.xml', 'treaty', '109-senate-1-244', '109'),
    ]:
        raw = (FIXTURES / filename).read_bytes()
        receipt = next(r for r in receipts if r['kind'] == kind)
        assert 'sha256:' + hashlib.sha256(raw).hexdigest() == receipt['sha256']
        native = ET.fromstring(raw)
        documents = [{c.tag.removeprefix('document_'): c.text for c in node} for node in native.findall('document')]
        amendments = [{c.tag.removeprefix('amendment_'): c.text for c in node} for node in native.findall('amendment')]
        con.execute('INSERT INTO roll_call_votes VALUES (?,?,?,?,?,?,?)', [
            key, congress, 'senate', json.dumps(documents), json.dumps(amendments),
            native.findtext('vote_question_text'), receipt['requested_url'],
        ])
    install_relationship_views(con, ['roll_call_votes'])
    assert con.execute('SELECT target_kind,target_key FROM vote_amendments_pairs').fetchall() == [('amendment', '108-samdt-3609')]
    assert con.execute('SELECT target_kind,target_key FROM vote_documents_pairs').fetchall() == [('treaty', '108-6')]
    assert con.execute('SELECT count(*) FROM vote_amendments_occurrences').fetchone()[0] == 2
    # Refuse disagreement or absent source context; do not use the computer's current Congress.
    con.execute("UPDATE roll_call_votes SET congress='119' WHERE vote_id='108-senate-2-172'")
    assert con.execute('SELECT count(*) FROM vote_amendments_pairs').fetchone()[0] == 0
    con.execute("UPDATE roll_call_votes SET congress=NULL")
    assert con.execute('SELECT count(*) FROM vote_amendments_pairs').fetchone()[0] == 0
    # The explicit treaty identity still stands independently of this context.
    assert con.execute('SELECT target_key FROM vote_documents_pairs').fetchall() == [('108-6',)]
