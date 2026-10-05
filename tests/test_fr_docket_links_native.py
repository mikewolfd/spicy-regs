"""Native admission preserves the Register's two docket-link statements."""
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import native_conversion as conversion
from spicy_regs.etl_receipts import ReceiptContext
from spicy_regs.pipelines.rollups.fr_docket_links import FrDocketLinksRollup
from spicy_regs.pipelines.rollups.subject_receipts import SubjectReceiptRollup
from spicy_regs.transforms.build_fr_docket_links import LINK_COLUMNS
from spicy_regs.transforms.regulations_receipts import policy, write_records


def test_links_use_the_maintained_receipt_rollup():
    assert issubclass(FrDocketLinksRollup, SubjectReceiptRollup)
    assert conversion._rollup_class('fr-docket-links') is FrDocketLinksRollup


def test_retained_link_statements_restore_exactly_through_native_admission(tmp_path):
    schema = pa.schema([(name, pa.int64() if name == 'docket_source_ordinal' else pa.string())
                        for name in LINK_COLUMNS], metadata={b'source-note': b'literal\x00bytes'})
    rows = []
    for ordinal, source, docket in [(0, 'printed', 'Docket No. A'), (1, 'printed', 'Docket No. A'),
                                     (2, 'both', 'A'), (None, 'regulations_dot_gov_info', 'B'),
                                     (None, 'regulations_dot_gov_info', 'C'), (3, None, 'historical')]:
        row = dict.fromkeys(LINK_COLUMNS)
        row.update(document_number='2026-00001', publication_date='2026-10-01', docket_id=docket,
                   docket_source_ordinal=ordinal, link_source=source, docket_ids_json='["Docket No. A", "Docket No. A", "A"]',
                   normalized_docket_candidates_json='["A"]', agency_slugs='epa,epa',
                   regulation_id_numbers_json='["1000-AA00", null, ""]', title='Retained statement')
        rows.append(row)
    held = tmp_path / 'fr_docket_links.parquet'
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), held)
    cls = conversion._rollup_class('fr-docket-links')
    assert cls is not None
    built = conversion._convert_rollup(cls, {'tables': {'fr_docket_links.parquet': {}}},
                                       {'fr_docket_links.parquet': held}, '', tmp_path / 'converted', [])
    restored = built.restore('fr_docket_links')
    assert pq.read_table(restored).equals(pq.read_table(held), check_metadata=True)
    assert pq.read_schema(restored).equals(schema, check_metadata=True)
    # Admission creates a separate receipt member; the public source marker remains literal text.
    native = pq.read_table(built.generation / 'fr_docket_links.parquet')
    assert native['link_source'].to_pylist() == [row['link_source'] for row in rows]
    receipts = pq.read_table(built.generation / 'etl_receipts.parquet').to_pylist()
    accepted = [row for row in receipts if row['outcome'] == 'accepted']
    assert len(accepted) == len(rows)
    assert len({row['record_id'] for row in accepted}) == len(rows)


def test_nullable_ordinal_keeps_document_identity_required_and_refusals_retained(tmp_path):
    good = {'document_number': '2026-00002', 'publication_date': '2026-10-01',
            'docket_source_ordinal': None, 'docket_id': 'B', 'link_source': 'regulations_dot_gov_info'}
    rows = [good, {**good, 'document_number': None}, {**good, 'publication_date': None},
            {**good, 'docket_id': None}, {**good, 'undeclared_source_field': 'must refuse'}]
    contexts = [ReceiptContext('g', str(i), 'new-link-regression', [{'source_id': 'retained-link',
                'source_uri': 'https://example.gov/links', 'sha256': 'a' * 64,
                'locator': f'/rows/{i}', 'body_version': None}]) for i in range(len(rows))]
    subject, receipt = write_records('fr_docket_links', zip(rows, contexts), tmp_path / 'written')
    assert pq.read_table(subject)['document_number'].to_pylist() == ['2026-00002']
    observations = pq.read_table(receipt).to_pylist()
    assert [row['outcome'] for row in observations] == ['accepted', 'refused', 'refused', 'refused', 'refused']
    assert 'undeclared_source_field' in observations[-1]['processing_json']
    declared = json.loads((Path(__file__).parents[1] /
                           'src/spicy_regs/etl_policies/fr_docket_links.json').read_text())
    assert declared == policy('fr_docket_links').descriptor()


def test_same_exact_link_identity_refuses_ambiguous_receipt_join(tmp_path):
    row = {'document_number': '2026-00003', 'publication_date': '2026-10-01',
           'docket_source_ordinal': None, 'docket_id': 'B', 'link_source': 'regulations_dot_gov_info'}
    contexts = [ReceiptContext('g', str(i), 'new-link-collision', [{'source_id': 'held',
                'source_uri': 'https://example.gov/links', 'sha256': 'b' * 64,
                'locator': f'/rows/{i}', 'body_version': None}]) for i in range(2)]
    with pytest.raises(ValueError, match='Duplicate or ambiguous receipt join'):
        write_records('fr_docket_links', zip([row, row], contexts), tmp_path / 'duplicate')
