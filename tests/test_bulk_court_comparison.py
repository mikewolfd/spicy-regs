"""The SQL court comparison only ever accepts what the row comparison accepts, and refuses nothing itself.

Each case runs both routes against the same retained original, at the real comparison boundary of a real
conversion: the unchanged table, then tables with one changed cell, reordered rows, a repeated row, an added or
dropped row, a null made a value, and changed Arrow metadata or schema.
"""
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.conversion_reads import _FamilyReader
from tests.test_court_opinions_prepared_conversion import opinions
from tests.test_native_conversion import DOCKET, bucket as _bucket, convert, publish_old

bucket = _bucket


def damage(table, change, column):
    rows = table.to_pylist()
    if change == 'value':
        rows[0][column] = 'changed'
    elif change == 'null':
        rows[0][column] = None if rows[0][column] is not None else 'made-a-value'
    elif change == 'order':
        rows.reverse()
    elif change == 'repetition':
        rows[-1] = rows[0]
    elif change == 'extra':
        rows.append(rows[0])
    elif change == 'missing':
        rows.pop()
    elif change == 'metadata':
        return table.replace_schema_metadata({b'changed': b'yes'})
    elif change == 'schema':
        return table.cast(pa.schema([f.with_type(pa.large_string()) if f.name == column else f for f in table.schema]))
    return pa.Table.from_pylist(rows, schema=table.schema)


CHANGES = ('value', 'null', 'order', 'repetition', 'extra', 'missing', 'metadata', 'schema')


def verdicts(reader, dataset, original, monkeypatch, compare):
    """(what the SQL proof said or None if never asked, refusal message or None) for each route's whole comparison."""
    asked, real = [], _FamilyReader._bulk_court_equal

    def spy(*args, **kwargs):
        asked.append(real(*args, **kwargs))
        return asked[-1]

    def run():
        try:
            compare(reader, dataset, original)
        except ValueError as refused:
            return str(refused)

    with monkeypatch.context() as patch:
        patch.setattr(_FamilyReader, '_bulk_court_equal', spy)
        bulk = run()
        patch.setattr(_FamilyReader, '_bulk_court_equal', lambda *args, **kwargs: False)
        row = run()
    return (asked[0] if asked else None), bulk, row


@pytest.mark.parametrize('family, dataset, rows, column', [
    ('court-opinions', 'court_opinions', opinions() + [r | {'opinion_id': str(90 + n)} for n, r in enumerate(opinions())], 'author_str'),
    ('courtlistener', 'court_dockets', [DOCKET, DOCKET | {'cl_docket_id': '9', 'parties_json': None}], 'case_name'),
])
def test_bulk_and_row_comparisons_reach_the_same_verdict(tmp_path, monkeypatch, bucket, family, dataset, rows, column):
    publish_old(bucket, monkeypatch, tmp_path, family, {dataset: rows})
    compare, seen = _FamilyReader.compare_original, []

    def probe(reader, name, original, **kwargs):
        if not seen:
            table = pq.read_table(original)
            seen.append(('unchanged', *verdicts(reader, name, original, monkeypatch, compare)))
            for change in CHANGES:
                path = tmp_path / f'{change}.parquet'
                pq.write_table(damage(table, change, column), path)
                seen.append((change, *verdicts(reader, name, path, monkeypatch, compare)))
        return compare(reader, name, original, **kwargs)

    monkeypatch.setattr(_FamilyReader, 'compare_original', probe)
    convert(family, tmp_path / 'work')
    unchanged, *changed = seen
    assert unchanged == ('unchanged', True, None, None)
    assert [change for change, *_ in changed] == list(CHANGES)
    for change, proof, bulk, row in changed:
        # A retained schema is read from the original itself, so a different physical string type that restores
        # the same values is accepted by the row comparison; whatever it decides, the SQL route decides alike.
        assert (row is None) == (change == 'schema'), change
        assert bulk == row, change
        assert proof is not True or row is None, change  # The SQL proof accepts only what the row route accepts.
        assert proof is not True or change == 'schema', change
