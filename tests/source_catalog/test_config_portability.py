"""Public catalog config keeps portable evidence locators without opening them."""
import json
from pathlib import Path
import re

from spicy_regs.source_catalog.importer import Importer
from spicy_regs.source_catalog.raw_classification import retained_content_reviews
from spicy_regs.source_catalog.storage import Inputs, connect, schema

CONFIG = Path(__file__).resolve().parents[2] / 'src/spicy_regs/source_catalog/config'
PRIVATE_ROOT = re.compile(r'/(?:Users|private|tmp|home|Volumes)/')


def strings(value):
    if isinstance(value, dict):
        for key, item in value.items():
            yield key
            yield from strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from strings(item)
    elif isinstance(value, str):
        yield value


def test_configs_have_portable_locators_and_no_machine_local_fields():
    for name in ['registry.v1.json', 'retained-content-reviews.v1.json']:
        values = list(strings(json.loads((CONFIG / name).read_text())))
        assert not any(PRIVATE_ROOT.search(value) for value in values)
        private = [value for value in values if value.startswith('private-evidence-locator:')]
        assert private and all(re.fullmatch(r'private-evidence-locator:sha256:[a-f0-9]{64}', value) for value in private)
        for value in values:
            if value.startswith('raw-archive:'):
                member = value.removeprefix('raw-archive:')
                assert member and not Path(member).is_absolute() and '..' not in Path(member).parts
            elif value.startswith('repo:'):
                member = value.removeprefix('repo:')
                assert member.startswith(('spicy-regs/', 'spicy-docs/')) and '..' not in Path(member).parts


def test_registry_evidence_locators_are_metadata_not_file_reads(tmp_path, monkeypatch):
    registry = json.loads((CONFIG / 'registry.v1.json').read_text())
    def no_evidence_reads(*args, **kwargs):
        raise AssertionError('Registry evidence locators must not drive file reads')
    monkeypatch.setattr(Inputs, 'pin', no_evidence_reads)
    with connect(tmp_path / 'metadata.sqlite') as db:
        schema(db)
        Importer(db, Inputs(tmp_path), registry)
        assert db.execute("SELECT count(*) FROM evidence WHERE kind='observations'").fetchone()[0] == len(registry['observations'])
    assert retained_content_reviews()  # Existing strict review validation still accepts all findings.
