"""Serving helpers must import without optional source-processing packages."""

import subprocess
import sys


def test_serving_helpers_import_without_source_dependencies():
    program = '''
import importlib.abc
import sys

class BlockSourceDependencies(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'pyarrow', 'polars', 'spicy_docs'}:
            raise AssertionError(f'Optional source dependency imported: {fullname}')
        return None

sys.meta_path.insert(0, BlockSourceDependencies())
from spicy_regs import acquisition_queue, citation_resolution, relationship_views
from spicy_regs.identifiers import action_evidence_rin, normalize_rin
assert normalize_rin(' 0648-ac64 ') == '0648-AC64'
assert action_evidence_rin('0648-XC39') is None
assert citation_resolution.normalize_rin is normalize_rin
assert acquisition_queue.QUEUE_RULE
assert relationship_views.install_relationship_views
assert 'spicy_regs.ontology' not in sys.modules
from spicy_regs.vocabulary_mapping import lookup_agency
assert lookup_agency('regulations.gov:agency', 'OPM')['status'] == 'reviewed_mapping'
assert lookup_agency('regulations.gov:agency', 'ARCTICGAS')['abstentions']
assert 'spicy_regs.ontology.common' not in sys.modules
'''
    result = subprocess.run([sys.executable, '-c', program], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr


def test_existing_identifier_imports_remain_the_same_functions():
    from spicy_regs import identifiers
    from spicy_regs.ontology import citations

    assert citations.normalize_rin is identifiers.normalize_rin
    assert citations.action_evidence_rin is identifiers.action_evidence_rin
