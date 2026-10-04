"""Ordinary court runners must pass the shared receipt and native schema hooks."""
import importlib

import pytest

from spicy_regs.court_receipts import write_court_rows
from spicy_regs.generations import verify_generation
from spicy_regs.pipelines.rollups.base import RollupPipeline

CASES = [
    ('court_citations', 'CourtCitationsRollup', {
        'court_citations': {'citation_id': '1'},
        'court_citation_map': {'citing_opinion_id': '1', 'cited_opinion_id': '2', 'depth': '3'},
        'court_parentheticals': {'parenthetical_id': '1'},
    }),
    ('court_opinions', 'CourtOpinionsRollup', {'court_opinions': {'opinion_id': '1'}}),
    ('court_opinion_clusters', 'CourtOpinionClustersRollup', {'court_opinion_clusters': {'cluster_id': '1'}}),
    ('courtlistener', 'CourtListenerRollup', {'court_dockets': {'cl_docket_id': '1', 'parties_json': '[]'}}),
]


@pytest.mark.parametrize('module_name,class_name,records', CASES)
def test_runner_admits_receipts_and_typed_subjects(tmp_path, monkeypatch, module_name, class_name, records):
    if not hasattr(RollupPipeline, 'generation_kwargs'):
        pytest.skip('Shared runner hook patch is an explicit integration dependency')
    monkeypatch.delenv('R2_PUBLIC_URL', raising=False)
    module = importlib.import_module('spicy_regs.pipelines.rollups.' + module_name)
    cls = getattr(module, class_name)

    def build(self, output):
        paths = tuple(write_court_rows(name, [row], output, generation_id='local-runner-build',
            witnesses=[{'source_id': 'fixture', 'source_uri': None, 'sha256': 'a' * 64,
                        'locator': None, 'body_version': None}]) for name, row in records.items())
        return paths if len(paths) > 1 else paths[0]

    monkeypatch.setattr(cls, 'build', build)
    cls(output_dir=tmp_path, skip_upload=True).run()
    generation, = (tmp_path / 'generations').iterdir()
    artifact = verify_generation(generation)
    assert artifact.root['spec']['etlReceipts']['generationId'] == 'local-runner-build'
    assert set(artifact.root['spec']['tables']) == {name + '.parquet' for name in records}
    assert (generation / 'etl_receipts.parquet').is_file()
