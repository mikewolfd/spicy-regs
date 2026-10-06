"""Hosted partitions retain the full default collection with disjoint execution."""
import json
import os
import subprocess
import sys
from pathlib import Path


def test_partitions_cover_default_collection_without_overlap(tmp_path):
    script = Path(__file__).resolve().parents[1] / 'scripts/run_ci_tests.py'
    (tmp_path / 'pytest.ini').write_text("[pytest]\naddopts = -m 'not integration'\nmarkers = integration: excluded live test\n")
    for number in range(8):
        (tmp_path / f'test_fixture_{number}.py').write_text(
            'import pytest\ndef test_current(): pass\n@pytest.mark.integration\ndef test_live(): raise AssertionError()\n')
    selected, collections = [], []
    for index in (0, 1):
        path = tmp_path / f'manifest-{index}.json'
        result = subprocess.run([sys.executable, str(script), str(index), '--manifest', str(path), '-q'], cwd=tmp_path,
                                env={**os.environ, 'PYTHONDONTWRITEBYTECODE': '1'},
                                capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, result.stdout + result.stderr
        [line] = [line for line in result.stdout.splitlines() if line.startswith('CI_PARTITION ')]
        summary = json.loads(line.removeprefix('CI_PARTITION '))
        manifest = json.loads(path.read_text())
        import hashlib
        assert summary['sha256'] == hashlib.sha256(path.read_bytes()).hexdigest()
        assert summary['selected'] == len(manifest['selectedNodeIds'])
        assert summary['collected'] == len(manifest['collectedNodeIds'])
        collections.append(set(manifest['collectedNodeIds']))
        selected.append(set(manifest['selectedNodeIds']))
    assert collections[0] == collections[1]
    assert all(node.endswith('::test_current') for node in collections[0])
    assert len(collections[0]) == 8
    assert selected[0].isdisjoint(selected[1])
    assert selected[0] | selected[1] == collections[0]
