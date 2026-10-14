"""Hosted partitions retain the full default collection with disjoint execution."""
import json
import hashlib
import itertools
import re
import os
import subprocess
import sys
from pathlib import Path


def test_partitions_cover_default_collection_without_overlap(tmp_path):
    script = Path(__file__).resolve().parents[1] / 'scripts/run_ci_tests.py'
    (tmp_path / 'pytest.ini').write_text("[pytest]\naddopts = -m 'not integration'\nmarkers = integration: excluded live test\n")
    fixture_names = [f'test_fixture_{number}.py' for number in range(16)] + ['test_scorecards.py']
    for name in fixture_names:
        (tmp_path / name).write_text(
            'import pytest\ndef test_current(): pass\n'
            '@pytest.mark.parametrize("value", [0, 1])\ndef test_parameter(value): pass\n'
            '@pytest.mark.integration\ndef test_live(): raise AssertionError()\n')
    workflow = (script.parents[1] / '.github/workflows/ci.yml').read_text()
    matrix = re.search(r'partition: (\[.*\])', workflow)
    assert matrix is not None
    partitions = json.loads(matrix.group(1))
    assert partitions == list(range(4))
    assert 'python -B scripts/run_ci_tests.py' in workflow
    selected, collections = [], []
    for index in partitions:
        path = tmp_path / f'manifest-{index}.json'
        result = subprocess.run([sys.executable, '-B', str(script), str(index), '--manifest', str(path), '-q'], cwd=tmp_path,
                                env={**os.environ, 'PYTHONDONTWRITEBYTECODE': '1'},
                                capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, result.stdout + result.stderr
        [line] = [line for line in result.stdout.splitlines() if line.startswith('CI_PARTITION ')]
        summary = json.loads(line.removeprefix('CI_PARTITION '))
        manifest = json.loads(path.read_text())
        assert summary['sha256'] == hashlib.sha256(path.read_bytes()).hexdigest()
        assert summary['partitionCount'] == manifest['partitionCount'] == 4
        assert summary['selected'] == len(manifest['selectedNodeIds'])
        assert summary['collected'] == len(manifest['collectedNodeIds'])
        collections.append(set(manifest['collectedNodeIds']))
        selected.append(set(manifest['selectedNodeIds']))
    assert all(collection == collections[0] for collection in collections)
    assert all('::test_live' not in node for node in collections[0])
    assert len(collections[0]) == len(fixture_names) * 3
    assert any(node.startswith('test_scorecards.py::') for node in collections[0])
    for name in fixture_names:
        assert sum(any(node.startswith(name + '::') for node in selection) for selection in selected) == 1
    assert all(left.isdisjoint(right) for left, right in itertools.combinations(selected, 2))
    assert set.union(*selected) == collections[0]


def test_failure_stops_execution_but_manifest_retains_all_selected_tests(tmp_path):
    script = Path(__file__).resolve().parents[1] / 'scripts/run_ci_tests.py'
    file = 'test_failure.py'
    partition = int.from_bytes(hashlib.sha256(file.encode()).digest(), 'big') % 4
    (tmp_path / file).write_text(
        'def test_first(): raise AssertionError("actual failure")\n'
        'def test_second(): raise AssertionError("should not execute")\n')
    path = tmp_path / 'manifest.json'
    result = subprocess.run([sys.executable, '-B', str(script), str(partition), '--manifest', str(path), '-q'],
                            cwd=tmp_path, env={**os.environ, 'PYTHONDONTWRITEBYTECODE': '1'},
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 1
    assert 'actual failure' in result.stdout and 'should not execute' not in result.stdout
    manifest = json.loads(path.read_text())
    assert manifest['selectedNodeIds'] == manifest['collectedNodeIds'] == [
        file + '::test_first', file + '::test_second']
