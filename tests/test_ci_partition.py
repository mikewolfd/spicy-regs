"""Hosted partitions retain the full default collection with disjoint execution."""
import json
import hashlib
import itertools
import re
import os
import runpy
import subprocess
import sys
from pathlib import Path

import pytest
import yaml


SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/run_ci_tests.py'


def git(root, *arguments):
    return subprocess.run(['git', *arguments], cwd=root, check=True, capture_output=True, text=True).stdout.strip()


def commit(root):
    git(root, 'add', '.')
    git(root, '-c', 'user.name=CI fixture', '-c', 'user.email=ci@example.invalid', 'commit', '-qm', 'fixture')
    return git(root, 'rev-parse', 'HEAD')


@pytest.fixture
def repository(tmp_path):
    git(tmp_path, 'init', '-q')
    (tmp_path / 'tests').mkdir()
    (tmp_path / 'tests/test_leaf.py').write_text('def test_leaf(): pass\n')
    (tmp_path / 'tests/test_other.py').write_text('def test_other(): pass\n')
    return tmp_path, commit(tmp_path)


@pytest.mark.parametrize('path', [
    'tests/test_leaf.py', 'tests/test_added.py', 'tests/conftest.py', 'tests/helpers.py',
    'tests/__init__.py', 'tests/fixtures/data.json', 'tests/nested/test_leaf.py',
    'src/runtime.py', 'pyproject.toml', 'uv.lock', '.github/workflows/ci.yml',
])
def test_only_top_level_leaf_test_changes_are_selected(repository, monkeypatch, path):
    root, base = repository
    changed = root / path
    changed.parent.mkdir(parents=True, exist_ok=True)
    changed.write_text('def test_changed(): pass\n')
    head = commit(root)
    monkeypatch.chdir(root)
    select = runpy.run_path(str(SCRIPT))['leaf_test_selection']
    selected, _ = select('pull_request', base, head)
    assert selected == ([path] if path in {'tests/test_leaf.py', 'tests/test_added.py'} else None)
    assert select('push', base, head)[0] is None
    assert select('pull_request', '', head)[0] is None
    assert select('pull_request', base, 'unavailable')[0] is None
    assert select('pull_request', base, base)[0] is None


@pytest.mark.parametrize('change', ['delete', 'rename', 'live', 'shared', 'removed-reference'])
def test_deletions_live_tests_and_shared_test_modules_keep_full_suite(repository, monkeypatch, change):
    root, base = repository
    leaf = root / 'tests/test_leaf.py'
    if change == 'removed-reference':
        (root / 'tests/test_other.py').write_text('from tests.test_leaf import test_leaf\n')
        base = commit(root)
        (root / 'tests/test_other.py').write_text('def test_other(): pass\n')
    if change == 'delete':
        leaf.unlink()
    elif change == 'rename':
        leaf.rename(root / 'tests/test_renamed.py')
    elif change == 'live':
        leaf.write_text('import pytest\n@pytest.mark.integration\ndef test_live(): pass\n')
    else:
        leaf.write_text('def test_leaf(): assert True\n')
        if change == 'shared':
            (root / 'tests/test_other.py').write_text('from tests.test_leaf import test_leaf\n')
    head = commit(root)
    monkeypatch.chdir(root)
    select = runpy.run_path(str(SCRIPT))['leaf_test_selection']
    assert select('pull_request', base, head)[0] is None


def test_leaf_pr_runs_changed_tests_once_and_records_empty_partitions(repository):
    root, _ = repository
    (root / 'tests/test_other.py').write_text('def test_unrelated(): raise AssertionError("unrelated executed")\n')
    base = commit(root)
    (root / 'tests/test_leaf.py').write_text('def test_first(): pass\ndef test_second(): pass\n')
    head = commit(root)
    env = {**os.environ, 'GITHUB_EVENT_NAME': 'pull_request', 'CHANGE_BASE': base, 'CHANGE_HEAD': head}
    decision = subprocess.run([sys.executable, '-B', str(SCRIPT), '--leaf-test-only'], cwd=root,
                              env=env, capture_output=True, text=True, timeout=30)
    assert decision.returncode == 0, decision.stdout + decision.stderr
    selected = []
    for index in range(4):
        manifest_path = root / f'manifest-{index}.json'
        result = subprocess.run([sys.executable, '-B', str(SCRIPT), str(index), '--manifest', str(manifest_path), '-q'],
                                cwd=root, env=env, capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, result.stdout + result.stderr
        manifest = json.loads(manifest_path.read_text())
        assert manifest['selectionMode'] == 'leaf-tests'
        assert manifest['changedTestFiles'] == ['tests/test_leaf.py']
        assert len(manifest['collectedNodeIds']) == 3
        selected.extend(manifest['selectedNodeIds'])
    assert sorted(selected) == ['tests/test_leaf.py::test_first', 'tests/test_leaf.py::test_second']


def test_changed_module_without_default_tests_falls_back_to_full_collection(tmp_path):
    namespace = runpy.run_path(str(SCRIPT))
    partition = namespace['Partition'](0, tmp_path / 'manifest.json', ['tests/test_empty.py'])
    # Exercise the collection boundary without importing the repository's suite.
    from types import SimpleNamespace

    paths = [f'tests/test_fixture_{number}.py' for number in range(16)]
    items = [SimpleNamespace(nodeid=path + '::test_current') for path in paths]
    partition.pytest_collection_modifyitems(SimpleNamespace(hook=SimpleNamespace(pytest_deselected=lambda **_: None)), items)
    manifest = json.loads((tmp_path / 'manifest.json').read_text())
    assert manifest['selectionMode'] == 'full'
    assert manifest['changedTestFiles'] is None
    assert manifest['selectedNodeIds']


@pytest.mark.parametrize('event,path,expected', [
    ('pull_request', 'tests/test_leaf.py', 'false'),
    ('pull_request', 'tests/test_live.py', 'true'),
    ('pull_request', 'tests/conftest.py', 'true'),
    ('pull_request', 'data_dictionary/descriptions.yaml', 'true'),
    ('pull_request', 'docs/research/fork-output-ledger-2026-09-21.md', 'true'),
    ('pull_request', 'src/spicy_regs/legislative_receipts.py', 'true'),
    ('pull_request', 'docs/tables/example.md', 'false'),
    ('schedule', 'tests/test_leaf.py', 'true'),
    ('workflow_dispatch', 'tests/test_leaf.py', 'true'),
    ('pull_request', 'unavailable-diff', 'true'),
])
def test_live_workflow_preserves_source_controls_and_scheduled_checks(repository, event, path, expected):
    root, _ = repository
    (root / 'scripts').mkdir()
    (root / 'scripts/run_ci_tests.py').write_bytes(SCRIPT.read_bytes())
    base = commit(root)
    changed = root / path
    changed.parent.mkdir(parents=True, exist_ok=True)
    changed.write_text('import pytest\npytestmark = pytest.mark.integration\n' if 'live' in path else 'changed\n')
    head = commit(root)
    workflow = yaml.safe_load((SCRIPT.parents[1] / '.github/workflows/integration.yml').read_text())
    shell = next(step['run'] for step in workflow['jobs']['integration']['steps'] if step.get('id') == 'changes')
    output = root / 'outputs.txt'
    env = {**os.environ, 'EVENT_NAME': event, 'GITHUB_EVENT_NAME': event, 'CHANGE_BASE': base,
           'CHANGE_HEAD': 'unavailable' if path == 'unavailable-diff' else head, 'GITHUB_OUTPUT': str(output)}
    result = subprocess.run(['bash', '-e', '-c', shell], cwd=root, env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
    assert output.read_text().strip() == 'run=' + expected


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
