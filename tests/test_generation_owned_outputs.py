"""Owned adoption saves copies without consuming retained or aliased inputs."""
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.generations import build_generation


def output(root, name='a.parquet'):
    root.mkdir(exist_ok=True)
    path = root / name
    pq.write_table(pa.table({'id': ['kept']}), path)
    return path


def test_default_copies_but_owned_adoption_moves_same_inode(tmp_path):
    source = output(tmp_path / 'owned')
    before = source.stat().st_ino
    build_generation(tmp_path / 'copied', family='test', files=[source], expected_keys=[source.name])
    assert source.exists() and (tmp_path / 'copied' / source.name).stat().st_ino != before
    target = source.parent / 'sealed'
    build_generation(target, family='test', files=[source], expected_keys=[source.name], owned_output_root=source.parent)
    assert not source.exists() and (target / source.name).stat().st_ino == before


@pytest.mark.parametrize('kind', ['outside', 'traversal', 'hardlink', 'symlink', 'ancestor'])
def test_adoption_refuses_input_custody_aliases_before_moving(tmp_path, kind):
    root = tmp_path / 'owned'
    source = output(root)
    original = source.read_bytes()
    if kind == 'outside':
        source = output(tmp_path / 'outside')
    elif kind == 'traversal':
        output(tmp_path / 'outside')
        source = root / '..' / 'outside' / 'a.parquet'
    elif kind == 'hardlink':
        os.link(source, tmp_path / 'retained.parquet')
    elif kind == 'symlink':
        linked = root / 'alias.parquet'
        linked.symlink_to(source)
        source = linked
    elif kind == 'ancestor':
        linked = tmp_path / 'alias'
        linked.symlink_to(root, target_is_directory=True)
        source = linked / source.name
    with pytest.raises(ValueError, match='Owned|owned'):
        build_generation(root / 'sealed', family='test', files=[source], expected_keys=[source.name], owned_output_root=root)
    assert source.exists() and source.read_bytes() == original
    assert not (root / 'sealed').exists()


def test_adoption_failure_keeps_moved_files_without_artifact(tmp_path):
    root = tmp_path / 'owned'
    first = output(root)
    second = output(root, 'b.parquet')
    with pytest.raises(ValueError, match='declared schema'):
        build_generation(root / 'sealed', family='test', files=[first, second], expected_keys=['a.parquet', 'b.parquet'],
            schemas={'a': [('wrong', 'VARCHAR')]}, owned_output_root=root)
    assert (root / 'sealed/a.parquet').exists() and not first.exists()
    assert second.exists() and not (root / 'sealed/artifact.json').exists()


def test_split_adoption_moves_members_and_refuses_unexpected_files(tmp_path):
    root = tmp_path / 'owned'
    member = root / 'split/source_namespace=one/part-000000.parquet'
    member.parent.mkdir(parents=True)
    pq.write_table(pa.table({'source_namespace': ['one'], 'id': ['x']}), member)
    junk = member.parent / 'unexpected.txt'
    junk.write_text('not admitted')
    with pytest.raises(ValueError, match='unexpected'):
        build_generation(root / 'refused', family='test', files=[root / 'split'], expected_keys=['split.parquet'],
            partitioned={'split.parquet': ['source_namespace']}, owned_output_root=root)
    assert member.exists() and not (root / 'refused').exists()
    junk.unlink()
    inode = member.stat().st_ino
    build_generation(root / 'sealed', family='test', files=[root / 'split'], expected_keys=['split.parquet'],
        partitioned={'split.parquet': ['source_namespace']}, owned_output_root=root)
    assert not member.exists()
    assert (root / 'sealed/split/source_namespace=one/part-000000.parquet').stat().st_ino == inode
