"""A scorecard capture writes only to the archive it is given, never to the shared raw archive.

Kept from the HRC deployment research tests when docs/research was deleted (2026-11-11): these two guard
CaptureEvidence itself; the rest pinned that deployment's one-off scripts.
"""

import pytest

from spicy_docs.transport.captured import CapturedBodyResponse
from spicy_regs.source_evidence import CaptureEvidence


@pytest.fixture(autouse=True)
def deny_shared_archive(monkeypatch, tmp_path):
    """Deny the shared archive writer to every test here."""
    from pathlib import Path
    from spicy_docs.storage import raw_archive

    original = raw_archive.CentralRawArchiveStore

    def forbidden_default():
        raise AssertionError('Synthetic captures require an explicit temporary archive')

    def isolated(root, *args, **kwargs):
        assert Path(root).resolve().is_relative_to(tmp_path.resolve()), 'Shared archive writer denied'
        return original(root, *args, **kwargs)

    monkeypatch.delenv('SPICYGOV_CATALOG_REQUEST', raising=False)
    monkeypatch.setenv('SPICYGOV_RAW_ROOT', str(tmp_path / 'central-raw'))
    monkeypatch.setattr(raw_archive, 'default_raw_archive_root', forbidden_default)
    monkeypatch.setattr(raw_archive, 'CentralRawArchiveStore', isolated)


def test_missing_explicit_archive_cannot_fall_back_to_shared_writer(tmp_path):
    with pytest.raises(AssertionError, match='explicit temporary archive'):
        CaptureEvidence(tmp_path, 'scorecards')


@pytest.mark.parametrize('outcome', ['complete', 'refused', 'exception'])
def test_offline_capture_lifecycle_stays_isolated(tmp_path, outcome):
    archive = tmp_path / 'central-raw'
    evidence = CaptureEvidence(tmp_path, 'scorecards', archive_root=archive)
    scope = evidence.for_source('hrc', 'full', parser_version='fixture/1', policy_decision_id='explicit-fixture-only')
    scope.capture(CapturedBodyResponse('https://example.org/hrc.pdf', 'https://example.org/hrc.pdf',
                  200, 'application/pdf', '2026-10-03T22:43:07Z', b'%PDF-fixture'), stage='pdf')
    if outcome == 'refused':
        scope.refusal(ValueError('synthetic source refusal'), stage='pdf')
    evidence.finish(RuntimeError('synthetic interruption') if outcome == 'exception' else None)
    assert list((archive / 'acquisitions/sha256').glob('*/*'))
