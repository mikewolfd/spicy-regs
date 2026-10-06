"""Time the actual changed-source court bridge; the parent owns the 900-second qualification."""
from __future__ import annotations

import argparse
import functools
import hashlib
import importlib.util
import inspect
import json
import os
import subprocess
from pathlib import Path
import resource
import sys
import time

SOURCE_REVISION = '07915382a7a76c284cd50f70d3e8f3a0d2ea8c4f'
SITE_REVISION = '6ea9dec7cd26c2b5c2b19c80df5d53a48e2ad1ce'
BRIDGE_SHA256 = '6de7be6139b8f47049e10496f94706c2b4b4c162df954bf040de19b7081bc685'
IMPLEMENTATION_SHA256 = 'sha256:36bedaf57c9f3730143b61e728bb8ff546bca3d46a21d3944e7edc5bd2d59928'
SOURCE_SHA256 = {
    'court_receipts': '00f910680d66697cc1120d303140d02115731032be4ea6129e71b251a7a2f0e3',
    'court_subjects': '8c72df37dfb41509973e46c414630769ea2d1eb3e62eefad00b252238e70c8dc',
    'etl_bulk': '10b6705c99face37d39cbbb51c49a0b505b8ec01d2e1790d5fc5c08ff3184212',
    'etl_receipts': 'f9d05fe538d531e5cb48ea13a52e7bd1cf5ef26b7ec20b1d015b58c44efbe0ef',
    'native_types': 'ed9e60f8fcb8fff9c25bdc7e3d81bbe70cabf7f73e4d7ba92b1e7ffcbab01c53',
    'parquet_rows': 'fd83a7975d986edba0aa3d09e72ec5cc305a717bc3650d25774214e7a98aef10',
}


def file_hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class StageLog:
    """Record inclusive timings without changing a wrapped call's result or error."""

    def __init__(self, directory):
        self.directory = directory
        self.sequence = 0
        self.stack = []
        self.failures = []

    def emit(self, event):
        try:
            with (self.directory / 'stages.jsonl').open('a', encoding='utf-8') as stream:
                stream.write(json.dumps(event, sort_keys=True) + '\n')
                stream.flush()
        except Exception as error:
            # Diagnostic failures must not replace the bridge's own outcome.
            self.failures.append({'errorType': type(error).__name__, 'error': str(error)})

    def begin(self, phase):
        try:
            return self._begin(phase)
        except Exception as error:
            self.failures.append({'errorType': type(error).__name__, 'error': str(error),
                                  'phase': phase, 'diagnosticOperation': 'begin'})
            return None

    def _begin(self, phase):
        self.sequence += 1
        call = self.sequence
        parent = self.stack[-1] if self.stack else None
        self.stack.append(call)
        started = time.monotonic()
        usage = resource.getrusage(resource.RUSAGE_SELF)
        self.emit({'call': call, 'parentCall': parent, 'phase': phase, 'status': 'started',
                   'monotonicSeconds': started, 'unixSeconds': time.time()})
        return call, started, usage

    def finish(self, token, phase, status, error):
        if token is None:
            return
        try:
            self._finish(token, phase, status, error)
        except Exception as diagnostic_error:
            self.failures.append({'errorType': type(diagnostic_error).__name__,
                                  'error': str(diagnostic_error), 'phase': phase,
                                  'diagnosticOperation': 'finish'})

    def _finish(self, token, phase, status, error):
        call, started, before = token
        after = resource.getrusage(resource.RUSAGE_SELF)
        event = {'call': call, 'phase': phase, 'status': status,
                 'seconds': time.monotonic() - started,
                 'userCpuSeconds': after.ru_utime - before.ru_utime,
                 'systemCpuSeconds': after.ru_stime - before.ru_stime,
                 'processPeakRss': after.ru_maxrss,
                 'processPeakRssUnit': 'bytes' if sys.platform == 'darwin' else 'KiB',
                 'unixSeconds': time.time(), 'timingSemantics': 'inclusive; nested stages overlap'}
        if error is not None:
            event.update(errorType=type(error).__name__, error=str(error))
        if self.stack and self.stack[-1] == call:
            self.stack.pop()
        else:
            self.failures.append({'errorType': 'StageStackMismatch', 'call': call})
        self.emit(event)

    def ordinary(self, phase, original):
        @functools.wraps(original)
        def measured(*args, **kwargs):
            token = self.begin(phase)
            failure = None
            try:
                return original(*args, **kwargs)
            except BaseException as error:
                failure = error
                raise
            finally:
                self.finish(token, phase, 'failed' if failure is not None else 'completed', failure)
        return measured

    def generator(self, phase, original):
        @functools.wraps(original)
        def measured(*args, **kwargs):
            token = self.begin(phase)
            failure = None
            try:
                # Preserve the delegate's send/throw/close and terminal return.
                return (yield from original(*args, **kwargs))
            except BaseException as error:
                failure = error
                raise
            finally:
                status = 'closed' if isinstance(failure, GeneratorExit) else (
                    'failed' if failure is not None else 'completed')
                self.finish(token, phase, status, failure)
        return measured


def install(bridge, log):
    """Wrap each actual lookup/alias used by this unchanged bridge exactly once."""
    from spicy_regs import etl_bulk, etl_receipts

    bridge.MAINTAINED.checked_member = log.ordinary('inputs.checked_member', bridge.MAINTAINED.checked_member)
    bridge.MAINTAINED.file_hash = log.ordinary('files.file_hash', bridge.MAINTAINED.file_hash)
    bridge.court_receipts.select_receipts = log.ordinary(
        'processing.select_receipts', bridge.court_receipts.select_receipts)
    bridge.select_receipts = log.ordinary('witnesses.select_receipts', bridge.select_receipts)
    bridge.court_receipts.restore_processing_input = log.ordinary(
        'processing.restore_processing_input', bridge.court_receipts.restore_processing_input)
    etl_bulk._check_receipts = log.ordinary('admission._check_receipts', etl_bulk._check_receipts)
    etl_bulk._check_subjects = log.ordinary('admission._check_subjects', etl_bulk._check_subjects)
    # Both modules have a bound reference; these expose reference fallback costs.
    etl_bulk._load_receipts = log.ordinary('reference.etl_bulk._load_receipts', etl_bulk._load_receipts)
    etl_receipts._load_receipts = log.ordinary('reference.etl_receipts._load_receipts', etl_receipts._load_receipts)
    bridge.read_attempts = log.generator('witnesses.read_attempts_and_caller_grouping', bridge.read_attempts)


def configured_path(name):
    value = Path(os.environ[name])
    if not value.is_absolute():
        raise ValueError('Stage source, website and evidence paths must be absolute')
    return value.resolve()


def check_revision(path, expected):
    actual = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=path, text=True).strip()
    if actual != expected:
        raise ValueError('Checkout revision differs from the reviewed stage pin')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--schema')
    arguments = parser.parse_args()
    source = configured_path('SPICYGOV_COURT_STAGE_SOURCE')
    site = configured_path('SPICYGOV_COURT_STAGE_SITE')
    bridge_path = site / 'scripts/restore_additional_coverage.py'
    check_revision(source, SOURCE_REVISION)
    check_revision(site, SITE_REVISION)
    if file_hash(bridge_path) != BRIDGE_SHA256:
        raise ValueError('Actual unchanged website bridge differs from the reviewed pin')
    sys.path.insert(0, str(source / 'src'))
    sys.path.insert(0, str(site / 'scripts'))
    specification = importlib.util.spec_from_file_location('hosted_actual_court_bridge', bridge_path)
    if specification is None or specification.loader is None:
        raise ValueError('Actual website bridge cannot be loaded')
    bridge = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(bridge)
    imported = {}
    for name, expected in SOURCE_SHA256.items():
        module = importlib.import_module('spicy_regs.' + name)
        filename = Path(inspect.getfile(module)).resolve()
        wanted = (source / 'src/spicy_regs' / (name + '.py')).resolve()
        if filename != wanted or file_hash(filename) != expected:
            raise ValueError('Runtime imported another source implementation: ' + name)
        imported[name] = {'path': str(filename), 'sha256': expected}
    if bridge.implementation_identity() != IMPLEMENTATION_SHA256:
        raise ValueError('Actual complete bridge implementation differs from the reviewed pin')
    # Schema preflight does not consume the fresh restoration evidence path.
    sys.argv = [str(bridge_path)]
    if arguments.schema is not None:
        sys.argv.extend(['--schema', arguments.schema])
        return bridge.main()
    directory = configured_path('SPICYGOV_COURT_STAGE_DIR')
    directory.mkdir(parents=True, exist_ok=False)
    identity = {'bridgePath': str(bridge_path), 'bridgeSha256': BRIDGE_SHA256,
                'sourceRevision': SOURCE_REVISION, 'websiteRevision': SITE_REVISION,
                'importedSourceModules': imported, 'implementationSha256': IMPLEMENTATION_SHA256,
                'instrumentationPath': str(Path(__file__).resolve()),
                'instrumentationSha256': file_hash(Path(__file__)),
                'normalBridgeArgv': [str(bridge_path)], 'jsonStdin': 'forwarded unchanged',
                'qualificationSeconds': 900,
                'timingSemantics': 'inclusive; nested stages overlap; witness generator includes suspended caller grouping',
                'qualification': 'Parent verifies complete bridge stdout, all final checks and actual whole-process 900-second timer'}
    (directory / 'instrumentation-identity.json').write_text(json.dumps(identity, indent=2) + '\n')
    log = StageLog(directory)
    install(bridge, log)
    failure = None
    try:
        return log.ordinary('bridge.main', bridge.main)()
    except BaseException as error:
        failure = error
        raise
    finally:
        outcome = {'status': 'FAIL' if failure is not None else 'COMPLETE_WITH_CHECKS',
                   'errorType': type(failure).__name__ if failure is not None else None,
                   'error': str(failure) if failure is not None else None,
                   'instrumentationFailures': log.failures,
                   'scope': 'Actual bridge returned or failed; parent must verify stdout identity/count/hash/witness checks within the unchanged 900-second qualification timer.'}
        if log.failures:
            outcome['status'] = 'FAIL'
        try:
            (directory / 'instrumentation-outcome.json').write_text(json.dumps(outcome, indent=2) + '\n')
        except OSError:
            # Preserve the original return/error. Missing outcome evidence fails parent certification.
            pass


if __name__ == '__main__':
    main()
