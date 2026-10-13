"""Bounded phase metadata; never reads source data or acquires the connection lock."""
from collections import deque
from contextlib import contextmanager
import threading
from time import monotonic

_lock = threading.Lock()
_active = {}
_recent = deque(maxlen=32)
_max_active = 16
_untracked = 0

@contextmanager
def phase(name, table=None):
    """Track one synchronous phase, restoring an enclosing phase on exit."""
    global _untracked
    if not isinstance(name, str) or len(name) > 64:
        raise ValueError('Diagnostic phase name exceeds bound')
    if table is not None and (not isinstance(table, str) or len(table) > 160):
        table = '<identifier exceeds diagnostic bound>'
    thread = threading.get_ident()
    started = monotonic()
    with _lock:
        previous = _active.get(thread)
        tracked = previous is not None or len(_active) < _max_active
        if tracked:
            record = {'phase': name, 'table': table, 'started': started,
                      'operation_started': previous['operation_started'] if previous else started}
            _active[thread] = record
        else:
            _untracked += 1
    error = None
    try:
        yield
    except BaseException as exc:
        error = type(exc).__name__
        raise
    finally:
        ended = monotonic()
        with _lock:
            if tracked:
                _recent.append({'phase': name, 'table': table,
                                'seconds': max(0, ended - started), 'error_type': error})
                if previous is None:
                    _active.pop(thread, None)
                else:
                    _active[thread] = previous
            else:
                _untracked -= 1

def snapshot():
    """Copy small diagnostic records; no publication, database or tool-slot access."""
    now = monotonic()
    with _lock:
        active = [{'phase': value['phase'], 'table': value['table'],
                   'phase_seconds': max(0, now - value['started']),
                   'operation_seconds': max(0, now - value['operation_started'])}
                  for value in _active.values()]
        recent = [dict(value) for value in _recent]
        untracked = _untracked
    return {'active': active, 'recent': recent, 'untracked_active_phases': untracked,
            'scope': 'In-process phase observations only; no progress, readiness or source qualification inferred.'}
