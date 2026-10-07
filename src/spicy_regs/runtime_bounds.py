"""Cooperative request bounds shared by admission readers and query cursors.

Outside an interactive request these helpers preserve ordinary build behavior.
Each request owns its cancellation targets; no timer interrupts another cursor.
"""
from contextlib import contextmanager
from contextvars import ContextVar
import hashlib
from threading import Event, Lock, Timer
from time import monotonic

_current: ContextVar['RequestBound | None'] = ContextVar('spicy_request_bound', default=None)


class RequestBound:
    def __init__(self, seconds, label='request timeout'):
        self.deadline = monotonic() + seconds if seconds is not None else None
        self.label = label
        self.cancelled = Event()
        self._lock, self._targets = Lock(), set()

    def cancel(self):
        self.cancelled.set()
        with self._lock:
            for target in tuple(self._targets):
                try:
                    target.interrupt()
                except Exception:
                    pass  # An abort may already have closed its owned connection.

    def check(self):
        if self.cancelled.is_set() or (self.deadline is not None and monotonic() >= self.deadline):
            self.cancel()
            raise TimeoutError(self.label)

    @contextmanager
    def scope(self):
        token = _current.set(self)
        timer = Timer(max(0, self.deadline - monotonic()), self.cancel) if self.deadline is not None else None
        if timer is not None:
            timer.start()
        try:
            self.check()
            yield self
            self.check()
        finally:
            if timer is not None:
                timer.cancel()
            _current.reset(token)

    @contextmanager
    def connection(self, connection):
        self.check()
        with self._lock:
            self._targets.add(connection)
        try:
            self.check()
            yield connection
            self.check()
        except BaseException:
            self.check()
            raise
        finally:
            with self._lock:
                self._targets.discard(connection)


def current_bound():
    return _current.get()


def checkpoint():
    held = current_bound()
    if held is not None:
        held.check()


def remaining_timeout(default):
    held = current_bound()
    checkpoint()
    if held is None or held.deadline is None:
        return default
    remaining = max(.001, held.deadline - monotonic())
    return min(default, remaining) if default is not None else remaining


@contextmanager
def bounded_connection(connection):
    held = current_bound()
    if held is None:
        yield connection
    else:
        with held.connection(connection):
            yield connection


def stream_sha256(stream):
    if current_bound() is None:
        return hashlib.file_digest(stream, 'sha256').hexdigest()
    digest = hashlib.sha256()
    while True:
        checkpoint()
        chunk = stream.read(1024 * 1024)
        if not chunk:
            break
        digest.update(chunk)
    checkpoint()
    return digest.hexdigest()
