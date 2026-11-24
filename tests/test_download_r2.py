"""Tests for download_r2: error handling and atomic writes.

These guard against the March 2026 incident where a silent download
failure caused the ETL to overwrite a 3.3 GB historical comments.parquet
with a fresh empty file.
"""

from contextlib import contextmanager
from unittest.mock import MagicMock

import httpx
import pytest

from spicy_regs.sources import r2 as download_r2
from spicy_regs.sources.r2 import download_from_r2


@pytest.fixture(autouse=True)
def legacy_publication(monkeypatch):
    # This module exercises legacy transfer semantics. Managed members and
    # real index requests are exercised by test_generation_publication.
    from spicy_regs.sources import publication
    monkeypatch.setattr(publication, "load_index", lambda url: publication.empty_index())
    monkeypatch.setattr(publication.time, "sleep", lambda seconds: None)  # transport retries back off


def _fake_stream(*, status_code: int = 200, body: bytes = b"", raise_exc: Exception | None = None):
    """Build a callable that mimics ``httpx.stream(...)`` as a context manager."""

    @contextmanager
    def fake_stream(method, url, follow_redirects=True, **request):
        if raise_exc is not None:
            raise raise_exc
        resp = MagicMock()
        resp.status_code = status_code
        resp.iter_bytes = lambda: iter([body]) if body else iter([])
        yield resp

    return fake_stream


class TestDownloadFromR2:
    """Pins download_from_r2's contract: False only for an unset URL or a 404.

    Anything else — 5xx, a mid-stream drop, a connection error — raises and
    leaves no new or partial target and no ``.tmp`` staging file behind.
    """
    def test_returns_false_when_r2_url_not_set(self, tmp_path, monkeypatch):
        monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
        target = tmp_path / "foo.parquet"

        assert download_from_r2("foo.parquet", target) is False
        assert not target.exists()

    def test_returns_false_on_404(self, tmp_path, monkeypatch):
        """404 is a legitimate 'not on R2 yet' signal (first run)."""
        monkeypatch.setenv("R2_PUBLIC_URL", "https://fake.r2.dev")
        monkeypatch.setattr(download_r2.httpx, "stream", _fake_stream(status_code=404))

        target = tmp_path / "missing.parquet"
        assert download_from_r2("missing.parquet", target) is False
        assert not target.exists()

    def test_raises_on_server_error(self, tmp_path, monkeypatch):
        """5xx means the object may exist — we must not silently continue,
        or the pipeline will happily overwrite production data."""
        monkeypatch.setenv("R2_PUBLIC_URL", "https://fake.r2.dev")
        monkeypatch.setattr(download_r2.httpx, "stream", _fake_stream(status_code=503))

        target = tmp_path / "foo.parquet"
        with pytest.raises(RuntimeError, match="503"):
            download_from_r2("foo.parquet", target)
        assert not target.exists()

    def test_raises_on_connection_error(self, tmp_path, monkeypatch):
        """A connection error that outlasts the bounded retries raises, never "absent"."""
        monkeypatch.setenv("R2_PUBLIC_URL", "https://fake.r2.dev")
        monkeypatch.setattr(
            download_r2.httpx,
            "stream",
            _fake_stream(raise_exc=httpx.ConnectError("conn refused")),
        )

        target = tmp_path / "foo.parquet"
        with pytest.raises(RuntimeError, match="without progress") as raised:
            download_from_r2("foo.parquet", target)
        assert isinstance(raised.value.__cause__, httpx.ConnectError)
        assert not target.exists()

    def test_writes_file_on_success(self, tmp_path, monkeypatch):
        monkeypatch.setenv("R2_PUBLIC_URL", "https://fake.r2.dev")
        monkeypatch.setattr(
            download_r2.httpx,
            "stream",
            _fake_stream(status_code=200, body=b"parquet-bytes"),
        )

        target = tmp_path / "foo.parquet"
        assert download_from_r2("foo.parquet", target) is True
        assert target.read_bytes() == b"parquet-bytes"

    def test_no_partial_file_on_mid_stream_error(self, tmp_path, monkeypatch):
        """If the stream raises mid-download, the destination file must not
        exist — otherwise callers will treat the corrupt partial as valid."""
        monkeypatch.setenv("R2_PUBLIC_URL", "https://fake.r2.dev")

        @contextmanager
        def dying_stream(method, url, follow_redirects=True, **request):
            resp = MagicMock()
            resp.status_code = 200

            def iter_bytes():
                yield b"first chunk"
                raise httpx.ReadError("connection dropped")

            resp.iter_bytes = iter_bytes
            yield resp

        monkeypatch.setattr(download_r2.httpx, "stream", dying_stream)

        target = tmp_path / "foo.parquet"
        with pytest.raises(RuntimeError, match="no ETag to resume on") as raised:  # restarted to the cap
            download_from_r2("foo.parquet", target)
        assert isinstance(raised.value.__cause__, httpx.ReadError)
        assert not target.exists()

    def test_preserves_existing_file_on_failure(self, tmp_path, monkeypatch):
        """If target already exists and download fails, the old file must
        remain untouched."""
        monkeypatch.setenv("R2_PUBLIC_URL", "https://fake.r2.dev")
        target = tmp_path / "foo.parquet"
        target.write_bytes(b"original content")

        monkeypatch.setattr(download_r2.httpx, "stream", _fake_stream(status_code=503))

        with pytest.raises(RuntimeError):
            download_from_r2("foo.parquet", target)

        assert target.read_bytes() == b"original content"

    def test_no_leftover_tmp_file_on_failure(self, tmp_path, monkeypatch):
        """The ``.tmp`` staging file must be cleaned up on any failure so
        it doesn't leak disk across retries."""
        monkeypatch.setenv("R2_PUBLIC_URL", "https://fake.r2.dev")

        @contextmanager
        def dying_stream(method, url, follow_redirects=True, **request):
            resp = MagicMock()
            resp.status_code = 200

            def iter_bytes():
                yield b"partial"
                raise httpx.ReadError("dropped")

            resp.iter_bytes = iter_bytes
            yield resp

        monkeypatch.setattr(download_r2.httpx, "stream", dying_stream)

        target = tmp_path / "foo.parquet"
        with pytest.raises(RuntimeError, match="no ETag to resume on"):
            download_from_r2("foo.parquet", target)

        # No lingering .tmp file either
        tmp_files = list(tmp_path.glob("*.tmp"))
        assert tmp_files == []


class TestDownloadAndBoundedGetRetries:
    def test_download_from_r2_retries_5xx_and_429_then_succeeds(self, tmp_path, monkeypatch):
        from spicy_regs.sources import publication

        monkeypatch.setenv("R2_PUBLIC_URL", "https://fake.r2.dev")
        codes = iter([503, 429, 200])

        @contextmanager
        def stream(method, url, follow_redirects=True, **kwargs):
            resp = MagicMock()
            resp.status_code = next(codes)
            resp.iter_bytes = lambda: iter([b"ok"])
            yield resp

        monkeypatch.setattr(publication.httpx, "stream", stream)
        target = tmp_path / "foo.parquet"
        assert download_from_r2("foo.parquet", target) is True
        assert target.read_bytes() == b"ok"

    def test_download_from_r2_does_not_retry_client_errors(self, tmp_path, monkeypatch):
        from spicy_regs.sources import publication

        monkeypatch.setenv("R2_PUBLIC_URL", "https://fake.r2.dev")
        calls = []

        @contextmanager
        def stream(method, url, follow_redirects=True, **kwargs):
            calls.append(1)
            resp = MagicMock()
            resp.status_code = 403
            yield resp

        monkeypatch.setattr(publication.httpx, "stream", stream)
        with pytest.raises(RuntimeError, match="403"):
            download_from_r2("foo.parquet", tmp_path / "foo.parquet")
        assert len(calls) == 1

    def test_bounded_get_retries_transient_errors_and_closes_streams(self, monkeypatch):
        from spicy_regs.sources import publication

        closed = []
        attempts = []

        # Attempt 1: ConnectError before headers; 2: 503; 3: mid-stream ReadTimeout; 4: succeeds.
        @contextmanager
        def scripted_stream(method, url, **kwargs):
            n = len(attempts) + 1
            attempts.append(n)
            resp = MagicMock()
            resp.close.side_effect = lambda: closed.append(n)
            if n == 1:
                raise httpx.ConnectError("refused")
            if n == 2:
                resp.status_code = 503
                yield resp
                return
            if n == 3:
                resp.status_code = 200
                resp.raise_for_status = MagicMock()

                def broken():
                    raise httpx.ReadTimeout("mid-stream timeout")

                resp.iter_bytes = broken
                yield resp
                return
            resp.status_code = 200
            resp.raise_for_status = MagicMock()
            resp.iter_bytes = lambda: iter([b"payload"])
            yield resp

        monkeypatch.setattr(publication.httpx, "stream", scripted_stream)
        assert publication._bounded_get("https://fake.r2.dev/publication.v2.json", allow_missing=True) == b"payload"
        assert attempts == [1, 2, 3, 4]
        assert closed == [2, 3, 4]

    def test_bounded_get_gives_up_and_does_not_retry_403_or_404(self, monkeypatch):
        from spicy_regs.sources import publication

        calls = []
        closed = []

        @contextmanager
        def fail_403(method, url, **kwargs):
            calls.append(1)
            resp = MagicMock()
            resp.status_code = 403
            resp.close.side_effect = lambda: closed.append(len(calls))
            resp.raise_for_status.side_effect = httpx.HTTPStatusError(
                "403", request=MagicMock(), response=resp
            )
            yield resp

        monkeypatch.setattr(publication.httpx, "stream", fail_403)
        with pytest.raises(httpx.HTTPStatusError):
            publication._bounded_get("https://fake.r2.dev/secret", allow_missing=True)
        assert len(calls) == 1 and closed == [1]

        calls.clear()
        closed.clear()

        @contextmanager
        def fail_503(method, url, **kwargs):
            calls.append(1)
            resp = MagicMock()
            resp.status_code = 503
            resp.close.side_effect = lambda: closed.append(len(calls))
            resp.raise_for_status.side_effect = httpx.HTTPStatusError(
                "503", request=MagicMock(), response=resp
            )
            yield resp

        monkeypatch.setattr(publication.httpx, "stream", fail_503)
        with pytest.raises(httpx.HTTPStatusError):
            publication._bounded_get("https://fake.r2.dev/down", allow_missing=False)
        assert len(calls) == publication._BOUNDED_GET_MAX_ATTEMPTS
        assert len(closed) == publication._BOUNDED_GET_MAX_ATTEMPTS
