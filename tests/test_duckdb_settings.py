"""Readers of the public endpoint retry a 429 instead of failing: batch readers patiently, the MCP server briefly."""

import http.server
import threading
import time

import duckdb
import pytest

from spicy_regs.duckdb_settings import INTERACTIVE_HTTP_RETRIES, PUBLIC_HTTP_RETRIES, load_public_http


@pytest.mark.integration
@pytest.mark.parametrize("retries", [PUBLIC_HTTP_RETRIES, INTERACTIVE_HTTP_RETRIES], ids=["public", "interactive"])
def test_load_public_http_applies_the_retry_settings(retries):
    """Live: httpfs installs, and DuckDB reads back each retry setting as set (they exist only once it loads)."""
    with duckdb.connect() as con:
        load_public_http(con, retries)
        applied = dict(con.execute("SELECT name, value FROM duckdb_settings() WHERE name IN "
                                   "('http_retries', 'http_retry_wait_ms', 'http_retry_backoff')").fetchall())
    assert {name: float(value) for name, value in applied.items()} == retries


@pytest.fixture
def throttled(tmp_path):
    """A local HTTP server holding one Parquet file that answers 429 to the next ``throttle["left"]`` requests.

    Yields its URL, the throttle, and each request's time and method.
    """
    path = tmp_path / "t.parquet"
    duckdb.execute(f"COPY (SELECT range AS id FROM range(1000)) TO '{path}' (FORMAT PARQUET)")
    data, throttle, requests = path.read_bytes(), {"left": 0}, []

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, format, *args):
            pass

        def _answer(self, body: bool):
            requests.append((time.monotonic(), self.command))
            if throttle["left"] > 0:
                throttle["left"] -= 1
                self.send_response(429)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            start, end = 0, len(data) - 1
            if (requested := self.headers.get("Range", "")).startswith("bytes="):
                first, _, last = requested[6:].partition("-")
                start, end = int(first), int(last) if last else end
            self.send_response(206 if requested else 200)
            self.send_header("Content-Length", str(end - start + 1 if body else len(data)))
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("ETag", '"t"')
            if requested:
                self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
            self.end_headers()
            if body:
                self.wfile.write(data[start:end + 1])

        def do_HEAD(self):
            self._answer(False)

        def do_GET(self):
            self._answer(True)

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}/t.parquet", throttle, requests
    server.shutdown()


@pytest.mark.integration
@pytest.mark.parametrize("extra", [0, 1], ids=["within the budget", "past the budget"])
def test_duckdb_retries_a_429_on_the_documented_schedule_and_the_lock_keeps_it(throttled, extra):
    """DuckDB 1.5.5 retries a 429: the first retry at once, then ``wait * backoff**(k - 2)`` before retry k.

    The settings are applied, as the MCP server applies them, before the configuration is locked; five retries
    outlast DuckDB's default three, so a read that survives five 429s used them.
    """
    url, throttle, requests = throttled
    retries = {"http_retries": 5, "http_retry_wait_ms": 20, "http_retry_backoff": 2}
    with duckdb.connect() as con:
        con.execute("SET allow_persistent_secrets = false")
        load_public_http(con, retries)
        con.execute("SET allowed_paths = [?]", [url])
        con.execute("SET enable_external_access = false")
        con.execute("SET lock_configuration = true")
        throttle["left"] = retries["http_retries"] + extra
        started = time.monotonic()
        if extra:
            with pytest.raises(duckdb.HTTPException) as refused:
                con.execute(f"SELECT count(*) FROM read_parquet('{url}')").fetchall()
            assert refused.value.status_code == 429
        else:
            assert con.execute(f"SELECT count(*) FROM read_parquet('{url}')").fetchone() == (1000,)
        elapsed = time.monotonic() - started
    throttled_requests = [method for _, method in requests[: retries["http_retries"] + 1]]
    assert throttled_requests == ["HEAD"] * (retries["http_retries"] + 1)
    assert elapsed >= (0 + 0.02 + 0.04 + 0.08 + 0.16) * 0.9  # the backoff was waited, not skipped
