"""Tool calls run on worker threads, each on its own cursor, at most TOOL_CONCURRENCY at once, off the event loop."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import duckdb
import pytest
from starlette.testclient import TestClient

from spicy_regs import mcp_server
from tests.test_mcp_query_results import call
from tests.test_mcp_server import _records

AGGREGATE = "SELECT MIN(date_received) AS min_dr, MAX(date_received) AS max_dr, COUNT(*) AS n FROM fcc_filings"
FEC = "SELECT subject_id, subject_type FROM fec_relationships WHERE subject_id = 'C00154625' LIMIT 10"


@pytest.fixture
def connection():
    with duckdb.connect() as con:
        con.execute("CREATE TABLE fcc_filings AS SELECT '2026-09-25' AS date_received FROM range(3)")
        con.execute("CREATE TABLE fec_relationships AS SELECT 'C00154625' AS subject_id, 'committee' AS subject_type")
        yield con


def test_overlapping_query_sql_calls_each_get_their_own_reply(connection, monkeypatch):
    """Overlapping calls on the shared connection, now on real worker threads, never cross results (2026-09-28)."""
    monkeypatch.setattr(mcp_server, "_get_connection", lambda: connection)
    cases = [(AGGREGATE, 5, ["min_dr", "max_dr", "n"]), (FEC, 25, ["subject_id", "subject_type"])] * 32
    with TestClient(mcp_server.build_app()) as http, ThreadPoolExecutor(max_workers=16) as pool:
        replies = list(pool.map(lambda case: call(http, "query_sql", {"sql": case[0], "max_rows": case[1]}), cases))
    for (sql, max_rows, columns), reply in zip(cases, replies, strict=True):
        body = reply["structuredContent"]
        assert (body["sql"], body["columns"], body["max_rows"]) == (sql, columns, max_rows)


def test_the_event_loop_answers_while_a_tool_call_waits(connection, monkeypatch):
    """A sync tool ran on the event loop, so one slow call (a 43 s connection build) stalled every request."""
    entered, release = threading.Event(), threading.Event()

    def waiting_connection():
        entered.set()
        assert release.wait(10)
        return connection

    monkeypatch.setattr(mcp_server, "_get_connection", waiting_connection)
    replies, pages = [], []
    with TestClient(mcp_server.build_app()) as http:
        slow = threading.Thread(target=lambda: replies.append(call(http, "query_sql", {"sql": "SELECT 1 AS one"})))
        slow.start()
        assert entered.wait(10)
        page = threading.Thread(target=lambda: pages.append(http.get("/").status_code))
        page.start()
        page.join(5)
        answered = list(pages)
        release.set()
        slow.join(10)
        page.join(10)
    assert answered == [200]  # before the tool call finished
    assert _records(replies[0]["structuredContent"]) == [{"one": 1}]


def test_tool_calls_beyond_the_limit_wait_for_a_worker(connection, monkeypatch):
    """The limiter bounds concurrent scans, and with them the request rate against the public endpoint."""
    monkeypatch.setattr(mcp_server, "TOOL_CONCURRENCY", 2)
    lock, release = threading.Lock(), threading.Event()
    counts = {"active": 0, "peak": 0}

    def counted_connection():
        with lock:
            counts["active"] += 1
            counts["peak"] = max(counts["peak"], counts["active"])
        try:
            assert release.wait(10)
            return connection
        finally:
            with lock:
                counts["active"] -= 1

    monkeypatch.setattr(mcp_server, "_get_connection", counted_connection)
    with TestClient(mcp_server.build_app()) as http, ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(call, http, "query_sql", {"sql": "SELECT 1 AS one"}) for _ in range(4)]
        deadline = time.monotonic() + 10
        while counts["active"] < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        time.sleep(0.2)  # time for a third call to enter, were the limit not holding it
        assert counts["peak"] == 2
        release.set()
        assert [future.result(10)["isError"] for future in futures] == [False] * 4


def test_a_statement_timeout_interrupts_only_its_own_cursor(connection, monkeypatch):
    """Each call's timer interrupts its own cursor, never a sibling call running on the same connection."""
    monkeypatch.setattr(mcp_server, "STATEMENT_TIMEOUT_SECONDS", 0.2)
    outcomes, finished = {}, {}

    def timed():
        cursor = connection.cursor()
        try:
            with mcp_server._statement_timeout(cursor):
                cursor.execute("SELECT count(*) FROM range(10000000000) t(i) WHERE hash(i) % 7 = 0").fetchall()
        except TimeoutError as error:
            outcomes["timed"] = str(error)
        finished["timed"] = time.monotonic()

    def sibling():
        cursor = connection.cursor()
        outcomes["sibling"] = cursor.execute("SELECT count(*) FROM range(1000000000) t(i) WHERE i % 7 = 0").fetchone()
        finished["sibling"] = time.monotonic()

    threads = [threading.Thread(target=sibling), threading.Thread(target=timed)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(120)
    assert "statement timeout" in outcomes["timed"]
    assert outcomes["sibling"] == (142857143,)  # multiples of 7 below 10**9, counted in full
    assert finished["sibling"] > finished["timed"]  # it was still running when the other call's timer fired
