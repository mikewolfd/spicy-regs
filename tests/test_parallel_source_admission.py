"""Independent local cursors overlap source binding without weakening fresh admission."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import threading

import duckdb
import pytest

from spicy_regs import mcp_server as server
from spicy_regs.sources import publication


class TrackedConnection:
    def __init__(self, raw, hook=lambda sql: None):
        self.raw, self.hook = raw, hook
        self.children = []
        self.lock = threading.Lock()
        self.active = self.peak = 0
        self.closed = False

    def execute(self, sql, parameters=None):
        with self.lock:
            assert self.active == 0, "parent execute overlapped worker execute"
        return self.raw.execute(sql, parameters) if parameters is not None else self.raw.execute(sql)

    def cursor(self):
        child = TrackedCursor(self, self.raw.cursor())
        self.children.append(child)
        return child

    def close(self):
        assert self.active == 0 and all(c.closed for c in self.children)
        self.closed = True
        self.raw.close()


class TrackedCursor:
    def __init__(self, parent, raw):
        self.parent, self.raw = parent, raw
        self.settings = {}
        self.closed = False

    def execute(self, sql, parameters=None):
        assert not self.closed
        if sql.startswith("SET "):
            self.settings[sql.split()[1].split("=")[0]] = parameters[0]
            if "http_retry" in sql or "http_retries" in sql:
                return self  # Core-only fixtures; real httpfs settings are checked separately below.
        with self.parent.lock:
            self.parent.active += 1
            self.parent.peak = max(self.parent.peak, self.parent.active)
        try:
            self.parent.hook(sql)
            return self.raw.execute(sql, parameters) if parameters is not None else self.raw.execute(sql)
        finally:
            with self.parent.lock:
                self.parent.active -= 1

    def close(self):
        assert not self.closed
        self.closed = True
        self.raw.close()


def sources(raw, tmp_path, count=4):
    result = []
    for i in range(count):
        path = tmp_path / f"s{i}.parquet"
        raw.execute(f"COPY (SELECT {i}::INTEGER AS id) TO ? (FORMAT PARQUET)", [str(path)])
        result.append(server._RemoteSourceView(f"s{i}", [str(path)], [["id", "INTEGER"]], True, False))
    return result


def test_four_independent_cursors_share_actual_views_and_settle_before_parent(tmp_path):
    barrier = threading.Barrier(4)

    def overlap(sql):
        if sql.startswith("CREATE VIEW") and not sql.startswith('CREATE VIEW "s4"'):
            barrier.wait(3)

    with duckdb.connect(config={"threads": 1}) as raw:
        planned = sources(raw, tmp_path, 5)
        con = TrackedConnection(raw, overlap)
        assert server._admit_remote_source_views(con, planned) == [True] * 5
        assert con.peak == 4 and len(con.children) == 4
        assert all(c.closed for c in con.children)
        assert con.execute("SELECT (SELECT id FROM s0),(SELECT id FROM s4)").fetchall() == [(0, 4)]
        for c in con.children:
            assert c.settings == {"home_directory": server.HOME_DIRECTORY, **server.INTERACTIVE_HTTP_RETRIES}


@pytest.mark.parametrize("damage", ["schema", "missing", "split-member", "corrupt"])
def test_required_failure_is_not_admitted_and_all_cursors_close(tmp_path, damage):
    with duckdb.connect(config={"threads": 1}) as raw:
        planned = sources(raw, tmp_path)
        bad = planned[0]
        if damage == "schema":
            bad = bad._replace(columns=[["wrong", "VARCHAR"]])
        elif damage == "missing":
            Path(bad.urls[0]).unlink()
        elif damage == "split-member":
            bad = bad._replace(urls=[*bad.urls, str(tmp_path / "missing.parquet")])
        else:
            Path(bad.urls[0]).write_bytes(b"invalid parquet")
        planned[0] = bad
        con = TrackedConnection(raw)
        with pytest.raises(RuntimeError, match="Published"):
            server._admit_remote_source_views(con, planned)
        assert con.active == 0 and all(c.closed for c in con.children)


@pytest.mark.parametrize("required,status,refuses", [(False, 404, False), (True, 404, True),
                                                    (False, 429, True), (False, 503, True)])
def test_only_legacy_http_404_skips(tmp_path, required, status, refuses):
    def refused(sql):
        if sql.startswith('CREATE VIEW "s0"'):
            error = duckdb.HTTPException(f"synthetic HTTP {status}")
            error.status_code = status
            raise error

    with duckdb.connect(config={"threads": 1}) as raw:
        planned = sources(raw, tmp_path, 1)
        planned[0] = planned[0]._replace(required=required)
        con = TrackedConnection(raw, refused)
        if refuses:
            with pytest.raises(RuntimeError, match="unavailable"):
                server._admit_remote_source_views(con, planned)
        else:
            assert server._admit_remote_source_views(con, planned) == [False]
        assert all(c.closed for c in con.children)


def test_failure_waits_for_other_inflight_cursor_before_return(tmp_path):
    entered, release = threading.Event(), threading.Event()

    def wait_or_fail(sql):
        if sql.startswith('CREATE VIEW "s1"'):
            entered.set()
            assert release.wait(3)
        elif sql.startswith('CREATE VIEW "s0"'):
            assert entered.wait(2)
            raise duckdb.IOException("synthetic required failure")

    with duckdb.connect(config={"threads": 1}) as raw:
        planned = sources(raw, tmp_path, 2)
        con = TrackedConnection(raw, wait_or_fail)
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(server._admit_remote_source_views, con, planned)
            try:
                assert entered.wait(2)
                assert not pending.done()
                assert not con.closed
            finally:
                release.set()
            with pytest.raises(RuntimeError, match="unavailable"):
                pending.result(timeout=3)
        assert con.active == 0 and all(c.closed for c in con.children)
        con.close()


def test_cursor_configuration_failure_closes_every_created_child(tmp_path):
    with duckdb.connect(config={"threads": 1}) as raw:
        planned = sources(raw, tmp_path)
        con = TrackedConnection(raw)
        original = con.cursor

        def bad_cursor():
            child = original()
            if len(con.children) == 2:
                child.execute = lambda *args: (_ for _ in ()).throw(ValueError("synthetic settings refusal"))
            return child

        con.cursor = bad_cursor
        with pytest.raises(ValueError, match="settings refusal"):
            server._admit_remote_source_views(con, planned)
        assert len(con.children) == 2 and all(c.closed for c in con.children)


def test_one_close_failure_does_not_leave_other_children_open(tmp_path):
    with duckdb.connect(config={"threads": 1}) as raw:
        planned = sources(raw, tmp_path)
        con = TrackedConnection(raw)
        original = con.cursor

        def bad_close():
            child = original()
            if len(con.children) == 1:
                close = child.close

                def close_then_fail():
                    close()
                    raise ValueError("synthetic close refusal")

                child.close = close_then_fail
            return child

        con.cursor = bad_close
        with pytest.raises(ValueError, match="close refusal"):
            server._admit_remote_source_views(con, planned)
        assert con.active == 0 and all(c.closed for c in con.children)


def test_remote_build_collects_paths_then_relationships_then_security(tmp_path, monkeypatch):
    connect = duckdb.connect
    with connect(config={"threads": 1}) as fixture:
        sources(fixture, tmp_path, 4)
    con = TrackedConnection(connect(config={"threads": 1}))
    monkeypatch.setattr(server.duckdb, "connect", lambda: con)
    monkeypatch.setattr(server, "DATA_DIR", None)
    monkeypatch.setattr(server, "TABLES", tuple(f"s{i}" for i in range(4)))
    monkeypatch.setattr(server, "R2_BASE_URL", str(tmp_path))
    monkeypatch.setattr(server, "_resolve_catalog_config", lambda: None)
    monkeypatch.setattr(server, "load_public_http", lambda *args: None)
    order = []

    def relationships(root):
        assert root is con and all(c.closed for c in root.children)
        order.append("relationships")

    def security(root, allowed):
        assert all(c.closed for c in root.children)
        assert allowed == [str(tmp_path / f"s{i}.parquet") for i in range(4)]
        order.append("security")
        return server_security(root, allowed)

    server_security = server._apply_security_settings
    monkeypatch.setattr(server, "_install_relationship_views", relationships)
    monkeypatch.setattr(server, "_apply_security_settings", security)
    try:
        assert server._build_connection(server._Publication(publication.empty_index(), None)) is con
        assert order == ["relationships", "security"]
        assert con.execute("SELECT * FROM s2").fetchall() == [(2,)]
        with pytest.raises(duckdb.Error):
            con.execute("SET enable_external_access=true")
        with pytest.raises(duckdb.Error):
            con.execute("SELECT * FROM read_text('/unselected-synthetic-file')")
    finally:
        con.close()


def test_real_httpfs_child_retry_and_home_settings_are_explicit(tmp_path):
    with duckdb.connect(config={"threads": 1}) as raw:
        platform = raw.execute("PRAGMA platform").fetchone()[0]
        path = Path.home() / ".duckdb/extensions" / ("v" + duckdb.__version__) / platform / "httpfs.duckdb_extension"
        if not path.is_file():
            pytest.skip("No installed local httpfs; never download an extension in this test")
        raw.execute("LOAD '" + str(path).replace("'", "''") + "'")
        planned = sources(raw, tmp_path, 1)
        observed = []

        class Connection:
            def cursor(self):
                child = raw.cursor()
                observed.append(child)
                return child

        # Record the real child settings during admission, before helper closure.
        from unittest.mock import patch

        admit = server._admit_remote_source_view

        def check(child, source):
            for name, value in server.INTERACTIVE_HTTP_RETRIES.items():
                assert float(child.execute(f"SELECT current_setting('{name}')").fetchone()[0]) == value
            assert child.execute("SELECT current_setting('home_directory')").fetchone()[0] == server.HOME_DIRECTORY
            assert child.execute("SELECT current_setting('threads')").fetchone()[0] == 1
            return admit(child, source)

        with patch.object(server, "_admit_remote_source_view", check):
            assert server._admit_remote_source_views(Connection(), planned) == [True]
        with pytest.raises(duckdb.Error):
            observed[0].execute("SELECT 1")
