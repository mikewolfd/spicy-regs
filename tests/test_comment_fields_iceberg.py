"""The comment fill's write phase on a real Iceberg catalog: rows move files, commits carry parents and summaries.

Runs Apache's disposable REST catalog fixture in Docker (the image DocSpec's tools/with_iceberg.py pins), on loopback
only, and never reaches R2. Marked ``integration`` with the other tests that need more than the Python environment;
the Integration workflow runs them, and a missing Docker fails rather than skips.
"""

import json
import shutil
import subprocess
import time
from contextlib import suppress
from pathlib import Path
from urllib.request import urlopen
from uuid import uuid4

import duckdb
import polars as pl
import pytest

from spicy_regs.pipelines import comment_fields as cf
from spicy_regs.pipelines import comment_fields_write as cfw
from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg

pytestmark = pytest.mark.integration
IMAGE = "apache/iceberg-rest-fixture:1.10.1@sha256:f7d679d30ac9c640bdeb2c015dff533cd3c8f1c7d491ebcb5d436f9a42db1d6f"
T0 = "2020-01-01T00:00:00Z"


@pytest.fixture(scope="module")
def rest_uri():
    # Under the home directory: Docker VMs on macOS (Colima, Docker Desktop) share it, not the system temp dir.
    root = (Path.home() / ".cache" / "spicy-regs-iceberg-tests" / uuid4().hex[:8]).resolve()
    root.mkdir(parents=True)
    name = "spicy-regs-fill-" + uuid4().hex[:8]
    subprocess.run(["docker", "run", "--detach", "--rm", "--name", name, "--user", "0", "--publish", "127.0.0.1::8181",
                    "--volume", f"{root}:{root}", "--env", f"CATALOG_WAREHOUSE={root}/warehouse",
                    "--env", f"CATALOG_URI=jdbc:sqlite:{root}/catalog.sqlite", IMAGE], check=True, stdout=subprocess.DEVNULL)
    try:
        port = subprocess.check_output(["docker", "port", name, "8181/tcp"], text=True).strip().splitlines()[0]
        uri = "http://127.0.0.1:" + port.rsplit(":", 1)[1]
        deadline = time.monotonic() + 90
        while True:
            try:
                with urlopen(uri + "/v1/config", timeout=2) as response:
                    json.load(response)
                break
            except OSError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.3)
        yield uri
    finally:
        with suppress(OSError, subprocess.SubprocessError):
            subprocess.run(["docker", "rm", "--force", name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        shutil.rmtree(root, ignore_errors=True)


@pytest.fixture
def lake(rest_uri, tmp_path, monkeypatch):
    """A fresh namespace holding three data files of four comments each, and the re-read of all twelve."""
    namespace = "n" + uuid4().hex[:8]

    def connect():
        con = duckdb.connect()
        con.execute("INSTALL iceberg; LOAD iceberg;")
        con.execute(f"ATTACH '' AS {iceberg._CATALOG_ALIAS} (TYPE iceberg, ENDPOINT '{rest_uri}', "
                    "CLIENT_ID 'admin', CLIENT_SECRET 'password')")
        return con

    monkeypatch.setattr(iceberg, "_connect", connect)
    monkeypatch.setenv("R2_CATALOG_NAMESPACE", namespace)
    monkeypatch.setenv(iceberg.CATALOG_LOCK_ENV, "test")
    for name in ("R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ENDPOINT"):
        monkeypatch.delenv(name, raising=False)
    with connect() as con:
        con.execute(f"CREATE SCHEMA {iceberg._schema_ref()}")
        con.execute(f"CREATE TABLE {iceberg._qualified(COMMENT)} ("
                    + ", ".join(f'"{c}" VARCHAR' for c in COMMENT.schema) + ")")
        for f in range(3):
            rows = [{**dict.fromkeys(COMMENT.schema), "comment_id": f"C-{f}-{i}", "agency_code": "EPA",
                     "docket_id": f"D{f}", "modify_date": T0, "comment": f"body {f}-{i}", "title": "t",
                     "text_content": f"text {f}-{i}", "text_extraction_status": "ok"} for i in range(4)]
            con.register("seed", pl.DataFrame(rows, schema=dict.fromkeys(COMMENT.schema, pl.Utf8)).to_arrow())
            con.execute(f"INSERT INTO {iceberg._qualified(COMMENT)} SELECT * FROM seed")
            con.unregister("seed")
    part = tmp_path / "parts" / "agency=EPA" / "p.parquet"
    part.parent.mkdir(parents=True)
    reads = [{**dict.fromkeys(cf.PART_SCHEMA), "key": f"k/C-{f}-{i}", "comment_id": f"C-{f}-{i}", "agency_code": "EPA",
              "modify_date": T0, "subtype": "Public Comment", "duplicate_comments": 0} for f in range(3) for i in range(4)]
    pl.DataFrame(reads, schema=cf.PART_SCHEMA).write_parquet(part)
    return connect


def rows(connect) -> dict[str, dict]:
    with connect() as con:
        return {r["comment_id"]: r for r in con.execute(
            f"SELECT *, filename FROM {iceberg._qualified(COMMENT)}").pl().to_dicts()}


def journal(tmp_path) -> list[dict]:
    (path,) = (tmp_path / "fill").glob("write-journal-*.jsonl")
    return [json.loads(line) for line in path.read_text().splitlines()]


class Proxy:
    def __init__(self, con, rewrite=None, after=None):
        self.con, self.rewrite, self.after = con, rewrite, after

    def execute(self, sql, *args):
        result = self.con.execute(self.rewrite(sql) if self.rewrite else sql, *args)
        if self.after:
            self.after(sql)
        return result

    def __getattr__(self, name):
        return getattr(self.con, name)


def test_rows_move_files_are_checked_before_commit_and_keep_every_other_column(tmp_path, lake):
    before = rows(lake)
    prepared = cfw.prepare(tmp_path)
    assert prepared["rows_to_fill"] == 12 and len(prepared["files"]) == 3
    with lake() as con:
        written = cfw.write(tmp_path, con=con, batch_bytes=1)  # one file per batch
    after = rows(lake)
    assert written == {"batches": 3, "rows_changed": 12, "files_skipped_verified": 0}
    assert all(after[k]["filename"] != before[k]["filename"] for k in before)  # merge-on-read moved every row
    assert all((after[k]["comment"], after[k]["text_content"]) == (before[k]["comment"], before[k]["text_content"])
               for k in before)
    assert {(r["subtype"], r["duplicate_comments"]) for r in after.values()} == {("Public Comment", "0")}
    assert [line["state"] for line in journal(tmp_path)] == ["pending", "verified"] * 3


def test_a_merge_that_blanks_a_column_is_rolled_back_and_nothing_is_committed(tmp_path, lake):
    cfw.prepare(tmp_path)
    with lake() as con:
        snapshot = iceberg._read_snapshot(con, COMMENT)
    blank = lambda sql: sql.replace("UPDATE SET ", 'UPDATE SET "text_content" = NULL, ', 1) if sql.lstrip().startswith("MERGE") else sql  # noqa: E731
    with lake() as con, pytest.raises(cfw.FillVerificationError):
        cfw.write(tmp_path, con=Proxy(con, rewrite=blank))
    with lake() as con:
        assert iceberg._read_snapshot(con, COMMENT) == snapshot
    assert all(r["text_content"] is not None and r["subtype"] is None for r in rows(lake).values())
    with lake() as con, pytest.raises(RuntimeError, match="clear-failure"):
        cfw.write(tmp_path, con=con)


def test_a_crash_after_a_commit_before_its_journal_line_is_recovered_on_rerun(tmp_path, lake):
    cfw.prepare(tmp_path)
    commits = {"n": 0}

    def crash(sql):
        if sql == "COMMIT":
            commits["n"] += 1
            if commits["n"] == 2:
                raise KeyboardInterrupt  # after batch 2's COMMIT, before its verified line

    with lake() as con, pytest.raises(KeyboardInterrupt):
        cfw.write(tmp_path, con=Proxy(con, after=crash), batch_bytes=1)
    assert [line["state"] for line in journal(tmp_path)] == ["pending", "verified", "pending"]
    with lake() as con:
        resumed = cfw.write(tmp_path, con=con, batch_bytes=1)
    states = [line["state"] for line in journal(tmp_path)]
    assert states == ["pending", "verified", "pending", "verified", "pending", "verified"]
    assert journal(tmp_path)[3].get("recovered") is True and resumed["rows_changed"] == 4
    assert {r["subtype"] for r in rows(lake).values()} == {"Public Comment"}


def test_a_foreign_commit_mid_run_stops_it_and_preparing_again_finishes(tmp_path, lake):
    cfw.prepare(tmp_path)
    commits = {"n": 0}

    def compaction(sql):
        if sql == "COMMIT":
            commits["n"] += 1
            if commits["n"] == 1:  # another writer (an ETL batch, R2 compaction) commits after batch 1
                with lake() as other:
                    other.execute(f"UPDATE {iceberg._qualified(COMMENT)} SET title = 't2' WHERE comment_id = 'C-2-3'")

    with lake() as con, pytest.raises(RuntimeError, match="another writer"):
        cfw.write(tmp_path, con=Proxy(con, after=compaction), batch_bytes=1)
    prepared = cfw.prepare(tmp_path)
    assert prepared["rows_to_fill"] == 8
    with lake() as con:
        assert cfw.write(tmp_path, con=con)["rows_changed"] == 8
    assert rows(lake)["C-2-3"]["title"] == "t2"

