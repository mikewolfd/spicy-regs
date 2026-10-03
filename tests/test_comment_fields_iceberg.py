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


def test_a_replacement_commits_as_one_rewritten_row_and_is_verified(tmp_path, lake):
    """An attachments list the re-read only adds to is replaced like a fill: one record and one position delete."""
    from tests.test_comment_fields_write import HELD, LISTED

    with lake() as con:
        con.execute(f"UPDATE {iceberg._qualified(COMMENT)} SET attachments_json = ? WHERE comment_id = 'C-0-0'", [HELD])
    part = tmp_path / "parts" / "agency=EPA" / "p.parquet"
    pl.read_parquet(part).with_columns(
        pl.when(pl.col("comment_id") == "C-0-0").then(pl.lit(LISTED)).otherwise(pl.col("attachments_json"))
        .alias("attachments_json")).write_parquet(part)
    prepared = cfw.prepare(tmp_path, profile="attachments")
    assert (prepared["rows_to_fill"], prepared["replaced_by_column"]) == (1, {"attachments_json": 1})
    with lake() as con:
        assert cfw.write(tmp_path, con=con)["rows_changed"] == 1
    after = rows(lake)
    assert after["C-0-0"]["attachments_json"] == LISTED and after["C-0-0"]["subtype"] is None  # the profile's column only
    assert [line["state"] for line in journal(tmp_path)] == ["pending", "verified"]


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



def _same(a: dict, b: dict) -> bool:
    return {c: a[c] for c in COMMENT.schema} == {c: b[c] for c in COMMENT.schema}


def _snapshot(connect) -> int:
    with connect() as con:
        return iceberg._read_snapshot(con, COMMENT).snapshot_id


def test_an_undo_is_a_forward_commit_that_plain_reads_and_the_etl_write_accept(tmp_path, lake):
    """The rehearsal the runbook requires before any live write: undo one batch, then read and write as usual."""
    before = rows(lake)
    cfw.prepare(tmp_path)
    with lake() as con:
        cfw.write(tmp_path, con=con, batch_bytes=1)
    middle = [line for line in journal(tmp_path) if line["state"] == "verified"][1]
    undone_ids = {k for k, r in before.items() if r["filename"] in middle["files"]}
    filled_at = _snapshot(lake)
    with lake() as con:
        undone = cfw.undo(tmp_path, middle["batch"], expected_snapshot=filled_at, con=con)
    assert undone["rows_restored"] == 4 and journal(tmp_path)[-1]["state"] == "undone"
    after = rows(lake)  # a plain read, on a new connection
    assert all(_same(after[k], before[k]) for k in undone_ids)
    assert {after[k]["subtype"] for k in set(before) - undone_ids} == {"Public Comment"}
    with lake() as con:
        metadata = cfw._table_metadata(con)
    (child,) = [s for s in metadata["snapshots"] if s.get("parent-snapshot-id") == filled_at]
    assert metadata["current-snapshot-id"] == child["snapshot-id"] == undone["snapshot_after"]["snapshot_id"]
    # The ETL's own write path next: a newer version of an undone row, and a new comment.
    k = sorted(undone_ids)[0]
    newer = {**{c: after[k][c] for c in COMMENT.schema}, "modify_date": "2021-01-01T00:00:00Z", "title": "etl"}
    new = {**dict.fromkeys(COMMENT.schema), "comment_id": "C-9-0", "agency_code": "EPA", "modify_date": T0}
    staging = tmp_path / "staging.parquet"
    pl.DataFrame([newer, new], schema=dict.fromkeys(COMMENT.schema, pl.Utf8)).write_parquet(staging)
    with lake() as con:
        assert iceberg._merge(con, [staging], COMMENT) == 2
    final = rows(lake)
    assert (final[k]["title"], final["C-9-0"]["modify_date"], len(final)) == ("etl", T0, 13)


def test_a_damaged_commit_found_on_recovery_is_refused_and_undone_whole(tmp_path, lake):
    """A check that missed the damage (disabled here) and a crash right after COMMIT: the rerun compares the commit
    with its pre-image, journals it failed, and the undo restores every column from the pre-image."""
    before = rows(lake)
    cfw.prepare(tmp_path)

    def damage(sql):
        if sql.lstrip().startswith("MERGE"):
            return sql.replace("UPDATE SET ", 'UPDATE SET "text_content" = NULL, ', 1)
        return "SELECT 0" if "EXCEPT ALL" in sql else sql

    def crash(sql):
        if sql == "COMMIT":
            raise KeyboardInterrupt

    with lake() as con, pytest.raises(KeyboardInterrupt):
        cfw.write(tmp_path, con=Proxy(con, rewrite=damage, after=crash), batch_bytes=1)
    with lake() as con, pytest.raises(cfw.FillVerificationError, match="differs"):
        cfw.write(tmp_path, con=con, batch_bytes=1)
    failed = journal(tmp_path)[-1]
    assert (failed["state"], failed["committed"]) == ("failed", True)
    with pytest.raises(RuntimeError, match="undo"):
        cfw.prepare(tmp_path)
    damaged = [k for k, r in rows(lake).items() if r["text_content"] is None]
    assert len(damaged) == 4
    with lake() as con:
        cfw.undo(tmp_path, failed["batch"], expected_snapshot=_snapshot(lake), con=con)
    after = rows(lake)
    assert all(_same(after[k], before[k]) for k in before)
    assert cfw.prepare(tmp_path)["rows_to_fill"] == 12


def test_a_commit_by_another_writer_during_the_batch_is_refused_at_commit(tmp_path, lake):
    """The transaction reads one snapshot from BEGIN on: another writer committing after the snapshot check makes
    the catalog refuse this COMMIT, so no batch lands on a table its pre-image did not see."""
    cfw.prepare(tmp_path)
    seen = {"n": 0}

    def foreign(sql):
        if "iceberg_load_table_response" in sql and not seen["n"]:
            seen["n"] += 1
            with lake() as other:
                other.execute(f"UPDATE {iceberg._qualified(COMMENT)} SET title = 'etl' WHERE comment_id = 'C-2-3'")

    with lake() as con, pytest.raises(Exception, match="409|Conflict|conflict") as refused:
        cfw.write(tmp_path, con=Proxy(con, after=foreign), batch_bytes=1)
    assert not isinstance(refused.value, cfw.FillVerificationError)
    after = rows(lake)
    assert after["C-2-3"]["title"] == "etl" and {r["subtype"] for r in after.values()} == {None}
    assert [line["state"] for line in journal(tmp_path)] == ["pending"]
