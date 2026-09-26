"""Fresh FCC filings stay bounded and never replace prior outputs after an incomplete fetch."""

import importlib
import inspect
from datetime import date

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

module = importlib.import_module("spicy_regs.transforms.build_fcc_ecfs")
row_writer = importlib.import_module("spicy_regs.transforms.parquet_rows")


def filing(identifier, *, text="fresh", received="2026-09-25", **extra):
    return module._shape_filing({"id_submission": identifier, "text_data": text, "date_received": received, **extra})


def write(path, rows):
    pq.write_table(pa.Table.from_pylist(rows, schema=module._FILING_SCHEMA), path)
    return path


def merge(tmp_path, rows, *, have_prior=False):
    return module._merge_incremental(
        tmp_path,
        output=module.FILINGS_OUTPUT,
        scratch_prefix="_fcc_filings",
        columns=module.FILING_COLUMNS,
        schema=module._FILING_SCHEMA,
        key="id_submission",
        order_by="date_received",
        rows=rows,
        prior_file=tmp_path / "_fcc_filings_prior.parquet",
        have_prior=have_prior,
    )


def observe_batches(monkeypatch):
    original = row_writer.pq.ParquetWriter
    written, closed = [], []

    class ObservedWriter:
        def __init__(self, *args, **kwargs):
            self.writer = original(*args, **kwargs)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            try:
                return self.writer.__exit__(*args)
            finally:
                closed.append(True)

        def write_table(self, table):
            self.writer.write_table(table)
            written.append(table.num_rows)

    monkeypatch.setattr(row_writer.pq, "ParquetWriter", ObservedWriter)
    return written, closed


def test_one_shot_input_flushes_bounded_batches_before_reading_more_rows(tmp_path, monkeypatch):
    written, closed = observe_batches(monkeypatch)

    class OneShot:
        calls = 0

        def __iter__(self):
            self.calls += 1
            assert self.calls == 1
            for index in range(4_005):
                if index >= 2_000:
                    assert sum(written) >= index // 2_000 * 2_000
                yield filing(str(index))

        def __len__(self):
            raise AssertionError("fresh input must not be materialized or tested for emptiness")

    rows = OneShot()
    output = merge(tmp_path, rows)
    assert rows.calls == 1 and written == [2_000, 2_000, 5] and closed == [True]
    assert pq.ParquetFile(output).metadata.num_rows == 4_005
    assert not (tmp_path / "_fcc_filings_new.parquet").exists()
    assert not (tmp_path / f".{module.FILINGS_OUTPUT}.partial").exists()


def test_source_failure_after_a_flushed_batch_preserves_prior_and_published_bytes(tmp_path, monkeypatch):
    prior = write(tmp_path / "_fcc_filings_prior.parquet", [filing("prior", text="retained prior")])
    output = write(tmp_path / module.FILINGS_OUTPUT, [filing("published", text="retained publication")])
    prior_bytes, output_bytes = prior.read_bytes(), output.read_bytes()
    written, closed = observe_batches(monkeypatch)

    def failing_rows():
        for index in range(2_001):
            if index == 2_000:
                assert written == [2_000]
            yield filing(str(index))
        raise RuntimeError("source iterator failed after a flushed batch")

    monkeypatch.setattr(duckdb, "connect", lambda: pytest.fail("merge began before source consumption finished"))
    with pytest.raises(RuntimeError, match="source iterator failed"):
        merge(tmp_path, failing_rows(), have_prior=True)
    assert prior.read_bytes() == prior_bytes and output.read_bytes() == output_bytes
    assert written == [2_000] and closed == [True]
    assert set(tmp_path.iterdir()) == {prior, output}


@pytest.mark.parametrize("have_prior", [False, True])
def test_empty_fresh_rows_keep_schema_and_prior_rows_when_present(tmp_path, have_prior):
    prior_rows = [filing("prior", text="retained")]
    prior = tmp_path / "_fcc_filings_prior.parquet"
    if have_prior:
        write(prior, prior_rows)
    output = merge(tmp_path, iter(()), have_prior=have_prior)
    table = pq.read_table(output)
    assert table.schema == module._FILING_SCHEMA
    assert table.to_pylist() == (prior_rows if have_prior else [])
    assert not prior.exists() and not (tmp_path / "_fcc_filings_new.parquet").exists()


def test_fresh_whole_row_wins_and_published_output_changes_only_after_merge(tmp_path, monkeypatch):
    prior_only = filing("prior-only", received="2026-09-20")
    prior = write(tmp_path / "_fcc_filings_prior.parquet", [filing("shared", text="old"), prior_only])
    output = write(tmp_path / module.FILINGS_OUTPUT, [filing("published", text="leave until complete")])
    output_bytes = output.read_bytes()
    original = module.merge_local_prior
    fresh = filing("shared", text=None)

    def observed_merge(connection, **kwargs):
        assert output.read_bytes() == output_bytes
        assert kwargs["out_file"] != output
        original(connection, **kwargs)
        assert output.read_bytes() == output_bytes
        assert kwargs["out_file"].exists()

    monkeypatch.setattr(module, "merge_local_prior", observed_merge)
    assert merge(tmp_path, (row for row in [fresh, fresh]), have_prior=True) == output
    assert pq.read_table(output).to_pylist() == [fresh, prior_only]
    assert not prior.exists() and not (tmp_path / "_fcc_filings_new.parquet").exists()


@pytest.mark.parametrize("failure", ["configure", "merge"])
def test_merge_failure_closes_connection_and_removes_partial_output(tmp_path, monkeypatch, failure):
    prior = write(tmp_path / "_fcc_filings_prior.parquet", [filing("prior")])
    output = write(tmp_path / module.FILINGS_OUTPUT, [filing("published")])
    prior_bytes, output_bytes = prior.read_bytes(), output.read_bytes()
    original_connect = duckdb.connect
    closed = []

    class Connection:
        def __init__(self):
            self.connection = original_connect()

        def execute(self, *args, **kwargs):
            if failure == "configure":
                raise RuntimeError("connection configuration failed")
            return self.connection.execute(*args, **kwargs)

        def close(self):
            closed.append(True)
            self.connection.close()

    def failed_merge(_connection, **kwargs):
        kwargs["out_file"].write_bytes(b"partial merged output")
        raise RuntimeError("merge failed after writing partial output")

    monkeypatch.setattr(duckdb, "connect", Connection)
    monkeypatch.setattr(module, "merge_local_prior", failed_merge)
    with pytest.raises(RuntimeError, match="failed"):
        merge(tmp_path, iter([filing("fresh")]), have_prior=True)
    assert closed == [True]
    assert prior.read_bytes() == prior_bytes and output.read_bytes() == output_bytes
    assert not (tmp_path / "_fcc_filings_new.parquet").exists()
    assert not (tmp_path / f".{module.FILINGS_OUTPUT}.partial").exists()


def test_builder_passes_a_shaping_generator_to_the_merge(tmp_path, monkeypatch):
    monkeypatch.setattr(module.r2, "download", lambda *_: False)
    calls = []

    def fetched(endpoint, **kwargs):
        calls.append((endpoint, kwargs))
        yield {"id_submission": "fresh", "date_received": "2026-09-25", "text_data": "source text"}

    original = module._merge_incremental

    def observed_merge(*args, **kwargs):
        assert inspect.isgenerator(kwargs["rows"])
        assert calls == []
        return original(*args, **kwargs)

    monkeypatch.setattr(module, "_fetch_fcc", fetched)
    monkeypatch.setattr(module, "_merge_incremental", observed_merge)
    output = module.build_fcc_filings(tmp_path, since=date(2026, 9, 25), proceedings=("17-108",))
    assert calls == [("filings", {"since": date(2026, 9, 25), "proceedings": ("17-108",), "transport": None})]
    assert pq.read_table(output).to_pylist() == [filing("fresh", text="source text")]


class Today(date):
    @classmethod
    def today(cls):
        return cls(2026, 9, 25)


@pytest.mark.parametrize(
    "prior_rows,since,expected",
    [
        (None, None, date(2026, 8, 26)),  # nothing published: the trailing 30 days
        ([], None, date(2026, 8, 26)),  # a published table with no max date: still the first-run bound
        (  # the prior's max received date, less the 7-day overlap
            [filing("older", received="2026-09-18T09:00:00Z"), filing("newest", received="2026-09-20T23:59:59Z")],
            None,
            date(2026, 9, 13),
        ),
        ([filing("newest", received="2026-09-20T23:59:59Z")], date(2026, 1, 2), date(2026, 1, 2)),  # explicit wins
    ],
)
def test_builder_derives_since_from_the_first_run_bound_or_the_prior_overlap(
    tmp_path, monkeypatch, prior_rows, since, expected
):
    downloads, calls = [], []

    def download(key, path):
        downloads.append((key, path))
        return prior_rows is not None and write(path, prior_rows).exists()

    def fetched(endpoint, **kwargs):
        calls.append((endpoint, kwargs))
        yield from ()

    monkeypatch.setattr(module, "date", Today)
    monkeypatch.setattr(module.r2, "download", download)
    monkeypatch.setattr(module, "_fetch_fcc", fetched)
    output = module.build_fcc_filings(tmp_path, since=since, proceedings=("17-108",))
    assert downloads == [(module.FILINGS_OUTPUT, tmp_path / "_fcc_filings_prior.parquet")]
    assert calls == [("filings", {"since": expected, "proceedings": ("17-108",), "transport": None})]
    newest_first = sorted(prior_rows or [], key=lambda row: row["date_received"], reverse=True)
    assert pq.read_table(output).to_pylist() == newest_first


def test_builder_closes_source_and_preserves_output_when_arrow_conversion_fails(tmp_path, monkeypatch):
    output = write(tmp_path / module.FILINGS_OUTPUT, [filing("published")])
    output_bytes = output.read_bytes()
    closed = []

    def fetched(*_args, **_kwargs):
        try:
            for index in range(2_001):
                yield {"id_submission": str(index), "text_data": {"invalid": "scalar"} if index == 0 else "text"}
        finally:
            closed.append(True)

    monkeypatch.setattr(module.r2, "download", lambda *_: False)
    monkeypatch.setattr(module, "_fetch_fcc", fetched)
    with pytest.raises((TypeError, ValueError)):
        module.build_fcc_filings(tmp_path, since=date(2026, 9, 25))
    assert closed == [True] and output.read_bytes() == output_bytes
    assert list(tmp_path.iterdir()) == [output]
