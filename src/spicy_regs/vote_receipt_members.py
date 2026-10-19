"""Opt-in ordered vote receipt files; this only changes emission, never carry decisions."""

from pathlib import Path

import pyarrow as pa

from spicy_regs.etl_bulk import bulk_connection, new_receipt_writer
from spicy_regs.etl_receipts import RECEIPT_ROW_GROUP, RECEIPT_SCHEMA
from spicy_regs.receipt_history import _raw_identity_sql


class VoteReceiptMembers:
    """Write contiguous Congress groups without sorting or inheriting upstream row-group offsets.

    One bounded batch is classified at a time. Only the current member and less
    than one row group remain buffered. An unsupported layout refuses instead
    of changing receipt order. The owner retains the private stage on failure.
    """

    def __init__(self, directory: Path):
        self.directory = directory
        directory.mkdir()
        self.paths: list[Path] = []
        self._name = None
        self._writer = None
        self._pending = RECEIPT_SCHEMA.empty_table()
        self._seen: set[str] = set()
        self._metadata = False
        self._tail = False

    def __enter__(self):
        self._connection = bulk_connection(self.directory.parent, threads=1, memory_limit="64MB")
        self._con, _ = self._connection.__enter__()
        return self

    def __exit__(self, exc_type, exc, traceback):
        try:
            if exc_type is None:
                self.close()
            elif self._writer is not None:
                self._writer.close()
        finally:
            self._connection.__exit__(exc_type, exc, traceback)

    def _flush(self, *, final=False):
        count = self._pending.num_rows
        whole = count if final else count - count % RECEIPT_ROW_GROUP
        if whole:
            self._writer.write_table(self._pending.slice(0, whole), row_group_size=RECEIPT_ROW_GROUP)
            self._pending = self._pending.slice(whole)

    def _emit(self, name, table):
        if name != self._name:
            if name in self._seen:
                raise ValueError("Vote receipt Congress groups must be contiguous")
            self._flush(final=True)
            if self._writer is not None:
                self._writer.close()
            if len(self._seen) >= 256:
                raise ValueError("Vote receipt layout exceeds 256 ordered members")
            self._seen.add(name)
            path = self.directory / (name + ".parquet")
            self._writer = new_receipt_writer(path)
            self.paths.append(path)
            self._name = name
        self._pending = pa.concat_tables([self._pending, table])
        self._flush()

    def write_table(self, table, **_):
        if self._tail:
            raise ValueError("Vote receipt rows cannot follow the unresolved-refusal tail")
        # Native projection of narrow tags from the already-present batch. Do not
        # decode whole processing records into Python or scan another receipt file.
        congress_sql = _raw_identity_sql("processing_json", "source_fields", ("congress",))
        for batch in table.to_batches(max_chunksize=RECEIPT_ROW_GROUP):
            piece = pa.Table.from_batches([batch])
            self._con.register(
                "vote_receipt_batch",
                piece.select(["dataset", "processing_json"]).append_column(
                    "batch_ordinal", pa.array(range(piece.num_rows), type=pa.int64())
                ),
            )
            try:
                keys = self._con.execute(f"""
                    WITH selected AS (SELECT batch_ordinal, dataset,
                        json_extract(list_extract(list_filter(json_extract(processing_json, '$[1]')::JSON[],
                            entry -> json_extract_string(entry, '$[0]')='entry_kind'), 1), '$[1]') AS kind,
                        {congress_sql} AS congress FROM vote_receipt_batch)
                    SELECT batch_ordinal, dataset,
                        CASE WHEN json_extract_string(kind, '$[0]')='str' THEN json_extract_string(kind, '$[1]') END,
                        CASE WHEN json_extract_string(congress, '$[0][0]')='str'
                            THEN json_extract_string(congress, '$[0][1]') END
                    FROM selected ORDER BY batch_ordinal""").fetchall()
            finally:
                self._con.unregister("vote_receipt_batch")
            names = []
            if [row[0] for row in keys] != list(range(piece.num_rows)):
                raise ValueError("Vote receipt grouping changed batch row order")
            for _, dataset, kind, congress in keys:
                if dataset != "member_votes":
                    raise ValueError("Congress receipt emission is only supported for member_votes")
                if kind == "table_metadata":
                    if self._metadata or names or self.paths:
                        raise ValueError("Vote receipt metadata must occur exactly once, first")
                    self._metadata = True
                    name = "member_votes_metadata"
                else:
                    if not self._metadata or kind != "row":
                        raise ValueError("Vote receipt row requires its original metadata and source fields")
                    if (
                        not isinstance(congress, str)
                        or not congress.isascii()
                        or not congress.isdecimal()
                        or int(congress) < 1
                    ):
                        raise ValueError("Vote receipt requires an explicit positive Congress")
                    name = "member_votes_congress_" + str(int(congress))
                names.append(name)
            start = 0
            while start < len(names):
                end = start + 1
                while end < len(names) and names[end] == names[start]:
                    end += 1
                self._emit(names[start], piece.slice(start, end - start))
                start = end

    def write_refusals(self, table):
        """The carry loop supplies only unresolved prior refusals in original order."""
        if any(value != "refused" for value in table["outcome"].to_pylist()):
            raise ValueError("Vote receipt tail must contain only unresolved refusals")
        self._tail = True
        if table.num_rows:
            self._emit("member_votes_refusals", table)

    def close(self):
        self._flush(final=True)
        if self._writer is not None:
            self._writer.close()
        if not self._metadata:
            raise ValueError("Vote receipts require their original metadata")
