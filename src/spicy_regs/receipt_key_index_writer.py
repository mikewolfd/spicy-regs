"""Build auxiliary key indexes without changing receipt identities or physical order."""
from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from spicy_regs.receipt_key_index import KEY, FORMAT, COLUMNS, THRESHOLD, verify_key_index


def build_key_index(receipts: Path, destination: Path | None = None, *, force: bool = False) -> dict | None:
    """Build one index above the receipt bound; ``force`` qualifies the same format on small fixtures.

    Callers admit the complete receipt bundle before publication. This function
    checks the positional map before exposing the new sidecar and returns exact pins.
    Production adoption requires the index readers to be deployed first.
    """
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq
    from spicy_regs.sources.publication import file_identity

    receipts = Path(receipts)
    with pq.ParquetFile(receipts) as parquet:
        count = parquet.metadata.num_rows
    if count <= THRESHOLD and not force:
        return None
    destination = Path(destination) if destination is not None else receipts.with_name(KEY)
    if destination.resolve() == receipts.resolve():
        raise ValueError("A key index cannot replace its receipt file")
    identity = file_identity(receipts)
    receipt = {"sha256": identity["sha256"], "byteSize": identity["bytes"], "rows": count}
    schema = pa.schema([(name, pa.int64() if dtype == "BIGINT" else pa.string()) for name, dtype in COLUMNS],
                       metadata={b"spicy_receipt_key_index": json.dumps({"format": FORMAT,
                           "receiptSha256": receipt["sha256"], "receiptByteSize": receipt["byteSize"],
                           "receiptRows": count}, sort_keys=True).encode()})
    destination.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="receipt-index-build-", dir=destination.parent) as temporary:
        with duckdb.connect() as con:
            con.execute("SET threads=4")
            con.execute("SET memory_limit='1GB'")
            con.execute("SET max_temp_directory_size='32GB'")
            con.execute("SET temp_directory=?", [str(Path(temporary) / "spill")])
            fields = ",".join('"' + n + '"' for n, _ in COLUMNS[:-1])
            reader = con.execute(f"SELECT {fields}, file_row_number AS row_number "
                                 "FROM read_parquet(?, file_row_number=true, hive_partitioning=false) "
                                 "ORDER BY dataset,record_id NULLS LAST,outcome,receipt_id,row_number", [str(receipts)]
                                 ).to_arrow_reader(batch_size=20_000)
            target = Path(temporary) / KEY
            with pq.ParquetWriter(target, schema, compression="zstd") as writer:
                for batch in reader:
                    writer.write_table(pa.Table.from_batches([batch]).cast(schema))
        pin = file_identity(target)
        descriptor = {"key": KEY, "format": FORMAT, "sha256": pin["sha256"], "byteSize": pin["bytes"],
                      "rows": count, "receiptSha256": receipt["sha256"]}
        verify_key_index(receipts, target, descriptor, receipt)
        if file_identity(receipts) != identity:
            raise ValueError("Receipt member changed while building its key index")
        target.replace(destination)
    return descriptor
