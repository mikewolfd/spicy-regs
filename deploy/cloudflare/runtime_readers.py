"""Exercise request-time native receipt selection using only the image's packages."""

from pathlib import Path
from tempfile import TemporaryDirectory
import sys

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.etl_receipts import RECEIPT_SCHEMA, select_receipts


def check_native_selection():
    # Selection must retain all outcomes, in file order, including failed
    # attempts without subjects. Validation belongs to the subsequent reader.
    rows = [
        {"dataset": "document_citations", "outcome": "accepted", "receipt_id": "first"},
        {"dataset": "other", "outcome": "accepted", "receipt_id": "unrelated"},
        {"dataset": "document_citations", "outcome": "error", "receipt_id": "failed"},
    ]
    with TemporaryDirectory() as temp:
        root = Path(temp)
        source, selected = root / "shared.parquet", root / "selected.parquet"
        pq.write_table(pa.Table.from_pylist(rows, schema=RECEIPT_SCHEMA), source)
        select_receipts(source, selected, dataset="document_citations")
        result = pq.read_table(selected)
        assert result.schema == RECEIPT_SCHEMA
        assert result.column("receipt_id").to_pylist() == ["first", "failed"]
        select_receipts(source, selected, dataset="absent")
        assert pq.read_table(selected).num_rows == 0
    assert "spicy_regs.transforms" not in sys.modules


if __name__ == "__main__":
    check_native_selection()
    print("Native citation receipt selection passed")
