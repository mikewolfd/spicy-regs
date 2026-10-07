"""Exercise request-time native receipt selection using only the image's packages."""

from pathlib import Path
from tempfile import TemporaryDirectory
import sys
import hashlib
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.etl_receipts import RECEIPT_SCHEMA, select_receipts


def check_public_download():
    from spicy_regs.local_data import file_state_from_stat
    from spicy_regs.sources import publication

    data = b"checked runtime download"
    member = publication.Member("fixture.parquet", "sha256:" + hashlib.sha256(data).hexdigest(), len(data), 1)
    class Response:
        status_code = 200
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def iter_bytes(self): yield data
    with TemporaryDirectory() as temp, patch.object(publication.httpx, "stream", return_value=Response()):
        target = Path(temp) / "download.parquet"
        checked = publication.fetch_member("https://fixture.invalid", member, target)
        assert isinstance(checked, publication.DownloadedMember)
        assert checked.sha256 == member.sha256 and checked.byte_size == len(data)
        assert target.read_bytes() == data
        assert checked.state == file_state_from_stat(target.lstat())
    assert "rulespec_artifacts" not in sys.modules


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
    check_public_download()
    print("Native citation receipt selection and checked public download passed")
