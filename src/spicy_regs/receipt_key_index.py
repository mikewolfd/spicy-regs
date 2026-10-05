"""Pinned key-to-position readers. Receipt files keep their original row order."""
from __future__ import annotations

import json
import hashlib
from pathlib import Path
import re
from collections.abc import Mapping, Sequence

KEY = "etl_receipts.keys.parquet"
FORMAT = "spicy-receipt-keys/1"
THRESHOLD = 2_000_000
COLUMNS = [[name, "VARCHAR"] for name in (
    "dataset", "record_id", "outcome", "subject_version", "receipt_id", "policy_version"
)] + [["row_number", "BIGINT"]]
_CHECKED: dict[tuple, list[int] | None] = {}
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")


def validate_descriptor(value: Mapping, receipt: Mapping) -> None:
    if (not isinstance(value, dict) or set(value) != {"key", "format", "sha256", "byteSize", "rows", "receiptSha256"}
            or value["key"] != KEY or value["format"] != FORMAT
            or not isinstance(value["sha256"], str) or not _DIGEST.fullmatch(value["sha256"])
            or any(type(value[k]) is not int or value[k] < 0 for k in ("byteSize", "rows"))
            or value["rows"] != receipt["rows"] or value["receiptSha256"] != receipt["sha256"]):
        raise ValueError("Invalid or stale receipt key index descriptor")


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Repeated receipt key index metadata key")
        result[key] = value
    return result


def _binding(metadata, receipt):
    expected = {"format": FORMAT, "receiptSha256": receipt["sha256"],
                "receiptByteSize": receipt["byteSize"], "receiptRows": receipt["rows"]}
    return (isinstance(metadata, dict) and metadata == expected
            and all(type(metadata[k]) is int for k in ("receiptByteSize", "receiptRows")))


def _verify_bytes(location, descriptor):
    """Rehash sidecar bytes once per immutable remote pin, or unchanged local signature."""
    from spicy_regs.local_data import file_signature
    remote = location.startswith(("https://", "http://"))
    signature = None if remote else file_signature(Path(location))
    key = (location, descriptor["sha256"], descriptor["byteSize"])
    if key in _CHECKED and _CHECKED[key] == signature:
        return
    digest, size = hashlib.sha256(), 0
    def consume(chunks):
        nonlocal size
        for chunk in chunks:
            size += len(chunk)
            if size > descriptor["byteSize"]:
                raise ValueError("Receipt key index exceeds its pinned byte size")
            digest.update(chunk)
    if remote:
        import httpx
        with httpx.stream("GET", location, timeout=60) as response:
            response.raise_for_status()
            consume(response.iter_bytes(chunk_size=1024 * 1024))
    else:
        with Path(location).open("rb") as stream:
            consume(iter(lambda: stream.read(1024 * 1024), b""))
        if file_signature(Path(location)) != signature:
            raise ValueError("Receipt key index changed while hashing")
    if size != descriptor["byteSize"] or "sha256:" + digest.hexdigest() != descriptor["sha256"]:
        raise ValueError("Receipt key index differs from its exact byte pin")
    if len(_CHECKED) >= 256:
        _CHECKED.clear()
    _CHECKED[key] = signature


def check_reader(cursor, location: str, descriptor: Mapping, receipt: Mapping) -> None:
    """Check the declared sidecar's schema, footer count and exact bound receipt identity.

    A remote location must be the immutable URL of the admitted, digest-pinned member.
    Local bytes are rehashed by the local selection reader before binding.
    """
    validate_descriptor(descriptor, receipt)
    try:
        _verify_bytes(location, descriptor)
        columns = [list(row[:2]) for row in cursor.execute(
            "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false)", [location]).fetchall()]
        metadata = {}
        seen = False
        for key, value in cursor.execute("SELECT key, value FROM parquet_kv_metadata(?)", [location]).fetchall():
            if bytes(key) == b"spicy_receipt_key_index":
                if seen:
                    raise ValueError("Repeated receipt key index metadata")
                seen = True
                metadata = json.loads(bytes(value), object_pairs_hook=_pairs)
        count = cursor.execute("SELECT num_rows FROM parquet_file_metadata(?)", [location]).fetchone()[0]
        if columns != COLUMNS or count != descriptor["rows"] or not _binding(metadata, receipt):
            raise ValueError("Receipt key index differs from its pinned receipt member")
    except Exception as error:
        raise ValueError("Declared receipt key index unavailable, corrupt or stale") from error


def lookup_receipts(cursor, receipt_location: str, index_location: str, descriptor: Mapping,
                    receipt: Mapping, *, dataset: str, record_ids: Sequence[str]) -> list[list[dict]]:
    """Return each requested key's accepted receipts in original file order.

    Canonical record hashes include null identity components and control characters.
    Duplicate keys preserve caller order. Missing keys return an empty list; no subject
    scan or alternative generation is used. Callers decide how to describe a miss.
    """
    from spicy_regs.etl_receipts import RECEIPT_SCHEMA, validate_receipt_row
    from spicy_regs.subject_catalog import policies

    if (not record_ids or len(record_ids) > 100 or not isinstance(dataset, str)
            or any(not isinstance(key, str) or not _DIGEST.fullmatch(key) for key in record_ids)):
        raise ValueError("Receipt key lookup needs one to 100 canonical record hashes")
    check_reader(cursor, index_location, descriptor, receipt)
    unique_keys = list(dict.fromkeys(record_ids))
    placeholders = ",".join("?" for _ in unique_keys)
    pointers = cursor.execute(
        "SELECT record_id, receipt_id, subject_version, policy_version, row_number FROM read_parquet(?, "
        f"hive_partitioning=false) WHERE dataset=? AND outcome='accepted' AND record_id IN ({placeholders}) "
        "ORDER BY row_number LIMIT 101", [index_location, dataset, *unique_keys]).fetchall()
    if len(pointers) > len(unique_keys) or len({p[0] for p in pointers}) != len(pointers):
        raise ValueError("Receipt key index contains ambiguous accepted keys")
    ordinals = [p[4] for p in pointers]
    if len(set(ordinals)) != len(ordinals) or any(type(n) is not int or not 0 <= n < receipt["rows"] for n in ordinals):
        raise ValueError("Receipt key index contains invalid positions")
    found = {}
    if pointers:
        result = cursor.execute(
            "SELECT * FROM read_parquet(?, file_row_number=true, hive_partitioning=false) "
            "WHERE " + " OR ".join("(file_row_number>=? AND file_row_number<?)" for _ in ordinals) +
            " ORDER BY file_row_number", [receipt_location, *(v for n in ordinals for v in (n, n + 1))])
        names = [item[0] for item in result.description]
        rows = [dict(zip(names, row)) for row in result.fetchall()]
        if len(rows) != len(pointers):
            raise ValueError("Receipt key index positions are absent from the receipt file")
        for pointer, row in zip(pointers, rows, strict=True):
            ordinal = row.pop("file_row_number")
            if (set(row) != set(RECEIPT_SCHEMA.names) or row["dataset"] != dataset or row["outcome"] != "accepted"
                    or (row["record_id"], row["receipt_id"], row["subject_version"], row["policy_version"], ordinal) != pointer):
                raise ValueError("Receipt key index pointer differs from the selected receipt")
            validate_receipt_row(row, policies())
            found.setdefault(row["record_id"], []).append(row)
    return [found.get(key, []) for key in record_ids]


def verify_key_index(receipt_path, index_path, descriptor: Mapping, receipt: Mapping) -> None:
    """Admission checks every indexed position and key against the unchanged receipt file."""
    from pathlib import Path
    from tempfile import TemporaryDirectory
    import duckdb
    from spicy_regs.etl_receipts import _parquet

    validate_descriptor(descriptor, receipt)
    with TemporaryDirectory(prefix="receipt-index-verify-") as temporary, duckdb.connect() as con:
        con.execute("SET threads=4")
        con.execute("SET memory_limit='1GB'")
        con.execute("SET max_temp_directory_size='32GB'")
        con.execute("SET temp_directory=?", [str(Path(temporary) / "spill")])
        with _parquet(index_path) as parquet:
            metadata = json.loads((parquet.schema_arrow.metadata or {}).get(b"spicy_receipt_key_index", b"null"), object_pairs_hook=_pairs)
            if (not _binding(metadata, receipt) or parquet.metadata.num_rows != receipt["rows"]
                    or parquet.schema_arrow.names != [c[0] for c in COLUMNS]):
                raise ValueError("Receipt key index shape or binding differs")
            for ordinal, batch in enumerate(parquet.iter_batches(batch_size=65536)):
                con.register("batch", batch)
                if ordinal == 0:
                    con.execute("CREATE TABLE keys AS SELECT * FROM batch")
                else:
                    con.execute("INSERT INTO keys SELECT * FROM batch")
                con.unregister("batch")
            if parquet.metadata.num_rows == 0:
                con.register("batch", parquet.schema_arrow.empty_table())
                con.execute("CREATE TABLE keys AS SELECT * FROM batch")
                con.unregister("batch")
        actual = [list(r[:2]) for r in con.execute("DESCRIBE keys").fetchall()]
        if actual != COLUMNS:
            raise ValueError("Receipt key index schema differs")
        names = [c[0] for c in COLUMNS[:-1]]
        with _parquet(receipt_path) as parquet:
            count = 0
            for batch in parquet.iter_batches(batch_size=65536, columns=names):
                import pyarrow as pa
                table = pa.Table.from_batches([batch]).append_column(
                    "row_number", pa.array(range(count, count + batch.num_rows), type=pa.int64()))
                con.register("batch", table)
                if count == 0:
                    con.execute("CREATE TABLE originals AS SELECT * FROM batch")
                else:
                    con.execute("INSERT INTO originals SELECT * FROM batch")
                con.unregister("batch")
                count += batch.num_rows
            if count == 0:
                con.execute("CREATE TABLE originals AS SELECT * FROM keys WHERE false")
        same = " AND ".join(f'k."{name}" IS NOT DISTINCT FROM o."{name}"' for name in names)
        result = con.execute(
            "SELECT EXISTS(SELECT 1 FROM keys GROUP BY row_number HAVING count(*)<>1) OR "
            "EXISTS(SELECT 1 FROM keys k FULL OUTER JOIN originals o USING(row_number) WHERE "
            f"k.row_number IS NULL OR o.row_number IS NULL OR NOT ({same}))").fetchone()
        assert result is not None
        unsorted = con.execute("SELECT EXISTS(SELECT 1 FROM (SELECT rowid+1 AS position, row_number() OVER "
                               "(ORDER BY dataset,record_id NULLS LAST,outcome,receipt_id,row_number) AS expected "
                               "FROM keys) WHERE position<>expected)").fetchone()
        assert unsorted is not None
        if count != receipt["rows"] or result[0] or unsorted[0]:
            raise ValueError("Receipt key index does not exactly map every receipt in file order")
