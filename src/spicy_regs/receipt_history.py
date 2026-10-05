"""One bounded set comparison carries immutable receipt occurrences in file order."""
from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from collections.abc import Sequence


def carry_receipt_history(current_path: Path, prior_paths: Sequence[Path], destination: Path) -> Path:
    """Keep identical attempts exactly; reference only the direct accepted predecessor on change.

    Inputs are fresh current receipts and an already validated selected prior bundle.
    Unmatched old records disappear. Duplicate nonaccepted occurrences match FIFO.
    The complete join spills to private disk; Python only decodes bounded output batches.
    """
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq
    from spicy_regs.etl_receipts import RECEIPT_SCHEMA, _digest, decode_exact_json, exact_json

    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="receipt-history-", dir=destination.parent) as temporary, duckdb.connect(
            str(Path(temporary) / "history.duckdb")) as con:
        con.execute("SET threads=4")
        con.execute("SET memory_limit='1GB'")
        con.execute("SET max_temp_directory_size='32GB'")
        con.execute("SET temp_directory=?", [str(Path(temporary) / "spill")])
        con.execute("SET preserve_insertion_order=true")
        con.execute("CREATE TABLE current AS SELECT *, file_row_number AS ordinal FROM read_parquet(?, "
                    "file_row_number=true, hive_partitioning=false)", [str(current_path)])
        con.execute("ALTER TABLE current DROP COLUMN file_row_number")
        con.execute("CREATE TABLE prior AS SELECT * FROM current WHERE false")
        offset = 0
        for path in prior_paths:
            count = pq.ParquetFile(path).metadata.num_rows
            con.execute("INSERT INTO prior SELECT * EXCLUDE(file_row_number), file_row_number+? AS ordinal "
                        "FROM read_parquet(?, file_row_number=true, hive_partitioning=false)", [offset, str(path)])
            offset += count
        duplicate = con.execute("SELECT EXISTS(SELECT 1 FROM prior WHERE outcome='accepted' "
                       "GROUP BY dataset,record_id HAVING count(*)>1)").fetchone()
        assert duplicate is not None
        if duplicate[0]:
            raise ValueError("Conflicting selected prior receipts")
        for name in ("current", "prior"):
            con.execute(f"CREATE TABLE {name}_ranked AS SELECT *, row_number() OVER(PARTITION BY dataset,outcome,"
                        "processing_json, CASE WHEN outcome='accepted' THEN record_id END, "
                        "CASE WHEN outcome='accepted' THEN subject_version END, "
                        "CASE WHEN outcome<>'accepted' THEN diagnostic_json END ORDER BY ordinal) AS occurrence "
                        f"FROM {name}")
        fields = ",".join(f'CASE WHEN p.ordinal IS NULL THEN c."{n}" ELSE p."{n}" END AS "{n}"'
                          for n in RECEIPT_SCHEMA.names)
        query = f"""SELECT {fields},
            CASE WHEN p.ordinal IS NULL AND c.outcome='accepted' THEN predecessor.receipt_id END AS predecessor_id,
            CASE WHEN p.ordinal IS NULL AND c.outcome='accepted' THEN predecessor.generation_id END AS predecessor_generation,
            CASE WHEN p.ordinal IS NULL AND c.outcome='accepted' THEN predecessor.processing_json END AS predecessor_processing
            FROM current_ranked c LEFT JOIN prior_ranked p ON c.dataset=p.dataset AND c.outcome=p.outcome
            AND c.processing_json=p.processing_json AND c.occurrence=p.occurrence
            AND (c.outcome<>'accepted' OR (c.record_id=p.record_id AND c.subject_version=p.subject_version))
            AND (c.outcome='accepted' OR c.diagnostic_json=p.diagnostic_json)
            LEFT JOIN prior predecessor ON c.dataset=predecessor.dataset AND c.record_id=predecessor.record_id
            AND predecessor.outcome='accepted' ORDER BY c.ordinal"""
        # No statements may run while this Arrow result is open.
        reader = con.execute(query).to_arrow_reader(batch_size=2000)
        target = Path(temporary) / "receipts.parquet"
        with pq.ParquetWriter(target, RECEIPT_SCHEMA, compression="zstd") as writer:
            for batch in reader:
                rows = batch.to_pylist()
                for row in rows:
                    predecessor = row.pop("predecessor_id")
                    generation = row.pop("predecessor_generation")
                    processing = row.pop("predecessor_processing")
                    if predecessor is not None:
                        diagnostics = decode_exact_json(row["diagnostic_json"])
                        diagnostics = {k: v for k, v in diagnostics.items()
                                       if k not in {"prior_receipts", "retained_processing", "prior_receipt"}}
                        diagnostics["prior_receipt"] = {"receipt_id": predecessor, "generation_id": generation,
                                                       "processing_sha256": _digest(decode_exact_json(processing))}
                        row["diagnostic_json"] = exact_json(diagnostics)
                        row["receipt_id"] = _digest({k: v for k, v in row.items() if k != "receipt_id"})
                writer.write_table(pa.Table.from_pylist(rows, schema=RECEIPT_SCHEMA))
        target.replace(destination)
    return destination
