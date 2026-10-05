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
    import pyarrow.compute as pc
    from spicy_regs.etl_receipts import RECEIPT_SCHEMA, _digest, decode_exact_json, exact_json

    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="receipt-history-", dir=destination.parent) as temporary, duckdb.connect() as con:
        con.execute("SET threads=4")
        con.execute("SET memory_limit='1GB'")
        con.execute("SET max_temp_directory_size='32GB'")
        con.execute("SET temp_directory=?", [str(Path(temporary) / "spill")])
        con.execute("SET preserve_insertion_order=true")
        def quoted(path):
            return "'" + str(path).replace("'", "''") + "'"
        con.execute("CREATE VIEW current AS SELECT * EXCLUDE(file_row_number), file_row_number AS ordinal "
                    f"FROM read_parquet({quoted(current_path)}, file_row_number=true, hive_partitioning=false)")
        offset, parts = 0, []
        for path in prior_paths:
            with pq.ParquetFile(path) as parquet:
                count = parquet.metadata.num_rows
            parts.append(f"SELECT * EXCLUDE(file_row_number), file_row_number+{offset} AS ordinal "
                         f"FROM read_parquet({quoted(path)}, file_row_number=true, hive_partitioning=false)")
            offset += count
        con.execute("CREATE VIEW prior AS " + (" UNION ALL ".join(parts) if parts else "SELECT * FROM current WHERE false"))
        duplicate = con.execute("SELECT EXISTS(SELECT 1 FROM prior WHERE outcome='accepted' "
                       "GROUP BY dataset,record_id HAVING count(*)>1)").fetchone()
        assert duplicate is not None
        if duplicate[0]:
            raise ValueError("Conflicting selected prior receipts")
        for name in ("current", "prior"):
            con.execute(f"CREATE VIEW {name}_ranked AS SELECT *, row_number() OVER(PARTITION BY dataset,outcome,"
                        "processing_json, CASE WHEN outcome='accepted' THEN record_id END, "
                        "CASE WHEN outcome='accepted' THEN subject_version END, "
                        "CASE WHEN outcome<>'accepted' THEN diagnostic_json END ORDER BY ordinal) AS occurrence "
                        f"FROM {name}")
        fields = ",".join(f'CASE WHEN p.ordinal IS NULL THEN c."{n}" ELSE p."{n}" END AS "{n}"'
                          for n in RECEIPT_SCHEMA.names)
        query = f"""SELECT {fields}, c.ordinal AS current_ordinal,
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
            expected_count = pq.ParquetFile(current_path).metadata.num_rows
            written = 0
            for batch in reader:
                expected_order = pa.array(range(written, written + batch.num_rows), type=pa.int64())
                if not pc.call_function("all", [pc.call_function("equal", [batch.column("current_ordinal"), expected_order])]).as_py():
                    raise ValueError("Receipt history changed current receipt order")
                table = pa.Table.from_batches([batch])
                changed = pc.call_function("indices_nonzero", [pc.call_function("is_valid", [table["predecessor_id"]])])
                if len(changed):
                    updates = table.take(changed).to_pylist()
                    diagnostics, identities = [], []
                    for row in updates:
                        predecessor = row.pop("predecessor_id")
                        generation = row.pop("predecessor_generation")
                        processing = row.pop("predecessor_processing")
                        row.pop("current_ordinal")
                        values = decode_exact_json(row["diagnostic_json"])
                        values = {k: v for k, v in values.items()
                                  if k not in {"prior_receipts", "retained_processing", "prior_receipt"}}
                        values["prior_receipt"] = {"receipt_id": predecessor, "generation_id": generation,
                                                 "processing_sha256": _digest(decode_exact_json(processing))}
                        row["diagnostic_json"] = exact_json(values)
                        diagnostics.append(row["diagnostic_json"])
                        identities.append(_digest({k: v for k, v in row.items() if k != "receipt_id"}))
                    # Replace only changed accepted rows. Every unchanged row stays Arrow, with no Python decoding.
                    positions = changed.to_pylist()
                    for name, values in (("diagnostic_json", diagnostics), ("receipt_id", identities)):
                        by_position = dict(zip(positions, values))
                        replacements = pa.array([by_position.get(i) for i in range(batch.num_rows)], type=pa.string())
                        table = table.set_column(table.schema.get_field_index(name), name,
                                                 pc.call_function("if_else", [pc.call_function("is_valid", [table["predecessor_id"]]),
                                                                             replacements, table[name]]))
                writer.write_table(table.select(RECEIPT_SCHEMA.names).cast(RECEIPT_SCHEMA))
                written += batch.num_rows
            if written != expected_count:
                raise ValueError("Receipt history changed current receipt count")
        target.replace(destination)
    return destination
