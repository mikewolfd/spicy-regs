"""Bounded lookups retain the maintained writers' exact receipt inheritance."""
from pathlib import Path
from tempfile import TemporaryDirectory
from collections.abc import Sequence


def inherit_current_receipts(current: Path, priors: Sequence[Path], destination: Path, *, inherit_observations: bool = True,
                             require_unique_prior: bool = False) -> Path:
    """Batch the disk lookup; preserve the current row writer's inheritance exactly.

    Callers admit both input bundles first. Matching accepted identity inherits
    regardless of whether values changed. Matching observations inherit in prior
    file order, with repeated receipt identities ignored as ReceiptLineage does.
    Failed attempts remain fresh. No prior row replaces a current receipt.
    """
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq
    from spicy_regs.etl_receipts import (RECEIPT_SCHEMA, ReceiptContext, decode_exact_json,
                                        exact_json, inherit_receipt, _digest as receipt_digest)
    with TemporaryDirectory(prefix="current-lineage-", dir=destination.parent) as temp, \
            duckdb.connect(str(Path(temp) / "lineage.duckdb")) as con:
        con.execute("SET threads=4; SET memory_limit='1GB'; SET max_temp_directory_size='32GB'")
        con.execute("SET temp_directory=?", [str(Path(temp) / "spill")])
        parts, offset = [], 0
        for prior in priors:
            literal = "'" + str(prior).replace("'", "''") + "'"
            parts.append(f"SELECT * EXCLUDE(file_row_number), file_row_number+{offset} AS ordinal "
                         f"FROM read_parquet({literal}, file_row_number=true, hive_partitioning=false)")
            offset += pq.read_metadata(prior).num_rows
        if not parts:
            raise ValueError("Prior inheritance requires an admitted prior")
        con.execute("CREATE VIEW prior AS " + " UNION ALL ".join(parts))
        conflicts = "count(*)>1" if require_unique_prior else "count(DISTINCT receipt_id)>1"
        conflicting = con.execute("SELECT EXISTS(SELECT 1 FROM prior WHERE outcome='accepted' "
                                  f"GROUP BY dataset,record_id HAVING {conflicts})").fetchone()
        if conflicting == (True,):
            raise ValueError("Conflicting selected prior receipts")
        con.execute("CREATE TABLE accepted_keys AS SELECT dataset,record_id,min(ordinal) AS ordinal "
                    "FROM prior WHERE outcome='accepted' GROUP BY dataset,record_id")
        observed = con.execute("SELECT * FROM read_parquet(?) WHERE outcome='observed'", [str(current)]).to_arrow_table()
        if observed.num_rows != 1:
            raise ValueError("Current inheritance requires one fresh metadata observation")
        metadata = observed.to_pylist()[0]
        metadata_context = ReceiptContext(metadata['generation_id'], metadata['attempt_id'], metadata['processor'],
                                          metadata['witnesses'], decode_exact_json(metadata['diagnostic_json']))
        if inherit_observations:
            prior_metadata = con.execute("SELECT * EXCLUDE(ordinal) FROM prior WHERE outcome='observed' "
                                        "AND dataset=? AND processing_json=? ORDER BY ordinal",
                                        [metadata['dataset'], metadata['processing_json']]).to_arrow_reader(batch_size=2000)
            seen = set()
            for batch in prior_metadata:
                for row in batch.to_pylist():
                    if row['receipt_id'] not in seen:
                        metadata_context = inherit_receipt(metadata_context, row)
                        seen.add(row['receipt_id'])
        metadata['witnesses'] = list(metadata_context.witnesses)
        metadata['diagnostic_json'] = exact_json(metadata_context.diagnostics)
        metadata['receipt_id'] = receipt_digest({k: v for k, v in metadata.items() if k != 'receipt_id'})
        aliases = ','.join(f'p."{name}" AS "prior_{name}"' for name in RECEIPT_SCHEMA.names)
        query = f"""SELECT c.*, {aliases}
            FROM read_parquet(?,file_row_number=true,hive_partitioning=false) c
            LEFT JOIN accepted_keys k ON c.outcome='accepted' AND c.dataset=k.dataset AND c.record_id=k.record_id
            LEFT JOIN prior p ON p.ordinal=k.ordinal ORDER BY c.file_row_number"""
        # All lookup statements finish before opening the streaming result.
        reader = con.execute(query, [str(current)]).to_arrow_reader(batch_size=2000)
        count = 0
        with pq.ParquetWriter(destination, RECEIPT_SCHEMA, compression='zstd') as writer:
            for batch in reader:
                result = []
                for row in batch.to_pylist():
                    ordinal = row.pop('file_row_number')
                    if ordinal != count:
                        raise ValueError("Prior inheritance changed current receipt order")
                    count += 1
                    prior = {name: row.pop('prior_'+name) for name in RECEIPT_SCHEMA.names}
                    matches = [prior] if prior['receipt_id'] is not None else []
                    if row['outcome'] == 'observed':
                        row = dict(metadata)
                    if matches:
                        context = ReceiptContext(row['generation_id'], row['attempt_id'], row['processor'],
                                                 row['witnesses'], decode_exact_json(row['diagnostic_json']))
                        for match in matches:
                            context = inherit_receipt(context, match)
                        row['witnesses'] = list(context.witnesses)
                        row['diagnostic_json'] = exact_json(context.diagnostics)
                        row['receipt_id'] = receipt_digest({k: v for k, v in row.items() if k != 'receipt_id'})
                    result.append(row)
                writer.write_table(pa.Table.from_pylist(result, schema=RECEIPT_SCHEMA))
        if count != pq.read_metadata(current).num_rows:
            raise ValueError("Prior inheritance changed current receipt count")
    return destination

