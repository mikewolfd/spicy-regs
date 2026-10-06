"""One bounded set comparison carries immutable receipt occurrences in file order."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from collections.abc import Sequence
from bisect import bisect_right
from contextlib import ExitStack


def _processing_key(column: str) -> str:
    """A narrow candidate key; matching raw values are checked before output."""
    return f"unhex(sha256({column}))"


def _take_prior_rows(priors, file_ends, group_ends, ordinals, schema):
    """Gather one output batch, reading each required prior row group once."""
    import pyarrow as pa

    requests = {}
    for position, ordinal in enumerate(ordinals):
        if ordinal is None:
            continue
        file = bisect_right(file_ends, ordinal)
        if ordinal < 0 or file == len(priors):
            raise ValueError("Receipt history selected an invalid prior ordinal")
        local = ordinal - (file_ends[file - 1] if file else 0)
        group = bisect_right(group_ends[file], local)
        row = local - (group_ends[file][group - 1] if group else 0)
        requests.setdefault((file, group), []).append((position, row))
    chunks, positions = [], []
    for (file, group), rows in requests.items():
        source = priors[file].read_row_group(group, columns=schema.names, use_threads=False)
        chunks.append(source.take(pa.array([row for _, row in rows], type=pa.int64())))
        del source
        positions.extend(position for position, _ in rows)
    if not chunks:
        return pa.Table.from_batches([], schema=schema), []
    order = sorted(range(len(positions)), key=positions.__getitem__)
    return pa.concat_tables(chunks).take(pa.array(order, type=pa.int64())), sorted(positions)


def carry_receipt_history(current_path: Path, prior_paths: Sequence[Path], destination: Path) -> Path:
    """Keep identical attempts exactly; reference only the direct accepted predecessor on change.

    Inputs are fresh current receipts and an already validated selected prior bundle.
    Unmatched old records disappear. Duplicate nonaccepted occurrences match FIFO.
    Narrow matching keys spill to private disk. Full prior rows are gathered by
    Parquet ordinal in output batches; Python only decodes changed predecessors.
    """
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyarrow.compute as pc
    from spicy_regs.etl_receipts import RECEIPT_SCHEMA, _digest, decode_exact_json, exact_json

    def compute(name, *args):
        # Arrow generates these kernels at import time; use its typed entry point.
        return pc.call_function(name, list(args))

    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with (
        TemporaryDirectory(prefix="receipt-history-", dir=destination.parent) as temporary,
        duckdb.connect(str(Path(temporary) / "history.duckdb")) as con,
        ExitStack() as files,
    ):
        con.execute("SET threads=1")
        con.execute("SET memory_limit='1GB'")
        con.execute("SET max_temp_directory_size='32GB'")
        con.execute("SET temp_directory=?", [str(Path(temporary) / "spill")])
        # Ordinals define occurrence and final output order explicitly. Avoid
        # extra import/export buffers while the wide history spills to disk.
        con.execute("SET preserve_insertion_order=false")

        def quoted(path):
            return "'" + str(path).replace("'", "''") + "'"

        con.execute(
            "CREATE VIEW current AS SELECT * EXCLUDE(file_row_number), file_row_number AS ordinal "
            f"FROM read_parquet({quoted(current_path)}, file_row_number=true, hive_partitioning=false)"
        )
        offset, parts, priors, file_ends, group_ends = 0, [], [], [], []
        for path in prior_paths:
            parquet = files.enter_context(pq.ParquetFile(path))
            priors.append(parquet)
            count = parquet.metadata.num_rows
            ends, total = [], 0
            for group in range(parquet.metadata.num_row_groups):
                total += parquet.metadata.row_group(group).num_rows
                ends.append(total)
            group_ends.append(ends)
            parts.append(
                f"SELECT * EXCLUDE(file_row_number), file_row_number+{offset} AS ordinal "
                f"FROM read_parquet({quoted(path)}, file_row_number=true, hive_partitioning=false)"
            )
            offset += count
            file_ends.append(offset)
        con.execute(
            "CREATE VIEW prior AS " + (" UNION ALL ".join(parts) if parts else "SELECT * FROM current WHERE false")
        )
        duplicate = con.execute(
            "SELECT EXISTS(SELECT 1 FROM prior WHERE outcome='accepted' GROUP BY dataset,record_id HAVING count(*)>1)"
        ).fetchone()
        assert duplicate is not None
        if duplicate[0]:
            raise ValueError("Conflicting selected prior receipts")
        for name in ("current", "prior"):
            con.execute(
                f"CREATE TABLE {name}_keys AS SELECT ordinal,dataset,outcome,record_id,subject_version, "
                "CASE WHEN outcome='accepted' THEN record_id ELSE '' END AS accepted_record_id, "
                "CASE WHEN outcome='accepted' THEN subject_version ELSE '' END AS accepted_subject_version, "
                f"{_processing_key('processing_json')} AS processing_key, "
                f"CASE WHEN outcome='accepted' THEN unhex('') ELSE {_processing_key('diagnostic_json')} "
                "END AS diagnostic_key "
                f"FROM {name}"
            )
            con.execute(
                f"CREATE TABLE {name}_ranked AS SELECT *, row_number() OVER(PARTITION BY dataset,outcome,"
                "processing_key, accepted_record_id, accepted_subject_version, "
                "diagnostic_key ORDER BY ordinal) AS occurrence "
                f"FROM {name}_keys"
            )
        # Outcome separates the branches, so ignored fields use fixed keys.
        # Ordinary equality preserves null refusal and admits a hash join
        # instead of an OR join that compares every current/prior pair.
        con.execute("""CREATE TABLE matches AS SELECT c.ordinal AS current_ordinal,p.ordinal AS prior_ordinal,
            CASE WHEN p.ordinal IS NULL AND c.outcome='accepted' THEN predecessor.ordinal END AS predecessor_ordinal
            FROM current_ranked c LEFT JOIN prior_ranked p ON c.dataset=p.dataset AND c.outcome=p.outcome
            AND c.processing_key=p.processing_key AND c.occurrence=p.occurrence
            AND c.accepted_record_id=p.accepted_record_id
            AND c.accepted_subject_version=p.accepted_subject_version
            AND c.diagnostic_key=p.diagnostic_key
            LEFT JOIN prior_keys predecessor ON c.dataset=predecessor.dataset AND c.record_id=predecessor.record_id
            AND predecessor.outcome='accepted'""")
        for name in ("current_ranked", "prior_ranked", "current_keys", "prior_keys"):
            con.execute(f"DROP TABLE {name}")
        # Only ordinals are sorted. Joining/sorting literal JSON and full rows
        # can exhaust the connection's memory even with spilling enabled.
        reader = con.execute("SELECT * FROM matches ORDER BY current_ordinal").to_arrow_reader(batch_size=2000)
        target = Path(temporary) / "receipts.parquet"
        with (
            pq.ParquetFile(current_path) as current,
            pq.ParquetWriter(target, RECEIPT_SCHEMA, compression="zstd") as writer,
        ):
            expected_count = current.metadata.num_rows
            written = 0
            for batch in current.iter_batches(batch_size=2000, columns=RECEIPT_SCHEMA.names, use_threads=False):
                try:
                    mapping = reader.read_next_batch()
                except StopIteration as error:
                    raise ValueError("Receipt history changed current receipt count") from error
                if mapping.num_rows != batch.num_rows:
                    raise ValueError("Receipt history changed current receipt count")
                expected_order = pa.array(range(written, written + batch.num_rows), type=pa.int64())
                if not pc.call_function(
                    "all", [pc.call_function("equal", [mapping.column("current_ordinal"), expected_order])]
                ).as_py():
                    raise ValueError("Receipt history changed current receipt order")
                table = pa.Table.from_batches([batch])
                ordinals = compute("coalesce", mapping.column("prior_ordinal"), mapping.column("predecessor_ordinal"))
                prior, positions = _take_prior_rows(priors, file_ends, group_ends, ordinals.to_pylist(), RECEIPT_SCHEMA)
                if positions:
                    selected = table.take(pa.array(positions, type=pa.int64()))
                    retained = pc.take(
                        compute("is_valid", mapping.column("prior_ordinal")), pa.array(positions, type=pa.int64())
                    )

                    def distinct(left, right):
                        equal = pc.fill_null(compute("equal", left, right), False)
                        return compute(
                            "invert",
                            compute("or", equal, compute("and", compute("is_null", left), compute("is_null", right))),
                        )

                    mismatch = compute(
                        "and",
                        retained,
                        compute(
                            "or",
                            distinct(selected["processing_json"], prior["processing_json"]),
                            compute(
                                "and",
                                pc.fill_null(compute("not_equal", selected["outcome"], "accepted"), False),
                                distinct(selected["diagnostic_json"], prior["diagnostic_json"]),
                            ),
                        ),
                    )
                    if compute("any", mismatch).as_py():
                        raise ValueError("Receipt history candidate keys differ from exact values")
                    indices = list(range(batch.num_rows))
                    for i, position in enumerate(positions):
                        if retained[i].as_py():
                            indices[position] = batch.num_rows + i
                    table = pa.concat_tables([table, prior]).take(pa.array(indices, type=pa.int64()))
                changed = [
                    position for position in positions if mapping.column("predecessor_ordinal")[position].is_valid
                ]
                if changed:
                    updates = table.take(pa.array(changed, type=pa.int64())).to_pylist()
                    predecessors = prior.filter(
                        pc.take(
                            compute("is_valid", mapping.column("predecessor_ordinal")),
                            pa.array(positions, type=pa.int64()),
                        )
                    )
                    diagnostics, identities = [], []
                    for row, predecessor in zip(
                        updates,
                        predecessors.select(["receipt_id", "generation_id", "processing_json"]).to_pylist(),
                        strict=True,
                    ):
                        values = decode_exact_json(row["diagnostic_json"])
                        values = {
                            k: v
                            for k, v in values.items()
                            if k not in {"prior_receipts", "retained_processing", "prior_receipt"}
                        }
                        values["prior_receipt"] = {
                            "receipt_id": predecessor["receipt_id"],
                            "generation_id": predecessor["generation_id"],
                            "processing_sha256": _digest(decode_exact_json(predecessor["processing_json"])),
                        }
                        row["diagnostic_json"] = exact_json(values)
                        diagnostics.append(row["diagnostic_json"])
                        identities.append(_digest({k: v for k, v in row.items() if k != "receipt_id"}))
                    # Replace only changed accepted rows. Every unchanged row stays Arrow, with no Python decoding.
                    for name, values in (("diagnostic_json", diagnostics), ("receipt_id", identities)):
                        by_position = dict(zip(changed, values))
                        replacements = pa.array([by_position.get(i) for i in range(batch.num_rows)], type=pa.string())
                        table = table.set_column(
                            table.schema.get_field_index(name),
                            name,
                            pc.call_function(
                                "if_else",
                                [compute("is_valid", mapping.column("predecessor_ordinal")), replacements, table[name]],
                            ),
                        )
                writer.write_table(table.select(RECEIPT_SCHEMA.names).cast(RECEIPT_SCHEMA))
                written += batch.num_rows
            if written != expected_count:
                raise ValueError("Receipt history changed current receipt count")
            if next(reader, None) is not None:
                raise ValueError("Receipt history changed current receipt count")
        target.replace(destination)
    return destination
