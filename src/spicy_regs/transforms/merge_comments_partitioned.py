"""Transform: merge staging comments into Hive-partitioned output files."""

from pathlib import Path

from loguru import logger

from spicy_regs.transforms.comment_partitions import HIVE_NULL, comment_partition_path, validate_comment_coordinates


def merge_comments_partitioned(
    staging_dir: Path,
    output_dir: Path,
    schema: dict,
    dedup_key: str,
    *,
    source_correction: bool = False,
    download_existing: bool = True,
) -> list[Path]:
    """Merge staging comments into partitioned output by agency/docket/year/month.

    Instead of merging all comments into one monolithic file (which OOM's on CI
    runners), each batch's comments go into small Hive-partitioned files
    (``comments/agency_code={A}/docket_id={D}/year={Y}/month={M}/part-0.parquet``):
    coordinates are validated first, then for each affected partition the
    existing file is downloaded from R2 (if it exists), merged with the new
    staging data, deduplicated by ``dedup_key`` keeping the latest
    ``modify_date``, and written back. Returns the list of changed partition
    file paths.

    ``source_correction`` additionally refuses duplicate identities in the
    staging input and any relocation of an existing identity to another
    partition: moving or removing old partitions requires a coherent generation
    rebuild, not a bounded repair.
    """
    import duckdb

    from spicy_regs.sources.r2 import download_from_r2

    staging_type_dir = staging_dir / "comments"
    if not staging_type_dir.exists():
        return []

    staging_files = list(staging_type_dir.glob("*.parquet"))
    if not staging_files:
        return []

    comments_dir = output_dir / "comments"
    target_columns = list(schema.keys())

    # Escape single quotes in paths for SQL.
    def sql_path(p: Path) -> str:
        return str(p).replace("'", "''")

    files_sql = ", ".join(f"'{sql_path(p)}'" for p in staging_files)
    col_select = ", ".join(f'CAST("{c}" AS VARCHAR) AS "{c}"' for c in target_columns)

    logger.info("Processing {} comment staging files into partitions...", len(staging_files))

    # Load staging into a temp table for efficient per-partition queries.
    con = duckdb.connect()
    try:
        con.execute("SET memory_limit='4GB'")
        con.execute("SET preserve_insertion_order=false")

        validate_comment_coordinates(con, f"SELECT * FROM read_parquet([{files_sql}], union_by_name=true)")
        if source_correction:
            duplicates = con.execute(f"""
                SELECT count(*) - count(DISTINCT "{dedup_key}")
                FROM read_parquet([{files_sql}], union_by_name=true)
            """).fetchone()
            if duplicates and duplicates[0]:
                raise ValueError("source correction requires distinct comment identities")

        con.execute(f"""
            CREATE TABLE _staging AS
            SELECT {col_select},
                TRIM(docket_id, '"') AS _clean_docket,
                EXTRACT(YEAR FROM CAST(posted_date AS TIMESTAMP))::INT AS _year,
                EXTRACT(MONTH FROM CAST(posted_date AS TIMESTAMP))::INT AS _month
            FROM read_parquet([{files_sql}], union_by_name=true)
        """)

        partitions = con.execute("""
            SELECT DISTINCT agency_code, _clean_docket, _year, _month
            FROM _staging
        """).fetchall()

        if not partitions:
            return []

        if source_correction:
            # The bounded local repair cannot leave a corrected identity behind in
            # a different old partition. Refuse relocation before writing any file;
            # moving/removing old partitions requires a coherent generation rebuild.
            # The guard must read the whole tree: a corrected identity's current
            # home can be any partition, not only one this staging touches.
            # An interrupted replacement can leave our temporary write behind;
            # it has never become part of the committed local prior.
            prior_files = [p for p in comments_dir.rglob("part-*.parquet") if not p.name.endswith(".tmp.parquet")]
            if prior_files:
                prior_paths = ", ".join(f"'{sql_path(p)}'" for p in prior_files)
                moved = con.execute(f"""
                    SELECT count(*) FROM _staging f
                    JOIN read_parquet([{prior_paths}], union_by_name=true, hive_partitioning=false, filename=true) p
                      ON f."{dedup_key}" = p."{dedup_key}"
                    WHERE f.agency_code IS DISTINCT FROM p.agency_code
                       OR f._clean_docket IS DISTINCT FROM trim(p.docket_id, '\"')
                       OR f._year IS DISTINCT FROM extract(year FROM try_cast(p.posted_date AS TIMESTAMP))
                       OR f._month IS DISTINCT FROM extract(month FROM try_cast(p.posted_date AS TIMESTAMP))
                       OR p.filename IS DISTINCT FROM concat(
                           '{sql_path(comments_dir)}/agency_code=', f.agency_code,
                           '/docket_id=', coalesce(f._clean_docket, '{HIVE_NULL}'),
                           '/year=', coalesce(f._year::VARCHAR, '{HIVE_NULL}'),
                           '/month=', coalesce(f._month::VARCHAR, '{HIVE_NULL}'), '/part-0.parquet'
                        )
                """).fetchone()
                if moved and moved[0]:
                    raise ValueError(
                        "source correction cannot relocate an existing comment partition; rebuild its generation"
                    )

        logger.info("Found {} affected comment partitions", len(partitions))

        # Download existing partitions from R2 for dedup.
        for agency, docket, year, month in partitions:
            partition_file = comment_partition_path(comments_dir, agency, docket, year, month)
            if not partition_file.exists():
                partition_file.parent.mkdir(parents=True, exist_ok=True)
                if download_existing:
                    r2_key = str(partition_file.relative_to(output_dir))
                    download_from_r2(r2_key, partition_file)

        col_select_plain = ", ".join(f'"{c}"' for c in target_columns)
        changed: list[Path] = []

        def existing_partition_select(path: Path) -> str:
            """Project ``target_columns`` from an existing partition file.

            Older partitions written before a schema change may be missing
            newly-added columns. Reading them with a plain ``SELECT "col"`` would
            raise a binder error, so emit ``NULL AS "col"`` for any column absent
            from the file (DuckDB ``union_by_name`` only fills NULLs across a
            multi-file read, not a single-file one). This lets incremental runs
            merge old partitions into the evolved schema instead of breaking.
            """
            present = {
                row[0] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{sql_path(path)}')").fetchall()
            }
            return ", ".join(f'"{c}"' if c in present else f'CAST(NULL AS VARCHAR) AS "{c}"' for c in target_columns)

        if source_correction:
            for agency, docket, year, month in partitions:
                partition_file = comment_partition_path(comments_dir, agency, docket, year, month)
                temp_file = partition_file.with_suffix(".tmp.parquet")
                docket_sql = "NULL" if docket is None else "'" + docket.replace("'", "''") + "'"

                staging_sql = f"""
                    SELECT {col_select_plain} FROM _staging
                    WHERE agency_code = '{agency}'
                      AND _clean_docket IS NOT DISTINCT FROM {docket_sql}
                      AND _year IS NOT DISTINCT FROM {"NULL" if year is None else year}
                      AND _month IS NOT DISTINCT FROM {"NULL" if month is None else month}
                """

                if partition_file.exists():
                    prior_sql = f"SELECT {existing_partition_select(partition_file)} FROM read_parquet('{sql_path(partition_file)}')"
                else:
                    prior_sql = f"SELECT {', '.join(f'NULL::VARCHAR AS "{c}"' for c in target_columns)} WHERE false"
                from spicy_regs.transforms.regulations_correction import correction_query

                corrected = correction_query(
                    con,
                    fresh_sql=staging_sql,
                    prior_sql=prior_sql,
                    columns=target_columns,
                    key=dedup_key,
                )
                con.execute(
                    f"COPY ({corrected} ORDER BY posted_date) TO '{sql_path(temp_file)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
                )
                temp_file.replace(partition_file)
                changed.append(partition_file)
        else:
            # One merge for every affected partition: staging rows plus the
            # existing files (multi-file read fills columns absent from older
            # files), deduplicated once by key keeping the latest modify_date,
            # then written in one ordered pass instead of one scan per group.
            from spicy_regs.transforms.partition_stream import write_partition_stream

            existing_files = [
                comment_partition_path(comments_dir, agency, docket, year, month)
                for agency, docket, year, month in partitions
                if comment_partition_path(comments_dir, agency, docket, year, month).exists()
            ]
            # One SELECT per existing file: older files may lack columns this
            # schema added later, and union_by_name only fills NULLs across a
            # multi-file read, so each file projects its own missing columns.
            existing_selects = [
                f"""
                SELECT {existing_partition_select(path)},
                       TRIM(docket_id, '"') AS _clean_docket,
                       EXTRACT(YEAR FROM CAST(posted_date AS TIMESTAMP))::INT AS _year,
                       EXTRACT(MONTH FROM CAST(posted_date AS TIMESTAMP))::INT AS _month
                FROM read_parquet('{sql_path(path)}')
                """
                for path in existing_files
            ]
            existing_select = "\nUNION ALL BY NAME\n".join(existing_selects)
            con.execute(f"""
                CREATE TEMP TABLE _merged AS
                SELECT {col_select_plain}, _clean_docket, _year, _month FROM (
                    SELECT {col_select_plain}, _clean_docket, _year, _month FROM _staging
                    {"UNION ALL BY NAME " if existing_select else ""}{existing_select}
                )
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY "{dedup_key}"
                    ORDER BY modify_date DESC NULLS LAST
                ) = 1
            """)
            written = write_partition_stream(
                con,
                """
                    SELECT * FROM _merged
                    ORDER BY agency_code, _clean_docket, _year NULLS FIRST, _month NULLS FIRST, posted_date
                """,
                key_columns=("agency_code", "_clean_docket", "_year", "_month"),
                path_of=lambda key: comment_partition_path(comments_dir, key[0], key[1], key[2], key[3]),
                drop_columns=("_clean_docket", "_year", "_month"),
                label="comments",
            )
            changed.extend(written)

    finally:
        con.close()
    logger.info("Updated {} comment partitions", len(changed))
    return changed
