"""Build docket-ordered agency files from one scan, then stream a flat export."""

from pathlib import Path
from shutil import rmtree
from tempfile import TemporaryDirectory

import duckdb
from loguru import logger

from spicy_regs.duckdb_settings import ExportResources
from spicy_regs.transforms.comment_partitions import validate_comment_coordinates


def agency_comments(con, files: list[Path]):
    """Restore agency as a string, including codes that look numeric."""
    paths = ", ".join("'" + str(path).replace("'", "''") + "'" for path in files)
    return con.sql(f"SELECT * FROM read_parquet([{paths}], hive_partitioning=true, "
                   "hive_types={'agency_code': 'VARCHAR'})")


def stage_comment_agencies(con, source_sql: str, staging: Path, *, resources: ExportResources) -> None:
    """Scan the caller's pinned source once using bounded native partition writers."""
    logger.info("Staging comments by agency from one source scan")
    con.sql(source_sql).to_parquet(str(staging), partition_by=["agency_code"], **resources.parquet_options)
    if not list(staging.glob("agency_code=*/*.parquet")):
        raise RuntimeError("Empty comments snapshot; refusing to replace the mirror")


def sort_comment_agencies(staging: Path, output_dir: Path, *, resources: ExportResources) -> Path:
    """Sort each agency once; release each connection before processing the next.

    Validate all local staging first and build in a fresh directory so failures
    leave the previous agency files intact and successful builds omit stale files.
    Writers retain whole strings, including values larger than the byte target.
    """
    destination = output_dir / "comments" / "agency"
    with TemporaryDirectory(prefix="comments-sort-", dir=output_dir) as work:
        work_dir = Path(work)
        finished = work_dir / "agency"
        finished.mkdir()
        with duckdb.connect() as con:
            resources.configure(con, work_dir / "spill")
            agency_comments(con, sorted(staging.glob("agency_code=*/*.parquet"))).create_view("comments_staged")
            validate_comment_coordinates(con, "SELECT * FROM comments_staged")
        for agency_dir in sorted(staging.glob("agency_code=*")):
            target = finished / agency_dir.name / "part-0.parquet"
            target.parent.mkdir()
            logger.info("Sorting comments for {}", agency_dir.name)
            with duckdb.connect() as con:
                resources.configure(con, work_dir / "spill")
                con.from_parquet(str(agency_dir / "*.parquet"), hive_partitioning=False).order(
                    "docket_id, posted_date, comment_id"
                ).to_parquet(str(target), **resources.parquet_options)
            rmtree(agency_dir)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            rmtree(destination)
        finished.replace(destination)
    return destination


def assemble_comments(con, partition_dir: Path, output_dir: Path, columns: list[str], *,
                      resources: ExportResources) -> Path:
    """Write the compatible public schema without sorting the full corpus."""
    target = output_dir / "comments.parquet"
    temporary = target.with_suffix(".tmp.parquet")
    projection = ", ".join('"' + name.replace('"', '""') + '"' for name in columns)
    logger.info("Streaming agency files into {}", target)
    agency_comments(con, sorted(partition_dir.glob("agency_code=*/part-0.parquet"))).project(
        projection
    ).to_parquet(str(temporary), **resources.parquet_options)
    temporary.replace(target)
    return target


def partition_comments(output_dir: Path, *, resources: ExportResources | None = None) -> Path:
    """Build agency files for callers starting from a retained monolith."""
    comments_file = output_dir / "comments.parquet"
    if not comments_file.exists():
        raise FileNotFoundError(f"comments.parquet not found in {output_dir}")
    resources = resources or ExportResources()
    with TemporaryDirectory(prefix="comments-stage-", dir=output_dir) as work:
        staging = Path(work) / "staging"
        with duckdb.connect() as con:
            resources.configure(con, Path(work) / "spill")
            con.from_parquet(str(comments_file)).create_view("comments_input")
            stage_comment_agencies(con, "SELECT * FROM comments_input", staging, resources=resources)
        return sort_comment_agencies(staging, output_dir, resources=resources)
