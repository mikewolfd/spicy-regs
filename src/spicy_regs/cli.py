#!/usr/bin/env python3
"""
Spicy Regs CLI - Download and explore federal regulations data.

Usage:
    uvx spicy-regs download           # Download all parquet files
    uvx spicy-regs stats              # Show dataset statistics
    uvx spicy-regs sample dockets     # Show sample rows from a dataset
    uvx spicy-regs search "climate"   # Search across datasets
"""

import argparse
import hashlib
import json
import re
import sys
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from uuid import uuid4

import httpx
from dotenv import load_dotenv

# Public URL for the R2 bucket
PUBLIC_URL = "https://data.spicy-regs.dev"
DATA_TYPES = ["dockets", "documents", "comments", "manifest"]
DEFAULT_OUTPUT_DIR = Path("./spicy-regs-data")

try:
    _PACKAGE_VERSION = version("spicy-regs")
except PackageNotFoundError:
    _PACKAGE_VERSION = "0.0.0"

# Cloudflare rejects the default `Python-urllib/*` / blank User-Agent with a 403
# on this bucket; send an honest identifier instead (this is our own data, so
# there's no need to spoof a browser UA the way sources/pdf.py does for
# downloads.regulations.gov).
DEFAULT_HEADERS = {"User-Agent": f"spicy-regs/{_PACKAGE_VERSION}"}


def _table_name(value: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9_-]*", value):
        raise argparse.ArgumentTypeError(
            "Use a table name containing lowercase letters, numbers, underscores or hyphens"
        )
    return value


def _local_files(output_dir: Path) -> dict[str, tuple[Path, str]]:
    """Capture current once, then resolve selected files before legacy files."""
    result = {}
    current = output_dir / "current"
    if current.is_symlink():
        directory = current.resolve(strict=True)
        if directory.parent != (output_dir / "download-runs").resolve():
            raise RuntimeError("Current download points outside download-runs")
        metadata = json.loads((directory / "download.json").read_text())
        if metadata["version"] != 1 or metadata["status"] != "complete":
            raise RuntimeError("Current download is incomplete")
        for name, selection in metadata["selected"].items():
            _table_name(name)
            path = directory / f"{name}.parquet"
            if not path.is_file():
                raise RuntimeError(f"Current download is missing {name}.parquet")
            result[name] = (path, selection["status"])
    elif current.exists():
        raise RuntimeError("Current download must be a symlink")
    for path in sorted(output_dir.glob("*.parquet")):
        if re.fullmatch(r"[a-z][a-z0-9_-]*", path.stem) and path.is_file():
            result.setdefault(path.stem, (path, "legacy-unversioned"))
    return result


def get_output_dir(args) -> Path:
    """Get output directory from args or default."""
    output_dir = Path(args.output_dir) if hasattr(args, "output_dir") and args.output_dir else DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def download_file(name: str, output_dir: Path, force: bool = False) -> Path | None:
    """Download a parquet file from R2."""
    from spicy_regs.sources.publication import current_index, table_location

    _table_name(name)
    key, published = table_location(current_index(PUBLIC_URL), f"{name}.parquet")
    url = f"{PUBLIC_URL}/{key}"
    local_path = output_dir / f"{name}.parquet"

    if local_path.exists() and not force and published is None:
        size_mb = local_path.stat().st_size / (1024 * 1024)
        print(f"  ✓ {name}.parquet already exists ({size_mb:.1f} MB)")
        return local_path

    print(f"  ⬇ Downloading {name}.parquet...")
    # Stream to a sibling temp file and rename only on success. A partial write
    # left at local_path would be indistinguishable from a complete one on the
    # next run (the exists() check above short-circuits), silently handing back
    # a truncated parquet — a real risk on comments.parquet at ~2.4 GB.
    tmp_path = local_path.with_suffix(".parquet.partial")
    try:
        digest = hashlib.sha256()
        size = 0
        with httpx.stream("GET", url, headers=DEFAULT_HEADERS, follow_redirects=True, timeout=60.0) as resp:
            resp.raise_for_status()
            with open(tmp_path, "wb") as f:
                for chunk in resp.iter_bytes():
                    f.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
        if published is not None and (
            size != published["byteSize"] or "sha256:" + digest.hexdigest() != published["sha256"]
        ):
            raise ValueError(f"Downloaded member differs from its generation pin: {name}")
        tmp_path.replace(local_path)
        size_mb = local_path.stat().st_size / (1024 * 1024)
        print(f"  ✓ {name}.parquet ({size_mb:.1f} MB)")
        return local_path
    except (httpx.HTTPError, OSError, ValueError) as e:
        tmp_path.unlink(missing_ok=True)
        print(f"  ✗ Failed to download {name}.parquet: {e}")
        return None


def cmd_download(args):
    """Download parquet files from R2."""
    output_dir = get_output_dir(args)
    print(f"Downloading to: {output_dir.absolute()}")

    types_to_download = list(dict.fromkeys(args.types or ["dockets", "documents", "comments"]))
    for name in types_to_download:
        _table_name(name)

    from spicy_regs.sources.publication import snapshot, table_location

    with snapshot(PUBLIC_URL) as index:
        selected = {}
        for name in types_to_download:
            key, published = table_location(index, f"{name}.parquet")
            selected[name] = {"key": key, "status": "managed" if published is not None else "legacy-unversioned"}
        managed = any(item["status"] == "managed" for item in selected.values())
        if managed:
            batch_id = uuid4().hex
            destination = output_dir / "download-runs" / batch_id
            destination.mkdir(parents=True, exist_ok=False)
            metadata = {
                "version": 1,
                "status": "incomplete",
                "base_url": PUBLIC_URL,
                "publication": index,
                "selected": selected,
            }
            (destination / "download.json").write_text(json.dumps(metadata, indent=2) + "\n")
        else:
            destination = output_dir
        for data_type in types_to_download:
            print(f"  {data_type}: {selected[data_type]['status']}")
            if download_file(data_type, destination, force=args.force) is None:
                raise RuntimeError(f"Download incomplete: {data_type}")
        if managed:
            metadata["status"] = "complete"
            (destination / "download.json").write_text(json.dumps(metadata, indent=2) + "\n")
            # The temporary link and current share a filesystem. Replace only
            # after every selected member passed download verification.
            link = output_dir / f".current-{batch_id}"
            try:
                current = output_dir / "current"
                if current.exists() and not current.is_symlink():
                    raise RuntimeError("Current download must be a symlink; preserving existing path")
                link.symlink_to(Path("download-runs") / batch_id, target_is_directory=True)
                link.replace(current)
            finally:
                link.unlink(missing_ok=True)

    print(f"\nDone! Data saved to: {destination.absolute()}")


def cmd_stats(args):
    """Show statistics for downloaded datasets."""
    try:
        import polars as pl
    except ImportError:
        print("Please install polars: pip install polars")
        sys.exit(1)

    output_dir = get_output_dir(args)

    print("=" * 60)
    print("Dataset Statistics")
    print("=" * 60)

    for data_type, (parquet_file, status) in _local_files(output_dir).items():
        df = pl.read_parquet(parquet_file)
        size_mb = parquet_file.stat().st_size / (1024 * 1024)

        print(f"\n{data_type.upper()} ({size_mb:.1f} MB; {status})")
        print("-" * 40)
        print(f"  Rows: {len(df):,}")
        print(f"  Columns: {', '.join(df.columns)}")

        # Agency breakdown
        if "agency_code" in df.columns:
            agency_counts = df.group_by("agency_code").len().sort("len", descending=True)
            top_agencies = agency_counts.head(5)
            print("  Top agencies:")
            for row in top_agencies.iter_rows():
                print(f"    {row[0]}: {row[1]:,}")


def cmd_sample(args):
    """Show sample rows from a dataset."""
    try:
        import polars as pl
    except ImportError:
        print("Please install polars: pip install polars")
        sys.exit(1)

    output_dir = get_output_dir(args)
    _table_name(args.data_type)
    parquet_file, status = _local_files(output_dir).get(
        args.data_type, (output_dir / f"{args.data_type}.parquet", "legacy-unversioned")
    )

    if not parquet_file.exists():
        print(f"File not found: {parquet_file}")
        print("Run: spicy-regs download")
        sys.exit(1)

    df = pl.read_parquet(parquet_file)

    if args.agency:
        df = df.filter(pl.col("agency_code") == args.agency)

    sample = df.sample(min(args.n, len(df)))

    print(f"\nSample from {args.data_type} ({len(df):,} total rows; {status}):")
    print("=" * 80)
    print(sample)


def cmd_search(args):
    """Search across datasets."""
    try:
        import polars as pl
    except ImportError:
        print("Please install polars: pip install polars")
        sys.exit(1)

    output_dir = get_output_dir(args)
    query = args.query.lower()

    print(f"Searching for: '{args.query}'")
    print("=" * 60)

    search_configs = {
        "dockets": ["title", "abstract"],
        "documents": ["title", "text_content"],
        "comments": ["title", "comment", "text_content"],
    }

    for data_type, (parquet_file, status) in _local_files(output_dir).items():
        df = pl.read_parquet(parquet_file)
        columns = search_configs.get(data_type, [name for name, dtype in df.schema.items() if dtype == pl.String])

        # Build filter for any column containing the query
        filters = None
        for col in columns:
            if col in df.columns:
                col_filter = pl.col(col).str.to_lowercase().str.contains(query, literal=True)
                filters = col_filter if filters is None else (filters | col_filter)

        if filters is not None:
            matches = df.filter(filters)
            if len(matches) > 0:
                print(f"\n{data_type.upper()}: {len(matches):,} matches ({status})")
                print("-" * 40)
                sample = matches.head(args.limit)
                for row in sample.iter_rows(named=True):
                    id_col = list(row.keys())[0]
                    title = row.get("title", "")[:80] if row.get("title") else "(no title)"
                    print(f"  {row[id_col]}: {title}")


def cmd_agencies(args):
    """List all agencies in the dataset."""
    try:
        import polars as pl
    except ImportError:
        print("Please install polars: pip install polars")
        sys.exit(1)

    output_dir = get_output_dir(args)
    files = _local_files(output_dir)

    # Try to get agency list from any available file
    for data_type in ["dockets", "documents", "comments"]:
        if data_type in files:
            parquet_file, status = files[data_type]
            df = pl.read_parquet(parquet_file, columns=["agency_code"])
            agencies = df["agency_code"].unique().sort().to_list()

            print(f"Agencies ({len(agencies)} total; {status}):")
            print("=" * 40)
            for agency in agencies:
                if agency:
                    print(f"  {agency}")
            return

    print("No data downloaded yet. Run: spicy-regs download")


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(
        prog="spicy-regs",
        description="Download and explore federal regulations data from Spicy Regs",
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        help=f"Output directory for data files (default: {DEFAULT_OUTPUT_DIR})",
        default=None,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Download command
    download_parser = subparsers.add_parser("download", help="Download parquet files")
    download_parser.add_argument("--force", "-f", action="store_true", help="Force re-download")
    download_parser.add_argument(
        "--types", nargs="+", type=_table_name, help="Specific table names to download, including rollups"
    )
    download_parser.set_defaults(func=cmd_download)

    # Stats command
    stats_parser = subparsers.add_parser("stats", help="Show dataset statistics")
    stats_parser.set_defaults(func=cmd_stats)

    # Sample command
    sample_parser = subparsers.add_parser("sample", help="Show sample rows")
    sample_parser.add_argument("data_type", type=_table_name)
    sample_parser.add_argument("-n", type=int, default=5, help="Number of rows")
    sample_parser.add_argument("--agency", help="Filter by agency code")
    sample_parser.set_defaults(func=cmd_sample)

    # Search command
    search_parser = subparsers.add_parser("search", help="Search across datasets")
    search_parser.add_argument("query", help="Search query")
    search_parser.add_argument("--limit", "-l", type=int, default=10, help="Max results per type")
    search_parser.set_defaults(func=cmd_search)

    # Agencies command
    agencies_parser = subparsers.add_parser("agencies", help="List all agencies")
    agencies_parser.set_defaults(func=cmd_agencies)

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    args.func(args)


if __name__ == "__main__":
    main()
