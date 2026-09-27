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
import json
import re
import sys
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from uuid import uuid4

import httpx
from dotenv import load_dotenv

from spicy_regs.public_url import DEFAULT_R2_BASE_URL, resolve_r2_base_url

PUBLIC_URL = DEFAULT_R2_BASE_URL
DATA_TYPES = ["dockets", "documents", "comments", "manifest"]
DEFAULT_OUTPUT_DIR = Path("./spicy-regs-data")

try:
    _PACKAGE_VERSION = version("spicy-regs")
except PackageNotFoundError:
    _PACKAGE_VERSION = "0.0.0"

# Cloudflare rejects the default `Python-urllib/*` / blank User-Agent with a 403
# on this bucket; send an honest identifier instead (this is our own data, so
# there's no need to spoof a browser UA the way the PDF enrichment does for
# downloads.regulations.gov).
DEFAULT_HEADERS = {"User-Agent": f"spicy-regs/{_PACKAGE_VERSION}"}


def _table_name(value: str) -> str:
    """Argparse type for a table name: lowercase letters, digits, underscores or hyphens."""
    if not re.fullmatch(r"[a-z][a-z0-9_-]*", value):
        raise argparse.ArgumentTypeError(
            "Use a table name containing lowercase letters, numbers, underscores or hyphens"
        )
    return value


def _local_selection(output_dir: Path):
    """Capture current once, then resolve selected files before legacy files."""
    from spicy_regs.local_data import local_selection

    return local_selection(output_dir, include_legacy=True)


def _polars():
    """The polars module, or exit saying how to install it."""
    try:
        import polars
    except ImportError:
        print("Please install polars: pip install polars")
        sys.exit(1)
    return polars


def _read(paths, **options):
    """One table from its local files: a split table's members as one frame, their ``col=value`` paths not columns."""
    return _polars().read_parquet(list(paths), hive_partitioning=False, **options)


def _megabytes(paths) -> float:
    return sum(path.stat().st_size for path in paths) / (1024 * 1024)


def get_output_dir(args) -> Path:
    """Get output directory from args or default."""
    output_dir = Path(args.output_dir) if hasattr(args, "output_dir") and args.output_dir else DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def download_file(name: str, output_dir: Path, force: bool = False, *, base_url: str | None = None) -> Path | None:
    """Download one table from R2: ``<name>.parquet``, or a split table's ``<name>/`` directory of members.

    Each file lands at its key within the generation (``Member.key``), through ``fetch_member``: a sibling temp
    file renamed only once complete and matching its pin, so a truncated file is never left at its path.
    """
    from spicy_regs.sources.publication import current_index, fetch_member, table_members

    _table_name(name)
    base_url = resolve_r2_base_url(base_url)
    members = table_members(current_index(base_url), f"{name}.parquet")
    label = members[0].key.split("/", 1)[0]
    local_path = output_dir / label
    files = [output_dir / member.key for member in members]

    # Existing loose files have no host provenance. Preserve the original
    # default-host cache behavior; a different host must supply its own bytes.
    if local_path.exists() and not force and members[0].sha256 is None and base_url == PUBLIC_URL:
        print(f"  ✓ {label} already exists ({_megabytes(files):.1f} MB)")
        return local_path

    print(f"  ⬇ Downloading {label}...")
    try:
        for member, path in zip(members, files, strict=True):
            path.parent.mkdir(parents=True, exist_ok=True)
            if not fetch_member(base_url, member, path, member.path, headers=DEFAULT_HEADERS, timeout=60.0):
                raise RuntimeError(f"{member.path} is not on {base_url} (HTTP 404)")
    except (httpx.HTTPError, OSError, RuntimeError) as e:
        print(f"  ✗ Failed to download {label}: {e}")
        return None
    print(f"  ✓ {label} ({_megabytes(files):.1f} MB)")
    return local_path


def cmd_download(args):
    """Download parquet files from R2."""
    output_dir = get_output_dir(args)
    print(f"Downloading to: {output_dir.absolute()}")

    types_to_download = list(dict.fromkeys(args.types or ["dockets", "documents", "comments"]))
    for name in types_to_download:
        _table_name(name)

    from spicy_regs.local_data import selection_record
    from spicy_regs.sources.publication import snapshot

    base_url = resolve_r2_base_url()
    with snapshot(base_url) as index:
        selected = {name: selection_record(index, name) for name in types_to_download}
        managed = any(item["status"] == "managed" for item in selected.values())
        current = output_dir / "current"
        # A prior current pointer wins over loose files when reading. Stage
        # host changes (including a return to legacy/default data) together so
        # it cannot continue shadowing a successful new download.
        staged = managed or base_url != PUBLIC_URL or current.is_symlink() or current.exists()
        if staged:
            batch_id = uuid4().hex
            destination = output_dir / "download-runs" / batch_id
            destination.mkdir(parents=True, exist_ok=False)
            metadata = {
                "version": 1,
                "status": "incomplete",
                "base_url": base_url,
                "publication": index,
                "selected": selected,
            }
            (destination / "download.json").write_text(json.dumps(metadata, indent=2) + "\n")
        else:
            destination = output_dir
        for data_type in types_to_download:
            print(f"  {data_type}: {selected[data_type]['status']}")
            if download_file(data_type, destination, force=args.force, base_url=base_url) is None:
                raise RuntimeError(f"Download incomplete: {data_type}")
        if staged:
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
    _polars()

    output_dir = get_output_dir(args)

    print("=" * 60)
    print("Dataset Statistics")
    print("=" * 60)

    selection = _local_selection(output_dir)
    for data_type, (_, status) in selection.files.items():
        paths = selection.paths(data_type)
        df, size_mb = _read(paths), _megabytes(paths)

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
    pl = _polars()

    output_dir = get_output_dir(args)
    _table_name(args.data_type)
    selection = _local_selection(output_dir)
    if args.data_type in selection.files:
        paths, status = selection.paths(args.data_type), selection.files[args.data_type][1]
    else:
        paths, status = (output_dir / f"{args.data_type}.parquet",), "legacy-unversioned"

    if not all(path.exists() for path in paths):
        print(f"File not found: {paths[0]}")
        print("Run: spicy-regs download")
        sys.exit(1)

    df = _read(paths)

    if args.agency:
        df = df.filter(pl.col("agency_code") == args.agency)

    sample = df.sample(min(args.n, len(df)))

    print(f"\nSample from {args.data_type} ({len(df):,} total rows; {status}):")
    print("=" * 80)
    print(sample)


def cmd_search(args):
    """Search across datasets."""
    pl = _polars()

    output_dir = get_output_dir(args)
    query = args.query.lower()

    print(f"Searching for: '{args.query}'")
    print("=" * 60)

    search_configs = {
        "dockets": ["title", "abstract"],
        "documents": ["title", "text_content"],
        "comments": ["title", "comment", "text_content"],
    }

    selection = _local_selection(output_dir)
    for data_type, (_, status) in selection.files.items():
        df = _read(selection.paths(data_type))
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
    _polars()

    output_dir = get_output_dir(args)
    selection = _local_selection(output_dir)

    # Try to get agency list from any available file
    for data_type in ["dockets", "documents", "comments"]:
        if data_type in selection.files:
            status = selection.files[data_type][1]
            df = _read(selection.paths(data_type), columns=["agency_code"])
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
