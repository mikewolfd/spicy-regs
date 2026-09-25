#!/usr/bin/env python3
"""Build and publish the comments mirror, or skip its already verified snapshot.

The workflow caller holds the comments-catalog-write lock. --skip-upload always
builds and validates locally; --force rebuilds even when a receipt matches.
"""

import argparse
from pathlib import Path

from dotenv import load_dotenv

from spicy_regs.duckdb_settings import ExportResources
from spicy_regs.pipelines.comments_mirror import publish_comments_mirror


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    parser.add_argument("--memory-limit", default=ExportResources.memory, help="DuckDB budget shared by export and validation")
    parser.add_argument("--threads", type=int, default=ExportResources.threads, help="DuckDB threads throughout the build")
    parser.add_argument("--skip-upload", action="store_true", help="Build and verify locally")
    parser.add_argument("--force", action="store_true", help="Rebuild an unchanged snapshot")
    args = parser.parse_args()
    load_dotenv()
    publish_comments_mirror(args.output_dir, resources=ExportResources(args.memory_limit, args.threads),
                            skip_upload=args.skip_upload, force=args.force)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
