#!/usr/bin/env python3
"""Stamp publishedAt on each publication.v2.json family entry that lacks it; a dry run unless --apply.

Run it only once the MCP server image whose ``parse_index`` admits publishedAt is serving: the reader deployed
before it requires the exact family key set and would refuse the whole index. The rules live in
``spicy_regs.sources.publication.backfill_published_at``; this is only its command line. It needs the R2
credentials and ``R2_BUCKET_NAME``, and prints what it planned or wrote as JSON.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from os import getenv

from spicy_regs.sources import publication, r2


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("--apply", action="store_true", help="Write the planned instants; without it nothing is written")
    args = parser.parse_args(argv)
    bucket = getenv("R2_BUCKET_NAME")
    if not bucket:
        parser.error("R2_BUCKET_NAME is not set")
    print(json.dumps(publication.backfill_published_at(r2.get_r2_client(), bucket, apply=args.apply), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
