#!/usr/bin/env python3
"""Retain and verify the mutable base versions used by a regulatory refresh.

Managed rollup generations retain their publication-index snapshot separately.
This receipt covers the base objects outside that index. The caller holds the
catalog writer lock from capture through verification.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path

import httpx

from spicy_regs.public_url import resolve_r2_base_url

BASE_KEYS = ('dockets.parquet', 'documents.parquet', 'comments.parquet', 'comments_index.parquet', 'manifest.parquet')


def observe(base_url: str) -> dict[str, dict]:
    pins = {}
    with httpx.Client(follow_redirects=True, timeout=60) as client:
        for key in BASE_KEYS:
            response = client.head(base_url.rstrip('/') + '/' + key)
            response.raise_for_status()
            etag = response.headers.get('etag')
            if not etag:
                raise RuntimeError(f'{key}: no ETag; cannot establish its version')
            pins[key] = {'etag': etag, 'bytes': int(response.headers['content-length'])}
    return pins


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=('capture', 'verify'))
    parser.add_argument('--receipt', type=Path, default=Path('output/refresh-inputs.json'))
    args = parser.parse_args()
    base_url = resolve_r2_base_url()
    pins = observe(base_url)
    if args.operation == 'capture':
        receipt = {'base_url': base_url, 'observed_at': datetime.now(UTC).isoformat(), 'objects': pins}
        args.receipt.parent.mkdir(parents=True, exist_ok=True)
        args.receipt.write_text(json.dumps(receipt, indent=2) + '\n')
    else:
        receipt = json.loads(args.receipt.read_text())
        if receipt['base_url'] != base_url or receipt['objects'] != pins:
            raise RuntimeError('Regulatory base versions changed during refresh; results require reconciliation')
    print(f'{args.operation}: regulatory base versions verified; receipt={args.receipt}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
