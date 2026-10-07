#!/usr/bin/env python3
"""Retain and verify the mutable base versions used by a regulatory refresh.

Managed rollup generations retain their publication-index snapshot separately.
This receipt covers the bare base objects and the managed dockets/documents
families that dependents read in their place. The caller holds the catalog
writer lock from capture through verification.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path

import httpx

from spicy_regs.public_url import resolve_r2_base_url
from spicy_regs.sources import publication

BASE_KEYS = ('dockets.parquet', 'documents.parquet', 'comments.parquet', 'comments_index.parquet', 'manifest.parquet')
BASE_FAMILIES = ('dockets', 'documents')
REUSED_FAMILIES = (*BASE_FAMILIES, 'docket-attributes', 'document-attributes', 'comment-attributes', 'comments')


def observe(base_url: str, *, reuse_published_base: bool = False, base_only: bool = False) -> dict[str, dict]:
    pins = {}
    with httpx.Client(follow_redirects=True, timeout=60) as client:
        for key in BASE_KEYS:
            if base_only and key in {'comments.parquet', 'comments_index.parquet'}:
                continue
            response = client.head(base_url.rstrip('/') + '/' + key)
            response.raise_for_status()
            etag = response.headers.get('etag')
            if not etag:
                raise RuntimeError(f'{key}: no ETag; cannot establish its version')
            pins[key] = {'etag': etag, 'bytes': int(response.headers['content-length'])}
    families = publication.load_index(base_url)['families']
    for family in REUSED_FAMILIES if reuse_published_base else BASE_FAMILIES:
        if base_only and family == 'comments':
            continue
        if reuse_published_base and family not in families:
            raise RuntimeError(f'{family}: selected managed family is required for a comments-only refresh')
        if family in families:
            pins[f'family:{family}'] = {'artifactDigest': families[family]['artifactDigest']}
    return pins


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=('capture', 'verify'))
    parser.add_argument('--receipt', type=Path, default=Path('output/refresh-inputs.json'))
    parser.add_argument('--reuse-published-base', action='store_true')
    parser.add_argument('--base-only', action='store_true')
    parser.add_argument('--base-receipt', type=Path)
    args = parser.parse_args()
    base_url = resolve_r2_base_url()
    if args.operation == 'capture':
        if args.base_only and not args.reuse_published_base:
            raise ValueError('Base-only capture requires explicit published-base reuse')
        pins = observe(base_url, reuse_published_base=args.reuse_published_base, base_only=args.base_only)
        if args.base_receipt is not None:
            original = json.loads(args.base_receipt.read_text())
            base_pins = {key: value for key, value in pins.items() if key not in {
                'comments.parquet', 'comments_index.parquet', 'family:comments'
            }}
            if (not args.reuse_published_base or args.base_only
                    or original.get('reuse_published_base') is not True
                    or original.get('base_only') is not True
                    or original.get('base_url') != base_url
                    or original.get('objects') != base_pins):
                raise RuntimeError('Published base versions changed before combined refresh capture')
        receipt = {'base_url': base_url, 'observed_at': datetime.now(UTC).isoformat(), 'objects': pins}
        if args.reuse_published_base:
            receipt['reuse_published_base'] = True
        if args.base_only:
            receipt['base_only'] = True
        if args.base_receipt is not None:
            receipt['original_base_objects'] = original['objects']
        args.receipt.parent.mkdir(parents=True, exist_ok=True)
        args.receipt.write_text(json.dumps(receipt, indent=2) + '\n')
    else:
        receipt = json.loads(args.receipt.read_text())
        pins = observe(base_url, reuse_published_base=receipt.get('reuse_published_base', False),
                       base_only=receipt.get('base_only', False))
        if receipt['base_url'] != base_url or receipt['objects'] != pins:
            raise RuntimeError('Regulatory base versions changed during refresh; results require reconciliation')
    print(f'{args.operation}: regulatory base versions verified; receipt={args.receipt}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
