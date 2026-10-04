#!/usr/bin/env python3
"""Read-only duplicate audit of the selected native comments catalog.

Native ingestion admits one subject and receipt per identity. This command never
rebuilds or migrates stored rows; a failed audit requires a qualified rebuild from
retained source evidence, through the ordinary native writer.
"""
from __future__ import annotations

import argparse
from loguru import logger
from spicy_regs.schemas.regulations import RECORD_TYPES
from spicy_regs.sources import iceberg


def main() -> int:
    argparse.ArgumentParser(description=__doc__).parse_args()
    if not iceberg.is_configured():
        logger.error('R2 Data Catalog is not configured; cannot audit')
        return 1
    con = iceberg._connect()
    try:
        duplicates = iceberg.audit_duplicates(con, RECORD_TYPES['comments'])
        for agency, rows, distinct in duplicates:
            logger.error('{}: {} rows for {} comment identities', agency, rows, distinct)
        if duplicates:
            return 1
        logger.info('Selected native comments contain no repeated agency/comment identities')
        return 0
    finally:
        con.close()


if __name__ == '__main__':
    raise SystemExit(main())
