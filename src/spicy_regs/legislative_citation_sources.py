"""Exact field lookups for receipt-migrated legislative subjects."""

import json

from spicy_regs.citation_sources import TEXT_SOURCES


NATIVE_TEXT_SOURCES = {
    "bill_section": TEXT_SOURCES["bill_section"],
    "report_section": TEXT_SOURCES["report_section"],
}


def source_digests(cursor, kind: str, key: str) -> list[tuple[str | None]]:
    """Keep duplicate bodies ambiguous and check exact complete subject identity."""
    spec = NATIVE_TEXT_SOURCES[kind]
    values = json.loads(key)
    if (not isinstance(values, list) or len(values) != len(spec.keys)
            or any(not isinstance(value, str) or not value for value in values)):
        raise ValueError("Native legislative field lookup needs the exact complete key")
    predicate = " AND ".join(f'CAST("{column}" AS VARCHAR) = ?' for column in spec.keys)
    return cursor.execute(
        f'SELECT \'sha256:\' || sha256("{spec.field}") FROM "{spec.table}" WHERE {predicate} LIMIT 2', values,
    ).fetchall()
