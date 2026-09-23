"""Validated DuckDB settings shared by the MCP server and the merge transforms."""

import re

#: A size with a unit, as DuckDB 1.5 parses it (``4GB``, ``2048MB``, ``16GiB``, ``1.5 GB``).
#: DuckDB refuses a percentage at ``SET``, and reads a bare number as bytes.
_MEMORY_LIMIT = re.compile(r"\d+(\.\d+)?\s*[KMGT]i?B", re.IGNORECASE)


def memory_limit(raw: str, source: str) -> str:
    """``raw`` stripped, or a ``RuntimeError`` naming ``source``; the value is interpolated into a ``SET``."""
    value = raw.strip()
    if not _MEMORY_LIMIT.fullmatch(value):
        raise RuntimeError(f"{source} is not a valid size: {raw!r}")
    return value
