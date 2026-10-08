"""Lossless decoding and retention of ordinary hosted readback results."""

import json
import os
from pathlib import Path
from uuid import uuid4

from spicy_docs.storage.publication import write_bytes_once


def write(path, value):
    """Keep the previous complete JSON until its replacement is durably written."""
    path = Path(path)
    raw = (json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        write_bytes_once(temporary, raw)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def query_rows(body):
    """Read either documented row representation without changing retained replies."""
    columns = body["columns"]
    if any(not isinstance(name, str) for name in columns) or len(set(columns)) != len(columns):
        raise ValueError("Hosted query columns are ambiguous")
    rows = []
    for row in body["rows"]:
        if isinstance(row, dict) and set(row) == set(columns):
            rows.append(row)
        elif isinstance(row, list) and len(row) == len(columns):
            rows.append(dict(zip(columns, row, strict=True)))
        else:
            raise ValueError("Hosted query row does not match its columns")
    return rows
