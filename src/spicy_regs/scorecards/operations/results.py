"""Lossless decoding and retention of ordinary hosted readback results."""

import json


def write(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


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
