"""Successful-read state stored with its output, including reads yielding no rows.

Keeping checkpoints in Parquet metadata makes the published rows and the
decision to skip their source part of one artifact. A missing or malformed
checkpoint never establishes that a source has been processed.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from loguru import logger


def _key(namespace: str) -> str:
    return f"spicy_regs.read_checkpoints.{namespace}.v1"


def _nonfinite(value: str) -> None:
    raise ValueError(f"Non-finite JSON number {value}")


def read_checkpoints(path: Path | None, namespace: str) -> list[dict[str, Any]]:
    """Read a family's records; its caller checks the source and processing versions."""
    if path is None:
        return []
    encoded = (pq.read_metadata(path).metadata or {}).get(_key(namespace).encode())
    if encoded is None:
        return []
    try:
        records = json.loads(encoded, parse_constant=_nonfinite)
    except (ValueError, UnicodeDecodeError):
        logger.warning("{}: invalid {} read checkpoints; sources will be re-evaluated", path.name, namespace)
        return []
    if not isinstance(records, list) or any(not isinstance(record, dict) for record in records):
        logger.warning("{}: invalid {} read checkpoints; sources will be re-evaluated", path.name, namespace)
        return []
    return records


def checkpoint_metadata(
    path: Path | None, namespace: str, checkpoints: Iterable[Mapping[str, object]]
) -> dict[str, str]:
    """Encode checkpoints for the merge writer, preserving unrelated metadata.

    ``ARROW:schema`` describes the previous file's schema and must be generated
    by its writer, rather than copied over a potentially changed schema. An
    unrelated non-UTF-8 metadata value raises instead of silently dropping it.
    """
    metadata = {} if path is None else pq.read_metadata(path).metadata or {}
    result = {
        key.decode(): value.decode()
        for key, value in metadata.items()
        if key != b"ARROW:schema" and key != _key(namespace).encode()
    }
    result[_key(namespace)] = json.dumps(list(checkpoints), sort_keys=True, separators=(",", ":"), allow_nan=False)
    return result
