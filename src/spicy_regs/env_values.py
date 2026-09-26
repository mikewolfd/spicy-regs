"""Typed values of the optional environment overrides that rollups and transforms read."""

from __future__ import annotations

import os
from datetime import date


def date_env(name: str) -> date | None:
    """Parse a ``YYYY-MM-DD`` env var, or None when unset; a malformed value raises ValueError naming it."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be YYYY-MM-DD, got {raw!r}") from exc


def flag_env(name: str) -> bool:
    """``true`` or ``false`` (any case; blank is false), refusing anything else."""
    raw = os.environ.get(name, "").strip().lower()
    if raw not in ("", "true", "false"):
        raise ValueError(f"{name} must be true or false, got {raw!r}")
    return raw == "true"
