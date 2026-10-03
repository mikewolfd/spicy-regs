"""Select the public data host without importing pipeline dependencies."""

import os
from pathlib import Path
from urllib.parse import urlparse

DEFAULT_R2_BASE_URL = "https://data.spicy-regs.dev"


def resolve_r2_base_url(value: str | None = None) -> str:
    """Prefer an explicit URL, reader override, publisher URL, then the default."""
    raw = (
        value or os.environ.get("SPICY_REGS_R2_URL") or os.environ.get("R2_PUBLIC_URL") or DEFAULT_R2_BASE_URL
    ).rstrip("/")
    parsed = urlparse(raw)
    if parsed.scheme != "https" or not parsed.netloc:
        raise RuntimeError("Public data URL must be an https:// URL")
    if any(c in raw for c in ("'", "\\", "\x00", "\n", "\r")):
        raise RuntimeError("Public data URL contains illegal characters")
    return raw


def comments_source(output_dir: Path) -> str:
    """The ``read_parquet`` target for the comments export: a local ``comments.parquet``, else the public one.

    A rollup that reads a few columns of the multi-GB export declares it in ``remote_inputs`` and reads it here, so
    projection pushdown fetches only those columns.
    """
    local = output_dir / "comments.parquet"
    if local.exists():
        return str(local)
    return f"{resolve_r2_base_url(os.environ.get('R2_PUBLIC_URL'))}/comments.parquet"
