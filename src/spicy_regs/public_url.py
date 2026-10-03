"""Resolve public service addresses without importing pipeline dependencies."""

import os
import re
from urllib.parse import urlparse

DEFAULT_DOMAIN = "spicy-regs.dev"
DEFAULT_R2_BASE_URL = f"https://data.{DEFAULT_DOMAIN}"


def resolve_domain() -> str:
    """Return a bare DNS domain, safe to use in URLs and deployment settings."""
    domain = (os.environ.get("SPICYREGS_DOMAIN") or DEFAULT_DOMAIN).strip().lower()
    label = r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    if len(domain) > 248 or not re.fullmatch(rf"{label}(?:\.{label})+", domain):
        raise RuntimeError("SPICYREGS_DOMAIN must be a bare domain, such as spicy-regs.dev (no scheme, path or port)")
    return domain


def service_url(service: str) -> str:
    """Derive a public service URL from the selected project domain."""
    if service not in {"data", "docs", "mcp", "app"}:
        raise ValueError(f"Unknown public service: {service}")
    return f"https://{service}.{resolve_domain()}"


def resolve_r2_base_url(value: str | None = None) -> str:
    """Prefer an explicit URL, reader override, publisher URL, then the domain."""
    raw = (
        value or os.environ.get("SPICY_REGS_R2_URL") or os.environ.get("R2_PUBLIC_URL") or service_url("data")
    ).rstrip("/")
    parsed = urlparse(raw)
    if parsed.scheme != "https" or not parsed.netloc:
        raise RuntimeError("Public data URL must be an https:// URL")
    if any(c in raw for c in ("'", "\\", "\x00", "\n", "\r")):
        raise RuntimeError("Public data URL contains illegal characters")
    return raw


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Resolve public deployment addresses without ETL dependencies.")
    parser.add_argument("service", choices=("domain", "data", "docs", "mcp", "app"))
    service = parser.parse_args().service
    if service == "domain":
        print(resolve_domain())
    elif service == "data":
        print(resolve_r2_base_url())
    else:
        print(service_url(service))
