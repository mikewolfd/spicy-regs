"""Render Wrangler's deployment config from the shared public-domain setting."""

import json
import os
from pathlib import Path
import sys
from urllib.parse import urlsplit

# Keep this usable by npm with only Python's standard library installed.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from spicy_regs.public_url import resolve_domain, resolve_r2_base_url  # noqa: E402


def render_config() -> str:
    template = (ROOT / "deploy/cloudflare/wrangler.template.jsonc").read_text(encoding="utf-8")
    guide_url = os.environ.get("MCP_GUIDE_URL", "").strip()
    if guide_url:
        parsed = urlsplit(guide_url)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("MCP_GUIDE_URL must be an absolute HTTPS URL without credentials")
    for token, value in {
        "SPICYREGS_DOMAIN": resolve_domain(),
        "R2_PUBLIC_URL": resolve_r2_base_url(),
        "MCP_GUIDE_URL": guide_url,
    }.items():
        template = template.replace("{{ " + token + " }}", json.dumps(value)[1:-1])
    return template


if __name__ == "__main__":
    target = ROOT / "deploy/cloudflare/wrangler.jsonc"
    target.write_text(render_config(), encoding="utf-8")
    print(f"Configured MCP at https://mcp.{resolve_domain()}")
