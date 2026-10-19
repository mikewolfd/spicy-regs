"""Render Wrangler's deployment config from the shared public-domain setting."""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import urlsplit

# Keep this usable by npm with only Python's standard library installed.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from spicy_regs.public_url import resolve_domain, resolve_r2_base_url  # noqa: E402

PRODUCTION_ACCOUNT = "174055408ff1560e60601c4d12c561c4"
PRODUCTION_VALUES = {
    "SPICYREGS_DOMAIN": "spicygov.ai",
    "R2_PUBLIC_URL": "https://data.spicygov.ai",
    "MCP_GUIDE_URL": "https://spicygov.ai/mcp/",
}


def render_config(
    *, production: bool = False, image_digest: str | None = None, release_sha256: str | None = None
) -> str:
    template = (ROOT / "deploy/cloudflare/wrangler.template.jsonc").read_text(encoding="utf-8")
    if production:
        for name, expected in {
            **PRODUCTION_VALUES,
            "SPICY_REGS_R2_URL": PRODUCTION_VALUES["R2_PUBLIC_URL"],
            "CLOUDFLARE_ACCOUNT_ID": PRODUCTION_ACCOUNT,
        }.items():
            if os.environ.get(name, "").strip() not in ("", expected):
                raise ValueError(f"{name} conflicts with the production target")
        for name, value in (("image-digest", image_digest), ("release-sha256", release_sha256)):
            if not value or not re.fullmatch(r"sha256:[0-9a-f]{64}", value):
                raise ValueError(f"production requires --{name} sha256:<64 lowercase hex digits>")
        values = PRODUCTION_VALUES
    else:
        if image_digest is not None or release_sha256 is not None:
            raise ValueError("release pins require --production")
        values = {
            "SPICYREGS_DOMAIN": resolve_domain(),
            "R2_PUBLIC_URL": resolve_r2_base_url(),
            "MCP_GUIDE_URL": os.environ.get("MCP_GUIDE_URL", "").strip(),
        }
    guide_url = values["MCP_GUIDE_URL"]
    if guide_url:
        parsed = urlsplit(guide_url)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("MCP_GUIDE_URL must be an absolute HTTPS URL without credentials")
    for token, value in values.items():
        template = template.replace("{{ " + token + " }}", json.dumps(value)[1:-1])
    if production:
        # The maintained template uses full-line comments. Refuse target drift
        # before writing the config or starting Wrangler; never inherit its old pins.
        config = json.loads(re.sub(r"^\s*//.*$", "", template, flags=re.MULTILINE))
        variables = config["vars"]
        containers = config["containers"]
        registry = f"registry.cloudflare.com/{PRODUCTION_ACCOUNT}/spicy-regs-reader@"
        if (
            config["account_id"] != PRODUCTION_ACCOUNT
            or config["name"] != "spicy-regs-mcp"
            or config["routes"] != [{"pattern": "mcp.spicygov.ai", "custom_domain": True}]
            or variables["SPICYREGS_DOMAIN"] != values["SPICYREGS_DOMAIN"]
            or variables["SPICY_REGS_R2_URL"] != values["R2_PUBLIC_URL"]
            or variables["MCP_GUIDE_URL"] != guide_url
            or len(containers) != 1
            or containers[0]["class_name"] != "McpContainer"
            or not containers[0]["image"].startswith(registry)
        ):
            raise ValueError("rendered configuration conflicts with the production target")
        containers[0]["image"] = registry + image_digest
        variables["SPICY_REGS_CONSUMER_IMAGE_DIGEST"] = image_digest
        variables["SPICY_REGS_FEC_RELEASE_SHA256"] = release_sha256
        return json.dumps(config, indent=2) + "\n"
    return template


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--production", action="store_true", help="select the fixed spicygov.ai production target")
    parser.add_argument("--image-digest", help="qualified registry image digest, sha256:<hex>")
    parser.add_argument("--release-sha256", help="published matching FEC receipt digest, sha256:<hex>")
    parser.add_argument("--deploy", action="store_true", help="deploy the checked production config")
    parser.add_argument("--profile", help="named Wrangler login for --deploy")
    args = parser.parse_args()
    if args.deploy and not args.production:
        parser.error("--deploy requires --production; use npm run deploy for a generic fork")
    if args.profile and not args.deploy:
        parser.error("--profile requires --deploy")
    try:
        rendered = render_config(
            production=args.production,
            image_digest=args.image_digest,
            release_sha256=args.release_sha256,
        )
    except (ValueError, KeyError, TypeError) as exc:
        parser.error(str(exc))
    target = ROOT / "deploy/cloudflare/wrangler.jsonc"
    target.write_text(rendered, encoding="utf-8")
    domain = PRODUCTION_VALUES["SPICYREGS_DOMAIN"] if args.production else resolve_domain()
    print(f"Configured MCP at https://mcp.{domain}", flush=True)
    if args.deploy:
        command = ["npm", "run", "deploy:configured", "--"]
        if args.profile:
            command.extend(["--profile", args.profile])
        subprocess.run(command, cwd=target.parent, check=True)


if __name__ == "__main__":
    main()
