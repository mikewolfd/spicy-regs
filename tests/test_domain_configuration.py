"""Public addresses must agree across readers, rendered pages and deployments."""

import ast
import json
import os
from pathlib import Path
import re
import runpy
import shutil
import subprocess
import sys
from types import SimpleNamespace

import pytest

from scripts import docs_domain, render_cloudflare_config
from spicy_regs import data_dictionary, mcp_server
from spicy_regs.public_url import comments_source, resolve_domain, resolve_r2_base_url, service_url

ROOT = Path(__file__).resolve().parents[1]
IMAGE_DIGEST = "sha256:" + "a" * 64
RELEASE_SHA256 = "sha256:" + "b" * 64


@pytest.fixture
def production_repo(tmp_path, monkeypatch):
    for relative in (
        "scripts/render_cloudflare_config.py",
        "src/spicy_regs/public_url.py",
        "deploy/cloudflare/wrangler.template.jsonc",
        "deploy/cloudflare/package.json",
    ):
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, target)
    monkeypatch.setattr(render_cloudflare_config, "ROOT", tmp_path)
    monkeypatch.delenv("MCP_GUIDE_URL", raising=False)
    return tmp_path


def test_production_uses_explicit_release_and_fixed_public_settings(production_repo):
    config = json.loads(
        render_cloudflare_config.render_config(
            production=True,
            image_digest=IMAGE_DIGEST,
            release_sha256=RELEASE_SHA256,
        )
    )
    assert config["account_id"] == "174055408ff1560e60601c4d12c561c4"
    assert config["routes"] == [{"pattern": "mcp.spicygov.ai", "custom_domain": True}]
    assert config["vars"]["SPICYREGS_DOMAIN"] == "spicygov.ai"
    assert config["vars"]["SPICY_REGS_R2_URL"] == "https://data.spicygov.ai"
    assert config["vars"]["MCP_GUIDE_URL"] == "https://spicygov.ai/mcp/"
    assert config["vars"]["SPICY_REGS_CONSUMER_IMAGE_DIGEST"] == IMAGE_DIGEST
    assert config["vars"]["SPICY_REGS_FEC_RELEASE_SHA256"] == RELEASE_SHA256
    assert config["containers"][0]["image"] == (
        "registry.cloudflare.com/174055408ff1560e60601c4d12c561c4/spicy-regs-reader@" + IMAGE_DIGEST
    )


@pytest.mark.parametrize(
    "name",
    [
        "SPICYREGS_DOMAIN",
        "R2_PUBLIC_URL",
        "SPICY_REGS_R2_URL",
        "MCP_GUIDE_URL",
        "CLOUDFLARE_ACCOUNT_ID",
    ],
)
def test_production_refuses_conflicting_environment(production_repo, monkeypatch, name):
    monkeypatch.setenv(name, "other-target")
    with pytest.raises(ValueError, match="conflicts with the production target"):
        render_cloudflare_config.render_config(
            production=True,
            image_digest=IMAGE_DIGEST,
            release_sha256=RELEASE_SHA256,
        )


@pytest.mark.parametrize(
    ("image", "receipt"),
    [
        (None, RELEASE_SHA256),
        (IMAGE_DIGEST, None),
        ("latest", RELEASE_SHA256),
        (IMAGE_DIGEST, "sha256:123"),
    ],
)
def test_production_requires_both_exact_pins(production_repo, image, receipt):
    with pytest.raises(ValueError, match="production requires"):
        render_cloudflare_config.render_config(production=True, image_digest=image, release_sha256=receipt)


@pytest.mark.parametrize(
    ("before", "after"),
    [
        ("174055408ff1560e60601c4d12c561c4", "another-account"),
        ("mcp.{{ SPICYREGS_DOMAIN }}", "wrong.example"),
        ("spicy-regs-reader@", "another-reader@"),
    ],
)
def test_production_refuses_template_target_drift(production_repo, before, after):
    template = production_repo / "deploy/cloudflare/wrangler.template.jsonc"
    template.write_text(template.read_text().replace(before, after))
    with pytest.raises(ValueError, match="configuration conflicts"):
        render_cloudflare_config.render_config(
            production=True,
            image_digest=IMAGE_DIGEST,
            release_sha256=RELEASE_SHA256,
        )


@pytest.mark.parametrize("conflicting", [False, True])
def test_production_npm_entry_deploys_checked_config_without_generic_rerender(production_repo, conflicting):
    npm = shutil.which("npm")
    if npm is None:
        pytest.skip("npm is required for the local command wiring check")
    commands = production_repo / "bin"
    commands.mkdir()
    wrangler = commands / "wrangler"
    wrangler.write_text(
        f"#!{sys.executable}\n"
        + """import json, os, pathlib, sys
pathlib.Path(os.environ["CALL_LOG"]).write_text(json.dumps({
    "argv": sys.argv[1:], "config": json.loads(pathlib.Path("wrangler.jsonc").read_text()),
}))
"""
    )
    wrangler.chmod(0o755)
    git = commands / "git"
    git.write_text('#!/bin/sh\nif [ "$1" = rev-parse ]; then echo abc1234; fi\n')
    git.chmod(0o755)
    log = production_repo / "upload.json"
    target = production_repo / "deploy/cloudflare/wrangler.jsonc"
    target.write_text("prior configuration\n")
    env = dict(os.environ, CALL_LOG=str(log), PATH=str(commands) + os.pathsep + os.environ["PATH"])
    if conflicting:
        env["SPICYREGS_DOMAIN"] = "wrong.example"
    result = subprocess.run(
        [
            npm,
            "run",
            "deploy:production",
            "--",
            "--image-digest",
            IMAGE_DIGEST,
            "--release-sha256",
            RELEASE_SHA256,
            "--profile",
            "test-production",
        ],
        cwd=target.parent,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if conflicting:
        assert result.returncode != 0
        assert "conflicts with the production target" in result.stderr
        assert not log.exists()
        assert target.read_text() == "prior configuration\n"
    else:
        assert result.returncode == 0, result.stderr
        call = json.loads(log.read_text())
        assert call["argv"] == [
            "deploy",
            "--tag",
            "abc1234",
            "--message",
            "spicy-regs abc1234",
            "--profile",
            "test-production",
        ]
        assert call["config"]["routes"] == [{"pattern": "mcp.spicygov.ai", "custom_domain": True}]
        assert call["config"]["vars"]["SPICY_REGS_R2_URL"] == "https://data.spicygov.ai"
        assert call["config"]["vars"]["MCP_GUIDE_URL"] == "https://spicygov.ai/mcp/"
        assert call["config"]["containers"][0]["image"].endswith("@" + IMAGE_DIGEST)
        assert call["config"]["vars"]["SPICY_REGS_CONSUMER_IMAGE_DIGEST"] == IMAGE_DIGEST
        assert call["config"]["vars"]["SPICY_REGS_FEC_RELEASE_SHA256"] == RELEASE_SHA256


@pytest.mark.parametrize("domain", [None, "", "spicygov.ai", "  SpicyGov.AI  "])
def test_domain_reaches_readers_pages_and_deployment(monkeypatch, tmp_path, domain):
    if domain is not None:
        monkeypatch.setenv("SPICYREGS_DOMAIN", domain)
    expected = "spicygov.ai" if domain else "spicy-regs.dev"
    assert resolve_domain() == expected
    assert resolve_r2_base_url() == f"https://data.{expected}"
    assert comments_source(tmp_path) == f"https://data.{expected}/comments.parquet"

    mcp_server._landing_page.cache_clear()
    try:
        html = mcp_server._landing_page().decode()
        assert html.count(f"https://mcp.{expected}/mcp") == 7
        assert f'href="https://docs.{expected}/"' in html
        assert "{{" not in html
    finally:
        mcp_server._landing_page.cache_clear()

    config = SimpleNamespace(site_url="", site_dir=str(tmp_path))
    docs_domain.on_config(config)
    docs_domain.on_post_build(config)
    assert config.site_url == f"https://docs.{expected}/"
    assert (tmp_path / "CNAME").read_text() == f"docs.{expected}\n"
    rendered = docs_domain.on_page_markdown(
        "{{ R2_PUBLIC_URL }}/x https://mcp.{{ SPICYREGS_DOMAIN }}/mcp "
        "https://data.spicy-regs.dev/historical https://www.regulations.gov"
    )
    assert rendered == (
        f"https://data.{expected}/x https://mcp.{expected}/mcp "
        "https://data.spicy-regs.dev/historical https://www.regulations.gov"
    )

    worker = json.loads(re.sub(r"^\s*//.*$", "", render_cloudflare_config.render_config(), flags=re.MULTILINE))
    assert worker["routes"] == [{"pattern": f"mcp.{expected}", "custom_domain": True}]
    assert worker["vars"]["SPICYREGS_DOMAIN"] == expected
    assert worker["vars"]["SPICY_REGS_R2_URL"] == f"https://data.{expected}"
    # The domain choice must not rewrite the independently pinned image.
    assert worker["containers"][0]["image"].startswith("registry.cloudflare.com/174055408ff1560e60601c4d12c561c4/")


@pytest.mark.parametrize("domain", ["https://spicygov.ai", "spicygov.ai/", "spicygov.ai:443", "x@y.ai", "x.ai\nX=1", "x.<b>"])
def test_invalid_domains_fail_before_rendering(monkeypatch, domain):
    monkeypatch.setenv("SPICYREGS_DOMAIN", domain)
    with pytest.raises(RuntimeError, match="bare domain"):
        render_cloudflare_config.render_config()


def test_explicit_data_urls_keep_precedence(monkeypatch):
    monkeypatch.setenv("SPICYREGS_DOMAIN", "spicygov.ai")
    monkeypatch.setenv("R2_PUBLIC_URL", "https://publisher.example/bucket/")
    assert resolve_r2_base_url() == "https://publisher.example/bucket"
    monkeypatch.setenv("SPICY_REGS_R2_URL", "https://reader.example/")
    assert resolve_r2_base_url() == "https://reader.example"
    assert resolve_r2_base_url("https://explicit.example/") == "https://explicit.example"
    assert service_url("docs") == "https://docs.spicygov.ai"


def test_dictionary_uses_domain_for_live_discovery(monkeypatch):
    from spicy_regs.sources import publication

    monkeypatch.setenv("SPICYREGS_DOMAIN", "spicygov.ai")

    class ReachedSelectedSource(Exception):
        pass

    def published_urls(base, *, index=None):
        assert base == "https://data.spicygov.ai"
        assert index is None
        raise ReachedSelectedSource

    monkeypatch.setattr(publication, "published_urls", published_urls)
    with pytest.raises(ReachedSelectedSource):
        data_dictionary.discover_schemas("r2")


@pytest.mark.parametrize("override", [False, True])
def test_standalone_helpers_and_notebook_code_use_selected_data(monkeypatch, override):
    monkeypatch.setenv("SPICYREGS_DOMAIN", "spicygov.ai")
    expected = "https://data.spicygov.ai"
    if override:
        monkeypatch.setenv("R2_PUBLIC_URL", "https://explicit.example/")
        expected = "https://explicit.example"
    for path in (ROOT / "plugins/spicyregs/skills/spicyregs/scripts").glob("*.py"):
        namespace = runpy.run_path(str(path))
        assert namespace["R2_BASE_URL"] == expected
    found = 0
    for path in (ROOT / "notebooks").glob("*.ipynb"):
        for cell in json.loads(path.read_text())["cells"]:
            if cell["cell_type"] != "code":
                continue
            source = "".join(cell["source"])
            if "SPICYREGS_DOMAIN" not in source:
                continue
            # Execute only the real URL assignment, without loading data or models.
            tree = ast.parse(source)
            assignment = next(
                n for n in tree.body if isinstance(n, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id in {"R2_BASE_URL", "R2_PUBLIC_URL"} for t in n.targets)
            )
            namespace = {"os": os}
            exec(compile(ast.Module(body=[assignment], type_ignores=[]), str(path), "exec"), namespace)
            assert namespace.get("R2_BASE_URL", namespace.get("R2_PUBLIC_URL")) == expected
            found += 1
    assert found > 0


@pytest.mark.parametrize("domain", ["spicy-regs.dev", "spicygov.ai"])
def test_cloudrun_passes_domain_and_checks_matching_host(tmp_path, domain):
    log = tmp_path / "calls.jsonl"
    stub = """#!{python}
import json, os, sys
with open(os.environ['CALL_LOG'], 'a') as f:
    f.write(json.dumps(sys.argv) + '\\n')
if '-w' in sys.argv:
    print('200')
else:
    print('fr_docket_links discovery_signals "deduped":true')
""".format(python=sys.executable)
    for name in ("gcloud", "docker", "curl"):
        path = tmp_path / name
        path.write_text(stub)
        path.chmod(0o755)
    env = dict(os.environ, SPICYREGS_DOMAIN=domain, PROJECT="test-project", CALL_LOG=str(log))
    env["PATH"] = str(tmp_path) + os.pathsep + os.environ["PATH"]
    result = subprocess.run(["bash", str(ROOT / "deploy/cloudrun/deploy.sh")], env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    deploy = next(args for args in calls if args[1:3] == ["run", "deploy"])
    variables = deploy[deploy.index("--update-env-vars") + 1]
    assert f"SPICYREGS_DOMAIN={domain}" in variables
    assert f"SPICY_REGS_R2_URL=https://data.{domain}" in variables
    curl_calls = [args for args in calls if Path(args[0]).name == "curl"]
    assert all(any(arg.startswith(f"https://mcp.{domain}/") for arg in args) for args in curl_calls)
