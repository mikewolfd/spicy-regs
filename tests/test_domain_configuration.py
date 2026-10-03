"""Public addresses must agree across readers, rendered pages and deployments."""

import ast
import json
import os
from pathlib import Path
import re
import runpy
import subprocess
import sys
from types import SimpleNamespace

import pytest

from scripts import docs_domain, render_cloudflare_config
from spicy_regs import data_dictionary, mcp_server
from spicy_regs.public_url import resolve_domain, resolve_r2_base_url, service_url
from spicy_regs.transforms.build_org_committee_links import _resolve_comments_source

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("domain", [None, "", "spicygov.ai", "  SpicyGov.AI  "])
def test_domain_reaches_readers_pages_and_deployment(monkeypatch, tmp_path, domain):
    if domain is not None:
        monkeypatch.setenv("SPICYREGS_DOMAIN", domain)
    expected = "spicygov.ai" if domain else "spicy-regs.dev"
    assert resolve_domain() == expected
    assert resolve_r2_base_url() == f"https://data.{expected}"
    assert _resolve_comments_source(tmp_path) == f"https://data.{expected}/comments.parquet"

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

    def published_urls(base):
        assert base == "https://data.spicygov.ai"
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
