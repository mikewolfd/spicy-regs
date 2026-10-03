"""Set the published documentation domain without rewriting source evidence."""

from pathlib import Path

from spicy_regs.public_url import resolve_domain, resolve_r2_base_url, service_url


def on_config(config):
    config.site_url = service_url("docs") + "/"
    return config


def on_page_markdown(markdown, **_kwargs):
    # Only explicit placeholders are substituted. Historical URLs stay literal.
    return (
        markdown.replace("{{ SPICYREGS_DOMAIN }}", resolve_domain())
        .replace("{{ R2_PUBLIC_URL }}", resolve_r2_base_url())
    )


def on_post_build(config):
    (Path(config.site_dir) / "CNAME").write_text(f"docs.{resolve_domain()}\n", encoding="utf-8")
