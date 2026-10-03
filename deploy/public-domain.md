# Public domain configuration

`SPICYREGS_DOMAIN` is the single base-domain setting. Use a bare DNS name,
without `https://`, a port or a path. Unset or empty keeps the upstream default,
`spicy-regs.dev`. Mike's fork uses `spicygov.ai`.

| Service | Derived address for Mike's fork |
| --- | --- |
| Public data | `https://data.spicygov.ai` |
| Documentation | `https://docs.spicygov.ai/` |
| MCP connector | `https://mcp.spicygov.ai/mcp` |

## GitHub Actions

Set **Settings → Secrets and variables → Actions → Variables → Repository
variables → New repository variable**, or run:

```sh
gh variable set SPICYREGS_DOMAIN --repo mikewolfd/spicy-regs --body spicygov.ai
```

Use a repository variable: the reusable ETL workflows and documentation build
do not select a GitHub environment. An environment-only variable would not
reach those jobs. No value is needed on upstream. See GitHub's
[variable availability and precedence](https://docs.github.com/en/actions/reference/workflows-and-actions/variables#configuration-variable-precedence).

The workflows derive `R2_PUBLIC_URL` from this variable. An existing
`R2_PUBLIC_URL` secret remains an explicit override for buckets served from a
different hostname. Keep that override consistent with the intended data
source; changing the domain does not move storage, change credentials or
provision DNS. The documentation workflow passes the domain into MkDocs, which
generates its canonical URLs, sitemap, current usage examples and `CNAME`.
After changing the variable, run **Deploy data dictionary** to rebuild it.

## Local readers and deployment

Export the variable before launching Python, a standalone plugin script or
Jupyter. The CLI also loads `.env`; `.env.example` shows how to derive the
publisher's `R2_PUBLIC_URL` with python-dotenv's variable expansion.

```sh
export SPICYREGS_DOMAIN=spicygov.ai
uv run spicy-regs --help
uv run mkdocs build --strict
cd deploy/cloudflare
npm run check
```

Data readers select an explicit URL argument first, then `SPICY_REGS_R2_URL`,
then `R2_PUBLIC_URL`, then `https://data.${SPICYREGS_DOMAIN}`. Existing upload
credentials and bucket selection remain separate. Local publishing still
requires `R2_PUBLIC_URL`; the example environment file derives it from the
domain. The MCP landing page derives its connector and documentation links
from the domain, including every copyable client configuration.

Cloudflare's `configure`, `types`, `check`, `dev` and `deploy` npm commands
render the ignored `wrangler.jsonc` from `wrangler.template.jsonc`. Edit image
pins, account settings and other bindings in the template. Rendering sets the
custom MCP domain and forwards the selected domain and data URL to the Python
container. The pinned container image must be rebuilt and admitted through the
existing release process to include Python/landing-page changes; changing the
Worker configuration alone does not rebuild it.

For Cloud Run, export the same variable before running `deploy/cloudrun/deploy.sh`.
The script supplies it to the container and derives the smoke-test hostname;
`SMOKE_BASE` still overrides the latter. Both deployment paths need their
existing account/project configuration.

## DNS, Pages and scope

Configure the `data`, `docs` and `mcp` DNS records and hosting custom domains in
their respective services. GitHub Pages also needs its repository custom-domain
setting updated to `docs.<domain>`; an artifact's `CNAME` alone does not change
that setting for Actions deployments. See GitHub's
[custom-domain setup](https://docs.github.com/en/pages/configuring-a-custom-domain-for-your-github-pages-site/managing-a-custom-domain-for-your-github-pages-site).
Terraform already takes an explicit
`custom_domain`; set it to `data.<domain>` for the chosen installation.

The separate `spicyregs-web` repository owns the landing site. Its `app` and
other deployment URLs are outside this repository. README/install links and
notebook narrative examples retain the documented upstream or fork addresses.
Changelogs, research evidence, publication receipts, captured measurements and
test fixtures retain their original URLs. Government sources, GitHub repository
links, registry image addresses and storage API endpoints are independent of
the public domain and remain unchanged.
