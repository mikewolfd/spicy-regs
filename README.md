<div align="center">

<img src="assets/icon.png" alt="Spicy Regs" width="96" height="96">

# Spicy Regs

**An open, queryable mirror of U.S. federal regulatory data — and the pipeline that builds it.**

<a href="https://www.civictechdc.org/">
  <img src="assets/civictechdc-logo.png" alt="Civic Tech DC" width="22" height="22" align="top">
</a>
&nbsp;A <a href="https://www.civictechdc.org/"><b>Civic Tech DC</b></a> project

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Slack](https://img.shields.io/badge/Slack-join%20us-4A154B?logo=slack&logoColor=white)](https://join.slack.com/t/civictechdc/shared_invite/zt-43eotbj04-QLQ_Ria296PtRYJU2EgwxQ)

[Explore the data](https://app.spicy-regs.dev) ·
[Data dictionary](https://docs.spicy-regs.dev/) ·
[MCP server](#use-it-from-an-ai-assistant) ·
[Changelog](CHANGELOG.md)

</div>

---

> **Superseded greenfield implementation:** This checkout is retained only as
> migration evidence until its accepted catalog semantics move to DocSpec.
> DocSpec owns `SourceCatalog`, capture, document processing, and
> `DocumentRelease`; the retained SpicyRegs product owns only source-specific
> acquisition and faithful source-native publication. Commands and ownership
> descriptions below document the predecessor and are not current platform
> authority.
>
> The public outcomes below are not discarded with this checkout. Anonymous
> Parquet/range SQL, downloads, schema discovery, browser search, MCP access,
> stable comment partitions, materialized generations, and declared freshness
> remain in the platform plan's predecessor value ledger until each has a tested
> owner and consumer cutover. The old implementation is superseded; its user
> value and external wire constraints are migration requirements.
>
> Current uncommitted catalog-universe and release-publisher changes in this
> checkout are also superseded implementation, not the replacement. Their useful
> behavior is retained only through the pinned DocSpec oracle before the code is
> removed; they must not be published as a new catalog authority.
>
> Only direct anonymous reads of retained public endpoints are executable
> examples in this archived README. Do not run `git clone`/`uv sync`, `uv run`,
> `uvx`, plugin or stdio installation, tests, builds, publishers, pipelines,
> generators, or validators from this revision. `pyproject.toml` and `uv.lock`
> require an editable `RefSpec` at `RefSpec`, but this worktree has only a broken
> untracked symlink and a clone or archive checkout has no usable dependency.
> Repository-local commands and links below are historical evidence unless they
> point to a named surviving owner. Public URLs remain listed as retained
> surfaces pending the platform roster's acceptance and cutover checks; this
> README does not prove their current health.

Every federal rule that gets proposed generates a public record: a docket, the
agency's documents, and the comments people file on it. That record is public
but awkward to work with — paginated APIs, rate limits, no bulk access, and no
way to join it to the rest of the federal picture.

The predecessor turned it into files users could query. Its scheduled pipeline read
[regulations.gov](https://www.regulations.gov) data (via the public
[Mirrulations](https://github.com/MoravianUniversity/mirrulations) mirror) plus a
dozen complementary federal sources, and publishes the result as Parquet and
Apache Iceberg on Cloudflare R2 — public, anonymous read, no API key.

The retained user outcome is anonymous query access to roughly 25 million public
comments from a laptop, browser, or assistant without a private database. The
platform roster must verify the current endpoints and counts.

This checkout historically contained the pipeline, rollups, and read-only MCP
server. [`CONTRIBUTING.md`](CONTRIBUTING.md) is repository evidence, not a
current build guide.

## Contents

- [Retained external surfaces](#retained-external-surfaces-pending-acceptance)
- [What's in the corpus](#whats-in-the-corpus)
- [Historical install and reader commands](#historical-install-and-reader-command-shapes--do-not-execute-here)
- [Historical pipeline commands](#historical-pipeline-command-shapes--do-not-execute-here)
- [Use it from an AI assistant](#use-it-from-an-ai-assistant)
- [Historical project layout](#historical-project-layout)
- [License](#license)
- [Contact](#contact)
- [Acknowledgments](#acknowledgments)

## Retained external surfaces pending acceptance

| I want to… | Go here |
|---|---|
| Browse dockets, agencies, and comment activity | **[app.spicy-regs.dev](https://app.spicy-regs.dev)** |
| Read every column of every published table | **[docs.spicy-regs.dev](https://docs.spicy-regs.dev/)** |
| Ask an AI assistant questions about the data | **[MCP server](#use-it-from-an-ai-assistant)** (`https://mcp.spicy-regs.dev/mcp`) |
| Historical notebook entry points | **[Binder inventory link](https://mybinder.org/v2/gh/civictechdc/spicy-regs/HEAD)** or historical [`docs/querying-python.md`](docs/querying-python.md) |

One line of SQL against the public bucket — no credentials, no download:

```sql
-- DuckDB, anywhere: CLI, notebook, or the browser
SELECT agency_code, comment_count
FROM read_parquet('https://data.spicy-regs.dev/agency_stats.parquet')
ORDER BY comment_count DESC
LIMIT 10;
```

## What's in the corpus

Everything is published under `https://data.spicy-regs.dev` with public,
anonymous read. Per-column reference and exact row counts live in the
[data dictionary](https://docs.spicy-regs.dev/) — it's generated from the
schemas in this repo. It is intended to be checked against live Parquet by CI,
but that gate currently suppresses failures and a known
`fr_docket_links.docket_key` mismatch remains open. Treat the live files as the
current surface until the platform plan's generated-schema gate passes.

**Core regulations.gov tables**

| Table | What it is | Scale |
|---|---|---|
| `dockets` | Regulatory proceedings | ~276K rows |
| `documents` | Documents within dockets | ~2.0M rows |
| `comments` | Public comments (Hive-partitioned Parquet + an Iceberg table) | ~25.4M rows |

**Rollups** — small, denormalized, meant to be read whole: `feed_summary`,
`agency_stats`, `agency_monthly_volume`, `comments_index`, `docket_search`,
`rulemaking_lifecycles`, `discovery_signals`, `fr_docket_links`.

**Complementary federal sources** — each ingested from its own API so the
rulemaking lifecycle, the organizations engaged in it, and its downstream
context are all joinable in one place:

- *Lifecycle:* `federal_register`, `unified_agenda`, `congress_bills`, `cfr_sections`
- *Organizations & influence:* `sam_entities`, `lobbying_filings`, `fec_committees`
- *Outcomes & context:* `usaspending_recipients`, `court_dockets`, `gao_reports`, `crs_reports`
- *Telecom:* `fcc_proceedings`, `fcc_filings`

Cross-source join keys: **RIN**, **CFR citation**, **UEI**, **`agency_code`**.
Some sources are deliberately bounded or sampled (e.g. `lobbying_filings` is
2024+, `usaspending_recipients` is the top 100K by award dollars) — the data
dictionary documents the scope of each.

## Historical install and reader command shapes — do not execute here

The following predecessor commands are retained only as acceptance evidence for
the surviving SpicyRegs clean-install and thin-reader gate. This checkout cannot
install because its archived dependency configuration points at local RefSpec.

```text
git clone https://github.com/civictechdc/spicy-regs.git
cd spicy-regs
uv sync                       # install dependencies into .venv
uv run pytest                 # run the test suite
uv run ruff check .           # lint
```

The replacement gate must preserve credential-free public reads while removing
the local RefSpec dependency. The historical raw-data reader command shapes are:

```text
uv run spicy-regs download                        # dockets, documents, comments
uv run spicy-regs download --types comments       # comments only
uv run spicy-regs download -o ./my-data           # custom output dir
```

The predecessor then exposed these local tasks:

```text
uv run spicy-regs stats                # row counts + top agencies per file
uv run spicy-regs sample comments -n 5 # 5 random rows from comments
uv run spicy-regs search "climate"     # substring search across files
uv run spicy-regs agencies             # list every agency code
```

The surviving SpicyRegs package owns these thin raw-data tasks only after its
no-RefSpec clean-install and command-acceptance gates pass. The historical
[`docs/querying-python.md`](docs/querying-python.md) remains migration evidence;
the public DuckDB query above is the only directly executable example here.

## Historical pipeline command shapes — do not execute here

These commands record predecessor acquisition, publication, and document-work
surfaces. Source-native acquisition/publication and source-schema documentation
move to surviving SpicyRegs. Catalog, cross-source schema meaning, capture,
text, and `DocumentRelease` work move to DocSpec. Neither route is claimed
implemented by this archived README.

```text
# Smallest useful run: one agency, recent dockets, comments only, no upload.
uv run run-pipeline --agency EPA --only-comments --since-year 2025
```

What you get:

- `output/comments.parquet` — merged and deduplicated comments
- `output/manifest.parquet` — a Bloom filter of already-processed source keys,
  so the next run was incremental. The predecessor used deletion or
  `--full-refresh` to request a rebuild.

Recorded flags:

| Flag | What it does |
|---|---|
| `--agency EPA` | Process a single agency instead of all of them |
| `--since-year 2025` | Skip dockets older than the given year |
| `--only-comments` | Stage comments only (skip dockets + documents) |
| `--skip-comments` | Inverse — dockets + documents only (much faster) |
| `--max-workers 8` | Agencies processed in parallel (default 4) |
| `--full-refresh` | Ignore the existing manifest and rebuild from scratch |
| `--no-skip-upload` | Also publish to R2 (needs credentials in `.env`) |
| `--use-iceberg` | Route dockets + comments through the R2 Data Catalog |
| `--chunk-size 50000` | Bounded-memory comment ingest for very large agencies |
| `--no-enrich-text` | Skip filling comment `text_content` from Mirrulations' pre-extracted attachment text |

### Rollups

Rollups are decoupled from the main ETL — each is its own console script and its
own cron workflow, so one failing source can't block the rest:

```text
uv run run-rollup-feed-summary        # derived from the core R2 tables
uv run run-rollup-federal-register    # ingests an external API → its own Parquet
```

Historical [`CONTRIBUTING.md`](CONTRIBUTING.md) records the full list and prior
extension points.

### Backfilling comment text

The ETL fills `text_content` inline only for comments it processes fresh — the
incremental manifest skips comments ingested before that feature landed, so they
stay `NULL`. Backfill from Mirrulations' pre-extracted text (read straight from
the bucket's `derived-data` prefix — no PDF download, no JSON re-ingest):

```text
uv run spicy-regs download --types comments     # grab the published parquet
uv run backfill-comment-text                     # fill text_content in place
uv run backfill-comment-text --limit 5000        # cap work for a trial run
uv run backfill-comment-text --upload            # republish to R2 (needs credentials)
```

The predecessor treated this as incremental and re-runnable. DocSpec now owns
document capture and text processing; `uv run enrich-pdf-text --target
documents` is a historical command, not a supported instruction here.

### Working on the data dictionary

```text
uv run spicy-regs-dict check        # verify descriptions match the schema
uv run spicy-regs-dict generate     # regenerate docs/tables/*.md
uv run --group docs mkdocs serve    # preview at 127.0.0.1:8000
```

`data_dictionary/descriptions.yaml` and the recorded commands are migration
evidence. The replacement owner must generate documentation from the same pinned
schemas and fail its publication gate on drift.

## Use it from an AI assistant

A read-only MCP server historically exposed SQL over the corpus with three
tools: `list_sources()`, `describe_table(table)`, and `query_sql(sql)`. The URL
is an unverified retained public surface. Plugin and local-process instructions
below are non-executable history until the surviving SpicyRegs thin reader passes
the central roster and plugin-acceptance gates.

| Client | Setup |
|---|---|
| **Remote MCP inventory** | Retained endpoint: `https://mcp.spicy-regs.dev/mcp`; historical [`mcp-server/README.md`](mcp-server/README.md) |
| **Claude Code plugin history** | `/plugin marketplace add civictechdc/spicy-regs` then `/plugin install spicyregs@spicy-regs-local` — do not execute from this revision |
| **Local stdio history** | `claude mcp add spicy-regs -- uvx --from "spicy-regs @ git+https://github.com/civictechdc/spicy-regs" spicy-regs-mcp` — do not execute from this revision |
| **Other client history** | Historical [`plugins/spicyregs/INSTALL.md`](plugins/spicyregs/INSTALL.md); eventual thin raw-data ownership remains with surviving SpicyRegs |

## Historical project layout

```
src/spicy_regs/
├── pipelines/        # Pipeline contract, the main ETL, and one module per rollup
│   └── rollups/      # Derived rollups + external-source ingests
├── sources/          # Readers/writers: Mirrulations S3, R2, Iceberg, external APIs
├── transforms/       # Merge, partition, enrich, and the rollup builders
├── manifest.py       # Incremental state (Bloom filter over processed source keys)
├── mcp_server.py     # Read-only SQL MCP server
├── data_dictionary.py# Generates the docs site from the schemas
└── cli.py            # Local data CLI (download / stats / sample / search)

mcp-server/           # Vercel deployment of the MCP server
scripts/              # Operational tooling (dedupe, seed, freshness checks)
notebooks/            # Historical example analyses and Binder inventory
tests/                # Unit suite; integration tests are opt-in
.github/workflows/    # ETL cron, per-rollup crons, CI, deploys
```

The predecessor ETL was assembled from `Reader → Transform → Writer`, wired by
a `Pipeline`. The historical
[architecture section of CONTRIBUTING.md](CONTRIBUTING.md#architecture-the-etl-building-blocks)
and `tests/test_example_pipeline.py` record that design; neither is a current
extension guide.

## Product boundary

REF-048 assigns the surviving boundaries:

- **SpicyRegs** owns source-native acquisition, faithful source records, and
  immutable public source-data publication.
- **DocSpec** owns `SourceCatalog`, cross-source catalog meaning, capture,
  document processing, and `DocumentRelease`.
- **RefSpec** owns vocabulary releases, concepts, labels, mappings, redirects,
  and explicit resolution of source terms.
- **Rulespec** owns the generic platform-artifact container and shared byte
  rules; its domain products retain their separately accepted responsibilities.
- **SpicySearch** owns query planning, retrieval, ranking,
  explanations, search receipts, indexes, and query-time coverage.

This predecessor also published experimental, digest-pinned source-profile facts
in [`policies/`](policies/). `source-profile-catalog-v0.json` describes the 17
source profiles without loading the document pipeline.
`profile-resource-applicability-v0.json` records only source-native
relationships to resources in the pinned RefSpec catalog. Search admission,
facets, expansion, and ranking remain SpicySearch decisions.

The recorded generator command was:

```text
uv run python tools/generate_source_profile_artifacts.py --write
```

The recorded caller-selected output command was:

```text
uv run build-source-profile-artifacts \
  --applicability-input policies/profile-resource-applicability-input-v0.json \
  --refspec-catalog RefSpec/portfolio/resource-catalog-v0.json \
  --output output/source-profile-artifacts
```

Do not execute either command or dereference that repository-relative RefSpec
path. Current reference use consumes a published, digest-pinned RefSpec artifact;
optional search admission belongs to SpicySearch.

The historical document-release command for the checked-in Regulations.gov JSON
record and exact four-page PDF was:

```text
uv run --frozen build-document-release-from-files \
  --manifest sample-data/mirrulations/document-release-file-manifest-v1.json \
  --output-dir ./output/mirrulations-document-release
```

The predecessor command verified both source-file digests, extracted embedded
PDF text, created page-derived Unicode passages, validated the release, and
wrote a source-complete distribution. `document-release.json` pointed to
content-addressed copies of the exact JSON and PDF bytes under `renditions/`
and to the captured-file manifest under `receipts/`. The Rulespec Core release
is a pinned dependency, not a copied file in this distribution; a validator
must receive the matching Core file through `--rulespec-core`. The repository
default was a fixture, so this command produced a `conformance` release rather
than production evidence. Its recorded source-byte closure command was:

```text
uv run --frozen validate-document-release-distribution \
  --distribution ./output/mirrulations-document-release
```

The same predecessor publication path handled exact source-native HTML and XML.
This checked representative contains one congressional bill and one Code of Federal
Regulations section:

```text
uv run --frozen build-document-release-from-files \
  --manifest sample-data/document-files/document-release-representative-manifest-v1.json \
  --output-dir output/markup-document-release
```

The local 34-document evaluation cache exercises PDF, HTML, and XML across
seven source families and four size bands. It remains evaluation input: its
lock refers to code-defined source specifications and lacks complete
source-issued version metadata, so the publication command does not accept it.
This actual-file release path claims only embedded-text PDF and UTF-8 HTML/XML.
Scanned PDFs without embedded text fail closed; it does not claim optical
character recognition or Office-document support. HTML semantic isolation
recognizes a literal single `<main>` plus `<title>`; publisher layouts that use
another main-content convention need a source-specific capture adapter before
publication. Malformed HTML that depends on HTML5 implicit tag closing is also
outside this conformance slice. PDF parsing currently runs in process, so this
command was for controlled, digest-pinned capture jobs rather than arbitrary
user uploads; production intake still needed resource limits and process
isolation. DocSpec now owns any replacement builder and verifier; this README
does not claim that replacement is implemented.

The synthetic M1 builder command remains historical conformance evidence:

```text
uv run build-document-release --output ./output/document-release-m1.json
```

It reads repository-local, digest-pinned source and Rulespec Core fixtures and
rejects any invalid digest, coordinate, classification, projection, or
reference. It is not evidence that acquired source files were processed.

The checked-in M1 release is
`src/spicy_regs/fixtures/spicyregs-m1-document-release-v1.json`; consumers pin
its `release_id` and `release_digest`, not a source-tree path.

## Historical managed-vocabulary incubation

This repository incubated managed-vocabulary and search experiments before the
current product boundary above. That evidence remains useful for migration, but
it is not SpicyRegs runtime authority. RefSpec now owns the managed vocabulary
capability and SpicySearch owns its search read models. The
[historical RefSpec roadmap at `3c1b94ace91f`](https://github.com/Formspec-Labs/RefSpec/blob/3c1b94ace91f/plans/managed-vocabulary-experiment-roadmap.md)
records the evidence at that commit; [current RefSpec authority](https://github.com/Formspec-Labs/RefSpec)
is separate.

This proves the specification and lookup mechanics against real sources. It
does not claim product accuracy, a sealed holdout, production deployment, or
real cross-scheme mapping; the selected native sources contain no authored
SKOS mapping assertions.

## License

[MIT](LICENSE) © Civic Tech DC.

The underlying data is public U.S. federal government information. Mirrulations
S3 is read with unsigned (anonymous) access, and the published Parquet/Iceberg
on R2 is openly readable.

## Contact

- **Slack** — [join the Civic Tech DC workspace](https://join.slack.com/t/civictechdc/shared_invite/zt-43eotbj04-QLQ_Ria296PtRYJU2EgwxQ)
  (open invite, anyone welcome), then say hello in
  [#spicy-regs](https://civictechdc.slack.com/archives/C09H576E6LU). This is the
  fastest way to reach us.
- **GitHub** — open an
  [issue](https://github.com/civictechdc/spicy-regs/issues/new/choose) for a bug,
  a feature idea, or a question about the data.
- **Email** — [eugene.kim@civictechdc.com](mailto:eugene.kim@civictechdc.com) for
  sponsorships, partnerships, or enterprise use cases.

## Acknowledgments

- [Mirrulations](https://github.com/MoravianUniversity/mirrulations) — the public
  regulations.gov mirror this pipeline reads from.
- [regulations.gov](https://www.regulations.gov) and the agencies that publish
  their dockets there.
- Everyone who has contributed code, issues, and analysis.

---

<div align="center">

<a href="https://www.civictechdc.org/">
  <img src="assets/civictechdc-logo.png" alt="Civic Tech DC" width="72" height="72">
</a>

Built and maintained by **[Civic Tech DC](https://www.civictechdc.org/)** — a
volunteer community using technology to serve the DC region.

**[Come build with us.](https://join.slack.com/t/civictechdc/shared_invite/zt-43eotbj04-QLQ_Ria296PtRYJU2EgwxQ)**

</div>
