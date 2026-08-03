<div align="center">

<img src="assets/icon.png" alt="Spicy Regs" width="96" height="96">

# Spicy Regs

</div>

SpicyRegs captures public regulatory sources and publishes immutable source
records, exact document versions, Unicode text representations, structural
passages, source observations, verified links, and acquisition coverage. It
provides the source facts that downstream products can reproduce and audit.

## Product boundary

SpicyRegs owns source acquisition and source-addressable document structure.
It does not own managed vocabulary policy, extracted semantic assertions, or
search ranking and serving:

- **RefSpec** owns vocabulary releases, concepts, labels, mappings, redirects,
  and explicit resolution of source terms.
- **Rulespec Core** owns portable evidence and semantic record shapes;
  **Rulespec Extrapolator** owns candidate extraction and validation.
- **SpicySearch** owns query planning, document retrieval, ranking,
  explanations, search receipts, indexes, and query-time coverage.

SpicyRegs also publishes experimental, digest-pinned source-profile facts in
[`policies/`](policies/). `source-profile-catalog-v0.json` describes the 17
source profiles without loading the document pipeline.
`profile-resource-applicability-v0.json` records only source-native
relationships to resources in the pinned RefSpec catalog. Search admission,
facets, expansion, and ranking remain SpicySearch decisions.

Regenerate the checked files from the selected RefSpec catalog with:

```bash
uv run python tools/generate_source_profile_artifacts.py --write
```

To build the same two artifacts into a caller-selected directory, run:

```bash
uv run build-source-profile-artifacts \
  --applicability-input policies/profile-resource-applicability-input-v0.json \
  --refspec-catalog RefSpec/portfolio/resource-catalog-v0.json \
  --output output/source-profile-artifacts
```

Build a release from the checked-in Regulations.gov JSON record and its exact
four-page PDF:

```bash
uv run --frozen build-document-release-from-files \
  --manifest sample-data/mirrulations/document-release-file-manifest-v1.json \
  --output-dir ./output/mirrulations-document-release
```

The command verifies both source-file digests, extracts embedded PDF text,
creates page-derived Unicode passages, validates the release, and writes a
source-complete distribution. `document-release.json` points to
content-addressed copies of the exact JSON and PDF bytes under `renditions/`
and to the captured-file manifest under `receipts/`. The Rulespec Core release
is a pinned dependency, not a copied file in this distribution; a validator
must receive the matching Core file through `--rulespec-core`. The repository
default is a fixture, so this command produces a `conformance` release rather
than production evidence. The source-byte closure check is also available as a
separate command:

```bash
uv run --frozen validate-document-release-distribution \
  --distribution ./output/mirrulations-document-release
```

The same publication path handles exact source-native HTML and XML. This
checked representative contains one congressional bill and one Code of Federal
Regulations section:

```bash
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
command is for controlled, digest-pinned capture jobs rather than arbitrary
user uploads; production intake still needs resource limits and process
isolation.

The synthetic M1 builder remains a small conformance fixture:

```bash
uv run build-document-release --output ./output/document-release-m1.json
```

It reads repository-local, digest-pinned source and Rulespec Core fixtures and
rejects any invalid digest, coordinate, classification, projection, or
reference. It is not evidence that acquired source files were processed.

The checked-in M1 release is
`src/spicy_regs/fixtures/spicyregs-m1-document-release-v1.json`; consumers pin
its `release_id` and `release_digest`, not a source-tree path.

## Quickstart

Prerequisites: Python 3.10+ and [uv](https://docs.astral.sh/uv/getting-started/installation/).

```bash
git clone --recurse-submodules https://github.com/civictechdc/spicy-regs.git
cd spicy-regs
uv sync                       # install dependencies into .venv
uv run pytest                 # run the test suite
uv run ruff check .           # lint
```

No credentials are needed to run the tests, download the published Parquet, or
run the pipeline against the public Mirrulations mirror. Copy `.env.example` to
`.env` only if you want to publish output to Cloudflare R2 or ingest a source
that requires an API key.

## Historical managed-vocabulary incubation

This repository incubated managed-vocabulary and search experiments before the
four-product boundary above. That evidence remains useful for migration, but
it is not SpicyRegs runtime authority. RefSpec now owns the managed vocabulary
capability and SpicySearch owns its search read models. The historical
[active roadmap](RefSpec/plans/managed-vocabulary-experiment-roadmap.md)
records the evidence and remaining decisions.

This proves the specification and lookup mechanics against real sources. It
does not claim product accuracy, a sealed holdout, production deployment, or
real cross-scheme mapping; the selected native sources contain no authored
SKOS mapping assertions.

### Download the published data locally

Download the published Parquet with the bundled CLI — no credentials:

```bash
uv run spicy-regs download                        # dockets, documents, comments
uv run spicy-regs download --types comments       # comments only
uv run spicy-regs download -o ./my-data           # custom output dir
```

Files land in `./spicy-regs-data/` by default. Then poke around:

```bash
uv run spicy-regs stats                # row counts + top agencies per file
uv run spicy-regs sample comments -n 5 # 5 random rows from comments
uv run spicy-regs search "climate"     # substring search across files
uv run spicy-regs agencies             # list every agency code
```

The current `spicy-regs search` command is a legacy exploratory surface. It
still searches dockets and comments and remains available only while its
consumers migrate; it is not the document-only SpicySearch API.

> Don't want to clone? Run it one-shot:
> `uvx --from "spicy-regs @ git+https://github.com/civictechdc/spicy-regs" spicy-regs download --types comments`

For SQL-first exploration, [`docs/querying-python.md`](docs/querying-python.md)
walks through querying the bucket directly with DuckDB.

## Running the pipeline yourself

The pipeline reads raw JSON from the public Mirrulations S3 mirror, flattens it,
and writes Parquet to `./output/`. Scope your first run tight so it finishes in
minutes instead of hours:

```bash
# Smallest useful run: one agency, recent dockets, comments only, no upload.
uv run run-pipeline --agency EPA --only-comments --since-year 2025
```

What you get:

- `output/comments.parquet` — merged and deduplicated comments
- `output/manifest.parquet` — a Bloom filter of already-processed source keys,
  so the next run is incremental. Delete it or pass `--full-refresh` to rebuild
  from scratch.

Useful flags (`uv run run-pipeline --help` for the full list):

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

```bash
uv run run-rollup-feed-summary        # derived from the core R2 tables
uv run run-rollup-federal-register    # ingests an external API → its own Parquet
```

Each defaults to `--skip-upload`. See [CONTRIBUTING.md](CONTRIBUTING.md) for the
full list and for what adding a new external source touches.

### Backfilling comment text

The ETL fills `text_content` inline only for comments it processes fresh — the
incremental manifest skips comments ingested before that feature landed, so they
stay `NULL`. Backfill from Mirrulations' pre-extracted text (read straight from
the bucket's `derived-data` prefix — no PDF download, no JSON re-ingest):

```bash
uv run spicy-regs download --types comments     # grab the published parquet
uv run backfill-comment-text                     # fill text_content in place
uv run backfill-comment-text --limit 5000        # cap work for a trial run
uv run backfill-comment-text --upload            # republish to R2 (needs credentials)
```

It's incremental and re-runnable — rows with a `text_extraction_status` are
skipped unless you pass `--overwrite`. Document text isn't published to
`derived-data`; backfill those with `uv run enrich-pdf-text --target documents`.

### Working on the data dictionary

```bash
uv run spicy-regs-dict check        # verify descriptions match the schema
uv run spicy-regs-dict generate     # regenerate docs/tables/*.md
uv run --group docs mkdocs serve    # preview at 127.0.0.1:8000
```

Edit descriptions in `data_dictionary/descriptions.yaml`. CI fails if they drift
from the schema.

## Use it from an AI assistant

A read-only MCP server exposes SQL over the corpus with three tools:
`list_sources()`, `describe_table(table)`, and `query_sql(sql)`.

| Client | Setup |
|---|---|
| **Claude.ai** or any remote MCP client | Add `https://mcp.spicy-regs.dev/mcp` as a Custom Connector. See [`mcp-server/README.md`](mcp-server/README.md). |
| **Claude Code** (plugin) | `/plugin marketplace add civictechdc/spicy-regs` then `/plugin install spicyregs@spicy-regs-local` |
| **Claude Code** (stdio, no deploy) | `claude mcp add spicy-regs -- uvx --from "spicy-regs @ git+https://github.com/civictechdc/spicy-regs" spicy-regs-mcp` |
| **Cursor / Continue / OpenAI / others** | See [`plugins/spicyregs/INSTALL.md`](plugins/spicyregs/INSTALL.md) for the full matrix, including a prompt-only fallback for assistants without MCP support. |

## Project layout

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
notebooks/            # Example analyses (runnable on Binder)
tests/                # Unit suite; integration tests are opt-in
.github/workflows/    # ETL cron, per-rollup crons, CI, deploys
```

The ETL is assembled from small composable pieces — `Reader → Transform →
Writer`, wired by a `Pipeline`. The
[architecture section of CONTRIBUTING.md](CONTRIBUTING.md#architecture-the-etl-building-blocks)
explains the contract, and `tests/test_example_pipeline.py` is a runnable
reference for adding your own.

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
