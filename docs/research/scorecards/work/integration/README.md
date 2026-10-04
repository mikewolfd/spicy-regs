# Reproduce scorecard integration

The [generated work queue](integration_progress.md) accounts for every publisher
in the [adapter matrix](adapter_support_matrix.md) and
[API inventory](publisher_api_inventory.md). Its pinned
[qualification ledger](integration_qualifications.json) advances only named
editions and renditions. Original discovery observations remain available;
the ledger does not imply that an entire publisher archive is supported.

## Retain source inputs

Keep original captures, model observations, comparison references and plans in
the external campaign corpus described by the SpicyDocs source workflow. Public
qualification receipts retain metadata and hashes. Publisher and model bodies
remain private under `hash_only` unless redistribution rights are established.

Each plan entry identifies the publisher adapter and exact `ScorecardEdition`,
then pins the reader module, ordered capture manifest, source qualification and
complete native table reference. Reviewed PDF readers additionally pin their
semantic observations or named `reader_inputs`. Shared extraction observations
use the lossless page JSON format; normal replay does not load pickle files.

Capture manifests preserve request method, body and header pins when applicable,
response bytes, observation time and direct or Zyte provenance. Replay requires
the exact request sequence and consumes every capture. An HTML or JSON API
reader still runs its own pagination, count and stable-reread checks.

## Build and adopt the provider

Use a frozen provider directory containing the reviewed scorecard modules. Build
against the consumer's existing pinned baseline wheel, rather than rebuilding
unrelated source readers from a concurrently changing checkout:

```sh
uv run --frozen python scripts/build_scorecard_overlay.py \
  --baseline-wheel "$SCORECARD_BASELINE_WHEEL" \
  --provider "$SCORECARD_FROZEN_PROVIDER" \
  --output "$SCORECARD_WHEEL_OUTPUT"
```

The builder creates the wheel twice, requires identical bytes, and proves that
unrelated runtime and dependency metadata are preserved. Retain `wheel.json`.
Copy the selected wheel to `vendor/`, update the matching dependency and source
pins in `pyproject.toml`, then run `uv lock` and `uv sync --frozen`. Verify the
installed reader hashes and run the affected consumer checks. Source
qualification, installed package, publication and scheduled refresh are
separate stages; a qualified reader is not automatically enabled.

## Generate coverage and prepare data

With the selected provider installed, generate bounded qualification metadata
from the private plan and its digest:

```sh
uv run --frozen python scripts/build_scorecard_qualifications.py \
  --plan "$SCORECARD_PLAN" --plan-sha256 "$SCORECARD_PLAN_SHA256" \
  --corpus "$SCORECARD_CORPUS" \
  --directory docs/research/scorecards/work/integration \
  --observed-at "$SCORECARD_OBSERVED_AT" \
  --published-ledger docs/research/scorecards/work/integration/integration_publications.json
uv run --frozen python scripts/build_scorecard_integrations.py
uv run --frozen python scripts/build_scorecard_integrations.py --check
```

The coverage plan includes every qualified scope being reported. A smaller
ingestion plan may select only newly qualified or refreshed scopes. Capture the
current public publication index, then prepare into fresh directories:

```sh
uv run --frozen python scripts/prepare_scorecard_integrations.py \
  --plan "$SCORECARD_INGESTION_PLAN" \
  --plan-sha256 "$SCORECARD_INGESTION_PLAN_SHA256" \
  --corpus "$SCORECARD_CORPUS" --index "$SCORECARD_PRIOR_INDEX" \
  --output "$SCORECARD_CANDIDATE" \
  --private-observations "$SCORECARD_PRIVATE_OBSERVATIONS"
```

Preparation replays installed readers without publisher network requests. It
compares every source field against the qualified reference, requires all
selected scopes to finish before writing tables, and uses the normal scoped
replacement and ETL evidence pipeline. Unselected historical rows must remain
identical. Public evidence is checked for metadata-only members and absence of
publisher or model bodies. `preparation.json` records the verified generation,
input plan, preservation results and limits. A failed run keeps its evidence;
retry uses fresh candidate directories and never translates failure into zero
scorecards.

## Publish and resolve

Commit the tested runtime, package pins and metadata before invoking
[`publish_candidate.py`](deployment/publish_candidate.py) with the preparation
and implementation commit. The publisher verifies the prepared generation and
uses the existing atomic family update, preserving other publication families.
Record authenticated and public readback separately. Download and inspect the
published tables and query the hosted MCP before recording a published scope in
the ledger.

Prepare `scorecard-analysis` against the accepted source generation and the
already published congressional tables. Member and legislative-item resolution
remain downstream, use exact versioned rules, and retain every unresolved or
ambiguous occurrence. Existing congressional data is not acquired again.

Live refresh uses the source registry and normal rollup CLI. Enable a source
only after its live route and runtime inputs reproduce the qualified rendition;
private PDF assets need explicit host configuration. Keep available archives,
unsupported shapes, access recovery and unresolved discovery in the generated
work queue. An unavailable route alone does not establish publisher retirement.
