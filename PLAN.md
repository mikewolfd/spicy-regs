# PLAN

What a session needs to pick this repository up. Current state only; the
July–August program's disposition is in `docs/disposition.md` and is not
repeated here.

## Where this sits, and the one rule that matters

`origin` is `github.com/civictechdc/spicy-regs`. **Eugene Kim owns `main`
there. We are guests.** `fork` is `github.com/mikewolfd/spicy-regs` and is
Mike's.

- **Do not push to `origin` unless Mike names the branch.** Local `main`
  deliberately tracks `fork/main`, not `origin/main`, so a bare `git push`
  cannot reach Eugene's trunk.
- Two older branches still track `origin/main` rather than a namesake
  (`fix/r2-publish-preflight`, `feat/source-domain-drift-gate-revived`). A bare
  push from either is *addressed at* Eugene's trunk and is refused only because
  `push.default` is unset and therefore `simple`. That is safe by
  configuration, not by intent — name the remote and branch explicitly.
- Three pushes have reached `origin`: `docs/disposition` (PR #193) and
  `feat/court-opinions` (PR #195), both on Mike's instruction, and local
  `main` itself on 2026-09-17, pushed by Mike.

## State

Two fixed points, and one command for everything that moves. The first draft of
this section quoted a commit and a count; the commit that added the section
made both wrong within the hour, which is the trap named at the bottom of this
file arriving in the file itself.

- **`origin/main` is `8737a26`.** On 2026-09-17 Mike pushed local `main`
  straight to origin — a fast-forward of 93 commits over Eugene's `1f02a7f`,
  the owner's own exception to the rule above. Before that, every commit on
  it was Eugene's.
- **`fork/main` is `8737a26`.** Local `main`, `fork/main` and `origin/main`
  point at the same commit; there is no unpushed delta in any direction.

```bash
git fetch origin fork --prune
git log --oneline fork/main..main      # expect empty
git status --porcelain                 # expect empty
uv run pytest -q                       # 1,230 passing as of 8737a26 (2026-09-17)
```

Nine PRs were open on `origin` before the 2026-09-17 direct push (ours: #181,
#182, #183, #193, #194, #195, #196; #95 and #144 are Eugene's and conflict
with main). The push landed two weeks of merged work directly, so those PRs
may overlap what origin already carries — reconcile before opening anything
new.

## Waiting on Mike — four decisions, none blocked on work

Stated in full, with the measurement that makes each answerable, at the top of
`docs/disposition.md`. In short:

1. **The FCC credential fix upstream, and key rotation.** Two questions, one
   secret. `c6695e0` is committed locally and pushed nowhere. Rotation is
   settled by no code change.
2. **The eight-table registration PR.** `feat/register-eight-tables`
   (`2854262`) is on the fork, unmerged, no PR. Eugene merging #194/#195/#196
   unblocks it; opening the PR is still Mike's call.
3. **`comment` and `restrictReasonType` on the `documents` table.** A schema
   revision; its cost depends on which lane sources it.
4. **Nine uncited `corpora/` scripts.** Default is they stay on the fork.

## Next actionable work

**Add `full_text_xml_url` to the Federal Register ingest.** A person cannot
search inside a rule today, and whoever builds that first will take the pointer
we publish — which is the HTML body. Publishing the wrong pointer exports the
defect downstream.

The case for XML is a measurement I did not take and have not re-derived: over
993 real Federal Register documents, HTML bodies carry publisher boilerplate on
993 of 993 with a 135-character median passage, XML on 0 of 993 with a median
of 610. It lives in
`~/Work/corpora/_preserved-2026-08-10/body-retrieval-corpus-2026-08-02/`
(`measurement.json` and `measurement-xml.json`); I confirmed both files exist
and carry those figures, nothing more.

**Fetch it; do not derive it.** The `html`→`xml` swap on `body_html_url` looks
right and is wrong: tested against the publisher's API across six eras it
matched five and failed on a document published 2000-01-03 carrying a 1999
document number, which has an HTML body and no XML one. A derived backfill
fabricates a 404 for exactly the carried-over documents. XML is otherwise
present on every month probed from 2000 to 2009. The field goes in the source's
field list and the published schema; the backfill is a full-range run of the
existing rollup, which is a publish and therefore Mike's.

## Downstream consumers

`data_dictionary/catalog.json` is a **vendored contract**, not a fetched one.
spicysearch holds a copy pinned by the digest in `catalog.json.sha256`; it
cannot import `spicy_regs`. Changing the file means the consumer must
re-vendor, so bump `CATALOG_FORMAT_VERSION` when the shape changes and say so.
It declares only what this repo *publishes* — it carries no searchability
field, and a test forbids even the words, because whether a class is indexed is
the serving side's fact.

## Source-provider work for dataset experiments

### BILLSTATUS subject acquisition

- [x] Replace the local bulk XML downloader/parser with the SpicyDocs 0.3.0
  wheel; qualify source identity, malformed/access failures, and table behavior.
  Keep Congress API selection and the six-column subject transform unchanged.
- [x] Validate the installed wheel and frozen lock on Python 3.12; provider
  revision is in [vendor/README.md](vendor/README.md), with wheel digests in
  `uv.lock`. Full suite: 1,040 passed; base-wheel CLI/MCP imports also qualified.

This is source reuse only: the subject table retains carrier and enrichment
time, not exact BILLSTATUS XML or text-version links. SpicyDocs returns those
captures to callers that need to retain them; this transform publishes its
existing subject fields. Dataset catalogs and processing remain outside it.

### Retained FEC committee records and reported relationships

- [x] Reuse the existing committee mapping for caller-supplied OpenFEC rows,
  writing bounded batches and keeping an existing output intact on failure.
  The default API builder uses the same writer; published columns are unchanged.
- [x] Map selected bulk, current API and original-statement relationship fields
  into one local, source-cited table. Preserve name-only targets, blanks, explicit
  NONE, source ID shape errors and conflicting statements. API observations keep
  their capture date rather than becoming facts for each cycle in `cycles`.
  Local handoff fixes emit every missing/null/empty sponsor-list state, keep
  source entity codes/labels with identifier roles, and retain exact array
  elements with parent pointers instead of copying whole arrays per row.
  The selected rebuilt output passes independent source-role/state verification;
  earlier output files retain their original qualification limits.
  See [inputs, use and limits](docs/fec-relationships.md).
- [x] Qualify the selected current committee census from an admitted SpicyDocs
  release through the existing committee writer and a DocSpec metadata catalog.
  The [installed-wheel delivery](/Users/mikewolfd/Documents/Codex/fec-handoff-fixes/integration/census-delivery-2026-09-12.md)
  preserves complete metadata companions and source evidence and checks every
  mapped field. No new SpicyRegs runtime dependency or default API switch was needed.
- [ ] Connect a complete, verified FEC distribution to the existing rollup.
  A local selected-record table is not the full committee population or a
  globally published table. The local census release does not include the bulk,
  gap-status, historical or original-filing populations. Bulk masters omit some API fields, and unverified-filer
  references require separate source status rather than invented values.

These open tasks own only SpicyRegs changes. They coordinate with DocSpec's
dataset workflow and SpicyDocs' source work, using planning sources DocSpec
`3e3e43e` and SpicyDocs `40921d3`. SpicyRegs remains independently usable for
public-data users. Retaining SpicyDocs separately is valid; none of these tasks
requires moving that package into SpicyRegs. Adding the backlog does not
establish that a capability is supported or that upstream work has been accepted.

<a id="sr01"></a>

- [ ] **SR01 — Select SpicyRegs capabilities to reuse and local duplication to remove.**
  **Owner: SpicyRegs.** Review actual source connectors, strict parsers, table
  and Iceberg publication, CourtListener handling, documented-value diagnostics,
  public imports and optional dependencies. Coordinate the local inventory with
  [SpicyDocs S11](../spicy-docs/docs/simplification-todo.md#s11), its
  [ownership decision S25](../spicy-docs/docs/simplification-todo.md#s25), and
  [DocSpec D41](../DocSpec/docs/dataset-experiments-todo.md#d41).
  **Done when:** each candidate names its current callers, exact implementation,
  beneficiary, supported or required use, KEEP/SHARE/REMOVE/DEFER decision and
  reason. Every selected reuse names the provider's public wheel capability and
  the copy or maintenance step it replaces; justified differences remain
  explicit. No DocSpec or search caller is required to justify independently
  useful source publication. [SR03](#sr03) owns selected SpicyRegs changes;
  [SpicyDocs S13](../spicy-docs/docs/simplification-todo.md#s13),
  [S14](../spicy-docs/docs/simplification-todo.md#s14),
  [S15](../spicy-docs/docs/simplification-todo.md#s15) and
  [S16](../spicy-docs/docs/simplification-todo.md#s16) own their local dispositions;
  [DocSpec D42](../DocSpec/docs/dataset-experiments-todo.md#d42) owns its adapter
  changes. Only the relevant ownership decision gates each handoff.

<a id="sr02"></a>

- [ ] **SR02 — Provide supported retained public-comment/table input facts.**
  **Owner: SpicyRegs.** Identify a bounded retained input and expose its supported
  public interface for [DocSpec D52](../DocSpec/docs/dataset-experiments-todo.md#d52),
  using the intake interface in [D06](../DocSpec/docs/dataset-experiments-todo.md#d06).
  Document source-qualified record identity, retained-input identity, field
  provenance, available comment text or candidate document locators, rejected
  rows and missing fields. State the observed scope and coverage assumptions,
  including how this table differs from other Regulations.gov representations;
  use explicit unavailable values where the input supplies no evidence.
  **Done when:** the selected reader/API and a bounded retained fixture are usable
  through the provider's installed wheel, with exact source facts and clear
  coverage limits. Any necessary source-schema/API change is explicit and
  qualified; table availability alone does not establish complete comment or
  document coverage. DocSpec owns catalog selection and inspection in
  [D07](../DocSpec/docs/dataset-experiments-todo.md#d07), its adapter/example in
  [D52](../DocSpec/docs/dataset-experiments-todo.md#d52), and broader experiment
  qualification in [D38](../DocSpec/docs/dataset-experiments-todo.md#d38).
  Source data remains usable without a DocSpec processing run or a recreated
  public-data pipeline inside DocSpec.

<a id="sr03"></a>

- [ ] **SR03 — Implement selected local source improvements and consumer handoffs.**
  **Owner: SpicyRegs.** Implement only the SpicyRegs changes selected in
  [SR01](#sr01), plus source changes needed for [SR02](#sr02). Link each change
  to its applicable [SpicyDocs S13](../spicy-docs/docs/simplification-todo.md#s13),
  [S14](../spicy-docs/docs/simplification-todo.md#s14),
  [S15](../spicy-docs/docs/simplification-todo.md#s15) or
  [S25](../spicy-docs/docs/simplification-todo.md#s25) handoff. Preserve exact
  source values, provenance, strict parsing and pagination/refusal behavior,
  bounded operation, and the distinct guarantees of raw, native and table output.
  Expose the smallest selected public wheel API; keep optional dependencies
  optional and the provider independent of DocSpec.
  **Done when:** selected local changes have focused source fixtures and an
  installed-wheel handoff recording source revision, package version, wheel
  digest and consumer requirements; replaced local code/dependencies are removed
  after the selected consumers switch. DocSpec owns its integration and
  qualification in [D42](../DocSpec/docs/dataset-experiments-todo.md#d42) and
  [D46](../DocSpec/docs/dataset-experiments-todo.md#d46); SpicyDocs owns its local
  switch/removal in [S16](../spicy-docs/docs/simplification-todo.md#s16) and S25.
  Record deferred changes separately. Distinguish local preparation, commits,
  proposed upstream work and accepted upstream work; follow this plan's existing
  authorization rules for publication and upstream submission.

<a id="sr04"></a>

- [x] **SR04 — Adopt the faithful CourtListener bulk reader.**
  SpicyRegs 0.1.5 uses SpicyDocs 0.17.0 directly in court-scope, opinion-cluster
  and opinion-body transforms; the 519-line duplicate reader is removed.
  All 3,361 retained court IDs and 397 federal classifications are preserved.
  The shared reader retains 11,808 quoted empty strings previously lost as null;
  independent CSV decoding confirms every value. Opinion tables deliberately
  retain their existing empty-to-null rule; docket maps preserve the distinction.
  Failed builds close the source and discard staging while preserving the error.

  **Qualified locally (2026-09-15):** 1,167 source tests and 213 ordinary
  installed-wheel tests pass, including BILLSTATUS, Agenda and PDF consumers.
  The base install imports CLI/MCP without source-reader dependencies. Package
  files match the wheel and committed source; independent review approves.
  SpicyRegs explicitly qualifies pypdf 6.14.2 because newer backend recovery
  changes malformed-page outcomes. The failed 0.1.4 installation remains evidence.
  Source commits: `3bdb468`, `83ed49b`. No data backfill or publication occurred.
  **PAR10 follow-up complete locally:** `dc4a33f` shares the two opinion-table
  writers; `b6cb3f1` (0.1.6) adopts SpicyDocs 0.18's guarded transfers.
  Eligible read exceptions resume only after matching the original HTTP object
  and byte range; access refusals and incomplete EOF stop immediately.
  All 1,176 source tests and 221 installed tests pass. Real local HTTP checks
  preserve all 3,361 retained courts after interruption; independent review approves.
  [Follow-up evidence](</Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/parsing-consolidation-2026-09-14/par10/delivery.md>).
  [Delivery and evidence](</Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/parsing-consolidation-2026-09-14/sr04/delivery.md>).

## How to run anything

Everything goes through `uv run`, never a binary from `PATH`. The gate:

```bash
uv run ruff check . && uv run ty check && uv run pytest
uv run spicy-regs-dict check      # descriptions vs schema
uv run spicy-regs-dict generate   # rewrites docs/tables/*.md AND catalog.json
```

`generate` writes the catalog and its digest too, so a contributor cannot leave
them stale by not knowing a third command exists.

## Traps that have cost time here

- **If you can state it without running it, you have not checked it.** Every
  wrong call in this lane came from reasoning about a shape; every correct one
  from executed output. `except OSError` catches no DuckDB read error. A
  matching identifier *shape* is not a matching namespace.
- **A record that a fix was applied is not a check that it worked.** Re-observe
  the thing. Applied, armed and scheduled are three different words.
- **Audit documents against your own recent changes first**, not only against
  the world. The README defect found on 2026-09-07 was introduced that morning
  by the auditor.
- **`grep` exits 1 on zero matches** and will silently break an `&&` chain that
  ends in a commit. `set -o pipefail`; zsh does not word-split unquoted `$var`.
- **State the population before quoting a rate**, and state a scan's reach:
  "clean everywhere I could reach" is a measurement, "clean" is a reassurance.
