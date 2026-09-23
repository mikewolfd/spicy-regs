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
- One older branch still tracks `origin/main` rather than a namesake
  (`feat/source-domain-drift-gate-revived`). A bare push from it is
  *addressed at* Eugene's trunk and is refused only because `push.default` is
  unset and therefore `simple`. That is safe by configuration, not by intent —
  name the remote and branch explicitly.
- Two pushes have reached `origin` on Mike's instruction: `docs/disposition`
  (PR #193) and `feat/court-opinions` (PR #195). A third, local `main` to
  `origin/main` on 2026-09-17, was a mistake; Mike force-reset `origin/main`
  to Eugene's `1f02a7f`. While the commits were on `main`, GitHub marked our
  seven PRs merged (#181–#183, #193–#196); their content is not on origin.

## State

Two fixed points, and one command for everything that moves. The first draft of
this section quoted a commit and a count; the commit that added the section
made both wrong within the hour, which is the trap named at the bottom of this
file arriving in the file itself.

- **`origin/main` is Eugene's `1f02a7f`** (2026-09-02), restored by the
  force-reset above. Every commit on it is Eugene's.
- **`fork/main` is the squashed history: 20 commits above `1f02a7f`**,
  rewritten 2026-09-18 from the 94-commit history that preceded it. The tree
  is byte-identical; the old history is preserved as
  `archive/pre-squash-2026-09-18` on the fork, and each squashed commit's body
  lists the original commits it absorbed. Commits 1–7 are the seven PRs, one
  each, in their original order; none depends on another.

```bash
git fetch origin fork --prune
git log --oneline fork/main..main      # expect empty
git status --porcelain                 # expect empty
uv run pytest -q                       # count: see the full host gate entry in docs/fork-generation.md
```

The seven PRs must be re-created. Cut each from its squashed commit as a
single-commit branch off `origin/main`, then open the follow-ups (dictionary
contract, SpicyDocs reader adoption, FEC retained inputs) after those merge,
and the eight-table registration last. Eugene's #95 and #144 remain open on
origin and conflict with main.

## Waiting on Mike — four decisions, none blocked on work

Stated in full, with the measurement that makes each answerable, at the top of
`docs/disposition.md`. In short:

1. **The FCC credential fix upstream, and key rotation.** Two questions, one
   secret. The fix is squashed commit 14 on `fork/main` (`fix(fcc-ecfs): send
   the api.data.gov key as a header`) and reaches origin only through a
   re-created PR. Rotation is settled by no code change.
2. **The eight-table registration — mostly landed, branch archived.** The
   branch's materialized-dataset reader is superseded by
   `pipelines/materialized.py` on main, and two of its eight tables
   (`bill_subjects`, `court_opinion_clusters`) are registered by the
   fork-generation work; the third, `court_opinion_bodies`, was withdrawn and
   removed (fork delivery decision 6). The five rulemaking-family
   tables (`rule_targets`, `proceedings`, `regulatory_agenda_items`,
   `agenda_item_proceedings`, `comment_periods`) register with T17's
   publication — the same gate this item always named
   (`materialized/rulemaking/latest.json` answering 200), now part of the
   fork task queue rather than a rebase of `feat/register-eight-tables`
   (archived 2026-09-22, decided: no PR to open).
3. **`comment` and `restrictReasonType` on the `documents` table.** A schema
   revision; its cost depends on which lane sources it. The table's own
   catalog entry already states both are not carried and points readers at
   spicy-docs' source-native release, which captures both.
4. **Nine uncited `corpora/` scripts.** They live in `src/spicy_regs/corpora/`
   on the archive branches, never on `main` or origin. Two produced outputs
   the plan relies on: the 993-document body-retrieval corpus and its HTML/XML
   measurement, and the mixed-real-data corpus, both under
   `~/Work/corpora/_preserved-2026-08-10/`. Default is they stay where they are.

## Next actionable work

**Name both Congresses on the cron across a Congress boundary.** The four
Congress-scoped index rollups and the Record-issue walk take their scope from
`current_congress`, so from 3 January of an odd year the outgoing Congress
drops out: its published rows survive the merge but stop updating, and a
late correction to a December Record issue is never re-read (the index
review, 2026-09-19). The workflows already expose the scope as a dispatch
input; set it to both Congresses on the cron for the first weeks of a new
Congress, or derive the overlap in `congress_scope`, and say which.
**Chosen: derived in `congress_scope`** — `default_congresses` names both
Congresses for `CONGRESS_BOUNDARY_OVERLAP_DAYS` (45, through mid-February)
after the boundary; a workflow that sets `BILL_FAMILY_CONGRESSES` explicitly
must name both during the window, since the derivation only fills the unset
default. The 45 days are the plan's "first weeks" instruction and a
placeholder until a publisher correction-lag measurement replaces it.

**Host the document-to-RIN links as their own table.** `federal_register.rin`
is the first RIN of `regulation_id_numbers_json`; 1,499 documents carry two or
more (max 41), so a communication whose RIN is not the first never joins.
The repository already hosts one-to-many links as `fr_docket_links`; an
`fr_document_rins` rollup in that shape completes the regulatory bridge in
both directions and retires the unnest caveat in the dictionary.

**Bound the per-request retry class on every Congress.gov consumer.** The
listing reader's `max_requests` is a per-request retry bound, not a run cap:
the budget resets on every `capture_validated`, and spicy-docs's retry waits
with jitter up to 60 seconds between attempts. The bill reader passes 500
(`sources/congress_bills.py`), the roll-call rollup 500 and the amendments
rollup 2,000, so one request that keeps failing can hold a run for hours
before it dies. The A11 review found this through the backfill's own 2,400
override, which is removed; the three pre-existing bounds are the repo's
convention and stand until measured. Pick one bound from a measured retry
distribution, or make the reader's cap a run cap, and say which in the
decision record. **Decided 2026-09-22: measured, and the three bounds are
now one.** A bounded credentialed run over the bill and house-vote routes
(receipt `~/Work/corpora/fork-execution-2026-09-21/retry-measurement-2026-09-22/attempt-distribution.json`)
observed 18/18 requests succeed on the first attempt — a fair-weather
sample; the tail it cannot see is exactly what the bound exists for. The
shared per-request bound is therefore **5** (observed 1 plus four jittered
retries, ~32s worst-case at the 60s ceiling): a stuck request now fails
within about a minute instead of hours, applied to the bill reader, the
roll-call rollup and the amendments rollup. Re-measure before re-opening.

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

### Accepted costs (decided 2026-09-22)

**The rollup repeated passes stay.** `build_proceedings` reads `documents`
twice and `build_bill_family` reads its prior a few times; both are linear
and bounded, and each merge would break a load-bearing property — the first
documents pass builds the trusted set that gates the FR-links loop before
the groups it would enrich exist, and the statutes-at-large join must stay
a best-effort post-merge step so a corrupt `laws` table cannot fail the
bill run. Re-measure before re-opening.

## Downstream consumers
`data_dictionary/catalog.json` is a **vendored contract**, not a fetched one.
spicysearch holds a copy pinned by the digest in `catalog.json.sha256`; it
cannot import `spicy_regs`. Changing the file means the consumer must
re-vendor, so bump `CATALOG_FORMAT_VERSION` when the shape changes and say so.

**The current digest is whatever `data_dictionary/catalog.json.sha256`
holds** — read the file. It moves whenever the dictionary does, and a digest
quoted in prose is stale by the next `generate`; this section quoted one
several times and was wrong each time. SpicySearch records the commit it copied
in `vendor/spicy-regs-dictionary-source-commit.txt`; when
`git diff <that commit> HEAD -- data_dictionary/catalog.json` is non-empty, it
must re-vendor. Nothing in SpicySearch reads the file today and no consumer
branches on `kind`, so the fifth `kind` value, `sampled` (added with
`CATALOG_FORMAT_VERSION` kept at `3` because no field changed shape), needs no
handling there; a future reader that branches on `kind` must handle it.
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
- [x] Adopt the installed SpicyDocs reader in the default committee acquisition,
  retaining raw pages and completion evidence. Preserve the existing reference
  table's fields and whole-row merge; refuse incomplete traversals before replacing
  its output. Fresh live traversal still requires a configured API credential.
- [x] Build official source discovery metadata, selected native record companions,
  collection coverage and reported relationships through the existing table path.
  Bulk files support pinned original/member selections and verified source headers;
  named fields retain their literal source records and coordinates.
- [x] Expose dictionary meaning and actual table availability through MCP, including
  a local Parquet directory for examining audited generations before publication.
  See [FEC integration and checks](docs/fec-integration.md).
- [x] Map selected summary ZIP fields through verified official HTML dictionaries,
  retaining literal definitions, original rows and exact source coordinates.
  Keep file inventories separate from row counts and financial interpretation.
  See the [bulk expansion audit](docs/research/fec-bulk-continuation-2026-09-21.md)
  for selected 2026 transactions and source header discrepancies. The subsequent
  [Senate recovery](docs/research/fec-senate-recovery-2026-09-21.md) acquired 598
  live originals and one separate archive mirror despite the broken directory.
- [x] Seal the selected FEC tables through the shared generation path and verify
  a complete local publication/download/MCP roundtrip. Local MCP now reads CLI
  download batches and retains their selected table pins; loose directories
  remain explicitly unversioned. See the
  [generation audit](docs/research/fec-generation-readiness-2026-09-21.md).
- [ ] Make caller scope, mapping status and original evidence usable by remote
  consumers; publish the selected audited FEC families and qualify the hosted
  reader. See FG01–FG02 and FG21 in the [FEC gap register](docs/fec-gaps.md).
- [ ] Adopt the assessed committee history and remaining selected retained bulk
  populations (FG03–FG04), and qualify the existing FEC/member/vote join (FG18).
- [ ] Select further populations and complete their source-specific acquisition,
  native qualification, interpretation, scale and refresh work (FG05–FG17).
  The [coverage census](docs/research/fec-coverage-2026-09-21.md) accounts for all
  26 broad families, all 26 bulk groups and research tasks T01–T19. Wider history,
  downstream document/search adoption and adjacent sources remain separate from
  the completed local selection. FG19–FG24 record their limits and the shared
  deployment issues discovered during this work.

This plan owns SpicyRegs changes; the linked gap register separately assigns
provider and operational work. The original coordination used planning sources
DocSpec `3e3e43e` and SpicyDocs `40921d3`; the register records the newer evidence.
SpicyRegs remains independently usable for
public-data users. Retaining SpicyDocs separately is valid; none of these tasks
requires moving that package into SpicyRegs. Adding the backlog does not
establish that a capability is supported or that upstream work has been accepted.

### BillTrax-derived table hosting

**2026-09-19, branch `billtrax-hosting-prep` (local only, not pushed).** Landed
the half of the table-contract layer's spicy-regs side that does not need
SpicyDocs 0.21.0: [`docs/research/table-contracts-2026-09-19.md`](../spicy-docs/docs/research/table-contracts-2026-09-19.md)
§5 is the build brief.

- [x] `RollupPipeline` grew `outputs: ClassVar[tuple[str, ...]]`. `output`
  stays a plain string on every existing rollup (Python resolves that
  attribute from the subclass, so it shadows the base's new `output` property
  entirely — every existing rollup is unchanged). A rollup
  that declares `outputs` instead picks up the property (`outputs[0]`) and can
  return a tuple of paths from `build()`; `run()` uploads each through the
  same shrink guard. This is for the bill family: one acquisition + model pass
  producing eleven tables, not eleven rollups repeating it.
- [x] `transforms/table_merge.py::merge_table` — the prior-download-plus-DuckDB-merge
  half of `build_congress_bills.py` (~135-210), lifted and parameterised over
  `columns`/`identity`/`version_column` so a table keyed on more than
  `bill_id` doesn't copy the SQL, plus a `prior_present` flag so a caller that
  already checked for (and didn't find) a prior table doesn't pay for the same
  failed R2 download twice. `build_congress_bills` now calls it; the
  behavior-preservation proof is one new end-to-end test —
  `test_build_congress_bills_merges_prior_and_fresh_rows` in
  `tests/test_congress_bills.py`, which seeds a prior Parquet, stubs the fetch
  and the R2 download, and asserts on the merged output. (The other 21 tests
  in that file are unchanged, but they only ever covered `_shape`, `_bill_id`
  and windowing — they don't call `build_congress_bills()` and can't prove
  this on their own.) `tests/test_table_merge.py` covers the helper directly
  (fresh-wins-on-repeat, prior-only rows kept, version ordering, missing-prior
  tolerance, null-identity refusal, and the `prior_present=False` cold-start
  case with a call-counting stub).
- [x] `.github/workflows/_rollup.yml` gained `bill_family_congresses` /
  `bill_family_bill_types` inputs and a `GEMINI_API_KEY` secret placeholder
  (named, no value — the bill family's `classify_sections`/`summarize_bill`
  calls are Gemini-backed per `spicy_docs.extraction.gemini`). No rollup
  workflow reads them yet.
- [x] Vendored `deltatrack-0.1.0-py3-none-any.whl` (built `uv build --wheel`
  from `civictechdc/DeltaTrack@c636448`, SHA-256
  `7f060e30af9702f4e45c305fa93c53d1e3e59bd70858d3c6717f6fc9cf825197` — see
  [`vendor/README.md`](vendor/README.md)) and pointed
  `[tool.uv.sources] deltatrack` at it. `uv lock` resolves clean and leaves
  `uv.lock` byte-identical with the entry present (nothing depends on it yet),
  so it's inert until the 0.21.0 pin below pulls DeltaTrack transitively.

**2026-09-19, same branch, second half.** SpicyDocs 0.21.0 is released, so the
four items this entry listed as blocked are done. Twenty-two tables are now
hosted; the public surface went from 24 tables to 45.

- [x] **The release.** `vendor/spicy_docs-0.21.0-py3-none-any.whl` (commit
  `ff92406`, tag v0.21.0, sha256 `ca3f26c5…1705`, verified after the copy);
  0.20.0 deleted; both `source-readers` pins at
  `spicy-docs[acquisition,pdf-pypdf,bill-diff]==0.21.0`. Two resolver refusals,
  both fixed by matching what the release declares:
  0.21.0 pins `rulespec-artifacts==1.0.13` (we had 1.0.12 — vendored 1.0.13,
  which is byte-identical to rulespec's own build and to SpicyDocs' copy), and
  `deltatrack` resolved to the registry because a `[tool.uv.sources]` entry
  binds only a dependency the *project* declares, while DeltaTrack arrives
  through SpicyDocs' `bill-diff` extra — declaring `deltatrack==0.1.0` directly
  in both `source-readers` lists binds the vendored wheel, the same shape
  `rulespec-artifacts` already had. No `diff=False` fallback was needed.
- [x] **The transforms.** `build_bill_family.py` (thirteen tables from one
  pass), `build_press_releases.py`, `build_amendments.py`,
  `build_roll_call_votes.py`, `build_members.py`,
  `build_committee_reports.py`. Each imports its contract from
  `spicy_docs.schemas.*` and publishes through `merge_contract_table`, the one
  door added to `table_merge.py` — no transform restates a column tuple, an
  identity or a version column. `congress_scope.py` holds the
  "which Congresses, which types" rule the three Congress-sourced transforms
  share; `model_call.py` adapts a Gemini `GenerationClient` to the narrower
  `ModelCall` seam the interpretation package takes (the two do not fit
  directly).
- [x] **The rollups**, all `inputs = ()`, their `run-rollup-*` console scripts,
  and six `rollup-*.yml` workflows on their own crons (02:00 through 03:40 UTC,
  clear of the existing 17:00–24:00 block). `_rollup.yml` gained
  `committee_reports_since`.
- [x] **The dictionary.** `DERIVED_SCHEMAS` is joined by `contract_schemas()`,
  generated from `TABLE_CONTRACTS` as all-VARCHAR; `descriptions.yaml` entries
  carry a hand-written `label`, `coverage`, `measured_on` and `summary` plus
  `columns_from: spicy_docs`, which `load_descriptions` resolves by reading the
  407 column sentences out of the wheel. Declaring both the marker and an
  inline `columns:` is refused at load. One `mcp_server.py` `TABLES` line per
  table, kept literal so the MCP server stays installable without the
  `source-readers` group.

**2026-09-19, branch `hosting-a11-backfill` (worktree `spicy-regs-wt-a11`).**
Gap A11 (spicy-docs' `closing-the-gaps-2026-09-19.md` §2): bills before the
108th have no BILLSTATUS bulk, so the family never reached them. Landed: the
rollup splits its one scope input on the publisher's own floor
(`BULK_STATUS_FLOOR` = 108) — a named Congress at or above it fills from the
zips as always; one below it is backfilled from the API `bill` route, newest
Congress first, under the same per-run cap, one detail request per bill not
already filled. The default scope (the current Congress) is unaffected: the
backfill walks only when a pre-108th Congress is named.

- [x] The walk reuses the `congress-bills` reader seam
  (`sources/congress_bills.py::listing_reader`, `bill_detail`) — no second
  reader of the route. A `401`/`403` aborts the run; any other detail refusal
  is that one bill's gap, retried next run; a list walk that refuses fails
  the run loudly.
- [x] Resume state beside `bill_family_archives`, on the CRS summaries
  pattern AGENTS.md points at (skip only a success, retry every failure):
  `bill_family_backfills` (per attempted bill, the list stamp and whether it
  was filled or refused — a refusal is retried first next run, one request,
  without a walk) and `bill_family_backfill_walks` (per `(congress,
  bill_type)`, the route's declared total against what was actually walked;
  a unit walked complete with every record filled, refused or unwalkable is
  settled and never re-walked, so a permanent gap costs one request a run,
  not a page walk). Every page and every detail is charged to the same cap.
  Both are published outputs (dictionary + MCP), all VARCHAR.
- [x] Provenance: the detail record states laws, sponsors, the latest action
  and a `cosponsors.count`, but only sub-route *counts* for actions,
  committees, titles, subjects, summaries and text versions — so the
  backfilled `congress_bills` row publishes `cosponsor_count` from the
  sub-route count (never the sponsor arithmetic), NULLs every count a zero
  would lie about, NULLs `stage` and `signed_date_rule` (no action was
  examined, so neither the default rung nor the no-became-law-action rule is
  a finding), and carries a NULL `schema_version` as the marker that it is
  the detail route's; the consumer's test is `congress < 108`. Said in the
  `congress_bills` dictionary entry.
- [x] Proof — receipt
  `~/Work/corpora/supply-2026-09-02/receipts/a11-pre-108th-backfill-2026-09-19/`:
  declared counts for all 26 candidate Congresses (the 92nd smallest at 767);
  the 92nd walked end to end against its declared total (767 of 767, 4 pages,
  `completed: true`); the 767 retained records replayed through the real
  code path offline per type (declared totals summing to 767, every unit
  settled, the 50 retained details filled, the 716 not retained recorded as
  refusals), then resumed under a budget of 2 with no list request and
  exactly the first two refusals retried. 82 keyed requests of the 120
  budget; the key only ever an `X-Api-Key` header; `verify_credentials.py`
  re-scans everything retained. Review round one (cosponsor arithmetic,
  the default stage, the per-request budget misread as a run backstop, the
  uncharged pages, the double count) fixed in the follow-up commits.

**Every rollup is incremental where its source allows.** The pattern is
`build_congress_bills`': take a watermark from the prior published table, ask
the publisher only for what changed since, cap the window so a deep backfill
converges over runs instead of timing out, and let the merge accumulate.

| Rollup | What a steady-state run fetches | What bounds a catch-up |
| --- | --- | --- |
| `bill-family` | The 8 folder listings, then only the archives whose zip has moved since the entry retained last run, then only bills whose `updateDateIncludingText` differs from the published row, and of those only printings not already captured (plus a new printing's neighbour, so the consecutive-pair diff still happens) | `MAX_VERSION_FETCHES` printings/run; skipping what is held means the next run resumes on new ground rather than re-walking the same prefix |
| `amendments` | The `updateDate` window from the prior max minus `OVERLAP_DAYS`, server-side via `fromDateTime`/`toDateTime` | `MAX_WINDOW_DAYS` (90); `AMENDMENTS_SINCE`/`AMENDMENTS_UNTIL` drive a chunk |
| `roll-call-votes` | The index walk, then Clerk files only for roll calls not already published with a tally, plus the newest `OVERLAP_VOTES` re-read for corrections | `MAX_VOTES_PER_RUN`, newest first |
| `committee-reports` | GovInfo packages modified since the prior max `last_modified` minus `OVERLAP_HOURS`; packages already published are not re-fetched | `MAX_PACKAGES_PER_RUN`; 30 days is the cold-start window only |
| `press-releases` | Both feeds, whole — the feed *is* the delta, and the merge accumulates what rotates off | n/a |
| `members` | Both roster files, whole — one small JSON each, with no partial-fetch route | n/a |
| `laws` | The `law/{congress}` list, whole (one page), then PLAW USLM files only for laws not yet `captured` or whose list `update_date` moved (`unavailable` — the bulk lag — is retried every run); the classification index and each session table it links, whole, replacing that session's rows; Table III pages only for acts not yet published, oldest first, a refused act stepped past and three consecutive refusals ending the Congress's walk | `MAX_USLM_PER_RUN` and `MAX_TABLE3_PER_RUN`, PLAW newest first; pinned in `tests/test_laws.py` |
| `committee-rosters` | The `committee/{congress}` list, whole (one page; the route over-declares and `walk_route` publishes what it served), then details only for committees not yet folded or whose list `update_date` moved; both chamber files, whole, each replacing its chamber's seats for the current Congress | `MAX_DETAILS_PER_RUN`, newest `update_date` first; pinned in `tests/test_committee_rosters.py` |

`tests/test_incremental_rollups.py` pins each of these with a counting stub
rather than a docstring: a seeded prior plus an assertion on what was actually
requested.

Three limits worth knowing, each a property of the source rather than a
shortcut taken here:

1. ~~**Bulk archives are not skipped by last-modified.**~~ **Filled by
   SpicyDocs 0.21.1** (see the entry below). The reviewer's condition was
   "where spicy-docs's bulk listing exposes each zip's
   `formattedLastModifiedTime`"; it now does, through
   `BulkStatusAcquirer.list_archives`, and `bill-family` retains each folder's
   entry and passes it back as `unchanged_since`. The saving is not the 8
   requests — it is up to 52 MB of zip per run, 32 MB of it the H.R. folder
   alone.
2. **The roll-call index walk is not short-circuited.** The `house-vote` route
   declares `sort_honored=False`, so the publisher's order is not guaranteed
   monotonic in roll number and stopping on a page of already-held votes could
   silently drop roll calls sitting later in an unordered listing. The walk is
   a few 250-row pages per session; the per-roll-call Clerk fetch is the real
   cost and that *is* skipped.
3. **A corrected roll call cannot be detected by comparison.** The
   `roll_call_votes` contract has no column for the publisher's `updateDate`,
   so there is nothing to compare a listing's `updateDate` against.
   `OVERLAP_VOTES` re-reads the newest few every run instead, the same way
   `OVERLAP_DAYS` stands in for exact change detection in `congress_bills`.

**Two things a reader should know before changing this.**

1. **The `congress_bills` frozen prefix is why the merge is compatible, and it
   is asserted, not assumed.** The contract's first ten columns are byte-equal
   to `build_congress_bills.COLUMNS` — other repositories pin that prefix by
   digest through `catalog.json` — and the family appends thirty-eight more.
   `merge_table` NULL-fills any column the prior published table lacks, so
   the first family run merges 48-column rows onto the live 10-column table as
   a backfill rather than a migration.
   `test_congress_bills_keeps_its_frozen_prefix` and
   `test_merge_null_fills_columns_the_prior_table_lacks` hold both halves.
2. **`congress_bills` has two writers, and the merge is column-wise because of
   it.** `congress-bills` walks the whole archive for the ten-column prefix;
   `bill-family` fills all forty-eight for the Congresses it is scoped to.
   Row-wise replacement was wrong here and was a live defect: the narrow writer
   passed its own ten-column tuple, `merge_table` projected the prior onto it,
   and the published file came back **ten columns wide for every row** — the
   other thirty-eight deleted, not nulled. Measured at realistic scale the
   result was 96.6% of the prior bytes, and the R2 shrink guard only refuses
   below 50%, so nothing would have stopped it; the next family run would then
   have emitted a spurious `stage_changed` with `from=None` for every scoped
   bill, and the digest-pinned catalog would have disagreed with the live table
   for the six hours between the two crons.
   Fixed in two halves: `build_congress_bills` publishes through
   `merge_contract_table`, so the shape is always the contract's; and
   `merge_table` gained `coalesce_prior`, which FULL OUTER JOINs on the
   identity and emits `COALESCE(fresh, prior)` per column, so a narrow writer's
   NULL cannot overwrite a value it simply does not populate.
   `COALESCED_TABLES` holds `congress_bills` alone — elsewhere a fresh NULL is
   a real value. `test_the_narrow_writer_does_not_drop_the_familys_columns` is
   the behavioral pin: it seeds a 48-column prior, runs the narrow writer's own
   merge, and asserts both the published schema and that a family-only `stage`
   survives.

**2026-09-19, same branch, third part.** SpicyDocs 0.21.1 is released and
adopted: `vendor/spicy_docs-0.21.1-py3-none-any.whl` (commit `6f8d20e`, tag
v0.21.1, sha256 `c519e231…68a6`, 1,025,308 bytes, verified after the copy and
recorded identically in `uv.lock`), 0.21.0 deleted, both `source-readers` pins
and `[tool.uv.sources]` moved together. The resolve is clean in one line —
`Updated spicy-docs v0.21.0 -> v0.21.1` — with no refusal: 0.21.1 pins the same
`rulespec-artifacts==1.0.13` and the same DeltaTrack commit through its
`bill-diff` extra, so both fixes 0.21.0 needed still hold.

- [x] **The sealed body preference is the default now**, in both spellings:
  `bodies.BODY_PREFERENCE = ("xml", "htm", "txt", "pdf")` on
  `GovInfoBodyAcquirer.acquire` and `bill_versions.DEFAULT_FORMAT_PREFERENCE =
  ("xml", "html", "txt", "pdf")` on `choose_format`. `build_bill_family`'s local
  `BODY_PREFERENCE = ("xml", "txt")` is deleted and
  `build_committee_reports`' `prefer=("txt", "htm", "xml")` with it; no
  `acquire(..., prefer=...)` call remains in `src/`. This was not cosmetic:
  bills before the 113th Congress offer no XML at all
  ([`docs/research/pdf-only-corpus-2026-09-19.md`](../spicy-docs/docs/research/pdf-only-corpus-2026-09-19.md)),
  so the old local order yielded them no body whatsoever, and CRPT/CHRG had no
  PDF fallback.
- [x] **`extraction.body_text` owns the text derivation.** In the bill family,
  a printing fetched as PDF goes through it and the resulting
  `GpoCleanupRecord` fills `BillVersionCapture.cleanup` — the `cleanup_*`
  columns and `cleanup_json`, which `bill_version_tables._page_cleanup` already
  serializes including 0.21.1's new `running_footer_lines` and `content_lines`.
  In committee reports it replaces `_body_text`, which decoded UTF-8 with
  replacement, split on a form feed **no GovInfo body of any rendition
  contains**, and ran the PDF normalizer over raw HTML. `page_count` is NULL
  wherever the rendition states no page boundary, which is every HTML body;
  before, every report published `page_count = 1`, a count of the separator's
  absence.
- [x] **The bulk-listing skip**, which is limit 1 above, now filled. Each
  `bill-family` run retains every BILLSTATUS folder's own `BulkListingEntry`
  and the next passes it as `unchanged_since`, so an unmoved zip costs one
  small listing request instead of up to 32 MB. Skipping a folder loses
  nothing: an unmoved zip cannot hold a changed bill, and every bill in it
  would have matched its published `updateDateIncludingText` and been skipped
  anyway. Skipped folders are counted in the run log.
- [x] **Where the entries live: `bill_family_archives`**, a fourteenth rollup
  output written through `merge_table`, keyed `(congress, bill_type)`. Not a
  JSON manifest — `manifest.py` is a Bloom filter of source keys and cannot
  hold a structured per-folder row — and not a contract table, because nothing
  in `spicy_docs.schemas` shapes it; the dictionary documents it as this
  rollup's processing state, with its columns in `DERIVED_SCHEMAS` and its
  prose inline. Only the four fields `acquire` compares are retained; the other
  six the publisher's listing states are left empty rather than guessed at. A
  retained entry naming a different file is refused upstream on purpose; it is
  recognised here *before any request* — `bulk_status_locator` spells the zip's
  own name and link, and `read_bulk_listing` proves every entry against that
  same locator — so a stale row costs one listing read (the one that replaces
  it) rather than that plus the refused call's own, and cannot wedge the
  rollup. Checked rather than caught because the upstream refusal carries no
  listing to reuse.
- [x] **Columns with no home, stated rather than invented.**
  `BodyText.derivation` has none on `bill_versions`, `committee_reports` or
  `hearing_transcripts`, and review settled that it earns none: it is a pure
  function of the rendition through `RENDITION_DERIVATIONS`, a static
  four-entry table, so a column would restate `format` in a second vocabulary
  and could only ever disagree with it by going stale. The renditions actually
  read are logged instead. `rendition` needs no column either —
  `bill_versions.format_name` and the package tables' `format` already say
  which one, filled from the fetched body itself.
- [x] **The PDF branch needed a provider, and a test to find that out.**
  `body_text`'s default extractor is `DocumentExtractor(NativeText())`, whose
  default reader opens PDFs with **PyMuPDF** — which this repository did not
  install at the time, pinning the narrow `pdf-pypdf` provider instead.
  SpicyDocs' own tests inject a fake extractor, so nothing upstream exercises
  that default either. Wired as first written, every PDF body would have
  raised `ModuleNotFoundError` inside a per-package `except` and been logged
  as one more refusal: the `cleanup_*` columns would have stayed NULL forever
  and every PDF-only CRPT/CHRG package would have published no row, with no
  failure louder than a warning. `transforms/pdf_text.py::PypdfPageExtractor`
  adapted the pinned provider to the extractor seam — an `Extractor`, not a
  `DocumentReader`, because `PypdfReader.open` takes a password rather than a
  media type and `body_text` reads only each page's text — and both callers
  passed it. The two tests that found it failed without it.

  **Superseded the same day, fourth part below:** the pypdf adapter made the
  PDF branch reachable but not correct — pypdf's line layout defeats
  `gpo_normalize`'s own layout detector. `PypdfPageExtractor` is deleted and
  both callers now take `body_text`'s default (PyMuPDF) instead.
- [x] **Coverage statements.** `bill_versions`, `bill_sections`,
  `committee_reports`, `report_sections` and `hearing_transcripts` now name the
  rendition order, say that bills before the 113th Congress have no XML so the
  section tree is absent for them, and say why `page_count` is NULL on a
  non-PDF rendition. 46 tables, `check` and `generate` clean.

The stub in `tests/test_bill_family.py` reproduces `BulkStatusAcquirer`'s
`unchanged_since` contract rather than recording the argument — including
refusing a mismatched name with `BillSourceError`, the error the real acquirer
raises — so the second-run assertion is discriminating: the cold run still
downloads, a zip whose size moved still downloads, and only the unchanged
folder does not. The PDF-rendition tests derive a PDF-only BILLSTATUS from the
same fixture rather than committing a second one, and build real PDFs with
`tests/pdf_fixtures.make_pdf` (as of the fourth part below, `make_multiline_pdf`
for the cleanup-record test, so a genuine GPO gutter-numbered page is really
extracted, not merely accepted as non-null).

**[SR01](#sr01) was untouched by all of this — it landed separately, below.**
It owned the `congress_bills` reader replacement (adopting SpicyDocs'
`listing.py` in place of the local `CongressBillsReader`). This work shared
exactly one thing with it — the frozen ten-column prefix — which is precisely
why the design froze that prefix: either side could land first, and SR01 did,
after this work, without touching it.

**2026-09-19, branch `hosting-sr01` (off `billtrax-hosting-prep`, local only,
not pushed).** Closes spicy-docs gap D2
(`docs/research/closing-the-gaps-2026-09-19.md` in spicy-docs): retired
`sources/congress_bills.py`'s hand-rolled `offset`/`limit` HTTP walk in favour
of spicy-docs' `CongressListingReader` over the measured `bill` route
(`sort_honored=True`, sealed 2026-09-19 — see spicy-docs
`docs/sources/listings.md`). `CongressBillsReader` keeps every one of this
repo's own responsibilities: the fetch window from the prior table's
watermark minus the overlap, `MAX_WINDOW_DAYS`, `CONGRESS_SINCE`/
`CONGRESS_UNTIL`, and this repo's own api.data.gov env fallback chain
(`_resolve_api_key`) — now handed to the spicy-docs reader as a header
(`X-Api-Key`), never a query parameter. `_bill_id`, `_shape`, `_bounded_until`
and `build_congress_bills.py`'s ten-column `COLUMNS`/`merge_contract_table`
call are untouched, so the frozen prefix and the data dictionary's contract
digest do not move (`spicy-regs-dict check` reports no diff).

The hand-rolled paging (`_paginate`, `_get_page`, `_get`, `_pagination_count`,
`SORT_NEWEST_FIRST`, the manual retry/backoff loop) is deleted along with its
seven tests; the reader also refuses — surfacing spicy-docs' own behavior —
when the publisher's declared and observed row counts disagree, instead of
this repo's old "warn below a completeness tolerance" heuristic (the shape of
the 510-day freeze this reader once caused). Three tests replace the deleted
seven, hermetic over `httpx.MockTransport`: the walk delegates correctly and
yields raw dicts, an unset window sends no bound, and a declared/observed
mismatch raises. 18 tests now cover this file (was 22: "21 existing tests plus
the end-to-end merge test"); `test_build_congress_bills_merges_prior_and_fresh_rows`
is unchanged and still green. 1,429 source tests pass (was 1,433; net four
fewer, matching the seven removed against the three added) — `uv run --frozen
pytest -q`, `ruff check .`, `spicy-regs-dict check`, `ty check` (the one
pre-existing `tests/test_fec_relationships.py` diagnostic — gap E3 in
spicy-docs `docs/research/closing-the-gaps-2026-09-19.md` — is unrelated and
untouched). Not pushed — local commit on `hosting-sr01` only.

**Same day, follow-up: refuse-and-retry replaces warn-and-publish.** Review
before merging into `billtrax-hosting-prep` named an operational risk the
change above did not cover: the spicy-docs reader refuses on *any* declared-
count disagreement, not only a shortfall at the end, and a nightly window
that closes at "now" is walked over several minutes — long enough for the
publisher to edit a bill's `updateDate` past `toDateTime` mid-walk, shrinking
the declared count out from under a request already in flight. Unlike the
old reader's warn-below-a-tolerance heuristic, an unmitigated refusal there
would publish *nothing* for the whole night on a transient publisher-side
race, not a real truncation. The reader's refusal stays absolute — it is the
evidence rule, and weakening it would reopen the exact defect this change
closed — but the *policy* of what to do about a refusal belongs one layer up,
where the window is chosen: `build_congress_bills.py` gained `_fetch_bills`,
which asks a fresh `CongressBillsReader` for the identical window up to
`FETCH_ATTEMPTS` (3) times with a `RETRY_PAUSE_SECONDS` (30s) pause between
attempts, discarding each failed attempt's partial rows (a partially-yielded
generator cannot be resumed, and keeping its rows would be the same
truncated-table shape refuse-and-retry exists to prevent). A refusal that
survives all three attempts propagates unchanged: the run publishes nothing
and fails loudly, exactly as a first-attempt refusal always did — retrying
buys tolerance for a drift that clears within a few attempts, not a license
to ever publish a window this repo did not walk in full. Both module
docstrings (`sources/congress_bills.py` and `transforms/build_congress_bills.py`)
now state this trade-off directly.

Two new tests pin the risk and the fix: `test_walk_refuses_when_the_declared_count_changes_mid_walk`
in `tests/test_congress_bills.py` is a two-page `httpx.MockTransport` fixture
where page two declares one fewer than page one, exercising the "declared
count changed during the traversal" refusal specifically (distinct from the
declared-vs-observed-at-the-end refusal already covered) — the exact shape
the retry exists for. `test_fetch_retries_a_walk_refusal_and_succeeds` and
`test_fetch_gives_up_after_max_attempts` use a stub reader whose `iter_records()`
raises on its first N constructions then succeeds, proving both that a
transient refusal is absorbed (first raises, second succeeds — the merged
table matches the stub's rows) and that a persistent one is not (three
attempts, then the original `PagedJsonSourceError` propagates from
`build_congress_bills()` itself). 21 tests now cover `tests/test_congress_bills.py`
(three more than the note above); 1,432 source tests pass. `ruff check .`,
`spicy-regs-dict check` (no diff) and `ty check` (same one pre-existing,
unrelated diagnostic) all still pass. Not pushed — local commit on
`hosting-sr01` only.

**Same day, two more nits before merge.** (A) `_fetch_bills`'s
`PagedJsonSourceError` import was still eager — it ran before
`CongressBillsReader(...)` even got a chance to short-circuit on a keyless
run, unlike `iter_records()`'s own discipline. Fixed by catching `Exception`
broadly and importing (then `isinstance`-checking, re-raising immediately if
it doesn't match) only inside the handler: Python never evaluates an
`except <Name>` clause's type at all when the `try` block doesn't raise, so
on the keyless happy path — the only path a base install without the
`source-readers` extra needs to survive — the import genuinely never runs
now. Verified directly: `_fetch_bills(None, None)` with no key set and
`spicy_docs` import blocked via a `sys.meta_path` hook still returns `[]`
without tripping the block. (B) The retry caught every
`PagedJsonSourceError`, not only the drift shape it exists for, so a
permanent refusal (malformed JSON, a bad date parameter, a 404) would have
burned two `RETRY_PAUSE_SECONDS` pauses under a "walk refused, retrying" log
line before failing anyway. Narrowed to retry only when the error's
`paged_json_acquisition` context carries both a `declaredCount` and an
`observedCount` — the two drift shapes — and re-raise everything else at
once. `test_fetch_does_not_retry_a_non_drift_refusal` pins it: a stub
refusal with neither count in its context propagates on exactly one
attempt. Both module docstrings restate the narrowed trade-off. 22 tests now
cover `tests/test_congress_bills.py`; 1,433 source tests pass;
`spicy-regs-dict check` still reports no diff (digest unchanged) and `ty
check` still shows only the same one pre-existing, unrelated diagnostic. Not
pushed — local commit on `hosting-sr01` only.

**Not wired, with the reason:**

- **Senate roll calls.** ~~`listing.py` has a `house-vote` route and no Senate
  equivalent, and the Senate LIS menu is not a reader this repository has. No
  Senate row is published rather than one with a NULL tally.~~ — **wired
  2026-09-21 (9c58095): the Senate LIS menu now enumerates Senate votes and
  the rollup acquires them under the shared cap; see the A3 amendment below.** ~~**`recordedVotes`
  as a second vote-linkage source.**~~ and ~~**`press_releases.bill_id`**,
  **`committee_reports.bill_id` / `hearing_transcripts.bill_id`**~~ — **all
  three wired 2026-09-19, fifth part below.** `hearing_transcripts.bill_id`
  stays NULL in practice, but by measurement now rather than by omission.
- **Coverage statements are honest, not flattering.** Sixteen of the new
  entries open "Sampled" — a fifth `COVERAGE_KINDS` entry added for this —
  because no run has been measured. Promoting one to "Window" or "True range"
  is a measurement, not an edit.

<a id="sr01"></a>

- [x] **SR01 — Select SpicyRegs capabilities to reuse and local duplication to remove.**
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

  **Closed 2026-09-19, branch `hosting-sr01`.** The candidate spicy-docs'
  gap register names under this label — `docs/research/closing-the-gaps-2026-09-19.md`
  gap D2, "spicy-regs's hand-rolled `congress_bills` reader still exists beside
  `listing.py`" — is decided KEEP/SHARE: keep this repo's window, watermark,
  env-key-resolution and frozen-shape responsibilities; share the walk itself
  by adopting `CongressListingReader` over spicy-docs' measured `bill` route.
  [SR03](#sr03) is the follow-on for implementing it, and this same change
  *is* that implementation — decision and implementation landed together for
  this one candidate, the same pattern [SR04](#sr04) set for CourtListener.
  See the narrative entry above (in "BillTrax-derived table hosting") for the
  specifics: files, tests removed/added, the unchanged contract digest, and
  the same-day follow-up that added a bounded retry around the reader's
  now-absolute refusal so a mid-walk publisher drift on a nightly window
  cannot fail a run the old heuristic would merely have warned about.
  CourtListener handling was already decided and implemented under SR04; the
  remaining named candidates (strict parsers, table/Iceberg publication,
  documented-value diagnostics, public imports and optional dependencies)
  were not reviewed by this change and stay open under SR03 if picked up.

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

  **First landed instance: the [SR01](#sr01) congress_bills decision, branch
  `hosting-sr01`, 2026-09-19** — `CongressBillsReader` now adopts spicy-docs'
  `CongressListingReader`; the replaced hand-rolled offset/limit walk and its
  seven tests are removed. This repo's fixtures cover it directly
  (`tests/test_congress_bills.py`, hermetic over `httpx.MockTransport`); no
  separate installed-wheel handoff doc was needed since spicy-docs 0.21.1 was
  already the pinned `source-readers` extra. Any further SR01 candidate that
  gets a KEEP/SHARE decision lands here the same way.

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

**2026-09-19, same branch, fourth part.** The pypdf adapter from the third
part made the GovInfo PDF branch *reachable*; it did not make it correct.
SpicyDocs `docs/research/gpo-normalizer-vs-upstream-2026-09-19.md` measured
both pipelines against real GovInfo PDFs and found `gpo_normalize.py`'s
layout detector never fires on pypdf's output on any of the five documents
measured, including the two that are genuinely GPO gutter-numbered: pypdf
glues the margin number onto its content line (`Representa-1`), while
PyMuPDF — the pipeline the normalizer was actually derived on — emits it as
its own physical line immediately after, the adjacency the detector looks
for. Under pypdf, three of the normalizer's rules are structurally
unreachable on every PDF-derived GovInfo body: layout detection, hyphen
rejoin (0 of 6, then 0 of 12, real gutter numbers rejoined, against 6 of 6
and 12 of 12 under PyMuPDF on the same two documents), and the
gutter-evidenced half of the bare-digit strip gate. The user's ruling: results
decide this, not licensing.

- [x] **`PypdfPageExtractor` deleted.** Both `build_bill_family.py` and
  `build_committee_reports.py` call `body_text(package)` with no `extractor=`
  argument, so the PDF branch now runs through `body_text`'s own default
  (`DocumentExtractor(NativeText())`, PyMuPDF) — the pipeline `gpo_normalize`
  was derived on and the one the hosted `cleanup_*` columns describe.
  `transforms/pdf_text.py::PYPDF_VERSION` stays: `extract_pdf_text`, the
  unrelated regulations.gov attachment-text path, still uses it.
- [x] **`pyproject.toml`.** Both `source-readers` pins move to
  `spicy-docs[acquisition,pdf,pdf-pypdf,bill-diff]==0.21.1` — `pdf` adds
  PyMuPDF for the body path above; `pdf-pypdf` stays only because
  `extract_pdf_text` still needs it. `uv lock` resolved clean in one line
  (`Added pymupdf v1.28.2`); Pillow was already present as a transitive
  dependency, so nothing else moved. `uv sync --frozen` installed exactly
  `pymupdf==1.28.2`.
- [x] **Tests.** `test_the_pdf_cleanup_record_reaches_the_cleanup_columns`
  (`tests/test_bill_family.py`) now asserts `cleanup_line_numbers == "true"`
  and `cleanup_hyphen_rejoins == "2"` rather than merely non-null, so a future
  extractor swap that defeats the normalizer fails loudly. It runs against
  `PDF_GPO_PAGES`, a genuinely gutter-numbered synthetic PDF
  (`tests/pdf_fixtures.make_multiline_pdf`, new — writes several real physical
  lines per page instead of `make_pdf`'s one `Tj` per page), not a live-fetched
  real fixture: spicy-docs' own `tests/fixtures/gpo_pdf_text/README.md`
  provenance has no document that is both under 200 KB and gutter-numbered —
  its two sub-200 KB documents (ENR at 196,785 bytes, the committee report at
  199,803 bytes) are never GPO line-numbered by GPO's own print convention,
  and its two gutter-numbered documents (223,439 and 206,513 bytes) both
  exceed 200 KB. `test_the_pdf_fallback_is_extracted_and_states_its_page_count`
  (`tests/test_committee_reports.py`) needed no content change, only a
  docstring update — it asserts page count and format, both extractor-agnostic.
- [x] **`vendor/README.md` and this file.** Both corrected: the PDF body path
  was never "this repository deliberately does not install PyMuPDF" as an
  ongoing design choice; it now does, for this reason.

1,433 source tests pass (`uv run --frozen pytest -q`). Not pushed — local
commit on `billtrax-hosting-prep` only, per instruction.

**2026-09-19, `hosting-adopt-0.21.2`, fifth part.** SpicyDocs 0.21.2 is
released and adopted: `vendor/spicy_docs-0.21.2-py3-none-any.whl` (commit
`3642aa1`, tag v0.21.2, sha256 `96eb4897…8ca3`, 1,152,040 bytes, verified
after the copy and recorded identically in `uv.lock`), 0.21.1 deleted, both
`source-readers` pins and `[tool.uv.sources]` moved together. The resolve is
one line — `Updated spicy-docs v0.21.1 -> v0.21.2` — with no refusal; the
same `rulespec-artifacts==1.0.13` and DeltaTrack pins hold. `vendor/README.md`
lists what the release carries and what this repository reads of it.

- [x] **`build_committee_reports.py` reads the MODS bills through spicy-docs.**
  The interim `_mods_bills`/`_primary_bill` parse (the A2 review finding
  below) is deleted: `package.mods.bills` and `package.mods.primary_bill`
  (`PackageModsIdentity`, `ModsBill`) come from the parse the acquirer already
  ran to prove the package. What remains here is one function, the key
  spelling — `natural_key(congress, normalized_bill_type, number)` — which
  declines to link a bill whose MODS `type` is outside `BILL_TYPES` rather
  than publish a key with a hole in it. A contextless `<bill>` arrives as
  `context=""` and counts as a mention, as before. The fixture-pinned
  behavior holds: `CRPT-119hrpt1` links `119-hres-53` (PRIMARY third of four)
  and `CHRG-119hhrg63127` links nothing and logs `'OTHER': 3`, `'BODY': 2`.
  The test stub builds `mods` with `validate_package_mods` over the fixture
  bytes — `bills` is a required field, so a hand-built identity no longer
  constructs, and the acquirer's own parse is the truer stub anyway. The
  `associated_bills_json` request stands, each entry keeping its `context`.
- [x] **Ten new contracts, none hosted.** `TABLE_CONTRACTS` is thirty-two:
  `house_communications`, `committee_meetings`, `record_issues`, `treaties`,
  `nominations`, `laws`, `law_code_sections`, `table3_records`, `committees`
  and `committee_assignments` are new, and no rollup here writes any of them.
  `data_dictionary.CONTRACT_TABLES` is a hand-enumerated tuple and
  `contract_schemas()` iterates it rather than the wheel, so a contract with
  no rollup never reaches `TABLES`, the MCP list or the catalog. The one place
  that assumed wheel == hosted was
  `test_every_hosted_table_is_registered_everywhere`; it now asserts hosted ⊆
  wheel and that the difference is exactly `UNHOSTED_CONTRACTS`, a named set,
  so the next wheel's new contract is a decision here rather than silence.
  `spicy-regs-dict check` passes (47 tables); `generate` moved exactly two
  hosted tables. `hearing_transcripts` appends `event_id` as its twentieth
  column — NULL here, since the transform does not read the Congress.gov
  hearing detail that states it. `congress_bills.statutes_at_large_cite`'s
  prose now says the host fills it by joining `laws` at merge time, which this
  repository does not do because it hosts no `laws` table; the column stays
  NULL and the sentence is the wheel's, printed verbatim. `catalog.json` gains
  the one column and its digest moves; the shape is unchanged, so
  `CATALOG_FORMAT_VERSION` stays at 3.
- [x] **Nothing else moved.** `CongressListRoute.single_record` and
  `paged_json.page()`/`pages()`'s `single_record` keyword are additive, and
  nothing here calls either positionally. `BODY_PREFERENCE` and
  `DEFAULT_FORMAT_PREFERENCE` gain `uslm` after `xml`; no caller here passes a
  preference, so both take the new default.

1,492 source tests pass, `ruff check .` and `spicy-regs-dict check` are clean.
`ty check` is clean on everything this branch touches; the two diagnostics it
reports in `vectordb/embed.py` appear only with the `embed` extra installed
(`uv sync --all-extras`) and are absent on the plain `uv sync --frozen`
checkout the gate has been run on. Not pushed — local commits on
`hosting-adopt-0.21.2`, per instruction.

**2026-09-19, branch `hosting-rollups-laws-rosters` (worktree
`spicy-regs-wt-laws`, from `billtrax-hosting-prep` at `f198b6e`).** Gaps A8
and A9 of spicy-docs' `closing-the-gaps-2026-09-19.md` §2: the spicy-regs side
of the five contracts the 0.21.2 wheel ships for them. Twenty-seven tables are
now hosted; the public surface is 54 tables. A sibling branch hosts the other
five 0.21.2 contracts (A5/A7/A10); every shared-registry edit here sits in an
`A8/A9` block at the end of its list so the two merge trivially.

- [x] **`laws` rollup** (`transforms/build_laws.py`, three outputs): the
  `law/{congress}` route walked whole per scoped Congress (108 for the 119th,
  one page), one row per `laws[]` entry through `shape_law`; the PLAW USLM
  file per law through `UslmAcquirer`, newest first under `MAX_USLM_PER_RUN`,
  with `uslm_outcome` saying why a citation is NULL — `unavailable` only for a
  `404`/`410` from the exact locator, `not_requested` for a cap or a transport
  failure, never absence from a failure. A law already `captured` is left
  standing unless its list row's `update_date` moved (no fresh row, so the
  row-wise merge keeps it); a held law the cap does not reach keeps its prior
  row; a `401`/`403` from any publisher aborts. `law_code_sections` from the
  OLRC classification index and each public-law-order session table it links
  for a scoped Congress (the code-order twin holds the same lines under
  colliding positions), each page replacing its session's rows;
  `table3_records` one act page per public law, oldest first under
  `MAX_TABLE3_PER_RUN`; a refused act is stepped past and three consecutive
  refusals (`TABLE3_STOP_AFTER`) are the lag, so it costs three requests a
  run and no act the table never serves blocks the ones behind it.
- [x] **`committee-rosters` rollup** (`transforms/build_committee_rosters.py`,
  two outputs): the `committee/{congress}` route walked whole with no `sort`;
  the detail folded through `shape_committee` newest `update_date` first under
  `MAX_DETAILS_PER_RUN`, a folded row left standing unless its list row moved,
  a held row never overwritten by a list-only one. `committee_assignments`
  from the House Clerk's `MemberData.xml` (its Congress proved by the reader)
  and the Senate's `cvc_member_data.xml` (states none; `congress_basis` =
  `caller`), current Congress only, each capture replacing its chamber's seats
  for that Congress through `table_merge.retire_prior_rows`, so a seat the file
  no longer lists is gone; an earlier Congress keeps its last capture.
- [x] **The route over-declares, and it is recorded.** `committee/119`
  declared 238 and served 236 on its one terminal page (no continuation), which
  spicy-docs' reader refuses. `transforms/congress_walk.py::walk_route` reads
  past exactly that terminal-page count refusal — matched on the reader's
  traversal context and its observed count equalling what was served — logs
  both numbers at WARNING, and lets every other refusal fail the run. The
  `ListingSource` Protocol the amendments and roll-call transforms each carried
  moved there too.
- [x] **The statutes join landed.** `congress_bills.statutes_at_large_cite`
  is filled at that table's merge, as its column sentence promises:
  `merge_contract_table` calls `fill_statutes_at_large_cite` after the
  coalescing merge (`COALESCED_TABLES` unchanged), LEFT-joining the published
  `laws` on `bill_id` and taking the law's citation where one is published,
  keeping the one already there where `laws` has none — nothing is ever
  cleared, since `laws` never publishes a captured citation as NULL. Both
  writers (`congress-bills`, `bill-family`) declare `laws.parquet` in
  `soft_inputs`, and the laws cron (01:00 UTC) fires before both (02:00,
  20:15). The `congress_bills` dictionary clause saying this host does not
  join is replaced by what it now does.
- [x] **Registries, dictionary, docs.** `CONTRACT_TABLES` (+5, so `TABLES`
  and `MCP_QUERYABLE` follow), `mcp_server.TABLES` (+5, literal),
  `descriptions.yaml` (five entries, coverage "Sampled. None yet; the first
  run fills it" until D1 measures), `mkdocs.yml` nav, two `run-rollup-*`
  scripts, two workflows (01:00 and 01:40 UTC), `UNHOSTED_CONTRACTS` down to
  the sibling's five, `HOSTED_ROLLUPS` +2. `spicy-regs-dict check` passes (54
  tables); `generate` wrote exactly the five new pages, `congress_bills.md`
  and the catalog.
- [x] **Proof — receipt
  `~/Work/corpora/supply-2026-09-02/receipts/rollups-laws-rosters-2026-09-19/`.**
  One capped live run per rollup, cold start, scope 119, every acquirer at
  `max_requests=1` so no retry could exceed the budget, request lists per
  acquirer (method, scrubbed URL, status) in the manifests: **laws** 1 keyed
  and 8 keyless — the PLAW cap of 4, newest first, met exactly the four
  lagging laws (119-103, -104, -109, -110, all `404` → `unavailable`), 104
  `not_requested`; both session tables (583 and 3,049 rows, the 1st session's
  a new count); Table III `119-1`, 8 records. **rosters** 4 keyed and 2
  keyless — 236 of 238 declared, 3 details folded, 2,516 House and 450 Senate
  seats over 532 members. Five keyed and ten keyless in all, against the
  brief's 60 and 10. Tests run on byte-identical copies of the wheel's own
  fixtures (`tests/fixtures/congress_laws/`, `congress_rosters/`, each README
  pinning the spicy-docs commit and digest), never re-fetched;
  `verify_credentials.py` counts the key across the receipt, the staged diff
  and untracked files.

1,563 tests pass; `ruff check .`, `spicy-regs-dict check` clean; `ty check`
clean but for the two pre-existing `vectordb/embed.py` diagnostics under
`--all-extras`. Not pushed, per instruction. Limits stated in the dictionary
rather than hidden: a Table III page is read once (a later status edit is
not picked up); a folded committee's counts are that day's, re-read only when
its list row moves; the classification index links the current Congress
only, so an earlier Congress gets no `law_code_sections`.

Merged `billtrax-hosting-prep` at `a431fef` (the A5/A7/A10 index rollups)
into this branch: ten mechanical conflicts, every registry keeping both
blocks, `descriptions.yaml` by hand, the catalog and its digest regenerated
rather than merged, `UNHOSTED_CONTRACTS` the empty set with the exact-equality
test kept. The index transform's own `ListingSource` copy is deleted in favour
of `congress_walk.ListingSource`. All thirty-two contracts are hosted; the
public surface is 59 tables.

**2026-09-19, branch `hosting-d1-measured` (worktree `spicy-regs-wt-d1`).**
Gap row **D1** of the
[spicy-docs gap register](../spicy-docs/docs/research/closing-the-gaps-2026-09-19.md):
one measured run per hosted rollup against the real publishers, under the
stated caps, local output only. Receipt:
`~/Work/corpora/supply-2026-09-02/receipts/d1-measured-run-2026-09-19/`
(README, `summary.md` generated from the manifests, per-rollup request log,
hour ledger, output digests). Thirteen rollups, 36 tables. The completed runs
cost **4,148 keyed and 1,452 keyless** publisher requests; the night as a whole
asked for **5,348 keyed and 2,054 keyless**, the difference being the bill
family's crashed first attempt (finding 1 below), and **the worst rolling hour
was 3,680 against the 4,000 ceiling**, which answers **D5**.

Read the margin rather than the verdict: **320 requests of headroom, and
2,400 of that hour was the bill family run twice.** A single clean
bill-family run leaves the hour at 2,480; the 3,680 figure exists because a
crash cost a full 1,200-request budget and the re-run landed inside the same
hour. The ceiling was cleared, but not comfortably, and it would not be
cleared by a night that added a second Congress to the scope or that lost two
runs instead of one. D5's own question -- whether the family's eight archive
listing reads are worth worrying about -- is answered no: they are 8 keyless
requests of 7,446. The register's "stop here unless it exceeds the cap" is
satisfied; the headroom is the thing to watch, not the archive reads.

- [x] **No cap was reduced.** The bill family's `MAX_VERSION_FETCHES` (600) is
  documented as three requests per printing, which would breach the
  1,500-keyed threshold the brief set — but the measured split is **2 keyed +
  1 keyless** per GovInfo package (`api.govinfo.gov` summary and MODS are
  keyed, the `www.govinfo.gov` body is not), so 600 printings is 1,200 keyed.
  Confirmed on `committee-reports` before the family ran and again by the
  family landing on exactly 1,200.
- [x] **Uploads impossible rather than skipped.** Every run started with no
  `R2_ACCESS_KEY_ID`/`R2_SECRET_ACCESS_KEY`, and the runner refuses to start if
  either is set; the transforms were called directly, so `RollupPipeline.run()`'s
  publish step was never reached. `R2_PUBLIC_URL` was set, because the prior
  table is read over it with a plain public GET — that read is what makes
  "size before" a number. Only `congress_bills` existed (419,565 rows, 10
  columns); the other 35 tables answered `404`, recorded as never-published
  rather than as zero (`prior-tables.json`).
- [x] **`descriptions.yaml` carries the numbers.** All 26 "no run has been
  measured yet" / "none yet; the first run fills it" statements are replaced
  with the run's figures, and publisher findings went into `data_quality`.
  `measured_on` was already `2026-09-19` and the run is that date, so most
  values do not move — what moves is what the field now means: the day a run
  checked the sentence, not the day someone wrote it. Regenerated; `generate`
  is idempotent (second run changes no byte).

**Findings the run turned up, in the order they cost something:**

1. **A `Private Law` printing killed the whole bill-family run.**
   `version_slug("Private Law")` resolves to `private-law` and `govinfo_suffix`
   refuses it (GovInfo publishes private laws as PLAW `pvtl`, not as a BILLS
   printing). In `_version_captures` that derivation sat *outside* the
   per-printing refusal boundary the function already keeps around the fetch,
   so the first attempt lost all seventeen outputs after 532 s and 1,802
   requests. The wheel answers the same refusal twice — in
   `BillVersionCapture.version_code_is_reprint_ambiguous`, whose docstring says
   in as many words "rather than left to abort a whole bill over one
   unrecognised printing", and in `_sorted_versions`. This repository was the
   third place and the only one that aborted. **Fixed here**, minimally: the
   refusal is caught, the printing still gets its row from the publisher's own
   facts, `package_id` stays `None` (which the capture's type already allows)
   and it is not fetched and does not spend the budget. Regression test
   mutation-checked. Two bills of the 119th carry such a printing
   (`119-hr-3377`, `119-hr-7194`) out of 18,956.
2. **OLRC Table III answers `200` and then truncates the body.**
   `uscode.house.gov` returns a clean status line and headers, then closes the
   connection mid-chunk (`RemoteProtocolError: peer closed connection without
   sending complete message body`). Intermittent per page, not per act
   (`119_1` and `119_4` were served whole in the same run). **Re-derived and
   retained**, not asserted: `scripts/table3_rederive.py` in the receipt
   re-asks the five failing acts with a plain `httpx.get` -- no acquirer, no
   retries -- *and one control act the same run read whole*, because five
   failures on their own are equally consistent with the site being down. The
   five failed again; the control returned 63,493 bytes. It blocked 99 of the
   106 public acts (108 laws less the 2 private ones, which are never
   requested); the rollup's step-past and stop-after-three guards held, spending
   25 OLRC requests instead of 300, with 20 attempts on the five failing acts
   of which 15 were retries. Written into `table3_records`' `data_quality`.
   **A status-code check cannot see this** -- every one of those 20 attempts
   returned a clean `200` status line -- worth remembering wherever a `200` is
   treated as a record.
3. **The CHRG per-run cap is spent on packages that cannot become rows.** 187
   of the 200 packages the `collections/CHRG` window served are SERIALSET ids
   the body grammar refuses, leaving 13 `hearing_transcripts`. The CRPT side
   loses 28 of 133 the same way and to MODS `accessId` mismatches. The window
   advances slowly for that reason, not because few hearings are published —
   a cheap pre-filter on the collection listing is the obvious next move.
4. **Seven committee meetings state a chamber the detail route will not
   spell**, so they stay list-only permanently rather than transiently.
5. **Page-boundary repeats on three more routes**: 12 of 4,975 communications,
   4 of 2,208 nominations, 50 of 7,066 amendments — the declared total counts
   entries, not records, as A11 found on the bill route.

**C1 (the model-backed modules, live for the first time): the adapter works
and the contract does not.** One `summarize_bill` call through
`transforms/model_call.py` and the real `GeminiClient` over one printing of the
fixture bill. `gemini-3.8-flash` answered in 4.45 s with 204 input and 213
output tokens — and `_read_answer` refused the answer.
`SUMMARY_PROMPT_TEMPLATE` asks for its three items in prose and **never names
the JSON keys**, while the adapter asks for `application/json`, so the model
chose its own: it returned `summary` (which matches), `affected_audience`
(reader wants `audience`) and `notable_provisions` (reader wants
`topThreeProvisions`). Every test stubs the call with the right keys, which is
why nothing caught it. **On this evidence a keyed production run publishes zero
`bill_summaries` rows, every bill refused**, and `diff_summaries` shares the
shape. The fix is spicy-docs' — either the sealed prompt names its keys (moving
`PROMPT_VERSION`) or the reader accepts the publisher-neutral spellings — and
sealing decisions are not this repository's, so it is recorded and not changed.
**Fixed and adopted, 2026-09-20 (spicy-docs 0.21.3, then 0.22.0).** The first
route was taken: each prompt now states the JSON object its reader parses,
from the one `AnswerField` declaration the reader reads, and 0.22.0 puts that
same declaration on the *request* as a JSON Schema — which is where BillTrax's
zod schemas sat, and the half this port had dropped. Two receipts, both
spicy-docs':

- `~/Work/corpora/supply-2026-09-02/receipts/c1-prompt-fix-2026-09-19/` — the
  summary prompts at `v2`. One `summarize_bill` call answered `summary`,
  `audience`, `topThreeProvisions` and was **read into one `bill_summaries`
  row** (250 in / 197 out, **USD 0.000567**); one `summarize_diff` call read
  into one `diff_summaries` row on a constructed pair.
- `~/Work/corpora/supply-2026-09-02/receipts/c1-classification-v3-2026-09-20/`
  — the classification prompt at `v3`, which closes the part the first receipt
  could not. Under `v2` two live runs were answered and refused: `sectionId`
  asked for "the bracketed id, copied exactly as given below" and the model
  copied the brackets, which `_read_row`'s batch guard refuses, so **zero
  `section_classifications` rows** both times. `v3` rewords that one field.
  **Two identical requests, three rows each**, no refusals, ids as sent, 534
  in / 200 out, **USD 0.00066**. Two calls because one success does not
  establish a reliable route.

So both defects were an unstated or ambiguously stated *request*, found only
by asking. `hosting-adopt-0.21.3`, extended to 0.22.0, adopts the wheels and
asserts each half here: the `v2` and `v3` stamps on produced rows, the
measured `v1`-era answer refused by name, and the diff reader on a changed
pair. **No keyed production run has been made in this repository**, so nothing
above about what a real corpus costs or yields is superseded.
Cost: **USD 0.00057-0.00059 per bill** -- a range, not a figure, because the
input is deterministic at 204 tokens and the output is not (208 / 213 / 206
across three calls). The rate is pinned in the receipt and is a pin, not a
measurement; the tokens are the publisher's own `usageMetadata`. The cost is
recorded whether or not the reader accepted the answer, which on this evidence
it does not -- a refused answer is still a billed call, and the receipt's
arithmetic used to sit in the success branch where it would have reported
nothing for the outcome that actually happened.

**What one run cannot establish.** Every table but `congress_bills` was a cold
start, so the incremental paths — skip-what-is-held, the watermark windows, the
BILLSTATUS unchanged-zip skip that `bill_family_archives` exists for — were
exercised only in their cold-start branch. **The second run is the one that
measures the skip**, and it is the cheapest useful measurement still
outstanding here.


**2026-09-19, branch `hosting-rollups-index` (worktree `spicy-regs-wt-index`),
sixth part.** Gaps A5, A7 and A10 of the
[spicy-docs gap register](../spicy-docs/docs/research/closing-the-gaps-2026-09-19.md):
the five Congress.gov index contracts 0.21.2 shipped unhosted are hosted, and
the two joins the register left to this repository are made. Fifty-four
tables; the sibling branch hosting A8/A9 (`laws`, `law_code_sections`,
`table3_records`, `committees`, `committee_assignments`) edits the same
registries in its own trailing block. Built by a subagent; review pending.

- [x] **Five rollups over one walk.** `transforms/build_congress_index.py::build_index_table`
  serves five `IndexSpec` entries (`house_communications`, `committee_meetings`,
  `record_issues`, `treaties`, `nominations`); the rollups are five classes in
  `pipelines/rollups/congress_index.py`, each with its own console script and
  06:00–06:40 UTC cron, so a refusal on one route fails one table's run. Every
  run walks the **whole** list (none of the five routes honours `sort`, and a
  date window is measured only on two of them), collapses the publisher's
  repeats across page boundaries by identity, and reads details only for rows
  the published table does not hold: the contract's own NULL-versus-`[]` rule
  is the resume marker (`committees_json`, `sections_json`, `titles_json`),
  with `update_date` as the stamp. Newest first, `MAX_DETAILS_PER_RUN` (1,000)
  a run; a new row beyond the cap is indexed list-only now and read next run;
  a held row at a stale stamp keeps its detail until re-read, because a
  list-only replacement would erase it. `401`/`403` aborts; a `404`, a
  malformed page, a transport failure or a detail naming another record is
  that row's refusal, counted and retried. The RIN is
  `rin_from_report_nature` at shape time on the detail only. A partitioned
  treaty is list-only by construction (no suffixed route in `LIST_ROUTES`).
  `record_issues` walks by session volume, `year - 1854`
  (`congress_scope.record_volumes`, pinned by 172 = 2026).
- [x] **A5's join: `federal_register.rin`**, the first RIN of
  `regulation_id_numbers_json`, derived in the merge for every row (so the
  696,679 pre-ingest rows and the prior's width are filled on the next run,
  not left NULL). Measured on the public table: 96,060 one-RIN documents,
  1,499 with two or more (max 41). The FR transform's merge is still its own
  copy of `merge_table`'s SQL; folding it in would need a computed-column
  hook for one caller, so it was left as the smaller change.
- [x] **A7's join: `hearing_transcripts.event_id`**, read by
  `build_committee_reports` from the Congress.gov `hearing-detail` route
  through the shared `listing_reader`, one keyed request per CHRG package
  fetched in the run, the detail proven to name the jacket asked for; three
  outcomes counted apart (meeting, no meeting, refused). Only packages fetched
  in the run are asked about; earlier rows keep NULL until re-fetched, and
  the dictionary says so.
- [x] **Proof — receipt `~/Work/corpora/supply-2026-09-02/receipts/rollups-index-2026-09-19/`:**
  one live run per rollup through the real reader at a cap of three details,
  scoped to the 119th: 56 keyed requests (1 spent on a receipt-transport bug,
  55 productive), every walk's declared count equal to its walked count
  (4,975 communications on 20 pages, 2,754 meetings across all chambers on
  12, 2,208 nominations on 9, 219 + 144 Record issues, 2 treaties), 15 and 4
  publisher repeats collapsed, and the resume assumption checked: the
  detail's `updateDate` equalled the list row's on 11 of 11. The retained
  pages are the fixtures in `tests/fixtures/congress_index/`
  (`tests/test_congress_index.py`); the hearing fixtures are copies of two
  earlier captures (`tests/fixtures/congress_hearings/`). Coverage statements
  say "none yet; the first run fills it" until D1.

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

**2026-09-19, same branch, fifth part.** The three linkage gaps the
[spicy-docs gap register](../spicy-docs/docs/research/closing-the-gaps-2026-09-19.md)
lists as A1, A2 and A3 — the columns whose contracts exist and whose seams
nothing drove. All three are now driven, with 1,458 tests passing and 47
hosted tables. None of the three added a publisher request to any run.

- [x] **A1 — `press_releases.bill_id` and its three match columns.**
  `build_press_releases` reads the published `congress_bills` table for the
  Congresses in scope, compiles one pattern per bill through
  `release_matching.compile_bill_patterns`, and runs `match_releases` over the
  run's items. Three row states stay distinct and the dictionary says so: a
  match; `match_rule = unmatched`, meaning the pass ran and named nothing; and
  an all-NULL `match_rule`, meaning no bills were published so no pass ran.
  `tests/test_press_releases.py` runs both captured feeds end to end — the
  House feed's four bill-naming items match on `title`, and the Senate branch
  is reached by editing one captured title, because as captured no Senate title
  names a bill and the Senate feed carries no description at all, so a Senate
  match can only ever be a title match.

- [x] **A2 — `committee_reports.bill_id` and `hearing_transcripts.bill_id`,
  from the MODS already in hand.** The gap doc proposed walking the
  Congress.gov `committee-report/{congress}` list route and indexing each
  report's CRPT package id from its text-format URL stem. Measuring first
  found a cheaper and stricter source: the package's **own MODS**, which
  `GovInfoBodyAcquirer` already fetches to prove identity before any body byte,
  carries `<extension><bill congress type number context/>`, and `context`
  states *how* the package relates to the bill. Only `PRIMARY` fills the
  column. Measured live on the twelve newest reports of the 119th, the MODS
  `PRIMARY` bill agreed with the route's own `associatedBill[0]` **12 of 12**,
  at zero additional requests against the route's one keyed detail call per
  report. First-listed would have been wrong: `CRPT-119hrpt1` lists S. 5 before
  the H. Res. 53 it actually accompanies.

  **Both measurements are retained**, 146 requests with every raw response, at
  `~/Work/corpora/supply-2026-09-02/receipts/report-bill-linkage-2026-09-19/`.
  **The hearing figures were corrected twice, and the receipt is why.** An
  exploratory pass over a different slice had said 18 hearings carried
  mentions, "up to 25 on one", and 6 had a meeting. Re-running it under
  retention gave 11, 38 entries and 12. Then the retained rows showed the
  `hearing/119` route is not stably ordered across offsets and had served two
  hearings twice, so 52 fetches covered **50 distinct hearings** — counting
  packages rather than fetches gives the 10 and 31 above, recomputed offline
  from what was already retained rather than by asking again. The two
  load-bearing figures never moved at any stage: 12 of 12 agreement on reports,
  and no hearing stating a `PRIMARY` bill. That is the argument for a receipt —
  a count over a source with duplicates in it looks exactly like a correct one.

  **`hearing_transcripts.bill_id` is NULL by measurement, not omission.** Over
  50 distinct hearings of the 119th, **no CHRG MODS carried a `PRIMARY`
  bill**; 10 carried `BODY`/`COVER` mentions, 31 entries in all, and of the 12
  whose detail named a committee meeting, **none** of those meetings'
  `relatedItems.bills` named a bill. So the `hearing → meeting → bill` chain
  the gap doc names buys nothing at two keyed requests per hearing, and a
  mention is never promoted to a linkage — publishing H.R. 1 as the subject of
  a hearing that merely cites it is a guess dressed as a fact. The mentions are
  counted by context in the run log.

  **A column to request in spicy-docs.** The contract has no home for the
  non-`PRIMARY` mentions. If they are wanted, the ask is one column on both
  package tables — `associated_bills_json`, each entry carrying the bill key
  *and its `context`* — because a list of bare bill ids would lose the very
  distinction that makes `bill_id` trustworthy. Not added here: this repository
  does not restate a published shape it does not own.

- [x] **A3 — `recordedVotes` as the second vote linkage, and the contract
  question it turned on.** The bill family emits a fifteenth output,
  `bill_vote_references` (bill, chamber, congress, session, roll number, the
  action index, url, date, `full_action_name`, `observed_at`; keyed on all but
  the last three), written through `merge_table`. The `roll-call-votes` rollup
  reads that published table at merge time, indexes it **before** the
  `house-vote` listing's own references so a bill's own action wins a
  disagreement, and fills `bill_id`, `match_rule`, `match_action_index`,
  `match_url` and `conflict_count`.

  **The choice: read the published table.** The brief allowed either that or,
  if `pipelines/rollups/base.py` forbade it outright, a second walk of bill
  actions inside the votes rollup under a cap. The contract's *prose* did
  forbid it — "never another rollup's output" — but its stated *reason* is "so
  pipelines stay independently schedulable with no cross-pipeline race", and
  three landed rollups already read a published **ingest** rollup's output for
  exactly that reason, each recording it: `fr_docket_links`
  (`federal_register`), `bill_subjects` (`congress_bills`) and
  `org_committee_links` (`fec_committees`), whose docstring argues the general
  case — an ingest table has no upstream dependency inside this repository, so
  reading it is an ordering preference the crons already honour, not a race.
  The prose was written before those three and had gone stale. Re-deriving
  instead would mean re-acquiring every scoped bill's BILLSTATUS — up to ~52 MB
  of bulk zip per run — to recompute what the family parsed an hour earlier.
  So: read it, and **amend the contract paragraph to say what its own reason
  implies and what four rollups now do**, rather than add a fourth silent
  violation. A *derived* rollup's output stays forbidden. A linkage input is
  read best-effort and is deliberately **not** declared in `inputs`, because
  `inputs` fails the run on absence and a missing linkage must instead leave
  the columns NULL.

  **`conflict_count` was wrong and is now per roll call.** It had been
  `len(index.conflicts)` — one run-wide number stamped on every row, which says
  every roll call was contested whenever any one was. With a single source a
  conflict was nearly unreachable, so nothing exposed it; with two sources it
  is reachable, and the contract's own sentence ("how many later references
  disagreed with the one that won") is a fact about one vote.
  `test_a_conflict_is_counted_on_the_row_it_belongs_to` pins it.

  **What is fetched is the House half of the union, not the listing alone.**
  The fetch set is every House roll call in the index: the listing's own walk
  plus any House roll call a bill's action names that the listing has not
  indexed yet. That is a superset, never a subset, so the coverage claim holds
  and a vote reaching a bill's action first is published a cron early rather
  than missed; such a row is not partial, since the Clerk file is addressable
  from the roll-call key alone. ~~The Senate half is *not* fetched, and the
  asymmetry is the point: for the House the references can only add to a
  complete enumeration, while for the Senate there is no enumeration at all, so
  those rows would *be* whatever the scoped bills happened to reference — 6 of
  the 34 measured — a biased sample that would read as a Senate vote table.~~
  — **Superseded 2026-09-21 (9c58095): the Senate LIS menu landed, so the
  Senate now has a complete enumeration of its own and its rows are acquired
  under the shared cap. The House-superset argument above and the 6-of-34
  measurement stay as written; the asymmetry they justified is gone.**

  **The proof is the measured sample, not a synthetic one.** The 58
  `recordedVotes` entries spicy-docs read off 20 bills of the 119th are copied
  into `tests/fixtures/congress_votes/recorded-votes-119-sample.json` with
  provenance, and resolve to **34 distinct roll calls with zero conflicts**
  through this transform's own reading path. The 58-to-34 collapse is the
  publisher's shape — one roll call recorded on both the passage action and the
  motion to reconsider — which is why the table is keyed by action index.

**Two things done along the way, neither an A-item.**

- `table_merge.published_table` replaces the "download the prior table unless
  it is already on disk" idiom that four transforms spelled out and these two
  linkage joins would have made six.
- `ty check` is clean for the first time: the pre-existing
  `tests/test_fec_relationships.py` diagnostic (gap E3) is fixed by annotating
  the dict the case deliberately puts three value shapes into, and
  `build_press_releases` gained the narrow `PressReleaseSource` Protocol its
  four sibling transforms already have, so a hermetic stub is typeable.

**Fixtures added, all with provenance READMEs.** The two press-release feeds
and `mods-CRPT-119hrpt1.xml` are byte-for-byte copies from the pinned
spicy-docs v0.21.1, digests verified against the tag;
`mods-CHRG-119hhrg63127.xml` was captured for this work on 2026-09-19 with the
api.data.gov key sent only as `X-Api-Key`, and the capture refused to write any
body containing the key or an `api_key=` parameter.

Every check through the project's runner: `uv run --frozen pytest -q`, `uv run
--frozen ruff check .`, `uv run --frozen spicy-regs-dict check` (47 tables),
`uv run --frozen ty check` (clean). Not pushed — local commits on
`hosting-linkages` only, per instruction.

**Review round, same branch.** One blocker and nine smaller findings, all
applied on top of a merge of `billtrax-hosting-prep` (which had moved: SR01
landed).

- **The blocker: an empty bill scope published a false measurement.**
  `compile_bill_patterns([])` is `()`, which is truthy-distinct from `None`, so
  a run whose bill scope came back empty matched every release against zero
  patterns and published `match_rule = unmatched` — and because
  `press_releases` merges row-wise, that NULL `bill_id` overwrote the correct
  one an earlier run had published. The trigger was seasonal and would have
  been hard to attribute after the fact: the scope defaulted to the *current*
  Congress while a feed's two-month rotating window still carries December's
  releases through January, naming the outgoing Congress's bills. Fixed in both
  halves the review named. Each release is now scoped by **its own `pub_date`**
  through `congress_scope.current_congress` (which knows a Congress convenes on
  3 January), so both sides of the flip are served from one run; and a Congress
  with no published bills is **absent** from the pattern map rather than
  present with an empty tuple, so its releases keep NULL match columns instead
  of asserting a false `unmatched`. `test_a_release_is_scoped_by_its_own_publication_date`
  is the boundary case: one item moved to December 2026 and one to January
  2027, both naming H.R. 6500, with only the 119th published — the December one
  matches, the January one stays NULL.

  **A residual, stated rather than hidden.** A run that cannot read
  `congress_bills` at all still republishes its releases with NULL match
  columns, and row-wise merge means that erases a previously published
  `bill_id`. It is re-derived on the next successful run for any release still
  in the window, so the exposure is one cron cycle — but a release that rotates
  off in exactly that window keeps the NULL. Closing it properly means either
  coalescing this table or a merge that distinguishes "unknown" from "empty";
  coalescing would contradict `COALESCED_TABLES`' documented single meaning
  (two writers owning different column subsets), so it is not done here.

- **The fetch set was described wrongly.** The docstring and this file said the
  votes rollup fetches "exactly what the House listing names"; it fetches the
  House half of the union with the recorded-vote references, which my own test
  proves. Rewritten to say so, and to say why the House half was acceptable
  where the Senate half was not until the Senate LIS menu landed — see the A3
  entry above and its 9c58095 amendment.
- **`test_an_ingesting_rollup_reads_no_base_table` passed by construction.** It
  asserted `inputs == ()`, which both new readers satisfy while reading a
  published table. Rollups now declare a `soft_inputs` tuple, and the retargeted
  test holds each entry to the two properties that make the read safe: its
  writer is an ingest rollup, and the reader's cron fires after the writer's
  (both parsed from the workflows). Mutation-checked by moving
  `rollup-press-releases` to 01:20 — the test fails, as it should.
- **The MODS `PRIMARY` rule is interpretation in the wrong repository**, and it
  re-parses bytes the acquirer already parsed. spicy-docs is adding
  `PackageModsIdentity.bills` and `.primary_bill` from that same parse; the two
  local helpers now carry a comment naming what deletes them, and the column
  request is sharpened to `associated_bills_json` with each entry keeping its
  own `context` — bare ids would drop the very distinction that makes `bill_id`
  trustworthy. **Delivered in spicy-docs 0.21.2 and adopted — the fifth part of the
  hosting-prep log above deletes the two helpers.**
- **The 12-of-12 and 0-of-52 claims now have a retained receipt** (above), which
  corrected two restated figures in the process.
- Smaller: `_full_action_name`'s matching branch is covered by putting a
  `<fullActionName>` on one of the two synthetic recorded votes; both scoped
  reads carry an explicit `ORDER BY` so the first-wins tie-break is a stated
  rule; both feeds are captured **before** the bills table is downloaded, so an
  R2 refusal cannot cost a rotating-window capture that can never be retaken;
  the listing stub's docstring gave the wrong reason for its own fix (a
  duplicated record names the same bill and cannot conflict with itself — what
  it duplicates is a *genuine* disagreement, inflating `conflict_count`); and
  the digest-drift note above no longer blames this work for drift that
  `2eaffe0` and `473966f` had already caused.

**2026-09-20, branch `hosting-adopt-0.21.3` (worktree `spicy-regs-wt-adopt3`,
from `billtrax-hosting-prep` at `bc82c51`).** SpicyDocs 0.21.3 and then
0.22.0 are released and adopted on the one branch, which keeps its first
name. Vendored: `vendor/spicy_docs-0.22.0-py3-none-any.whl` (commit `b76a1f0`,
tag v0.22.0, sha256 `782735d8…bafa1`, 1,188,118 bytes, verified after the copy
and recorded identically in `uv.lock`), 0.21.3 deleted in turn, both
`source-readers` pins and `[tool.uv.sources]` moved together each time. Each
resolve is one line — `Updated spicy-docs v0.21.2 -> v0.21.3`, then `v0.21.3
-> v0.22.0` — with no refusal; the same `rulespec-artifacts==1.0.13` and
DeltaTrack pins hold throughout. Together the two releases close the C1
finding above.

- [x] **Nothing in the dictionary moved, across both releases, and that is a
  measurement.** The wheels were unzipped and diffed pairwise: 0.21.3 changed
  three `interpretation/` modules and added `schemas/document_capture/1.0/`
  (DocumentCapture v1 schemas and six capture profiles, registering no
  contract and unread here); 0.22.0 changed five modules and added
  `interpretation/gemini_call.py`, all of them the model seam. `TABLE_CONTRACTS`
  was then dumped from each wheel and compared field by field — thirty-two
  contracts, 617 columns, same columns, identity, version column, grain and
  per-column prose, **identical since 0.21.2**. So `UNHOSTED_CONTRACTS` stays
  the empty set with nothing to decide, `spicy-regs-dict check` (59 tables)
  passes, and `generate` leaves `docs/tables` and `data_dictionary` untouched
  — run twice, byte-identical the second time, so the empty diff is
  idempotence and not a first-run artefact. The dictionary is generated from
  the contracts, so checking it against them would have agreed with itself;
  the field-by-field dump is the independent half.
- [x] **The local Gemini adapter is deleted for spicy-docs'.** 0.22.0 gave
  `ModelCall` a `response_schema` argument that every generator passes, so
  `transforms/model_call.py`'s `call(*, model, prompt)` raised `TypeError` on
  every call — a copy that could not stay in step, which is the argument for
  not having had one. `spicy_docs.interpretation.gemini_call` replaces it, and
  the module keeps only `resolve_gemini_key`: which environment variable this
  host reads, the one part that is the application's. Its adapter tests went
  upstream with the code (`tests/test_interpretation_gemini_call.py`), where
  they sit beside what decides them; the key-resolution tests stayed and
  gained the two cases they were missing (the second variable, and an
  exported-but-empty one).
- [x] **The `survive_refused_answer` wrapper is deleted.** It existed because
  a `ModelCallError` escaped spicy-docs' `build_bill_family` and aborted the
  whole rollup; 0.22.0's `_model_answer` runs all three generators inside the
  guard the row shapers already ran inside and files a `FamilyRefusal` naming
  the reader's message, while a credential refusal and a transport failure
  still abort. The wrapper had to report a refused summary as a *declined*
  one — "its text is below the minimum" — and the diff path had the same
  misstatement; both are fixed upstream. Two review findings against that
  wrapper (a docstring claiming the classifier's caller files a refusal, which
  it did not; and its blast radius) are answered by the deletion.
  **The blast radius is real and now upstream's to state, which it does:**
  `classify_sections` batches at 30 and returns only when every batch has been
  read, so one refused answer discards the batches already paid for — up to 90
  answers on a five-batch printing. One `FamilyRefusal` is filed for the
  printing. Nothing here can narrow that without re-implementing the batching.
- [x] **Refusal reasons reach the run log.** The reason is why a
  `FamilyRefusal` carries one, and this rollup counted refusals by table and
  threw the reason away — the C1 defect would have read as "the model tables
  are short". Counted now by `(table, reason)` over `folded.refusals`, which
  also fixes a gap: the per-bill loop this replaces never counted the
  backfill pass's refusals at all, and `concat` carries them. Distinct reasons
  are logged with their counts, capped at `REFUSAL_REASONS_LOGGED` (20), so a
  prompt every bill refuses is one line rather than one per bill; reasons are
  scrubbed with the key before logging.
- [x] **All three readers are now run in a test here, which none was before.**
  Every model stub was checked: `tests/test_model_call.py` stubbed the *client*
  and asserted the adapter, and `tests/test_bill_family.py` ran the family
  keyless, so the three model tables were only ever asserted empty. No
  spicy-regs test had reached `_read_answer` — the same blind spot as "every
  test stubs the call with the right keys", one level out, and why the defect
  reached a live call. `StubGemini` now stands in for `GeminiClient`, behind
  `gemini_call.model_call` and all three readers, dispatching on
  `response_schema` compared against the three constants the generators send
  (exact where a prompt substring was not, and it fails if the schema stops
  reaching the request). **The answer keys in it are literal spellings on
  purpose**: derived from `field.key` the stub would silently follow a rename,
  and the test would be asking whether the file agrees with itself. Three
  tests — the `v2`/`v3` stamps on produced rows; the measured `v1`-era answer
  refused with both missing keys named while the run completes; and
  `summarize_diff`/`_read_diff_answer` on a pair with one section rewritten
  and one added, which had no coverage at all because the committed printings
  settle entirely `unchanged` and the generator declines before asking.
  Mutation-checked: the classification stamp fails with the installed
  `PROMPT_VERSION` forced to `v2`, and the diff test fails on the unchanged
  fixture pair.

1,615 source tests pass, `ruff check .` is clean, `spicy-regs-dict
check`/`generate` are clean and idempotent. `ty check` reports only the two
known `vectordb/embed.py` diagnostics, which appear solely with the `embed`
extra installed (`uv sync --frozen --all-extras`, used here) and are
pre-existing. The count is lower than the 1,623 of the 0.21.3 step because the
adapter tests moved upstream with the adapter. Not pushed — local commits on
`hosting-adopt-0.21.3`, per instruction. Reviewed once (REQUEST CHANGES on
prose and one coverage gap, no code defect); this extension answers all four
findings and supersedes the code two of them were about.

**2026-09-20, 0.23.0 adoption precedent** (`adopt-spicy-docs-0.23.0`,
from `billtrax-hosting-prep` at `2d13f81`). Vendored `73b10e5` / v0.23.0,
1,259,241 bytes, SHA-256 `36d619a6…4f66`; both pins, uv source and lock moved
as one change. Independent wheel and registry dumps proved 32 unchanged
contracts plus five new ones: 37 contracts, 766 columns, 64 hosted tables.
The procedure for the next adoption is the same: verify release bytes, compare
both registries independently, explicitly account for every contract, import
the package's shapes, assign one owner per acquisition pass, declare caps
before measuring, retain request ledgers and rows, and update coverage from
the resulting artifacts. Use the frozen project runner for every check and
generate twice to prove idempotence; local completion does not imply a push.

The two new owners were print-citations and senate-expenditures. Their receipt,
`rollups-pdf-families-2026-09-20/`, retains the initial HTML failure and the
subsequent PDF-first and resume measurements. Final counts were 41 activity
reports, 23 budget volumes, 12,700 actions, 49,792 citations and 3,272 Senate
expenditures; worst observed keyed hour 271. Resume skipped held work, and
tests pinned the Senate cap accounting and print-family interleaving fixes.
The 17 BUDGET packages outside the six-part grammar were the next package
request, resolved in 0.24.0 below. Checks: 1,683 tests, clean ruff and ty,
64-table dictionary and idempotent generation. No push.

**2026-09-20, spicy-docs 0.24.0 adoption** (`adopt-spicy-docs-0.24.0`,
this worktree, from `billtrax-hosting-prep`). Release `713c821` / v0.24.0:
1,319,390 bytes, SHA-256
`0a03ce34916cfedf4162356dd49e8dc4d95cb50e9dd280f4256c087d379f680a`, verified
before and after vendoring. Both pins and the uv source moved; 0.23.0 was
deleted and the lock refreshed. Independent registry dumps establish 39
contracts over 813 columns: two new tables and appended columns on three.
All 39 are registered, the unhosted set is empty, and 67 tables are described
(the two additions plus this host's committee_report_reads checkpoint).

- The package owns Mirrulations downloads, retries and access refusals. The
  temporary host guard required a nonblank string data.id before staging or
  manifesting a key. The 0.24.1 patch adopted below supplies that check for
  every ingested type, so both ingestion paths now use the supplier directly;
  unexpected objects remain unresolved with a named reason.
  Unresolved keys retry first with attempts carried forward. failed_keys.parquet
  restores from R2 and publishes before the manifest, including on zero-row
  passes and when recovery clears the last failure. Only this retry checkpoint
  is exempt from the data-size shrink guard. Legacy parse failures remain
  eligible even if the old manifest incorrectly marked them processed.
- Print selection and PDF preference now import the package rules. All thirteen
  measured BUDGET parts reach acquisition; a root-format refusal is counted as
  the publisher's answer and creates no volume or citation row.
- Committee reports own hearing_bill_links and the thirteen CBO letter columns.
  cover_links reads the MODS already fetched; read_cbo_estimate gates the letter
  on the cover recital. The own-output checkpoint distinguishes completed empty
  covers, pending work and failures, and queues old rows for enrichment once.
  Successfully evaluated hearings replace their prior relationship rows,
  including when the corrected cover has no links; unread or refused bodies
  keep their prior links.
- Bill family hosts build_bill_family's cbo_cost_estimates rows and the bill's
  outcome, including requested-empty. Old rows without that outcome invalidate
  the archive skip once. House communications retain the publisher source_route
  on every existing row. No new rollup reads another rollup's output.
  Successfully evaluated CBO lists replace each bill's prior estimate rows,
  including absent or empty lists. Unexpected list shapes retain prior rows.
  A credential refusal during printing acquisition aborts before any later
  printing capture, as the report, print-citation and communication paths do.

One measured run per changed rollup, with caps written first and never widened:

| Rollup | Declared work cap | Requests, keyed / keyless | Final local rows |
| --- | --- | --- | --- |
| print-citations | 40 packages; 160 HTTP | 38 / 8 | 41 activity reports, 29 budgets, 12,700 actions, 49,935 citations |
| committee-reports | 30 bodies per collection; 400 HTTP | 101 / 43 | 105 reports, 1,246 sections, 13 hearings, 0 links, 118 checkpoints |
| bill-family | 118 HR/S; 4 printing captures; 40 HTTP | 8 / 8 | 16,213 bills, 1,431 CBO estimates; all 18 outputs in receipt |
| house-communications | 119; 10 details; 60 HTTP | 30 / 0 | 4,969 communications, all publisher-route |

The receipt is
`/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/rollups-0-24-0-adoption-2026-09-20/`.
It retains caps, request ledgers, logs, output digests, row counts and source
pins. The combined worst observed hour was **177 keyed / 4,000**; unrelated
unlogged activity is not measured. The dictionary carries measured_on=2026-09-20.
The report pass evaluated 30 rows: four recitals, three letter spans, 26 absent
recitals; 75 remain NULL and pending. Thirteen acquired hearings stated zero
cover links; recall remains unmeasured. Bills state 1,368 populated and 14,845
requested-empty:absent outcomes, with no NULL outcome in the measured scope.

**Deferred and next:** agenda acquisition has cap zero because this pass lacks
a verified meeting-to-jacket join and House repository locator; no guessed
agenda URL is requested. Eleven budget roots offered no supported rendition
(TAB, DB, CLIMATE and LRB have no root PDF); the package's budget shaper accepts
package metadata, so granule acquisition is deferred. Seventy-five report
reads remain pending under the unchanged cap. Model calls were zero and only
four bill printings were acquired; the measured CBO index needs neither.
The next acquisition rollup is the **House communication Record backfill**,
about **11,000 GovInfo requests**, separately budgeted. Its official and agency
split remains NULL: held-out precision 85.3% and 88.4% missed the declared 90%
gate; the source sentence must remain beside the NULLs. The new dictionary
pages preserve that limit, per-source link precision and the recital gate.

Checks through the frozen runner: **1,718 passed, 3 deselected**, ruff and ty
clean, dictionary check **67 tables**, generation idempotent twice. Local
commits only; no push or upload. Credential audit over the branch diff and the
receipt: **0 matches**, including decoded Parquet values.

Review follow-up: all four findings fixed locally. Regression tests cover
identity-free Mirrulations objects in both ingestion paths, printing credential
refusals, unresolved history across fresh runners, and removal of corrected
hearing/CBO relationships. Frozen-runner checks: **1,748 passed, 3 deselected**,
ruff and ty clean, dictionary check **67 tables**, generation idempotent across
69 artifacts. Credential grep over the new diff: **0 matches** for the current
API_GOV value and credential patterns. No push or live acquisition run.

**2026-09-20, spicy-docs 0.24.1 patch adoption.** The current pin replaces
0.24.0 in both dependency lists and the uv source. The wheel is **1,321,932
bytes**, SHA-256
`ce25270b5328ccd4da4f51d2e241141531b4fb058313da38628c647f63531fa6`, verified
before and after copying. Independent imports from both wheel archives prove
all **39 contracts and 813 columns** identical, including column order,
identities, version columns and descriptions. Registry dumps and comparison:
`/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/spicy-docs-0-24-1-adoption-2026-09-20/`.

| Ingested type | Host identity | Supplier 0.24.1 requirement | Local guard |
| --- | --- | --- | --- |
| dockets | Nonblank string `data.id` -> `docket_id` | Same; also rejects `errors` | Deleted |
| documents | Nonblank string `data.id` -> `document_id` | Same; also rejects `errors` | Deleted |
| comments | Nonblank string `data.id` -> `comment_id` | Same; also rejects `errors` | Deleted in standard and chunked ingest |

No host-only identity requirement remains. The supplier's record definitions,
the host's extractors and the removed guard agree for all three types.
The regression suite retains the original empty-data, publisher-error,
blank-ID and numeric-ID reproductions, expands them to every ingested type,
and checks retry counts across two runs. Direct supplier-reader tests prove
the identity mapping, raw-field preservation and scrubbed publisher reasons,
including error bodies that also carry a valid ID.

Checks through `uv run --frozen`: **1,801 passed, 3 deselected**; `ruff check .`
and `ty check` clean; `spicy-regs-dict check` passes for **67 tables**;
`spicy-regs-dict generate` leaves all **69 artifacts byte-identical**.
Credential grep over the new binary diff: **0 matches** for the current
API_GOV value and credential patterns; the unpacked new wheel also contains
zero matches for that value. Local adoption only; no push or live acquisition.
