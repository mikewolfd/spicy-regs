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
uv run pytest -q                       # 1,230 passing on this tree (2026-09-17)
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
2. **The eight-table registration PR.** `feat/register-eight-tables`
   (`2854262`) is on the fork, unmerged, no PR, and based on the pre-squash
   history; rebase its one commit onto `main` first. It is unblocked only when
   the re-created #194/#195/#196 merge and their workflows publish the eight
   objects — check `materialized/rulemaking/latest.json` answers 200 before
   opening. Opening the PR is still Mike's call.
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

**Outstanding as of 2026-09-19: spicysearch must re-vendor.** Hosting the
BillTrax-derived tables took the catalog from 24 classes to 45 and
`congress_bills` from 10 columns to 48.
`spicysearch/vendor/spicy-regs-catalog-dictionary.json` is still the 24-class
copy (`01c77a4a…`, verified 2026-09-19) and no longer matches the 47-class
catalog this repository publishes. **The current digest is whatever
`data_dictionary/catalog.json.sha256` holds** — read the file; it moves
whenever the dictionary does, which is often, and a value quoted here is stale
by the next `generate`. This paragraph quoted one twice and was wrong both
times: it recorded `d08822d8…`, which `2eaffe0` had already moved by wiring the
sealed body preference and `473966f` again by correcting coverage statements;
the linkage work below moved it again by adding `bill_vote_references`; and the
commit that wrote "a note quoting one is stale as soon as it is written" quoted
one, which the very next `generate` in the same review round invalidated. The
commit chain is the durable part, because those are facts that do not move — a
consumer re-vendoring reads the file.
`CATALOG_FORMAT_VERSION` stays `3` on
purpose — no field changed shape, so a reader that only reads fields keeps
working — but the `kind` vocabulary gained a fifth value, `sampled`, which a
consumer branching on `kind` must handle before it re-vendors. This is a
consumer-side change in another repository and is not done here.
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

- **Senate roll calls.** `listing.py` has a `house-vote` route and no Senate
  equivalent, and the Senate LIS menu is not a reader this repository has. No
  Senate row is published rather than one with a NULL tally. ~~**`recordedVotes`
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
  from the roll-call key alone. The Senate half is *not* fetched, and the
  asymmetry is the point: for the House the references can only add to a
  complete enumeration, while for the Senate there is no enumeration at all, so
  those rows would *be* whatever the scoped bills happened to reference — 6 of
  the 34 measured — a biased sample that would read as a Senate vote table.

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
  proves. Rewritten to say so, and to say why the House half is acceptable
  where the Senate half is not — see the A3 entry above.
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
  trustworthy.
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
