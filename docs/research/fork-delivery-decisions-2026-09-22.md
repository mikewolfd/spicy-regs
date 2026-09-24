# Fork delivery decisions — September 22, 2026

Mike answered the open delivery questions in the [fork output ledger](fork-output-ledger-2026-09-21.md)
and [consolidated backlog](../fork-generation.md) on September 22, 2026. Each entry states
the question in terms of what users get, the decision, and the work it sets. The ledger owns
status and pins, the backlog the order of work, and the execution log the dated narrative
(decision 16); this record holds only decisions and how their meaning changed.
Decisions 14–29 were delegated on 2026-09-23 (below).

| # | Question | Decision | Consequence |
| --- | --- | --- | --- |
| 1 | Should people get bill history back to earlier Congresses, not just the current one? | **Publish the broad family, labelling inherited values.** | Accept the 11,021 URLs absent from raw BILLSTATUS but inherited from the same bill identity with proven lineage, provided each carries an inherited-provenance label. Reconcile the newer public 119th Congress rows into the 419,839-bill candidate, then publish. Unblocks the 18 T09 tables, T16 subjects and the narrow bill writer, and vote/press bill links. |
| 2 | Which term does a vote count toward on the day one Congress ends and the next begins? | **Half-open terms, with a unique inclusive-end fallback.** | Match `term_start <= vote_date < term_end`; only where that finds no term, accept an inclusive end if it yields exactly one term. On the frozen 1,573-vote selection this resolves 15 of 18 unmatched rows with no ambiguity (all 1,855 inclusive ambiguities fall on 2025-01-03); the remaining three `Not Voting` rows (`G000578` 2025-01-03, `S001157` twice on 2026-04-22) stay explicitly unmatched. Evidence: `votes-qualification/complete-member-join-replay.json`. |
| 3 | Should people be told which public tables are verified? | **Leave as is.** | No per-table verification status is added to the catalog or MCP metadata, and scheduled runs keep publishing before audit. The ledger remains the record of qualification. |
| 4 | How should we answer "how long does this agency take to finalize a rule?" | **Wait for source-linked rulemaking relationships.** | `rulemaking_lifecycles` stays unpublished; do not patch the earliest-proposal/earliest-final pairing. Derive timing from the T17 outputs (`proceedings`, `rule_targets`, …) once their five parents qualify. |
| 5 | What should people see when a member's committee is not in the committee list? | **Source-backed aliases.** | Retain the complete Congress.gov committee listing and details, map the 138 assignments across fourteen converted codes only through source-backed aliases (House select `hs`/`hl`, joint committees, `JSIK00`), and keep any still unresolved visible as not listed. Never match by name: the Joint Economic Committee has several codes. |
| 6 | How should people get the text of court opinions? | **Link out only.** | Do not build or host `court_opinion_bodies`; the ~109 GB build and its capacity plan are dropped. The published clusters' `cluster_id` and `absolute_url` are the route to text through CourtListener. No MCP fetch tool is added. Remove the output from the intended set and its ledger/backlog rows. |
| 7 | How complete must public comments be before improvements are published? | **Publish in cohorts.** | Publish the six-agency repair after independent review, then continue agency by agency, ACF next (129,052 comment originals, 380,321,010 bytes, fully listed). |
| 8 | Which court cases should the dockets table cover? | **Keep the APA-only selection.** | The first answer (opinion-linked dockets) was given without the earlier approval of the APA-899 scope recorded in `court-dockets-qualification/MANUAL-AUDIT.md`; asked again, Mike kept APA-only. The published 11,459-row selection stands, the full 71,677,647-row bulk set stays private, and clusters reach any case through `cl_docket_id` and `absolute_url`. |
| 9 | Do people need the parties and lawyers in each case? | **Fetch a bounded subset.** | Bulk exports omit party/attorney tables. Acquire them from the CourtListener API for an explicitly scoped subset with a token and retained responses. The subset is not yet chosen; the natural candidate is the published 11,459-row APA selection. PACER is not used. |
| 10 | Should contractor (SAM) and lobbying data come back? | **SAM first, then lobbying.** | Resume SAM through spicy-docs' bulk-extract path once source-specific access is confirmed, with a bounded retained initial load; then lobbying. The workflows already forward `SAM_API_KEY` and `LDA_API_KEY`, but neither secret is set on the fork yet. |
| 11 | What happens to the unpushed spicy-docs and spicy-regs commits? | **Pushed.** | spicy-docs `1dc7701..54ddbea` to `civictechdc/spicy-docs`; spicy-regs `4f78b32..0f038f2` to `mikewolfd/spicy-regs`. Scheduled runs now use that code. |
| 12 | How should the stalled daily docket refresh be fixed? | **Use a CourtListener token.** | The scheduled `rollup-courtlistener` run timed out on 2026-09-21 and 2026-09-22 under HTTP 429 retries. The workflow already forwards `COURTLISTENER_API_TOKEN`; the token from the workspace `.env` (`COURTLISTENER_API_KEY`) passed an authenticated request and is now the fork's secret. Local runs still need it exported under the `COURTLISTENER_API_TOKEN` name the reader uses. |
| 13 | Keep the 54.6 GB opinions original now that bodies are not built? | **Keep it.** | It is verified retained evidence; revisit only if disk becomes the blocker. |

## Found while acting on these

The vendored `vendor/spicy_docs-0.26.5-py3-none-any.whl` in this repository was rebuilt
with post-0.26.5 SpicyDocs code (it contains `spicy_docs/sources/sam_extract.py`) while
keeping the 0.26.5 label, so two different wheels now carry that name across the stack
(DocSpec, SpicySearch and Engine vendor the release built from `1dc7701`). Every
`spicy_regs.sources` and `spicy_regs.transforms` module imports against it; the next
SpicyDocs release should carry a new version and a recorded source commit.

Resolved the same day: SpicyDocs 0.26.6 (`6673fa3`) and Rulespec Artifacts 1.1.1 (`a3acb04`)
carry the changed code under new versions, with DocSpec 0.9.1 (`2cdde74`) and RefSpec
0.1.0.dev14 (`1e1d2d7`) rebuilt on them. This repository, DocSpec, RefSpec, SpicySearch and
Engine now vendor byte-identical copies, each recorded with its source commit; the
mislabelled 0.26.5 wheel stays here only for replay.

## Later changes

Status and pins for each consequence are in the ledger; these entries record only
what changed about a decision or its consequence.

- **Decision 12, 2026-09-22 (`0fcbc21`):** the token reaches only the
  `court_dockets` rollup. A free token's limits (10/minute, 100/hour, 250/day)
  cover the daily docket delta but not the cluster rollup's search catch-up,
  which stays keyless.
- **Decision 4, 2026-09-23:** a scheduled run had published `rulemaking_lifecycles`;
  it is withdrawn from the index and its workflow stays off. Agency timing now
  comes from the bootstrapped rulemaking dataset (`proceedings`,
  `comment_periods`); its docket-join and day-rule limits are addressed by
  decision 18.
- **Decision 6, 2026-09-23:** the opinion-body builder, rollup, workflow and
  registration are deleted; the push deleted the fork workflow.
- **Decision 5, 2026-09-23:** the House select aliases (`hs`/`hl`, from the native
  committee type and parent) are live. 28 seats stay unlisted for want of a
  publisher link, not by policy: 25 House seats on the Clerk's `EC00`, `IT00`,
  `JL00` and `JP00` (typed `joint`, with no Congress.gov code) and three Senate
  seats on `JSIK00` (no such committee on Congress.gov in the 118th or 119th).
  Nothing is matched by name.
- **Decision 2, 2026-09-23 (`c113329`):** implemented as the derived table
  `member_vote_terms`; `member_votes` is unchanged.
- **Decision 7, 2026-09-23:** the six-agency cohort and then ACF are published,
  each after independent review. The next cohort is the next agency.
- **Decision 1, 2026-09-23:** the reconciled family is published; every inherited
  URL carries `url_source = inherited` (SpicyDocs 0.28.0, adopted in spicy-regs
  `6d34a1b`).
- **Decision 10, 2026-09-23:** SAM's and then lobbying's bounded initial loads are
  published, and the fork has `SAM_API_KEY` and `LDA_API_KEY`. Both workflows
  were re-enabled after the push (decision 22). Local runs read `LDA_API_KEY`;
  the workspace `.env` names the key `LDA_KEY`.
- **Decision 11, 2026-09-23:** pushed as fast-forwards: spicy-regs
  `a9c794e..b89c7dc` to `mikewolfd/spicy-regs` and spicy-docs `dca01d4..1b0c6df`
  (through release 0.29.0) to `civictechdc/spicy-docs`. The seven workflows held
  for the push were re-enabled (decision 22). `cfr_sections` stays off until the
  CFR ancestry fix (SpicyDocs consolidation plan A8), and `rulemaking_lifecycles`
  by decision 4.

## Delegated decisions, 2026-09-23

Mike delegated the questions left open by the parsing survey and its validation to
an independent reviewer with no prior context, which decided each from the
evidence it was given and could read. Mike can overturn any of them. The rulings
refer to §5 of SpicyDocs' consolidation plan
(`spicy-docs/docs/research/consolidation-path-2026-09-22.md`), which carries the
items; the evidence is in `spicy-docs/docs/research/parsing-survey-2026-09-23.md`.

| # | Question | Decision | Why |
| --- | --- | --- | --- |
| 14 | Does spicysearch drop its citation grammar when RefSpec's moves into spicy-docs (plan B4)? | **No; only the data-side grammar moves.** | The plan's §11 keeps spicysearch's query grammar behind an adjudicated boundary test; `identifiers.py` detects shapes in query strings and deliberately refuses unlabelled tokens, so its differences from RefSpec are query-intent choices, not defects. |
| 15 | The plan's suggested ids SR05–SR08 collide with spicy-docs' SR01–SR15 series. | **Keep spicy-regs' own sequential series and name the namespace.** | spicy-regs' SR01–SR04 already share the letters; renumbering the new rows would imply one series. Cross-repo cites carry the repo name, as `build_lobbying_filings.py:59` does. |
| 16 | How should the fork docs stop drifting? | **One home per fact; qualified pins with a checking script; a units convention; the dated narrative in a versioned log; load-bearing numbers cite a script or receipt.** | Most validation errors were one fact repeated in several places, "live" claims overtaken by scheduled runs, and unit mix-ups. The log lives in `docs/research/` because the receipts directory is not versioned. |
| 17 | Ruling 5: `cfr_ref` for title 43's subpart-numbered sections. | **The printed citation, e.g. `43-1601.0-1`, with `part` 1600.** | `cfr_ref` is a join key, and the Federal Register side composes it from printed text, so only the printed spelling joins; NULL would drop 3,018 joinable rows. `part` carries the structure and `cfr_ref` the citation, and the column description says so. |
| 18 | Ruling 6: admit label-derived dockets and unpadded FR numbers in the rulemaking tables? | **Admit both, with one actor-id bump per table.** | Only 48,169 of 899,227 FR–docket links join today; label-aware reading roughly triples that, and unpadding resolves 40,340 of 49,403 misses with no ambiguity. Both read the publisher's own spellings. Use RefSpec's reader, delete the unused local copy, and keep unresolved links as rows. |
| 19 | Ruling 7: comment text provenance. | **Status `derived`, not `ok`; one Mirrulations tool per comment by a fixed preference order, recorded per attachment.** | `ok` means spicy-regs ran the extractor. Pick the order by one bounded measurement of how often tools overlap, and pin it as a policy constant. |
| 20 | Ruling 8: keep `catalog.json`? | **Keep only `table_metadata.json`.** | `catalog.json` is an exact projection of it, unpublished, and its only consumer is an unread vendored copy in spicysearch. Delete the file, its sidecar, the `catalog` subcommand, its tests and the vendored copy. |
| 21 | Ruling 9: who normalizes Federal Register and Regulations.gov search fields? | **spicysearch keeps deriving for now; spicy-docs is the destination after D3.** | The differences with DocSpec's stored values are staleness of an older build, not disagreement. Keep the agreement measurement as a drift check. |
| 22 | Re-enable the seven workflows held for the push? | **Yes, all seven.** | The only reason for disabling them, older code overwriting corrected tables, ended with the push. Re-enabled 2026-09-23. |
| 23 | Fix the logged-key defect now or wait for a spicy-docs route (plan A5)? | **Now, in spicy-regs.** | Send the key only in `X-Api-Key`, never log a URL carrying it, and abort on 401/403; the other Congress readers already do this. |
| 24 | The stale `fr-audit-2026-09-23/monthly-diff.json` receipt. | **Delete it.** | No retained script produces it and a recount confirms the ledger (393 months equal, 1,009,005 rows). The receipts directory is unversioned, so the deletion waited for Mike, who approved it on 2026-09-23; it is deleted. |
| 25 | The consolidation plan's §6 gate cannot pass as written. | **Compare only over the reference's keys.** | Restrict both `EXCEPT`s to the reference's (`document_number`, `publication_date`) keys and require zero rows; count generation-only keys, each of which must post-date 2026-09-14. Rebuilding the reference from the artifact under test would only check it against itself. |
| 26 | Ruling 10: DocSpec stores Regulations.gov deadlines as the UTC date, a day late. Move its policy now? | **No; correct the consumers now, DocSpec's policy with the Track D rebuild.** | Nothing downstream reads DocSpec's stored `commentCloseDate`: spicysearch reads the raw instant and Engine converts calendar bounds at UTC midnight, so the user-visible defect lives in the consumers. The preparer derives its day fields with the spicy-docs helper, "through X" filters on them, and spicy-regs' `comment_periods` imports the same helper. DocSpec's policy version moves once, in the rebuild D3/D4 force. |
| 27 | Ruling 11: which Federal Register document-number IRI space? | **RefSpec's rkaf spaces; delete `urn:spicy-regs:frdoc`.** | No published spicy-regs column carries the local space: the tables mint `urn:rkaf:us:rin:` and the canonical CFR IRI, both in full agreement with RefSpec, and the docs make the federalregister.gov URL the fallback identity. The only live emitter of the local prefix is rulespec-projection's copy, which nothing runs. One partner namespace is still to be named when the minter moves. |
| 28 | Ruling 12: where the IRI minters live. | **spicy-docs, beside the identifier shapes; not Rulespec Core.** | The minters are refusal logic over the shapes and a collision table, not format-only; a Core minter would either copy the shapes or take pre-validated strings, and adding `rulespec-conformance` (rdflib, pyshacl) to spicy-regs for an f-string is not justified. REF-024's mechanism (installed packages, never sibling trees) is met by the spicy-docs wheel; narrowing its "identity functions" clause so Core owns the lexical spaces and spicy-docs' tests assert its minters against them is a cross-product amendment Mike confirms. |
| 29 | Ruling 13: how the committee-report tables represent multi-part GovInfo reports. | **One row per part: identity `(package_id, part_id)` plus `part_number`; `committee_report_reads` stays keyed by package; hold `CRPT-119hrpt811` out until this lands.** | A refused refresh keeps the prior row, so a Part-1 row published under the package identity would sit stale and be retried forever once Part 2 appears (Part-1-only records persist for years: `CRPT-112hrpt38`), and `CRPT-119hrpt494` is already silently half a report. The contract family already keys nominations and treaties on a part-bearing citation or suffix. Mike confirms the identity move (DocSpec 0003 precedent); the constituents reader and the appended column can start first. |
| 30 | How U.S. Code section keys join when the grammar lower-cases section letters (`31-5318a`) and `law_code_sections` prints them (`5318A`). | **Lower-cased on both sides: the grammar's `usc_section` key stays, and `law_code_sections` and `table3_records` carry a derived `usc_section_key` beside the as-printed `usc_section`.** | Case carries no identity in the Code (no case-only pair in the 119th table, the OLRC release point or 1.57M annual rows), lower-case is already the join key in RefSpec's oracle, spicy-regs' citation ontology and spicysearch, and a verbatim column beside a derived key is the tables' existing pattern; folding at join time in every consumer is how the miss (142 of 814 lettered rows) went unnoticed. Blind ruling 2026-09-23. |
| 31 | Whether the narrow `congress_bills` list writer keeps running while plan A1 is open. | **Disabled on 2026-09-23 until A1 restricts it to columns the family writer does not own.** A1 retired it instead: the family's daily run refreshes the whole 119th, the list writer's schedule no more often. | Its run of 2026-09-23 published `c789aaa9…`, replacing 3,044 `update_date` instants with same-day dates against the documented larger-value merge and 3,095 congress.gov page URLs with API resource URLs, and skipping 13 in-window bills; a stale list costs one cron's lag, a wrong one costs every consumer a re-read (drift audit receipt). |
