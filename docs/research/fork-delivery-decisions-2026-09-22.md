# Fork delivery decisions — September 22, 2026

Mike answered the open delivery questions in the [fork output ledger](fork-output-ledger-2026-09-21.md)
and [consolidated backlog](../fork-generation.md) on September 22, 2026. Each entry states
the question in terms of what users get, the decision, and the work it sets. The ledger and
backlog still own status; update their rows when the work lands.

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

- **Decision 12, 2026-09-22 (`0fcbc21`):** the token now reaches only the
  `court_dockets` rollup. A free token's measured limits (10/minute, 100/hour,
  250/day) cover the daily docket delta but not the cluster rollup's keyless
  search catch-up of about 660 pages, which ran keyless in about 17 minutes.
- **Decision 4, 2026-09-23:** a scheduled run had published `rulemaking_lifecycles`;
  the family is withdrawn from the index and its workflow disabled and unscheduled.
- **Decision 6, 2026-09-23:** the opinion-body builder, rollup, workflow and
  registration are removed; the fork workflow is disabled.
- **Decision 5, 2026-09-23:** the House select aliases (110 seats, `hs`/`hl`
  prefixes from the native committee type and parent) are live through the
  SpicyDocs roster fix. 28 seats remain unlisted for want of a publisher link,
  not a policy choice:
  - 25 House seats on the Clerk's `EC00`, `IT00`, `JL00` and `JP00`. The Clerk
    types them `joint` but carries no Congress.gov code, and Congress.gov's two
    main JEC codes (`jsec00`, `jjec00`) even share one website.
  - Three Senate seats on `JSIK00`. Congress.gov lists no such committee in the
    118th or 119th Congress, and its detail route answers none.

  They stay in `committee_assignments`, unmatched and documented; nothing is
  matched by name.
- **Decisions 4 and T17, 2026-09-23:** all five rulemaking parents qualified and
  the rulemaking dataset is bootstrapped (`snapshot_0e799850…`), so agency timing
  can now come from `proceedings` and `comment_periods` as decided.
- **Decision 2, 2026-09-23 (`c113329`):** implemented as the derived table
  `member_vote_terms` (generation `890481eb…`), leaving `member_votes`
  unchanged. It reproduces the decision's numbers on the current votes: 15 of
  18 half-open misses resolved, the three `Not Voting` rows unmatched.
- **Decision 7, 2026-09-23:** the reviewed six-agency cohort is published as the
  fork's `comments.parquet` and `comments_index.parquet` (23,889,665 rows). ACF
  (129,052 originals, fully listed) is the next cohort and is not yet acquired.
- **Decision 1, 2026-09-23:** published as reconciled generation `a846cb44…`
  (419,866 bills, eighteen tables). Every inherited URL carries
  `url_source = inherited` (SpicyDocs 0.28.0, `6d34a1b`). The newer live 119th
  rows are reconciled in; nothing from either input was lost.
- **Decision 10, 2026-09-23:** SAM access is confirmed, and the bulk-extract
  path is repaired (SpicyDocs 0.28.0 and 0.28.2). The bounded retained initial
  load of 147,254 active 2026 registrations is published (`56dd0f65…`). Wider
  years, the fork's `SAM_API_KEY` secret and lobbying remain.

