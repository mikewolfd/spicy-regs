# Disposition of the July–August research program

Written 2026-09-04, pruned 2026-09-07. One record of what the July–August
program built, what each piece gives a user, and where each piece now lives.
Completed steps have been deleted rather than kept as ticked boxes; the merge
chain is in `git log` on the fork. **Start at *Open questions*: three decisions
are Mike's and the rest of this document describes them in the past tense.**

Every claim below was checked against a pinned commit, not read from a
document. The pins:

| repo | ref | commit |
|---|---|---|
| spicy-regs | `origin/main` (live upstream) | `1f02a7f` |
| spicy-regs | `archive/landing-final` (was local `main`) | `9a79569` |
| spicy-regs | `integrate/payload-prereqs` | `8d9e7a2` |
| spicysearch | `main` | `fc1f5fa` |
| DocSpec | `main` | `0bb2add` |
| spicy-docs | `main` | `9f8c7ee` |
| RefSpec | `main` | `da4fe055` |
| rulespec | `main` | `a87d839` |
| spicy-docs | `main`, for the 2026-09-05 pricing | `86e8416` |
| rulespec | `main`, for the 2026-09-05 pricing | `a519d06` |
| RefSpec | `main`, for the 2026-09-05 pricing | `00f22e9c` |

## Open questions

Four decisions are Mike's, none blocked on work. Elsewhere this document
describes them in the past tense, which reads as settled; they are not. The
credential pair is first because it is the only one with a live secret in it.

1. **Whether the FCC credential fix goes upstream, and whether the key is
   rotated.** Two questions, one secret. `c6695e0` moves the api.data.gov key
   out of the request URL and onto an `X-Api-Key` header: the key was a query
   parameter, httpx renders the full URL into `HTTPStatusError`, and both
   error handlers log the exception — the retry handler on every attempt, so a
   429 from FCC wrote it repeatedly. Verified both directions rather than
   assumed: a constructed 404 does render the key, and the API answers
   `API_KEY_INVALID` to a bad header where it answers `API_KEY_MISSING` to
   none, so the credential can leave the URL rather than be scrubbed out of an
   error string. It is committed locally and pushed nowhere, because origin is
   civictechdc and Eugene owns `main`. Separately: the same key leaked into
   `supply-2026-09-02/receipts/crs-summaries-2026-09-07.jsonl` through a
   congress.gov 404 and was redacted in place 2026-09-07. Re-grepped after,
   not merely recorded as fixed: zero occurrences in that file, in all five
   repos, in every corpora `.log`, and in every corpora receipts tree, with
   the key's length asserted at 40 first so an unset variable could not match
   everything. The ~404 GB of blobs and parquet is unscanned. A fix closes the
   mechanism; only rotation closes the exposure, and that is his.
2. **The eight-table registration branch.** `feat/register-eight-tables` on the
   fork, unmerged, no PR. It registers the five rulemaking tables, two court
   tables and bill subjects in the data dictionary and MCP server, and carries
   the generation-aware reader those five need. Held because none of the eight
   objects exists in the bucket until #194, #195 and #196 merge upstream and
   their workflows run. Verified 404 on all eight, 2026-09-06.
3. **Whether the `documents` table should carry `comment` and
   `restrictReasonType`.** A schema revision. The payload has both; the
   published table has neither. Measured: `comment` is populated on 410,329
   documents, 94.7% of them documentType "Other", overlapping the comments
   table by 157 rows. The route decides the cost — cheap from spicy-docs'
   source-native capture, a re-ingest of about 2M payloads from this side.
4. **The nine `corpora/` scripts no evidence file cites.** They stay on the
   fork unless someone names a run. Six of the fifteen are cited by an in-tree
   evidence or receipt file; these nine are cited by none:
   `artifact_retrieval_baseline`, `body_retrieval_corpus`,
   `mirrulations_document_corpus`, `mixed_real_data`, `profile_evaluation`,
   both `relation_exclusion_evaluation` scripts, `segmentation_evaluation`,
   `segmentation_sparse_retrieval`.

## Findings that were nearly lost

Recorded here because they existed only in session messages, which is how the
CFR-text finding stayed lost for a month.

- **The Federal Register body pointer we publish is the worse of the two.**
  The table carries `html_url`, `pdf_url` and `body_html_url` and no XML
  pointer, so a body extraction takes the HTML route by construction. Measured
  over 993 documents: HTML carries publisher boilerplate on 993 of 993, median
  passage 135 characters; XML on 0 of 993, median 610. The publisher returns
  `full_text_xml_url` and `raw_text_url` for the same document (checked against
  its API, 2026-09-07). Now stated on the `federal_register` table page;
  adding the column is small, populating it is a backfill.
- **Reranking's decisive missing number is R@50 on the queries that fail.**
  The archived rerank result roughly doubles rank-1 with R@50 unchanged, so it
  reorders an already-retrieved set. Its 35 queries are anchor-derived, which
  makes them a lookup test, and lookup is the mode that already works. On
  question-shaped queries nobody has measured whether the answer is in the top
  50 at all; if it is not, reranking reorders the wrong 50.
- **The "Georgia-Pacific About US Report" boilerplate is not from our titles.**
  Searching `documents` and `comments` titles for about-us, contact-us and
  privacy-policy patterns returns 78 apparent hits, every one a false positive
  on "about use of" in a real title. That string entered downstream of these
  tables and its source is unidentified. Another lane's record may still carry
  the wrong attribution.

## What the program was

Between 2026-07-23 and 2026-08-29 a document-AI research program ran on this
repository: read federal documents, segment them, tag what they are about,
extract how rules relate, and publish the result in a portable graph format
that other products can consume. It measured everything it did — 202 evidence
files, 20 design specs, 14 notebooks — and it left behind about 270,000 lines
across two branches.

Users of the platform ask four kinds of question this program answers:

- *Follow one rule* — from the Unified Agenda through its docket, CFR parts,
  RIN, comment periods, and final rule, across the four identifier systems the
  government uses for it.
- *What is this document about* — in words no 1995 thesaurus anticipated.
- *Which cases touch this rule* — court opinions cite statutes, not CFR parts,
  and nothing joined them.
- *Is this all of them* — whether a result set is complete or the platform
  never fetched the rest.

The sections below say which piece answers which question and where it ships.

## Where the work lives

Two branches carry the content. They forked from `origin/main` at `01ecbef`
(2026-08-19) and were cut one day apart from the same program. Neither removes
anything from upstream; the nine files they lack are what upstream added after
the fork.

| branch | commit | date | vs `origin/main` | holds |
|---|---|---|---|---|
| `integrate/payload-prereqs` | `8d9e7a2` | 2026-08-28 | +649 files | the shared core plus source-native, courts, bills, the table publisher |
| `archive/landing-final` | `9a79569` | 2026-08-29 | +609 files | the shared core plus `source_catalog/`, `universes/` |

Both are on `github.com/mikewolfd/spicy-regs` (a fork; remote `fork`) with
four earlier snapshots whose tip trees are copies of one or the other:
`archive/landing-main-pre-reorg` `31a4bfe`, `archive/integrate-payload-prereqs-pre-reorg`
`a6ab98a`, `archive/pre-strip-2026-08-26` `57d46bf`, `backup/pre-marker-fix`
`bc8f534`, `feat/rkaf-boundary-freeze` `a8938b4`. Everything has a remote.

Fifteen `src/` files exist on both branches with different content. Two matter:
`sources/supreme_court_opinions.py` and `transforms/build_supreme_court_opinions.py`.
The `8d9e7a2` versions are the fuller ones — `9a79569` stripped the measured
403 rate-limit guard and the bound-volume page-range logic — so `8d9e7a2` is
the branch to cut from. The rest belong to pieces that ship from one branch
only.

Neither branch has PRs #181, #182, or #183. Both carry the earlier
`uscode_uslm.py` and `integrate` carries the earlier `source_domains.py`; the
corrected versions are on their own branches, and the PRs below cut from
`integrate` by file, never wholesale.

## Ships to spicy-regs

Each of these becomes one PR cut from `integrate/payload-prereqs` (or
`archive/landing-final` where noted) onto `origin/main`, with the same
treatment as #181: rebase, gate through `uv run`, validate against real data,
one-paragraph description. Items 1–3 shipped on 2026-09-04 as PRs #194, #195,
#196 — open on civictechdc, merged on the fork.

### 1. The rulemaking join surface

`transforms/build_rule_targets.py`, `build_proceedings.py`,
`build_regulatory_agenda.py`, `build_comment_periods.py`, with `published.py`
and `pipelines/materialized.py` that serve their tables. On both branches.

**What a user gets.** Today the platform pairs a proposed rule with its final
rule (`rulemaking_lifecycles`) and links Federal Register documents to dockets
(`fr_docket_links`). It cannot follow one rule end to end, because a rule is a
RIN on reginfo, a docket on regulations.gov, a set of CFR parts in the Code, and
a document number in the Register, and nothing joins the four. These transforms
build that spine — docket ↔ CFR ↔ RIN — then promote each rulemaking to a
first-class proceeding with its actions, link agenda items to those actions,
and materialize every comment period including reopenings. A user follows one
rule from agenda to final under every name the government gives it.

This is the product the platform was built to be.

### 2. Courts

Two sub-clusters, on `integrate`. **CourtListener** — `sources/courtlistener_bulk.py`,
`transforms/build_court_opinion_bodies.py` (356 lines),
`build_court_opinion_clusters.py` (410), `court_scope.py` (348),
`pipelines/rollups/court_opinion_{bodies,clusters}.py`,
`scripts/backfill_cluster_court_scope.py`, plus 84 additive lines in
`sources/courtlistener.py`. Shipped as PR #195. **Supreme Court** —
`sources/supreme_court_opinions.py`, `transforms/build_supreme_court_opinions.py`,
`pipelines/rollups/supreme_court_opinions.py` — held back: its source imports
`bs4`, which `origin/main` does not carry and no other source uses, so that
dependency is a separate PR and a separate decision. `transforms/pdf_text_pymupdf.py`
is not courts at all: it is imported by `docpipeline/source.py`, and it is
AGPL; it goes with the document-AI producer, where the licence choice is made
explicitly. A real run exists: `output/court-data-2026-08-22/`, 5.7 GB.

**What a user gets.** Court opinions cite statutes, not CFR parts, so a user
asking "which cases touch this rule" gets nothing today. These pipelines bring
CourtListener's bulk dumps and the Supreme Court's own opinion PDFs into the
same tables as everything else — opinion text, cluster identity, and the court
that decided (which the cluster dump omits) — so the U.S.C. bridge can join
litigation to rulemaking. spicysearch already tags court opinions it is handed
as pinned parquet; this is what produces that parquet.

Ships whole. The 21 tables on `origin/main` each keep their fetch beside their
transform; courts follows the same shape.

### 3. Bill subjects

`sources/bill_subjects.py`, `transforms/enrich_bill_subjects.py`,
`pipelines/bill_subjects.py`. 722 lines on `integrate`, already wired with a
cron entry point and workflow.

**What a user gets.** Every bill carries a CRS policy area and legislative
subjects that Congress.gov assigns. With them in the tables, a user filters
bills by topic and relates bills to rules by subject — the first link between
the legislative and regulatory halves of the corpus that does not go through a
statute citation.

Ships whole, same reasoning as courts.

### 4. The document-AI producer

`docpipeline/` and `corpora/` on `8d9e7a2`: the only code that writes Rulespec
RKAF from a real document. Seven production modules across three repos read
that format and nothing else produces it.

**What a user gets.** Topic tags in open language for documents no curator
anticipated, with every claim bound to an exact span of source text, in a
format other products already read.

**Ruled 2026-09-05, split in two, both lanes briefed the same day.**

- **Deterministic layer to `rulespec/packages/rulespec-projection`.** 21
  functions, 1,307 lines of `rkaf_projection.py` plus 12 of its 13 classes.
  Zero new dependencies: the functions import nothing outside the standard
  library, and what they reach is copied at the name because the host modules
  import pyarrow and loguru. Two seams stay with the caller as injected
  adapters.
- **Orchestration to spicy-docs.** The 13 model-path functions (1,150 lines),
  six provider adapters, and the task modules; it depends on the package above
  for the 878 lines both use. Its binding constraint is spicy-docs'
  reader-closure guard: model packages must never be import-reachable from the
  read and verify path, and that guard now probes all five modules DocSpec
  imports.
- **`corpora/` moves per module, last, never with the adapters.** Six of
  fifteen are cited by an in-tree evidence file; see *Open questions*.

Execution is sequenced behind SpicySearch's first composition. The full
per-module dependency pricing lives in the briefs held by the rulespec and
spicy-docs lanes; it is not repeated here.

## Ships to spicy-docs

### Publisher-issued document populations

`sources/document_populations.py` (468 lines) and seven digest-pinned captures
under `sample-data/document-populations/` — CBO's cost-estimate feed, FCC ECFS
filing pages, GovInfo PREMIS records. On `integrate`.

**What a user gets.** When a query returns 412 CBO cost estimates, the user
cannot tell today whether that is all of them. This captures what the publisher
itself says exists, so the platform answers "412 of 415" and can distinguish
"there are no more" from "we never fetched the rest." The parsers refuse a
bot-challenge page rather than report an empty population, so a blocked fetch
never reads as a complete one.

spicy-docs is the acquisition product — "spicy-docs gets; it does not
interpret" — and already carries `source_domains.py`, the documented-vs-observed
drift gate. Populations are the same kind of thing: acquisition metadata. It
goes through spicy-docs' own intake, commit-never-push.

### The table publisher

Landed in spicy-docs at `2bb1dcf` as the canonical copy. No spicy-regs branch
re-cuts these modules. One pricing miss is recorded against it: the brief
costed `duckdb`, which only the test imports, and missed `pyarrow`, which the
module imports itself, so the port hit an import error in another lane's tree.

## Ships to RefSpec

### The rulespec publish gate

`ontology/rulespec_release.py` (178 lines). On both branches.

**What a user gets.** Confidence that what RefSpec publishes was checked
against the rulespec release it claims to depend on. Today RefSpec verifies its
pin against its own recorded digest — proof the note did not change, not proof
the note is true — and its `profiles/rulespec-dependency.json` says
`localUnpublished`. This gate recomputes the L0 contract digest from the tagged
rulespec archive and refuses to publish on mismatch.

Committed to RefSpec, not pushed, once the current merge settles.

### One missing minter

`canonical_usc_chapter_iri` and its lowercase-suffix rule
(`ontology/citations.py:404-418` at `e2da4b3`; the unused grammar was deleted
after it, fork delivery decision 18). The 2026-08-31 port of the citation grammar
into RefSpec's `iri_minting.py` carried seven of eight producers; this is the
eighth.

**What a user gets.** `26 U.S.C. chapter 13A` and `chapter 13a` resolve to the
same place.

Goes to whoever is editing `iri_minting.py`; it lives inside that file.

## Already delivered elsewhere

Users have these today through the product that maintains them. The
spicy-regs copies stay on the fork as the record of where each started.

| piece | where users get it now | what they get |
|---|---|---|
| `source_native*.py`, `schemas/source_native_release/1.0/` (22 files) | spicy-docs `src/spicy_docs/source_native.py` and siblings, from `70a5b16` 2026-08-29; integrate's copy is the byte-identical ancestor at `ff8d202` | faithful, digest-pinned captures from the Federal Register, regulations.gov, GAO product pages, and CourtListener — GAO capture the original never had |
| `sources/uscode_uslm.py`, `uscode_olrc.py` | RefSpec `tools/build_usc_source_credits.py` (`5b8f4d8b`), `registry/act_resolution.py` | from an act section to the U.S. Code section it created, with the division that public Table III lacks. Both copies gained the memory bound the same day: spicy-regs `5ecfd5a`, RefSpec `93d244a3` |
| `ontology/citations.py` | RefSpec `registry/iri_minting.py` (`582461fe`) — seven minters, 1,570 test lines, contract-tested across four compiled forms | every CFR, U.S.C., RIN, Federal Register, and docket identifier resolves to one stable IRI |
| `ontology/act_index.py` | RefSpec `registry/act_resolution.py`, which names it as its provenance | "section 107 of the X Act" resolves through two OLRC sources |
| `transforms/build_authority_edges.py` | RefSpec `registry/unified_agenda_parquet.py` | Unified Agenda legal-authority strings become edges a user can traverse |
| `document_release.py`, `document_release_v3*.py`, `fixtures/releases/` (77 files) | DocSpec DocumentRelease 2.0 — schemas, verifier, 49 sealed fixture cases, three mint receipts (8,284 documents, 220,582 segments) | sealed document releases with bodies and segments, minted and verified |
| `docpipeline/source.py`, `segments.py` | DocSpec `processing/bounded_segmentation.py`, `retention_floors.py` (2026-08-30) | bounded, retention-checked segmentation. DocSpec addresses UTF-8 bytes where spicy-regs addressed codepoints; ids from the two do not compare equal |
| `sources/source_domains.py` | `feat/source-domain-drift-gate-revived` `f8e9e35` (PR #192, ready to reopen) | a check that the values in `document_type`, `rule_stage`, `rin_status` still match what GSA and reginfo document — it already found that every `rin_status` row uses a spelling the publisher's schema does not |
| `enrichment/accepted_output.py`, `managed_release.py` | RefSpec `src/refspec/managed_release.py` (2,638 lines) | candidate lookup against curated, sealed vocabulary releases |
| `docpipeline/retrieval.py` (5,479 lines) | spicysearch `search_application.py`, `POST /v1/search` — lexical, semantic, and concept lanes in production | search over the corpus, served |
| `candidate_release.py` | RefSpec managed releases | the same lookup against a format RefSpec maintains, rather than the Atlas 1.0 format retired 2026-08-09 |
| `source_catalog/` (10 files, 7,239 lines; `archive/landing-final` only) | DocSpec `src/docspec/domain/source_catalog.py`, `schemas/source_catalog/1.0/` — about 1,800 live lines | the catalog of what exists, what was requested, and what was admitted |

One thing outlives the `source_catalog/` code: DocSpec pins
`urn:spicy-regs:source-catalog-release:v1:` in 83 files. That prefix is
DocSpec's contract. spicy-regs never reuses it.

## Preserved on the fork

These wait for a reader. Each names what would bring it back.

**The open-vocabulary concept lifecycle** — `ontology/concepts.py`,
`concept_dimensions.py`, `transforms/build_concept{s,_assignments,_events}.py`,
with `ann_index.py` and `candidate_channels.py` as its serving layer. A SKOS
registry that grows from the corpus: mint a term from what documents say,
promote it on multi-source evidence, deprecate it, merge within one facet.
What a user would get: a vocabulary that tracks what agencies write rather than
what a curator listed. Why it waits: RefSpec is closed-world by design and
spicysearch is closed-vocabulary by design, so no product reads a minted term
today (RefSpec REF-053, 2026-08-31). It returns when one does, and REF-053
asks that the quota and single-facet merge rules already paid for be weighed
then.

**The rest of `ontology/`** — `ledger`, `invariants`, `receipt`,
`relation_findings`, `evaluation`, `common`, `adapters`, `subjects`,
`segmentation`, `checkpoint`, `llm`, `codex_cli`. Supporting modules for the
lifecycle above and for experiments now concluded. RefSpec took what it needed
from `invariants.py` (`2b4960e1`).

**`universes/`** — three JSON files naming the requested regulations.gov
universes, with 600 lines of the archived `PLAN.md` describing them. DocSpec
owns the universe now.

**The record** — `docs/superpowers/specs/` (20), `docs/evidence/` (202), 12
of 14 notebooks. Every decision above has its measurement here.

**`feat/rkaf-boundary-freeze`'s 26 fixture schemas** — an earlier v3 fixture
layout.

## What remains

Everything else in this document has shipped; the steps that recorded it are
deleted rather than kept as ticked boxes. The merge chain is in `git log` on
the fork.

- Tell Eugene. Seven PRs sit in his repo from a collaborator, one of them this
  document.
- Register the eight new tables (the branch above, once the three PRs merge).
  The first `materialize-rulemaking` run needs `--allow-bootstrap` via
  `workflow_dispatch`.
- The Supreme Court sub-cluster, which waits on the `bs4` decision.
- The RefSpec minter, to whoever holds `iri_minting.py`.

## Supersedes

Three documents recorded parts of this before, each from its own product's
side. This one reads across all of them against the pinned commits and adds
the decisions.

- spicy-regs `docs/migration/spicysearch-product-migration-manifest.json`
  (2026-08-29, on the archived branches only) inventoried sixteen product
  surfaces for the SpicySearch migration and marked itself
  `retirement_authorized: false`, pending a reconciliation. This is that
  reconciliation.
- RefSpec `plans/2026-08-31-refspec-intake-ledger.md` planned six ports from
  this program. All six landed the day it was written (`5b8f4d8b`, `582461fe`,
  `2b4960e1`). The ledger stands as the record of what was planned; the
  table above records what shipped.
- spicysearch `docs/history/2026-09-01-script-product-disposition.md` drew the
  product boundaries this document uses, and set deletion gates for
  spicysearch's own body-fetch scripts. Its boundaries hold; its premise that
  the source-native gate had no survivor predates spicy-docs' `70a5b16` by
  three days.

Each of the RefSpec and spicysearch documents takes a one-line note pointing
here, through its own repo.
