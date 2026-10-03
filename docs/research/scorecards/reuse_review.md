# Congressional scorecards: component reuse review

Reviewed 2026-10-03. Reuse the existing congressional data layer and PDF
extraction infrastructure. None of the inspected projects supplies an authorized,
source-faithful replacement for the publisher readers. Keep the frozen
`scorecards-v1` source model; extend the existing member crosswalk to retain
Vote Smart IDs.

The project identities below were established from official sites and repository
links. Code findings refer to pinned commits in the linked review notes. Public
specification availability does not establish complete data coverage. API calls
requiring credentials were not exercised.

## Project and API findings

| Project/API | What it already solves | Schema/IDs worth reusing | Code or endpoint worth integrating | License/access constraints | Recommended role in SpicyDocs/SpicyRegs |
| --- | --- | --- | --- | --- | --- |
| [GovTrack advocacy scorecards](https://github.com/govtrack/advocacy-organization-scorecards/tree/fa63a2b5326edd4b8835386082c2317627f058ce) | Historical collection of published member scores and original-publisher links; YAML metadata followed by CSV values. | Named GovTrack IDs, publisher, source link, literal period and unit. Its single-value model does not cover items, components or exclusions. | Read `scorecards/*.yaml` for census leads; compare through existing `members.govtrack_id`. `lint.py` is a file normalizer, not a completeness checker. | Public repository; no license grant found in the inspected tree. Publisher rating rights remain separate. | Discovery and secondary comparison. Acquire actual ratings from the original publisher. Its `updated` can mean publication or import date. |
| [unitedstates/congress](https://github.com/unitedstates/congress/tree/8184bcb13160da2389dea4c902de67817db681fc) | Official bill, amendment and roll-call parsing and incremental acquisition. | Bill/amendment natural keys; chamber/Congress/session/roll; separate action occurrences. | Consult `congress/tasks/{utils,bill_info,vote_info,amendment_info}.py`; use local `BillIdentity`, `VoteKey` and published congressional tables. | CC0 code. This grants no rights to third-party scorecard content. | Identifier guidance. Do not add a second congressional collector. Translate upstream calendar-year vote sessions to our ordinal sessions explicitly. |
| [congress-legislators](https://github.com/unitedstates/congress-legislators/tree/d5af3d2d2490f8c6532c9490b1f47a76cacca699) | Congressional identity crosswalks and historical service terms. | Bioguide, LIS, FEC, GovTrack, Vote Smart, ICPSR, OpenSecrets, Wikidata; dated terms and alternative names. | Extend existing `LegislatorsAcquirer` → `shape_member` → `members.votesmart_id` → exact scorecard resolver. | CC0; public current/historical files. IDs are incomplete and require collision checks. | Direct reuse through existing members ingestion. Preserve multiple FEC IDs and historical context. Do not import the interactive wildcard lookup. |
| [Open States](https://github.com/openstates/api-v3/tree/58696e5e921bbae6afb6d8f4aa693c3eb65837b0) | State legislative people, organizations, bills, sponsorships, actions and votes. | OCD IDs; jurisdiction/session; nullable person links with original voter names; vote events independent of bill identity. | `api/schemas.py`, `api/bills.py`; future `/people` and `/bills/{jurisdiction}/{session}/{bill_id}` with votes/sponsorships. | MIT code; data dedication has exceptions. API key, tiers and rate limits apply. | Schema reference now; potential future state reader. It does not replace the existing federal tables. |
| [Open Civic Data](https://github.com/opencivicdata/python-opencivicdata/tree/c089b29a024ceb726765c24f3db792c49fdf8275) | Common Person, Organization, Membership, Bill and VoteEvent models and civic identifier conventions. | Namespaced alternate IDs; geography distinct from legislative organization; bill-less motions and unresolved named voters. | `opencivicdata/legislative/models/vote.py` and the OCD identifier specification. | BSD-3-Clause Python code; division identifiers CC0. Documentation has separate, partly outdated material. | Borrow model distinctions. Preserve supplied OCD identifiers; do not mint identities for unresolved names or add its Django database stack. |
| [CIV.IQ](https://github.com/civdotiq/civ.iq/tree/d1c70cf23341bf43d610730fb591349fe3dda29f) | Civic API, dataset metadata, entity descriptions and a separate entity-resolution package. | Bioguide, source URL, generation time, record count and dataset license. | Inspect exact alias/crosswalk code in `packages/entity-resolution`; `/api/v1/representatives/{bioguideId}` is an optional comparison surface. | Root Apache-2.0; package MIT; live docs call the platform MIT. Resolve file-specific license differences before adoption. Public API documents no key and rate limits. | Reference only at present. Its fuzzy matching and single-FEC-ID mapping do not meet our exact historical resolution requirements. |
| [Civitas](https://github.com/kamoras/civitas/tree/3747dcc51f404ef85f0096583a44f520056342c5) | Its own representation scores, dimensions, ranks, history and paginated public API. | Publisher member slugs, score snapshots, dimension confidence and stored algorithm version. Public profiles omit stored Bioguide; history omits stored algorithm version. | `backend/app/api/public.py`, `schemas.py`, `models.py`; `/api/public/v1/senators`, `/representatives` and their `/{id}/history` routes. | AGPL-3.0 implementation. Public API documents no key and rate limits; a separate dataset redistribution grant was not established. | Analysis reference; possible original publisher if separately qualified. Do not copy its score formula into published source values. |
| [Capitol Trace](https://github.com/CapitolTrace) | Hosted congressional intelligence; an open typed Congress.gov client and attribution UI. Core ingestion is private. | Bioguide, historical terms, fully qualified vote identity and source URL. | MIT `congress-api` client as reference; `capitoltrace-ui`'s `SourceLine` for attribution presentation. | [Hosted terms](https://capitoltrace.com/terms) prohibit dataset redistribution and competing-product use. Public code licensing does not grant hosted-data rights. | Documentation/UI reference. Existing Python readers already perform the official-data work. No hosted-data integration. |
| [Sentinel Intelligence](https://github.com/Sentinel-Intelligence/sentinel-public/tree/9c56267387ef5d0978dac6f70afcede9ad71b08a) | Public influence-graph methodology, example queries and receipt verification. Full ingestion is not published. | Source record, ingestion time, batch and content-integrity evidence. | `scripts/sentinel_canon_v1_verify.py` as an independent verification reference. | MIT public tooling; [no public API](https://sentinelintel.org/api-docs/) or established private-dataset grant. | Evidence design reference. Keep existing artifact admission; hashes and signatures do not establish completeness or factual truth. |
| [Vote Smart Rating API](https://api.votesmart.org/docs/Rating.html) | Category → interest group → rating edition → candidate rating. | Distinct `categoryId`, `sigId`, `ratingId`, `candidateId`; literal timespan, rating name/text and value. | `Rating.getCategories`, `getSigList`, `getSig`, `getSigRatings`, `getRating`, if licensed. | Approved API license/key and fees; [terms](https://api.votesmart.org/docs/terms.html) impose attribution/use/removal conditions. Current authenticated behavior was not tested. | Architectural/discovery reference only. Follow original-publisher URLs; do not substitute its normalized ratings for publisher literals. |
| [Congress.gov API](https://github.com/LibraryOfCongress/api.congress.gov) | Official members, bills, amendments, sponsorship, actions and House vote links. | Bioguide; congress/type/number for legislation; congress/session/roll for votes. | Existing `CongressListingReader`; `/v3/member/{bioguideId}`, bill cosponsors and House-vote member routes. | api.data.gov key and limits. Published docs conflict on House-vote historical coverage; verify the required edition against actual qualified inputs. | Reuse published official tables for exact joins. Keep House Clerk/Senate LIS coverage until a replacement is qualified. |
| [GovInfo API/bulk](https://www.govinfo.gov/developers) | Official publication discovery, package/granule metadata, renditions and BILLSTATUS bulk XML. | Package/granule IDs; legislative citations; source modification time distinct from publication time. | Existing `GovInfoDiscoveryReader`, body acquisition and congressional `bulk_status`; package summary and collection change routes. | API key for service calls; public bulk access. Government collections can contain third-party copyrighted material. | Existing official evidence/citation targets. Missing official coverage belongs in that pipeline, not scorecard acquisition. |
| [OpenFEC](https://github.com/fecgov/openFEC) | Candidate/committee records, histories and reported finance relationships. | Candidate/committee IDs, election cycle and reporting period; none equals Bioguide or proves congressional service. | Existing `FecClient`/candidate profile; `/v1/candidate/{candidate_id}/history/{cycle}/`. | Key and limits; mixed code licenses and specific data-use terms. | Optional exact crosswalk input from existing data. Financial relationships cannot establish a publisher's preferred action or a member match. |

Detailed code, schema, endpoint and license evidence:
[congressional ecosystem](work/reuse/congress_ecosystem.md),
[civic platforms](work/reuse/civic_platforms.md),
[official APIs](work/reuse/official_apis.md).

## Reuse decisions

1. **Reuse directly:** the existing members/terms, bills/amendments, roll calls,
   member votes and cosponsors; existing Congress.gov, GovInfo and FEC readers;
   bounded capture, evidence policies, atomic publication, Parquet/DuckDB and
   generic MCP tools. For PDF extraction, use existing SpicyDocs extraction and
   DocSpec's retained-page lifecycle with explicit OvisOCR2 or configured Docling
   defaults. SpicyDocs must not import its downstream consumer DocSpec.
2. **Borrow selectively:** CC0 identifier conventions from `unitedstates/congress`;
   OCD's separate event, organization and unresolved-name models; Civitas's
   distinction between score history and formula version; Sentinel's independent
   integrity verification; CIV.IQ's source/freshness metadata. No new external
   runtime dependency or broad code port is justified by this review. A later
   port must name the exact files, license and parity tests.
3. **Still missing from these projects:** original-publisher readers with complete
   edition proofs, literal items/results, scoring rules, exclusions and rights
   evidence. Locally, the remaining work is adapter and format qualification across
   the candidate inventory, PDF table/glyph checks, publisher-specific historical
   resolution coverage, rights dispositions and methodology-specific reproduction.
   Exact resolution and safe edition replacement already have local implementations
   and tests; broader source qualification and release remain separate. A public
   API or directory does not complete these tasks.
4. **Schema consequences:** add `members.votesmart_id` through the existing
   member pipeline. Retain the scorecard tables and their literal values,
   namespaced identifiers, separate item occurrences, optional metric context,
   snapshot evidence and methodologies. Keep import, observation and publication
   times distinct. Explicitly translate calendar-year versus ordinal sessions.
   A future Civitas adapter must qualify dated score history and formula-version
   availability; missing API fields stay missing. No new source table is supported
   by this review alone.

The new member column changes the required official input schema. Its provider
wheel and members generation must be adopted together before an analysis that
requires that column can run. Existing published data is not silently rewritten.

## Repeatable repository review method

1. **Establish identity.** Follow the official product site's repository link.
   Record owner/repository, public/private scope, default branch, commit SHA and
   review time. Record ambiguous candidates instead of conflating names.
2. **Inspect before executing.** Read the license and package manifests, then
   trace one actual source record through acquisition, parsing, storage and the
   public API. Cite commit-pinned files/symbols. A README claim is not evidence
   that a field survives the public response.
3. **Measure the interface.** Inspect OpenAPI and schemas; use bounded public
   GETs for one representative record and pagination where access allows. Record
   URL, status, observation time, type, size and digest. Record inaccessible
   endpoints explicitly. Keep credentials out of receipts and source captures
   private unless their redistribution is established.
4. **Compare one difficult case.** Test historical identity, repeated actions on
   one bill, a non-vote item, a missing value, pagination termination and an
   unavailable upstream. Map each external identifier into the existing local
   namespace. Preserve unresolved candidates and inspect error semantics.
5. **Separate permissions.** Record code license, dataset rights, API terms and
   source-document rights independently, with exact evidence. Public source code
   does not grant hosted-data or publisher-document redistribution rights.
6. **Choose one disposition.** Direct reuse, small attributed port, model
   reference, discovery only, or unavailable/unsuitable. Name the local integration
   file, dependency cost, gaps and required tests. Produce exactly the six fields
   in the table above; link supporting receipts rather than expanding claims.
7. **Verify adoption.** Have a separate reviewer trace the proposed local call
   path and inspect the smallest representative fixture. Pin any adopted package,
   preserve license notices, test success/refusal/ambiguity, and record the
   artifact hash. Re-review when the upstream commit, API major version, license,
   source layout or local input schema changes.

Each review receipt should retain `project_id`, `official_url`, `repository_url`,
`commit_sha`, `reviewed_at`, inspected `file_paths`, endpoint observations,
separate code/data/access evidence, local integration paths, disposition,
schema impacts, test results and unresolved questions. The six-field report is
the human-readable decision; the receipt makes it repeatable.

## Concrete integration target: LCV 2025

Complete a local, end-to-end `lcv:2025` candidate through the existing LCV reader,
source family, exact resolver, analysis family, dictionary and generic MCP query
surface. This follows the completed census/schema milestone and is one
qualification target within the [full candidate task manifest](work/integration/adapter_tasks.json).

- **Input:** original LCV 2025 downloadable member CSV plus publisher item,
  methodology and member-result pages. Select one authoritative rendition per
  field group; keep CSV literals even when HTML formats them differently.
- **Processing:** `list_scorecards()`/`acquire_scorecard()` produce all applicable
  frozen source tables. Reconcile CSV EOF, discovered member/item sets and required
  detail pages. Retain capture hashes/parser version and private replay material.
- **Official links:** read one pinned set of existing members/terms,
  bills/amendments and roll calls. Resolve only exact identifiers or qualified
  historical names. Record unresolved and ambiguous rows; never fetch Congress
  again from inside the scorecard adapter.
- **Output:** one atomic local `scorecards` generation and a separately pinned
  `scorecard-analysis` generation. Expose publisher, edition, metric, literal
  value, source locator and resolution status through `describe_table`/`query_sql`.
- **Acceptance:** real-corpus counts reconcile against that capture's receipt;
  stable IDs survive row reordering; repeated successful acquisition replaces
  only this edition; truncated CSV, missing required detail, 404 and parser drift
  preserve prior rows; public evidence contains no source body under `hash_only`;
  every analysis row pins the selected source snapshot and official inputs.
- **Reviewable proof:** store input/reader/wheel hashes, count reconciliations,
  unresolved rows, source/evidence policy checks, publication audit and an
  attributed example SQL result. Pass provider and consumer gates. A local
  candidate is distinct from publication or a scheduled refresh.

Companion qualification cases remain necessary: Heritage Action for cosponsorship,
and NEA/Humane/C4IP for source-specific PDF layouts using the required extraction
infrastructure. Success on LCV does not qualify those formats or all census leads.
