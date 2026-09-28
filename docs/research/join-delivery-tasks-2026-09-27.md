# Join delivery tasks grounded in source records

The first delivery should make existing relationships trustworthy and queryable. Start with complete keys and target resolution, then expose source-stated links. Extend SpicyDocs only where replay shows that its reader loses a field or cannot yet read a required source shape.

This is an implementation proposal, not a claim that these outputs have been published. It refines the [populated-table audit](populated-table-joins-2026-09-27.md) and maps every earlier proposal to a task. The companion [task register](join-delivery-tasks-2026-09-27.json) maps every audited populated table to those tasks. Table coverage comes from that audit; the raw-record review below is a selected, replayed cohort, not a complete review of every source record.

## What the raw review changed

| Source evidence | Current behavior verified | Consequence for delivery |
| --- | --- | --- |
| Regulations.gov comment `ODNI-2009-0004-0002` has `docketId=null`, `commentOnDocumentId=ODNI-2009-0004-0001`, `commentOn=0900006480a18cfe` and `originalDocumentId=ODNI_FRDOC_0001-0004`. | SpicyDocs `classify_comment` preserves the complete record. SpicyRegs `_extract_comment` omits the parent-document fields. | Add an explicit comment-to-document link. Keep the three identifier systems separate and preserve the null docket. T03 is a new, higher-value refinement of the earlier proposals. |
| The retained 118 HR 1 fixture contains 49 cosponsors, including a sponsorship date and original-cosponsor flag. | `parse_bill_status` reads the list but `BillSponsor` keeps only Bioguide ID and full name. | Extend the source reader before promising dated cosponsorship. A positive withdrawal specimen is still required. T05. |
| The native Senate vote `119/senate/1/522` lists 48 documents and 48 amendment blocks whose identifiers are empty. | `parse_senate_vote` preserves these independent lists. The first document is `PN55-25`. | Expand documents and amendments independently; preserve nomination suffixes. No new vote parser is needed. T07. |
| A retained U.S. Code section contains native `ref href` links and a `sourceCredit`. | `scan_uscode_references` emits 30 reference observations and one source credit from the section-423 fragment. | Use the native links before applying prose extraction. Keep their XML locations and edition uncertainty. T13. |
| CFR part 18 has separate `AUTH` and `SOURCE` elements. | `scan_ecfr_authority_notes` retains both roles and XML ancestry. The grammar extracts `44 U.S.C. 1506` but reports a partial parse. | Publish source-stated authority separately from amendment-source citations. Preserve unparsed material and qualify other authority element shapes. T12. |
| A retained legislator term for `T000254` contains Democrat and Republican intervals within one term. | `parse_legislators` returns only the term's single `party=Republican` value. | Preserve the nested intervals before using party as of a vote date. T19. |
| The Federal Register record `2026-17334` lists two RINs and two docket strings. | SpicyDocs retains the arrays. Its projector does not produce the application's first-RIN scalar. | Expand every array element, keeping publication date and source ordinal. A shared RIN is a relationship, not document identity. T11. |
| Senate expenditure pages B-1243 and B-1244 show multiple expense lines under a document/payee and a continuation page without the office heading. | Visual review confirms the layout; the generic table extraction puts body text into large cells. The existing source schema deliberately does not claim individual payments. | Qualify line segmentation and office carry-forward before publishing payee or payment rows. T23. |

**Revision 2026-09-27:** Corrected the Senate example from `DUST20250194` / `ERAI SHIRVANI` to `DJST20250194` / `ERAJ SHIRVANI` after high-resolution native PDF review. The pinned truth set is `spicy-docs/tests/fixtures/senate_expenditures/payment-review-2026-09-27.json` (workspace-relative); this correction does not qualify automated payment parsing.

## Evidence and review limits

Reviewed repository revisions: SpicyDocs `f486e733d5363416652338d898907210b924d5f2`; SpicyRegs `a86bb01ca5eec36db1527b067d13b8ff3b5a21ed`. These are the code revisions used for this review, not release or deployment claims.

Evidence is retained under `/Users/mikewolfd/.codex/artifacts/spicy-regs-join-tasks-20260927/`:

- [Input manifest](/Users/mikewolfd/.codex/artifacts/spicy-regs-join-tasks-20260927/input-manifest.json): local paths, byte counts, and SHA-256 digests; adjacent source-fixture READMEs distinguish complete captures from reductions.
- [Reader and normalizer replay](/Users/mikewolfd/.codex/artifacts/spicy-regs-join-tasks-20260927/parser-replay.json): related bills, cosponsors, votes, member identifiers, meetings, regulatory attributes, citations, FEC metadata and synthetic identifier controls. Entries explicitly labeled as shaper/code review are not executable parser results.
- [Native legal-reference replay](/Users/mikewolfd/.codex/artifacts/spicy-regs-join-tasks-20260927/legal-replay.json), [comment and party-history replay](/Users/mikewolfd/.codex/artifacts/spicy-regs-join-tasks-20260927/additional-replay.json), and [communication replay](/Users/mikewolfd/.codex/artifacts/spicy-regs-join-tasks-20260927/communications-replay.json).
- [Senate raw-page/table observations](/Users/mikewolfd/.codex/artifacts/spicy-regs-join-tasks-20260927/senate-raw-review.json): fixture pages 1, 7 and 8 were extracted; pages 7–8, corresponding to printed B-1243–B-1244, were also rendered and visually inspected. This is not a qualified payment parser.
- [Earlier live join evidence](/Users/mikewolfd/.codex/artifacts/spicy-regs-join-audit-20260927T191845Z/verification.json): exact queries, selected publications, target coverage and cardinality observations.

The bill fixtures are reduced native records with retained complete relevant blocks. The legislator fixtures contain selected source records. The U.S. Code and CFR fixtures are native XML fragments, with their limitations documented beside them. Congressional Record entries are unchanged within excerpted HTML. Citation regression inputs are retained normalized text, not a new review of the originating PDFs. Synthetic docket/FR/USC controls test a transformation; they do not measure population coverage. No fresh source acquisition was performed for this proposal.

## Delivery rules

SpicyDocs owns acquisition, faithful source parsing, native identifiers, and its existing reusable interpretation functions. SpicyRegs owns application relationships, target resolution and query views. Use RefSpec's existing qualified vocabulary/mapping artifacts where appropriate. DocSpec retains document-processing state. Do not create another universal parser, entity store or citation writer.

Every relationship needs its source record key, source version or digest, field/XML/text location, and occurrence ordinal when the source can repeat values. Derived findings also need the rule version and exact input-text digest. A distinct-pair view may remove repeated pairs for navigation; it must remain distinguishable from source occurrences.

Keep parsing status separate from target status. Resolution is relative to a selected target snapshot: `not_checked`, `found`, `missing`, `ambiguous`, or `unsupported`. Preserve the source spelling and all candidate identities. A normalized identifier is not proof of an existing target, legal applicability, or common person/organization identity.

For a source-reader change, land the provider change and fixtures in SpicyDocs, publish/pin it through the existing dependency process, then land the SpicyRegs consumer. A SQL view does not need a new source parser. Materialize only when retained evidence or measured query cost calls for it.

Use Iceberg snapshot IDs when reading the existing catalog during ingestion or bulk replay. Record immutable publication identities for serving; pin bytes for files that remain unversioned. Record each table's selected snapshot rather than claiming an atomic multi-table snapshot that was not obtained. Iceberg supplies version selection and lineage; it does not infer these relationships. A serving-backend migration is outside these tasks.

## Recommended first delivery

| Order | Tasks | Result visible to the user |
| --- | --- | --- |
| 1 | T01, T02 | Complete join keys; an honest distinction between a citation spelling and an available target. |
| 2 | T03, T04, T08 | Navigate a comment to its named document, a bill to related bills, and a member to source-listed FEC identifiers. |
| 3 | T06, T07, T10, T11 | Navigate meetings, votes, FCC filings and rulemaking records through their existing arrays. |
| 4 | T05, T12, T13, T19 | Preserve fields currently lost and expose native legal references. |
| 5 | Remaining tasks below | Add evidence tracing, safe analytical joins, broader extraction and explicitly qualified identity. |

Each task below has one principal output. Tasks spanning providers and consumers have an explicit two-step delivery. Broader extraction work is staged by source family so a successful family can ship without waiting for unrelated formats.

## Task specifications

### T01 — Correct join keys and publish measured cardinality

Owner: SpicyRegs. Depends: none. Covers: P01, P22.

**Change.** Update `src/spicy_regs/table_joins.py`, its generated JSON and table metadata through their existing generation path. Declare `(bill_id,version_code,source)` for bill versions, `(package_id,part_id)` for report parts, and `(congress,chamber,event_id)` for meetings. Complete missing identity declarations. Rebaseline the populated document/docket attribute relationships before removing their stale `empty` classification.

**Output.** Each join declares the full key, observed parent multiplicity, unmatched distinct keys, input/output row counts, selected publications and measurement scope. Example: a version reference with `source=govinfo` joins only the matching provider version.

**Done when.** Every advertised key change has a replayable query over pinned inputs; duplicate parent keys are either rejected or declared as multiplicity. Regression cases cover reused version codes, report-part IDs and event IDs. Do not describe preventive key hardening as a reproduced production duplication defect: the earlier audit did not observe that amplification. Full attribute rebaselining must replace the earlier bounded parent sample.

### T02 — Resolve citation targets against selected publications

Owner: SpicyRegs. Depends: T01. Covers: P16, P02.

**Change.** Add a resolution layer beside `document_citations`, reusing `ontology/citations.py`, the current join catalog and the source findings from SpicyDocs `interpretation/citations.py`. Preserve the existing meaning of `target_resolved` during a documented migration; expose an unambiguous `target_status` and target snapshot. Route by citation kind, including dated FR identities and alternate citation systems.

**Output.** A source occurrence yields `(occurrence_key, target_kind, normalized_key, target_snapshot, status, candidate_keys, resolution_rule, expected_cardinality)`. A single-target reference is found only after a unique qualified match in that selection; an ambiguous result retains candidates. Routes intentionally matching several records, such as RIN-to-agenda editions, retain that set and its declared grain. Example: the retained `Public Law 114–254` finding supplies `114-public-254`; the resolver still has to look up the selected laws table.

**Done when.** Tests cover found, missing, ambiguous, unsupported and unread targets; reused FR numbers; partial citations; and an unchanged source with a newly available target. Resolve each distinct target key once per snapshot. Reproduce the earlier occurrence/target coverage report without treating key normalization as existence. Keep the source-text digest predicate. A found target proves a lookup, not that the extractor interpreted its context correctly; expose the derivation rule and context so T17's extraction checks remain a separate gate.

### T03 — Recover source-stated comment-to-document links

Owner: SpicyDocs verification, then SpicyRegs projection. Depends: T01, T02. Covers: P03, P21, P22.

**Change.** Reuse SpicyDocs `sources/regulations_gov/records.py:classify_comment` and its existing allowed fields. Extend SpicyRegs `schemas/regulations.py:_extract_comment` and the corresponding schema/export path to retain `commentOnDocumentId`, `commentOn` and `originalDocumentId` as separate source values. Expose `comment_document_references` from the explicit parent-document ID.

**Raw → output.** The retained ODNI comment produces `(ODNI-2009-0004-0002, ODNI-2009-0004-0001, comment_on_document, /data/attributes/commentOnDocumentId)`, with `docket_id=null`. Preserve `0900006480a18cfe` as a native object reference and `ODNI_FRDOC_0001-0004` as an unresolved original-document reference.

**Done when.** Replay the null-docket fixture and a bounded retained cohort with absent/null/empty parent fields; resolve named documents against a pinned target. No identifier-prefix docket inference and no equality across the three identifier namespaces. Verify schema evolution and Iceberg/public export preservation before a checkpointed larger replay; avoid an exploratory scan of all comment bodies.

### T04 — Expose publisher-listed related bills

Owner: SpicyRegs. Depends: T01, T02. Covers: P07.

**Change.** Reuse SpicyDocs `sources/congress/bill_status.py:RelatedBill` and `parse_bill_status`; project the already retained related-bill arrays. No new text parser. Publish an occurrence view and a distinct related-bill pair view.

**Raw → output.** The reduced native status for `119-hr-300` yields `119-hr-1630`, source ordinal 0, relationship type `Related bill`, identifying authority `CRS`, and the complete relationship-details list. Keep direction and every publisher label; do not infer reciprocity or identical bill text.

**Done when.** Source array element counts reconcile with occurrences, repeated pairs remain visible in the occurrence view, and the pair view reproduces the earlier pinned coverage result. Malformed elements receive a recorded disposition. A retained fixture with multiple relationship details proves that only the first detail is not selected silently.

### T05 — Preserve and publish dated cosponsorship

Owner: SpicyDocs reader, then SpicyRegs table. Depends: T01. Covers: P08, P19.

**Change.** Extend the bill-status model with a source-faithful cosponsor type, or equivalent explicit cosponsor fields, rather than overloading the two-field `BillSponsor`. Preserve sponsorship date, original status, withdrawal date when supplied, and source party/state/district. Add a `bill_cosponsors` projection keyed by bill/source observation/list ordinal, with member ID as a reference.

**Raw → output.** `118-hr-1`, ordinal 0, `M001159`, `sponsorship_date=2023-03-14`, `is_original_raw=True`, state `WA`, district `5`. A missing withdrawal field stays missing; it does not prove that withdrawal never occurred.

**Done when.** Replaying the retained 49-entry cosponsor block preserves every entry and field. Add a retained positive withdrawal example before claiming withdrawal support. Test absent/empty lists, repeated member entries, malformed dates and historical member targets. Reconcile the published count using its documented inclusion of withdrawn entries. Deliver provider tests/pin first, consumer projection second.

### T06 — Expose meeting, hearing and publication references

Owner: SpicyRegs using SpicyDocs projections. Depends: T01, T02. Covers: P09.

**Change.** Reuse `schemas/congress_index_tables.py:shape_committee_meeting` and current meeting/hearing shapers. Publish independent occurrence views for meeting bills, hearing jackets, committees and offered documents, retaining meeting status and full scoped keys. Keep witness records and their native locators; infer no witness/document pairing.

**Raw → output.** Meeting `(119,house,119565)` yields nine bill occurrences, starting with `119-hr-1653`. Meeting `(119,house,119003)` yields jacket references `63019` and `64431`. These examples do not connect those jackets to the other meeting's bills.

**Done when.** Reconcile every array separately, preserve duplicates, resolve hearing jackets with Congress/chamber, and retain canceled/postponed status. The current shaper maps a missing detail field to `[]` once a detail object exists; it does not preserve absent/null/empty distinctions at that field. If the output promises those distinctions, add a raw-field-state projection and fixtures first. Do not fabricate them from existing arrays.

### T07 — Expose vote document references without pairing unrelated lists

Owner: SpicyRegs. Depends: T01, T02. Covers: P10.

**Change.** Reuse SpicyDocs `sources/congress/votes.py:parse_senate_vote`, `VoteDocument` and `VoteAmendment`. Expand documents and amendments into separately keyed occurrences. Route native PN references to nominations; route bills, amendments and treaties only under their respective source shapes.

**Raw → output.** Vote `119-senate-1-522`, document ordinal 0, native Congress `119`, native type `PN`, native number `55-25`, target citation `PN55-25`. The corresponding empty amendment block creates no amendment target.

**Done when.** The retained full vote produces all 48 document occurrences, including suffixes, with independent amendment observations. Preserve unsupported/missing targets and distinguish confirmations from other motion/question types. Add positive native amendment/treaty specimens before enabling their routing. Recompute missing-nomination coverage at the chosen publication; no default-current-Congress fallback.

### T08 — Connect members to source-listed FEC candidate IDs

Owner: SpicyRegs. Depends: T01, T02. Covers: P11.

**Change.** Expand `members.fec_ids_json` through a `member_fec_ids` view. Reuse SpicyDocs `sources/legislators.py:parse_legislators` and `schemas/legislator_tables.py:shape_member`; use existing FEC candidate/committee relationships for the next step.

**Raw → output.** Member `C000127` produces source-listed IDs `S8WA00194` and `H2WA01054`, each with its list ordinal and community-crosswalk provenance. Preserve the supported presidential candidate-ID shape too.

**Done when.** Reconcile the source list, check crosswalk conflicts, distinguish each candidate/committee/member path from distinct committees, and retain FEC cycle/designation fields where the source states them. No election cycle from ID spelling, no present authorization from historical linkage, and no donation claim. The earlier measured paths are a baseline at their pins, not a permanent expected count.

### T09 — Make FEC relationship evidence directly inspectable

Owner: SpicyRegs, reusing SpicyDocs metadata. Depends: T01. Covers: P12.

**Change.** Reuse `transforms/fec_relationships.py:api_relationships`, the source companion catalog, and SpicyDocs `sources/fec/metadata.py`. Provide a lookup from each relationship observation to its exact source file/digest/record pointer and native value.

**Raw → output.** The retained `C00008896` record has `committee.candidate_ids=[]`, a cycles array and designation `U`. It yields an `empty_list` observation at `/results/0/committee/candidate_ids`, not a candidate edge. Preserve nested source fields and exact decimal handling.

**Done when.** Verify existence and digest of each selected companion, not just non-null coordinates. Replay scalar, list, empty, absent, null and malformed cases. Keep candidate membership, committee designation, filing association and source observations as different relation types. The earlier coordinate-completeness measurement is not substituted for this byte-level check.

### T10 — Expose FCC filing membership and retain native role detail

Owner: SpicyRegs; existing SpicyDocs reader supplies raw captures. Depends: T01, T02. Covers: P14.

**Change.** First expose filing-to-proceeding occurrences from held arrays. Then extend the selected-field projection in `transforms/build_fcc_ecfs.py:_shape_filing` from retained `FccEcfsReader` records where native proceeding IDs or participant/asset fields were discarded. Keep filer, author and other roles separate.

**Raw → output.** Filing `26110074741` lists FCC proceeding `26-189`, native proceeding ID `1784669453334`, bureau `PSHSB`; filer `Scott Pingrey` has a name only in this specimen. Produce the membership with source ordinal and both native identifiers. A name-only filer remains a role observation, not a resolved person.

**Done when.** Match held arrays without losing repeats, qualify the native-ID/name relationship against the selected proceeding population, and never match an FCC short docket number to a court or Regulations.gov number by spelling alone. Obtain a retained nonempty document/participant specimen before claiming support for additional fields. Do not reacquire raw records already retained by the capture journal.

### T11 — Publish complete regulatory memberships and identifier candidates

Owner: SpicyRegs using SpicyDocs normalizers. Depends: T01, T02. Covers: P02, P03.

**Change.** Expand all native FR RINs/dockets, document additional RINs, agenda-edition membership, proceeding membership and comment-period memberships. Reuse `schemas/federal_register.py:project_federal_register_document`, `interpretation/identifier_shapes.py:normalize_docket_references`, `unpadded_federal_register_document_number`, and the existing proceeding/lifecycle builders. Retain native and normalized values side by side.

**Raw → output.** FR `(2026-17334,2026-08-25)` yields RINs `3206-AO36` and `3206-AO80` and each stated docket string. A document's posting date cannot supply a missing FR publication date. Route lifecycle date evidence by `dated_by`, rather than assuming its record's source supplied the date.

**Done when.** Source-array counts reconcile; each agenda match retains edition; shared RINs do not collapse separate actions. Preserve multiple candidate matches. Synthetic controls show `94-0190` and `94-190` both normalize to `94-190`; therefore that normalizer cannot establish unique identity. Preserve excluded historical docket mentions and SEC file numbers in raw evidence; broaden recognition only with a retained source cohort and a named rule.

### T12 — Publish structured CFR references and authority observations

Owner: SpicyDocs extraction qualification, then SpicyRegs resolution. Depends: T01, T02. Covers: P05.

**Change.** Project native FR `cfr_references`, parse explicit regulatory `cfrPart` values with the existing grammar, and add authority/source-note occurrences from `sources/cfr/authority.py:scan_ecfr_authority_notes`. Record part/section scope from XML ancestry and capture metadata. Keep authority and source-note relationships distinct.

**Raw → output.** `cfrPart="17 CFR Part 2"` yields title 17/part 2 without a section. The part-18 `AUTH` text yields a source-stated authority occurrence for `44 U.S.C. 1506` at its XML path, with `parse_status=partial`; the separate `SOURCE` text supplies `37 FR 23609`. Preserve the rest of both notes.

**Done when.** Complete the raw-to-output trace for both specimens, including input digest and unresolved portions. Scope title/date only from source metadata: the isolated part fragment itself does not state a CFR title. Qualify `PARAUTH`/`SECAUTH` and other unsupported forms separately before claiming complete authority coverage. Part-only references never become fabricated section targets; historical compilation and FR page citations use typed resolution.

### T13 — Publish native U.S. Code references and classification links

Owner: SpicyRegs projection using SpicyDocs scanners/normalizers. Depends: T01, T02. Covers: P06.

**Change.** Use `sources/uscode/references.py:scan_uscode_references` for native XML `ref` and `sourceCredit` observations. Reuse `schemas/tables.py:usc_section_key` for compatible section spellings and the held law-code/Table III records. Preserve source hrefs, fragment identifiers, release/edition and amendment-history context.

**Raw → output.** The retained section `/us/usc/t5/s423` names `/us/usc/t5/s401` in a native href; publish the occurrence and exact XML location. Its source credit states `Pub. L. 117–286, § 3(b), Dec. 27, 2022, 136 Stat. 4255`; retain that text as a separate historical assertion before extracting typed references.

**Done when.** Replay all 30 reference observations and the source credit in this fragment, retain unrecognized href forms, and label its unspecified release point as unknown. Test letter suffixes and dash forms while preserving raw spellings. Classification rows remain many-to-many and edition-aware; source credit does not automatically prove current legal effect. Report observed overlap separately from normalization gains—the previous sample showed no gain from normalization.

### T14 — Join spending recipients to SAM without multiplying money

Owner: SpicyRegs. Depends: T01. Covers: P13.

**Change.** Publish a distinct UEI identifier view and a separate UEI-to-SAM-registration relation. Reuse SpicyDocs SAM validation and the current SAM/USAspending shapers. Preserve provider IDs, recipient level, native UEI spelling, DUNS and registration EFT fields; treat each identifier system separately.

**Raw → output.** The retained SAM record has UEI `CJLMN78UULH4` and null EFT code; the retained USAspending record has a provider recipient ID ending `-R`, UEI `JE73CDQUAPA7`, and recipient level `R`. These two specimens illustrate different grains and are not claimed to match each other. On matching production UEIs, one recipient may lead to several registration rows.

**Done when.** Reconcile recipient counts and exact decimal totals before and after the entity-level enrichment. Report registration multiplicity separately. No arbitrary current/first registration selection, no null-to-empty identity rewrite, and no adding parent/child recipient totals as if disjoint. Validate the declared shape and disposition of malformed identifiers before any normalization.

### T15 — Expose the existing court graph with typed endpoints

Owner: SpicyRegs using SpicyDocs CourtListener readers. Depends: T01, T02. Covers: P15.

**Change.** Connect held courts, dockets, opinion clusters, opinions and citation edges using provider IDs and the current `build_court_*` transforms. Reuse `sources/courtlistener/{csv,local,search}.py`; keep search-result snippets distinct from acquired opinion text. Treat the separate Supreme Court source as a distinct identity route until a crosswalk is qualified.

**Raw → output.** A retained CourtListener search result identifies cluster `10960737`, docket `74714368`, court `connappct`, and nested opinion `11428342` with cited-opinion IDs. Publish native endpoint references with missing-target status; the snippet is not a full opinion body.

**Done when.** Validate endpoint ID types, direction and selected-population coverage in projected key batches. Resolve citations at the correct opinion/cluster level without substituting case-name matches. Start with retained target-ID cohorts; perform the full graph check as a bounded offline job, not repeated broad MCP joins. Missing targets feed T24.

### T16 — Publish a source-artifact reference index

Owner: SpicyDocs source rendition interfaces; SpicyRegs application index. Depends: T01. Covers: P21.

**Change.** Reuse source rendition/file-reference readers, including Regulations.gov `comment_rendition_rows`, GovInfo bodies, meeting documents and FCC raw document arrays. Publish `(source_key, source_version, role, ordinal, offered_url, format, source_size, retained_digest, acquisition_status)` with provider locators. Retain every alternative rendition and attachment role.

**Example output.** A source may offer a PDF URL while `retained_digest` is null and acquisition status is `not_acquired`; an acquired rendition points to its retained bytes and receipt. No URL-only record is described as a processed body.

**Done when.** Replay at least one nonempty native attachment/rendition case per enabled family, plus empty/missing cases, redirects and duplicate URLs in different roles. Offered, retained, parsed and refused states stay separate. Do not deduplicate away source occurrences or let T16 automatically fetch everything. T24 chooses bounded acquisitions using this index.

### T17 — Extend citation extraction through source-specific adapters

Owner: SpicyDocs reusable interpretation; SpicyRegs scheduling/writer. Depends: T02, T16. Covers: P17.

**Change.** Reuse `interpretation/citations.py:find_citations`, its grammar and `schemas/document_citation_tables.py`; extend the existing application citation write path rather than creating per-source citation stores. First qualify held bill/report text, then independently qualify FCC, LDA activity text and comment bodies. Each enabled family gets a bounded retained cohort, source context and a read-completion record.

**Raw → output.** `Public Law 114–254` in retained budget text yields a source-text digest, span, exact matched text and normalized key; T02 supplies target existence. LDA activity text `transportation and infrastructure funding` produces no bill identifier. Its prior covered-position text does not establish current employment.

**Done when.** Score precision and missed mentions against reviewed source spans, including negative and historical-Congress cases. The current citation code documents limits to fallback document Congress; qualify explicit/historical context and abstention before broad use. A successful zero-result reread clears only that complete source/text/rule scope; failed or capped reads preserve earlier findings. Bound body reads and checkpoint work; do not scan the entire comment corpus interactively.

### T18 — Preserve and expose structured report and communication references

Owner: SpicyDocs existing readers; SpicyRegs field projections. Depends: T02, T11. Covers: P18, P06, P20.

**Change.** Reuse `sources/congress/record_communications.py`, `interpretation/communication_rin.py`, report MODS readers and existing report-section tables. Project legal-authority, committee-referral, RIN and stated subject fields with their roles, rather than treating them all as undifferentiated text mentions. Preserve report requirements and communication identities under their complete source keys.

**Raw → output.** Retained House communication 4329, in `CREC-2016-02-12-pt1-PgH815-4`, states docket `OSHA-2015-0003`, RIN `1218-AC97`, `5 U.S.C. 801(a)(1)(A)`, Public Law `104-121`, and a committee referral. Emit separately located role-specific observations. Entry 4340 retains `General Counsel, Peace Corps` even though the existing official/agency splitter refuses to split it.

**Done when.** Replay the existing publisher-detail comparisons and source HTML, keeping joint referrals and unresolved splits. The current `rin_from_report_nature` returns only the first labeled RIN: retain its compatibility result and add an all-occurrences function only after qualifying a positive multi-RIN source specimen. Retain MODS fields directly where present; do not replace them with inferred prose values. Test source correction and repeated communication editions.

### T19 — Preserve party intervals and qualify dated roles

Owner: SpicyDocs source model, then SpicyRegs dated joins. Depends: T01. Covers: P19.

**Change.** Extend `sources/legislators.py:Term` and `schemas/legislator_tables.py` to preserve nested `party_affiliations` in source order. Publish member/term/affiliation occurrences, then use the selected source dates in the existing member-vote-term path. Review committee assignment date semantics separately before using them as historical membership.

**Raw → output.** `T000254`, term index 3, yields Democrat `1961-01-03` to `1964-09-16` and Republican `1964-09-16` to `1967-01-03`; retain the term-level `Republican` assertion separately. These are source-listed dates, not a newly inferred political history.

**Done when.** The retained historical record replays without losing either interval. State interval boundary semantics and test the transition date, overlaps, gaps and missing end dates. A current roster cannot fill historical unknowns. Distinguish a dated native affiliation from a fallback term-level value. Provider change/pin precedes rebuilding dated application outputs.

### T20 — Add namespaced agency and topic occurrence views

Owner: SpicyRegs consuming source vocabulary and RefSpec artifacts. Depends: T01, T02. Covers: P04, P20.

**Change.** Expand native FR agencies/topics, Congress subjects/policy areas, LDA issue/contacted-agency objects and regulatory keywords. Reuse existing source readers and the committed RefSpec agency projection/unresolved artifacts. Publish provider namespace/ID/label/version before any cross-source mapping; review proposed mapping rows with evidence and dates.

**Raw → output.** FR agency ID `406` and slug `personnel-management-office` stay FR identifiers. LDA activity index 0 in filing `a934e791-d564-4fd3-8b78-041f8cfcf115` has issue `BUD` and an empty government-entity list. Its description does not create a contacted-agency edge. The existing LDA activity/lobbyist rows already preserve indexes; reuse them.

**Done when.** Reconcile array occurrences and publish unmatched or contested mappings. Check Regulatory `display_properties_json` before interpreting a field: `organization` can mean “Pre-EDOCKET ID.” Cross-source topics are related concepts only under a reviewed mapping, not identical merely because labels match. Demonstrate a native-ID match, historical mapping and abstention for each enabled mapping route.

### T21 — Anchor each text difference to both exact source versions

Owner: SpicyRegs metadata/projection, reusing SpicyDocs diff output. Depends: T01. Covers: P24.

**Change.** Use `schemas/bill_diff_tables.py` and `interpretation/section_diff.py` outputs to expose exact from/to version identities and source-specific section locators. Include version source, text digests and rule revision; keep unchanged and unmatched section outcomes when the current output supports them.

**Example output.** A change row points independently to `(bill_id,from_version_code,from_source,from_section_locator)` and its corresponding complete `to_*` identity. Section numbering alone does not establish continuity between versions.

**Done when.** Both endpoints resolve on full keys with no row multiplication. Replay insertion, deletion, renumbering, moved text, equal version code from different providers and missing endpoint text. Change the diff algorithm only if a retained counterexample requires it; the immediate task is faithful endpoint navigation, not a new semantic amendment engine.

### T22 — Build an evidence-backed identity candidate queue

Owner: SpicyRegs review workflow; RefSpec for accepted reusable mappings. Depends: T09, T14, T20. Covers: P25.

**Change.** Keep current `org_committee_links` and other name matches as candidates with explicit method and status. Retrieve candidates using bounded normalized-name/address indexes and provider identifiers; record positive and conflicting evidence. Accepted links require a source identifier or a review decision with supporting sources and validity scope.

**Example output.** A name-only FCC filer or Regulations.gov organization produces `(source_observation, candidate_entity, match_features, evidence_locations, decision=pending)`. It does not silently become a SAM entity, FEC committee or lobbyist. Preserve the observed name exactly.

**Done when.** A retained review set includes same-name/different-entity, historical rename, parent/subsidiary and unresolved cases. Reviewed matches retain decision provenance and can be revoked/rebuilt. No all-pairs corpus comparison and no funds aggregation through pending links. Distinguish acting role, represented client, employer and recipient rather than flattening them into one organization relation.

### T23 — Qualify Senate expenditure line extraction before publishing payments

Owner: SpicyDocs reusable PDF extraction, then SpicyRegs interpretation. Depends: T01, T16. Covers: P26.

**Change.** Start from `schemas/senate_expenditure_tables.py`, `shape_senate_expenditure_rows`, `page_context`, `parse_amount` and `summary_totals`. Create a hand-checked line-level truth set on the retained PDF ranges before extending the parser. Preserve parent cells, bounding boxes, printed/source page mapping, header roles and section boundaries.

**Raw → candidate output.** Printed B-1243 lists document `DJST20250194`, payee `ERAJ SHIRVANI`, and three expense lines with separate amounts. B-1244 continues the office's statement without restating the office heading. Candidate output must distinguish document/payee groups from individual expense lines; no one-to-one zip of extracted names, dates and amounts is justified.

**Done when.** Visual annotation and parser output agree for multiline descriptions, repeated payees, subtotal/total rows, negative values and continuation pages. Carry office context only within a proven section boundary and keep the origin page of that context. Reconcile complete summary sections with decimal arithmetic and stated totals. Retain unsupported/unparsed blocks. Publish only qualified grids and reporting periods; this review does not cover the later compensation/mail-allocation sections.

### T24 — Acquire the missing targets and bodies that unlock useful links

Owner: SpicyDocs acquisition; SpicyRegs selection and publication. Depends: T02, T15, T16. Covers: P27.

**Change.** Produce a deduplicated missing-target/body queue from resolution and artifact states, grouped by provider/native identifier/edition. Rank by source occurrences affected, unique records affected and intended user query. Inspect retained bytes/catalogs before selecting an origin acquisition route. Start with the missing law/USC/GAO targets surfaced by the audit and selected court opinion bodies.

**Output.** A work item carries exact requested identity, source scope, requesting occurrences, acquisition route, retained receipt/digest, and successful/empty/refused/unavailable outcome. An acquired body advances to extraction and publication only after its source qualification checks.

**Done when.** Complete a bounded end-to-end cohort for each enabled source, including refusal and retry paths, then re-resolve only affected occurrences against the new publication. A search snippet, offered URL, schema/catalog entry or failed fetch cannot count as an acquired body. No false empty tables on authentication/source failure. Document remaining historical editions and population gaps.

### T25 — Make aggregate joins and coverage claims reproducible

Owner: SpicyRegs catalog/validation and existing ingestion receipts. Depends: T01. Covers: P23, P28.

**Change.** Attach source selection, measurement period, table grain and read/publication status to aggregate and operational views. Reconcile comments/index, agency statistics, lifecycle summaries, feed summaries and source-coverage/checkpoint records within their declared generation. Use catalog snapshots for bulk input selection and retain public export pins separately.

**Example output.** A comments-index row remains an `(agency_code,docket_id,year,month)` cohort, including nullable keys. A lifecycle date may cite FR even when the event was assembled from a Regulations.gov record. A successful source checkpoint reports its actual selection; it does not assert an entire archive was ingested.

**Done when.** Aggregate each side at the comparison grain before joining; use null-aware equality for nullable grouping keys; verify totals before/after enrichment. Tests distinguish failed, capped, valid empty, acquired, parsed and published states. Retain per-table snapshots when no atomic multi-table selection exists. Show user-facing coverage in the data dictionary/MCP response without claiming unversioned files have immutable identity.

## Validation and adoption

For each task, record the exact input selection, code/rule revision, raw observations, expected output, rejects/unresolved results, occurrence counts, distinct pairs, parent multiplicity and elapsed/query cost. Use retained source examples plus meaningful negative controls. Run the owning repository's required checks after implementation; this documentation proposal does not claim those future implementations passed them.

Before publication, replay the task's declared scope and inspect output rows directly against raw specimens. After publication, use the deployed MCP to read the new schema/view, a positive example, a missing/ambiguous example and source evidence. Keep code completion, source replay, data publication and deployed verification as separate completion fields.

The next implementation should start with T01/T02 and the explicit-link views. The raw review supports those decisions now. Cosponsor dates, dated party history, additional authority shapes and Senate payment lines have precise provider-side work to complete before their richer outputs can be trusted.
