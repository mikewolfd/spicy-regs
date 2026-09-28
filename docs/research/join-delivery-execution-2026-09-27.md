# Join delivery execution — 2026-09-27

The original proposal’s selected acceptance criteria are complete. The implementation is pushed, the fork MCP is deployed, and the selected data publications and dependent refreshes are verified. This does not establish complete historical source coverage; the remaining expansion work is listed below.

The [machine-readable ledger](join-delivery-execution-2026-09-27.json) retains exact inputs, publication pins, receipts, tests and prior deployment history. The [original proposal](join-delivery-tasks-2026-09-27.md) remains the acceptance baseline. SpicyDocs owns acquisition and source parsing; SpicyRegs owns application interpretation and publication.

## Verified delivery

- SpicyDocs 0.50.0 at `8844068` includes upstream Congress partition support and the qualified source readers. Two wheel builds match SHA-256 `c739bc6f6bd488ca2afcb37d1bbcff57f14c0abe051638d4679ed17abf9eb64b`; the consumer vendors that wheel. No package-registry upload.
- SpicyRegs application `3e423cd` runs as Worker `ece7e6c7-4f1f-4e36-a2af-f017d7c175ec`, image `d9d89058…`. All 22 final live checks passed, including restricted attachment records, alternative formats, explicit unread/empty states and file/write refusal. This verifies observed requests, not every running instance.
- Bill-family, FCC, native legal references, court PDFs and citation generations passed exact public byte readback. The ledger retains their bounded native source scopes and coverage gaps.
- Comments snapshot `50021510906269595` is published with 26,314,364 rows. Three corrected ODNI records passed public MCP readback while retaining native NULL dockets. [Hosted refresh 36356400996](https://github.com/mikewolfd/spicy-regs/actions/runs/36356400996) passed public and raw-catalog uniqueness, complete index coverage and dependent refresh verification.
- Documents generation `b4f1d751…` publishes fresh attachment observations for three documents: one with two attachment records and two confirmed empty. All 2,002,888 IDs and every legacy cell remain unchanged; all other attachment observations remain unread NULL. Working-copy publication used an expected-ETag condition, and managed publication used index compare-and-swap.
- Agency stats, monthly volume, feed summary, discovery signals and the rulemaking dataset were refreshed after the document publication. Base versions stayed unchanged during the refresh. All eight aggregate checks passed against exact hosted bytes with zero differing groups.
- Full repository validation passed: 10,392 provider tests and 3,407 consumer tests, plus lint, formatting, types, generated dictionary, Cloudflare and minimal-container checks. Hosted provider CI, consumer CI and live integration passed at the implementation commits.
- [SpicyRegs PR #1](https://github.com/mikewolfd/spicy-regs/pull/1) and [SpicyDocs PR #4](https://github.com/mikewolfd/spicy-docs/pull/4) are pushed draft reviews. Latest fork changes were integrated without dropping partitioned bill storage, refusal memory or qualified joins.

The [fork MCP server](https://spicy-regs-mcp.mdeeb.workers.dev/mcp) serves selected public Parquet files. Iceberg remains the ingestion, update and replay version boundary. Independent table generations do not form one atomic cross-table snapshot.

## Current task acceptance

### T01 — Correct join keys and publish measured cardinality

**Verified:** Full selected key columns measured at independent publication pins on 2026-09-27; version, report-part, hearing and both attribute joins report OK. Synthetic collisions cover full-key separation.

**Selected acceptance:** Complete for the stated source scope and input pins.

**Limits and follow-on work:** None for the selected full-key delivery. Measurements are tied to independent table pins; broader source completeness and an atomic cross-table snapshot are not claimed.

**Code:** `spicy-regs/src/spicy_regs/table_joins.py`; `spicy-regs/scripts/check_table_joins.py`; `spicy-regs/src/spicy_regs/join_measurements.json`; `spicy-docs/src/spicy_docs/schemas/bill_version_tables.py`; `spicy-docs/src/spicy_docs/schemas/bill_diff_tables.py`; `spicy-docs/src/spicy_docs/schemas/committee_report_tables.py`.

### T02 — Resolve citation targets against selected publications

**Verified:** All 52728 citations at 026be1bc have exact current parent text digests; c8e49dbb preserves them and adds 18 digest-current court findings. Eight-kind target coverage refreshed; GAO 1/337 occurrences now found.

**Selected acceptance:** Complete for the stated source scope and input pins.

**Limits and follow-on work:** Recompute dated coverage after future input changes. Broader extraction/acquisition routes remain separately scoped in T17/T24.

**Code:** `spicy-regs/src/spicy_regs/citation_resolution.py`; `spicy-regs/src/spicy_regs/mcp_server.py`.

### T03 — Recover source-stated comment-to-document links

**Verified:** Checked atomic MERGE and rollback qualified on live scratch catalog, including concurrent update, insert and commit conflicts. Three native ODNI rows corrected in snapshot 50021510906269595; independent replay found no remaining changes. Hosted 26,314,364-row mirror, public/index/partition ID coverage, raw catalog integrity, dependent refresh and public MCP readback all passed; source NULL dockets preserved.

**Selected acceptance:** Complete for the stated source scope and input pins.

**Limits and follow-on work:** None for the selected three-record correction. Wider source-reference rereads remain separate backfill scope.

**Code:** `spicy-docs/src/spicy_docs/schemas/regulations.py`; `spicy-regs/src/spicy_regs/schemas/regulations.py`; `spicy-regs/src/spicy_regs/relationship_views/comments.py`; `spicy-regs/src/spicy_regs/sources/iceberg.py`; `spicy-regs/src/spicy_regs/pipelines/repair_regulations.py`.

### T04 — Expose publisher-listed related bills

**Verified:** All 142698 retained related-bill occurrences conserved and resolve uniquely at selected pins; native multiple-details fixture retained.

**Selected acceptance:** Complete for the stated source scope and input pins.

**Limits and follow-on work:** Selected task acceptance complete; no reciprocal or identical-text relationship is inferred.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/congress.py`; `spicy-regs/src/spicy_regs/relationship_views/core.py`.

### T05 — Preserve and publish dated cosponsorship

**Verified:** Published 146248 exact native cosponsor occurrences including 67 withdrawals; all resolve unique member IDs. 146246 sponsorship dates match one half-open chamber term; 2 boundary-day cases remain explicit missing. All 16140 selected bill counts reconcile.

**Selected acceptance:** Complete for the stated source scope and input pins.

**Limits and follow-on work:** Selected task acceptance complete. 405325 held bills remain unread for this new field, and 19 source/held edition mismatches were skipped explicitly; those are later backfill scope.

**Code:** `spicy-docs/src/spicy_docs/sources/congress/bill_status.py`; `spicy-docs/src/spicy_docs/schemas/bill_tables.py`; `spicy-docs/src/spicy_docs/interpretation/bill_family.py`; `spicy-regs/src/spicy_regs/transforms/build_bill_family.py`.

### T06 — Expose meeting, hearing and publication references

**Verified:** All seven arrays reconcile across 6096 meetings; 3352 scoped jacket references yield 87 found/3265 missing/0 ambiguous.

**Selected acceptance:** Complete for the stated source scope and input pins.

**Limits and follow-on work:** Selected task acceptance complete. Earlier absent/null collapse is documented; no source states are invented from held empty arrays.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/congress.py`.

### T07 — Expose vote document references without pairing unrelated lists

**Verified:** Native Senate amendment/treaty fixtures qualify vote-native-routing/2. 3000 S.Amdt. routes yield 166 found/2834 missing; 22 typed treaty routes remain missing. Native treaty Congress stays 108 for vote 109.

**Selected acceptance:** Complete for the stated source scope and input pins.

**Limits and follow-on work:** Selected routing acceptance complete; broader target acquisition is T24 scope. Missing nomination targets remain explicit.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/congress.py`.

### T08 — Connect members to source-listed FEC candidate IDs

**Verified:** All 1738 member FEC occurrences valid candidate IDs; 935 have selected candidate records, 803 do not. No candidate ID is shared across bioguide IDs. P000619/H2AK01158 source page and native cycle arrays replay exactly.

**Selected acceptance:** Complete for the stated source scope and input pins.

**Limits and follow-on work:** Selected task acceptance complete; historical candidate linkage never implies current committee authorization or donation attribution.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/congress.py`.

### T09 — Make FEC relationship evidence directly inspectable

**Verified:** All 649 FEC collection counts reconcile to 13,717,161 source records and 183,390 relationships. Every relationship has one companion with matching recorded page digest; selected positive and empty native cases are byte-qualified. Native scalar, positive list, empty list and null fields replay from a fully rehashed retained OpenFEC page; absent and malformed cases remain synthetic tests.

**Selected acceptance:** Complete for the stated source scope and input pins.

**Limits and follow-on work:** None for the selected original acceptance. Selected byte-qualified replay and required shape tests pass; rehashing every original page is outside the selected acceptance.

Wider raw-byte qualification across relationship kinds remains; recorded digest equality is distinct from rehashing every original page.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/fec.py`; `spicy-regs/src/spicy_regs/transforms/fec_relationships.py`.

### T10 — Expose FCC filing membership and retain native role detail

**Verified:** All 5780 held FCC rows now source-qualified and published; one explicit proceeding-name correction, all other prior cells unchanged. Native proceeding references: 8,498 found, 9 missing, 154 unsupported. Three offered PDFs acquired with diagnostics.

**Selected acceptance:** Complete for the stated source scope and input pins.

**Limits and follow-on work:** Selected task acceptance complete. Complete FCC archive and attachment text coverage are not claimed.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/regulatory.py`; `spicy-regs/src/spicy_regs/relationship_views/fcc_native.py`; `spicy-regs/src/spicy_regs/transforms/build_fcc_ecfs.py`.

### T11 — Publish complete regulatory memberships and identifier candidates

**Verified:** Nine full membership arrays conserve selected source counts. All 135362 lifecycle date references resolve uniquely, including 72 Regulations.gov events dated by FR. 52092 agenda items retain editions; 5845 missing. Native month-only timetable values remain literal alongside application date policy.

**Selected acceptance:** Complete for the stated source scope and input pins.

**Limits and follow-on work:** Selected task acceptance complete; unsupported historical identifier forms remain source observations.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/regulatory.py`; `spicy-regs/src/spicy_regs/relationship_views/agenda.py`; `spicy-regs/src/spicy_regs/transforms/build_fr_docket_links.py`; `spicy-regs/src/spicy_regs/relationship_views/lifecycle_dates.py`.

### T12 — Publish structured CFR references and authority observations

**Verified:** Complete retained eCFR Title 1 requested as of 2026-08-10 admitted and published with 60 AUTH/SOURCE observations; exact enclosing bytes and capture metadata verified.

**Selected acceptance:** Complete for the stated source scope and input pins.

**Limits and follow-on work:** Selected source trace complete. PARAUTH/SECAUTH and other authority forms remain explicitly unsupported until separately qualified.

**Code:** `spicy-docs/src/spicy_docs/schemas/native_reference_rows.py`; `spicy-docs/src/spicy_docs/sources/cfr/authority.py`; `spicy-regs/src/spicy_regs/transforms/native_legal_references.py`; `spicy-regs/src/spicy_regs/pipelines/rollups/native_legal_references.py`.

### T13 — Publish native U.S. Code references and classification links

**Verified:** Complete retained USC Title 1 release 119-103 ZIP/XML admitted and published with 821 href/source-credit observations. Exact statute/public-law hrefs supported; scoped classification targets retain edition and many-to-many grain.

**Selected acceptance:** Complete for the stated source scope and input pins.

**Limits and follow-on work:** Selected task acceptance complete; full Code body coverage/current legal effect and other editions are not inferred.

**Code:** `spicy-docs/src/spicy_docs/schemas/native_reference_rows.py`; `spicy-docs/src/spicy_docs/sources/uscode/references.py`; `spicy-regs/src/spicy_regs/transforms/native_legal_references.py`; `spicy-regs/src/spicy_regs/pipelines/rollups/native_legal_references.py`.

### T14 — Join spending recipients to SAM without multiplying money

**Verified:** Recipient/SAM selected population verified: 188535 matched, 24,286 valid UEIs missing, 19,431 null/unsupported; 1935 matched recipients have registration multiplicity. Exact Decimal totals conserved separately for P/C/R levels; one native response row replays every field.

**Selected acceptance:** Complete for the stated source scope and input pins.

**Limits and follow-on work:** Selected task acceptance complete; no cross-level grand total or arbitrary current registration choice.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/entities.py`.

### T15 — Expose the existing court graph with typed endpoints

**Verified:** Full pinned graph checked offline within bounded memory. Missing endpoints: opinion–cluster 21; cluster–docket 10,067,451; citation citing 3,758 / cited 1; reporter–cluster 63; parenthetical described 0 / describing 618; no ambiguous endpoints. Three offered native PDFs match held SHA1 and publish derived text.

**Selected acceptance:** Complete for the stated source scope and input pins.

**Limits and follow-on work:** None for the selected original acceptance. Full selected typed graph checks pass. The original requires the independent Supreme Court route to stay separate until qualified, not to force a crosswalk.

Independent Supreme Court crosswalk remains unqualified; missing targets are acquisition candidates, not case-name matches.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/courts.py`.

### T16 — Publish a source-artifact reference index

**Verified:** FCC offered URLs now link to three retained-body digests and extraction outcomes; three court offered PDFs retain redirects, native fingerprints and derived-text outcomes. Source occurrences remain separate. Added a native-qualified document attachment relationship field and record/rendition views. Main formats keep the document_content role; restricted attachments survive without URLs; unread is NULL and validated complete empty is []. HTTP failure, pagination, forged document association and capture/record mismatch refuse. Published a bounded fresh source reread for three documents in complete documents generation b4f1d751. One document has two native attachment records; two have complete-empty lists; all other documents remain unread NULL. Every prior document identity and legacy cell is preserved. Exact public bytes, relationship rows and dependent refresh passed.

**Selected acceptance:** Complete for the stated source scope and input pins.

**Limits and follow-on work:** None for enabled bounded source-artifact acceptance. Remaining document attachments and additional bodies require explicit acquisition/backfill; publication does not imply global attachment completeness.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/artifacts_topics.py`; `spicy-regs/src/spicy_regs/relationship_views/congress.py`; `spicy-regs/src/spicy_regs/relationship_views/fcc_native.py`.

### T17 — Extend citation extraction through source-specific adapters

**Verified:** Added communication role fields and derived court PDF adapter through existing bounded writer. Court review retains 18 exact findings plus successful zero; an actual proclamation false positive caused source-specific docket-rule exclusion. Complete rereads preserve correction/zero/failure semantics.

**Selected acceptance:** Complete for the stated source scope and input pins.

**Limits and follow-on work:** None for the selected original acceptance. Qualified enabled cohorts pass reviewed-span, historical-context and scoped correction/failure checks. Additional bodies and broader recall are expansion scope.

Wider reviewed recall and additional body families remain. Committee names, executive orders, verbose FR and exact statutory-note targets retain documented limitations; comments publication now verified; no new comment-body extraction cohort claimed.

**Code:** `spicy-docs/src/spicy_docs/interpretation/citations.py`; `spicy-docs/src/spicy_docs/schemas/document_citation_tables.py`; `spicy-regs/src/spicy_regs/citation_sources.py`; `spicy-regs/src/spicy_regs/transforms/held_citations.py`; `spicy-regs/src/spicy_regs/pipelines/rollups/held_citations.py`; `spicy-regs/src/spicy_regs/mcp_server.py`.

### T18 — Preserve and expose structured report and communication references

**Verified:** Published 12 role-specific communication authority/report-description findings with exact spans and pins; missing printed entries stay unread. Prior all-RIN route remains published and qualified. Retained native HTML preserves three-way joint referrals and the unresolved Peace Corps sender. Two acquired native 2004 editions contain the same 26 communication identities; merge retains 26 current rows and both originals. No actual publisher correction is claimed.

**Selected acceptance:** Complete for the stated source scope and input pins.

**Limits and follow-on work:** None for the bounded original acceptance. Actual publisher corrections were not observed; separate constructed controls verify correction behavior.

**Code:** `spicy-docs/src/spicy_docs/interpretation/communication_rin.py`; `spicy-docs/src/spicy_docs/schemas/congress_index_tables.py`; `spicy-regs/src/spicy_regs/transforms/build_congress_index.py`; `spicy-regs/src/spicy_regs/relationship_views/congress.py`.

### T19 — Preserve party intervals and qualify dated roles

**Verified:** All 59 native affiliations resolve unique terms and valid interval dates. Existing transition/overlap/gap/fallback tests and public dated example retained.

**Selected acceptance:** Complete for the stated source scope and input pins.

**Limits and follow-on work:** Selected affiliation task acceptance complete. Committee-assignment historical semantics remain a distinct unqualified route.

**Code:** `spicy-docs/src/spicy_docs/sources/legislators.py`; `spicy-docs/src/spicy_docs/schemas/legislator_tables.py`; `spicy-regs/src/spicy_regs/transforms/build_members.py`; `spicy-regs/src/spicy_regs/pipelines/rollups/members.py`; `spicy-regs/src/spicy_regs/relationship_views/affiliations.py`.

### T20 — Add namespaced agency and topic occurrence views

**Verified:** Exact agency lookup exposed through the application/MCP with digest-admitted owner files, current-lineage evidence and explicit temporal/topic abstentions; minimal runtime excludes PyArrow. Owner-approved REF-072 HCFA to CMS transition provides the historical example in both enabled namespaces; native positive lookups and dated abstentions verified. Signature-date evidence stays distinct from identity validity.

**Selected acceptance:** Complete for the stated source scope and input pins.

**Limits and follow-on work:** None for enabled agency routes. Arbitrary-date identity intervals and new cross-source topic mappings remain separate expansion scope.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/artifacts_topics.py`; `spicy-regs/src/spicy_regs/vocabulary_mapping.py`.

### T21 — Anchor each text difference to both exact source versions

**Verified:** All 28431 pairs have unique full-key version endpoints; 881383 from and 1,076,723 to section endpoints have exact digest matches. Missing sides remain explicit. Native HR5334 covers additions/removals/move/renumber with changed text.

**Selected acceptance:** Complete for the stated source scope and input pins.

**Limits and follow-on work:** Selected task acceptance complete; no new semantic amendment interpretation or identity from section numbering.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/diffs.py`; `spicy-docs/src/spicy_docs/schemas/bill_diff_tables.py`.

### T22 — Build an evidence-backed identity candidate queue

**Verified:** Real three-candidate GOA review retained: one rejected for insufficient identity evidence, two pending, no accepted money-attribution edge. Review decisions retain pinned hash-chain provenance. Four same-ID FEC name-change candidates were found in a fixed native page cohort; dated rename remains unqualified. Native lobbying roles, SAM legal/DBA names and connected-organization names do not establish parent/subsidiary identity. SEC native formerNames supplies a scoped historical-name observation; primary SEC exhibit review supplies direct-or-indirect subsidiary and shared-trade-name/distinct-entity cases. The exhibit is retained as a bounded web review; direct raw HTTP returned 403. The review workflow preserves revocation/rebuild provenance and no pending link attributes funds.

**Selected acceptance:** Complete for the stated source scope and input pins.

**Limits and follow-on work:** None for the original retained review-set acceptance. No reusable cross-source mapping or money attribution is admitted; wider entity populations and raw exhibit replay remain separate.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/identity_candidates.py`; `spicy-regs/src/spicy_regs/identity_review.py`.

### T23 — Qualify Senate expenditure line extraction before publishing payments

**Verified:** Both retained complete summary sections reconcile all seven monetary columns with Decimal difference 0.00. Native negative amounts and C/D grids reviewed; missing page-local office/layout support stays refused. Complete retained B-1243–B-1245 office section: travel detail 13,713.22 and asset detail 69,709.04 exactly match native category totals. The implemented digest-pinned ReviewedOfficeSection produces 56 partial candidates on B-1243–1245, preserves office origin B-1243/source page17, and stops before B-1246. Independent review confirms the original continuation criterion is met.

**Selected acceptance:** Complete for the stated source scope and input pins.

**Limits and follow-on work:** None for the bounded original qualification behavior. Numeric document IDs, unpriced lines, negative payment candidates and whole-office reconstruction remain unsupported; partial output retains publication_qualified=false.

**Code:** `spicy-docs/src/spicy_docs/reading/senate_payment_review.py`; `spicy-docs/tests/fixtures/senate_expenditures/payment-review-2026-09-27.json`; `spicy-docs/src/spicy_docs/reading/senate_payment_candidates.py`.

### T24 — Acquire the missing targets and bodies that unlock useful links

**Verified:** GAO target metadata, complete native legal inputs and three court PDF bodies are now published; 18 court citations re-resolved incrementally. Fingerprints, extraction outcomes and failed initial candidates retained separately.

**Selected acceptance:** Complete for the stated source scope and input pins.

**Limits and follow-on work:** None for the selected original acceptance. Bounded enabled source cohorts completed acquisition, publication and affected-occurrence re-resolution with refusal/retry evidence. Additional targets and historical editions remain documented expansion scope.

Broader source/edition cohorts and missing targets remain. GAO PDF bytes are evidence without a published derived-text field; no source-wide completeness claim.

**Code:** `spicy-regs/src/spicy_regs/acquisition_queue.py`; `spicy-docs/src/spicy_docs/sources/gao/files.py`; `spicy-regs/src/spicy_regs/transforms/build_gao_target.py`; `spicy-docs/src/spicy_docs/sources/gao/product_metadata.py`.

### T25 — Make aggregate joins and coverage claims reproducible

**Verified:** Materialized and catalog-export lineage now supported. Agency-comment, feed-comment, lifecycle checks pass at exact pins; public comments-index scan timed out and local mirror check succeeds. Checkpoints distinguish 96 listings, legacy v2 markers (no current v3 reusable completion), 40 refusal maps, empty backfills, 290 complete / 2 size-refused report reads and 2 complete native legal reads. Post-refresh exact hosted files passed all eight aggregate checks with zero differing groups; comments/index reconcile 26,314,364 rows. Original local candidate physical bytes differed and were not used to certify hosted files. After attachment publication and all declared document-dependent refreshes, all eight checks passed again at fresh exact pins, with zero mismatched groups and stable public versions.

**Selected acceptance:** Complete for the stated source scope and input pins.

**Limits and follow-on work:** None for selected acceptance. Exact hosted comparisons, resource bounds, lineage and differentiated processing states pass.

Measurements belong to exact independent table pins, not a globally atomic snapshot or complete source population.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/core.py`; `spicy-regs/src/spicy_regs/mcp_server.py`; `spicy-regs/src/spicy_regs/join_measurements.json`; `spicy-regs/src/spicy_regs/aggregate_checks.py`; `spicy-regs/scripts/check_table_joins.py`.

## Remaining expansion work

- Backfill unread cosponsorship and attachment observations through explicit native rereads; selected publications do not cover all historical source records.
- Acquire missing citation, court and legislative targets in bounded cohorts, retaining refusal and successful-empty outcomes.
- Qualify arbitrary-date agency identity intervals only when owner evidence supports them; preserve existing historical-transition evidence and dated abstentions.
- Extend identity review to additional real source populations without attributing funds through pending or name-only links.
- Extend Senate expenditure layout coverage and reconcile complete payment populations before publishing partial candidates as payments.

## Evidence and interpretation limits

Counts are dated measurements at exact pins. Earlier failed attempts, incomplete candidates and old deployments remain in the JSON as history; the current task fields and finalization records supersede their pending states.

T20’s historical example is the owner-reviewed HCFA-to-CMS transition, supported by the [Federal Register reorganization notice](https://www.federalregister.gov/documents/2001/07/05/01-16800/centers-for-medicare-and-medicaid-services-statement-of-organization-functions-and-delegations-of). The retained signature-date basis does not become an identity validity interval. Arbitrary-date lookup still abstains.

T22’s review set includes a native SEC former-name observation, unresolved GOA candidates, and a primary [SEC subsidiary exhibit](https://www.sec.gov/Archives/edgar/data/6201/000000620125000010/ex211q42410k.htm) showing separately named subsidiaries that share the American Eagle trade name. The exhibit was reviewed through the web reader; direct raw retrieval returned HTTP 403. Ownership is direct or indirect as reported. No cross-source identity or funds attribution follows from these review specimens.

Senate candidates remain explicitly unqualified for payment publication where their layout or whole-office reconciliation is unsupported. Citation target coverage is separate from extraction recall and source completeness. Offered attachments remain separate from retained bytes and parsed bodies.

Absolute receipt paths in the JSON identify retained local evidence, not public release links.
