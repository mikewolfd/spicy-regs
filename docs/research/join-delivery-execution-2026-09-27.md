# Join delivery execution — 2026-09-27

Third-wave source qualification and selected data publications are verified. Third-wave application deployment and 17 bounded public MCP checks passed; the serialized comments refresh is running. The full proposal is not complete: each task below separates its selected acceptance from wider source coverage and remaining work.

The [machine-readable ledger](join-delivery-execution-2026-09-27.json) retains exact evidence, publication pins, tests and prior deployment history. This ledger follows the [proposal](join-delivery-tasks-2026-09-27.md). Source behavior belongs to SpicyDocs; application behavior belongs to SpicyRegs.

## Current delivery boundary

- Provider: SpicyDocs 0.48.1 at `8bcfef5`; two identical wheel builds with SHA-256 `6f5ca008096fe60765a49183345fc78acde2ffa99c2451720d53c14a5b554a0b`. No registry upload.
- Third-wave bill-family, FCC, native legal references, court PDF extractions and citation generations are published, with all public member bytes verified. Exact pins and receipts are in `finalization.third_wave_publications`.
- Comments: three native ODNI rows were corrected in catalog snapshot `50021510906269595`, changing twelve intended cells. Independent replay found no remaining changes. The full 26,314,364-row mirror is validated locally; hosted mirror publication and consumer refresh remain pending.
- Serving: app `4466d85`, Worker `16bf8167-287e-4d59-ad43-2903fb4d301a`, image `77573fd0…`. All 17 bounded public checks passed after the replacement container started; the interrupted initial rollout probe is retained. This verifies observed requests, not every instance.
- Code: [SpicyRegs PR #1](https://github.com/mikewolfd/spicy-regs/pull/1) and [SpicyDocs PR #4](https://github.com/mikewolfd/spicy-docs/pull/4) are pushed as draft reviews. Provider gate and 3,361 consumer tests passed, plus lint, types, dictionary, strict docs, Cloudflare and minimal-container checks.
- Comments refresh: [run 36356400996](https://github.com/mikewolfd/spicy-regs/actions/runs/36356400996) holds the shared writer lock; base publications passed and mirror export is running.
- Concurrent write protection: two live catalog connections verified refusal of intervening updates, unexpected inserts and a commit after the prior-row check. The latter returned a catalog snapshot conflict (HTTP 409); intended NULL replacement passed. Scratch tables were removed.
- Public publication, local qualification and deployed behavior are separate claims. Independent table generations do not form an atomic cross-table snapshot. Source commits are pushed in [SpicyDocs PR #4](https://github.com/mikewolfd/spicy-docs/pull/4); package-registry release remains separate.

The endpoint is [the fork MCP server](https://spicy-regs-mcp.mdeeb.workers.dev/mcp). It serves selected public Parquet files. Iceberg remains the ingestion and replay version boundary.

## Current task acceptance

### T01 — Correct join keys and publish measured cardinality

**Verified:** Full selected key columns measured at independent publication pins on 2026-09-27; version, report-part, hearing and both attribute joins report OK. Synthetic collisions cover full-key separation.

**Remaining:** None for the selected full-key delivery. Measurements are tied to independent table pins; broader source completeness and an atomic cross-table snapshot are not claimed.

**Selected acceptance:** Complete at the stated input pins.

**Code:** `spicy-regs/src/spicy_regs/table_joins.py`; `spicy-regs/scripts/check_table_joins.py`; `spicy-regs/src/spicy_regs/join_measurements.json`; `spicy-docs/src/spicy_docs/schemas/bill_version_tables.py`; `spicy-docs/src/spicy_docs/schemas/bill_diff_tables.py`; `spicy-docs/src/spicy_docs/schemas/committee_report_tables.py`.

### T02 — Resolve citation targets against selected publications

**Verified:** All 52728 citations at 026be1bc have exact current parent text digests; c8e49dbb preserves them and adds 18 digest-current court findings. Eight-kind target coverage refreshed; GAO 1/337 occurrences now found.

**Remaining:** Recompute dated coverage after future input changes. Broader extraction/acquisition routes remain separately scoped in T17/T24.

**Selected acceptance:** Complete at the stated input pins.

**Code:** `spicy-regs/src/spicy_regs/citation_resolution.py`; `spicy-regs/src/spicy_regs/mcp_server.py`.

### T03 — Recover source-stated comment-to-document links

**Verified:** Checked atomic MERGE/rollback qualified on live scratch catalog. Three native ODNI rows corrected in snapshot 50021510906269595; independent reread finds zero changes. Full 26314364-row mirror built and validated locally.

**Remaining:** Serialize and verify hosted mirror publication/consumer refresh; wider source-reference rereads remain outside the three-record correction.

**Selected acceptance:** Remaining acceptance is explicit; full task completion is not claimed.

**Code:** `spicy-docs/src/spicy_docs/schemas/regulations.py`; `spicy-regs/src/spicy_regs/schemas/regulations.py`; `spicy-regs/src/spicy_regs/relationship_views/comments.py`; `spicy-regs/src/spicy_regs/sources/iceberg.py`; `spicy-regs/src/spicy_regs/pipelines/repair_regulations.py`.

### T04 — Expose publisher-listed related bills

**Verified:** All 142698 retained related-bill occurrences conserved and resolve uniquely at selected pins; native multiple-details fixture retained.

**Remaining:** Selected task acceptance complete; no reciprocal or identical-text relationship is inferred.

**Selected acceptance:** Complete at the stated input pins.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/congress.py`; `spicy-regs/src/spicy_regs/relationship_views/core.py`.

### T05 — Preserve and publish dated cosponsorship

**Verified:** Published 146248 exact native cosponsor occurrences including 67 withdrawals; all resolve unique member IDs. 146246 sponsorship dates match one half-open chamber term; 2 boundary-day cases remain explicit missing. All 16140 selected bill counts reconcile.

**Remaining:** Selected task acceptance complete. 405325 held bills remain unread for this new field, and 19 source/held edition mismatches were skipped explicitly; those are later backfill scope.

**Selected acceptance:** Complete at the stated input pins.

**Code:** `spicy-docs/src/spicy_docs/sources/congress/bill_status.py`; `spicy-docs/src/spicy_docs/schemas/bill_tables.py`; `spicy-docs/src/spicy_docs/interpretation/bill_family.py`; `spicy-regs/src/spicy_regs/transforms/build_bill_family.py`.

### T06 — Expose meeting, hearing and publication references

**Verified:** All seven arrays reconcile across 6096 meetings; 3352 scoped jacket references yield 87 found/3265 missing/0 ambiguous.

**Remaining:** Selected task acceptance complete. Earlier absent/null collapse is documented; no source states are invented from held empty arrays.

**Selected acceptance:** Complete at the stated input pins.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/congress.py`.

### T07 — Expose vote document references without pairing unrelated lists

**Verified:** Native Senate amendment/treaty fixtures qualify vote-native-routing/2. 3000 S.Amdt. routes yield 166 found/2834 missing; 22 typed treaty routes remain missing. Native treaty Congress stays 108 for vote 109.

**Remaining:** Selected routing acceptance complete; broader target acquisition is T24 scope. Missing nomination targets remain explicit.

**Selected acceptance:** Complete at the stated input pins.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/congress.py`.

### T08 — Connect members to source-listed FEC candidate IDs

**Verified:** All 1738 member FEC occurrences valid candidate IDs; 935 have selected candidate records, 803 do not. No candidate ID is shared across bioguide IDs. P000619/H2AK01158 source page and native cycle arrays replay exactly.

**Remaining:** Selected task acceptance complete; historical candidate linkage never implies current committee authorization or donation attribution.

**Selected acceptance:** Complete at the stated input pins.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/congress.py`.

### T09 — Make FEC relationship evidence directly inspectable

**Verified:** All 649 FEC collection counts reconcile to 13,717,161 source records and 183,390 relationships. Every relationship has one companion with matching recorded page digest; selected positive and empty native cases are byte-qualified. Native scalar, positive list, empty list and null fields replay from a fully rehashed retained OpenFEC page; absent and malformed cases remain synthetic tests.

**Selected acceptance:** Complete. Selected byte-qualified replay and required shape tests pass; rehashing every original page is outside the selected acceptance.

**Expansion limits:** Wider raw-byte qualification across relationship kinds remains; recorded digest equality is distinct from rehashing every original page.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/fec.py`; `spicy-regs/src/spicy_regs/transforms/fec_relationships.py`.

### T10 — Expose FCC filing membership and retain native role detail

**Verified:** All 5780 held FCC rows now source-qualified and published; one explicit proceeding-name correction, all other prior cells unchanged. Native proceeding references: 8,498 found, 9 missing, 154 unsupported. Three offered PDFs acquired with diagnostics.

**Remaining:** Selected task acceptance complete. Complete FCC archive and attachment text coverage are not claimed.

**Selected acceptance:** Complete at the stated input pins.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/regulatory.py`; `spicy-regs/src/spicy_regs/relationship_views/fcc_native.py`; `spicy-regs/src/spicy_regs/transforms/build_fcc_ecfs.py`.

### T11 — Publish complete regulatory memberships and identifier candidates

**Verified:** Nine full membership arrays conserve selected source counts. All 135362 lifecycle date references resolve uniquely, including 72 Regulations.gov events dated by FR. 52092 agenda items retain editions; 5845 missing. Native month-only timetable values remain literal alongside application date policy.

**Remaining:** Selected task acceptance complete; unsupported historical identifier forms remain source observations.

**Selected acceptance:** Complete at the stated input pins.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/regulatory.py`; `spicy-regs/src/spicy_regs/relationship_views/agenda.py`; `spicy-regs/src/spicy_regs/transforms/build_fr_docket_links.py`; `spicy-regs/src/spicy_regs/relationship_views/lifecycle_dates.py`.

### T12 — Publish structured CFR references and authority observations

**Verified:** Complete retained eCFR Title 1 requested as of 2026-08-10 admitted and published with 60 AUTH/SOURCE observations; exact enclosing bytes and capture metadata verified.

**Remaining:** Selected source trace complete. PARAUTH/SECAUTH and other authority forms remain explicitly unsupported until separately qualified.

**Selected acceptance:** Complete at the stated input pins.

**Code:** `spicy-docs/src/spicy_docs/schemas/native_reference_rows.py`; `spicy-docs/src/spicy_docs/sources/cfr/authority.py`; `spicy-regs/src/spicy_regs/transforms/native_legal_references.py`; `spicy-regs/src/spicy_regs/pipelines/rollups/native_legal_references.py`.

### T13 — Publish native U.S. Code references and classification links

**Verified:** Complete retained USC Title 1 release 119-103 ZIP/XML admitted and published with 821 href/source-credit observations. Exact statute/public-law hrefs supported; scoped classification targets retain edition and many-to-many grain.

**Remaining:** Selected task acceptance complete; full Code body coverage/current legal effect and other editions are not inferred.

**Selected acceptance:** Complete at the stated input pins.

**Code:** `spicy-docs/src/spicy_docs/schemas/native_reference_rows.py`; `spicy-docs/src/spicy_docs/sources/uscode/references.py`; `spicy-regs/src/spicy_regs/transforms/native_legal_references.py`; `spicy-regs/src/spicy_regs/pipelines/rollups/native_legal_references.py`.

### T14 — Join spending recipients to SAM without multiplying money

**Verified:** Recipient/SAM selected population verified: 188535 matched, 24,286 valid UEIs missing, 19,431 null/unsupported; 1935 matched recipients have registration multiplicity. Exact Decimal totals conserved separately for P/C/R levels; one native response row replays every field.

**Remaining:** Selected task acceptance complete; no cross-level grand total or arbitrary current registration choice.

**Selected acceptance:** Complete at the stated input pins.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/entities.py`.

### T15 — Expose the existing court graph with typed endpoints

**Verified:** Full pinned graph checked offline within bounded memory. Missing endpoints: opinion–cluster 21; cluster–docket 10,067,451; citation citing 3,758 / cited 1; reporter–cluster 63; parenthetical described 0 / describing 618; no ambiguous endpoints. Three offered native PDFs match held SHA1 and publish derived text.

**Selected acceptance:** Complete. Full selected typed graph checks pass. The original requires the independent Supreme Court route to stay separate until qualified, not to force a crosswalk.

**Expansion limits:** Independent Supreme Court crosswalk remains unqualified; missing targets are acquisition candidates, not case-name matches.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/courts.py`.

### T16 — Publish a source-artifact reference index

**Verified:** FCC offered URLs now link to three retained-body digests and extraction outcomes; three court offered PDFs retain redirects, native fingerprints and derived-text outcomes. Source occurrences remain separate. Added a native-qualified document attachment relationship field and record/rendition views. Main formats keep the document_content role; restricted attachments survive without URLs; unread is NULL and validated complete empty is []. HTTP failure, pagination, forged document association and capture/record mismatch refuse.

**Remaining:** Publish a bounded, source-edition-associated attachment backfill before claiming this new relationship is available on public documents. Existing public schema is explicitly unsupported for the attachment views; native fixture and local view qualification do not prove a public backfill.

**Selected acceptance:** Publication remains pending.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/artifacts_topics.py`; `spicy-regs/src/spicy_regs/relationship_views/congress.py`; `spicy-regs/src/spicy_regs/relationship_views/fcc_native.py`.

### T17 — Extend citation extraction through source-specific adapters

**Verified:** Added communication role fields and derived court PDF adapter through existing bounded writer. Court review retains 18 exact findings plus successful zero; an actual proclamation false positive caused source-specific docket-rule exclusion. Complete rereads preserve correction/zero/failure semantics.

**Selected acceptance:** Complete. Qualified enabled cohorts pass reviewed-span, historical-context and scoped correction/failure checks. Additional bodies and broader recall are expansion scope.

**Expansion limits:** Wider reviewed recall and additional body families remain. Committee names, executive orders, verbose FR and exact statutory-note targets retain documented limitations; comment-publication input remains pending.

**Code:** `spicy-docs/src/spicy_docs/interpretation/citations.py`; `spicy-docs/src/spicy_docs/schemas/document_citation_tables.py`; `spicy-regs/src/spicy_regs/citation_sources.py`; `spicy-regs/src/spicy_regs/transforms/held_citations.py`; `spicy-regs/src/spicy_regs/pipelines/rollups/held_citations.py`; `spicy-regs/src/spicy_regs/mcp_server.py`.

### T18 — Preserve and expose structured report and communication references

**Verified:** Published 12 role-specific communication authority/report-description findings with exact spans and pins; missing printed entries stay unread. Prior all-RIN route remains published and qualified. Retained native HTML preserves three-way joint referrals and the unresolved Peace Corps sender. Two acquired native 2004 editions contain the same 26 communication identities; merge retains 26 current rows and both originals. No actual publisher correction is claimed.

**Remaining:** None for the bounded original acceptance. Actual publisher corrections were not observed; separate constructed controls verify correction behavior.

**Selected acceptance:** Complete for the bounded original scope.

**Code:** `spicy-docs/src/spicy_docs/interpretation/communication_rin.py`; `spicy-docs/src/spicy_docs/schemas/congress_index_tables.py`; `spicy-regs/src/spicy_regs/transforms/build_congress_index.py`; `spicy-regs/src/spicy_regs/relationship_views/congress.py`.

### T19 — Preserve party intervals and qualify dated roles

**Verified:** All 59 native affiliations resolve unique terms and valid interval dates. Existing transition/overlap/gap/fallback tests and public dated example retained.

**Remaining:** Selected affiliation task acceptance complete. Committee-assignment historical semantics remain a distinct unqualified route.

**Selected acceptance:** Complete at the stated input pins.

**Code:** `spicy-docs/src/spicy_docs/sources/legislators.py`; `spicy-docs/src/spicy_docs/schemas/legislator_tables.py`; `spicy-regs/src/spicy_regs/transforms/build_members.py`; `spicy-regs/src/spicy_regs/pipelines/rollups/members.py`; `spicy-regs/src/spicy_regs/relationship_views/affiliations.py`.

### T20 — Add namespaced agency and topic occurrence views

**Verified:** Exact agency lookup exposed through the application/MCP with digest-admitted owner files, current-lineage evidence and explicit temporal/topic abstentions; minimal runtime excludes PyArrow.

**Remaining:** Historical event dates do not establish validity intervals; topic mapping and dated identity routes require separate owner evidence.

**Selected acceptance:** Remaining acceptance is explicit; full task completion is not claimed.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/artifacts_topics.py`; `spicy-regs/src/spicy_regs/vocabulary_mapping.py`.

### T21 — Anchor each text difference to both exact source versions

**Verified:** All 28431 pairs have unique full-key version endpoints; 881383 from and 1,076,723 to section endpoints have exact digest matches. Missing sides remain explicit. Native HR5334 covers additions/removals/move/renumber with changed text.

**Remaining:** Selected task acceptance complete; no new semantic amendment interpretation or identity from section numbering.

**Selected acceptance:** Complete at the stated input pins.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/diffs.py`; `spicy-docs/src/spicy_docs/schemas/bill_diff_tables.py`.

### T22 — Build an evidence-backed identity candidate queue

**Verified:** Real three-candidate GOA review retained: one rejected for insufficient identity evidence, two pending, no accepted money-attribution edge. Review decisions retain pinned hash-chain provenance. Four same-ID FEC name-change candidates were found in a fixed native page cohort; dated rename remains unqualified. Native lobbying roles, SAM legal/DBA names and connected-organization names do not establish parent/subsidiary identity.

**Remaining:** Historical rename/parent-subsidiary truth sets and source-supported accepted mappings remain incomplete; pending links cannot carry funds.

**Selected acceptance:** Remaining acceptance is explicit; full task completion is not claimed.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/identity_candidates.py`; `spicy-regs/src/spicy_regs/identity_review.py`.

### T23 — Qualify Senate expenditure line extraction before publishing payments

**Verified:** Both retained complete summary sections reconcile all seven monetary columns with Decimal difference 0.00. Native negative amounts and C/D grids reviewed; missing page-local office/layout support stays refused. Complete retained B-1243–B-1245 office section: travel detail 13,713.22 and asset detail 69,709.04 exactly match native category totals. The implemented digest-pinned ReviewedOfficeSection produces 56 partial candidates on B-1243–1245, preserves office origin B-1243/source page17, and stops before B-1246. Independent review confirms the original continuation criterion is met.

**Remaining:** None for the bounded original qualification behavior. Numeric document IDs, unpriced lines, negative payment candidates and whole-office reconstruction remain unsupported; partial output retains publication_qualified=false.

**Selected acceptance:** Complete for the bounded original scope.

**Code:** `spicy-docs/src/spicy_docs/reading/senate_payment_review.py`; `spicy-docs/tests/fixtures/senate_expenditures/payment-review-2026-09-27.json`; `spicy-docs/src/spicy_docs/reading/senate_payment_candidates.py`.

### T24 — Acquire the missing targets and bodies that unlock useful links

**Verified:** GAO target metadata, complete native legal inputs and three court PDF bodies are now published; 18 court citations re-resolved incrementally. Fingerprints, extraction outcomes and failed initial candidates retained separately.

**Selected acceptance:** Complete. Bounded enabled source cohorts completed acquisition, publication and affected-occurrence re-resolution with refusal/retry evidence. Additional targets and historical editions remain documented expansion scope.

**Expansion limits:** Broader source/edition cohorts and missing targets remain. GAO PDF bytes are evidence without a published derived-text field; no source-wide completeness claim.

**Code:** `spicy-regs/src/spicy_regs/acquisition_queue.py`; `spicy-docs/src/spicy_docs/sources/gao/files.py`; `spicy-regs/src/spicy_regs/transforms/build_gao_target.py`; `spicy-docs/src/spicy_docs/sources/gao/product_metadata.py`.

### T25 — Make aggregate joins and coverage claims reproducible

**Verified:** Materialized and catalog-export lineage now supported. Agency-comment, feed-comment, lifecycle checks pass at exact pins; public comments-index scan timed out and local mirror check succeeds. Checkpoints distinguish 96 listings, legacy v2 markers (no current v3 reusable completion), 40 refusal maps, empty backfills, 290 complete / 2 size-refused report reads and 2 complete native legal reads.

**Selected acceptance:** Complete. Exact selected comparisons, resource bounds, lineage and differentiated processing states pass. Current hosted mirror refresh is tracked separately as a freshness/publication gate.

**Expansion limits:** Public comments/index and dependent-output reconciliation awaits serialized mirror refresh. MCP metadata declares checks/grain/policy without inventing live measurements.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/core.py`; `spicy-regs/src/spicy_regs/mcp_server.py`; `spicy-regs/src/spicy_regs/join_measurements.json`; `spicy-regs/src/spicy_regs/aggregate_checks.py`; `spicy-regs/scripts/check_table_joins.py`.

## Evidence and limits

The JSON ledger preserves earlier dated checks and deployments as history. Its current task fields and third-wave evidence supersede earlier statements about unpublished native legal candidates, partial FCC enrichment, unrouted Senate amendments, pending cosponsors and preview-only comment repair.

T02: all 52,728 citations at `026be1bc…` match exact current parent text digests. Generation `c8e49dbb…` preserves those rows and adds eighteen digest-current court findings. This checks selected lookup coverage, not extraction recall. See `spicy-regs-citation-refresh-20260927/report.md`.

T09: `spicy-regs-entity-fec-reconcile-20260927/t09-native-shapes.json` verifies a complete retained OpenFEC page and exact companion records for scalar, positive-list, empty-list and null fields. Absent and malformed shapes remain synthetic-only tests.

T25: checkpoints distinguish listed archives, legacy completion metadata, refusals, empty backfill populations and completed selected native shapes. Empty or absent failure records do not prove a complete run. Comments-dependent reconciliation remains tied to the pending mirror refresh.

The JSON ledger lists absolute local receipt paths under `third_wave_evidence`; these are retained evidence, not public release links.
