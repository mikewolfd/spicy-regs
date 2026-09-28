# SpicyRegs populated-table join audit — 2026-09-27

**The largest immediate gain is to expose relationships already held in scalar keys and arrays, then add explicit citation-target resolution.** New extraction should follow those steps. Cross-source organization and person identity needs its own evidence and review.

Implementation follow-up: [concrete delivery tasks and raw-source replay](join-delivery-tasks-2026-09-27.md). That review identifies an additional explicit comment-to-document link and pinpoints source fields currently lost by the cosponsor and legislator readers. It refines the proposals below without changing this audit's historical measurements.

This audit covers every populated table advertised by the [fork MCP endpoint](https://spicy-regs-mcp.mdeeb.workers.dev/mcp): **85 populated tables**, with the six successful empty tables listed below. The current declaration catalog contains **71 scalar joins**, but **28 populated tables have no incoming or outgoing join declaration**. Several of those already contain useful relationship arrays or foreign keys. A missing declaration is not a missing relationship or proof a table is unusable.

The [machine-readable register](/Users/mikewolfd/Work/spicy-stack/spicy-regs/docs/research/populated-table-joins-2026-09-27.json) contains every table, its columns, source grain, declared joins, selected publication, coverage limits and proposed work. [Raw evidence, exact SQL and verification](/Users/mikewolfd/.codex/artifacts/spicy-regs-join-audit-20260927T191845Z/verification.json) remain outside Git. Counts here are measurements at the recorded pins, not ongoing table-size promises.

## Decisions supported by the live probes

| Relationship | What is held and measured | Recommendation |
| --- | --- | --- |
| Related bills | 142,698 distinct publisher-listed bill pairs; every target is held. [22](/Users/mikewolfd/.codex/artifacts/spicy-regs-join-audit-20260927T191845Z/candidate-22-related-bill-links.json) | Expose `bill_relationships`; retain each publisher relationship type and `identifiedBy`. |
| Members → candidates → committees | 1,738 member/FEC-candidate pairs yield 6,771 committee/candidate/member paths. No candidate ID points to multiple members in this selection. [06](/Users/mikewolfd/.codex/artifacts/spicy-regs-join-audit-20260927T191845Z/candidate-06-member-fec.json) | Expose the two identifier bridges. This establishes candidate-linked committees, not donations or current authorization. |
| Meetings → bills | 4,576 source elements produce 4,547 distinct meeting/bill pairs; every target is held. [14](/Users/mikewolfd/.codex/artifacts/spicy-regs-join-audit-20260927T191845Z/candidate-14-meeting-bills.json) | Expose source occurrences and a separate distinct-pair view; preserve meeting status. |
| FCC filings → proceedings | 8,661 source elements produce 8,658 distinct filing/proceeding pairs; every target is held. [15](/Users/mikewolfd/.codex/artifacts/spicy-regs-join-audit-20260927T191845Z/candidate-15-fcc-links.json) | Expose the existing array relation. Use the FCC namespace. |
| Spending recipients → SAM | 188,012 of 212,269 recipient rows with UEI match SAM. A raw registration join yields 191,041 rows; 4,557 SAM UEIs have multiple registrations. [07](/Users/mikewolfd/.codex/artifacts/spicy-regs-join-audit-20260927T191845Z/candidate-07-uei.json) | Use a distinct entity-identifier view for entity-level analysis; keep registration detail separately and preserve recipient-level totals. |
| Votes → nominations | 3,988 native Senate PN reference elements identify 2,155 distinct nominations; 1,337 elements have a hosted target. [23](/Users/mikewolfd/.codex/artifacts/spicy-regs-join-audit-20260927T191845Z/candidate-23-nomination-links.json) | Expose suffix-preserving references and missing-target status; use native Congress, not a guessed current Congress. |
| Citations → current source text | All 52,674 citation occurrences match the current parent record and text digest in the two held source families. [21](/Users/mikewolfd/.codex/artifacts/spicy-regs-join-audit-20260927T191845Z/candidate-21-citation-parent-repaired.json) | Keep that digest predicate as other families and revisions are added. |
| U.S. Code classification overlap | 1,465 of 2,157 distinct law-code section spellings overlap `table3_records`; normalization makes no difference in this particular comparison. Joining complete rows gives 4,500 pairs. [13](/Users/mikewolfd/.codex/artifacts/spicy-regs-join-audit-20260927T191845Z/candidate-13-usc-bridge.json) | Reuse the existing normalizer for correctness; do not claim a measured recall gain or one-to-one classification relation. |
| Comment-period membership | 488 proceeding references in the first 1,000 ordered period IDs all resolve. This is a bounded cohort. [16](/Users/mikewolfd/.codex/artifacts/spicy-regs-join-audit-20260927T191845Z/candidate-16-period-proceedings.json) | Expose the existing membership array; validate all arrays before adoption. |

### Citation keys and target existence must be separate

`document_citations.target_resolved=true` means the extractor settled a target key spelling. It does **not** mean the selected target table contains that record. The following counts are citation **occurrences**, including repeated mentions, not distinct laws or documents. Exact/declared key comparisons measure selected target existence, not extraction precision or legal applicability. [24](/Users/mikewolfd/.codex/artifacts/spicy-regs-join-audit-20260927T191845Z/candidate-24-citation-target-existence.json)

| Citation kind | Occurrences marked resolved | Occurrences with a hosted target |
| --- | ---: | ---: |
| `bill_number` | 25,621 | 25,610 |
| `public_law` | 9,916 | 1,024 |
| `usc_section` | 8,285 | 1,113 |
| `cfr_section` | 244 | 231 |
| `committee_name` | 6,065 | 6,065 |
| `gao_product_id` | 337 | 0 |
| `crs_report_id` | 5 | 5 |
| `docket_number` | 151 | 130 |

The practical fix is a typed resolution result carrying `found`, `missing` or `ambiguous`, the selected target snapshot, and candidate identities. Keep the original finding. Broaden the law/USC/GAO target selection where the user value justifies acquisition. For FR volume/page, Statutes volume/page, RIN and U.S. Reports citations, route by citation kind: equality with the target table’s primary ID is not the correct lookup.

### Correct metadata and strengthen keys without overstating defects

- `document_attributes` and `docket_attributes` still declare `kind=empty`, although they hold 2,002,888 and 279,406 rows. The first 1,000 rows of each resolve to their source parent. Rebaseline the full key sets before changing the published threshold. [10](/Users/mikewolfd/.codex/artifacts/spicy-regs-join-audit-20260927T191845Z/candidate-10-attribute-parents.json)
- `bill_versions` has two repeated `(bill_id,version_code)` keys and no duplicate `(bill_id,version_code,source)` keys. The first repeated-key cohort has no sections; every current diff endpoint resolves on the full key, with no subset-key amplification observed in those diffs. Use `source` in the declarations as preventive hardening; this audit did not demonstrate duplicated section/diff results. [01](/Users/mikewolfd/.codex/artifacts/spicy-regs-join-audit-20260927T191845Z/candidate-01-version-grain.json) [02](/Users/mikewolfd/.codex/artifacts/spicy-regs-join-audit-20260927T191845Z/candidate-02-section-fanout.json) [03](/Users/mikewolfd/.codex/artifacts/spicy-regs-join-audit-20260927T191845Z/candidate-03-diff-version-grain.json)
- All currently selected report `part_id` values and meeting `event_id` values are unique; full report-part and scoped event joins resolve. Still document the complete source identities `(package_id,part_id)` and `(congress,chamber,event_id)`. [04](/Users/mikewolfd/.codex/artifacts/spicy-regs-join-audit-20260927T191845Z/candidate-04-report-grain.json) [05](/Users/mikewolfd/.codex/artifacts/spicy-regs-join-audit-20260927T191845Z/candidate-05-hearing-grain.json)
- The dictionary leaves `identity_columns` empty for 23 populated tables; their grains are often described in prose. The register below supplies reviewed source grains, without claiming that every key has been globally uniqueness-tested.
- Twenty ordered examples of `document_attributes.original_document_id` resolve to none of the tested current, legacy or object ID fields. Treat those values as unresolved source references, not a safe direct foreign key. [25](/Users/mikewolfd/.codex/artifacts/spicy-regs-join-audit-20260927T191845Z/candidate-25-original-id-routes.json)

## What each class of work produces

| Class | Concrete output | Example question enabled |
| --- | --- | --- |
| Expose | Thin SQL views or small native relationship tables with complete keys and source positions. | Which bills were listed for a meeting? Which committees are linked to a legislator’s FEC candidate IDs? |
| Normalize | Raw value plus namespace, canonical comparison key, rule version and unresolved/candidate status. | Do these two spellings cite the same CFR section, in which edition? |
| Extract | A source occurrence with parent identity, text digest, exact span/page/JSON pointer, literal text and typed target candidates. | Which lobbying descriptions explicitly mention this bill or rule? |
| Resolve identity | A separate, reviewable mapping supported by native identifiers or adjudicated source evidence. | Is this commenter the same organization as a lobbying client or award recipient? |
| Acquire | Exact source-target request with retained successful, empty or refused outcome; only then a qualified record/body. | Which missing historical laws or GAO reports would make these existing citations navigable? |

A field named `organization` is insufficient for an entity join. For example, Regulations.gov can label `docket_attributes.organization` as “Pre-EDOCKET ID.” Likewise, short docket numbers from FCC and courts do not share an identifier namespace. Name matching can retrieve review candidates; it cannot certify common identity.

## Delivery order and ownership

1. **Make current answers reliable:** distinguish normalized citation keys from target existence; correct stale empty-join metadata; state full keys, cardinality and aggregate grain. Preserve the existing per-kind resolution floors rather than replacing them with an arbitrary 100% rule.
2. **Expose existing structured links:** related bills, member/FEC identifiers, meeting bills/committees, FCC filing membership, proceeding membership, native Senate document references, FEC evidence companions, court graph endpoints and artifact references. Use simple views first; materialize only for measured query cost or durable evidence needs.
3. **Add bounded source projections and extraction:** native cosponsors, complete structured legal references, citation adapters for held bill/report/comment/FCC/lobbying text, dated roles and controlled terms. Reuse owner readers and the existing single citation writer.
4. **Resolve harder identity and acquisition gaps:** organization/person review, source-grounded expenditure interpretation and selected missing bodies/records. Prioritize by useful unresolved references rather than ingesting every possible source.

SpicyDocs owns acquisition, literal source fields and reusable source readers. SpicyRegs owns these application relationships, interpretations and query views. RefSpec supplies source-qualified vocabularies and reviewed identity mappings. DocSpec owns retained document processing state. Reuse these owners instead of introducing a parallel universal entity store or grammar stack.

Iceberg/catalog snapshots can improve reproducibility and source-version selection; they do not create any of the semantic relationships described here. This audit does not change the serving backend. Prefer exact publication/snapshot receipts for bulk work and the existing query boundary for these views. The earlier [MCP catalog assessment](mcp-chaos-2026-09-27.md) addresses the serving decision separately.

## Complete table-by-table register

Each populated table appears once below. “Usable path” means the fields and meanings support that path; it does not assert that the path is already declared, complete across the source population, one-to-one, or newly validated here. `Pxx` links to an implementable proposal later in this report. Full schemas and existing declarations are in the JSON register.

### Regulatory participation

| Table and observed rows | Source grain / usable path | Create, extract or normalize next |
| --- | --- | --- |
| `dockets` — 279,429 | **docket_id.** Parent for documents, comments, attributes and feed_summary; RIN relates to agenda items and multiple agenda editions. | Expose derived proceeding membership. Keep native missing docket IDs; never infer them from comment prefixes or merge actions on shared RIN. [P02](#p02), [P03](#p03), [P04](#p04), [P23](#p23) |
| `documents` — 2,002,888 | **document_id.** Docket by docket_id; attributes by document_id; FR references need dated resolution of fr_doc_num. | Expand additional_rins and attachment occurrences; extract cited legal IDs from held text. Posting date cannot supply FR publication date. [P02](#p02), [P05](#p05), [P17](#p17), [P21](#p21), [P22](#p22) |
| `comments` — 26,314,331 | **comment_id.** Native docket_id joins dockets; agency_code joins the source agency roster. Organization names can retrieve existing org_committee_links candidates. | Extract explicit identifiers from available comment/body text with spans; retain attachment roles. Name equality cannot identify a person or organization. [P17](#p17), [P21](#p21), [P25](#p25) |
| `comments_index` — 143,408 | **agency_code + docket_id + year + month.** Aggregate cohort joins to dockets and agency dimensions; reconciliation against comments within the same generation. | Use NULL-aware equality for nullable grouping keys; aggregate each side before joining. No one-row-per-comment interpretation. [P23](#p23) |
| `document_attributes` — 2,002,888 | **document_id.** Document parent; source original_document_id, legacy_id, object_id and citation fields offer separate reference routes. | Refresh stale empty-join metadata. Original IDs sampled here did not resolve; retain them and obtain source migration evidence rather than rewriting them. [P01](#p01), [P05](#p05), [P22](#p22) |
| `docket_attributes` — 279,406 | **docket_id.** Docket parent; publisher labels explain organization, keywords and legacy fields. | Refresh stale empty-join metadata. organization may mean Pre-EDOCKET ID; inspect display_properties_json before treating it as an organization. [P01](#p01), [P20](#p20), [P22](#p22) |

### Regulatory activity summaries

| Table and observed rows | Source grain / usable path | Create, extract or normalize next |
| --- | --- | --- |
| `feed_summary` — 279,429 | **docket_id.** Docket summary joins its parent and selected proceedings through docket membership. | Carry contributing generation and measurement window so repeated summaries are not independent evidence. [P03](#p03), [P23](#p23) |
| `agency_stats` — 316 | **agency_code.** Source agency dimensions; docket/document/comment totals for a defined publication. | Keep one row per agency; never sum the total again after joining many documents. [P04](#p04), [P23](#p23) |
| `agency_monthly_volume` — 77,972 | **agency_code + year + month + document_type.** Agency lookup and matched monthly cohorts. | Normalize dates and align source coverage before comparisons; missing periods are not automatically zero. [P04](#p04), [P23](#p23) |
| `discovery_signals` — 8 | **agency_code within one computed window.** Agency dimension and the volume records used to calculate the signal. | Retain window/generation/method; a spike is a calculated observation, not a new agency event. [P04](#p04), [P23](#p23) |

### Rule publication and authority

| Table and observed rows | Source grain / usable path | Create, extract or normalize next |
| --- | --- | --- |
| `federal_register` — 1,009,313 | **document_number + publication_date.** Dated FR/docket links and resolved proceedings; issuer agency IDs/slugs; structured CFR and RIN arrays. | Expose all RINs, topics, issuers and legal references. Scalar rin only contains the first. Volume/page citations need their own resolver. [P02](#p02), [P04](#p04), [P05](#p05), [P16](#p16), [P20](#p20) |
| `fr_docket_links` — 899,630 | **docket_id + document_number + publication_date, subject to source duplicates.** Exact dated FR parent; normalized docket references through the existing label-aware resolver. | Expose raw reference, normalized candidates and status; a printed docket-like label may belong to another namespace. [P01](#p01), [P02](#p02) |
| `cfr_sections` — 321,010 | **granule_id; package and edition identify the annual source.** Legal references by title/part/section plus edition and structural level; granule/package locate evidence. | Publish a typed reference lookup. Part containment is not section equality; these are annual metadata, not regulation bodies. [P05](#p05), [P06](#p06), [P21](#p21) |
| `unified_agenda` — 233,250 | **rin + agenda_edition.** RIN relates agenda editions to regulatory_agenda_items and evidence-backed proceedings. | Expand CFR/legal-authority and timetable observations. Retain literal month/day precision and edition; a RIN join deliberately returns many editions. [P02](#p02), [P05](#p05), [P06](#p06), [P23](#p23) |

### Rulemaking lifecycle interpretation

| Table and observed rows | Source grain / usable path | Create, extract or normalize next |
| --- | --- | --- |
| `rule_targets` — 617,135 | **docket_id + cfr_ref + rin + source; evidence retained separately.** Docket/CFR/RIN relationships and source-typed evidence IDs. | Expose existing interpreted edges with method and unresolved FR observations; distinguish a part target from an annual section record. [P02](#p02), [P03](#p03), [P05](#p05), [P16](#p16) |
| `proceedings` — 172,742 | **proceeding_id.** Agenda-item links and lifecycle parent; arrays hold dockets, dated FR IDs, RINs and CFR references. | Flatten each membership array. Preserve joined_by and unresolved references; authority_refs_json is intentionally empty here, not an extraction opportunity to invent authority. [P03](#p03), [P05](#p05), [P23](#p23) |
| `regulatory_agenda_items` — 52,092 | **agenda_item_id.** RIN to agenda editions and agenda_item_proceedings. | Link latest_agenda_edition with RIN as a pair; keep agenda-item identity distinct from any individual regulatory action. [P02](#p02), [P03](#p03) |
| `agenda_item_proceedings` — 148,011 | **relationship_id.** agenda_item_id and proceeding_id; typed evidence source, ID, URI and date. | Expose evidence lookup and source qualifications. Several evidence rows can support one item/proceeding pair; deduplicate only in a separately named pair view. [P03](#p03), [P16](#p16) |
| `comment_periods` — 281,635 | **comment_period_id.** Arrays name dockets, proceedings, RINs and opening evidence. | Flatten each array independently; opened_by_artifact_ids_json holds URLs. Interval overlap alone cannot assign comments to a specific opening document. [P03](#p03), [P21](#p21), [P23](#p23) |
| `rulemaking_lifecycles` — 75,270 | **proceeding_id.** Proceeding parent; proposal/final/withdrawal IDs link within lifecycle_events using proceeding_id too. | Expose exact anchors and their date owners; maintain censor date, selection and competing withdrawal outcome. [P03](#p03), [P23](#p23) |
| `lifecycle_events` — 135,362 | **proceeding_id + document_id.** Proceeding parent; route document_id by dated_by to dated FR, Regulations.gov or agenda-item records. | Resolve agenda evidence_id to RIN plus edition. source names the stage authority and can differ from dated_by; routing by source misidentifies records. [P03](#p03), [P16](#p16), [P23](#p23) |
| `agency_lifecycle_stats` — 344 | **agency_code + stratum within a run/censor date.** Agency and explicitly defined lifecycle cohorts. | Retain censor date and method; these estimates must not be joined as individual rule outcomes or added across overlapping strata. [P04](#p04), [P23](#p23) |

### Organizations and public funding

| Table and observed rows | Source grain / usable path | Create, extract or normalize next |
| --- | --- | --- |
| `sam_entities` — 792,846 | **uei + entity_eft_indicator.** UEI relates registrations to spending recipients; CAGE and other codes identify source-specific roles. | Expose a distinct UEI lookup before entity-level joins; keep every registration and observation date. Registry coverage is selected active/public entities. [P13](#p13), [P25](#p25) |
| `usaspending_recipients` — 231,700 | **recipient_id including recipient_level.** UEI to SAM entity/registrations; DUNS as a separate legacy identifier. | Use distinct UEI targets and keep recipient level, observation time and source capture. Mixed-level amounts cannot be summed as award transactions. [P13](#p13), [P25](#p25) |

### Lobbying disclosure

| Table and observed rows | Source grain / usable path | Create, extract or normalize next |
| --- | --- | --- |
| `lobbying_filings` — 934,408 | **filing_uuid.** Activities and lobbyists through full activity keys; source registrant_id/client_id; government entity objects. | Create provider-scoped actor references and contacted-agency links; retain amendment/filing period and do not repeat filing money across activities. [P04](#p04), [P13](#p13), [P17](#p17), [P25](#p25) |
| `lobbying_activities` — 1,814,037 | **filing_uuid + activity_index.** Filing parent, activity lobbyists, issue code and government_entities_json. | Expand contacted entities at activity level; extract explicit bills, RINs and legal cites from description with evidence spans. Mention is not influence. [P04](#p04), [P17](#p17), [P20](#p20) |
| `lobbying_activity_lobbyists` — 4,735,060 | **filing_uuid + activity_index + lobbyist_index.** Full activity parent and provider lobbyist_id. | Create person-role occurrences; covered_position can yield a dated former-office mention. Do not map names to members without reviewed evidence. [P19](#p19), [P25](#p25) |

### Campaign-finance organizations

| Table and observed rows | Source grain / usable path | Create, extract or normalize next |
| --- | --- | --- |
| `fec_committees` — 89,689 | **committee_id.** Committee history; typed FEC relationships; candidate_ids_json to member candidate IDs. | Flatten candidates and cycles independently. A committee-candidate link is not automatically authorization or a donation. [P11](#p11), [P12](#p12), [P20](#p20) |
| `fec_committee_history` — 298,395 | **committee_id + cycle.** Committee parent; candidate_id to member candidate identifiers; cycle-specific names and organization observations. | Keep role, cycle and source spelling; connected_organization_name generates candidates, not verified affiliation to SAM or lobby clients. [P11](#p11), [P12](#p12), [P25](#p25) |
| `org_committee_links` — 5,740 | **organization + committee_id.** Committee parent; raw comment organization spelling retrieves heuristic candidate pairs. | Retain match method/confidence and review status; never turn name similarity into a certified cross-source entity ID. [P25](#p25) |

### FEC source inventory and evidence

| Table and observed rows | Source grain / usable path | Create, extract or normalize next |
| --- | --- | --- |
| `fec_source_catalog` — 26 | **source_family.** fec_collections and source records identify the catalog family. | Expose collection coverage by family, API route and profile. Catalog-only families are acquisition opportunities, not held transaction tables. [P12](#p12), [P27](#p27), [P28](#p28) |
| `fec_collections` — 649 | **collection_id.** Source family to catalog; parent for source_records and relationship locator coordinates. | Expose requested scope, outcomes and digests. Overlapping collections remain separate observations; empty success differs from never collected. [P12](#p12), [P28](#p28) |
| `fec_source_records` — 13,717,161 | **collection_id + source_record_id.** Collection parent; literal committee/candidate/filing/legal/audit IDs; exact FEC relationship companions. | Build only family-specific ID projections after inspecting metadata_json/native fields; preserve amendments, source hash, URL and locator. [P11](#p11), [P12](#p12), [P21](#p21), [P27](#p27) |
| `fec_relationships` — 183,390 | **source digest + source locator + relation/role + array position; occurrence identity.** Companion by locator collection_id + source_record_id + source_sha256; typed committee/candidate IDs connect to registries. | Promote only reported populated values as source-stated relations; preserve absence states and unresolved names. Do not zip independent arrays. [P11](#p11), [P12](#p12), [P25](#p25) |

### Oversight and policy research

| Table and observed rows | Source grain / usable path | Create, extract or normalize next |
| --- | --- | --- |
| `gao_reports` — 42 | **report_id.** Product-code mentions can use a normalized source-key comparison; URLs locate reports. | The held RSS selection does not cover cited historic products. Acquire selected report details/bodies before extracting agencies or laws; arrays are not a complete agency index. [P16](#p16), [P27](#p27) |
| `crs_reports` — 14,145 | **report_id with version retained.** CRS IDs in document_citations; report metadata and publisher URLs. | Acquire and retain selected versions before extracting bill/law/agency references; metadata presence is not body availability. [P16](#p16), [P27](#p27) |

### Judicial records and citations

| Table and observed rows | Source grain / usable path | Create, extract or normalize next |
| --- | --- | --- |
| `court_dockets` — 11,478 | **cl_docket_id.** Opinion clusters by cl_docket_id; docket-group endpoints; CourtListener court_id; native PACER coordinates. | Separate courts, cases and party/law-firm mentions. Case numbers need court and scope; this selected docket set is much smaller than the opinion universe. [P15](#p15), [P25](#p25) |
| `court_docket_groups` — 901 | **cl_docket_id within edition/rule_version.** Both child and parent_cl_docket_id reference court_dockets. | Keep confidence and rule version. A derived grouping is a candidate same-case relation, not permission to merge original cases. [P15](#p15) |
| `court_opinion_clusters` — 10,070,727 | **cluster_id.** Court docket, court code and opinion/citation children; SCDB ID can target an external source. | Extract citations from held headnotes/syllabus only within their scope. Do not claim these fields contain complete opinion text. [P15](#p15), [P17](#p17), [P27](#p27) |
| `court_citations` — 18,123,788 | **citation_id.** cluster_id to opinion clusters; reporter/volume/page supplies a citation lookup. | Normalize reporter aliases with source evidence and keep candidate clusters. Citation strings can identify multiple records; retain pin/date. [P15](#p15), [P16](#p16) |
| `court_citation_map` — 77,460,014 | **citing_opinion_id + cited_opinion_id.** Both ends reference court_opinions, then clusters and selected dockets. | Declare/expose both directions. depth is citation frequency, not positive or negative judicial treatment; no new full-corpus join measured here. [P15](#p15) |
| `court_parentheticals` — 6,408,887 | **parenthetical_id.** described_opinion_id and describing_opinion_id both target opinions; group_id has no hosted authority table. | Expose source-target explanations; text can provide bounded mentions. Scores and generated parentheticals are not reviewed treatment classifications. [P15](#p15), [P17](#p17) |
| `court_opinions` — 10,798,347 | **opinion_id.** Cluster parent; author_id needs a separate CourtListener judge/person authority; download URL and SHA locate possible content. | No held opinion body in this table. Acquire selected bodies/judge registry before text-based law links or person joins; author_str alone is insufficient. [P15](#p15), [P21](#p21), [P27](#p27) |

### FCC proceedings and filings

| Table and observed rows | Source grain / usable path | Create, extract or normalize next |
| --- | --- | --- |
| `fcc_proceedings` — 21,684 | **name; id_proceeding retained as separate native ID.** Parent for expanded filing proceeding_names_json; bureau_code/name references FCC organizational units. | Use the FCC namespace. A name such as 24-5 is not a Regulations.gov or court docket ID; cross-source matter links require explicit evidence. [P04](#p04), [P14](#p14) |
| `fcc_filings` — 5,780 | **id_submission.** All proceeding names; named filers/authors/law firms/bureaus; document assets and express text. | Flatten membership and role occurrences. Extract stated bill/CFR/RIN/docket references from held text; acquire linked PDFs before claiming their contents. [P14](#p14), [P17](#p17), [P21](#p21), [P25](#p25) |

### Legislative identity and history

| Table and observed rows | Source grain / usable path | Create, extract or normalize next |
| --- | --- | --- |
| `congress_bills` — 421,465 | **bill_id = congress + type + number.** Parent for bill family, sponsors, actions, laws, reports, amendments and votes. Related-bill objects already name full targets. | Flatten related bills and all relationship_details; expose stage_action_index and signed_date_action_index links; project parsed native cosponsors. [P07](#p07), [P08](#p08), [P19](#p19) |
| `bill_actions` — 930,779 | **bill_id + action_index.** Bill parent; stage/signed action references and bill_vote_references. | Preserve ordered source actions and expose existing index joins. Extract named committees/votes only with full native keys; action text alone does not prove enactment. [P01](#p01), [P17](#p17), [P19](#p19) |
| `bill_committees` — 286,100 | **bill_id + system_code.** Bill and committee parents; parent_system_code identifies parent committee. | Retain snapshot/referral role. Committee membership during a Congress is a separate time-qualified relationship. [P19](#p19) |
| `bill_publisher_summaries` — 189,604 | **bill_id + summary_version_code + action_date.** Bill parent and type-routed public activity events. | Expose text citations as a new extraction family if useful. Summary version codes are not bill printing version codes. [P17](#p17), [P21](#p21) |
| `bill_subjects` — 175,095 | **bill_id.** Bill parent; policy area and subject assignments. | Flatten subject occurrences with Congress vocabulary/version. Null or empty assignments need separate source states; FR topics are a different vocabulary. [P20](#p20) |
| `bill_versions` — 225,706 | **bill_id + version_code + source.** Bill parent; source-specific sections and diffs; equivalent XML pair; package and digest identify renditions. | Use complete version keys. Two subset keys currently repeat, though sampled sections and all current diffs showed no amplification. [P01](#p01), [P21](#p21), [P24](#p24) |

### Legislative text change and interpretation

| Table and observed rows | Source grain / usable path | Create, extract or normalize next |
| --- | --- | --- |
| `bill_sections` — 2,659,863 | **bill_id + version_code + source + seq.** Full version parent; body digests and paths support diff endpoint evidence. | Extract legal references per section/digest; paths and element IDs need uniqueness checks before use as section keys. [P01](#p01), [P17](#p17), [P24](#p24) |
| `section_diffs` — 28,431 | **bill_id + from_version_code + from_source + to_version_code + to_source.** Two full version endpoints; parent for section_diff_items. | Expose both qualified endpoints and pair rule. Comparison is a derived interpretation, not an amendment or enacted change by itself. [P01](#p01), [P24](#p24) |
| `section_diff_items` — 1,150,597 | **full section_diffs key + seq.** Exact diff parent; from/to element IDs and text digests can anchor original sections when uniquely resolved. | Carry endpoint seq or an explicit candidate set after checking path/ID/digest. Diff seq is not section seq; amount arrays describe mentions. [P24](#p24) |

### Legislative activity and voting

| Table and observed rows | Source grain / usable path | Create, extract or normalize next |
| --- | --- | --- |
| `public_activity_events` — 420,867 | **bill_id + event_type + subject_id + occurred_at.** Bill parent; event_type determines whether subject_id names a version or publisher summary. | Decode existing composite subject keys with the owner helper; retain detected_at separately from source occurrence time. [P21](#p21), [P23](#p23) |
| `amendments` — 7,106 | **amendment_id = congress + type + number.** Amended bill or amended amendment; sponsor Bioguide; Senate vote amendment objects. | Expose amendment-to-amendment links and native vote roles. Do not pair independent Senate document/amendment lists by position. [P10](#p10), [P19](#p19) |
| `press_releases` — 36 | **release_id.** Existing regex-derived bill link; issuer/feed and source URL. | Retain existing match evidence, extend bounded legal-reference extraction. Shared Senate mailbox authors do not identify a senator. [P17](#p17), [P21](#p21), [P25](#p25) |
| `roll_call_votes` — 23,358 | **vote_id = congress + chamber + session + roll_number.** Member votes; bill/action references; native Senate document and amendment objects. | Expose typed nomination/treaty/amendment references, retaining Congress and nomination suffix. Missing target rows are coverage gaps, not zero votes. [P10](#p10), [P19](#p19) |
| `member_votes` — 7,384,694 | **vote_id + member_key.** Vote parent; Bioguide or LIS routes; member_vote_terms for historical office. | Use vote-time party from source and dated terms. Current member party or one term party can misstate party-switch histories. [P19](#p19) |
| `members` — 12,770 | **bioguide_id.** Terms, sponsors and assignments; provider LIS/FEC/ICPSR/GovTrack/OpenSecrets/Wikidata IDs. | Expose provider-scoped identifier rows, especially FEC candidate IDs; names are labels and last-term attributes are not historical attributes. [P11](#p11), [P19](#p19), [P25](#p25) |
| `member_terms` — 45,535 | **bioguide_id + term_index.** Member parent; member_vote_terms; date-qualified office roles. | Retain date intervals and source precision; extracting native party histories would add information that term_party lacks. [P19](#p19) |
| `member_vote_terms` — 7,384,694 | **vote_id + member_key.** Vote-member parent; matched bioguide_id + term_index to member_terms. | Keep term_match and unmatched status. Preserve half-open dates and documented inclusive fallback instead of a broad date BETWEEN join. [P19](#p19) |
| `bill_vote_references` — 19,775 | **bill_id + chamber + congress + session + roll_number + action_index.** Bill action by bill_id/action_index; roll_call_votes by Congress/chamber/session/roll. | Expose source reference as its own evidence row; retain unmatched native vote references rather than guessing a chamber or session. [P01](#p01), [P10](#p10) |

### Congressional deliberation and fiscal evidence

| Table and observed rows | Source grain / usable path | Create, extract or normalize next |
| --- | --- | --- |
| `committee_reports` — 146 | **package_id + part_id.** Bill index and distinct cover-recital bill; sections by full parent key; CBO structured report citations. | Resolve report citations by Congress/type/number plus explicitly named part. Index association and cover evidence express different relationships. [P01](#p01), [P18](#p18), [P21](#p21) |
| `report_sections` — 1,953 | **package_id + part_id + seq.** Full report-part parent; text spans and heading/page context. | Extract legal references from held section text with digest/part locator. agency_key and agency_label are not an agency mapping to backfill by guessing. [P01](#p01), [P17](#p17), [P21](#p21) |
| `hearing_transcripts` — 145 | **package_id.** Full Congress/chamber/event to meetings; hearing_bill_links is the bill bridge. | The scalar bill_id is deliberately unused. Separate noticed subjects from bills demonstrably heard; source jackets may resolve transcripts with explicit scope. [P01](#p01), [P09](#p09), [P21](#p21) |
| `hearing_bill_links` — 86 | **package_id + bill_id + link_source.** Transcript, bill, committee and scoped event parents. | Retain relation, evidence text, held_dates_json and source. Multiple dates do not pair each bill to each date automatically. [P09](#p09) |
| `cbo_cost_estimates` — 12,732 | **bill_id + publication_id.** Bill parent; report_citations_json names Congress/type/number and optional part. | Expose cited-report observations and candidate text locations. A report citation is not proof a CBO letter was acquired; restatements remain observations. [P18](#p18), [P21](#p21) |
| `house_activity_reports` — 40 | **package_id.** Author committee, submitting Bioguide, structured associated bills/laws/USC sections/related reports; citation/action children with digest. | Flatten source arrays retaining context and covered_congress; join body findings to current text digest. Filing Congress may differ from the Congress described. [P16](#p16), [P18](#p18), [P19](#p19), [P21](#p21) |
| `budget_volumes` — 29 | **package_id.** Structured associated bills/laws/USC/CFR/Statutes; citation children by document key and digest. | Prefer stated Congress on indexed bills. A printed H.R. number without Congress stays unresolved; fiscal year cannot substitute for Congress. [P05](#p05), [P06](#p06), [P16](#p16), [P18](#p18), [P21](#p21) |
| `bill_committee_actions` — 12,695 | **document_key + text_sha256 + bill_id + print_phrasing + span_start.** Typed source document; bill parent; related citation only when bill and mention span agree. | Expose evidence-backed interpretations with rule/confidence. Joining every action to every citation in a document would fabricate relationships. [P16](#p16), [P19](#p19) |
| `document_citations` — 52,674 | **document_key + text_sha256 + cite_kind + target_key + span_start.** Typed citing document and per-kind target route; source span is the occurrence identity. | Separate key normalization, target existence and identity confidence. target_resolved=true does not mean a row exists in the selected target table. [P05](#p05), [P06](#p06), [P15](#p15), [P16](#p16), [P17](#p17) |
| `senate_expenditures` — 2,623 | **package_id + file_name + page + table_ordinal + row_ordinal + text_sha256.** Package/page evidence; local office/account/funding period grouping; amounts indexed within cells. | A new cell-role interpretation is needed for payees/transactions. Scope forward-fill within file/section and prove totals; no vendor join from a multiline payee block. [P21](#p21), [P26](#p26) |

### Enacted law and codification

| Table and observed rows | Source grain / usable path | Create, extract or normalize next |
| --- | --- | --- |
| `laws` — 113 | **congress + law_type + number; law_id is its joined form.** Bill parent; classifications by law_id; Statutes volume/page and USLM citable identifiers. | Expand aliases and qualified law citations; retain source law type. Small hosted scope cannot resolve all historic law mentions. [P06](#p06), [P16](#p16) |
| `law_code_sections` — 3,655 | **congress + session + seq.** Law parent; USC title + normalized section to other classification references. | Many laws affect one section; keep action, act section, preparation date and source release. This is not the current consolidated USC text. [P06](#p06) |
| `table3_records` — 2,983 | **act_key + seq within release_point.** USC title + section key to classifications; modern public-law act keys can resolve laws. | Retain pre-1957 act/date/chapter cases separately; do not coerce every act_key into a Congress-law ID or pair a cited page with a law start page blindly. [P06](#p06) |

### Congressional institutions and business

| Table and observed rows | Source grain / usable path | Create, extract or normalize next |
| --- | --- | --- |
| `committees` — 817 | **system_code.** Parent committee, bill referrals, meetings, reports and assignments; subcommittees/history arrays. | Expose source history and parent roles with time context. A current roster is insufficient to identify historical committee names automatically. [P19](#p19), [P20](#p20) |
| `committee_assignments` — 2,966 | **congress + system_code + bioguide_id.** Committee and member parents; scoped parent committee. | Keep Congress basis and file date. A Congress-wide join answers a cohort question, not exact membership on every date. [P19](#p19) |
| `house_communications` — 5,006 | **communication_id = congress + communication_type + number.** First referral code plus all committee objects; RIN; Congressional Record package/granule. | Expose all referrals and exact record_issue package link; separate submitting agency name from receiving committee and rulemaking reference. [P02](#p02), [P04](#p04), [P18](#p18), [P19](#p19) |
| `committee_meetings` — 6,096 | **congress + chamber + event_id.** Bills, all committees, hearing jackets, witnesses and documents; linked transcripts. | Flatten arrays preserving source ordinal/status. Do not identify witnesses or assert attendance from agenda names alone. [P09](#p09), [P19](#p19), [P21](#p21), [P25](#p25) |
| `record_issues` — 368 | **volume + issue; package_id provides GovInfo link.** House communications by record_package_id; sections/granules through source JSON. | Expose issue-to-section/document locators. Fetch article evidence before extracting speakers, legislation or floor actions absent from the held issue metadata. [P18](#p18), [P21](#p21), [P27](#p27) |
| `treaties` — 2 | **congress_received + number + suffix; treaty_id.** Native related documents, parts, country/index terms; scoped Senate document references where stated. | Keep received and considered Congress distinct and suffix intact; package IDs are only available under the supported unsuffixed grammar. [P10](#p10), [P18](#p18), [P20](#p20) |
| `nominations` — 2,214 | **congress + citation, including part suffix.** Native Senate PN documents can reference exact nominations; organization is a source label. | Preserve PN129-10 as its own target; broaden nomination coverage for unmatched native references. Office/nominee names require additional source identity work. [P10](#p10), [P19](#p19), [P25](#p25) |

### Acquisition operations and coverage auditing

| Table and observed rows | Source grain / usable path | Create, extract or normalize next |
| --- | --- | --- |
| `bill_family_archives` — 96 | **name/link for a captured source listing.** Congress + bill type identifies a source cohort; archive bytes and capture determine provenance. | Build an explicit archive-member inventory before claiming exact per-bill membership. A same-Congress join is only a scope relation. [P28](#p28) |
| `committee_report_reads` — 292 | **package_id checkpoint.** Report output by package_id is potentially one-to-many over parts; failed/empty reads may have no output. | Expose last modification, rule version and outcome as acquisition/processing coverage. No-output checkpoints are not missing entity edges. [P28](#p28) |

### Successfully empty tables excluded from relationship validation

`financial_changes`, `section_classifications`, `bill_summaries`, `diff_summaries`, `bill_family_backfills`, `bill_family_backfill_walks`. Their schemas remain in the historical inventory. Empty data cannot validate a join; do not manufacture rows or classify an unavailable source as empty. Some are uncomputed model outputs; others are operational backfill tables.

## Proposed relationship work, with exact inputs and acceptance conditions

These are proposals, not deployed views. Phase numbers correspond to the delivery order above: 0 is correctness work; 1 exposes held structure; 2 adds bounded interpretation/extraction; 3 needs harder identity, source review or acquisition. A proposal may include several related physical views; the number of proposals is not a claimed count of independent joins.

<a id="p01"></a>

### P01 — Complete and describe the existing joins

**Phase 0; metadata correction and key hardening.** Can I join sections to versions without multiplying the same section?

- **What goes in:** bill_sections; section_diffs; bill_versions; report_sections; committee_reports; hearing_transcripts; committee_meetings; attribute tables; bill_actions; bill_vote_references
- **What comes out:** Update the existing table_joins declarations and identity_columns; add cardinality checks to selected joins.
- **Keys and multiplicity:** Versions: (bill_id,version_code,source); diffs carry from_source/to_source. Reports: (package_id,part_id). Meetings: (congress,chamber,event_id). Attributes: native document_id/docket_id, many-to-one or one-to-one as measured.
- **Existing implementation to reuse:** src/spicy_regs/table_joins.py and .json; existing dictionary generator and scripts/check_table_joins.py.
- **Meaning and limits:** The existing check measures distinct-key existence, not row multiplication. Two version subset keys repeat; current diffs and selected sections did not amplify. Report and meeting subset keys are unique in the current selection: hardening is preventive. Rebaseline populated attributes instead of keeping kind=empty.
- **Owner:** SpicyRegs table catalog; SpicyDocs owns source identities.
- **How to check it:** Fresh cases 01-05,10; all full version diff endpoints resolve. Attribute samples each 1,000/1,000. Before adoption measure complete attribute key sets and parent uniqueness; add negative duplicate-parent fixtures.

<a id="p02"></a>

### P02 — Expose every regulatory identifier observation

**Phase 1; flatten and normalize retained fields.** Which House communications and Federal Register notices mention the same RIN, including secondary RINs?

- **What goes in:** federal_register.regulation_id_numbers_json/docket_ids_json; documents.additional_rins/fr_doc_num; dockets.rin; unified_agenda; regulatory_agenda_items; house_communications.rin
- **What comes out:** fr_rin_observations and regulatory_reference_observations, initially SQL views over existing fields and resolver outputs.
- **Keys and multiplicity:** An observation is keyed by parent record, field and array position. Dated FR identity is (document_number,publication_date); RIN-to-agenda is one-to-many over editions.
- **Existing implementation to reuse:** docs/regulatory-rins.md; docs/federal-register-identity.md; ontology/rins.py; ontology/federal_register.py and citations.py.
- **Meaning and limits:** Retain literal value, namespace, usable key, resolution status and candidate set. Never use only federal_register.rin, posting date as FR publication date, or shared RIN as proof of identical actions.
- **Owner:** SpicyDocs lexical reader; SpicyRegs source-context relation; RefSpec identity authority where adopted.
- **How to check it:** Schema and existing implementation review. Validate raw-array element accounting, malformed values and dated collisions before exposing new views; no fresh full regulatory cross-join in this audit.

<a id="p03"></a>

### P03 — Expose proceeding membership and event evidence

**Phase 1; flatten existing interpreted arrays.** For this comment window, show the linked action and the source documents supporting its dates.

- **What goes in:** proceedings; comment_periods; rule_targets; regulatory_agenda_items; agenda_item_proceedings; rulemaking_lifecycles; lifecycle_events
- **What comes out:** proceeding_dockets, proceeding_fr_documents, proceeding_rins, period_proceedings and typed event_evidence views.
- **Keys and multiplicity:** Parent ID + source field + ordinal for occurrences; optional distinct parent/target view for counting. Lifecycle anchors require (proceeding_id,document_id).
- **Existing implementation to reuse:** Existing materialized rulemaking tables already produce most edges. Expose them before designing another ontology.
- **Meaning and limits:** Use dated_by to route lifecycle document_id, and evidence_id for source support. Agenda evidence identifies an edition. Preserve unresolved candidates and derivation version; avoid cross products of separate arrays.
- **Owner:** SpicyRegs ontology and rollup owners.
- **How to check it:** Case 16: 488/488 proceeding elements resolve in the first 1,000 ordered periods. Case 19: 72 FR-dated events have source=regulations_gov, proving the route distinction. Full-array validation remains.

<a id="p04"></a>

### P04 — Build source-qualified agency references

**Phase 2; normalize source identifiers; review cross-source identity.** Which records concern the same bureau rather than merely its parent department?

- **What goes in:** Regulations.gov agency_code; FR agencies_json IDs/slugs; agenda agency_code/name; FCC bureau_code; LDA government_entities_json; submitting_agency; source reference rosters
- **What comes out:** agency_identifier and agency_reference projections; reviewed cross-source agency relations with exact-match, parent-unit and candidate types.
- **Keys and multiplicity:** Key by provider namespace + native ID + source release; retain organization level and dates where supplied. One document can name several issuers.
- **Existing implementation to reuse:** RefSpec registry/agency_crosswalk.py, regulations_gov_agencies.py and source agency rosters.
- **Meaning and limits:** The RefSpec co-occurrence crosswalk ranks candidates and can select a parent department. Treat it as evidence for review, not unconditional same-entity equivalence. No docket-prefix agency inference.
- **Owner:** RefSpec reference mappings; SpicyDocs retained rosters; SpicyRegs consumer relations.
- **How to check it:** Code/doc inspection plus earlier retained agency-ID experiment, not a new identity-quality census. Acceptance: explicit ID agreement, parent/child negative controls, unresolved and retired agencies retained.

<a id="p05"></a>

### P05 — Create typed CFR reference occurrences

**Phase 2; flatten structured citations; normalize and extract literal citations.** Show proposed rules, comments and legislation that cite a particular CFR part or section.

- **What goes in:** federal_register.cfr_references_json; unified_agenda.cfr_references_json; rule_targets/proceedings; document_attributes.cfr_part; budget associated_cfr_parts_json; document_citations
- **What comes out:** cfr_reference_occurrences: citing identity/digest/locator, title, part, section/subsection if stated, literal text, source edition and selected target candidates.
- **Keys and multiplicity:** Reference identity includes location; target identifies title/part/section at a stated or explicitly chosen annual edition. Part containment returns many sections.
- **Existing implementation to reuse:** ontology/citations.py already handles structured FR objects; spicy_docs.interpretation.citation_grammar owns prose parsing.
- **Meaning and limits:** Do not invent section identity from a title/part-only reference, overwrite original tokens, or infer legal effectiveness from a citation. cfr_sections is annual metadata and contains structural rows.
- **Owner:** SpicyDocs grammar; RefSpec controlled legal identities; SpicyRegs relationship projection.
- **How to check it:** Case 24: 231/244 key-resolved CFR citation occurrences match a current cfr_ref. Verify edition/level/cardinality and exceptional section spellings before publication.

<a id="p06"></a>

### P06 — Connect laws, U.S. Code and Statutes references

**Phase 2; normalize and route legal references.** Which laws have classified changes to the section a rule cites as authority?

- **What goes in:** laws; law_code_sections; table3_records; unified_agenda.legal_authority_json; document_citations; house/budget associated_laws/usc/statutes arrays
- **What comes out:** legal_reference_occurrences and law_usc_classification views, preserving the source action and release point.
- **Keys and multiplicity:** Law key is Congress + public/private + number. USC key is title + usc_section_key; classifications are many-to-many. Statutes volume/page is a separate citation kind.
- **Existing implementation to reuse:** schemas.tables.usc_section_key already normalizes case and Unicode dashes; reuse it on both sides.
- **Meaning and limits:** Neither classification table is consolidated current USC text. Keep pre-1957 acts, appendix references, ranges and subsections explicit. A law start page is not every page in the law; ambiguous volume/page targets remain candidates.
- **Owner:** SpicyDocs source schemas/grammar; RefSpec legal vocabulary; SpicyRegs links.
- **How to check it:** Case 13: 1,465 of 2,157 distinct law-code section spellings overlap table3 both before/after normalization in this selection; 4,500 classification pairs. No measured recall gain here. Case 24 shows selected law/USC target coverage gaps.

<a id="p07"></a>

### P07 — Expose related-bill relationships

**Phase 1; flatten existing native objects.** Find companion and related bills, including the publisher explanation of the relationship.

- **What goes in:** congress_bills.related_bills_json
- **What comes out:** bill_relationships with source bill, related bill, each relationship_details identifiedBy/type, parent pointer and ordinal.
- **Keys and multiplicity:** Target is native congress + lower-case bill_type + number. Preserve occurrence and relationship-detail granularity; distinct pair view separately.
- **Existing implementation to reuse:** RelatedBill in spicy_docs/sources/congress/bill_status.py already parses and retains details; no new model needed.
- **Meaning and limits:** A related bill is not necessarily identical, a successor or enacted text. Keep the publisher relation labels; do not reduce all relationship_details to one label.
- **Owner:** SpicyDocs native BILLSTATUS reader and projection; SpicyRegs view.
- **How to check it:** Cases 08,18,22: all 142,698 distinct current related-bill pairs resolve to hosted bills. Reconstruct raw objects and retain source detail arrays.

<a id="p08"></a>

### P08 — Publish native bill cosponsors

**Phase 2; project already parsed source records.** Who cosponsored related bills, and how do those networks compare with committee assignments?

- **What goes in:** Retained BILLSTATUS XML -> BillStatus.cosponsors; congress_bills.cosponsor_count; members
- **What comes out:** bill_cosponsors with bill_id, source-listed Bioguide/name, ordinal, capture/digest and membership/status fields only when source retained them.
- **Keys and multiplicity:** One source-listed cosponsor occurrence per bill observation; resolve Bioguide to members. Any distinct active-cosponsor count needs source status.
- **Existing implementation to reuse:** BillStatus.cosponsors exists at bill_status.py; schemas/bill_tables.py presently promotes the count only.
- **Meaning and limits:** The public table exposes only a count, including withdrawn entries. Current parser retains Bioguide/name but does not establish all cosponsorship dates/status. Extend the source reader for those fields from raw XML before claiming temporal membership.
- **Owner:** SpicyDocs source reader/projection; SpicyRegs publication and analysis.
- **How to check it:** Code verified, no replay of retained XML here. Acceptance: source list/count reconciliation, missing-vs-empty distinction, exact Bioguide resolution and a withdrawal fixture if statuses are added.

<a id="p09"></a>

### P09 — Expose meeting, hearing and witness relations

**Phase 1; flatten existing native objects; retain interpreted hearing evidence.** Show bill-related meetings, their committees, notices, transcript evidence and listed witnesses.

- **What goes in:** committee_meetings bill_ids/committees/hearing_jackets/witnesses/document arrays; hearing_transcripts; hearing_bill_links
- **What comes out:** meeting_bills, meeting_committees, meeting_hearing_references and meeting_participant_occurrences.
- **Keys and multiplicity:** Meeting identity includes Congress/chamber/event_id. Transcript is package_id. Jacket lookup must retain Congress/chamber/type and candidates. Witness occurrences are scoped to meeting and ordinal.
- **Existing implementation to reuse:** Existing hearing_bill_links covers interpreted bill links; reuse its source roles and rules.
- **Meaning and limits:** Agenda subject or witness listing is not proof of a held hearing or attendance. Preserve source status, link_source and held-date evidence; do not zip bills to dates or witnesses to documents without explicit pairing.
- **Owner:** SpicyDocs source fields; SpicyRegs hearing bridge.
- **How to check it:** Case 14: 4,576 source elements, 4,547 distinct meeting/bill pairs, all targets held. Case 05: all 16 referenced transcript event keys resolve; event IDs currently unique but full keys remain authoritative.

<a id="p10"></a>

### P10 — Connect votes to nominations, amendments and treaties

**Phase 1; parse structured native keys.** Which nomination was a vote about, including the correct nomination part?

- **What goes in:** roll_call_votes.documents_json/amendments_json; bill_vote_references; nominations; amendments; treaties
- **What comes out:** vote_document_references and vote_amendment_references with native role, ordinal and target status.
- **Keys and multiplicity:** PN documents use native Congress and PN-prefixed number including suffix. Bills use their full native keys; amendment type/chamber/number requires explicit source context. Treaty received Congress cannot be replaced by considered Congress.
- **Existing implementation to reuse:** Native Senate arrays already preserve full number spelling, including PN129-10. Existing bill_vote_references supplies complete action/vote keys.
- **Meaning and limits:** Never zip independent arrays. A placeholder amendment object with null number is not an amendment. Preserve unresolved complete references even when parent coverage is narrower.
- **Owner:** SpicyDocs source field rules; SpicyRegs vote relationships.
- **How to check it:** Cases 08,20,23: 3,988 native PN elements reference 2,155 distinct nominations; 1,337 elements find hosted targets. Inspect unmatched scope before acquisition; amendment/treaty routes unmeasured.

<a id="p11"></a>

### P11 — Connect members and campaign committees through FEC IDs

**Phase 1; flatten existing provider identifiers.** Which committees are explicitly candidate-linked to a legislator?

- **What goes in:** members.fec_ids_json; fec_committees.candidate_ids_json; fec_committee_history.candidate_id; typed fec_relationships
- **What comes out:** member_candidate_ids and committee_candidate_observations, then a member_committee view.
- **Keys and multiplicity:** Member Bioguide -> FEC candidate ID -> committee ID. Keep cycle/election year and source relation where present; these have different meanings.
- **Existing implementation to reuse:** No name matching required; native IDs are already held.
- **Meaning and limits:** Linking a candidate and committee does not establish current authorization, a contribution or an employer. Multiple candidate IDs per member remain distinct; current API cycles are not one current-election label.
- **Owner:** SpicyDocs retained source IDs; SpicyRegs typed views.
- **How to check it:** Case 06: 1,738 distinct member candidate IDs, no candidate mapped to multiple members in this selection, and 6,771 distinct member/committee/candidate paths. Full source relationships can provide additional roles.

<a id="p12"></a>

### P12 — Make FEC source evidence and family joins explicit

**Phase 1; flatten exact locators and typed ids.** For this committee relationship, show the exact source record and the field that states it.

- **What goes in:** fec_source_catalog; fec_collections; fec_source_records; fec_relationships
- **What comes out:** fec_relationship_evidence by collection_id/source_record_id/source_sha256; family-specific committee/candidate/filing/legal/audit ID views.
- **Keys and multiplicity:** The companion join includes the source hash. Source-record identity is collection_id + source_record_id. A reported relationship observation can repeat across sources or amendments.
- **Existing implementation to reuse:** docs/fec-relationships.md already documents exact companion SQL and source array positions.
- **Meaning and limits:** Keep value_status, subject/object type and ID status; absence states are observations, not edges. An ID-shaped field does not prove a resolved entity. Do not deduplicate amendments or overlapping collections without family semantics.
- **Owner:** SpicyDocs source records; SpicyRegs FEC relationship mapper.
- **How to check it:** Case 17: all 183,390 relation observations carry exact companion coordinates; this does not freshly verify every companion row. Family-specific relationship precision and complete companion/hash join remain acceptance gates.

<a id="p13"></a>

### P13 — Use UEI and source IDs before organization names

**Phase 1; expose entity-level identifier lookup.** Which observed recipients have SAM registrations, without duplicating their spending totals?

- **What goes in:** sam_entities.uei/entity_eft_indicator; usaspending_recipients.uei/duns; LDA registrant_id/client_id; FEC committee IDs
- **What comes out:** distinct_sam_ueis and provider_actor_identifiers, followed by scope-specific spending-to-registration relations.
- **Keys and multiplicity:** UEI is an entity identifier; UEI + EFT is a registration. Spending recipient_id also encodes recipient_level. LDA/FEC codes stay in their own source roles/namespaces.
- **Existing implementation to reuse:** The existing UEI join can be exposed at correct grain; no new matching system is required.
- **Meaning and limits:** Do not copy recipient totals to each registration, sum parent and child recipient amounts, or equate numerically identical IDs across providers. Names/DUNS need explicit mapping evidence for a cross-source merge.
- **Owner:** SpicyRegs views; RefSpec reviewed cross-provider mappings if created.
- **How to check it:** Case 07: 212,269 recipient rows carry UEI; 188,012 match SAM; raw join yields 191,041 rows. SAM has 4,557 UEIs with multiple registrations. Acceptance: preserve recipient-level money totals before/after joining.

<a id="p14"></a>

### P14 — Expose FCC filing-to-proceeding links

**Phase 1; flatten existing native arrays.** Which filings concern several FCC proceedings, and what was filed in each?

- **What goes in:** fcc_filings.proceeding_names_json; fcc_proceedings.name
- **What comes out:** fcc_filing_proceedings with submission_id, source ordinal and proceeding name; distinct pair view for counts.
- **Keys and multiplicity:** One filing can list several proceedings; repeated native elements survive in the occurrence view.
- **Existing implementation to reuse:** Already present as source arrays; use native name rather than invented ID parsing.
- **Meaning and limits:** FCC proceeding names are source-specific. Equality with a short court or other-agency docket number is not a cross-source link.
- **Owner:** SpicyDocs native arrays; SpicyRegs view.
- **How to check it:** Case 15: all 8,661 elements resolve; 8,658 distinct filing/proceeding pairs. Verify counts at occurrence and pair grain separately.

<a id="p15"></a>

### P15 — Expose the existing court graph and citation lookup

**Phase 1; declare native foreign keys; normalize typed citation keys.** Which opinions cite a decision, and which parentheticals explain those citations?

- **What goes in:** court_opinions; clusters; citations; citation_map; parentheticals; dockets; docket_groups
- **What comes out:** Document both opinion endpoints of citation_map and parentheticals; expose cluster/docket paths and typed reporter citation lookup.
- **Keys and multiplicity:** Opinion IDs, cluster IDs and docket IDs are different namespaces. Citation-map edges are opinion-to-opinion. Docket-group endpoints point to selected dockets.
- **Existing implementation to reuse:** Existing bulk citation graph already exists. Reuse it before extracting the same edges from all opinion bodies.
- **Meaning and limits:** Selected dockets cover much less than the bulk opinion tables. Reporter citations can have multiple targets; depth/score do not express precedential treatment. Author IDs require a separate judge registry.
- **Owner:** SpicyDocs CourtListener fields; SpicyRegs views; RefSpec CourtListener codes for reference labels.
- **How to check it:** Schemas and existing baseline declarations reviewed; no new full 77-million-edge join. Acceptance: pinned endpoint coverage by export date and indexed/batched target lookups; retain expected export lag.

<a id="p16"></a>

### P16 — Add explicit citation target resolution

**Phase 0; route existing citation occurrences to typed target records.** This report cites a law: can I open the actual law, and if not, is its key known but the target missing?

- **What goes in:** document_citations; current parent digests; congress_bills/laws/classifications/committees/dockets/FR/courts/GAO/CRS; bill_committee_actions
- **What comes out:** citation_target_resolutions with occurrence key, target namespace, normalized key, target snapshot, status (found/missing/ambiguous) and candidate identities.
- **Keys and multiplicity:** Retain (document_kind,document_key,text_sha256,cite_kind,target_key,span_start). Match current source digest first. RIN, FR volume/page, U.S. Reports and Statutes keys use different routes even when target_table repeats.
- **Existing implementation to reuse:** Existing document_citations owns occurrences; extend with a separate resolution result or view rather than overwrite the source finding.
- **Meaning and limits:** target_resolved currently means a settled key spelling, not target existence. A matched legal reference is not proof of legal applicability. Link committee-action mentions by bill and mention span, not every citation in the same document.
- **Owner:** SpicyRegs resolver consumes SpicyDocs grammar and RefSpec identities; no second extraction writer.
- **How to check it:** Cases 11,21,24: all 52,674 occurrences match a current held parent digest. Selected target existence varies sharply by kind; see measurement table. Acceptance: unresolved/ambiguous outputs and namespace confusion fixtures.

<a id="p17"></a>

### P17 — Extract explicit mentions from bodies already retained

**Phase 2; new extraction, using existing readers.** Which lobbying activities explicitly name the bill or regulation we are researching?

- **What goes in:** bill_sections.body; report_sections.body; lobbying_activities.description; FCC text_data; comments.comment/text_content; documents.text_content; publisher summaries; press releases; selected cluster/parenthetical text
- **What comes out:** Extend the existing citation occurrence pipeline to admitted document families, or add a family-specific adapter retaining source key/text digest/span.
- **Keys and multiplicity:** An occurrence is a mention at a location, not an entity merge. Attachments and document body segments need their own rendition identity.
- **Existing implementation to reuse:** Reuse spicy_docs.interpretation.citation_grammar and current print-citation provenance/correction lifecycle; avoid a new regex/model stack per family.
- **Meaning and limits:** Start with native IDs and deterministic grammars; contextualize Congress/court/date only when the source states enough. Distinguish author, commenter, cited entity and rule target. A mention does not establish lobbying success or causal influence.
- **Owner:** SpicyDocs text/source evidence; SpicyRegs single citation writer and relationship meaning.
- **How to check it:** Proposal only; no new extraction run here. Acceptance: source-context review, per-family held-out precision, malformed/ambiguous negatives, and replacement only after a complete successful re-read.

<a id="p18"></a>

### P18 — Expose report, communication and publication references

**Phase 1; flatten existing publisher metadata.** Which report or budget volume references this bill, and is that from the publisher index or the printed text?

- **What goes in:** house_activity_reports and budget_volumes associated arrays; cbo_cost_estimates.report_citations_json; house_communications committees/record keys; record_issues sections; treaties related_docs/parts
- **What comes out:** document_index_references, cbo_report_references, communication_referrals and record_section_locators.
- **Keys and multiplicity:** Keep source field, context, ordinal and complete target key. CBO reports resolve Congress/type/number plus an explicit part, with unresolved/multiple candidates retained.
- **Existing implementation to reuse:** All source arrays already held; current document_citations.stated_by_index can separate added text evidence.
- **Meaning and limits:** An index relation and a text citation are distinct evidence. A named report does not prove its CBO letter was acquired. Treaty/record article locators are not automatically hosted content.
- **Owner:** SpicyDocs structured source projections; SpicyRegs views.
- **How to check it:** Case 08 inspected native shapes; full parent-target resolution unmeasured for these arrays. Acceptance: element accounting, scoped report-part matching and typed indexed-vs-text evidence.

<a id="p19"></a>

### P19 — Expose dated people, offices and committee roles

**Phase 2; use existing joins; project missing temporal source detail.** Who held the relevant office or committee seat when a vote or proceeding happened?

- **What goes in:** members; member_terms; member_vote_terms; committee_assignments; committees.history_json; sponsors; bill_actions; hearing and nomination roles
- **What comes out:** person_identifier, office_terms, committee_membership_observations and source-qualified role references.
- **Keys and multiplicity:** Use Bioguide/LIS/provider IDs; role identity includes term or Congress, source observation and temporal precision. Join vote/term on the existing exact assignment.
- **Existing implementation to reuse:** Existing member_vote_terms handles half-open intervals and a documented inclusive fallback; retain it instead of recreating date matching. Native party histories require additional projection.
- **Meaning and limits:** Term party can omit midterm changes. Committee assignments are snapshots or Congress-scoped, not daily service histories. Nominee/witness/lobbyist name matches remain candidates until supported.
- **Owner:** SpicyDocs native people/roster fields; SpicyRegs time-qualified joins; RefSpec reference identity as needed.
- **How to check it:** Current schemas and earlier retained vote-term experiment; no new party-history extraction here. Acceptance: turnover-day, party-switch, same-name and assignment-date fixtures.

<a id="p20"></a>

### P20 — Normalize controlled categories without merging vocabularies

**Phase 2; flatten vocabulary assignments and import code authorities.** Compare bills, rules and lobbying in a subject area while showing how each source classified it.

- **What goes in:** bill subjects/policy areas; FR topics; agenda priority; LDA issue codes; FEC code fields; SAM NAICS; agency/docket topics; treaty country/index terms
- **What comes out:** source_term_assignments keyed by provider vocabulary/release/code or literal label; separate reviewed cross-vocabulary mappings.
- **Keys and multiplicity:** One source record can have many assignments. Include vocabulary namespace and release, not just normalized English label.
- **Existing implementation to reuse:** Existing RefSpec managed registries and source code readers; no generic lowercase-label universal topic ID.
- **Meaning and limits:** A shared word is not equivalent subject coverage. NAICS is an industry code, LDA issue is a disclosure category and FR topic is a publisher subject term. Keep missing and unassigned distinct.
- **Owner:** RefSpec vocabularies; SpicyDocs source assignments; SpicyRegs consumption.
- **How to check it:** Schema/code review; no new classification-quality experiment. Acceptance: source code round trips, versioned labels and separately typed exact/broader/related mappings.

<a id="p21"></a>

### P21 — Expose document and attachment evidence relationships

**Phase 1; flatten retained artifact metadata.** Show the original files and exact text rendition behind an extracted claim.

- **What goes in:** documents/comments attachments; bill_versions formats/digests; FCC documents; meeting document arrays; FEC assets/embedded bodies; report/hearing/budget digests; public_activity_events
- **What comes out:** source_artifact_references with parent identity, role, ordinal, URL, media type and retained digest/capture locator when available.
- **Keys and multiplicity:** One source record can reference many renditions/assets; identical URL is not proof of identical bytes. Provenance digest and rendition identify extracted text.
- **Existing implementation to reuse:** Use existing assets and provenance objects. No duplicate document store or bespoke file identity service.
- **Meaning and limits:** Do not treat download_url/local_path/offered format as acquisition. Decode typed activity-event subject IDs using the owner format; summary versions and printing versions differ.
- **Owner:** SpicyDocs artifact retention; DocSpec processed document state; SpicyRegs relation views.
- **How to check it:** Metadata reviewed; parent digest case 21 validates held citation parents only. Acceptance: acquired/offered/refused states, changing URL bytes, multi-format and attachment-parent fixtures.

<a id="p22"></a>

### P22 — Interpret native attribute and legacy-ID fields

**Phase 2; source-aware normalization; unresolved legacy references.** Is this uploaded document a copy of another record, and which native field supports that claim?

- **What goes in:** document_attributes.original_document_id/legacy_id/object_id/cfr_part/source_citation; docket_attributes.organization/display_properties_json
- **What comes out:** document_native_references with field namespace, source display label, literal value, resolution route and explicit status.
- **Keys and multiplicity:** Current document IDs and source legacy/object IDs are separate key spaces; use a publisher-supported mapping before selecting a target.
- **Existing implementation to reuse:** Existing attribute tables already carry the data and labels; extend semantic interpretation only where a source rule is supported.
- **Meaning and limits:** Do not strip legacy decorations to invent an ID or treat every organization field as a company. Preserve raw labels/values and multiple candidates.
- **Owner:** SpicyDocs source attributes and migration evidence; SpicyRegs target lookup.
- **How to check it:** Cases 09,25: 20 ordered nonempty original_document_id examples matched neither current document_id, legacy_id nor object_id. This small cohort supports retaining unresolved, not a global orphan rate.

<a id="p23"></a>

### P23 — Make temporal and aggregate joins explicit

**Phase 0; preserve grain and source windows.** Do two trends still agree when measured over the same sources, periods and record types?

- **What goes in:** comments_index; feed_summary; agency_stats; monthly_volume; discovery_signals; agenda timetable; periods/lifecycles/stats; activity events
- **What comes out:** Documented cohort views and lineage fields, not one universal event table.
- **Keys and multiplicity:** Aggregate by native cohort before joining; retain source event date, observation date, edition, window and generation separately.
- **Existing implementation to reuse:** Existing derived tables and lifecycle methods; use their definitions rather than summing unlike record grains.
- **Meaning and limits:** SQL NULL equality differs from grouping semantics. Preserve partial dates and censoring. Same month, agency or overlapping interval is an analytical association, not event identity or causation.
- **Owner:** SpicyRegs analytics; source date semantics from SpicyDocs.
- **How to check it:** Schemas reviewed and case 19 proves date/source roles differ. Acceptance: NULL group, interval boundary, missing-period and pre/post-join total preservation checks.

<a id="p24"></a>

### P24 — Anchor diff items to source sections

**Phase 2; expose full keys; resolve or retain endpoint candidates.** Show precisely which original sections support this reported change.

- **What goes in:** section_diff_items from/to_element_id, match paths, evidence_json and text digests; bill_sections; section_diffs; bill_versions
- **What comes out:** diff_section_endpoints with side, full version identity, unique section seq or explicit unresolved/candidate status.
- **Keys and multiplicity:** Diff row key is full version pair + diff seq. Section key is bill/version/source + section seq. These sequences are unrelated.
- **Existing implementation to reuse:** Use existing pairing_rule/evidence_json; add source section seq at build time if uniquely known instead of re-resolving expensively at query time.
- **Meaning and limits:** Paths/element IDs can repeat; text digest can repeat across boilerplate sections. Keep matcher evidence and ambiguity; a diff amount mention is not a budget estimate.
- **Owner:** SpicyRegs differ and section interpretation; SpicyDocs source text identity.
- **How to check it:** Cases 01-03 establish version-key context only; endpoint uniqueness not measured. Acceptance: moved/duplicate/repeated-heading sections and both full endpoint identities.

<a id="p25"></a>

### P25 — Build reviewable organization and person candidates

**Phase 3; extract role occurrences; resolve identity only with evidence.** Does the organization commenting on a rule also appear as a lobbying client or award recipient, and how certain is that identity?

- **What goes in:** Comment organizations; FCC filers/authors/law firms; court parties/attorneys/firms; LDA clients/registrants/lobbyists; FEC connected/sponsor names; SAM/spending names; org_committee_links; witnesses
- **What comes out:** participant_occurrences plus identity_candidates and reviewed identifier links, scoped to source role/time.
- **Keys and multiplicity:** Occurrence identity is source record + field/ordinal/span; an accepted entity link is separate and carries basis, reviewer/rule version and dates.
- **Existing implementation to reuse:** Use explicit UEI/FEC/Bioguide/provider IDs first. Avoid importing a universal entity database merely to hide uncertainty.
- **Meaning and limits:** Normalize names for retrieval only. Preserve suffixes/raw names/DBAs and do not equate subsidiary with parent, lawyer with client, donor with employer or shared person names. Existing heuristic org_committee_links remains a candidate source.
- **Owner:** RefSpec cross-source identity evidence; SpicyDocs native roles; SpicyRegs application use.
- **How to check it:** Not implemented or quality-measured. Acceptance: frozen exact-ID positives, known same-name and parent/subsidiary negatives, abstention, reviewable evidence and separate coverage/precision reporting.

<a id="p26"></a>

### P26 — Interpret Senate expenditure cells before entity joins

**Phase 3; new source-grounded table interpretation.** What can these pages actually say about an office or payee without inventing transactions?

- **What goes in:** senate_expenditures cells_json/column_headers_json/amounts_json with page/file/digest and source PDFs
- **What comes out:** First: page-context and amount-occurrence views. Later: validated transaction/payee interpretations where the print actually supports them.
- **Keys and multiplicity:** Keep package/file/page/table/row/cell/amount index; office/date context propagation must stay within verified printed section boundaries.
- **Existing implementation to reuse:** Reuse retained table rows, headers, page locators and printed totals; do not add a name parser and call it transactions.
- **Meaning and limits:** Most detailed payee blocks are multiline single cells, not parsed transaction rows. Totals and fiscal-year stacks must reconcile before splitting. Sampled capped pages cannot become full Senate spending claims.
- **Owner:** SpicyDocs faithful cells; SpicyRegs reviewed interpretation; DocSpec rendition evidence.
- **How to check it:** Schema/source-quality notes reviewed; no new PDF extraction or pixel inspection here. Acceptance requires rendered source review, field-role checks, totals and duplicate full-report/part handling.

<a id="p27"></a>

### P27 — Acquire missing target records and bodies selectively

**Phase 3; targeted acquisition, separate from joins.** Which missing records would resolve the largest useful set of currently unresolved citations?

- **What goes in:** Unmatched citations; GAO/CRS metadata; court opinion download URLs; meeting documents; FEC catalog-only families; unmatched nomination references
- **What comes out:** A source-scoped acquisition queue with retained success/empty/refused outcomes and qualified target/body outputs.
- **Keys and multiplicity:** Queue by exact native identifier/version/source locator, not by a guessed global entity name. Track request and observation time.
- **Existing implementation to reuse:** Existing source workflows and resumable retained captures; no new scraper in the MCP query process.
- **Meaning and limits:** Catalog entries and URLs are discovery metadata. Do not call missing targets absent from the world or treat a failed request as empty. Bodies may exist elsewhere in retained stores, but are not established by these MCP tables.
- **Owner:** SpicyDocs acquisition; DocSpec document state; SpicyRegs prioritization.
- **How to check it:** Case 24 establishes selected target gaps; case 23 establishes unmatched nomination references. Acceptance: source-native record verification and body digest/provenance before new citation extraction.

<a id="p28"></a>

### P28 — Expose processing coverage and source lineage

**Phase 1; normalize operational metadata, not entity relationships.** How do we know which source releases and processing runs produced the records in this answer?

- **What goes in:** bill_family_archives; committee_report_reads; FEC catalog/collections; publication generation metadata
- **What comes out:** Acquisition/processing coverage views plus explicit archive-member inventories when retained source bytes support them.
- **Keys and multiplicity:** An archive listing denotes a source cohort; report read checkpoint denotes an attempted package/rule-version processing state. Outputs can be zero or many parts.
- **Existing implementation to reuse:** Existing checkpoints and immutable generation artifacts. Iceberg snapshot lineage can support this boundary; it does not supply semantic joins.
- **Meaning and limits:** Do not join a whole Congress to an archive as if each bill was verified present. Preserve failed reads, empty success and old retained outputs separately. Matching generation labels for unversioned files do not prove byte identity.
- **Owner:** SpicyDocs source receipts; SpicyRegs processing/publication; DocSpec retained state where used.
- **How to check it:** Inventory refresh found four generation changes with unchanged counts. Acceptance: exact source/output membership, current-rule outcomes, digest verification and coherent generation selection.

## Architecture verdict and checks before adoption

**Proceed with the existing source tables, owner normalizers and thin relationship views.** The data supports useful joins without a new database architecture. Doing nothing leaves structured source links difficult to discover and encourages incomplete scalar joins. A universal name-based entity graph would connect more rows but would also silently assert identities the sources do not establish. The proposed separation keeps the original observations readable and lets each stronger claim earn its evidence.

For each adopted relationship, retain the exact source fields and input generation; count source observations, distinct keys, unmatched targets, ambiguous targets and output rows separately. Check the full parent key for uniqueness before calling a join many-to-one. Test repeated array elements, null/empty/malformed values, reused IDs across namespaces, changed source bodies and historical dates. Validate precision from source context for extracted or reviewed links; a join success rate alone cannot do that.

Keep occurrence views and distinct-pair views separate. Perform aggregate reconciliation before and after joins. Preserve unsuccessful reads and unresolved observations. A successful re-read may replace findings for the same source/text/rule scope; an incomplete or failed read must preserve the old findings.

Array expansion is linear in the number of source elements. Identifier joins should use projected keys and indexed or batched target lookups. Avoid all-pairs organization-name comparisons, repeated scans of entire body columns and large cross products between independent arrays. The court graph and comment corpus need bounded cohorts or materialized/indexed access, not repeated broad MCP joins.

## Evidence scope and reproducibility

- Preregistered plan: [bounds and decision rule](/Users/mikewolfd/.codex/artifacts/spicy-regs-join-audit-20260927T191845Z/preregistered.md). Candidate SQL: [initial cases](/Users/mikewolfd/.codex/artifacts/spicy-regs-join-audit-20260927T191845Z/candidate-cases.json), [adaptive cases](/Users/mikewolfd/.codex/artifacts/spicy-regs-join-audit-20260927T191845Z/adaptive-cases.json). Raw responses retain exact SQL, response rows, duration and connection publication identities.
- Twenty-five candidate SQL calls were attempted; twenty-four succeeded. The citation-parent query first failed because `family` was used as a reserved alias. Case 21 repairs that query and uses the observed document kinds; the failed response is retained. No query result was truncated. This was a bounded join audit, not a load test or proof of every possible join.
- Every populated schema and current count was reviewed. Counts from the prior complete inventory were reused only when the selected immutable publication identity agreed at discovery; changed/unversioned tables were refreshed. The complete retained inventory includes zero-row outcomes and historical coverage qualifications.
- Four publications changed between initial discovery and the candidate queries: dockets, documents and both attribute tables. Every successful candidate query reported the same newer publication selection. Those four schemas and counts were refreshed afterward; their row counts did not change. Each result retains its actual selection rather than claiming an atomic all-table snapshot.
- Comments and comments_index remain labeled unversioned. Fresh counts were taken; equal labels do not prove equal bytes. No new candidate-validation query joined those large comment tables. Published availability and a successful join do not certify source-wide coverage or scientific validity.
- Full cross-corpus court joins, organization resolution, new text extraction, cosponsor replay and missing-body acquisition remain unperformed. Their proposals are grounded in fields and owner code, with explicit acceptance conditions.
- Changes made by this audit are documentation and a machine-readable research register. No new production views, extraction runs, data publications or deployments were performed.

## Implementation evidence

- [Current join declarations and distinct-key baseline semantics](/Users/mikewolfd/Work/spicy-stack/spicy-regs/src/spicy_regs/table_joins.py:1).
- [Version, hearing and report join declarations](/Users/mikewolfd/Work/spicy-stack/spicy-regs/src/spicy_regs/table_joins.py:154).
- [Stale attribute empty-join declarations](/Users/mikewolfd/Work/spicy-stack/spicy-regs/src/spicy_regs/table_joins.py:259).
- [Complete RIN arrays and relationship meaning](/Users/mikewolfd/Work/spicy-stack/spicy-regs/docs/regulatory-rins.md:1).
- [Dated FR identity and existing reference resolver](/Users/mikewolfd/Work/spicy-stack/spicy-regs/docs/federal-register-identity.md:1).
- [FEC companion join and source-value semantics](/Users/mikewolfd/Work/spicy-stack/spicy-regs/docs/fec-relationships.md:148).
- [Citation occurrence and target-key semantics](/Users/mikewolfd/Work/spicy-stack/spicy-regs/docs/tables/document_citations.md:7).
- [Senate cell/amount and coverage limits](/Users/mikewolfd/Work/spicy-stack/spicy-regs/docs/tables/senate_expenditures.md:7).
- [Single writer and digest-qualified replacement of citation findings](/Users/mikewolfd/Work/spicy-stack/spicy-regs/src/spicy_regs/transforms/build_print_citations.py:694).
- [Native related-bill details](/Users/mikewolfd/Work/spicy-stack/spicy-docs/src/spicy_docs/sources/congress/bill_status.py:101).
- [Parsed native cosponsors](/Users/mikewolfd/Work/spicy-stack/spicy-docs/src/spicy_docs/sources/congress/bill_status.py:254).
- [Published cosponsor count, including withdrawn entries](/Users/mikewolfd/Work/spicy-stack/spicy-docs/src/spicy_docs/schemas/bill_tables.py:59).
- [Existing U.S. Code comparison-key normalizer](/Users/mikewolfd/Work/spicy-stack/spicy-docs/src/spicy_docs/schemas/tables.py:430).
- [Agency crosswalk ranking and parent/child ambiguity](/Users/mikewolfd/Work/spicy-stack/RefSpec/src/refspec/registry/agency_crosswalk.py:1).
- [CourtListener code authority and scope](/Users/mikewolfd/Work/spicy-stack/RefSpec/src/refspec/registry/courtlistener_codes.py:1).
