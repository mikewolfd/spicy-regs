# Source facts and recorded associations

The main tables carry the fields needed to understand a record and follow its declared relationships. Receipts retain the original observations, input pins, conversion details, and failures. Navigation uses main fields; evidence does not create an edge by itself.

- Comment periods retain independent proceeding, docket, and RIN memberships. New `evidence_occurrences` retain repeated references, source order, role, and the complete native document identity. A Federal Register reference includes its publication date. A Regulations.gov reference uses its document ID. Earlier periods without these details keep `evidence_occurrences` null until their maintained builder runs over the selected sources.
- Lifecycle events retain `source`, `dated_by`, `evidence_id`, and `joined_by` beside `event_date`. A missing date basis cannot establish a Federal Register publication date. Document and comment extraction results describe an attempt; an offered attachment, a captured body, and usable text are separate facts.
- Court PDF rows identify an opinion and a captured body. They retain the recorded parent publication and member pins, URLs, digests, extraction result, and extractor version. That recorded parent is separate from lookup against today's opinion table. A new captured cohort requires all exact selected parent members and exactly one matching opinion, cluster, offered URL, and native digest. SHA-1 alone never establishes a parent link.
- GAO decisions retain every B-number occurrence and any truncation flag. A shared B-number does not establish a GAO report ID. Senate expenditure records retain their package, file, body, page, grid, table, and row context. Payee and office names do not establish person identities.
- Scorecard subjects retain their recorded snapshot and capture associations while their logical business keys stay unchanged. Snapshots retain ordered capture roles, parsing and completeness facts. Resolution rows retain status, rules, candidates, and selected input pins. Valid unresolved, ambiguous, or conflicting decisions remain main rows with no resolved target. Only `resolved` decisions can supply an official-record link; current lookup remains a separate result.

## Reader and data release order

Release the reader before new data. Exact earlier declarations remain available in `source_context_policy_history.json`, `fec_policy_history.json`, and the existing navigation history. A selected historical file is read under its original schema and receipts. New writers use the new policy; they never relabel old bytes. A historical receipt-only dataset stays unavailable as a main table until a real main member is published. A real current empty main file remains a visible empty table.

The changed policies are `regulations-native-v2` for eight affected regulation datasets, `courts/2` for opinions and PDF extractions, `government-sources/3` for GAO reports and decisions, and `scorecards-etl-v2` / `scorecards-etl-ratings-v4` for scorecards. Unchanged Court and bill prepared families retain their selected producer and publication pins.

## Rebuild only affected families

Use the existing APIs with explicit selected files and generation IDs in a private output directory. These steps prepare local candidates; publication and service deployment remain separate.

| Data | Maintained entry points | Required input |
| --- | --- | --- |
| Existing regulation facts | `regulations_receipts.read_internal(ReceiptInput(...))`, then `build_local_generation(...)` | Exact matching subject/receipt pair; original source strings preserved |
| New period occurrences | `regulations_receipts.build_from_receipts(..., builder=build_comment_periods, outputs=["comment_periods"])` | Selected documents, Federal Register, proceedings and other builder inputs |
| Court retained facts | `court_receipts.restore_processing_input(...)`, then `write_court_rows(..., prior_receipts=...)` | Exact old subject/receipt pair and its original processing schema |
| New court captures | `build_court_pdf_extractions.prepare_captured_opinions(..., parent_subjects=...)` | Every selected opinion member, explicit bounded captures and recorded read snapshot |
| GAO retained facts | `government_receipts.processing_input(...)`, then its existing governed writer | Exact selected prior plus its retained source observations |
| Scorecard source facts | `scorecards.etl.read_indexed_family(...)`, then `write_family(...)` | Complete exact selected source family, including historical snapshot receipts |
| Scorecard resolutions | `build_scorecard_analysis(...)` | Exact main source/official members, receipt generations and input pins |

Do not run normal provider-refresh commands merely to promote retained fields. Rebuild period occurrences and scorecard resolutions when their interpretation changed. After preparation, verify complete identities, row counts, refusals, unresolved outcomes, and all affected forward/reverse relationships against matching new publications. Earlier measurements remain evidence only for their original pins.
