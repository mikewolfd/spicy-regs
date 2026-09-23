# Federal Register record identity

The source table identifies a published record by the literal pair
`(document_number, publication_date)`. The number alone is not unique. Retained
publisher responses contain `00-111` for the Treasury housing-credit rule on
2000-01-14 and the Interior land notice on 2000-01-18. Both records must survive.

This implements the existing SpicyDocs public-table decision, projection 1.1.
Neither key column changes its type or spelling. A later successful fetch
replaces the previous observation of the same pair. Conflicting observations
within one fetch or prior file refuse publication; identical repeats collapse.
An unknown or noncanonical date also refuses publication. The output replaces
the previous local file only after the complete merge succeeds.

RefSpec's REF-064 and REF-066 address a different question: which identifier
denotes a document or matter. Its retained census contains 474 repeated numbers
across 957 dated observations, including seven modern-form collisions. Five of
the seven require refusal of a number-only matter identifier; two are explicit
self-corrections. All 957 dated observations remain separate source records here,
regardless of matter-level adjudication. No number unpadding, case folding,
collision whitelist or new IRI scheme is part of this change.

## Consumer migration

`fr_docket_links` already carries both identity columns. Consumers join on both,
and dictionary prose now states that requirement. The materialized rulemaking
generation uses SpicyDocs `classify_document` for key validation and
`federal_register_source_record_id` for its existing `number@YYYY-MM-DD` encoding.
These source keys are not Rulespec IRIs. The installed source-readers extra is
required for the materialization, as in the repository's default environment.

| Output | Change |
| --- | --- |
| `rule_targets` | Dated FR/docket joins and dated FR evidence IDs. Appended `fr_references_json` retains each literal number-only observation, evidence row, status and candidate keys. Ambiguous or missing references emit an observation row with null CFR/RIN targets. |
| `proceedings` | Appended `fr_document_ids_json` drives grouping and continuity; `fr_document_numbers_json` remains the literal-number query aid. Appended `unresolved_fr_references_json` retains unresolved bridge and prior-generation observations without choosing an identity. |
| `regulatory_agenda_items` | Appended `unresolved_fr_references_json` retains candidate links from old number-only proceedings. An unresolved candidate never creates an agenda relationship. |
| `agenda_item_proceedings` | Dated evidence IDs distinguish relationships; evidence URLs include the publication date. No new column. |
| `comment_periods` | Dated FR membership, evidence IDs and URLs. Appended `unresolved_fr_references_json` retains old ambiguous references. An interval with only unresolved candidates stays attached to its dated source artifact, with empty docket/proceeding arrays. |

Resolution statuses are `dated`, `single_candidate_in_input`, `ambiguous` and
`missing`. A single candidate means only one candidate exists in the held input;
it does not certify worldwide number uniqueness or complete historical coverage.
The literal number is matched first; a number the generation does not hold is
compared on SpicyDocs' comparison key (`unpadded_federal_register_document_number`:
dashes and case folded, the sequence's zero padding removed, both sides reduced),
because Regulations.gov pads numbers the Register did not (`2010-02394` for
`2010-2394`) and writes en dashes (`2018–28359`). A match that only folds is
`folded_dated` or `folded_single_candidate_in_input`; one that unpads is
`unpadded_dated` or `unpadded_single_candidate_in_input`. Two sequences both padded
to different widths (`2015-0674` against `2015-00674`) do not match, and a key two
held numbers share (five 1994–1997 pairs) is `ambiguous` with both candidates, even
when a publication date would pick one (fork delivery decision 18).
Federal Register docket values are read through their label ("Docket No.
SSA-2010-0037") with SpicyDocs' `normalize_docket_reference`, keeping a literal
Regulations.gov identifier as itself; a link joins only a docket the
Regulations.gov records assert, and a link that names none stays out as before.
A Regulations.gov document's posting date is not used as an FR publication date.
Old number-only proceeding identities migrate only when the held input supplies
one candidate. Ambiguous prior identities remain evidence, and do not select a
new record's ID. Existing docket-based continuity remains unchanged.

These additive materialized fields require consumer adoption with the same
generation. Interpretation actor versions move with the changed rules. The
legacy `federal_register_identifier` lexical helper had no production caller
and was deleted with the rest of the unused citation grammar; adopting RefSpec's
matter-level mint/refusal API is separate work.

## Validation and publication boundary

The bounded native fixture retains both complete `00-111` result objects and
their parent-response paths/digests. The two complete retained daily responses
contain 237 records: the old host merge writes 236; the corrected merge writes
237. An identity-only replay of the pinned RefSpec census retains all 957 pairs.
A local repair of the pinned 803,996-row public artifact using the native pair
writes 803,997 rows, adds only `00-111@2000-01-14`, loses no prior keys and creates
no duplicate pairs. This is a replay result, not a public deployment.

Receipts, scripts and local output artifacts:
`~/Work/corpora/supply-2026-09-02/receipts/remediation-sprint-2026-09-21/fr-identity/`.
The authoritative raw replay is `verified-replay/replay-results.json`. The first
attempt remains retained: ranking complete wide rows exceeded DuckDB's 4GB
limit. Ranking key/source/row positions before reading payloads completed the
full-file repair under the same limit.

The seven-day incremental overlap cannot restore older rows already discarded
by the public number-only merge. Publication still needs a pinned historical
backfill, source/output key accounting, bridge regeneration and coherent
materialized-consumer adoption. This change performs no origin crawl, package
release or upload.
