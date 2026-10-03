# Scorecard schema gate review

**Verdict: RECONSIDER the current draft before freezing.** Keep the ownership
boundary and source-faithful tables. Remove the mandatory metric relationship
from member-item results, retain item-level publisher positions, and specify
one current accepted snapshot per edition with explicit bundle checks. The
coordinator has accepted these directions; the frozen schema and fixtures still
need to embody them. See [machine-readable review](schema_review.json) for input
hashes, source locators and closure tests. This is a review, not a schema freeze.

## Findings

### F1 — BLOCKER: metric-free item grids do not fit

`proposed_schema.json:12–17` permits publisher targets only on metric-item rows
and requires `metric_id` and `participation_id` in every member-item-result key.
NRF's Senate 119th grid has item results and explicit item positions but no
aggregate metric. The row for Angela Alsobrooks contains source-provided image
alt text, including `opposes nrf's position`; item `M-2391-12549` states
`NRF POSITION: SUPPORT`. The table supplies no overall score to instantiate.
Evidence: capture `census-nrf-119-senate`, table and item detail anchors, bound in
`schema_review.json:E-NRF`.

**RESHAPE:** use result key
`(scorecard_id, item_id, publisher_member_key, result_id)` and nullable
`metric_id`/`participation_id`. Always require item and member references. When
a participation is supplied, require its complete declared key and matching
metric/item; a metric may be supplied without participation only where the
publisher identifies that metric context. Neither field is supplied for NRF.
Keep `result_id` an adapter-versioned occurrence key, not a fabricated score.
Add item-level `publisher_position_text`, `position_basis` and its locator;
retain explicit metric-specific positions separately without treating a missing
metric-specific position as a source assertion. Preserve the NRF alt text rather
than extracting an empty visible-text cell. Store source blank as an empty
string with its legend meaning, distinct from absent acquisition.

Closure: an NRF example has members, items, targets and results, zero metrics,
zero ratings and no fabricated overall concept. Its foreign references validate.

### F2 — BLOCKER: snapshot grain and relationships are not yet executable

`proposed_schema.json:5,8–18` states common snapshot fields outside the actual
table field lists; snapshots can appear historical while fact keys remain
edition-scoped. That shape cannot both retain several snapshots' facts and
resolve them unambiguously. Changing FFRF methodologies and rolling scores make
this an observed requirement, not only a hypothetical concurrency problem.
The parent requires a new capture to be a new snapshot of the same edition
(`docs/scorecard-integration.md:74–78`).

**RESHAPE:** accept the coordinator's minimal model: every family generation
contains exactly one accepted snapshot for each held edition; older snapshots
remain in prior immutable generations. Expand common fields into actual source
table columns. Publisher identity metadata remains separately captured and does
not require an edition snapshot.

Do not add composite foreign references to non-identity parent columns.
`spicy-docs/src/spicy_docs/schemas/tables.py:215–221` requires a `Reference` to
name the parent's exact identity; `schemas/__init__.py:168–176` enforces it.
Keep normal references to `scorecards(scorecard_id)`,
`scorecard_snapshots(snapshot_id)`, and each declared member/item/metric key.
Add bundle checks that:

- Each edition has exactly one snapshot; it points back to that edition.
- Every edition fact has that edition's selected snapshot ID.
- Every referenced member, item, metric, participation and methodology has the
  same selected snapshot; a valid key in another snapshot is insufficient.
- Each fact capture belongs to the accepted snapshot's declared capture set;
  capture roles and policy-permitted identity are explicit.

Closure: mixing an old methodology or item with a new rating refuses even when
all ordinary key references resolve. A failed edition refresh preserves the
prior snapshot across all its tables.

### F3 — CONCERN: repeated-action and reference-occurrence rules need fixtures

The draft's identity direction is correct (`work/gate/schema_decisions.json`,
`identity`), but a bill citation plus title does not identify an action.
AFL-CIO's House workbook repeats the same H.R. 4 title in cells M1 and P1;
its vote listing supplies two distinct detail URLs. The workbook also includes
a letter, while the Senate workbook includes nomination `PN346-2`.
Evidence: `E-AFL` in the review receipt. NIAC's edition labeled 117th includes
a 2015 action and a committee-only action (`E-NIAC`).

**RESHAPE:** require an adapter identity fixture proving that repeated columns
remain separate actions. A source-proven column-to-detail mapping can use the
distinct publisher URLs; matching duplicate headers or relying on column order
alone cannot establish durable identity. If the mapping is unresolved, refuse
that adapter's accepted bundle or explicitly admit snapshot-local occurrence
keys with a documented continuity limitation. Do not silently collapse it.
Letters and nominations keep their source types and do not require bill links.
The acquired AFL-CIO letter detail explicitly explains that its voting-system
`no` denotes signing the letter and that its passed/failed label is inapplicable
(`E-AFL-LETTER`). Preserve the literal action and bind its publisher-specific
meaning to that rule. A generic conversion from `no` to opposition would reverse
the source meaning; no new official roll call should be invented.

The ordered JSON decision also needs a small defined member shape before freeze:
reference occurrence ID, raw citation, source kind, source-stated period/context
and locator. Preserve duplicates. Downstream reference links must include the
source snapshot and reference occurrence, not just edition/item. Never infer an
item's Congress from the edition when source evidence supplies another period.

### F4 — CONCERN: choose a source rendition before applying rating uniqueness

The LCV CSV reports Katie Britt's lifetime value as `2`; HTML displays `2%`.
Both fit the same edition/member/metric key but have different literal values.
`scorecard_member_ratings` cannot retain both as the same row without a selection
rule (`proposed_schema.json:16`; `E-LCV`).

**KEEP with an explicit selection rule:** the coordinator selects CSV rating
values and uses HTML to check membership. Record capture roles and the
field-group selection rule in the accepted snapshot. Store CSV `2` unchanged;
any percent unit must cite the publisher's metric definition, not a fabricated
percent sign in `value_text`. Conflicting score values across selected sources
must be an explicit refusal or recorded conflict decision. A second observation
table is unnecessary when the product deliberately publishes one selected
rendition. The CSV's body scope, not the chamber query parameter, determines
which membership was actually returned.

### F5 — CONCERN: relative periods need rules, not invented year ranges

Planned Parenthood states a cumulative calculation over the current and prior
six sessions, while edition tables show Congress-specific values. The word
session must remain the publisher's wording until its exact meaning is proven.
NIAC's action period can precede the edition. Evidence: `E-PP`, `E-NIAC`.

**RESHAPE:** make the chosen JSON period shape distinguish source-stated fixed
periods, relative window rules and unknown bounds. Preserve the literal window
and as-of observation; do not expand it into unverified Congresses or dates.
Give annual, lifetime and cumulative values separate metric identities and
periods. Define a stable logical live-edition key for a rolling source, with new
snapshots on refresh, rather than inventing a publisher-issued dated edition.
Explicitly exclude this acquisition shape from V1 if a stable scope cannot yet
be proved. Methodology-only evidence is not a complete member snapshot.

## Artifact summary and lineage

Reviewed: `proposed_schema.json` proposal-0.1 and
`work/gate/schema_decisions.json` pending research acceptance. The problem is
preserving publisher ratings and scoring observations without forcing them into
roll-call data. Category: research and proof infrastructure serving attributed
query results. No production implementation or ratification was observed.

| Prior artifact | Relationship | Citation |
| --- | --- | --- |
| Integration plan | Parent ownership, literal values, identity and completeness commitments | `docs/scorecard-integration.md:Ownership and existing integration points; Invariants to settle before freezing V1` |
| Source workflow | Raw-reader recovery remains downstream; replay requires retained inputs | `spicy-docs/docs/source-workflows.md:Choose the output you need; Read the result before using it` |
| Profiles and bounded examples | Shape evidence; explicitly not complete accepted snapshots | `shape_profiles.json`; `schema_examples.json:scope` |
| Operations design | Same-family atomic publication, prior retention, evidence modes | `work/operations/README.md:Evidence format and policy; Complete-scope replacement` |
| Table definitions | Identity-targeted references and string serialization | `spicy-docs/src/spicy_docs/schemas/tables.py:80–113,215–251` |

| Seam | Owner | Input → output | Named? |
| --- | --- | --- | --- |
| Source reading | SpicyDocs | Publisher captures → literal rows, completeness result | Yes, integration plan acquisition section |
| Scope replacement | SpicyRegs | Qualified edition bundle + prior → complete family generation | Yes, operations design |
| Identity resolution | SpicyRegs | Source facts + existing congressional pins → attributed links | Yes, integration plan resolution section |
| Query delivery | SpicyRegs | Published generation → Parquet, DuckDB and existing MCP tools | Yes, integration plan ownership section |

## Invariants and user value

| Commitment | Assessment | Failure prevented |
| --- | --- | --- |
| Preserve source facts without inventing metrics | **Broken by current result key; F1** | NRF results become a fabricated score. |
| One complete accepted edition replaces one scope | **Relied upon; requires F2 checks** | Mixed old/new methodology or chamber data. |
| Multiple actions on one bill remain distinct | **Preserved in intent; F3 qualification open** | Different H.R. 4 actions merge. |
| Literal values, blanks and publisher identity remain unchanged | **Preserved if parser fixtures enforce it** | `ｘ` becomes an official no, blank becomes zero, or `NES` silently becomes `NE`. |
| Ratings do not require reproducible methodology | **Preserved by support-dimension decision** | NEA grades are discarded solely because contributions are undisclosed. |
| Reuse congressional data | **Preserved** | Duplicate upstream ingestion and inconsistent identities. |

NEA's literal `NES` for Pete Ricketts must remain on the source member row;
resolution can remain unresolved or use an explicitly reviewed rule. AFL-CIO
`✓`, fullwidth `ｘ`, `◯` and blank are distinct tokens; normalize their meanings
only from the publisher's legend. ILA's constituency-relative grades fit a
distinct metric and methodology description; no observed sample requires a new
formula system or an invented metric per district. National Parks' explicit
House-only coverage is a valid complete source scope, not a missing Senate
failure (`E-NEA`, `E-AFL`, `E-ILA`, `E-PARKS`).

The beneficiary is a reader comparing attributed member results and tracing
their source. The model earns its complexity only if it keeps those distinctions
queryable. Adding an all-purpose formula engine or parallel evidence system
would add debt without serving that initial outcome. Current-generation rows
plus immutable prior generations deliver snapshot history without duplicating
every history key in the public current tables.

## Counterfactuals and review trace

Kill criterion: an admitted source result requires an invented metric, period,
action identity or corrected literal to pass validation. That would invalidate
the claimed source-faithful model. Without source-specific records, the first
consumer failure is an unexplained comparison or untraceable rating; without
the new snapshot table, current rows could still publish but could not enforce
the intended accepted-edition grouping. Existing generation artifacts already
provide immutable historical storage, so a second append-only scorecard-history
system is unnecessary for V1.

The review tested these hypotheses: mandatory metric participation covers all
results (**refuted by NRF**); the current draft settles snapshot relationships
(**refuted**); ordered source references can preserve non-roll-call and repeated
actions (**confirmed with required occurrence rules**); rating-only and
relative-grade publishers require new formula machinery (**refuted**). Every
finding traces to the parent source-fidelity commitments and the named storage
and resolution seams. No phase of the architecture review was skipped.

Operational readback is separate from this schema verdict. The coordinator's
`operational-prerequisites.json` confirms the requested congressional tables are
present and the configured development hostname returned the same index bytes
as the custom domain at the recorded observation. It also records that neither
scorecard family is published. This removes an assumed missing-data prerequisite;
it does not qualify the schema, prove deployment readiness or authorize release.

Verdict basis: intent and present shape diverge at F1/F2; user value is supported;
the proposed reshapes honor the commitments and pay down ambiguity; sibling
subsumption is partial through existing immutable generations. Confidence is
high for the structural findings and bounded observed examples. Census and
sample qualification remain in progress, so this review makes no completeness
claim for the source universe or any production adapter.
