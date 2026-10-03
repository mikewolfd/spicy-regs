# Congressional scorecards: integration plan and research gate

For selected blocked public publishers, see [explicit Zyte acquisition](scorecard-acquisition.md).

The [component reuse review](research/scorecards/reuse_review.md) identifies the
existing readers, identity conventions and extraction infrastructure to retain.
It also defines the repeatable repository review method and concrete LCV 2025
qualification target. The [implemented ecosystem takeaways](research/scorecards/work/integration/ecosystem_takeaways.md) record the discovery and identity changes. Broader implementation follows the
[candidate task manifest](research/scorecards/work/integration/adapter_tasks.json).

Build scorecards as a SpicyDocs source vertical and publish them through
SpicyRegs. Preserve the publisher's rating, methodology and preferred action as
attributed source facts. Resolve people and legislative references against the
existing congressional tables. Begin production ingestion only after the source
survey supports the model and every sampled shape has an explicit disposition.

**Status:** sampled source model frozen as `scorecards-v1`; implementation is
enabled by the [accepted gate](research/scorecards/work/gate/freeze_receipt.json).
See the [research baseline](research/scorecards/README.md),
[generated coverage](research/scorecards/coverage_summary.json) and
[sample dispositions](research/scorecards/schema_fit.md). The
[bounded row examples](research/scorecards/schema_examples.json) preserve sampled
literal values and exercise proposed relationships. These files do not
enable a source, change publication, or schedule a refresh.

## Ownership and existing integration points

| Owner | Responsibility | Existing code to reuse |
| --- | --- | --- |
| SpicyDocs | Bounded acquisition, publisher-specific parsing, literal fields, validation, provenance and reusable table schemas | `spicy_docs.transport`, `spicy_docs.reading`, `spicy_docs.schemas`; [source workflow](../../spicy-docs/docs/source-workflows.md) |
| SpicyRegs | Source selection, rights/evidence policy, cadence, backfill, complete-scope replacement, identity resolution and publication | `src/spicy_regs/source_evidence.py`, `pipelines/rollups/base.py`, `sources/publication.py`, `transforms/` |
| SpicyRegs consumers | Table descriptions, joins and attributed queries | `table_metadata.json`, `table_joins.json`, existing `list_sources`, `describe_table`, `query_sql` |

`CaptureEvidence` now applies source-bound `full`, `hash_only` and
`metadata_only` policies to capture and evidence admission. Existing federal
sources retain their full-evidence behavior. The scorecard registry defaults to
hash-only evidence. The publication layer checks fixed family membership and
immutable input pins; reader qualification remains separate from these checks.

The inspected local revisions are recorded in the survey's validation receipt;
the workspace had unrelated changes. This plan makes no claim about deployment.

## What the survey changes in the candidate model

The [proposed schema](research/scorecards/proposed_schema.json) keeps the original
publisher, edition, metric, item, metric-item and rating concepts. The evidence
requires additional source facts:

| Observation | Required representation |
| --- | --- |
| LCV excludes excused or ineligible missed votes; HRC records exceptions for leaders and certain procedural votes | Publisher member-item results, eligibility, contribution and reason; these are distinct from an official vote comparison. |
| Humane World Action Fund publishes leadership symbols and `100+` | Preserve literal ratings and adjustments. `100+` has no invented numeric equivalent. |
| The Chamber combines component scores; C4IP also distinguishes dimensions and historical activity | Metric-to-metric components, source-stated weights, period and rule scope. A scalar weight on a metric cannot describe all relationships. |
| AJP Action covers activity across Congresses and uses different grade thresholds for new members | Edition identity separate from activity period; chamber/cohort methodology and ungraded member records. |
| HRC prints earlier Congress scores beside the current score, including prior House service | Give each measurement its own period and chamber context. Do not relabel historical values as current. |
| NEA grades include committee activity and accessibility; NORML grades include public positions | Published-rating-only sources are valid. Missing item detail stays unknown. |
| FFRF's live House and Senate methods differ and can change | Captured method versions; bounded edition snapshots rather than an unversioned continuing series. |
| NRF publishes an item-by-member grid without aggregate ratings | Item-level publisher targets and member results with optional metric context; no invented overall metric. |
| AFL-CIO scores two actions on H.R. 4 separately and uses `no` to mean signing one letter | Distinct publisher action identities and literal action labels with their source explanation. A publisher label is not an official congressional vote. |

Evidence: [LCV FAQ](https://www.lcv.org/congressional-scorecard/about-the-scorecard/),
[Chamber method](https://www.uschamber.com/congressional-scorecard-and-legislative-leadership-list),
[Humane method](https://humaneaction.org/humane-scorecard/methodology),
[AJP method](https://ajpaction.org/scorecard2026/),
[NEA report](https://www.nea.org/advocating-for-change/action-center/nea-in-congress/report-card),
[NORML rubric](https://vote.norml.org/about), and the exact captures linked from
[each profile](research/scorecards/shape_profiles.json).

The additions are `scorecard_snapshots`, `scorecard_methodologies`,
`scorecard_metric_components`, `scorecard_members`,
and `scorecard_member_item_results`.
They preserve reported facts; they do not implement a general scoring engine.
Standalone `scorecard_member_adjustments` is deferred until a real member
adjustment ledger supplies a qualified fixture. Preserve observed `100+`
ratings, item-level adjustment text and methodology without inventing points.

`source_url`, `source_path`, capture identity and accepted snapshot identity must
be available for each source observation. Snapshot manifests link all source
responses used by a parse. Publisher registry observations can have their own
capture without depending on one scorecard's snapshot. Methodology tables can
retain short literal rules and locators; substantial source text remains subject
to the configured evidence and redistribution policy.

## Frozen V1 invariants

1. **Stable scope.** `scorecard_id` names a publisher-defined edition. Keep a
   separate series identifier and source edition key. A new capture or parser
   version creates a new snapshot, not a new logical edition. An edition may
   contain separate chamber or cohort metrics. Publication time, observation
   time and scored activity period are different fields.
2. **Literal values.** Keep `value_text`, units, grade, rank, weight text,
   annotations and missing-value markers. An exact numeric projection of `94%`
   may be `94` with a percent unit; it must not silently become `0.94`. `A+`,
   `100+` and `N/A` do not become numbers. A displayed `100` does not acquire a
   percent sign just because other publishers use percentages.
3. **Source identity.** Use publisher member and item IDs where supplied.
   Names, district strings, party and raw identifiers remain unchanged. A local
   key must state its versioned rule and collision behavior. Row position is a
   locator, not automatically a durable person or item key. Ambiguous key drift
   refuses replacement.
4. **Relationships.** Metric items reference existing items and metrics;
   components reference metrics within the same edition and form no cycles.
   Member ratings and results reference source members, including excluded or
   ungraded members. Do not require all known members of Congress to appear.
   Member-item results may omit metric and participation together when the
   publisher supplies an item grid without a measurement. Item-level target
   positions carry their own source locator; metric-specific targets retain
   their metric context.
5. **Unknowns.** Missing target, unknown weight, not disclosed, not eligible,
   blank cell and parser failure are distinct. Never infer equal weights from
   absent weights, or an opposing position from a missing action. A position
   inferred from presentation must say so and cite the legend.
6. **Action scope.** Bills, amendments, motions, nominations, letters,
   sponsorship, caucus membership and committee actions can all be scored.
   Multiple actions on one bill remain separate. Preserve repeated legislative
   references; do not collapse companion bills into one invented identifier.
7. **Attribution.** Publisher-reported member results, official actions and our
   comparisons occupy different tables. HRC's credited support for a strategic
   no vote must never overwrite the official no.
8. **Selected snapshot.** A family generation contains exactly one accepted
   snapshot per edition. Every edition fact carries that snapshot ID and a
   capture in its manifest. Referenced members, items and metrics must share
   the snapshot. Ordinary foreign keys check identity; bundle validation checks
   snapshot equality. Prior snapshots remain in immutable prior generations.
9. **Selected rendition.** Record capture roles and the source rendition chosen
   for each field group. LCV's CSV `2` remains `2` even when HTML displays `2%`.
   Corroborating pages validate scope; they do not silently rewrite literals.
10. **Relative periods.** Preserve ordered period and reference occurrences,
    including their raw text and source locators. A rule such as current and
    preceding six sessions does not authorize invented dates. A historical
    action within a later edition keeps its own stated Congress.

The revised proposal uses VARCHAR columns, including exact numeric text, and
ordered JSON arrays for repeated periods and references. This matches the
existing source-table representation and avoids floating-point conversions.
The research gate validated these choices against the expanded examples.
Source-model fit, acquisition completeness,
parser qualification and score-reproduction readiness are separate decisions.

## Acquisition, evidence and publication

Each publisher adapter belongs under `spicy_docs.sources.scorecards`, with
explicit `list_scorecards()` and `acquire_scorecard()` behavior. Shared code
covers HTTP bounds, URL handling, source captures and literal parsing mechanics.
The publisher adapter owns page structure, pagination, legends, identities and
completeness. Use source-specific fixture layouts; do not build a universal
scorecard scraper. XLSX and embedded data are observed acquisition cases in
addition to HTML, CSV, JSON, APIs and PDFs.

An adapter must return evidence for a source-defined replacement scope:
edition, included chambers/metrics, capture IDs, parser version, expected and
observed membership, and the successful end condition. EOF proves that a file
was read; it does not by itself prove that a downloadable file is the full
scorecard. Check the expected format, edition and source-declared scope as well.
Pagination, member tables and supporting vote/methodology pages must agree on
the same edition. A partial chamber may replace only a separately defined
complete chamber scope; otherwise refuse the whole edition update.

SpicyRegs' registry at `src/spicy_regs/scorecards/sources.yaml` chooses
`publisher_id`, adapter, enabled state, cadence, backfill, evidence policy and
rights status. Keep the research census independent of the operational registry.
A verified or profiled publisher is not automatically enabled.

`CaptureEvidence` supports policy per capture/source:

| Policy | Public evidence | Limit |
| --- | --- | --- |
| `full` | Response metadata, digest and admitted body bytes | Requires established redistribution rights; supports replay when all inputs are retained. |
| `hash_only` | URL, status, content type, observation time, byte size, SHA-256 and parser version | Default for scorecards. The digest identifies bytes but does not recreate them. |
| `metadata_only` | Available request/response metadata and parser version | No fabricated hash or size. State whether bytes were read; qualification must account for weaker evidence. |

Apply policy to success, refusals, request bodies, temporary staging, failure
artifacts and workflow uploads. Credential scrubbing and access refusal remain
mandatory. A hash-only journal must not advertise a downloadable blob. The
publication verifier must admit its actual members and explicitly recorded
policy, while preserving the existing behavior of federal sources.

Build the source family through `build_scorecards.py`,
`pipelines/rollups/scorecards.py` and `run-rollup-scorecards`. Stage all
source-backed tables from one pinned prior `scorecards` generation, replace rows
only for scopes with complete accepted results, validate, seal, and advance one
family pointer. Every untouched scope retains its prior snapshot and evidence.
Refuse a stale prior if another writer advanced the family; retry against the
new prior rather than losing concurrent updates.

```text
complete source-defined edition -> replace that edition in every dependent table
failed or incomplete edition     -> preserve every prior row in that edition
edition absent from current index -> preserve historical edition
```

A successful changed edition may remove an item only when the new complete
snapshot proves that item's absence within the same scope. Empty responses,
404s, challenge pages and parser errors never authorize deletion. If all scopes
fail, publish no new pointer. A mixed-success run may advance successful scopes
atomically alongside unchanged failed scopes and an explicit failure journal.

## Resolve against existing congressional data

Reuse `members`, `member_terms`, `congress_bills`, `amendments`, `roll_call_votes`,
`member_votes` and `bill_cosponsors`. Do not reacquire Congress data for scorecards.
The inspected keys are `members.bioguide_id`,
`member_terms.(bioguide_id, term_index)`, `congress_bills.bill_id`,
`amendments.amendment_id`, `roll_call_votes.vote_id`, and
`member_votes.(vote_id, member_key)`. Senate member-vote keys can use LIS IDs;
do not assume every source member key is a Bioguide ID.

Resolve members in the requested order: explicit Bioguide, exact known crosswalk
ID, exact historical name with chamber/state/district, exact unique normalized
name within the historical term, versioned override, unresolved. Unknown IDs and
contradictory identifiers remain explicit conflicts; do not silently fall through
to a weaker name match. Never fuzzy-match in V1. Preserve candidates and reasons
as well as candidate counts; retain the rule version and the immutable input
generation pins. Use historical terms rather than the latest member summary.

Resolve a roll call through Congress, chamber, session and roll number. An exact
bill citation alone cannot select one of several votes on that bill. A
cosponsorship item links to a bill, leaving roll-call fields empty. Repeated
references need separate link occurrences (`reference_index` or equivalent),
not one overloaded result row. Add `rule_version` and input pins to item links
as well as member links. Unresolved nominations or committee actions stay as
source facts; they do not become fictional floor votes.

Publish links in `scorecard-analysis`, pinned to both a `scorecards` generation
and the exact congressional generations. Later action comparisons and
reproductions use those same pins. Cosponsorship comparison must consider the
publisher cutoff, sponsorship date and withdrawal date; a current cosponsor
list alone cannot establish a historical absence.

Keep comparison results descriptive: `same`, `different`, `not_voting`,
`present`, `unresolved`, `not_applicable`. A missing official action is not
automatically `different`. Reproduce scores only through a publisher-specific,
versioned method with sufficiently complete inputs. Always return the published
value alongside the reproduced value and unresolved-item counts.

## Delivery order and acceptance

| Step | Acceptance evidence |
| --- | --- |
| Census | Reproducible discovery inputs, stable publisher/series IDs, original locations, aliases, explicit unknowns and generated coverage. |
| Shape survey | Edition samples and historical layout samples, hashed captures, locators, generated matrix and a disposition for every profile. |
| Schema proposal / freeze | Source-faithful example rows exercise every admitted relationship and literal value; unsupported shapes have explicit boundaries. Resolve remaining design choices before freezing. |
| SpicyDocs adapters | Acquisition/error, completeness, drift, stable-ID and schema checks using bounded fixtures; qualification report per source/edition. |
| SpicyRegs ingestion | Evidence-policy tests, registry, scoped replacement, mixed-success and failed-read preservation, atomic family publication, dictionary and existing MCP query checks. |
| Resolution | Historical terms, redistricting, chamber changes, ambiguity, conflicts, exact motions/amendments, multiple votes per bill, cosponsorship and unresolved citations. |
| Analysis | Attributed official-action comparisons, pinned inputs and optional source-specific reproductions; published ratings remain authoritative. |

The qualification summary must reconcile source-declared and parsed counts for
editions, metrics, items and ratings; report member/item resolution and ambiguous
rows; and include source hashes, parser version, scope and completeness proof.
Raw parsing qualification and downstream resolution are separate checks; an
unresolved identity does not justify dropping a publisher's rating.

## Query the attributed ratings

The source tables are declared in the dictionary and existing MCP surface.
`list_sources` reports whether their generations are available. A declaration
does not establish live data; the operational registry remains disabled until
its source qualification is accepted.

Once source and analysis generations are available, this query returns reported
values with publisher attribution and an optional exact member link. The
snapshot condition prevents a newer source rating from inheriting an older
analysis result. An absent link does not remove the rating.

```sql
SELECT
    COALESCE(s.publisher_name_text, p.name) AS publisher,
    s.title AS scorecard,
    s.edition_label_text AS edition,
    m.member_name,
    m.chamber_text,
    m.state,
    m.district,
    metric.name AS metric,
    r.value_text AS published_value,
    l.bioguide_id,
    l.resolution_status,
    r.source_url,
    r.source_path
FROM scorecard_member_ratings r
JOIN scorecards s USING (scorecard_id)
JOIN scorecard_publishers p USING (publisher_id)
JOIN scorecard_members m
  ON m.scorecard_id = r.scorecard_id
 AND m.publisher_member_key = r.publisher_member_key
 AND m.snapshot_id = r.snapshot_id
JOIN scorecard_metrics metric
  ON metric.scorecard_id = r.scorecard_id
 AND metric.metric_id = r.metric_id
 AND metric.snapshot_id = r.snapshot_id
LEFT JOIN scorecard_member_links l
  ON l.scorecard_id = r.scorecard_id
 AND l.publisher_member_key = r.publisher_member_key
 AND l.source_snapshot_id = r.snapshot_id
WHERE s.year_text = '2025'
LIMIT 20;
```

Keep the publisher's metric name and literal value together. A grade, a points
total and a percentage are different measurements even when they rate the same
member. Official-action comparisons belong in the separate analysis family.

Choose the initial adapters after the matrix has enough complete row fixtures.
A useful provisional coverage set is LCV, AFL-CIO, Heritage Action, Chamber,
Humane World Action Fund and NEA: this spans archives, workbook export,
cosponsorship, components, bonuses and qualitative grades. It is a proposed set,
not a measured maximum or final production selection. Keep HRC, NTU, C4IP, AJP
and FFRF as mandatory schema challenges even if their adapters come later.

Backfill the current 119th Congress, then the 118th, then recent history and
complete archives. Use the same qualified adapters and explicit historical
layout versions throughout. Keep discovery and source qualification ongoing;
neither an initial census nor one successful parse proves universal coverage.
