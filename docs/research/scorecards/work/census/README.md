# Bounded census handoff

This packet extends the source census and tests observed source shapes against
`proposal-0.2`. It does not qualify production adapters or complete snapshots.
Measured coverage for this dated pass is in [coverage_receipt.json](coverage_receipt.json).

## Integrate in this order

1. Add [capture receipts](capture_receipts_additions.json), preserving failed and
   wrong-content attempts. HTTP success and complete response bytes do not prove
   a complete scorecard.
2. Review [catalog additions](catalog_additions.json) and apply
   [catalog updates](catalog_updates.json) by stable `source_id`. Updates append
   observations; they must not erase earlier evidence or replace known fields
   with null. Multiple profiles can update one source.
3. Add [shape profiles](shape_profiles_additions.json) and
   [schema examples](schema_examples_additions.json) by `profile_id`. Each literal
   example binds to a specific source field. `example:` keys are local research
   assignments, not qualified production identity rules.
4. Retain [candidate outcomes](candidate_outcomes.json) as the disposition of
   previously unprofiled sources. Keep remaining leads discovered and unprofiled.

The [discovery run](discovery_run.json) records the bounded inputs. The
[Vote Smart inventory](votesmart_directory_leads.json) contains directory IDs,
category memberships and last-rating labels. These are leads, not verified
federal publishers: endorsements-only entries, state groups and repeated category
memberships remain distinguishable. Existing-source name crosswalks are partial;
unmatched directory names do not establish new publisher identities. Do not
promote the whole inventory into the federal catalog.

The [historical CRS inventory](historical_crs_directory.json) enumerates full
organization entries from printed CRS pages 1–21. Appendix repetitions and
abbreviation cross-references are excluded. A historical directory entry does
not establish a surviving original scorecard, its edition year, numerical rating,
current publisher continuity or redistribution rights. Candidate-only surveys
stay outside the federal-rating catalog. Original-publisher verification remains
pending for the historical additions.

## Decisions supported by the examples

- NRF exposes a member-by-item grid without an aggregate rating. Its S.1404
  target and member result are retained using `scorecard_items` and
  `scorecard_member_item_results`, with a `result_id` and null metric/participation.
  Proposal-0.2 resolves the earlier `C-M01` issue. A bill column alone does not
  establish cosponsorship.
- Planned Parenthood describes a rolling window in sessions. Preserve that
  relative wording; this capture does not establish concrete period endpoints or
  a complete member table. NIAC's named congressional edition includes older
  actions, so item dates cannot be constrained to the edition Congress.
- ILA's district-relative method can be retained as disclosed text. Its
  proprietary calculation remains unavailable. Grade preservation and score
  reproduction are separate decisions.
- National Parks explicitly defines a House-only edition. This supports a
  source-defined chamber scope; an empty or failed Senate read would not.
- ACEC mixes votes, cosponsorship and a sign-on letter. FP4A separates excluded
  actions from counted items and displays uncounted leadership actions. Published
  values and source symbols remain distinct from our interpretation of official
  congressional actions.

## Readiness and limits

`source_model_fit` is `fits_proposal` or `unsupported` for the observed fragments.
Methodology-only or scope-only admission covers those observed facts, not unseen
member rows. `acquisition_readiness` remains `source_snapshot_not_qualified`.
`reproduction_readiness` separately records analytic readiness, including
`blocked_undisclosed_formula`. `readiness_limits` identifies the category and
reason for a limitation. The historical ADA scanned grid remains `C-U01`, a
source-fixture deferral with no checked member cell.

The examples are deliberately partial. No accepted `scorecard_snapshots` row,
complete roster, parser qualification or production-supported source is asserted.
The Boilermakers historical series is distinct from its current redistribution
of AFL-CIO material. Report labels that differ from scored years remain separate.
Signed contributions are not evidence of negative item weights.

Public evidence is hash-only: URLs, response metadata, source hashes, parser
version and short source fragments. Full source bodies, extracted documents and
rendered checks remain in temporary research storage outside the repository.
The failed-access record includes wrong-content HTTP 200 responses; neither
failures nor missing editions imply deletion or retirement.

See [validation_receipt.json](validation_receipt.json) for packet checks. The
coordinator owns canonical merge, generated reports and the schema gate; this
worker changed no canonical survey, runtime, task status or Git history.
