# Reconcile GovTrack discovery leads

This workflow produces a review report for the scorecard source census. It reads
GovTrack's publisher metadata and original links, then proposes exact matches or
new discovery leads. It never imports GovTrack's ratings, verifies an original
publisher, edits the census, or changes production support status.

The installed SpicyDocs provider owns bounded acquisition and the mixed YAML/CSV
parser. SpicyRegs owns private retention, explicit aliases and census matching.
The provider requires its existing `acquisition` and `yaml` extras. Install the
pinned package through this repository's lockfile; do not import a sibling tree.

## Capture once, review offline

Run from the SpicyRegs repository. Select an empty private directory outside the
repository for the mixed upstream bodies; the report contains metadata only.

```sh
uv run --frozen python -m spicy_regs.scorecards.govtrack_discovery acquire \
  --commit fa63a2b5326edd4b8835386082c2317627f058ce \
  --retained-dir /private/corpora/govtrack-discovery/run-001

uv run --frozen python -m spicy_regs.scorecards.govtrack_discovery reconcile \
  --retained-dir /private/corpora/govtrack-discovery/run-001 \
  --catalog docs/research/scorecards/scorecard_source_catalog.json \
  --aliases docs/research/scorecards/work/integration/govtrack_discovery/aliases.json \
  --output /private/reviews/govtrack-discovery.json \
  --imported-at 2026-10-03T20:00:00Z
```

Use the actual import timestamp for a new review. Replaying an existing report
with its original timestamp and pinned inputs is deterministic. `reconcile`
performs no network calls. It checks the capture manifest, every stored body
hash, the commit's root tree, exact tree membership and each file's Git blob hash.
Output is written atomically only after the entire replay and reconciliation
succeeds. An invalid capture leaves any earlier review intact.

A capture stops on access refusal. A failed manifest keeps safe, bounded refusal
metadata separately from successful captures when the shared transport provides
it. It does not save error strings or failed response bytes. Missing status or
observation fields mean the transport did not supply them; they are not inferred.
A failed attempt must use a fresh empty directory when retried. A complete
capture proves repository metadata membership, not complete original scorecards.

## Review identity explicitly

The catalog's `source_id` is unique. Multiple source series may share a
`publisher_id`; their identities and statuses survive in
`catalog_sources_at_import`. Exact names (case and whitespace normalized) and
literal URLs produce candidate publishers. No fuzzy matching or URL rewriting
occurs. Multiple candidates remain `ambiguous`.

An alias file has `version: 1` and a `rules` list. Each rule gives a
`repository_path`, existing `publisher_id`, and review `reason`. Aliases are
checked against exact-name/URL candidates; a conflicting alias remains ambiguous
instead of overriding evidence. Repeated or absent paths refuse. Multiple
GovTrack files matched to one publisher remain distinct leads and appear in
`duplicate_lead_groups`.

Unmatched files receive a stable proposed publisher ID derived from repository
path, `discovery_status: discovered`, and an unreviewed rights status. A reviewer
must investigate original publisher evidence before merging any proposal. No
proposal changes the catalog automatically. Existing publisher names, rating
sources and support statuses are never replaced by this import.

`observed_at` records acquisition; `imported_at` records this reconciliation.
Upstream `updated_text` remains literal because its documented meaning includes
publication or import. It is not `published_at` or `last_verified_at`.

## Rights and qualification

The pinned upstream tree contains no license grant. The reader is independently
implemented, does not copy `lint.py`, and ignores the CSV rating tail. Private
whole-file retention is necessary for blob verification; it is not permission to
republish upstream files or original publisher scorecards. Fixtures are synthetic.

See the [pinned discovery report](research/scorecards/work/integration/govtrack_discovery/README.md)
for the measured import and the proposed alias decisions. Focused checks:

```sh
uv run --frozen pytest tests/test_govtrack_discovery.py
uv run --frozen python -m spicy_regs.scorecards.govtrack_discovery --help
```
