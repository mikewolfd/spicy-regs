# Exact resolution handoff

`spicy_regs.scorecards.resolution.resolve_scorecard_links` produces
`scorecard_member_links` and `scorecard_item_links` as lists of dictionaries with
only strings and nulls. It reads supplied rows and performs no acquisition,
filesystem writes, publication or changes to source ratings.

```python
links = resolve_scorecard_links(
    source_tables,       # scorecards, scorecard_members, scorecard_items
    official_tables,     # existing members, member_terms, bills, amendments, votes
    input_pins,          # verified immutable table and family pins
    member_overrides=(), # optional explicit versioned records
)
```

The module exports `SOURCE_TABLES`, `OFFICIAL_TABLES`, `INPUT_TABLES`,
`OFFICIAL_COLUMNS`, `MEMBER_LINK_COLUMNS`, `ITEM_LINK_COLUMNS`, `LINK_COLUMNS`,
`LINK_KEYS` and `RULE_VERSION`. The coordinator can use `OFFICIAL_COLUMNS` to
avoid loading unrelated congressional text. See [analysis_tables.json](analysis_tables.json)
for the column and key handoff. These tables belong to `scorecard-analysis`.

## Inputs and reproducibility

Input mappings use logical table names without `.parquet`. Every declared input
requires its verified publication pin: `family`, `artifactDigest`, `byteSize`,
and exactly one of `sha256` or `tableDescriptorDigest`. A single file uses its
SHA-256; a split table uses the digest of the complete canonical descriptor
binding all its immutable members. Digests use `sha256:<64 lowercase hex>`.
The full supplied pin mapping, including integer byte sizes and any partition
metadata, is retained as JSON text on every link. The caller verifies the
actual files and descriptors before invoking the resolver.

Every source row must match its edition's selected `snapshot_id` and have a
capture, URL and locator. Link rows preserve that snapshot, the original source
context, the resolver version and exact input pins. Duplicate source identities,
duplicate official identities, orphan member terms, invalid pins and source
snapshot mismatches refuse the build. Ratings are independent inputs and are
never filtered by link success.

## Member decisions

The order is explicit Bioguide, exact known crosswalk, exact historical name
with chamber/state/district, unique normalized name within historical terms,
versioned override, then unresolved. Known identifier schemes correspond to
the existing `members` fields, including LIS, GovTrack, Vote Smart, ICPSR, OpenSecrets,
Wikidata and the FEC identifier array. An arbitrary publisher member ID is not
treated as a Bioguide ID. Source identifier labels outside that vocabulary are
retained and reported; they do not establish a known crosswalk.
Heritage's `cong_id` is one such deferred label: the survey observed strings
that resemble Bioguide IDs but did not qualify that semantic crosswalk. V1
does not reinterpret those strings as Bioguide IDs. A separately qualifying
historical name or versioned override can still resolve the member.

Rule version `scorecard-resolution-v1.1` adds the exact `votesmart` and
`votesmart_id` schemes, both reading `members.votesmart_id`. The source integer
is published as text by the existing members pipeline; source identifier text
is matched exactly, without padding removal or numeric coercion. Repeated
crosswalk values retain every candidate. A name cannot break an identifier
collision; exact historical context can qualify one candidate while keeping
the rejected candidates in the output.

Rule version `scorecard-resolution-v1.2` also preserves the crosswalk's
`bioguide_previous_json` and `other_names_json`. An explicit previous Bioguide
joins the same candidate set as current Bioguide IDs, with
`bioguide_previous` recorded in the candidate's matching evidence. A collision
with another current or previous ID remains ambiguous unless exact historical
context identifies one candidate. A conflicting known identifier still refuses
weaker name matching.

Historical names apply the source's partial first/last-name patch to unchanged
attributes. They qualify only when at least one explicit ISO date bound exists,
all stated bounds are recognized, and the alias interval, source period and
same congressional term overlap. The resolver uses conservative half-open
`[start,end)` alias intervals; it does not claim the upstream dictionary defines
an inclusive end day. A missing bound is limited by the known term and source
period, while an undated patch cannot establish a historical alias match.
Unknown qualifiers, invalid/reversed dates, or out-of-period aliases remain
unresolved and retain candidate evidence. Candidate JSON records the literal
patch and its source list index. Current primary names retain their existing
historical-term rule; no omitted date or name attribute is written back to the
source. See [the pinned measurement](historical_identity_measurement.json) and
[reproducible measurement script](measure_historical_identities.py).

Adoption requires the SpicyDocs package containing `members.votesmart_id`,
`members.bioguide_previous_json` and `members.other_names_json`, and a rebuilt
members publication carrying those columns. `OFFICIAL_COLUMNS` requires
these fields, so the analysis builder refuses an older member artifact with a
missing-column error. An existing null column means the source did not supply
that person's ID; it is distinct from an artifact whose schema lacks the
column. This code change does not rebuild or publish congressional inputs.

Unknown recognized identifiers and conflicting identifiers block weaker
matching. Recognized state, chamber, district and period assertions must agree
with a historical term before an identifier is linked. A name mismatch alone
does not invalidate an explicit ID: the crosswalk does not provide a complete
historical alias or nickname history.

Name matching uses the crosswalk's first and last names against historical
terms. Supported exact display orders are first-last, last-comma-first and
last-first. Normalization uses Unicode NFKC, case folding, punctuation and
whitespace; it does not use edit distance, nickname substitution, accent removal
or token subsets. Surname-only matching requires the complete geographic and
chamber context. Name matching requires an exact supported historical period.
Only the source's dated historical aliases extend those name forms.

Terms are half-open. For an exact source day with no matching half-open term,
an inclusive-end fallback applies only when it identifies one person. A source
year means its calendar interval; a numbered Congress means its historical
Congress interval. ISO day and half-open ISO interval text are also supported.
Relative or lifetime periods remain unsupported for historical name matching.
The source member's period overrides the edition period because it may refer
to a different historical context.
Otherwise, explicit `periods_json` coverage precedes `year_text`, which precedes
`congress_text`. This preserves C4IP's earlier congressional coverage instead
of matching only its 2024 release year. An unrecognized explicit period remains
unresolved; it does not fall back to a narrower release-year interval.

State names and postal abbreviations are exact aliases. Unknown state `NES`
remains `NES` in `source_context_json`; it is not repaired to Nebraska. The
resolver records `unknown_state_literal_ignored:NES`, skips the complete-context
name rule and may still resolve a unique full name in the historical term.
Unknown chamber or district labels receive equivalent explicit notes.
For an exactly stated Senate chamber, a publisher district such as `00` is
not applicable to historical Senate terms. It remains literal in source context
and is reported with `senate_district_not_applicable_literal:00`. House `00`
still constrains the match to an at-large district.

Overrides require `scorecard_id`, `publisher_member_key`, `bioguide_id`,
`version` and `reason`. They can settle an unresolved or ambiguous name, but
cannot bypass a known ID conflict or a contradictory recognized term context.
Candidates, rejected contexts and qualifying term rows remain in the output.

## Legislative decisions

An exact floor roll requires source identifiers sufficient to select one official vote.
An exact bill or amendment citation identifies the measure alone. The resolver
accepts the existing natural identifiers and anchored common citation spellings;
unsupported prose remains unresolved. A reference occurrence with a typed
`kind` can use its literal `citation_text`. There is no inference from title
similarity, a bill's available votes, or an edition Congress. In particular, an
older NIAC action without its own Congress does not inherit the edition's.

The explicitly typed bill citation also accepts `hr4274-115`-style identifiers,
observed in Heritage's original cosponsorship API and matched to the pinned
[`unitedstates/congress` grammar](https://github.com/unitedstates/congress/blob/8184bcb13160da2389dea4c902de67817db681fc/congress/tasks/utils.py#L127-L154).
This branch does not reinterpret arbitrary `publisher_item_id` values. See
the `heritage-cosponsorship-api` and `heritage119-cosponsor-s382-119` captures
in the source survey for the historical and current source evidence.
The anchored amendment spelling `hamdt2-119` follows the pinned
[`build_amendment_id`](https://github.com/unitedstates/congress/blob/8184bcb13160da2389dea4c902de67817db681fc/congress/tasks/amendment_info.py#L141-L142)
and selects only an existing amendment. A typed roll reference `h1-119.2025`
selects official candidates by Congress, chamber and roll number, then uses
the chamber's printed `roll_call_votes.vote_date` through the existing
SpicyDocs `vote_day` parser. The selected canonical `vote_id` supplies its
actual ordinal session. Multiple qualifying sessions remain ambiguous, and
explicit conflicting session/measure/context assertions refuse the link.
The required vote-date column already belongs to the official table; missing,
unrecognized or non-chamber date spellings do not establish a year match.
No calendar-to-session arithmetic is used.

Rule version `scorecard-resolution-v1.3` also accepts a literal four-digit
`item_date_text` year with the item's chamber and roll number. LCV's CSV
`Year` preamble supplies this evidence. The resolver searches actual official
vote dates and takes Congress and session from the unique matching canonical
vote ID. Explicit source Congress/session values constrain that search; a
conflict or multiple candidates remains visible. Missing or invalid official
dates cannot qualify a link. An edition year never substitutes for an item's
year, and this rule does not select floor rolls for committee, sponsorship,
cosponsorship or bill-only items.

Heritage's shorter `h168-2025` spelling remains unsupported because it omits
Congress. Opaque publisher item IDs are never automatically parsed as roll
references. Four-digit values placed directly in `session_text` remain source
literals and yield `calendar_year_session_not_ordinal`; they never populate
the ordinal `session` column.

Each repeated source reference is retained independently. Direct item fields
use `reference_id=item:direct`; a source occurrence uses
`reference:<occurrence_id>` and keeps its unmodified `source_reference_id`.
Occurrence context does not silently inherit item-level or edition-level
identifiers. Multiple actions on one bill keep separate item keys.

Cosponsorship, sponsorship and bill items select measures only, leaving vote,
session and roll fields null. Committee actions are not mapped to floor rolls.
An exact vote can supply its existing bill link when that bill exists in the
pinned bill table. Contradictions between explicit bill, amendment and vote
identities clear selected targets and record `conflict`. A separately exact
amendment and roll are retained as source-cited targets, with an explicit note
that their mutual relationship has not been validated from these inputs.

`unresolved` may retain an independently exact bill or amendment when another
stated identifier is incomplete. Consumers must check `resolution_status` and
`reason` rather than treating every non-null field as a complete action match.
For members, `candidate_count` counts retained distinct Bioguides, including
context-rejected candidates. For items, it counts retained exact target
identifiers across the bill, amendment and vote types. The JSON gives each
candidate's identity and context; these counts are not matched-row totals.

## Validation

Run `uv run --frozen --no-sync pytest -q tests/test_scorecard_resolution.py
tests/test_member_vote_terms.py` from `spicy-regs`. The focused tests exercise
historical terms, chamber changes, redistricting, ambiguity, identifier conflicts,
versioned overrides, exact legislative references, repeated references,
cosponsorship, amendments, unknown citations, input pins and source preservation.
This handoff qualifies resolver behavior on fixtures, not publisher coverage or
publication freshness. The coordinator owns the pinned analysis rollup and
shared table/dictionary metadata.
