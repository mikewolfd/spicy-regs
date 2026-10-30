# Scheduled source evidence

The `members` and `committee-reports` rollups retain the exact responses used
by ordinary scheduled runs. SpicyDocs still acquires and parses each source.
Host adapters observe its `CapturedBodyResponse` results before later
validation can fail; they do not add another HTTP client or parser.

Each run writes to `output/source-evidence/<run-id>/`. Its `artifact/`
directory contains a journal and content-addressed blobs written through
SpicyDocs storage. Journal receipts include the source observation time,
requested and resolved URLs, response status, encoding, content type, byte
size, SHA-256, and request-body identity when present. `recorded_at` is the
journal time and never replaces the source's `observed_at`. A capture may
appear first at acquisition and again when used; these stages are not a
request count. Identical bodies share one local blob.

Members retain both roster responses, including a successfully parsed current
roster that later fails its required LIS identifier check. Reports retain
used collection listings, package summaries, MODS metadata, bodies and hearing
details. Successful metadata survives a later body or parse failure. The
page observer retains a parsed listing or detail response before the source
traversal checks count drift or overflow, so an offending page remains
available even when the traversal never yields it. The
journal records selected, deferred, unchanged and refused packages, the
discovery window, page and package caps, and checkpoint time. Agenda requests
remain outside this rollup's selected scope.

Credential refusals stop the run. Their bodies are excluded from storage;
configured credential echoes in response or request bytes are also refused.
Recorded URLs and errors pass through the source owner's credential scrubber.
Storage or journal failures abort the build rather than becoming a skipped
source record. Source responses retained before a later failure remain in the
failed run's audit artifact.

Rulespec admits a separate `spicy-regs-source-evidence` artifact. It is an audit
of observed responses, not a claim of complete source coverage or a supported
SpicyDocs source-native release. The table generation remains Parquet-only.
Its root `inputs` names the audit artifact with role `source-evidence` and the
captured prior family with role `prior-generation` when one exists. This
preserves the lineage of unchanged bodies and merged rows without changing
their observation times.

The retained prior root must match the publication index's immutable pin.
Its existing input references remain visible in the journal. A prior root
with `inputs=[]` is explicitly recorded as an inherited raw-source evidence
gap. This feature does not retroactively qualify it. Without a managed prior,
any legacy or local merged inputs likewise remain unqualified.

The Congress.gov index tables close that gap by reading again. A held detail
that no retained response backs is read once more, after the run's own new,
changed and unread records and under the same per-run detail cap. At first
that is every held detail: a legacy prior backs none, and neither does an
evidenced prior whose journal states no remainder. Each run journals what is
left as `unevidenced` in its `congress-index-selection` event. The next run
inherits that remainder from the prior's evidence journal, which it reads
through the prior root, the member manifest and the journal digest, without
reading any blob. The remainder only shrinks, so each detail is re-read once.
A held row the publisher no longer lists cannot be re-read, and stays in the
remainder by name.

The same remainder carries a held row that an older detail shape read
differently. Each selection event names its `shape_version`; since spicy-docs
0.54.0 that is `lists=stated-001`, under which a list the detail does not
state is NULL. When the prior was journaled under another shape, or none, the
rows the new shape can read differently join the remainder once. Under
`lists=stated-001` those are rows published before `detail_read` that hold
`[]` in a list column. Columns a stored field already gives a held row are
derived again on every row each run, with no request: `detail_read` from the
old list marker, the RIN columns and `nominations.is_civilian`.

Before publishing a table pointer, the publisher requires the exact admitted
audit input and re-admits it from storage. Missing, altered or failed evidence
blocks publication. Existing generations with empty inputs remain readable and
retain their original evidentiary limits.

The artifact root, member manifest and journal are stored under
`source-evidence/<artifact-digest>/`. Each blob member `blobs/sha256/<hex>` is
stored once, at `source-evidence/blobs/sha256/<hex>`, and shared by every
artifact that cites the same bytes. A daily run that re-captures an unchanged
source therefore sends, stores and reads back only its new bytes and small
metadata. Consumers resolve a `source-evidence` input with the same public base
URL: `blobs/` members under `source-evidence/`, all other members under the
artifact's digest prefix, then admit it with the input's expected pin.
Artifacts published before September 25, 2026 keep their blobs under their own
prefix and stay readable there.

A blob is created only from bytes whose SHA-256 equals its key, with
Content-MD5 on every request and a create-only condition, and admission reads
it back once. A blob that already exists is not downloaded again when its
stored size and ETag equal those of the locally admitted bytes; rulespec then
admits the artifact over those local bytes. Any other existing object is read
back and refused unless its bytes match. This check cannot see a same-size
replacement that also reproduces the MD5-based ETag, storage corruption that
leaves the ETag unchanged, or a replacement between the check and a later
reader's download. The shared prefix must be written only by this publisher.
A full audit can still read every blob and admit it.

The reusable workflow always uploads `output/source-evidence/` with the run
audit, on success and failure. The directory is outside hidden `.builds/`
paths. Failed builds have no new table pointer; `run-outcome.json` records the
run result outside the immutable artifact. A transport failure during the
pointer request can have an uncertain publication result, which requires
reading the public index again. Workflow artifacts follow the retention
period configured in `_rollup.yml`; successfully published evidence remains
under its immutable object-storage prefix.

Every ordinary table rollup also saves a normalized input proof under
`output/generation-inputs/<family>/<artifact-digest>/inputs.json`. The common
publisher stores the same receipt and its pinned native controls under
`generation-inputs/<family>/<artifact-digest>/` before advancing the table
pointer. Proof upload or byte-verification failure blocks that pointer update.
The proof uses the existing `spicy-publication-inputs` format consumed by source
catalog reconciliation. It records exact table members, managed parent versions,
prior generations and source observations. A parent held only by ETag and size
stays separate from cryptographically identified originals. Creating the proof
reads native controls, not the original bodies again. Build-only rollups retain
the local proof in their normal workflow output artifact.

A source-aware build can call `evidence.selected_inputs(manifest)` before
`evidence.inputs()` seals its journal. The manifest has `format` equal to
`spicy-selected-inputs`, `version: 1`, a named `scope: {id, finite}`, a boolean
`complete`, and separate `used`, `available` and `expected` arrays. Each inline
entry states `source_id`, `source_version`, `version_kind`, `asset_kind`,
`location`, `sha256` and `bytes`. Identity/version duplicates and contradictory
byte pins refuse. Every used original must match an admitted capture or retained
raw original by bytes, native identity or locator, location and version.
An unqualified native version cannot substitute for a content digest.
`complete: true` requires an explicitly finite scope and describes the
declared used set; it does not mean every expected item was available or used.
The declaration is a build responsibility, not an inference from all captures.

Large comparisons can supply `coverage_inventories`, each with `role` equal to
`used`, `available` or `expected`, `format: parquet`, `path`, `sha256`, `bytes`,
`columns: {source_id, source_version}`, `version_kind`, `asset_kind` and
`complete`. `selected_inputs(manifest, base=...)` retains and verifies these files
through the existing evidence store, outside the raw archive. The proof streams
them into its own `inventories/` directory and checks the final pins again;
the publisher uploads and verifies them before moving the table pointer. Local
runner paths are replaced by retained member paths. Referencing
an external used inventory does not qualify a complete inline used set. Native
keys without versions still cannot establish an exact version match.

The explicit `MembersRollup.rebuild_complete_rosters` route declares the current
and historical roster captures at the maintained source-use loop. Its finite
scope is those exact roster files.

Readers that discover their inputs while processing call
`evidence.declare_used_inputs(inputs, scope_id=..., complete=False)` after
successful parsing. Each input gives `location`, `sha256`, `bytes` and
`asset_kind`. Repeated calls accumulate one partial scope. A complete finite
selection uses one call with `complete=True`. At successful seal, the shared
owner matches these references to recorded originals and writes one selected
manifest. Unknown, conflicting, refused or derived references fail admission;
a failed build does not acquire a successful accumulated declaration. Direct
`selected_inputs` and accumulated declarations cannot be mixed in one build.

The maintained scorecard and FEC candidate/committee history builders declare
their successful original reads. The scheduled CFR, committee roster, Federal
Register, press release, Senate expenditure and Unified Agenda paths share
those declarations with generation evidence. Their incremental scopes remain
incomplete. Filtered rows still mean the source file was read; a fetched body
that fails source validation remains an observation. Derived builders reuse
existing parent-table pins. Unsupported builders keep source use unknown; the
hook does not reconstruct their historical inputs.

The comment-attributes seed also retains its selected Mirrulations keys, ETags
and sizes beside its native generation. That inventory describes winning copies
for this seed only. Mirrulations bodies remain external, unknown versions remain
unknown, and later incremental builds need their own input evidence. See
`comment-fields-fill.md` for the existing seed command and optional comparison.

Compare retained Parquet inventories with the maintained command:

```sh
uv run --frozen python -m spicy_regs.source_catalog.coverage_diff \
  --expected expected.json --actual available-or-used.json --output comparison/
```

Each descriptor names its `path` or explicit `files`, `format: parquet`, `role`,
`columns: {source_id}`, `asset_kind`, `complete` and identical named
`scope: {id, finite}`. Optional `source_version` and `bytes` column mappings
retain version and size evidence; mapping `source_version` also requires
`version_kind`. A pinned descriptor also
supplies `sha256` and `bytes` for its single file. The comparison writes a
disk-backed `diff.parquet` and summary with two separate results.
`identity_status` in the diff and `identity_counts` in the summary report
`present`, `missing` or `unexpected` for each `source_id` and `asset_kind`,
regardless of version tokens. `identity_parity` is `equal` or `different` only
when both inventories declare complete coverage of the same finite scope; it is
`unqualified` when either inventory is incomplete. Partial matching sets cannot
establish full coverage. Each side's `measurements` distinguishes input rows,
unique identities, `duplicate_identities` (identities with repeated rows) and
`duplicate_rows` (rows beyond the first for each identity).

The existing `status`, `reason` and summary `counts` retain the exact-version
results: `same`, `missing`, `stale`, `unknown` and `unexpected`. A key can be
`present` while its version is `unknown` or `stale`; conflicting duplicate
versions also remain `unknown`. JSON bodies and attachment bodies remain
distinct in both results. Memory and worker settings are bounded by
`--memory-limit` and `--threads`; comparison does not read original source bodies.

Without a selected manifest, captures remain observations and the used set is
unknown. Generic transitive source closure stays `complete: false` even with a
finite selected declaration: historical parents and actual interpretation still
require their own evidence. No available inventory or publisher-wide denominator
is invented. The maintained member-roster regression exercises source acquisition,
native table/receipt generation, proof publication and catalog import locally;
it does not regenerate production tables.

Offline regression tests are in `tests/test_source_evidence.py`. They check
literal input bytes against retained blobs and native output fields, failed
postconditions, late parsing and listing failures, credential exclusion,
unchanged resumes, caps, prior-root pins and publication refusal. They do not
establish live population completeness or deploy this feature.

Federal Register scheduled builds retain each exact API response before the
SpicyDocs reader parses it. Successful reads declare the used page hashes; the
incremental selection stays incomplete and inherits earlier source uncertainty.
The shared publisher retains original bodies and the acquisition-event journal
in R2 before advancing the table publication. It supplies no invented HTTP
status, response headers or acquisition time where the byte-only fetcher exposes
none. The journal preserves the requested URL, byte hash and size.

On hosted Federal Register runs, `SPICYGOV_RAW_ROOT` points inside
`output/raw-source-originals`. The existing failure/dry-run output artifact then
includes central originals and acquisition receipts alongside the run evidence.
After restoring that artifact elsewhere, set `SPICYGOV_RAW_ROOT` to its restored
`raw-source-originals` directory. Successful publications use the existing R2
evidence writer. `tests/test_federal_register_retention.py` checks both lifetimes
through the scheduled rollup, including malformed/refused input and an interrupted
store publication; local store tests do not establish a completed hosted CI run.

Other unsupported builders adopt the existing successful-read declaration when
next regenerated, as tracked in `ss-es27`. Retention alone does not prove a file
was used. No builder-wide migration or reconstruction of historical processing
is required to maintain the source inventory.
