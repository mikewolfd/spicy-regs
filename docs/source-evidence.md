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

Offline regression tests are in `tests/test_source_evidence.py`. They check
literal input bytes against retained blobs and native output fields, failed
postconditions, late parsing and listing failures, credential exclusion,
unchanged resumes, caps, prior-root pins and publication refusal. They do not
establish live population completeness or deploy this feature.
