# Scorecard evidence and refresh guarantees

Status: implementable design for `ss-vbf9`; no runtime change, publication,
deployment, or source enablement. The inspected code is pinned in
[code receipt](code_receipt.json). Source decisions are in
[evidence policy](source_policies.json); required behavior is in the
[test matrix](test_matrix.json).

Extend the existing `spicy-regs-source-evidence` artifact with versioned
receipt rules. Keep the existing generation publisher. Add publisher-bound
policy selection, complete-edition replacement, and policy-aware auditing.
An admitted hash receipt proves the integrity of the receipt, not the source
body or the parser's interpretation.

## What the code already supplies

| Observed behavior | Existing implementation | Consequence |
| --- | --- | --- |
| Captures retain exact payload bytes before content decoding. | SpicyDocs `transport/captured.py:25`, `CapturedBodyResponse` | Hash the captured payload, recording content encoding. Do not hash extracted text and call it the response hash. |
| Capture and file paths always write blobs; refusal handling has its own byte-retention path. | `source_evidence.py:151` `capture`, `:183` `_retain_file`, `:215` `retain_file`, `:322` `refusal` | Policy must apply at every admission path, including failures and request bodies. |
| Streamed temporary files sit under the evidence run directory. | `source_evidence.py:267`, `RetainedStream.__iter__` | Move publisher spools outside `output/` before introducing hash-only capture. Normal cleanup is insufficient for a killed process. |
| The workflow uploads evidence after every run, and all `output/` after failure or a build-only run. | `.github/workflows/_rollup.yml`, final artifact-upload steps | Prevent source bytes from entering these trees; filtering the R2 upload cannot prevent workflow leakage. |
| Evidence admission checks artifact integrity, kind, and build outcome. It does not validate journal-to-blob semantics. | `source_evidence.py:494`, `verify_evidence` | Add receipt-version validation here. Keep generic Rulespec admission for manifest/digest checks. |
| The auditor expects every capture hash to identify a blob member. | `generation_audit.py:887`, `_body_shapes` | Digest-only captures need an explicit unavailable-by-policy result, not a missing-blob failure or a passed body check. |
| A generation binds one evidence artifact and its prior family; table parents bind exact input files. | `generations.py:93` `verify_generation_source`, `:164` `_check_parents` | Keep one evidence artifact even when a run uses different publishers' policies. Use `parents` for analysis inputs; do not invent extra root-input roles. |
| Publication verifies the whole family, evidence pins, remote bytes and a conditional pointer write. | `sources/publication.py:894`, `_publish_verified_generation` | Reuse this path. Another scorecard writer makes the build stale; another family's update can be merged safely. |
| `merge_table` already replaces complete parent scopes. | `transforms/table_merge.py:397`, `replace_parents` | Use it only after all tables in an accepted edition pass validation. Validate keys first because this helper deduplicates rows. |

Line numbers describe the inspected files, not a promise that future edits keep
them stable. `code_receipt.json` holds their hashes. The SpicyDocs instructions
require caller-owned recovery and explicit evidence limits for raw readers;
hash-only scorecards are not source-native replayable releases.

## Evidence format and policy

Keep `kind=spicy-regs-source-evidence`, the `source-evidence` input role,
existing storage prefixes and the existing Rulespec library. Add
`spec.evidence_version=2` for new policy-aware artifacts. A missing version
means legacy full evidence, read under the existing rules. Reject unknown
versions. New readers must understand both before new writers are enabled.

Keep `CaptureEvidence(output_dir, family)` equivalent to `full` for existing
federal callers. A scorecard run explicitly defaults to `hash_only`. Bind an
immutable source context such as
`evidence.for_source(publisher_id, policy, parser_version, policy_decision_id)`;
the child delegates to the same journal and artifact. Do not switch a mutable
run-global policy while concurrent publisher tasks are capturing responses.
Reject missing or contradictory publisher policies before acquisition.

Each capture receipt records `capture_id`, publisher and parser identity,
policy decision, requested/resolved URLs, method, HTTP status, content type,
content encoding, source `observed_at`, response-completion status, and
`body_retained`. Preserve journal `recorded_at` separately. The receipt's
format version distinguishes source digest fields from stored-object links.

| Mode | Public receipt | Source body and request body | Claim allowed |
| --- | --- | --- | --- |
| `full` | Metadata, measured size, SHA-256, explicit blob member | Retained only under an established source-specific redistribution decision | The retained bytes match the receipt; replay remains adapter-specific. |
| `hash_only` | Metadata, measured size and SHA-256; no blob locator | Used transiently, never admitted or uploaded | These bytes were observed and identified; offline body validation and replay are unavailable. |
| `metadata_only` | Metadata and measured size when a complete response was consumed; no source SHA-256 or blob locator | Used transiently, never admitted or uploaded | The operation and metadata were observed; body identity is unavailable. |

Use an opaque capture identifier for `metadata_only`; never derive it from the
raw response digest. Snapshot identifiers may bind policy-permitted source
metadata, emitted records, observation context and parser identity. They must
not smuggle the forbidden source digest into `capture_id`, `snapshot_id`, a URL
or an error field. The dataset-generation digest over published tables and
receipts remains valid and is a different identity. The schema must allow absent
source-body digests in this mode rather than requiring an invented replacement.

In both non-retaining modes, apply the same policy to request bodies, redirects,
refused responses, PDF renders, extracted full text, and parser diagnostics.
For incomplete streams record received-byte count and a refusal reason; do not
write a complete-body hash or let HTTP EOF alone establish scorecard completeness.
Never substitute HTTP `Content-Length` for measured body size. A source `304`
is not a body: V1 should issue unconditional GETs rather than claim a newly
parsed snapshot from it.

Keep producer-owned lineage roots and receipts as public metadata even for
hash-only sources. `inherit()` currently puts a prior artifact root in the blob
store; that is our JSON, not publisher bytes. Version-2 manifests must identify
this distinction (`lineage-metadata` versus `source-body`) and the verifier must
reject undeclared, orphaned, or source-body members in a non-retaining scope.
A hash-only receipt must not point to a pre-existing shared blob merely because
another artifact happens to contain the same bytes.

`verify_evidence` must validate policies, required/forbidden receipt fields,
capture/member references, and unknown versions after generic admission.
`_publish_evidence` can continue uploading only admitted members; use the same
semantic verifier for remote readback. The audit should report policy-separated
capture totals and explicit `not_assessed_by_policy` body checks. Preserve
credential scans over public metadata and published table cells in every mode.

Preserve existing credential behavior: `401`/`403` and credential echoes abort
the run, with no body retained. Other publisher-specific failures can preserve
their edition and allow independent successful editions to publish. Global
journal, storage, schema, or integrity failures abort the whole run. A non-full
refusal should journal an allowlisted reason code, error class and locator;
arbitrary `str(error)` can contain source excerpts even after credential
scrubbing. Do not expose raw parser exception text in public artifacts.

Use private temporary directories outside `output/`, preferably the runner's
temporary directory, for response spools and extraction assets. Close/delete on
success, failure and cancellation; keep the hard-kill case safe through placement,
not cleanup promises. No private raw-file path should enter a public receipt.
Synthetic fixture payloads should contain recognizable sentinels so tests can
scan the complete candidate/workflow upload trees for leaked bytes.

## Source and fixture decisions

[source_policies.json](source_policies.json) records a decision for every source
in the input census. All remain `rights_unreviewed` and public `hash_only`.
That is a retention choice, not a finding that acquisition or data publication
has been legally approved. No source has an established grant in this packet.
Full-byte rights must identify the exact license/permission, covered materials,
restrictions, evidence URL/capture, reviewer and decision date; an available
download or federal subject matter does not establish redistribution rights.

Commit synthetic publisher-shaped fixtures, bounded examples and hash receipts.
Keep complete research captures, rendered pages and full extracted row sets in
the external campaign directory chosen by the samples worker, with an explicit
private retention decision; they are not repository fixtures or workflow
artifacts. A qualification receipt can cite a hash without promising public
replay. If source terms prohibit needed use or qualification cannot inspect the
complete scope, mark the source blocked instead of silently changing policy.

## Complete-scope replacement

V1 replacement scope should be the whole publisher-defined edition
(`scorecard_id`). A House-only capture may replace only a separately qualified
House edition identity; it must not erase or silently inherit the Senate part
of an edition defined as both chambers. If independent chamber updates are
required, the schema gate must make that scope first-class and put it on every
dependent table. Do not infer scope from the rows that happened to parse.

1. Capture one publication index and verify the managed prior's complete family.
   Download prior tables through that snapshot. A managed download failure is
   fatal; never reinterpret it as a cold start or use old local caches.
2. Select registry publishers/editions and journal the selection. Discovery
   listings add candidates; their omissions never delete historical editions.
3. Acquire one edition into isolated private staging. The adapter supplies the
   scope identity, constituent captures, parser version and completeness witness
   (export EOF plus expected structure, completed pages and declared counts,
   or the validated official document and complete member grid).
4. Validate the complete row bundle: expected edition/chambers, non-null unique
   keys, cross-table references, literal value preservation and declared versus
   parsed counts. Validate before `merge_table`, which could otherwise conceal
   duplicate keys. A requested empty edition needs an explicit publisher-backed
   empty success; an empty response or parser output never supplies that proof.
5. Build an accepted-edition set only from fully validated bundles. Stage no rows
   from failed editions. For every edition-scoped source table call
   `merge_table(..., replace_parents=("scorecard_id", accepted_ids),
   coalesce_prior=False)`. A successful empty child table clears that edition's
   old children. It does not remove its edition metadata or other editions.
6. Publisher metadata uses its own validated publisher scope. Preserve publishers
   referenced by any retained edition; do not delete them because current
   discovery omitted them. Journal accepted, failed, deferred and unchanged
   scopes, plus exact retired identities using the existing `rows-retired`
   event shape. Keep actual `observed_at` and accepted snapshot IDs on old rows.
7. Validate the merged bundle again; write every family table, including valid
   empty tables. Publish once as `name="scorecards"`, with `outputs` containing
   the entire frozen source-table set. Do not set `publication_family` for the
   primary writer: that flag is the existing partial-table writer mechanism.

For mixed success, replace only accepted scopes and preserve failed/prior scopes
exactly. Record a build-complete candidate with scope failures explicitly; build
completion means the replacement rules passed, not that every request succeeded.
If all selected acquisitions fail, fail the run without building a new table
generation. If nothing is due, record a no-op without changing the pointer.
The current base class expects paths from every `build()`; implement an explicit
no-op result/guard rather than returning an empty tuple or a copied fake success.

Do not allow total failed cold starts to publish an empty corpus. A mixed cold
start may publish qualified editions while reporting requested failures and
coverage; it is not publisher-universe completeness. Keep prior generations and
their evidence pins in lineage. Scope removal still faces the existing byte
shrink guard, so a large legitimate correction requires a reviewed explicit
override; source completeness must never enable that override automatically.

## Concurrency and analysis inputs

Use one writer lock group for manual refresh, schedule and backfill of
`scorecards`; queue rather than cancel active runs. This reduces collisions but
does not replace the conditional pointer write. Current publication refuses a
stale scorecards family, and retries an unrelated-family index race while
preserving the other update. A transport error after pointer submission has an
unknown result: reread the exact public family pin before retrying.

Resolution runs capture one index and use its immutable scorecards and existing
congressional table members through declared `inputs`. Store the table digests,
family pins and resolver version in their own `scorecard-analysis` generation.
Source rows never wait for successful identity resolution. A newer congressional
or source generation may appear while analysis runs: the output remains valid
for its stated input pins, but must be reported as stale relative to current
data. Existing publication does not enforce latest upstream inputs. If the
product requires latest-only analysis, add an explicit pin check immediately
before publishing; do not imply the current family guard already does this.

Do not use unpinned bare URLs or `remote_inputs`' mutable ETag path for these
tables. The base class permits derived rollups to read ingest outputs but
forbids derived-to-derived chains. Keep initial member/item resolution in one
analysis rollup over original source and congressional inputs. Later comparisons
can run in the same generation builder or need a deliberate policy change.

## Focused implementation and release order

1. Add policy-aware evidence versioning in `source_evidence.py`; cover captures,
   files, transport, refusals, inheritance, admission and run outcomes. Add the
   source-context constructor in `RollupPipeline` with a backwards-compatible
   full default. Update `generation_audit._body_shapes` and remote evidence
   verification in `sources/publication.py`. Existing federal full evidence and
   both existing storage layouts must remain readable. Existing federal callers
   keep full retention unless they explicitly select another policy.
2. Add scorecard registry validation and the new rollup/transform. Reuse
   `table_merge.replace_parents` and the current generation publisher. Add
   `test_scorecard_refresh.py` plus focused extensions to existing evidence,
   publication and hosted-wiring tests. The [test matrix](test_matrix.json)
   names observable assertions rather than implementation-mirroring tests.
3. Register frozen SpicyDocs schemas through the installed wheel, then update
   `data_dictionary.CONTRACT_TABLES`, descriptions, generated metadata/table
   pages, joins and MCP table exposure together. `data_dictionary._contracts`
   deliberately imports the installed wheel; source-tree-only tests cannot
   qualify the receiving application.
4. Build the wheel from a clean archive of a recorded provider commit plus an
   explicit scorecard-file/hunk allowlist. The currently vendored provider
   baseline is recorded in `vendor/README.md`; verify whether using a newer
   committed baseline is intended before adopting its unrelated changes. Do not
   build the dirty provider checkout wholesale. Use a task-specific local
   version derived from the reviewed tree/patch digest, record archive and patch
   hashes, build twice, and compare wheel bytes and package-file differences.
   Preserve the source checkout's existing version edits. If a required change
   overlaps unrelated edits, extract only the reviewed hunks into the archive.
5. Run provider tests from that archive, then consumer tests in a separate
   temporary environment using the wheel (not a global editable install or a
   shared `.venv` mutation while workers run). Record the imported package path
   and version. Only after review update both `source-readers` lists,
   `tool.uv.sources`, vendor wheel/README and `uv.lock` as one adoption change.
   No registry release is implied by a local candidate wheel.
6. Add a manual-only `rollup-scorecards.yml` using `_rollup.yml`, defaulting to
   build-only until qualified. Add explicit publisher/edition/backfill selectors
   to workflow inputs and invocation receipts. Registry cadence decides due
   sources when a later single scheduler invokes it. Do not create independent
   publisher workflows that race on the same family. Keep publication secrets
   out of adapter tests and avoid handing unrelated source credentials to the
   scorecards job.
7. Qualify object-store publication in a disposable target, then perform the
   authorized first production publish and anonymous readback before enabling
   schedules. Check current target configuration rather than assuming historical
   upstream deployment docs describe this fork: `deploy/fork-setup.md` specifies
   a separate bucket/base URL. Verify the actual MCP deployment reads that URL,
   serves the new table dictionary and resolves the published generation. Build
   and deploy are distinct from data publication.

Task H prerequisites: qualified adapters and scopes; successful installed-wheel
tests; policy-safe success/failure artifacts; current deployment target and
publication pin; explicit publish/deploy/schedule authorization; recoverable
prior generation; manual build and anonymous readback/audit receipts. Track
`last_attempt_at`, `last_successful_scope_at`, refusal reason and published
snapshot separately. Do not report a failed refresh as fresh because a previous
edition remains available. No external state was inspected or changed here.
