# GovTrack discovery independent review

Reviewed the working-tree provider/consumer and their tests on 2026-10-03 using
the semi-formal code review workflow. This is static inspection; the review did
not acquire sources, run these tests, or edit the reviewed implementation.

## Findings and readback

The initial findings below are **resolved** in the subsequent working-tree
readback. Consumer `_failure_metadata` (`:246–293`) retains allowlisted
URL/status/hash/context without raw bodies or exception text. `acquire`
(`:334–338`) records that metadata on a failed manifest. Catalog reconciliation
now groups by publisher while enforcing unique `source_id` (`:49–71`) and
retains each source's status (`:182–190`). New tests directly replay private
captures and reject corrupt manifests/bodies (`test_govtrack_discovery.py:222–325`),
exercise multiple source families (`:328–339`), and verify bounded failure
evidence (`:342–397`). No outstanding blocking finding remains. Shared access
refusal evidence lacks an exact 401/403 status; the consumer correctly leaves
that field absent instead of inventing it.

Initial inspection locations below identify the code before these corrections:

1. **WARNING — failure evidence is dropped from the retained attempt.**
   Consumer `govtrack_discovery.py:271–275` persists only `error_type` on
   acquisition failure. The provider invokes its retention callback only after
   successful `capture_validated` (`spicy-docs/.../govtrack_discovery.py:272–287`).
   However, `SourceAcquirer._attach` already attaches failure capture/context
   (`source_acquirer.py:221–228,278–280`). A refused metadata request therefore
   leaves no durable requested locator/status/hash in the consumer manifest.
   Preserve available bounded refusal metadata separately from accepted capture
   membership. A refused attempt must never make the manifest complete.

2. **WARNING — reconciliation assumes a publisher-grained catalog.**
   Consumer `govtrack_discovery.py:49–55` rejects a repeated `publisher_id`.
   Canonical catalog validation keys entries by `source_id`
   (`scripts/build_scorecard_survey.py:48`), allowing separate source families
   from one publisher. The current catalog has no repeated publisher IDs, so
   this does not block the measured import. Group source rows by publisher and
   retain their separate source/status references, or explicitly document the
   narrower accepted catalog shape before additional families are admitted.

3. **WARNING — consumer retention/replay checks lack direct test coverage.**
   `tests/test_govtrack_discovery.py:158–215` mocks `load_retained`; the inspected
   test module never exercises consumer `acquire` or `load_retained` directly.
   Provider tests exercise source parsing/acquisition separately. Add a retained
   round trip plus changed body/hash, unsafe path, incomplete manifest and
   failed-request evidence cases at the consumer boundary.

## Patch summary

SpicyDocs reads metadata before the explicit YAML/CSV boundary in a complete,
commit-selected repository tree. SpicyRegs privately retains those captures
and emits a separate discovery review against the existing census. The CLI's
`main` calls `acquire` or `load_retained` → `reconcile` → `_write_json`.
No rating table, publisher adapter, canonical catalog or support status is
written by this path.

## Function trace

Paths below are relative to the named repository; `provider` means
`src/spicy_docs/sources/scorecards/govtrack_discovery.py`, and `consumer` means
`src/spicy_regs/scorecards/govtrack_discovery.py`.

| Function | Location | Input → output | Verified behavior |
| --- | --- | --- | --- |
| `commit_url`, `tree_url`, `metadata_url` | provider:34–49 | SHA/path → locator | Full lowercase SHA; direct YAML paths only. |
| `_checked`, `_instant` | provider:52–79 | capture → bounded body | Exact GET locator, HTTP/media/encoding/time and size checks. |
| `_object`, `parse_commit` | provider:82–102 | commit JSON → tree SHA | Duplicate JSON keys refuse; requested commit and root tree are checked. |
| `parse_tree` | provider:105–141 | tree capture → file entries | No truncation; exact tree SHA; unique paths and bounded supported blobs. |
| `_url` | provider:144–162 | literal → original-link lead | Credential-free HTTP(S), outside named discovery hosts; does not claim verification. |
| `parse_metadata` | provider:189–237 | blob → `DiscoveryLead` | Checks complete Git blob SHA/size, parses only YAML header, retains literal metadata. |
| `parse_retained` | provider:240–248 | capture set → leads | Requires exact commit/tree/file membership. |
| `GovTrackDiscoveryAcquirer.acquire` | provider:267–295 | selected commit → leads | Bounded shared request budget; complete read or refusal. |
| `load_bounded_yaml` | `spicy-docs/src/spicy_docs/reading/yaml_input.py:15–81` | bytes → object | Literal dates; refuses aliases, duplicate/merged keys and depth/node excess. |
| `capture_validated`, `_attach` | `spicy-docs/src/spicy_docs/transport/source_acquirer.py:221–281` | bounded HTTP → parsed response/error | Carries failed capture/context for bounded consumer retention. |
| `_instant`, `_name`, `reconcile` | consumer:27–231 | leads/catalog/aliases → review | Exact normalized-name/literal-URL candidates plus explicit aliases; source-family identities survive and conflicts remain ambiguous. |
| `_write_json` | consumer:234–243 | review → file | Same-directory temporary write and replace. |
| `_failure_metadata` | consumer:246–293 | exception → bounded facts | Retains allowlisted source URL/status/hash/context, without exception text or raw body. |
| `acquire` | consumer:296–340 | private empty directory → manifest | Retains successful bodies/metadata; failed attempts are distinct from accepted capture membership. |
| `load_retained` | consumer:343–400 | manifest/body files → leads/pin | Bounds reads, checks digest and safe body paths, then invokes provider replay. |
| `main` | consumer:403–438 | CLI arguments → review | Protects catalog/alias inputs and retained directory; hashes inputs and writes only after success. |

## Data flow and invariants

- Commit JSON identifies the root tree; complete tree entries identify each
  file by path, size and Git blob SHA. Metadata bytes must match that entry
  before their header can escape as a discovery lead (provider:91–248).
- Raw mixed files remain in caller-selected retained storage. The public
  lead contains metadata and hashes, not the CSV rating body
  (provider:196–236; consumer:312–328).
- `updated_text` stays literal and carries explicit publication-or-import
  semantics. It does not populate publisher publication or verification dates
  (provider:233; consumer:164–166,202–203).
- Explicit aliases add a candidate; they cannot erase a conflicting exact
  name/URL match. Every file remains a separate discovery observation
  (consumer:142–180,210–230).
- The production catalog is read, never mutated. Review output is emitted
  after complete replay and reconciliation; refusal preserves prior output
  (consumer:420–433).

## Test behavior and concrete edges

Provider tests cover literal timestamps/header boundaries and ignored rating
tails (`test_scorecards_govtrack_discovery.py:74–89`), malformed headers and
URLs (`:92–125`), stable discovery IDs (`:128–134`), tree/hash/membership and
path refusals (`:137–162`), bounded acquisition (`:165–178`), and HTTP refusal
before another file is read (`:181–206`). These assertions align with the
traced implementation. Whole-blob verification remains required even though
the CSV tail is not interpreted (`:209–212`).

Consumer tests cover unchanged original status, explicit renaming, conflicting
aliases, duplicate source observations, stable proposed IDs, stale alias
refusal, and metadata shape refusal (`test_govtrack_discovery.py:59–155`).
CLI tests prove failed replay does not overwrite a prior review and successful
reconciliation hashes its catalog input, using a mocked replay (`:158–215`).
The readback adds direct coverage for repeated publisher IDs across valid
source families, retained replay and corrupt captures, and persisted refused
HTTP attempts (`:222–397`). The assertions follow the corrected code paths.

## Conclusion

**VERDICT: APPROVE** after correction readback. The commit/tree/blob chain,
discovery-only authority, alias-conflict handling, source-family grain,
literal-date behavior and durable failure evidence satisfy their stated
intent. Coverage of the reviewed paths is **adequate**; confidence in the
static review is **high**. This review does not establish publisher rights,
production acquisition reliability or a published release.
