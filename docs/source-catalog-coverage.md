# Source coverage and controlled metadata refresh

The offline catalog separates expected, advertised, acquired, processed and
published evidence. Run `python -m spicy_regs.source_catalog coverage --index
<selected-index>` from a pinned environment. The report identifies its catalog
generation and captured publication. It does not update the index or select a
release.

Each measurement states its unit, selected scope, route, period, version and
evidence references. Advertised publisher metadata items, retained file records,
qualified candidate table versions and captured publication artifact versions
are different units. Their counts cannot establish a percentage. Expected
denominators remain unknown until a qualified observation explicitly states
them. Period assertions provide context; a measurement with a null period does
not apply to every period in that context.

The acquired stage records manifest hashes and presence at index build time;
it does not rehash decoded source bytes. The processed stage records existing
candidate qualifications and record counts where the pinned proof supplies
them; this report does not replay the processing or
revalidate the proof. The published stage requires a captured publication time,
capture digest and exact table artifact digest. It describes that snapshot,
and requires separate live readback for current serving claims.

Source owners can submit an explicit `coverage_measurement` in a discovery
observation for the `expected` stage. It must include `stage`, a nonnegative integer `count`, `unit`,
`scope`, `basis` and `version`; use `route_id` and `period` for qualified
bindings. Other stages use their own indexed authorities and reject these
denominator assertions. Keep the supporting
observation and original evidence. An assertion states its evidence basis;
the catalog does not independently validate publisher completeness.

The U.S. Code acquisition declaration links to the implemented current
release-point and historical annual edition-structure destinations. Their
source qualification remains limited to selected native structure. Consolidated
Code, enacted-law sections and Table III classification remain distinct uses.
Section-body processing and publication need their own qualified evidence.

## Refresh procedure

First inspect retained catalog manifests and captures. Reuse the required
version and decoded hash when available. For a new observation, use the
maintained SpicyDocs metadata replay/refresh command documented in
`spicy-docs/docs/sources/source-catalog.md`. Choose new capture/output paths,
retain receipts and errors, and preserve the old observation. B reviews source
bindings; C owns traversal and acquisition support; A owns release selection.

Compare two selected discovery documents:

```sh
python -m spicy_regs.source_catalog compare-inventory \
  --previous <previous-discovery.json> --current <current-discovery.json> \
  --output <new-change-report.json> --max-age-days 7 --owner <responsible-owner>
```

The output refuses replacement. Comparison requires the same selected URLs,
queries and metadata kinds. It identifies new items, changed item metadata,
unconfirmed absence, retained parse errors and stale observations. A changed
capture `version_id` alone is ignored: it fingerprints metadata capture bytes,
not downloaded dataset content. Different scopes require a separately
qualified identity mapping before comparison. Partial captures may contain
new facts, but absent items remain unconfirmed.

Confirmed removals require **both** snapshot-bound completion proofs via
`--previous-completion` and `--current-completion`. A proof uses format
`spicy-source-traversal-completion`, version `1`, status `complete`, the selected
file's `snapshot_sha256`, its `scope_key` returned by `refresh.scope_key`, every
`observation_id` in `observation_ids`, `inventory_stable: true`, and a
`qualification_input` with `path` and `sha256`. Relative qualification paths
resolve beside the proof. The comparator checks those bytes and bindings and
rejects removal claims if either snapshot has errors. The source owner must
qualify full page coverage and inventory stability in that retained proof;
the comparator cannot establish them from a terminal page. C supplies the
source-specific traversal qualification. Removal means absent from that
qualified inventory scope, not deleted source bytes or retired result tables.

## Cadence and failure ownership

Use an operator-reviewed policy per source route: refresh mutable catalog/API
inventories weekly and stable annual/archive listings monthly, or follow a
publisher's documented release calendar. These are proposed operational
cadences; no job is scheduled by this change. The operator supplies the exact
`--max-age-days` and `--owner`; the report retains both. B owns catalog triage,
C owns traversal/retry failures, and the relevant family owner qualifies
identity or scope changes. A owns deployment and publication failures.

A failed refresh retains its earlier successful observation and new refusal
evidence. Do not turn a redirect, maintenance response, parse failure or
interrupted pagination into an empty inventory. Escalate to the named owner;
resume with C's support when continuation state is available. The public reader
and source-details view display scoped measurements. Public release and
deployed readback remain A's dependency. Internal
coverage reports contain evidence references and must not be published as
public exports without the existing field allowlist and privacy checks.

## Retained publication observations

Select a retained response explicitly with `build --publication-observation
<selection.json>`. This additive input uses format
`spicy-retained-publication-selection`, version `1`: a credential-free
`publication_url` and exact `path`, `sha256` and
`bytes` pins for `publication`, `headers`, `capture`, `row_bindings`, and optional
`evidence_inputs`. The importer checks the successful digest-matched capture,
HTTP Date, complete descriptor bindings and selected historical inventory.
Every selected input contributes to the new catalog generation.

The resulting collection detail can include `publicationObservations`. Each
observation records the response digest, family artifact digest and prefix,
individual table or partition member identities, and three separate times:
client capture time, family `publishedAt`, and server HTTP Date. Partitioned
tables retain member keys, partition values and digests; they have no invented
aggregate member digest. The reader validates this optional field and the
existing detail request loads it. Older details remain readable.

Historical inventory tables and coverage measurements remain intact. An exact
family and table association locates the retained observation; it does not
verify member content, establish current live status, qualify source coverage
or choose an active release. Unqualified associations receive a separate
`publication:<family>` collection. In particular, serving `comments` and
inventory `comments-mirror` remain separate until their owner qualifies an
association. Court's accepted processing delta remains independent evidence;
a retained Court publication does not establish the full historical merge or
qualify that delta for publication. The coordinator owns any future release
selection and canonical inventory changes.
