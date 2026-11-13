# Trace selected federal award spending

The USAspending pipeline turns one retained full agency/year/award-family archive
into queryable transactions, awards and original CSV-member metadata. SpicyDocs
owns literal ZIP/CSV reading; SpicyRegs checks the population and exact money,
then builds the ordinary immutable generation. No Django runtime is required.

The first qualified local population is Denali Commission (download database ID
`120`, Treasury agency code `513`), FY2025, assistance, with no geographic filter.
All observed assistance types in that archive are `04` project grants, with
native non-aggregate record type `2`. Aggregate or unknown assistance record
types require separate interpretation and refuse this financial path. The
publisher inventory names `FY2025_513_Assistance_Full_20261106.zip`, updated
`2026-11-06`; its native CSV member has `20261107` in its name. Both literal
identities survive. A filename's date does not override inventory or action dates.

The original header mapping is adopted from the publisher's CC0-1.0
`usaspending-api` revision `0a6d86a1e7e4105f349ec5db193ed4c7fca00466`.
See SpicyDocs `docs/sources/usaspending-monthly-files.md` for attribution, exact
mapping identity, bounds and literal-reader behavior. The publisher is a service,
not an adopted application dependency; the runtime uses maintained `csv`, ZIP,
HTTP, Arrow and generation tools.

## Inputs and replay

`capture(evidence, agency_id=..., agency_code=..., fiscal_year=...,
award_family=..., file_name=...)` resolves one explicitly reviewed full filename
through `UsaspendingMonthlyFilesReader`, then captures that exact ZIP through
`CaptureEvidence` into `CentralRawArchiveStore`. Query the selected source
catalog before calling it. Reuse a matching retained selection instead of
requesting the same version again. Download database IDs come from the
publisher's POST `bulk_download/list_agencies/` with `type=award_agencies`;
`references/toptier_agencies/` exposes a different agency ID.

The saved selection pins the inventory's original response and original request
bytes, metadata and request scope, plus the archive hash/size and native item.
Offline replay renews their identities and proves that the ZIP is the exact
advertised fiscal-year full item. It never substitutes reconstructed request
bytes for capture evidence. Raw originals live only in the central raw archive;
controls and derived tables live outside it.

```sh
python -m spicy_regs.pipelines.usaspending_awards \
  --selection /path/to/selection.json --output /new/candidate
```

A fresh candidate replaces one complete selected cohort. It never appends another
full snapshot. All-year deltas, undocumented inventory pagination, duplicate
transaction keys, deleted transactions, mismatched agency/year/family, malformed
money and conflicting repeated award totals refuse. A new full version uses a
new pinned selection and candidate; prior versions remain separate. The bounded
unchanged replay requires no HTTP. Revisions to the same transaction in different
snapshots remain different source facts, never extra spending in one cohort.

## Tables and ordinary queries

`usaspending_transactions` holds one original archive and transaction key.
`federal_action_obligation` is exact signed USD; negative adjustments stay
negative. `raw_cells_json` retains every native cell, including blanks, source
codes, descriptions, loan fields and funding lists. Empty source strings remain
empty; typed unknown monetary values are null and never replaced with zero.
Recipient and place-of-performance locations remain separate.

`usaspending_awards` holds one original archive and award key. It sums only
selected-year transaction obligations. The publisher's overall-award obligations
and outlays are lifetime values, checked for agreement across contributing rows
and stored once per award. They are not transaction or selected-year outlays.
Blank outlays prevent claiming a complete known outlay total. The transaction
table retains their repeated observations for traceability; never add them there.

`usaspending_archive_members` preserves member hashes, native ordered headers,
header drift and observed data-record counts. Transactions name their original
member, data-record number and physical line span.

The MCP server supports these tables through managed generations or an explicit
local selection. Use `SPICY_REGS_DATA_DIR=/path/to/candidate/generation` for the
qualified local candidate. This explicit directory is reported as local unmanaged
data by the consumer; independent generation verification establishes the artifact
pin separately. Table metadata does not establish public publication.

```sql
-- Compare recipients using the selected snapshot's transaction obligations.
SELECT recipient_uei, recipient_name, count(*) AS transactions,
       sum(federal_action_obligation) AS selected_fy_obligations
FROM usaspending_transactions
GROUP BY recipient_uei, recipient_name
ORDER BY selected_fy_obligations DESC;

-- Trace a signed adjustment to its exact retained original record.
SELECT transaction_key, award_key, federal_action_obligation,
       source_member, source_member_sha256, record_number, line_start, line_end,
       archive_sha256
FROM usaspending_transactions
WHERE federal_action_obligation < 0
ORDER BY federal_action_obligation
LIMIT 10;
```

For transaction-to-award joins, match **both** `archive_sha256` and `award_key`.
The award side must be unique. Sum transaction obligations after that one-to-many
join only; obtain lifetime totals from the award table before attaching detail.
A recipient/SAM join needs exact UEI, current scope and key-uniqueness checks;
there is no automatic name match or allocation. Funding, broker submissions,
subawards, other agencies/years and contracts are outside the first qualified
cohort. Assistance loan cells survive literally, but no loan/subsidy aggregate is
qualified by the project-grant cohort.

## Evidence and delivery limits

The retained source aggregate uses the publisher's `spending_over_time` endpoint
with `spending_level=transactions`, awarding Denali, all assistance codes and
`2024-10-01` through `2025-09-30`. Its exact total reconciled to the full archive.
Aggregate calls and monthly archives have different observation/version times;
a future mismatch is a revision or scope investigation, never permission to
force totals to agree. No outlay comparison is inferred from an obligation match.

The lane's `HANDOFF.md` links original hashes, viewed raw CSV images, final source
pins, ordinary MCP queries, focused tests and bounded resource measurements.
The coordinator owns independent review, shared package adoption, catalog/public
selection and deployment. `complete-family` means that the generation holds all
declared tables for its exact selected full archive. The source evidence's
`usaspending-cohort` event names the awarding agency, fiscal year, award family,
absence of geographic filtering, original archive hash/version and output row
counts. The maintained `selected-inputs` declaration binds every used original
to that same named finite scope. These declarations do not claim all USAspending
agencies, years or record grains, or a complete outlay total. Public availability
still requires separate coordinator-owned integration and selection.

## Rehearse the maintained delivery entry point

`publish-local-generation` registers the exact `usaspending-awards` family.
For a bounded private rehearsal, use the retained generation and the exact
source-evidence artifact it pins, with a private prior index made through
`spicy_regs.sources.publication.empty_index()`. Pass that fixture explicitly;
using the public index is a separate operation with a different resource scope.

```sh
uv run --frozen --no-sync publish-local-generation \
  --family usaspending-awards \
  --generation /path/to/retained/candidate/generation \
  --evidence /path/to/retained/candidate/source-evidence/RUN/artifact \
  --live-index /private/rehearsal/empty-prior.json \
  --dry-run --output /private/rehearsal/new-receipt
```

This executes the maintained publication guards against a private disk store.
It does not write shared publication state, refresh coverage or change comments
holds. For genuine central-raw evidence, keep the shared-archive check enabled;
`--scratch-raw` is for separately declared scratch builds.

The ordinary build admits an inventory-bound fiscal-year full archive, validates
all of its native records and requires the transaction and member tables to
retain every parsed record/member. Only then does it seal the complete finite
selected-input set and build `complete-family`. Missing/refused inputs, deltas,
out-of-scope rows or a converter that drops a native record cannot become a
complete generation. Null lifetime outlays remain unknown values; they are not
missing source records and never become known zeroes or FY outlay coverage.

The earlier candidate's unconditional `local-partial` status confused bounded
publisher coverage with incomplete family construction. Rebuilding from the
same pinned originals creates a new artifact with the exact finite cohort in
its evidence. It does not edit or relabel the earlier artifact or publisher
bytes. The generic publisher continues to refuse every `local-partial`
candidate, including the retained earlier candidate. Family identity, source
evidence pins, table membership and shrink guards also remain unchanged.

An accepted private CLI rehearsal establishes maintained delivery eligibility
for that exact artifact and declared cohort. It does not establish public
availability or permission to replace a different agency/year/family cohort.
Before any future shared selection, the coordinator must compare the live
family's declared scope and apply the maintained prior-generation safeguards.
