# FEC identity and context receipts

The local identity builder writes useful domain values to subject Parquet files
and processing evidence to one `etl_receipts.parquet`. The explicit field rules
are in `src/spicy_regs/fec_identity_context_fields.json`. Unknown mapper fields
refuse admission. Source inventory, collection outcomes, API controls, representation decisions, source records and evidence associations have main logical rows. Raw source bodies and conversion diagnostics stay in receipts. Collection-level witnesses retain their context column/pointer and generation pin; they never acquire an invented individual source-record identifier.

`fec-identity-context-receipts/2` preserves each control/evidence observation with a generation-scoped `observation_ordinal`. Collections use their stated `collection_id`; source records use the complete `(collection_id, source_record_id, source_sha256)` key. Repeated evidence observations remain distinct.

Candidate status, committee activity, cancellation, notice scope and treasurer
names remain domain values. `treasurer_text` is a PostgreSQL search vector; its
exact spelling remains in source evidence. Candidate IDs, sponsor IDs and cycles
use ordered native lists. Invalid candidate-ID strings remain visible. An
unrepresentable typed element occupies a null position, and its original value
and position remain in receipt diagnostics. Empty, missing and null inputs are
separately recoverable from receipt fields. Meeting dates retain their distinction
between ranges and independently listed dates. Linked document URLs remain
domain links; capture URLs and byte coordinates remain receipt witnesses.

Use the local manifest builder with an explicitly selected output directory:

```sh
python -m spicy_regs.pipelines.rollups.fec_identity_context \
  --manifest identity-selection.json --output-dir identity-build
```

A version-1 manifest contains `generation_id`, `tables` and `inputs`. Each input
names `mode`, `table`, `path`, `sha256` and exact `rows`. `mapped` consumes the
reviewed current mapper shape. `source_records` additionally supplies
`source_generation_pin` and `jobs`; each job explicitly names `kind` and its
native `entry`, with a retained `header_row` for committee master mapping.
Supported kinds are `candidate_api`, `committee_api`, `committee_history`,
`registry` and `filings`. `context` selects exact `collection_ids` and a
`source_generation_pin`; `filing_feed: true` assembles the selected parts of one
RSS capture. It preserves each contributing context and its event positions.
The input file digest and membership are checked before and after reading.

The `rollup` subcommand calls the existing committee, candidate-history,
committee-history, source-catalog and organization-link producers through the
receipt writer. Committee increments require `--prior-bundle` and
`--prior-generation-id`; an initial build requires `--full-walk`. The internal
reader validates the prior subject content and receipt generation before making
processing fields available. An absent or ambiguous receipt never becomes a
legacy-table fallback. Local source acquisition remains the existing producer's
responsibility; the retained-manifest path performs no network reads.

`seal_identity_context` binds the local subjects and receipt member into the
existing immutable generation format with `local-partial` status. It neither
publishes nor changes the selected remote generation. Failed or unsupported
source pages retain refusal receipts without subject rows. Failed selected
native mappings remain receipt attempts. Corrupt input files, unknown fields and
ambiguous subject identities abort the output directory.

Internal consumers use `read_identity_rows` or `read_identity_processing` with
an explicit generation. The filing and quality-notice wrappers in
`fec_identity_consumers.py` pass reconstructed values through the existing
financial checks. They do not select amendments, infer donor identity, exclude
transactions automatically or turn captured previews into financial totals.

The scheduled committee, candidate-history, committee-history, source-catalog
and organization-link rollups build through this receipt writer
(`FecReceiptRollup`), and `build-fec-observations` writes its subjects and
receipts the same way. The shared dictionary and the public MCP/view catalog
describe the migrated subjects: native list columns replace the old
array-expansion views, and the old processing-control views are not registered
against them. Read processing values through explicit internal receipt reads.
A receipt-only dataset such as `fec_source_catalog` emits only receipt rows and
seals as a generation with no subject table.

## Relationship endpoints

Relationship main rows retain the literal source locator, digest, source coordinates and recorded source/array ordinals. The existing source locator determines these fields; missing coordinates produce `unavailable_locator_coordinates`.

`subject_endpoint_status` and `object_endpoint_status` express lookup eligibility, not a successful target match. Candidate/committee endpoints require the retained type, valid native ID classification and stated cycle. Name-only, reported-none, unsupported, invalid-ID and missing-cycle observations remain visible with no inferred edge. The builder never expands a list of possible cycles into historical relationships.

Replay retained mapped/source/context inputs through the same manifest builder to obtain the new main schemas. Publish the new subjects and receipts together with matching navigation metadata; preserve old generation/policy pins. This local writer change does not refresh providers or publish data.
