# FEC identity and context receipts

The local identity builder writes useful domain values to subject Parquet files
and processing evidence to one `etl_receipts.parquet`. The explicit field rules
are in `src/spicy_regs/fec_identity_context_fields.json`. Unknown mapper fields
refuse admission. The source inventory, collection outcomes, API controls,
representation decisions, source records and evidence associations are receipt
inputs; they do not create subject tables.

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

The new local entrypoints are implemented and tested. Switching the older
scheduled rollup entrypoints to receipt-aware generation admission, regenerating
the shared dictionary, and changing the public MCP/view catalog remain an
integration step. The old array-expansion and processing-control views must not
be exposed against the migrated subjects. Use native list columns and explicit
internal receipt reads. A receipt-only local bundle can be written and checked;
the shared generation builder currently requires a subject table to seal it.
