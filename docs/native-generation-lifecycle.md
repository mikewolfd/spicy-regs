# Native generation selection and receipt history

Scheduled Congress, legislative and FEC identity builds use
`selected_generations.SelectedInputs`. A captured remote index selects the exact
subject and receipt members used throughout a publishable build. Explicit local
mode (`public_url=""`) selects immutable members pinned by the local
`.native-state/selection.json` file. A missing receipt or changed local member
refuses the read; the scheduler does not convert old published tables.

`remember_selection` advances a complete batch of local dataset selections with
one file replacement. The common generation runner selects all receipt-backed
datasets from the admitted immutable artifact, including families that supply
receipts through their own generation hook. Direct builder calls select only
after their complete native candidate passes validation. Each build uses a new
private directory; refused candidates retain their evidence without replacing
visible files. Public convenience copies are outputs, not incremental inputs. Successful empty partitioned tables
remain empty directories with receipt-only file state; they do not gain invented
partition keys.

`ReceiptLineage` performs a disk-backed lookup of accepted prior subjects and
unchanged processing observations. `inherit_receipt` retains ordered witnesses,
prior receipt identities and digest-keyed exact source/processing values inside
the new receipt. These payloads are flat and deduplicated; repeated carries do
not nest prior receipt bodies. `resolve_receipt_witness` verifies and resolves
`receipt-processing:` references and
`receipt.values.<field>` references through the retained history. Explicit
removals use `retire_receipt` to preserve evidence without an orphan accepted
subject join. Original source files and capture evidence remain retained.

See [receipt writer adoption](receipt-writer-adoption.md) for the distinction
between included helpers and maintained writer activation, optional key indexes,
and replacement of the earlier receipt drafts.

Native subjects are authoritative for domain readers. Maintained source builders
may read their exact original source fields and processing checkpoints after the
shared reader validates subject identity, version and receipt membership. The
Congress reader supports all selected table members together and requires their
retained source schemas and footer metadata to agree. Missing source facts are
not synthesized by reversing native values into old source spellings.

Boundary checks live in `tests/test_subject_receipt_rollups.py`,
`tests/test_congress_receipts.py`, `tests/test_legislative_rollups.py`,
`tests/test_fec_identity_receipts.py` and `tests/test_etl_receipts.py`.

## Ordered receipt members

`build_generation(receipt_paths=...)` accepts an explicit ordered, complete
selection of retained receipt files. It records stable filenames under
`etl_receipts/` in `etlReceipts.version = 2`; the artifact pins every member and
the publication index includes each member's SHA-256, byte size and row count.
The declaration's generation identifies the selected publication. Unchanged
carried receipts retain their original occurrence generation and full evidence.
Admission still checks every receipt and its exact native subject match.

Publication copies an unchanged member from the prior generation only when its
key, digest and byte size match. Changed members upload normally. Remote byte
readback still checks **all** selected members, including copied history.
`SelectedInputs`, local native selection and Congress source replay preserve
member order. No temporary combined receipt is needed for scoped replay.
Monolith declarations and their existing key indexes remain supported.

This is an explicit local vote-family boundary, not automatic partitioning or a
live rollout. Split receipt indexing and owned receipt adoption refuse. Remote
staging can verify and publish metadata built by `build_generation`; its own
metadata builder still requires a monolith. MCP serves native tables, generic
receipt SQL and processing replay. Per-key `read_receipt_fields` supports
unindexed split votes under one aggregate receipt scan budget, checking every
member's policy identity and preserving ambiguity across members. Discovery
checks representation support without reading footers; a lookup still refuses
above the existing `SCAN_ROW_BOUND`, so splitting does not make an oversized
unindexed vote table queryable. Other split families remain unavailable; split
indexing remains unsupported. Scorecard analysis
accepts split vote receipts only on its projected native route, which reads
verified subject columns; source-scorecard and nonprojected receipt restoration
still require a monolith. An operational partitioning writer and live acceptance
remain migration gates before selecting split votes in production.
Focused coverage is in
`tests/test_receipt_member_publication.py` and `tests/test_split_receipt_lookup.py`.
