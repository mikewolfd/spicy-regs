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

Native subjects are authoritative for domain readers. Maintained source builders
may read their exact original source fields and processing checkpoints after the
shared reader validates subject identity, version and receipt membership. The
Congress reader supports all selected table members together and requires their
retained source schemas and footer metadata to agree. Missing source facts are
not synthesized by reversing native values into old source spellings.

## Order of an admitted receipt member

`build_generation` writes every family's `etl_receipts.parquet` through
`etl_receipts.sort_receipts`: rows ascend by `dataset`, then `record_id` with
NULL last in its dataset, then `receipt_id`, each compared as UTF-8 bytes. A
row group holds one dataset and at most `RECEIPT_ROW_GROUP_ROWS` rows, so a read keyed on
`(dataset, record_id)` can skip to the group whose footer statistics hold the
key; `receipts_sorted` reports that property from the footer alone. The sort
changes no row, digest or schema. Files a builder hands to admission, and
generations admitted before this rule, keep the order their rows were emitted
in.

The position of a row in a receipt file therefore carries no meaning. A reader
that replays rows in the order a builder emitted them takes it from the
position each attempt identity records: the Congress source ordinal
(`restore_processing_input`), the legislative member and row (`restore_prior`),
the retry ordinal (`read_checkpoint`) and the FEC identity writer's emitted
count (`read_identity_processing`). A Congress receipt records its row's
place in one source file, and nothing records the sequence of several files. A
receipt file still in the order its shards were combined states that sequence.
From a sorted member the reader takes it from the subject members the files'
accepted rows went to, and refuses a file whose rows have no subject to place
it by. Subjects restored through `read_with_receipts` follow the subject table,
which admission copies unchanged. Rows that live only in receipts and record no
position, such as scorecard snapshots, come back in the member's order.

Boundary checks live in `tests/test_subject_receipt_rollups.py`,
`tests/test_congress_receipts.py`, `tests/test_legislative_rollups.py`,
`tests/test_fec_identity_receipts.py`, `tests/test_etl_receipts.py` and
`tests/test_receipt_order.py`.
