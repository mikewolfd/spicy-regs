# Recovering a completed remote Comments subject

Remote preparation can fail after the subject upload completes but before its
receipt upload closes. The completed subject remains an unpublished input;
missing receipt bytes cannot be inferred from it. Preparation records
`completedSubject` as soon as its upload closes, before building the index.
That checkpoint is not a complete generation or publication permission.

For a completed subject without a persisted byte digest, pass
`--retained-remote-subject` and `--retained-remote-subject-sha256` to
`scripts/prepare_comments_selected_export.py`, together with the existing
remote native preparation options. The descriptor is a regular JSON file with
exactly these fields:

| Field | Required value |
| --- | --- |
| `format` | `comments-remote-subject-recovery/1` |
| `bucket` | The preparation's bucket |
| `source` | The unchanged captured pair snapshot |
| `originalMembers` | The exact captured input descriptor's `members` |
| `subjectPolicy` | The unchanged current Comments policy descriptor |
| `subject` | `key`, `etag`, `byte_size`, and `rows` for the completed staging input |
| `subjectByteDigest` | `null`: no previous full byte digest is asserted |

Use a new, empty output staging prefix. The preserved input must use a
distinct `staging/comments/<attempt>/comments.parquet` key. Before execution,
qualify ownership, closure, resource capacity, original input custody, and the
exact source release through the existing operational controls. No preparation
or publication starts merely by writing this descriptor.

The maintained reader opens the preserved subject with an ETag condition on
every request and checks its schema and row count. The original selected pair
and retained scan are still read to reconstruct the missing receipt contexts
and subject versions. Each resulting subject batch is compared with preserved
rows in the same order, including all values, nulls, lists and repetitions.
Short input, trailing input, or differing values refuses. This reconstruction
is necessary receipt work; it is not an assertion that no original data is
processed. The preserved rows supply the repacked output.

The shared Arrow writer coalesces batches without converting them back to
Python rows. It bounds a group by the row and logical-byte settings in
`sources/remote_parquet.py`; an oversized supplied chunk flushes immediately,
and slices can retain input buffers. This separates the existing small input
batch from Parquet row groups. The schema comparison uses the maintained
Parquet serializer's readback schema with metadata checks enabled, including
its compliant structural LIST child names. It preserves application fields,
types, nullability and metadata rather than discarding schema checks.

New uploads retain their produced-byte SHA-256, size, ETag and row count.
Complete generation admission still independently verifies bytes, pages and
receipt relationships before publication. Preserve the old input object until
that validation and the operational recovery decision complete. A final main
materialization can download the compact admitted subject once, checking its
known digest during that transfer, then reuse that same custody-checked file
for the actual final readers. It does not require a separate download solely
to discover the old input's missing digest.

The old footer must still be parsed during repacking. Coalescing avoids carrying
its row-group overhead into new outputs; it does not guarantee a particular
output size, execution time or memory peak. Keep preparation and final-reader
limits unchanged and qualify actual output metadata, capacity and final checks.
