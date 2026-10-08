# Generate large tables in remote storage

Remote generation keeps Parquet tables in fork object storage while retaining
the small artifact root and member manifest locally. It uses the same
generation format and publication gates as local builds. This avoids a full
local output and the additional publication copy.

Install the `source-readers` dependencies. `smart_open` owns the seekable S3
streams and multipart writer; PyArrow owns Parquet serialization. The host
supplies conditional requests, a serialized-output limit and byte hashing.
SpicyDocs continues to own source acquisition and parsing.

## Build and verify

Use a new staging prefix for each attempt. That prefix must contain exactly the
declared table objects. Keep receipts, logs and other files outside it; full
membership admission rejects unexpected objects.

`write_remote_parquet` consumes Arrow batches, preserves schema metadata and
returns a `StoredParquet` receipt. Its SHA-256 includes the final footer. The
receipt describes produced bytes; independent generation admission must still
read the object. `max_bytes` bounds serialized output, including the footer.
RAM holds the supplied batch, a multipart upload buffer and accumulated Parquet
footer metadata. Callers must bound their batches and measure row-group/footer
growth for the intended population.

The CourtListener opinion-body stager that first used this path was removed with
its table on 2026-09-23 (decision 6 in
[fork delivery decisions](research/fork-delivery-decisions-2026-09-22.md): opinion
text links out to CourtListener). `write_remote_parquet` stays for other large
outputs; `tests/test_remote_parquet.py` holds its contract.

The remote path does not merge prior populations automatically. A full-source
replacement needs an explicit prior-identity/value audit. Preserve source
originals and earlier generations throughout qualification.

See [generation publication](generation-publication.md). The storage experiment and
its actual R2 copy/refusal receipts are retained under
`~/Work/corpora/fork-execution-2026-09-21/court-body-remote-probe/`.

## Comments native preparation

`prepare_comments_selected_export.py --native-only --remote-staging-prefix <new-prefix>`
uses the captured-input and retained-scan pins to admit the historical pair.
The existing record splitter produces paired subject and receipt batches from
one consumed stream. Context-managed instances of the maintained R2 writer
store Comments and its receipts under the new, unpublished prefix. The same
index builder reads pinned remote coordinate columns; index receipts join the
open shared receipt stream. No complete body or combined receipt copy is local.

`comments-remote-preparation.json` states the actual staging keys, upload pins,
derivation, captured predecessor and validated artifact pin. It never claims
canonical local body paths or a local export seal. Full remote generation
admission hashes every member, decodes every page and validates current receipt
coverage and retained history before it reports an unpublished complete generation.
The publisher repeats source and predecessor checks immediately before each
conditional pointer attempt. Anonymous readback uses pinned, seekable HTTP
streams through the same generation verifier and keeps only artifact metadata local.

Historical admission, narrow matching/index data and current receipt admission
still need local scratch. Arrow batches, simultaneous upload buffers and Parquet
footer metadata still use memory. This route removes complete local output and
publication/readback copies; it does not establish a final resource peak or change
the operational reserve, storage, memory or elapsed-time stop limits.
The existing Navigation measurement cache still needs one genuine final
Comments main file. Its single receiver uses the completed public member's
actual size and digest; it runs after remote admission scratch closes and only
when that copy fits above the existing reserve. Cache and route checks reuse
that same file. No local full-generation directory or completed export seal is
invented for this handoff.
