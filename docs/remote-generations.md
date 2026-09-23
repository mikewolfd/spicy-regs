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
