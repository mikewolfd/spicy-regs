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

For a complete retained CourtListener opinions original, use
`stage_court_opinion_bodies_remote`. Supply the actual compressed-file SHA-256,
dump date and an explicit output allowance. It verifies the complete source
before opening an upload, uses the ordinary source reader and version-2 field
mapping, and refuses a changed or incompletely consumed input before completing
the staged object. This path has no source row cap or local output copy. The
ordinary local builder retains its disk-headroom policy.

The opinion stager flushes at 2,000 rows or 64 MiB of UTF-8 values, whichever
comes first. A larger individual record, bounded by the source reader, forms
its own row group. These are payload limits, not total process-memory limits;
Python/Arrow objects, encoding buffers and footer metadata also consume memory.
Full runs require measured resource limits and independent output audits.

Pass the staged receipts, exact expected keys, dictionary schemas and captured
publication index to `prepare_remote_generation`. It writes only
`artifact.json` and `members.json` locally. Rulespec hashes every stored byte;
the host also decodes every Parquet column and checks schema and row count.
Every source GET, including seeks and retries, uses the same ETag condition.
The ETag identifies which object to read; it is not accepted as SHA-256 proof.

Retain the staged receipts and metadata directory together. Use
`verify_remote_generation` to recheck them. A plain local generation verifier
cannot verify a metadata-only directory.

## Publish and recover

Native-field audits, complete selected-population checks and preservation of
prior rows remain required before calling `publish_remote_generation`.
Storage verification alone cannot establish those facts.

The remote publisher runs the ordinary family, membership, shrink and source
evidence checks. It conditionally copies staged tables into the final
digest-based generation prefix, with a source ETag condition on every part and
an absent-destination condition on completion. It admits the final stored bytes
before changing the publication index through the existing conditional write.
It never points consumers at staging objects.

A failed build aborts its unfinished multipart upload. A completed but
unqualified staging object stays unreferenced. Failed promotion may leave
immutable destination objects; a retry can reuse them only if independent byte
admission passes. A stale source, corrupt target or concurrent index change
refuses publication. If a network error makes the index-write result uncertain,
read the index before deciding whether another attempt is needed.

The remote path does not merge prior populations automatically. A full-source
replacement needs an explicit prior-identity/value audit. Preserve source
originals and earlier generations throughout qualification.

See [generation publication](generation-publication.md) and
[opinion text semantics](court-opinion-bodies.md). The storage experiment and
its actual R2 copy/refusal receipts are retained under
`~/Work/corpora/fork-execution-2026-09-21/court-body-remote-probe/`.
