# Regulatory catalog subject and receipt writes

Regulatory catalog writes use native business fields in the configured namespace
with `_native` appended. The prior namespace remains a retained migration input.
`regulatory_catalog.ensure_native` first prepares empty physical tables. It then
converts existing rows, validates every subject against its receipt, and commits
the populated pair with a shared initialization receipt. Until that checked
receipt exists, reads continue using the retained legacy source. A crash after
empty preparation therefore cannot select an empty dataset. A retry resumes the
atomic population step. Invalid source values
refuse migration without changing the retained source table.

`replace_native` reads the prior subject and receipt in one transaction, compares
any caller-provided prior and snapshot, converts the replacement, and writes the
subject and its current receipt in the same multi-table Iceberg transaction.
The readback checks exact identity, policy, content digest, and restored values.
Prior source witnesses carry forward. Exact source records remain in receipts;
their witness digests refer to their canonical `exact_json` bytes.

Processing and retry fields are reconstructed only by `processing_table` from a
qualified pair. The scheduler, repair, text enrichment, field fill, seed, and
backfill callers use that boundary. Refused writes roll back their subjects and retain a refused attempt receipt.
Unselected older or duplicate source observations remain rejected receipts. Prior table and
receipt versions remain subject to the catalog's snapshot retention policy.

Exports capture native subjects and receipts together, qualify them, and retain
both in a generation-specific sidecar. Publication uses that exact pair, including
its receipt lineage. Fixed public mirrors contain native subject fields.
Receipt-only checkpoints use selected generations before acquisition state
advances. No registry override enables a legacy production writer.

Local tests exercise rollback, stale priors, invalid receipts, migration, scope,
and the scheduler and repair entrypoints. The disposable Apache Iceberg REST
fixture additionally exercises actual multi-table commits, interrupted migration,
crash recovery, concurrent conflicts, and forward undo. The configured catalog
must support multi-table transactions; unsupported catalogs raise rather than
falling back to separate subject and receipt commits.
