# Re-seeding `comment_attributes` after receipt retirement

The Regulations.gov ETL owns the `comment-attributes` family. Normal,
non-chunked comment batches upsert by `comment_id` over the table selected from
`publication.v2.json`: they keep untouched rows, replace matching rows, and add
new IDs. The writer validates fresh rows against the native schema, rejects null
or duplicate IDs, and publishes the result without per-row receipts. If no seed
is selected, the ETL warns and drops the batch's attribute rows rather than
publishing a partial table. Chunked comment ingestion does not stage attributes.

## Recover or rebuild the seed

Use the last known-good retained native generation for recovery when available.
If a full rebuild is necessary, exclude concurrent writers under the
`comments-catalog-write` lock before taking the final manifest and until the
new seed has been verified. Use the [comment re-read's plan and read steps](comment-fields-fill.md#commands)
with profile `all` and the current manifest, including keys added since any
reused read. Then run:

```sh
uv run --frozen fill-comment-fields attributes --workdir W
```

This only builds `W/comment_attributes.parquet` and `W/attribute_refusals.json`;
it does not publish. Attachment-only plans cannot seed this table. The seeder
applies the reviewed exclusions and chooses one copy per comment by latest
`modifyDate`, breaking ties by smallest ETag. Check read coverage, reconcile
refusals, and verify the expected row count and unique, non-null `comment_id`
values before accepting a seed.

There is no hosted re-seed job. A full seed requires a reviewed maintenance
publication through the receipt-free native writer
(`regulatory_publication.finish_dataset` with a captured `SelectedInputs`
index), not a loose Parquet upload. Build locally for review before publishing;
read back the selected `comment-attributes` generation and verify its table
identity, row count, and absence of `etlReceipts` before resuming incremental
batches. If the prior generation still has receipts, the
[receipt-archive pin](comment-attributes-receipt-archive.md#pin-and-merge-window)
is still required.

Do not follow the old working-copy upload and `run-rollup-comment-attributes`
sequence: that rollup is now a no-op, and the one-time native converter is not
a re-seed path. `sweep-regulations-attributes.yml` only seeds docket and document
attributes.
