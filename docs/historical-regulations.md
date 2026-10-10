# Rebuild issue and dated snapshot destinations offline

The maintained producer reads explicit original-byte pins and writes local
subject tables with the shared ETL receipt writer:

```sh
uv run --frozen python -m spicy_regs.transforms.historical_regulations selection.json output
```

The selection uses `format: spicy-historical-regulations`, `version: 1`, and a
nonempty `sources` array. Every source supplies `path`, decoded-original
`sha256` and `bytes`, with optional storage `encoding: gzip`. `fr-issue`
requires `package_id` and issue MODS. `ecfr-snapshot` requires an explicit
`selection` (`title`, `date`, optional `part` and `section`) and the exact
canonical `source_url`. The original input paths may stay outside the output.
This executable does not download or publish.

`federal_register_issues` records native issue metadata observations, keyed by
package and original metadata digest. Metadata page ranges remain literal;
renditions stay advertised unless separately pinned in `retained_renditions`.
Such retained originals must match a URL stated in that issue's MODS and pass
their decoded digest/size checks. No documents are inferred from a page span.

`ecfr_snapshots` records explicit title/date/scope observations keyed together
with the original XML digest. `ecfr_snapshot_sections` contains native
`SECTION` and `APPENDIX` occurrences keyed by snapshot and XML element position.
The same native number can occur more than once. Requested date, amendment
dates, source URL, part ancestry, literal text, attributes and source positions
stay distinguishable. Source XML retains fields outside these selected views.

All selected sources must finish before the requested output directory becomes
visible. A digest mismatch, unsupported source, wrong native identity or capped
read refuses the whole selection. Existing directories are never replaced.
The bundle includes the exact selection, policy descriptors and combined
generation-bound receipts. Its receipt generation is the SHA-256 of the exact
selection manifest: it identifies that selected input scope, not a code build
or public publication generation. External validation receipts pin the reader,
adapter and dependency code separately. Original MODS/XML and the manifest
each have an exact-digest witness; pinned rendition metadata stays traceable
through the manifest. Empty tables mean no selected source of that kind;
they do not mean publisher absence.

Use the retained selection and validation receipts under the canonical corpus
`metadata/historical-rebuilds/` to inspect offline evidence. The source-only
proof applies to these new selected-input destinations. Existing
`federal_register` document history and annual `cfr_sections` still require
their baselines until their original API/edition input histories are recovered
and separately replayed. Issue metadata and a dated snapshot cannot fill those
missing inputs.
