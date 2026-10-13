# Rebuild retained regulation destinations offline

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
generation-bound receipts. Its receipt generation combines the selected recipe
with the SHA-256 of the exact selection manifest. Selections containing annual
CFR use `retained-annual-cfr-v1`; other selections use
`historical-regulations-public-v2`. It identifies the selected input scope and
receipt recipe, not a public publication generation. External validation receipts pin the reader,
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

Public row receipts use original content digests as witness references. Private
filesystem paths remain in `selection.json`; publish only explicitly selected
subject and receipt Parquet members. Keep every other local file outside the
public artifact. Selection metadata admits only the defined
source fields, and retained rendition metadata admits only publisher URL, digest
and decoded size. Reader observations preserve native publisher text. Create
fresh receipts for this recipe; inheriting old path-bearing receipt history
would retain those earlier private witnesses.

## Retained annual CFR volumes

An `annual-cfr` source supplies an explicit `selection` (`year`, `title`,
`volume`), `mods` and `xml` originals, and complete inventory expectations
(`expected_constituents`, `expected_sections`). Each original supplies `path`,
decoded `sha256`, `bytes`, canonical `source_url`, and a `capture` with its own
`path`, `sha256`, `bytes` and complete `expected_record`. The capture must bind
the original's URL, hash, decoded size and successful response. Storage may be
gzip; capture size is bounded separately in the producer.

`expected_edition` and `expected_native_identity` must match the complete
maintained reader results. MODS issue/original issue dates and cover-only status
remain separate from the XML's native revision and requested edition. A body
may state no volume; its identity records whether the volume came from the
request URL. No modification date is inferred from an issue date.

The maintained `prepare_retained_cfr_rows` adapter keeps all ordered direct
MODS constituents, including a declared first title node whose identifier is
the package itself. It reuses the established CFR shaping and placement rules
and requires complete, unique section-token coverage. The bundle writes the
established `cfr_sections` schema; it does not merge a prior table.

Annual rows use the explicit `retained-annual-cfr-v1` receipt recipe. Witnesses
link both originals, both captures and the selected manifest. Receipt processing
keeps native edition/identity facts and each row's literal MODS selectors,
constituent position, placement class and annual occurrence position. The
maintained scan does not emit XML tree paths, so this recipe does not invent
`annual_source_path` values. It is distinct from an external qualification
checker that retains independent tree paths; receipt byte equivalence is not
implied. Private source paths and unselected manifest fields stay outside
public receipts.

Focused synthetic tests establish admission and mapping behavior. Qualification
of a retained volume still needs an exact input/code/runtime selection and
independent comparison of every destination field and intended receipt context.
A qualified selected volume does not establish full API history, a real Federal
Register join, publication authority or baseline replacement.
