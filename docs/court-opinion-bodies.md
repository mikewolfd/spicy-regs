# CourtListener opinion text variants

`court_opinion_bodies.parquet` version 2 retains all eight source text fields as
nullable strings: `plain_text`, `html`, `html_lawbox`, `html_columbia`,
`html_anon_2020`, `html_with_citations`, `xml_harvard`, and `xml_scan`. It adds six
columns after the original nineteen. Existing named selections of `plain_text`
and `html_with_citations` continue to work.

The strings are source values, including whitespace, markup, quoting and line
endings. An empty source string stays `""`; a source NULL or absent field stays
NULL. The remaining metadata columns retain their existing empty-to-NULL policy.
Non-string text values refuse the row instead of being stringified.

`available_text_fields` lists nonempty fields actually stored in that row, in
the eight-field order above. `text_char_count` is the largest stored variant's
length in Unicode code points, represented as a string under this table's
existing all-VARCHAR policy. It counts markup too; it is not a prose length,
byte count, sum across variants or completeness measure. No nonempty text means
NULL availability and `"0"` characters.

## Source and consumer boundary

SpicyDocs already owns bulk acquisition and the PostgreSQL CSV decoder. The
builder continues using `CourtListenerBulkReader`; it does not add another
source reader. The source opinion ID, cluster ID, dump date and source
creation/modification dates remain available beside the native field names.
Acquisition receipts must retain the actual bulk object's locator, ETag and
captured byte range/hash. `download_url` names CourtListener's upstream source
document; it is not a claim that this URL returns one of these exact strings.

These fields are not substituted for each other or treated as plain text.
Even a field called `html_with_citations` can contain an XML declaration, as the
retained native sample demonstrates. Consumers must choose and parse a variant
explicitly before describing its media type or extracted text.

Rulespec's `DocumentCapture` represents a document's source artifact, derived
text stream, structure and evidence spans. None of those conversions happen
here. Wrapping these raw strings in a capture would introduce an unperformed
conversion and misleading provenance. A downstream converter may use that
existing owner shape after selecting a retained variant and recording its own
parsing and source binding. This change introduces no parallel capture schema.

## Versioning and rebuilding

Parquet metadata records
`spicy-regs:court-opinion-bodies-schema-version = "2"`. Both a first build and
an incremental merge write it in the same artifact as the rows. A prior must
have that version and the complete 25-column string schema.

Version 1 retained only two variants and collapsed empty text to NULL. It
cannot establish the missing six values or recover the source's empty/NULL
distinction. The builder therefore refuses a legacy or incompatible prior
before reading new source rows or replacing the output. It does not fill the
missing variants with invented values or retain old availability claims.

For an intentional rebuild, call `build_court_opinion_bodies(..., rebuild=True)`
in a fresh output directory. This skips remote-prior acquisition and merging;
it produces only the explicitly selected source scope. For example, a retained
compressed prefix requires its original byte bound:

```python
build_court_opinion_bodies(
    fresh_output_dir,
    local_file=retained_prefix,
    dump_date=date(2026, 6, 30),
    max_compressed_bytes=2 * 1024**2,
    rebuild=True,
)
```

Rebuilding a small slice does not repair the old full artifact or authorize
replacing it. Retain the old generation and review scope/coverage before any
publication. A version-2 merge replaces a refreshed opinion's whole row,
including text removed or changed to empty by the source; unread opinions keep
their prior values.

## Direct qualification

The September 21 remediation retained one original HTTP range: the first
2,097,152 bytes of `opinions-2026-06-30.csv.bz2`, with ETag
`150e073ae1e7adad94ca9a57fb449980-6505` and compressed SHA-256
`601e773eef7e10b0689913a22c9da3a18eae447d597a29e2a1540fff42011d70`.
The owner reader yielded 284 complete records. Independently re-encoding their
CSV values reproduced every decoded source byte through the last complete
record. Seven text fields were populated; `xml_scan` was not observed nonempty
and is covered by explicit synthetic controls.

Opinion `380204` carries 17,461 characters in `html`, SHA-256
`716ae706f3d0aad0c2bc5fd5fa8f5a447e1f9d434fcc32bd007fe21c38084e8b`.
Its retained older output agrees on all nine compared nonempty shared native
fields, including `html_with_citations`, but has no `html` column. The fixture
preserves the complete source CSV record and header, recompressed locally.

New receipts live at
`~/Work/corpora/supply-2026-09-02/receipts/remediation-sprint-2026-09-21/court-text/`.
`capture.json`, `raw-output-witness.json`, `fixture-selection.json` and
`materialized-replay.json` retain exact inputs, locators, hashes and output
comparisons. This is a bounded local correction qualification, not a full-dump
backfill, public replacement or release.

The qualified artifact occupies 3,626,326 bytes for 284 rows. Disk planning now
uses 16 KiB per opinion for sized cluster selections and twice the compressed
input size for unfiltered or unsized selections. These values replace the
smaller two-variant estimates; this sample does not establish a population
upper bound or remove the need for operational headroom monitoring.
