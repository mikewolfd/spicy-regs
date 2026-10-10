# Retained U.S. Code editions

`build-uscode-editions --manifest selection.json --output fresh-directory`
builds separate local tables from pinned Office of the Law Revision Counsel
archives. It accepts annual XHTML archives, a complete release-point XML archive,
or an explicitly selected title XML archive. It reads original ZIP files directly
and uses the maintained SpicyDocs archive and structure readers.

The manifest has format `spicy-uscode-edition-inputs`, version `1`, and a `sources`
array. Every source requires `path`, `sha256` (with the `sha256:` prefix), and
`kind`. Annual entries require `year`; `release-point-corpus` entries require
`release_point`; `release-point-title` entries also require `title`. Relative
paths resolve against the manifest directory. Select one pinned version of each
edition or title; conflicting or duplicate selections are refused.

`uscode_editions` identifies every admitted archive member, including supporting
files. Annual archive year and native publication year remain separate. A carried
appendix therefore keeps its older native year. XML editions keep their exact
release point.

`uscode_edition_structure` preserves native annual itempaths and XML section and
chapter observations. Each row links to its member, original byte span or XML
position, literal identifier, label, status and complete reader observation.
Annual itempaths can describe ranges, stubs or nonsections; the adapter does not
expand them into invented sections.

`uscode_edition_sections` adds section content from the same selected originals.
It links each section occurrence to its edition and source position and retains
the complete content-reader observation. Annual named fields preserve byte
spans and separate statutory body, source credits and notes. XML fields preserve
positional XPath, ancestry and literal child text. Quoted sections nested in
notes retain explicit source context. Missing body fields yield null; explicit
empty fields yield empty text. Unknown XML children remain separate with issues.
See SpicyDocs' `docs/sources/uscode-section-bodies.md` for exact reader semantics.
These destinations do not replace enacted-law text or determine legal status;
original members retain exact markup.

The tables use `DatasetPolicy` and row receipts. The build verifies original
archive hashes, reader identity and selected input stability, stages provisional
rows privately, and installs the output only after all archives succeed. It
refuses an existing output directory and retains a `rebuild.json` containing
input pins, reader pins, output hashes and scope. No baseline or network access
is used by the producer.

These are implemented local destinations. They are intentionally absent from the
published table registry until a separate publication selects their outputs.
The shared source catalog records their route choices and qualification evidence.
See `../../docs/historical-source-rebuilds-2026-10-10.md` in the workspace for the
retained corpus qualification and baseline decisions.

The `uscode-editions-public-v2` recipe creates fresh row receipts whose archive
witness references use content digests. Private archive paths remain in
`selection.json` and `rebuild.json`; exclude those files from public artifact
members. Publish only selected subject Parquet members and their fresh
`etl_receipts.parquet`. Earlier path-bearing receipt history must not be inherited.

Section-content output uses batches of at most64records through the maintained
receipt writer. Bodies and complete source observations can contain large text,
so a row-count bound does not establish a byte or process-memory bound. Edition
and structure output keep the writer's default batching. Qualification still
requires measured resource limits and independent content and receipt checks
for the selected originals.

All edition, structure and content outputs explicitly select the maintained
reference row validator. This preserves complete schema, subject and receipt
checks while avoiding the bulk validator's fixed DuckDB resource settings.
It does not establish a process-memory bound; Parquet reading and normalization
still batch rows. Resource qualification requires an independently supervised
run of the exact selected inputs.
