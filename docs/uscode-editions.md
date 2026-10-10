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
expand them into invented sections. Full section bodies remain in the original
members. This destination does not replace enacted-law text or provide a complete
normalized Code text model.

Both tables use `DatasetPolicy` and row receipts. The build verifies original
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
