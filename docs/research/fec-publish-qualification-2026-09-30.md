# FEC candidate and agency local qualification — September 30, 2026

This is the earlier bounded selection. The subsequent
[retained-corpus qualification](fec-retained-corpus-qualification-2026-09-30.md)
reconciles it with the prior observation family, adds native Word and archive
readers, and records the user's PDF-processing deferral. The measurements below
preserve this earlier run's exact scope.

The candidate-history rollup and retained agency-report receiver passed local
source-to-table checks. These results establish the selected local generations;
no remote publication, hosted MCP availability or deployment was performed.

The source wheel is `spicy-docs==0.53.0+fec.3fc8388b368b`, built from
`3fc8388b368b204a57f6ea4029e26eadb132cfad`. Its SHA-256 is
`9b1b944f1dce879310d1bf37cbb4c9ae560dceb70d7eb9816d0862eb11b700b8`.
Independent archive builds produced identical bytes. The local version
identifies this candidate separately from released `0.53.0`; see
`vendor/README.md` in the repository.

## Candidate master

The exact input manifest covers every even-year bulk candidate master from
1980 through 2026. The header and 2024/2026 files reuse retained September 12
captures. The previously unretained 1980–2022 files were captured through free
official requests on September 30. All original acquisition dates, requested and
resolved URLs, byte sizes and digests remain in the manifest. The PostgreSQL
dump's older cycles are outside this selection.

The standard rollup ran with HTTP responses supplied from those verified retained
bytes. Its evidence artifact retains both the input manifest and response bodies;
replay transport timestamps are explicitly identified as local replay times.
The sealed generation is:

`sha256:dc9634bae8e1e0d881e34487c4d7115915ba281972dfff98a41426b657ec9ec0`

An independent Python CSV/ZIP reader compared every field by candidate ID and
cycle: **130,562 rows and 2,088,992 cells**, with no missing, extra or duplicate
identities and no differing cells. Only empty nonidentity source fields map to
NULL. Every generation member and source-evidence digest verified. The ordinary
local MCP server described the expected schema and returned matching SQL row
counts for every cycle.

This is complete coverage of the selected files, whose observation dates differ.
It does not assert that the current live source bytes are unchanged. The scheduled
producer will acquire every cycle afresh. A missing, empty, malformed or duplicate
cycle, changed header, or redirect to another cycle stops the build before a new
generation is published. Complete successful refreshes replace prior rows,
including retiring rows that disappear from the selected files.

## Agency reports

The installed source wheel processed the retained selection of 17 XML originals
and 103 Oversight pages. The FEC 2009 XML-named original is Word Flat OPC and
was an explicit native-parser refusal in this run. The later retained-corpus
selection uses a separate native Word reader. The receiver admitted every successful
source release: **119 collections and 9,191 records**, covering 16 FOIA XML
originals and 103 Oversight pages. No candidate or committee relationship was
inferred. The local generation is:

`sha256:2f2123013c3902c6b426064a3920beb712c86d9483e26821a50a96d483d8f356`

Independent comparison matched every received record against the retained native
report maps, including assets, bodies, fields and capture facts. A separate
standard-library XML traversal also matched all 7,743 XML elements.

The receiver accepts `profile: agency` through the existing retained-input
manifest, either as a directly selected original or a pinned source release.
Source-release identity, original digest, acquisition date, ordinal, format,
native metadata, bodies and declared assets survive in the observation tables.
The `html` dependency extra is pinned for Oversight parsing. Missing or altered
bytes, a wrong release pin and an unsupported representation refuse the whole selected output before
installation. The later Word reader preserves package and text/control facts
without claiming rendered or OCR output.

This agency-only local generation is a qualification selection. Before updating
the existing public `fec-observations` family, prepare and qualify the intended
complete manifest that preserves the existing selected collections alongside
these additions. This local generation alone is not a replacement for that
larger published family. PDF-only years, other report types, broader enumeration
and current recommendation status remain open under [FG11](../fec-gaps.md#fg11-adopt-agency-report-evidence-and-extend-declared-collections).

## Retained evidence

Receipts live under `fec-publish-completion-20260930/` in the retained corpora:

- `source/wheel.json` and `source/build_wheel.py`: exact build identity and recipe.
- `consumer/candidate-inputs/manifest.json`: original capture facts and provenance
  for the reused and newly acquired inputs.
- `consumer/candidate-replay.json` and `consumer/candidate-mcp.json`: sealed
  generation, evidence pin and actual local MCP replies.
- `independent/candidate-output-comparison.json`: independent whole-table comparison.
- `source/agency-release-manifest.json`, `source/agency-refusals.json` and
  `source/installed-replay/qualification.json`: every agency outcome and source pin.
- `consumer/agency-replay.json`: receiver selection, counts, member digests and
  verified local generation.
- `independent/agency-output-comparison.json`: complete receiving-table comparison.
- `consumer/gate.json`: repository checks and their logs. Initial failures and
  corrective checks remain retained alongside the final results.

## Repository validation

The installed-wheel full test run completed with 3,759 passing tests, two skipped
and 14 live integration tests deselected. Its sole failure found that the
inherited candidate schedule shared 20:00 UTC with Unified Agenda. The candidate
schedule now uses 20:10 UTC; the targeted hosted-workflow suite then passed all
93 checks. The full suite was not rerun after this schedule-only fix. Lint,
type checking and dictionary checks passed on the final files. The original
nonzero gate result remains in `consumer/gate.json`; the corrective results are
recorded in `consumer/final-verification.json`.
