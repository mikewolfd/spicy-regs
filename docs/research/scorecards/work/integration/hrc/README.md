# HRC source qualification

HRC's final 118th Congress source facts now pass the explicit provider reader
against the reviewed observation pin and a fresh exact original-PDF capture.
The reader emits every named member, published rating, displayed item result and
scored item in the inspected source scope. Independent reader review passed every selected record and the complete mapped
output. Normal provider package adoption remains a separate gate; HRC has not
been published.
See `reader_qualification_v2.json` and `candidate_checkpoint.json` for current
status. `reader_qualification.json` preserves the earlier parser-1 qualification.

The reader now normalizes exact standalone `NA` to `N/A` in native rating
columns, as requested. The pinned raw observations still retain the original
`NA` spelling. Parser `hrc-118-qualified-gemini/2` changes exactly one rating
(Foushee’s 116th Congress score); all other native fact fields match the earlier
reference output. `value_number` remains null for either missing-value spelling.
Its fixed schema preserves the vacancy as a privately retained non-member
disposition with exact label, district, note, N/A scores and blank cells. It
creates no vacancy member or member-dependent rows. The public completeness
witness accounts for this source row and the V1 tabular limit explicitly.
Item narratives and source-only notes remain in the pinned private structured
asset. No raw source values, generated records or previous failed observations
were patched; normalization occurs only while mapping native table columns. Historical Senate observations retain their original semantic-v2
provenance alongside later semantic-v3 observations.

`semantic_qualification.json` preserves the earlier single-page experiment.
Its Senate item observation was refused after full readback found an incorrect
bill citation for item J. The later half-page observations preserve the correct
source citation; see `senate_items_notes_independent_readback.json`. The earlier
model response remains unchanged and is excluded from accepted observations.

`house_semantic_qualification.json` records a refused House observation. Its
schema and source-key coverage passed, and direct readback matched the member
identities and published scores. Several action marks still differed from the
original image. The raw response remains unchanged in the private corpus;
the refusal prevents this observation from qualifying a source snapshot.
`house_tight_crop_qualification.json` records a later bounded success using a
tighter image at 300 dpi: the reviewed member data, published ratings and visible
item results match the original. These observations preserve separate scopes;
the newer paired-image observations have separate readback receipts. A passing
band does not qualify the full House table.
The controlled comparison returned identical, source-matching records at medium
and high thinking with Gemini 3.8 Flash. Retained requests confirm that only the
thinking setting changed between those two calls. The earlier broader crop used
medium thinking too; these observations do not show that higher thinking was
needed to recover the reviewed facts. Google's
[thinking guide](https://ai.google.dev/gemini-api/docs/thinking/) documents the
model's supported levels.

`capture_manifest.json` records original HTTP captures. `advertised_editions.json`
records the original index links. `docling_diagnostic.json`, `qualification.json`
and the independent readback files retain earlier extraction experiments;
their positional outcomes are diagnostic history, not the current schema.
Failed observations are never substituted into accepted source tables.

Private source bytes and complete model observations are retained at
`~/Work/corpora/supply-2026-09-02/receipts/scorecards-hrc-2026-10-03/`.
That directory contains the PDF, raw index, rendered images, raw model requests
and responses, schema/configuration, and rejected observations. Repository
receipts retain hashes and bounded qualification metadata only.

## Reproduce a bounded probe

Run from the consumer environment containing the installed provider with
`hrc-118-source-facts/3` and structured-mode `additional_images` support. Earlier
receipts keep their original schema and package versions. Supply an unused private output
directory. The scripts read the existing PDF and never acquire congressional
records or publish data.

```sh
uv run --frozen --no-sync python /path/to/probe_gemini_crop.py \
  --source /private/hrc/118-pdf.body \
  --output /private/hrc/new-member-probe \
  --credentials /private/config.env

uv run --frozen --no-sync python /path/to/probe_gemini.py \
  --source /private/hrc/118-pdf.body \
  --output /private/hrc/new-item-probe \
  --credentials /private/config.env \
  --pages 3 --media-resolution MEDIA_RESOLUTION_HIGH --thinking-level medium

uv run --frozen --no-sync python /path/to/probe_gemini_house_crop.py \
  --source /private/hrc/118-pdf.body \
  --output /private/hrc/new-house-probe \
  --credentials /private/config.env \
  --scope arizona-context --thinking-level medium
```

The credential file uses the existing `GEMINI_API_KEY` name. The member probe
uses the existing renderer and crop function; its region, source hash and image
hash stay in the private receipt. The current schema does not ask the model to
emit row coordinates, panel names or visual headers.

The qualification host initially could not resolve Google's Gemini hostname.
An explicitly selected `--resolve-ip` diagnostic verifies the address against
a fresh HTTPS DNS response for that exact original hostname, retains the DNS
receipt, and preserves original-host TLS verification. It changes neither
system DNS nor production transport. It sends no credential to the DNS service.
Omit it when ordinary DNS works. Never reuse an address unless the script's
fresh original-host lookup validates it.

Every live result still requires direct original-source comparison. A schema,
member count, repeated model answer, or matching item-key set does not prove
that a glyph was transcribed accurately. The separate historical experiments
demonstrate this distinction explicitly.

## Reproduce the source qualification

`probe_gemini_house_bands.py` reads the already captured PDF, selects paired
small source bands, supplies original table and state headings as image context,
and retains every request, image, response and setting in a private directory.
It checks source-key coverage and writes an independent native-source diagnostic;
it never labels those checks as complete source qualification. Vacancies have a
separate semantic collection and retain blank item results as nulls.

`house_source_validation.py` independently reads the PDF's existing native span
metadata. For the reviewed symbol font it distinguishes filled and stroked
circles using the original rendering flags. Plain native text alone is
insufficient because both circles use the same character. Unclassified marks,
including crossed circles, remain explicit image-review work. The check was
calibrated against the independently reviewed Alabama band and the known failed
broad House crop; it neither changes nor supplies model output.

`render_house_source_checks.py` prepares original-cell contact sheets for those
unclassified source marks, alongside filled/open controls. The sheets contain no
model predictions. Their crop bounds, source hash and image hashes remain in the
private corpus. Source review results must be recorded separately before an
edition can qualify.

`pending_provider_na.patch` and `na_rule_proposal.json` preserve the earlier
isolated proposal and validation experiment. The literal-NA fix is now applied
in the provider source and covered by focused tests; package adoption is tracked
separately. The old refused House candidate and its receipts remain unchanged.

The complete private input remains `qualified-118-observations.json`, with pin
`d8ac93e130bfb17bbb8657b6d25b6419bd79ca964fe03608a889b21c9385bebb`.
Its assembly helper `assemble_qualified_observations.py` and the replay helper
`qualify_hrc_reader.py` are retained beside it in the private corpus. The asset
copies complete selected records unchanged, with original outcome selectors,
page numbers, source and observation hashes, and independent review receipts.
The parser-2 replay helper `qualify_hrc_reader_v2.py` writes a separate
`reader-qualification-v2/` output and compares every native fact with parser 1.
It does not overwrite earlier reference tables or qualification artifacts.
The source-tree replay uses the actual fresh `deployment-source/source.receipt.json`
HTTP observation; it does not fabricate an HTTP response for the structured asset.
The reader's callback retains that asset privately and returns an opaque UUID for
public field locators. No model calls are needed to replay this qualified edition.
