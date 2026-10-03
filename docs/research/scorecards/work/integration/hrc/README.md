# HRC source qualification

HRC's original 118th Congress PDF is available. The current outcome schema
extracts source data: members, ratings keyed by their stated periods, results
keyed by publisher item IDs, scored items, requested actions and notes. Image
regions remain private input evidence. No HRC edition has been published and
no complete production reader is qualified.

See `semantic_qualification.json` for the current result and artifact hashes.
The bounded Senate member crop matches the original-source readback, including
the historical House-score carets and member footnote marker. The item-page
probe preserves the reviewed identifiers and requested cosponsorship actions.
Full prose transcription, the House spread, complete source-page coverage and
historical layouts remain open qualification work.

`house_semantic_qualification.json` records a refused House observation. Its
schema and source-key coverage passed, and direct readback matched the member
identities and published scores. Several action marks still differed from the
original image. The raw response remains unchanged in the private corpus;
the refusal prevents this observation from qualifying a source snapshot.
`house_tight_crop_qualification.json` records a later bounded success using a
tighter image at 300 dpi: the reviewed member data, published ratings and visible
item results match the original. These observations preserve separate scopes;
no continuation results or full House page have been joined or qualified.
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

Run from a SpicyDocs environment containing `hrc-118-source-facts/2`, or the
consumer after adopting that provider package. Supply an unused private output
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
