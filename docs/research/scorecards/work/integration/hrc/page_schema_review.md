# HRC structured-page review

The HRC schema and prompt fit the inspected original page layouts. No blocking
defect was found in the page-observation prototype. The first successful model
response fails source fidelity: independent readback found eleven incorrect
action cells and removed party punctuation. A subsequent left-grid crop corrects
those defects and passes the reviewed visible fields; full-page and edition
admission remain closed.

Review date: 2026-10-03. Method: semi-formal code review, with direct visual
inspection of the retained original PDF renders and focused synthetic tests.

## Source and scope

The [capture manifest](capture_manifest.json) identifies the original HRC final
118th Congress PDF, SHA-256
`804c1157e71da53a00821fdcaf15a1790448c682eb49d44613a577cfce9cb855`.
Private evidence is under
`~/Work/corpora/supply-2026-09-02/receipts/scorecards-hrc-2026-10-03/`.

Direct inspection of `page-6.png` confirms a Senate grid with three period-score
columns, actions A–O, twenty member rows and seven endnotes on the right printed
page. `page-10.png` confirms twenty-six House rows with actions A–N in the left
grid and O–OO continuing the same rows in the right grid. Printed labels are
10/11 and 18/19 respectively; they differ from physical page numbers.

The retained `gemini-page6` and `gemini-page6-b` receipts record refusals. Their
failure records contain requests but no provider response; the second diagnostic
is a hostname-resolution failure. A later retained response under
`gemini-page6-resolved/` completed with `STOP` and valid JSON shape. Its outcome
SHA-256 is `79f007643d646e9d10088ce142e1a9b65367a136354297ea885669b37412fa93`.
The exact submitted raster matches `page-6.png`, SHA-256
`8eb545dd6d39445edd88ecc4e3e99120f308f688cb3e84feacb5e6995c1de7d9`.
No credential was read for this review, and no paid call was made by the reviewer.

The later `gemini-page6-left/` pilot supplies only the left half of the original
physical page at 2200×1700 pixels. Independent direct inspection of its actual
input and output matches twenty member names and party literals, sixty scores,
all three hundred A–O action cells, and Manchin's member endnote marker. It
corrects every previously recorded action and party-punctuation failure. The
[crop readback](page6_crop_independent_readback.json) pins the outcome and records
the selected high image resolution and medium thinking settings. Right-side
endnotes are intentionally absent from this crop's schema and have not been
merged; this is a bounded successful pilot, not a qualified physical page.

## Function trace

Paths below are relative to the workspace root.

| Function | Location | Verified behavior |
| --- | --- | --- |
| `outcome_schema` | `spicy-docs/src/spicy_docs/sources/scorecards/hrc_outcomes.py:41` | Declares page kind, chamber, labels, uncertainty, notes, literal scores and exact-width action arrays. |
| `outcome_prompt` | `spicy-docs/src/spicy_docs/sources/scorecards/hrc_outcomes.py:146` | Preserves source text and glyphs; distinguishes blanks/unknowns; combines House continuation columns by row; forbids inferred positions and outside-page states. |
| `validate_outcome` | `spicy-docs/src/spicy_docs/sources/scorecards/hrc_outcomes.py:99` | Refuses uncertainty, unsupported glyphs, incomplete representative row counts, duplicate identities and missing terminal Senate endnotes. |
| `main` | `spicy-regs/docs/research/scorecards/work/integration/hrc/probe_gemini.py:25` | Pins original PDF bytes, selects physical pages, retains page objects and JSON, and labels success as schema-validated but not source-qualified. |
| `Gemini.recognize` | `spicy-docs/src/spicy_docs/extraction/gemini.py:250` | Retains exact input/request/configuration, requires STOP, strictly parses JSON and validates the declared shape; no inferred table geometry. |

## Data flow and checks

Pinned PDF bytes pass through the existing `DefaultReader`, `FullPage` strategy
and injected Gemini backend. The schema and prompt become retained request
settings; a validated JSON object stays under the observation's `raw.outcome`.
Source-specific validation is a separate step. The probe deliberately does not
promote schema success into source admission.

The synthetic HRC suite passed 14 tests at this checkpoint. Tests explicitly
cover literal caret/N/A scores, action symbols, missing cells, unknown or blank
glyph refusal, representative row counts, endnotes, model uncertainty, House
continuations, duplicate identities and unexpected fields
(`spicy-docs/tests/test_scorecards_hrc_outcomes.py:32–106`). The shared backend's
extraction/API/interpretation suite passed 89 tests, including failure retention,
duplicate keys, nonfinite numbers, STOP, schema references and input rendering.

## Findings and admission limits

- **Blocker for source admission — incorrect action cells:** The retained model
  output changes Romney's F/I cells from `⊗` to `○`. It also changes Murray's
  H/I/J cells and Manchin's, Johnson's and Barrasso's H/I cells. These are eleven
  concrete differences across five rows, recorded with JSON locators in
  [the independent readback](page6_independent_readback.json). All twenty party
  strings omit the visible source parentheses despite the explicit prompt.
  The model reports no uncertainty. Shape validation cannot detect these errors;
  keep this run refused without altering the original model output.
- **Observation — historical score chamber:** Page 6 explicitly defines the
  caret as “Indicates House Score”; Welch's two historical score values carry
  this mark. The page schema preserves the caret and legend. A later adapter
  must retain that source context when mapping historical metrics instead of
  assuming every score in a Senate grid describes Senate service.
- **Observation — edition coverage:** `validate_outcome` checks row counts for
  directly inspected representative pages, not every House page. Its docstring
  states this limitation. Full edition admission still needs every selected
  page, source count reconciliation, continuation-state rules and source-value
  readback. Schema acceptance alone is insufficient.
- **Observation — bounded fidelity review:** This review establishes concrete
  failure examples, not a complete error count for every field or an assessment
  of all PDF pages. It provides no basis to run or admit a full edition with the
  same configuration. The shared backend correctly retains the failed-to-qualify
  response as an observation; no source adapter has admitted it.

## Review pins

| File | SHA-256 |
| --- | --- |
| `spicy-docs/src/spicy_docs/extraction/gemini.py` | `99699e569496a19b7ebdb4ae6cfe558a616b81f1f8abee4cdcdd428f846d31a5` |
| `spicy-docs/src/spicy_docs/sources/scorecards/hrc_outcomes.py` | `fbd1b65d93bdc46a9ca0dfe8b08a61963ded885aea149d8e8618b09b46f2281f` |
| `spicy-docs/tests/test_scorecards_hrc_outcomes.py` | `d1b011a3876748896402bd78db4d3161de2d55b88de3c5d4064f7fdc0e5c41e2` |
| `spicy-regs/docs/research/scorecards/work/integration/hrc/probe_gemini.py` | `b14acde80d8cf7646f34fa32fb598afbb3f87e6ca2b33a50302cf9ad4fa7a438` |

VERDICT: APPROVE the page-schema/probe implementation; REQUEST CHANGES to the
extraction coverage and qualification before source admission. The reviewed
left-grid crop now succeeds, while the original full-page failure remains intact.
Coverage is adequate for the stated prototype scope. Confidence is high in the
concrete full-page failures, crop readback and shape/refusal behavior. Production
admission stays closed.
