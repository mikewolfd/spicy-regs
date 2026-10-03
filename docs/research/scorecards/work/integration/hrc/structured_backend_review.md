# Independent review: Gemini structured page outcomes

Reviewed at 2026-10-03T20:46:26.625241+00:00 by the samples worker, independently
of the implementation worker. No substantive correctness finding remains in the
reviewed change. No files in the backend or its tests were edited by this reviewer.

This review covers the shared structured-output mechanism and synthetic tests.
It does not qualify HRC, validate any publisher source body, assess a model's
reading accuracy, or authorize publication.

## Reviewed files

| File | SHA-256 |
| --- | --- |
| `spicy-docs/src/spicy_docs/extraction/gemini.py` | `99699e569496a19b7ebdb4ae6cfe558a616b81f1f8abee4cdcdd428f846d31a5` |
| `spicy-docs/tests/extraction/test_gemini.py` | `71cc64ee10ce4fda131a4975ab28f909496c65e1890ad5b712e3ba50ee966f18` |

## Cases checked

- The caller must supply a prompt and JSON schema for structured mode. Invalid
  schemas and nonlocal references refuse before a provider call.
- Schema copying isolates the effective configuration from later changes to the
  original caller dictionaries. Local references resolve without remote schema
  retrieval.
- Duplicate object keys, malformed or fenced JSON, non-finite numbers and
  overflowing float literals refuse. Schema violations remain extraction errors.
- The provider must report a single completed `STOP` result. Token limits,
  safety stops and missing completion status cannot admit otherwise valid JSON.
- Success retains the request schema, effective configuration, source raster,
  full provider response and validated outcome. Failure retains its diagnostic
  input/output context. A structured outcome does not fabricate table geometry.
- Existing `DocumentExtractor` composition retains the observation and rendering.
  The structured branch does not silently select another OCR backend.

An additional constructor probe checked valid schemas with a root `$id` and a
local `$defs` reference, and with a local `$anchor` reference. Both were accepted.
An external reference in an unused definition was refused before any call.

## Verification

Executed from `spicy-docs`:

```sh
uv run --frozen --no-sync pytest -q tests/extraction/test_gemini.py
```

Result: **44 passed** for the files pinned above. These tests use injected clients
and synthetic input; no live provider or scorecard-fidelity conclusion follows.
The review found no need to change the implementation. Real publisher extraction
qualification remains a separate, source-specific check.
