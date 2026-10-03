# Zyte scorecard transport review

**Approved for the reviewed local implementation. No blocking finding.** This reviews transport and evidence behavior, not complete IJM acquisition, publication, or source qualification. No runtime files were changed by this review.

## Function trace and verified behavior

| Entry point | Trace and result |
| --- | --- |
| `fetch_for_publishers` (`acquisition.py:76`) | Rejects unknown adapter IDs. Only explicitly named publishers use Zyte; other publishers keep the existing direct path. Registry enablement and edition selection remain separate. |
| `zyte_fetch` (`acquisition.py:17`) | Obtains the credential through the existing provider helper, uses the existing `ZyteHttpFetcher`/`ZyteTransport` and bounded capture, requests original `httpResponseBody`, and closes the client. |
| `fetch` (`acquisition.py:46`) | One attempt per URL. The request cap stops before another paid call. A source 401/403 propagates `CredentialRefusedError`; other transport failures become a sanitized fatal `ScorecardTransportError`. No implicit direct/proxy fallback. |
| `_fatal` (`build_scorecards.py:68`) | Walks wrapped causes/contexts and rethrows credential, evidence, or selected-proxy failure. The build cannot treat those as a skippable publisher error and advance another scope. |
| CLI and workflow | `ScorecardsRollup.__init__:43` refuses simultaneous injected and selected transport. `--zyte-publisher` is explicit. Workflow inputs enter environment variables and quoted argument-array elements (`rollup-scorecards.yml:62–92`); the workflow remains manual and defaults to build-only. |

The credential enters the provider Authorization header, not the publisher request. The installed provider checks reflected raw and Basic-encoded credentials in response bytes and metadata before returning a target body (`spicy_docs/sources/zyte.py:229–320`). The consumer's journal recursively scrubs the configured credential before serialization (`source_evidence.py:148–177`), and generic provider failure prose is not retained.

A successful response goes through normal policy-bound source capture. Separate proxy receipts state provider, rendition, request ID and original-byte status, without retaining a body. `metadata_only` suppresses the raw source digest in that separate receipt as well (`acquisition.py:58–68`); normal capture and snapshot IDs remain opaque. `hash_only` retains the source digest but no original body. The caller does not create disk spools or a new workflow upload path.

## Checks

- `uv run --frozen --no-sync pytest tests/test_scorecard_acquisition.py tests/test_scorecard_refresh.py -q`: **34 passed**. This covers policy-specific receipts, target 401/403, provider failure, wrapped fatal errors, explicit selection, missing credentials, request caps, and refresh preservation.
- Supplemental injected-response check scanned every sealed `metadata_only` artifact member: no original bytes, raw source SHA-256 hex, or synthetic credential appeared.
- Supplemental oversized-response check used a 10-byte bound and a 100-byte injected target: acquisition refused after exactly one provider call.
- Read the installed provider's transport, credential reflection checks and bounded capture implementation, in addition to the consumer changes.

## Limits

The default 2,000-call ceiling is **per selected publisher context**, not a shared invocation-wide spending limit or a 2,000-per-minute rate. The caller uses a 0.1-second minimum request interval. This is bounded as implemented; a future invocation-wide cap would require sharing one provider budget across selected publishers. None is asserted here.

These local checks used synthetic credentials and injected responses, with no paid calls. A successful first IJM page does not qualify pagination, identity, completeness or the full edition; the separate real-source qualification must establish those.

## Reviewed bytes

Installed SpicyDocs: `0.53.0+scorecards.6faacdb84390`.

| File | SHA-256 |
| --- | --- |
| `src/spicy_regs/scorecards/acquisition.py` | `53f1e2c5ce1f1bbaf46f41fc6281f60595b4ae9a7603d5339c43ff525ac85e89` |
| `src/spicy_regs/transforms/build_scorecards.py` | `a64dccd3405e5bb729c64fefc637cae7bd9b56fd8fceb69626d6cacc2bd480a4` |
| `src/spicy_regs/pipelines/rollups/scorecards.py` | `468bed8285551fd92fef28c0e3fe4553a7dc99d7afbe7e03bc6018b56fe09351` |
| `.github/workflows/rollup-scorecards.yml` | `2a0694a7fdcb8a7e2d30f1f6f04180211b04dc5d5e2caa4e2232455b070dead9` |
| `tests/test_scorecard_acquisition.py` | `7b33fa8b1da169aeae49241b597558043ee40e4d8b54d859b1d629960f34a183` |
| `tests/test_scorecard_refresh.py` | `204db379dfd35a1d0aa035188bb2deeda35d5d579af2749805ca6ffe617b5e3a` |
