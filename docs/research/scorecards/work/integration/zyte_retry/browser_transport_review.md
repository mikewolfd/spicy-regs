# Browser transport review

VERDICT: APPROVE the explicit IJM browser transport path. No blocking correctness
or evidence-policy defect was found. This approves transport behavior, not IJM
parser completeness, source qualification, registry enablement or publication.

Review date: 2026-10-03. The review traces the consumer selector through the
installed provider transport and capture boundary. No live or paid requests were
made by the reviewer.

| Function or path | Evidence | Verified behavior |
| --- | --- | --- |
| `zyte_fetch` | `src/spicy_regs/scorecards/acquisition.py:23` | Refuses browser mode for other publishers; creates body/DOM routes using one shared paid-call budget. |
| Nested `fetch` | `src/spicy_regs/scorecards/acquisition.py:53` | Selects browser DOM only for HTTPS, exact `scorecard.ijm.org` authority, and `/wp-json/rds-bt50-scorecard/v1/` prefix. Index/scripts use publisher-byte mode. Each capture has one attempt. |
| Proxy evidence | `src/spicy_regs/scorecards/acquisition.py:73` | Records representation and `body_is_publisher_bytes` independently; never retains response bodies in the proxy event; omits source-byte digest under metadata-only policy. |
| `fetch_for_publishers` | `src/spicy_regs/scorecards/acquisition.py:98` | Requires explicit publisher selection, rejects conflicting modes and unknown publishers, leaves unselected publishers on the existing direct path. |
| `ScorecardsRollup` / CLI | `src/spicy_regs/pipelines/rollups/scorecards.py:24` | Propagates explicit `--zyte-browser-publisher`; refuses combining it with an injected fetch factory. Source enablement and qualification checks remain separate. |
| `_fatal` | `src/spicy_regs/transforms/build_scorecards.py:68` | Finds typed credential/provider failures through exception cause/context chains; stops the invocation. |
| Manual workflow | `.github/workflows/rollup-scorecards.yml:21` | Passes selection through environment variables and quoted shell-array arguments; defaults remain local-build/manual-only. |

The provider's `ZyteBudget.take` spends before each attempted provider request;
both routes share that counter. `ZyteTransport.handle_request` permits only GET,
refuses a changed resolved URL, and distinguishes `browserHtml` from
`httpResponseBody`. `BoundedHttpCapture.capture` recognizes target 401/403 before
body parsing and raises `CredentialRefusedError`. Provider errors are replaced by
a fixed public diagnostic, avoiding reflected secret text. Source capture remains
subject to the immutable source evidence policy.

The focused acquisition/refresh suite passed 40 tests. Its new cases cover mixed
body/DOM provenance, the shared budget, non-API routes, mode conflicts and injected
factory conflicts (`tests/test_scorecard_acquisition.py:130–169`). Independent
injected checks additionally exercised browser API responses under hash-only and
metadata-only policies, sealed and verified both evidence artifacts, and scanned
all artifact members: no response bytes or secret appeared; the raw response
digest was also absent under metadata-only. Browser target 401 and 403 each
raised the typed refusal after exactly one provider attempt.

The budget is a total provider-call ceiling per publisher context, not a global
invocation or per-minute rate limit. Request spacing belongs to each mode's
bounded client. These limits are explicit and do not imply source completeness.

Coverage is adequate for the new transport branch. Confidence is high for the
reviewed selection, provenance, secrecy and refusal paths. Remaining admission
work belongs to the publisher-specific parser and retained live qualification.
