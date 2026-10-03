# API reader qualification

[AFP qualification](afp_qualification.json) records a complete observed current
federal API scope, verified on 2026-10-03 through the installed SpicyDocs package.
The [capture manifest](afp_capture_manifest.json) retains source URLs, actual
observation times and hashes. It contains no original response bodies or headers.
The accepted roster includes the source's ungraded member without inventing a
rating. Both current member-detail passes and repeated catalogs agreed; every
emitted literal was compared independently with its raw JSON location.

[IJM qualification](ijm_qualification.json) records the complete observed current
federal API scope through Zyte. Both member-detail passes and repeated catalogs
agree. The [selected capture manifest](ijm_capture_manifest.json) distinguishes
original `httpResponseBody` bytes from retained `browserHtml`. The latter contains
the original API response as JSON in one rendered `body/pre`; emitted locations
explicitly identify that DOM rendition. Every emitted literal was independently
compared with the selected retained source. Ungraded source members remain members
without invented current ratings. Source grade thresholds and labels are retained
as methodology observations; individual browser grades are not calculated.

The [earlier incomplete qualification](ijm_native_incomplete_qualification.json)
and [original attempt manifest](ijm_zyte_capture_manifest.json) preserve the direct
403, native-response download failures, and correct partial-corpus refusal.
After renewed authorization, bounded rendered-response probes recovered the
remaining scope. The [complete attempt audit](ijm_zyte_attempt_manifest.json)
retains provider and source outcomes, and the [budget revision](ijm_budget_revision.json)
records the expanded finite request limit. No original response bodies or headers
are included in these public research files.

Qualification itself does not enable a registry entry or publish data remotely. Historical
editions, browser-calculated lifetime scores and general catalog rationale are
outside the documented V1 mapping.

The private campaign is under
`~/Work/corpora/supply-2026-09-02/receipts/scorecards-api-readers-2026-10-03/`.
Run from `spicy-regs` to exercise the installed provider:

```sh
uv run --frozen --no-sync python ~/Work/corpora/supply-2026-09-02/receipts/scorecards-api-readers-2026-10-03/replay_afp.py
uv run --frozen --no-sync python ~/Work/corpora/supply-2026-09-02/receipts/scorecards-api-readers-2026-10-03/verify_afp.py
uv run --frozen --no-sync python ~/Work/corpora/supply-2026-09-02/receipts/scorecards-ijm-zyte-2026-10-03/replay_ijm.py
uv run --frozen --no-sync python ~/Work/corpora/supply-2026-09-02/receipts/scorecards-ijm-zyte-2026-10-03/verify_ijm.py
```

The first command validates the accepted tables against the frozen schema; the
second independently follows emitted source locations and reconciles raw array
membership and counts. Both read retained bytes, check their hashes and write
only to the private campaign. Synthetic parser tests are in
`spicy-docs/tests/test_scorecards_afp.py` and `test_scorecards_ijm.py`.
