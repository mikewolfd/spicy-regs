# API reader qualification

[AFP qualification](afp_qualification.json) records a complete observed current
federal API scope, verified on 2026-10-03 through the installed SpicyDocs package.
The [capture manifest](afp_capture_manifest.json) retains source URLs, actual
observation times and hashes. It contains no original response bodies or headers.
The accepted roster includes the source's ungraded member without inventing a
rating. Both current member-detail passes and repeated catalogs agreed; every
emitted literal was compared independently with its raw JSON location.

[IJM qualification](ijm_qualification.json) records incomplete bulk acquisition
through Zyte. The original index, API configuration and catalog recovered after
its direct HTTP 403 refusal. Only part of the member-detail scope succeeded;
provider timeouts, HTTP 520/521 download failures and publisher HTTP 500 responses
prevented full capture. An isolated longer-timeout probe returned an explicit
website-ban failure. The [Zyte manifest](ijm_zyte_capture_manifest.json) separates
provider outcomes from target status and records `httpResponseBody` provenance.
The installed reader refused the partial corpus. IJM remains unqualified; this
does not establish that the publisher or scorecard is retired.

Neither result enables a registry entry or publishes data remotely. Historical
editions, browser-calculated lifetime scores and general catalog rationale are
outside the documented V1 mapping.

The private campaign is under
`~/Work/corpora/supply-2026-09-02/receipts/scorecards-api-readers-2026-10-03/`.
Run from `spicy-regs` to exercise the installed provider:

```sh
uv run --frozen --no-sync python ~/Work/corpora/supply-2026-09-02/receipts/scorecards-api-readers-2026-10-03/replay_afp.py
uv run --frozen --no-sync python ~/Work/corpora/supply-2026-09-02/receipts/scorecards-api-readers-2026-10-03/verify_afp.py
```

The first command validates the accepted tables against the frozen schema; the
second independently follows emitted source locations and reconciles raw array
membership and counts. Both read retained bytes, check their hashes and write
only to the private campaign. Synthetic parser tests are in
`spicy-docs/tests/test_scorecards_afp.py` and `test_scorecards_ijm.py`.
