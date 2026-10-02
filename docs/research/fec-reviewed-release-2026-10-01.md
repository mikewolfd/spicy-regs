# Reviewed FEC release — October 1, 2026

The retained non-PDF FEC tables and their qualified views are available at
[the public MCP service](https://mcp.spicygov.ai/mcp). The
[completion receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/hosted-release-20261001/reviewed-release/completion.json) binds the independently observed
Cloudflare configuration and running image to successful local and public MCP
queries. This checkpoint supersedes earlier pending-consumer status statements.

## Reviewed changes and pins

The source, data-construction and consumer reviewers approved their scoped
changes after fixes. Full reports, function traces and limitations are retained
in [source review](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/hosted-release-20261001/reviews/source-review.md),
[data review](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/hosted-release-20261001/reviews/data-review.md) and
[consumer review](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/hosted-release-20261001/reviews/consumer-review.md).

- Source commit: `69964fe27c6ddc437e817041a3c5cca57a02723c`, merged into `mikewolfd/spicy-docs` fork `main`.
- Consumer build commit: `41b7121a6731a0d98e924fec9d7a44070be06ea3`.
- Deployment configuration commit: `8005e88da756b79922979d269be1a026c897cd73`.
- Image: `sha256:711b07bf5bc04bac5b3a2261f21ed7c8dd047ba7c8725c35f0c3186f19a596e3`.
- Receipt: [`sha256:447af67b642078d3c4e14b3f38df187600632ff4555971fd50ad4bd100ef53d1`](https://data.spicygov.ai/source-evidence/blobs/sha256/447af67b642078d3c4e14b3f38df187600632ff4555971fd50ad4bd100ef53d1).
- Cloudflare Worker version: `4775ea88-54b0-470f-acfd-df0e17fde15b`.

The source changes add regression tests for PostgreSQL decoder rejection and
cleanup, plus JSON-only API shape-refusal observations. Repeated source-wheel
builds are identical; their production package files match the previously
qualified source wheel. SpicyRegs pins the reviewed source wheel and lockfile.

The spreadsheet amount reader now rejects shared-string indices, booleans,
date serials and nonnumeric formula caches as money while retaining their raw
evidence. The [impact scan](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/hosted-release-20261001/reviews/workbook-cell-impact.json)
found no affected rows in the published retained workbook output. The table
generations therefore remain unchanged. Candidate-history publication wording
was corrected in its owning ledger and regenerated metadata. Deployment logs
and sampled traces are enabled.

## Verification and recovery

The [validation record](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/hosted-release-20261001/reviewed-release/validation.json) identifies executed checks:
the changed-scope suite, focused post-fix/source-repin checks, source non-PDF
tests, lint/type checks, dictionary consistency and Worker checks. Local full-source
repository tests would process PDF fixtures and were deliberately excluded; the
existing source fork CI passed independently after push.
The static reviews state their depth limits for generated mapping constants.

The exact image passed discovery, every qualified-view schema query, and
representative row/description checks through the standard MCP client, locally
and at the public endpoint. The receipt and image identities, required source
parents and responses agree. Exact-image compatibility checks also reject
missing/advanced parents, wrong image/policy and missing receipts, then accept
the original configuration. These negative checks use isolated configuration
and index copies; they are not a live production rollback or mutation drill.

The prior healthy release receipt
`sha256:6ae87a6ad5057c653641a16f345db8a7139090f78dd95e18dfffe1367f4dc554`
and image `sha256:1d16dad0e38cf84b5457d79fc353c9c62b973dbcdbbcd8dede8804c571e25615`
remain retained. Rollouts exposed a recurring native proxy error claiming the container was
not running; it also occurred with a single-platform image. Redeploying the Worker
after the image rollout restored service. The Worker now confirms that specific
channel contradiction with a bounded HEAD probe and resets the Durable Object,
without replaying a request. The [recovery review](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/hosted-release-20261001/reviews/container-recovery-review.md)
and mocked boundary tests cover that branch; no live firing of the reset guard
was observed. The final public smoke passed after redeployment. The platform
root cause remains unproven.

GitHub Actions in the fork validates code and publishes documentation; no MCP
deployment workflow was found. The authenticated Cloudflare build-trigger read
returned 403, so a dashboard-configured trigger was not ruled out. This release
was deployed explicitly with the project-local Wrangler and verified afterward.

## Remaining tasks

FR12 image/receipt binding and positive hosted FR13 acceptance are complete.
The live dependency/policy mutation and rollback drill remains open. FR11 full
restore remains required before FR14–FR15 source deletion; the earlier requested
small restore/replay smoke passed. Source originals are retained. PDF processing,
historical acquisition, current/net totals and unsupported amendment semantics
remain deferred or unqualified. This deployment does not claim those outcomes.
