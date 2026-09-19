# GovInfo package MODS fixtures

Two complete, unchanged MODS responses, one per collection the
`committee-reports` rollup publishes. They exist so
`tests/test_committee_reports.py` can prove `bill_id` is read from the
package's own MODS — the metadata the transform already fetches for every
package, so the linkage costs no request — and that a hearing whose MODS
names bills only as body mentions publishes a NULL rather than one of them.
These U.S. government documents are public domain.

| File | Publisher response | Bytes | SHA-256 | Provenance |
| --- | --- | --- | --- | --- |
| `mods-CRPT-119hrpt1.xml` | [`packages/CRPT-119hrpt1/mods`](https://api.govinfo.gov/packages/CRPT-119hrpt1/mods) | 9,787 | `d73ea7b12140ca7e1ad08649092a9e14a432a9fce8948d8a4975e4f3cd43f9d2` | Copied byte-for-byte from spicy-docs `tests/fixtures/govinfo_bodies/` at tag v0.21.1 (`6f8d20e`), the release this repository pins; spicy-docs captured it 2026-09-19 with the key sent only as `X-Api-Key`. |
| `mods-CHRG-119hhrg63127.xml` | [`packages/CHRG-119hhrg63127/mods`](https://api.govinfo.gov/packages/CHRG-119hhrg63127/mods) | 33,283 | `0070c86ed50c50331a060d692957081d1feb41c04d82742c78765f3f77d66849` | Captured 2026-09-19 for this repository with `Accept-Encoding: identity` and the api.data.gov key sent only as `X-Api-Key`; the capture refused to write any body containing the key or an `api_key=` parameter, and this one contained neither. |

## What each one states about a bill

A GovInfo MODS carries the bills a package relates to as
`<extension><bill congress=".." type=".." number=".." context=".."/>`, and
the `context` attribute is the publisher's own statement of *how*:

- `CRPT-119hrpt1` (a four-page House Rules Committee report) lists four bills:
  H. Res. 53 as `PRIMARY` — the resolution the report accompanies — and
  H. Res. 53 again, S. 5 and H.R. 471 as `OTHER`. The `associatedBills`
  element beside them reads `H. Res. 53, 119th`. Measured live 2026-09-19 on
  the twelve newest reports the `committee-report/119` list route names, the
  `PRIMARY` bill agreed with the route's own `associatedBill[0]` on **12 of
  12**.
- `CHRG-119hhrg63127` (the House Homeland Security hearing *Worldwide Threats
  to the Homeland*) lists H.R. 1 and H.R. 5371, both as `BODY`: bills the
  transcript mentions, not a bill the hearing was held on. Over 52 hearings of
  the `hearing/119` route, **no CHRG MODS carried a `PRIMARY` bill** — 11
  carried `BODY` or `COVER` mentions, 38 entries in all, the most on any one
  hearing being 11 — and of the 12 hearings whose detail named a committee
  meeting, **none** of those meetings' `relatedItems.bills` named a bill. So
  `hearing_transcripts.bill_id` stays NULL, and this fixture is the case that
  proves a mention is not promoted to a linkage.

**Both measurements are retained**, with every request and every raw response,
at `~/Work/corpora/supply-2026-09-02/receipts/report-bill-linkage-2026-09-19/`
— 146 requests, with its own README stating the method and the per-package
verdicts. The restatements elsewhere in this repository (the transform
docstring, the two dictionary entries and `PLAN.md`) cite that receipt rather
than re-deriving the numbers.

Offline tests establish behavior for these shapes; they do not establish
coverage or continuing live availability. Replace the CRPT file only by
re-copying from the spicy-docs release this repository pins.
