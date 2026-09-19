# Congress.gov hearing details

Two exact `hearing/{congress}/{chamber}/{jacket}` responses, the route
`build_committee_reports` reads `hearing_transcripts.event_id` from. Neither
was requested for this repository: both are copies of retained captures, with
the SHA-256 of the bytes as copied.

| File | Request | Bytes | SHA-256 | Provenance |
| --- | --- | --- | --- | --- |
| `hearing-119-house-63127.json` | GET https://api.congress.gov/v3/hearing/119/house/63127?format=json | 1,190 | `c1ec0ea3dc1443e92958aefebe2184e1ba49f72cd78f11599bff161f6c5c5fcb` | `~/Work/corpora/supply-2026-09-02/receipts/report-bill-linkage-2026-09-19/responses/detail-hearing-119-house-63127.json`, captured 2026-09-19 for the A2 linkage measurement. The jacket of the CHRG MODS fixture in `../govinfo_bodies/`. States **no** `associatedMeeting`: the honest NULL case. |
| `hearing-119-house-64431.json` | GET https://api.congress.gov/v3/hearing/119/house/64431 | 1,396 | `90187189e5e3d73ad8089eedace2aa5b31dd121f0f857309ad884f17c4c0e614` | spicy-docs `tests/fixtures/listings/congress-hearing-detail.json` (its README row, captured 2026-09-19 for the `hearing->meeting` edge). States `associatedMeeting.eventId` 119003. |

The api.data.gov key travelled only as an `X-Api-Key` header in both captures
and appears in neither body.
