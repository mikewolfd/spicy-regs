# Enacted-law fixtures (the wheel's own)

Byte-identical copies of the fixtures spicy-docs ships in its `tests/fixtures/`
tree at the commits below (all ancestors of the v0.21.2 tag, `3642aa1`, that
built the vendored wheel), copied rather than re-fetched so the `laws` rollup's
tests run on the same captures the wheel's own tests prove the shapers on.
Each is either a complete, unchanged publisher response or a reduction stated
exactly in spicy-docs' README for its directory. The two Table III fixtures
are this repository's own captures, each stated in its row. The publisher's values are
U.S. government data in the public domain. Offline tests establish behavior
for these shapes; they say nothing about coverage or what the publisher
serves today.

| Fixture | spicy-docs path (commit) | Publisher object | Bytes | SHA-256 |
| --- | --- | --- | --- | --- |
| `congress-law-list.json` | `tests/fixtures/listings/` (`10c3139`) | GET `https://api.congress.gov/v3/law/119`, 2026-09-19: complete, unchanged response; 3 of a declared 108 | 2,411 | `66f548d8a9a9bed4dcf3787e94837d846c8c507fc2a91d32230df580a49085c6` |
| `congress-law-119-1.json` | `tests/fixtures/listings/` (`d5f3e50`) | One record copied verbatim from the 108-record first page of `law/119` (receipt `laws-contract-2026-09-19/law-119-p0.json`); Public Law 119-1 (S. 5), the law `plaw-119publ1.xml` states | 433 | `93af96bb6703463bce0f4aeed1013e8621f1a8b127ac1d71e09522634399f377` |
| `plaw-119publ1.xml` | `tests/fixtures/uslm/` (`7dec0c9`) | `PLAW-119publ1.xml` inside `https://www.govinfo.gov/bulkdata/PLAW/119/public/PLAW-119-public.zip`, publisher Last-Modified 2026-09-09 15:30:21 GMT; complete | 23,379 | `ee0e7a5d534411f78dd325405c42d386a1cfcf14f3870c93f5488874bb7acb54` |
| `classification-tables-index.shtml` | `tests/fixtures/uscode/` (`d5f3e50`) | `https://uscode.house.gov/classification/tables.shtml`, 2026-09-19 (39,140 bytes) minus its 27 KB navigation menu, everything else kept | 13,285 | `6a4fbfe2c5834745dfbaabd4418549e25bf3dca3b2ac25a63e3e7fc241cfcba0` |
| `classification-tbl119pl_2nd-head.htm` | `tests/fixtures/uscode/` (`d5f3e50`) | `https://uscode.house.gov/classification/tbl119pl_2nd.htm`, 2026-09-19 (115,140 bytes, 583 rows) through its column header plus 9 of the 583 data lines, closed; the session id replaced by `SESSIONID` | 13,941 | `e764ef99a725a3efaa00448b70fa3f03d1f7ad439e1f231b03a3d1725c4d30c8` |
| `table3-119_37.htm` | captured by this repository (`9818b2e`) | `https://uscode.house.gov/table3/119_37.htm`, 2026-09-25: complete; states release point 119-73 and 110 rows, four with a blank act section | 87,292 | `1e3fcbc7dbe87541c2183a50286567e2416074c4454c7ab707ce576f8b2e087d` |
| `table3-bulk-119-73-excerpt.xml` | cut by this repository (receipt `fork-execution-2026-09-21/table3-bulk-2026-09-26/build_fixture.py`) | `fulldump@119-73.xml` inside `https://uscode.house.gov/table3/table3-xml-bulk.zip`, 2026-09-26 (zip `93e1f233…`, member `985a9bb3…`): the 19 whole `<act>` fragments of 1789-08-07:9, 87-845, 118-2, 119-30, 119-37 and 119-53, verbatim and in the member's order, joined by newlines; the tests zip it as `fulldump@119-73.xml` | 52,728 | `26fc17dae9fc6ebf691497e49910d82eed38f7d71516f7cb3abe77b791be4d4f` |

The whole-corpus facts the tests do not establish — 108 laws on the route,
104 in PLAW bulk, 4 lagging (119-103, -104, -109, -110); 583 classification
rows in both orders, the same multiset — are receipted in
`corpora/supply-2026-09-02/receipts/laws-contract-2026-09-19/` and
`olrc-classification-2026-09-19/`; the capped live run of this rollup is in
`rollups-laws-rosters-2026-09-19/`.
