# Committee and roster fixtures (the wheel's own)

Byte-identical copies of the fixtures spicy-docs ships in its `tests/fixtures/`
tree at the commits below (all ancestors of the v0.21.2 tag, `3642aa1`, that
built the vendored wheel), copied rather than re-fetched so the
`committee-rosters` rollup's tests run on the same captures the wheel's own
tests prove the readers and shapers on. Each is either a complete, unchanged
publisher response or a reduction stated exactly in spicy-docs' README for its
directory. The publisher's values are U.S. government data in the public
domain. Offline tests establish behavior for these shapes; they say nothing
about coverage or what the publisher serves today.

| Fixture | spicy-docs path (commit) | Publisher object | Bytes | SHA-256 |
| --- | --- | --- | --- | --- |
| `congress-committee-list.json` | `tests/fixtures/listings/` (`10c3139`) | GET `https://api.congress.gov/v3/committee/119`, 2026-09-19: complete, unchanged response; 3 of a declared 238 | 1,222 | `7b491b8a77905ca3afeb545258a118dea3e885c5207b099bc4b4e122c4bcebf0` |
| `congress-committee-hsju00-list-row.json` | `tests/fixtures/listings/` (`d5f3e50`) | One record copied verbatim from the 236-row first page of `committee/119` (receipt `roster-comparison-2026-09-19/committee-119-p0.json`); the House Judiciary list row the detail below folds onto | 1,556 | `332487c27d2e7142711237552b779d17fcb69243e0a2c5cdd04e3afc302b44fd` |
| `congress-committee-detail.json` | `tests/fixtures/listings/` (`10c3139`) | GET `https://api.congress.gov/v3/committee/house/hsju00`, 2026-09-19: complete, unchanged response; 15 subcommittees and its `history` | 4,978 | `90b7b4a9990812daa30ae378a54728999a8b553281920409d3ae013ba8436c30` |
| `memberdata-119-excerpt.xml` | `tests/fixtures/congress_rosters/` (`d5f3e50`) | `https://clerk.house.gov/xml/lists/MemberData.xml`, 2026-09-19 (556,936 bytes, `sha256:07aec659…cb18`): everything before `<members>`, five complete `<member>` elements (AK00, AL01, AL03, CA11 and the FL20 vacancy), and the complete `<committees>` block, closed | 47,942 | `e08fe24e5b524b6d5cefc98be01e95862089b78711c204010966b9f5ba8c945d` |
| `cvc-member-data-excerpt.xml` | `tests/fixtures/congress_rosters/` (`d5f3e50`) | `https://www.senate.gov/legislative/LIS_MEMBER/cvc_member_data.xml`, 2026-09-19 (67,618 bytes, `sha256:9dd64488…6d96`): everything through `<lastUpdate>`, six complete `<senator>` elements, closed | 3,847 | `e11c521410a81b5d9992314e84021a3ecffc9d438e832b9438e602b53d26683a` |

The whole-file facts the tests do not establish — 236 served of 238 declared
on the committee route, 2,516 House assignments, 450 Senate seats, 9
placeholders, the 555-vs-541 member comparison — are receipted in
`corpora/supply-2026-09-02/receipts/roster-comparison-2026-09-19/`; the capped
live run of this rollup is in `rollups-laws-rosters-2026-09-19/`.
