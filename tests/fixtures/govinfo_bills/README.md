# Bill fixtures (copied from spicy-docs)

Three files, copied byte-for-byte from
`spicy-docs/tests/fixtures/govinfo_bills/` at spicy-docs commit `ff92406`
(tag v0.21.0) on 2026-09-19. They exist so `tests/test_bill_family.py` can run
the family transform end to end with a stubbed acquirer and no network.

| File | What it is |
| --- | --- |
| `status-119hr6028.xml` | 119 HR 6028 BILLSTATUS, as reduced by spicy-docs (unrelated fields omitted, two actions and two subjects retained, summaries shortened to their first HTML paragraph, XML reformatted). Both offered text versions remain, which is what makes the consecutive pair below reachable from the status. |
| `text-119hr6028ih.xml` | The same bill's Introduced-in-House printing: identity fields and first section retained, other content omitted, XML reformatted. |
| `text-119hr6028eh.xml` | The same bill's Engrossed-in-House printing, reduced the same way. |

`ih` and `eh` are consecutive printings of one bill, which is why this pair and
not a single file: it is the smallest input that exercises `bill_sections`,
`section_diffs` and `section_diff_items` as well as the status-derived tables.
It does **not** exercise `financial_changes`: the pair changes no dollar
figure, so that table comes back empty, and a case for it would need a
different fixture.

**Provenance and what these do not establish.** spicy-docs retrieved all three
from GovInfo with unauthenticated GET on 2026-09-12; they are U.S. government
documents in the public domain. spicy-docs' own
`tests/fixtures/govinfo_bills/README.md` records the pre-reduction response
digests and the reduction applied to each. Because they are reduced, they
establish the *shape* the transform handles and nothing about coverage, size
distribution, or what GovInfo serves today. No credential appears in any of
them, and none is needed to read them.

Replace these only by re-copying from the spicy-docs release this repository
pins, so the two do not drift into different bytes under the same name.

## Native printings for order and section identity (2026-09-26)

Eight more files back `tests/test_bill_family_order.py`, which rebuilds the
tables spicy-docs published before 0.35.0 fixed two defects the 2026-09-26
qualification found in the bill family, and checks the next run repairs them
(receipt
`fork-execution-2026-09-21/drift-qualification-2026-09-26/bills-citations/`,
`results.json` → `tables.section_diffs` and `tables.bill_sections`). They are
native: whole where the table says so, and otherwise reduced by cutting whole
elements out of the native bytes, every other byte left as served. spicy-docs
0.35.0 carries the same bytes in its own `tests/fixtures/govinfo_bills/`. `.gitattributes` keeps them
byte-exact. All are U.S. government works in the public domain, read without a
credential.

| File | Bytes | SHA-256 | Source |
| --- | --- | --- | --- |
| `status-119hr983.xml` | 21,021 | `434f32c57fad34538e1409ba9dcbf21fae62b1189303df230602ecae56913008` | `https://www.govinfo.gov/bulkdata/BILLSTATUS/119/hr/BILLSTATUS-119hr983.xml`, byte-for-byte from the receipt's `native/` copy (read 2026-09-26T01:11Z). Its `Enrolled Bill` item has `<date/>` and is listed first; the `Public Law` item comes last. |
| `text-119hr983ih.xml` | 5,685 | `efb81403638be325bb68a53f1fc024331a7ab83b7d95a9d7b5e7a5efaeb5b3d9` | `https://www.govinfo.gov/content/pkg/BILLS-119hr983ih/xml/BILLS-119hr983ih.xml`, whole |
| `text-119hr983eh.xml` | 5,433 | `e7677f36f1fc09d8fe182f7f972887e3244567b2286821753c5e436e77902060` | `…/BILLS-119hr983eh/xml/BILLS-119hr983eh.xml`, whole |
| `text-119hr983rfs.xml` | 5,609 | `a0e5c4a6c0b989ca30ef583daef74dedd83ae5a37de6bbcc2968998b007e0e26` | `…/BILLS-119hr983rfs/xml/BILLS-119hr983rfs.xml`, whole |
| `text-119hr983enr.xml` | 5,544 | `b95447fdcbe46553d563f2a656e32f4b3a9861b92e4d75e57ba0812c5eb3a12a` | `…/BILLS-119hr983enr/xml/BILLS-119hr983enr.xml`, whole; identical to the receipt's `native/` copy (read 2026-09-26T01:07Z) |
| `text-119hr5334ih.xml` | 4,346 | `c456aa223105d0b36d54c9817d09d516bf55556679043abda30559201f1b85ea` | `…/BILLS-119hr5334ih/xml/BILLS-119hr5334ih.xml`, whole |
| `status-119hr5334.xml` | 4,440 | `064612eb7f2a7d2c0e13445627f70d4ad20b80ff665aed715574ddd0d71b41aa` | Reduced from `BILLSTATUS-119hr5334.xml` in the receipt's `native/` (108,169 bytes, `6c942ee61f174e587f964acebc70e8b611095c4be4edeab235654f90d4e21f29`, read 2026-09-26T01:11Z): the top-level `constitutionalAuthorityStatementText`, `committees`, `committeeReports`, `relatedBills`, `actions`, `cosponsors`, `cboCostEstimates`, `subjects`, `summaries`, `titles` and `amendments` elements are cut; `textVersions` is byte-identical to the native. |
| `text-119hr5334enr.xml` | 9,478 | `e71e46d361aab11f9d7ad6863dd90c558f007eb38a09e540f08c06179c7e2dbd` | Reduced from `…/BILLS-119hr5334enr/xml/BILLS-119hr5334enr.xml` (113,233 bytes, `50bf7a3c48fc607dee1cc800389c0e5afd682474aa3c399e2aa1e363d6096ca2`): Division A's two titles (`H42DEB9C…`, `H751A661…`) are cut, keeping Division A `Sec. 1` (`H7962367…`) and Division B `Sec. 1` (`H99FFDB5…`), the two sections that share one `bill_sections` identity. The pinned engine parses 10 sections from it; seqs 3 and 6 collide as the native's seqs 3 and 80 do. |

The bodies the receipt lacks were read keyless from GovInfo on 2026-09-26
(UTC). Each whole file's digest equals the `bill_versions.sha256` the audited
run published for that printing, so these are the bytes that run parsed; so
does the reduced enrolled printing's source digest. Replace them only from the
publisher, recording the new digests here.
