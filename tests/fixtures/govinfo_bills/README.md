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

## Numbered reprints (2026-09-26)

Seventeen more files back `tests/test_bill_family_printings.py`, which rebuilds
the bill family generation `d380cdc0…` published before spicy-docs 0.37.0 gave
a numbered reprint its own code, and checks the next run repairs it (receipt
`fork-execution-2026-09-21/repeated-printings-2026-09-26/`, under
`~/Work/corpora`). All were read keyless from GovInfo on 2026-09-26 (UTC).

The three BILLSTATUS files and 119 HR 3426's six printings are byte-identical
to spicy-docs 0.37.0's `tests/fixtures/govinfo_bills/`, whose README records
their sources and digests: `status-119hr6644.xml` (reduced to its identity and
`textVersions`), `status-119hr3426.xml`, `status-118hr7643.xml` and
`text-119hr3426{ih,rh,eh1s,rfs,rhuc,rfs2}.xml` (complete and unchanged).

119 HR 6644's eight printings are reduced by the receipt's `reduce6644.py`,
which cuts whole elements and leaves every other byte as served. Each keeps its
first section and the first section of its first title (the first two for
`eas2`), without any table of contents, so the reprint keeps more sections than
the printing it follows, as the whole printings do (276 against 198). Each
source's digest equals the `bill_versions.sha256` generation `d380cdc0…`
published for it, except `eas2`, which that generation refused.

| Fixture | Bytes | SHA-256 | Source package (bytes, SHA-256) |
| --- | --- | --- | --- |
| `text-119hr6644ih.xml` | 18,665 | `b0e00a90c72dd52a3473f081a9a5ca0532d1261d316b5eeca0f045dd70049e20` | `BILLS-119hr6644ih` (234,162, `a2f7cfd04bfe69d391c3adee2d8e94592b67a5ff21bcdbdfae183173aa895c22`) |
| `text-119hr6644rh.xml` | 23,490 | `8b7b0035923919416cb38b1189d75eb71a2644e6965253d26ed13881474c7586` | `BILLS-119hr6644rh` (260,167, `f39bff03ec1a1582faa868bd5f6871e3f10675a4433130263e26ee7376fb0329`) |
| `text-119hr6644eh.xml` | 19,495 | `3b4a90e6fdd54fd7ec89d5218d4239d5e4fc301adcea95b309f1fcbdbb06c376` | `BILLS-119hr6644eh` (362,126, `99317586072b7117889ff6d2bbc9cc888cf52b4931356211788d28a166dd996b`) |
| `text-119hr6644pcs.xml` | 19,889 | `33744480b934d9e026d88da887038c23c0b24d2d3d64c6e5c308a7c3e2a09de7` | `BILLS-119hr6644pcs` (360,418, `3df5df67eb528f7042e33bed0d8fb99e7f23b2c188ab1d49176fd5e60fe56b7b`) |
| `text-119hr6644eas.xml` | 12,685 | `ff75f9c2d970d37ec61f4ee16ad66ca161895023bae537b425728c82f341021a` | `BILLS-119hr6644eas` (553,541, `f1c0f564c13d883b1c9ec0adc4d60b33ab876cadacaac31c6ba259eddfcbe1c8`) |
| `text-119hr6644eah.xml` | 12,868 | `d9b86cb4fbdbe181bc06a5b22f5263a83d1e8f827a7f6c29c2995122015f312a` | `BILLS-119hr6644eah` (561,415, `9b35145c0e9db247f0710e85064b7724413e58b66ca5c51b82885bcf0c82f16a`) |
| `text-119hr6644eas2.xml` | 18,307 | `b7bd3b907138980453729d71fbe7f304dcc5b481140a1cd9f700c763ab4131f1` | `BILLS-119hr6644eas2` (680,326, `2cb469edb93517edd8ef4434c054bc62381769c4c611c1acd1578e5f6f03c8e2`) |
| `text-119hr6644enr.xml` | 12,429 | `a4c3e8991492e683059b7487b3e68a449018c4e399550104cb4780747abfeeae` | `BILLS-119hr6644enr` (677,824, `874b8e04260ece8075b3d8a97af3ffa3820e0af46ec6115434e1a1f35ed03492`) |
