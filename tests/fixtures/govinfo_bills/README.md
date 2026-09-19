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
`section_diffs`, `section_diff_items` and `financial_changes` as well as the
status-derived tables.

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
