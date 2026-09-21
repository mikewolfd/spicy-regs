# CFR regression evidence

Copied unchanged from the corresponding `spicy-docs/tests/fixtures/` files.
The list pages were captured on 2026-09-14. They contain two rows each and
have outstanding continuations; neither establishes a complete population.
Tests that alter their counts or continuations name that synthetic change.

| File | Source | SHA-256 |
| --- | --- | --- |
| `govinfo-published-cfr.json` | [GovInfo published CFR packages](https://api.govinfo.gov/published/2025-01-01/2025-01-31?offsetMark=*&pageSize=2&collection=CFR) | `812aad43c6741776ca6791a29ceaf507706c3e45f2e729c687b8438923a4bc9e` |
| `govinfo-package-granules.json` | [GovInfo 2025 Title 1 granules](https://api.govinfo.gov/packages/CFR-2025-title1-vol1/granules?offsetMark=*&pageSize=2) | `abcec8690f74d6741050e6dd505b59783cd9b9f4367bd5167dba54fe2a18d4ed` |

`ecfr-api-title14-numbering.xml` is the provider's reserialized excerpt from
a retained eCFR Title 14 request dated 2026-08-19. It preserves section
`19-8.1` beneath native Part `241`. It disproves a general rule that derives
part ancestry from the section prefix. It does not establish the ancestry of
a different annual edition, so the regression requires unknown ancestry to
remain unknown instead of substituting this eCFR part into an annual row.

The independent annual-ID counterexample and source provenance are in
`spicy-docs/docs/research/data-validation-regulatory-2026-09-21.md`, section
“CFR part identifiers are truncated,” and its retained `regulatory/supplement.json`.

`govinfo-title14-counterexamples.json` retains the complete annual
`CFR-2025-title14-vol4` package summary and two unchanged records selected
from the first native granule-list page captured on 2026-09-21. This combined
file is an authored excerpt, not an API response. The complete source traversal
received all 1,926 declared granules. Its raw captures, URLs, timestamps, and
before/raw/after comparison are retained under
`corpora/fork-execution-2026-09-21/cfr-title14-repair/`.

- Package summary SHA-256: `4d61be89ef4063411d3e33210d2ee780f8ca6358f32efbfed871fb27933f48ed`.
- First list-page SHA-256: `f192ae2a88bb4c1912ef90c68c6bc5d6996931907c48897e8e61ab40f42a02fa`.
- The publisher's package `lastModified` is `2025-06-17T21:29:08Z`, matching
  every row of this package in the pinned current public table. The list rows
  retain literal identifiers and headings but provide no parent part.
