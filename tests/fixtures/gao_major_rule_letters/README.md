# GAO major-rule letters

Product pages of GAO's major-rule reports, and one report's PDF, for `tests/test_gao_major_rule_letters.py` and
`tests/test_gao_reports.py`. `www.gao.gov` refuses plain clients, so the pages were captured through Zyte on
2026-10-04 (`~/Work/corpora/fork-execution-2026-09-21/gao-cra-major-rules/`: `letters-2026-10-04/` and
`early-2026-10-04/`). These U.S. government pages are public domain. A test builds a capture directory from them
(`receipts.jsonl` and `blobs/sha256/<hex>`), the shape `spicy_regs.sources.gao_major_rule_letters` reads.

The four cut pages are spicy-docs' own fixtures, unchanged (`tests/fixtures/gao_major_rule_letters/` at `02e9d63`):
each keeps the captured page's `<link rel="canonical">` and its View Decision block byte for byte inside a bare
wrapper. `ogc-01-6.html` and `ogc-01-6.pdf` are the publisher's bytes whole, since that page's Full Report link
is what the test reads.

| Fixture | Page | SHA-256 of the captured page | Here for |
| --- | --- | --- | --- |
| `gao-04-193r.html` (cut) | <https://www.gao.gov/products/gao-04-193r> | `9066dc98d5424919e52ecc7e8a58fd57c9beb188bf5cc01e8655eb3035d8e956` | a letter that states all three: agency, one RIN, one citation |
| `gao-01-300r.html` (cut) | <https://www.gao.gov/products/gao-01-300r> | `14ae937270e1ca56077d646c91cfb285df9a322c54b4b1ba5d96c6399999fb2a` | an FCC docket where the RIN would be: the RINs are NULL, `not-stated` |
| `ogc-97-44.html` (cut) | <https://www.gao.gov/products/ogc-97-44> | `656987a81597d7ac483159012fcd79a3499186d501e88c032dc7aac98943194a` | a report on two rules: two citations and one RIN, so the lists are not pairs |
| `b-330560.html` (cut) | <https://www.gao.gov/products/b-330560> | `4ff3b6cdca7832984b40e23f409db1ead99b3671c7c2884e1a83769b5f173141` | the page prints B-330546's letter: refused, `not-this-letter` |
| `ogc-01-6.html` (whole) | <https://www.gao.gov/products/ogc-01-6> | `30e5b1810913b74a6c97d87b4a1cbe87f325e98fa21370fe762cf070a18c1aad` | the page prints the letter with `801( a)( 2)( A)`, so no opening: the letter is read from its PDF |
| `ogc-01-6.pdf` (whole) | <https://www.gao.gov/assets/ogc-01-6.pdf> | `a42dc631b4b61d1d8e57e4b02d8fa2d52689d2900d24baba17e6b4ec29ff8650` | the Full Report that page links |
