# Generation audit: a retained HTTP 200 HTML body

`plaw-119pvtl1-http-200.html` is the exact body GovInfo served with HTTP 200 on
2026-09-25 for
`https://www.govinfo.gov/content/pkg/PLAW-119pvtl1/xml/PLAW-119pvtl1.xml`: a
"Page Not Found | GovInfo" page, 44,172 bytes, SHA-256
`ab214ce9f7a706cc4ee93703502f950222090c2547e47fafd75a299545919743`. The
request for `PLAW-119pvtl2.xml` returned the same bytes. It is retained
unmodified from
`~/Work/corpora/fork-execution-2026-09-21/parallel-rollup-audit-2026-09-25/congress-documents/laws/raw/`.

That audit's `laws_check.py` took the 200 status as evidence of acquisition;
its cross-review (`external-sources/cross-review.md` there) found both bodies
were HTML. `tests/test_generation_audit.py` keeps the audit tool from
accepting it again. The valid USLM for the same law is
`tests/fixtures/congress_laws/plaw-119pvtl1.xml`.
