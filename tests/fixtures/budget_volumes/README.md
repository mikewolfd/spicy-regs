# Budget-volume MODS fixture

One file, 6,072 bytes: the package MODS GovInfo served for `BUDGET-2026-MSR`,
retained by spicy-docs' PDF-family rollup on 2026-09-20 and copied here
byte-for-byte from `spicy-docs/tests/fixtures/budget_volumes/`. A U.S.
government record, public domain.

It is here and not imported because a wheel does not ship its test fixtures.
It is the *only* thing copied: the volume's extracted text, its summary JSON
and the second volume stay in spicy-docs, where the rules that read them are
established. What this repository tests with it is its own seam — that a
BUDGET package walks, fetches, shapes and merges — so it needs a MODS the real
`validate_package_mods` accepts under the real package-id grammar, and nothing
more.

`tests/test_print_citations.py` reads it. The body beside it in that test is a
synthesized PDF from `tests/pdf_fixtures.py`, not the publisher's 1.1 MB one:
what the print says is spicy-docs' measurement, and re-asserting it here would
be a second copy of the same claim.

| File | What it is | SHA-256 |
| --- | --- | --- |
| `mods-BUDGET-2026-MSR.xml` | Package MODS for the FY2026 Mid-Session Review | `05e2eb1ce0ae5d67dbad04c8ac2b53ba7c4feb294f71ab728473e995463f6c2e` |
