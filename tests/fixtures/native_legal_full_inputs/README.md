# Native legal qualification fixtures

These files are exact copies of maintained `spicy-docs` fixtures at commit
`743288d0a2384bab19bbfbbe11d2371c82df5e72`. Keeping them here makes
`test_native_legal_full_inputs.py` runnable in a standalone `spicy-regs` checkout.
No fixture was downloaded or changed during this copy.

| File | Origin path in spicy-docs | SHA-256 | Provenance |
| --- | --- | --- | --- |
| `plaw-119publ77.xml` | `tests/fixtures/uslm/plaw-119publ77.xml` | `f2e78bf02fd1f5512906abf487ec07a15d9080cc651a49b01c839b549223b4e7` | Original GovInfo [PLAW-119publ77.xml](https://www.govinfo.gov/bulkdata/PLAW/119/public/PLAW-119publ77.xml), captured 2026-10-02; origin README records retained capture in `corpora/mcp-chaos-2026-10-02/round3/audit-ruth/`. |
| `constructed-bill-divisions.xml` | `tests/fixtures/congress_bill_tree/constructed-bill-divisions.xml` | `9a2e6a24f4536bbadc8883577e2119b6746be74d43bfd7f334fef6eec7e82df7` | Constructed bill fixture with invented identifiers, headings and amounts; it is not captured publisher data. |

The law fixture exercises native law identity with an unknown edition. The bill
fixture exercises the declared `BILLS-119hr4000ih` printing identity. These tests
check qualification and identity refusals; the constructed bill does not prove
publisher coverage or authenticity. Source provenance comes from the origin
`tests/fixtures/uslm/README.md` and `tests/fixtures/congress_bill_tree/README.md`.
