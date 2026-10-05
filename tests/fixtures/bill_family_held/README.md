# Held bill rows under an earlier stage rule

`e264e62b-rows.json` holds every `congress_bills` and `bill_actions` row of three bills exactly as bill family
`e264e62bfcc4…a594be` published them on 2026-10-03, before spicy-docs' round-6 stage rule: every column, every value,
in action order. They back `tests/test_bill_family_cbo.py`'s tests that a run re-reads a held row's stage and signing
date from its stored actions without reading the bill.

| Bill | As published (old rule) | What the running rule reads from the same actions |
| --- | --- | --- |
| 119-s-240 | `conference`, from the Library of Congress's "Resolving differences -- Senate actions: Senate agreed to the House amendment" (action 2) | `cleared`, from the Senate's agreement to the House amendment (action 1) |
| 119-hr-3377 (Private Law 119-1) | `signed_date` NULL, `no_public_law` | 2026-03-26, `private_law_and_became_law_action`, from E40000 (action 0) |
| 104-hr-517 (Public Law 104-11) | a detail-route row: no stored action, `stage` and `signed_date_rule` NULL | nothing: no rule reads a bill with no action |

Cut by `corpora/mcp-chaos-2026-10-02/round6/impl-W2/a/cut_fixture.py` from the round-6 audit's copies of the two
published files, each checked against its sha256 first: `bill_actions.parquet` `34a9065c27697d47…539c94c` and
`congress_bills.parquet` `741a04eb920a2ff0…2a6969`. The rows are U.S. government data in the public domain, as
Congress.gov's BILLSTATUS states them.
