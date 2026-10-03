# Independent AFP / IJM review

**APPROVE. No remaining implementation blockers at this checkpoint.**

Reviewed `afp.py`, `ijm.py`, `rds_bt50.py`, behavior-critical `common.py`
callees, and their focused tests. This was static review plus direct raw-body
readback; the adapter owner separately executed the qualification and tests.
The final qualification receipt reports 43 focused tests and installed-provider
replay. The reviewer verified that the installed
`0.53.0+scorecards.f0059f78a27f` helper matches the reviewed worktree bytes:

`rds_bt50.py` SHA-256:
`f00baae65044753ab3b358ce15b3774efad9f73fb8955f6bc765d362fe0e2905`.

## Raw evidence and corrected findings

Original bodies were read from private receipt directories
`scorecards-api-readers-2026-10-03/` and `scorecards-inventory-2026-10-03/` under
`~/Work/corpora/supply-2026-09-02/receipts/`.

- **Numeric weights:** AFP application bytes near offset 226105 distinguish a
  signed score from presentation-derived Support/Oppose/Neutral labels.
  `afp-detail-15878-first.body` preserves literal `voteRating` and vote values
  (SHA-256 `bc9049d7653cf90501fa7dc829fc2bed17356e42de35aacb400224f812cbf1cf`).
  The corrected helper leaves preferred-action fields null and keeps signed
  coefficients as weights with field locators (`rds_bt50.py:338`, `:396`).
- **Ungraded roster member:** AFP raw member bytes near offset 509462 contain
  Christian Menefee, native ID `30764`, `score_sets: []`, and state `usa`.
  The raw application retains its public roster array near offset 111741.
  `_members` now preserves this member and literal state, fetches its detail
  twice, and emits no fabricated rating (`:166`, `:324`).
- **Source disappearance:** Preserving ungraded rows initially weakened the
  no-current-ratings guard. `_members` now explicitly requires an actual
  selected score set (`:190`). Tests cover all-empty and historical-only score
  sets, so loss of current ratings cannot become a successful empty replacement.
- **Shared values and references:** Repeated shared items must agree on their
  coefficient, coefficient field and literal references (`:379`). A second
  member's differing weight refuses the edition. Exact score-set array indices
  replace predicate locators (`:329`). Final `referenceUrl` / `referenceUrlText`
  retention uses separate literal occurrences and exact field locators (`:351`);
  neither becomes a target or an inferred legislative identifier.

## Trace and coverage

| Trace | Verified behavior |
| --- | --- |
| Explicit adapters → `_site` (`rds_bt50.py:221`) | Original host, logged-out application, bounded captures, expected routes and browser pagination. |
| `_catalog` (`:81`) / `_members` (`:124`) / `_styles` (`:195`) | Current federal edition, unique IDs, declared bill count, retained roster context, supported display scope and typed score fields. |
| `acquire_current` (`:267`) → `item` (`:338`) | Distinct bill/vote/sponsor/cosponsor identities; multiple bill actions stay separate; conflicting shared facts refuse. |
| `_read` → `common.json_data` (`common.py:147`) | Decimal lexemes and nulls survive; malformed JSON and duplicate fields refuse. |
| `acquire_current` → `Bundle.finish` (`common.py:281`) | Every member detail and full catalog/configuration repeats unchanged; frozen row/FK/capture checks precede success. |

Read tests cover literal decimals/nulls, multiple actions, stable IDs, HTTP
failure, repeated-read drift, unsupported adjustments/display changes, ungraded
members, conflicting coefficients, references, and loss of all current ratings.
Coverage is adequate; confidence in the reviewed source semantics is high.

See [AFP qualification](afp_qualification.json) for the completed observed-source
replay and hashes. [IJM qualification](ijm_qualification.json) still records its
original-index HTTP 403; bounded older examples do not qualify a current full
snapshot. Stable repeated arrays prove observed-source closure, not a publisher
transaction or official congressional roster census. Production enablement,
historical editions, browser conversions and score reproduction remain outside
this approval.
