# Scorecards stopping point — October 4, 2026

Work stopped at the user's request. Reviewed implementation is committed and
pushed. The latest retained-source batch is preserved but **not prepared or
published**. All parallel workers stopped and froze their results.

The [machine-readable handoff](stopping_point_20261004.json) pins the current
publication, runtime, interrupted attempt and review packets. Paths below are
relative to its `campaign_root`; original publisher and model bodies remain
outside the repositories.

## Delivered

| Area | Completed work | Release state |
|---|---|---|
| SpicyDocs | National Farmers Union's archived 2016 tables; SACE's named regional PDFs; Reproductive Freedom for All's 2024/2025 originals; AFGE's 2022/2023 originals with grouped actions and member notes | Reviewed, registered, documented and pushed to `mikewolfd/spicy-docs` `main` at `a477f80ee4482f2ab2267ecc4dd9cfe8cbe9a14c` |
| Shared PDF checks | AFGE, SACE and Reproductive Freedom reuse `require_reviewed_pdf`, existing URL checks and `check_pdf_bytes`; source-specific trailer windows and refusal messages remain exact | Committed and pushed with the readers; independent full-table replay and compatibility checks passed |
| SpicyRegs ratings | Exact `DECIMAL(38,19)` ratings, explicit legacy/current policy admission and one shared indexed-family reader for prior publication inputs | Tested and pushed at `57693645a0354f4a0cebfdf04a8d33601676aca4` on `scorecards-integration-20261004` |
| Publisher API inventory | Added original-client and archived FreedomWorks JSON observations | Discovery metadata only; leadership cards and party charts do not establish complete member coverage or a working current API |

The [SpicyDocs source guides](../../../../../../spicy-docs/docs/README.md) describe
the named source boundaries. AFGE keeps conflicting grid/prose roll numbers,
member-specific credit and blanks. Reproductive Freedom keeps vacancies,
historical context and undisclosed party values. National Farmers Union keeps
malformed cells and contradictory state/party observations. No outside identity
record repairs these publisher facts.

The clean SpicyDocs release checkout passed its full repository gate; see
`afge/root-producer-gate-v4-indexed-guides.log`. Consumer runtime verification is
retained in `etl-conversion-review/root-full-tests-v3-indexed-reader.log`.
Earlier failed checks and immutable source references remain available.

## Published data and the interrupted batch

The public index was read again at this checkpoint. Its selected scorecards
generation remains
`sha256:4508fa881535786411d6cf3d3e769e7a432a1bb1ea88cc149fc5d48a51a0a81a`.
It reports **175 editions from 39 publishers**. These are measurements of the
pinned checkpoint, not maintained coverage totals. See
`stopping-point-20261004/checkpoint.json` and its retained public index.

**League of Conservation Voters is included:**
[`lcv:2025`](qualifications/lcv--2025--published.json) is published. Its reader
reconciles original CSV exports, both chamber HTML tables and vote details.
Remaining LCV archives require separate backfill qualification.

The next batch completed every selected retained-source acquisition and wrote
native tables and ETL receipts. Physical metadata reports 210 editions from
44 publishers, but these files have **not completed preparation qualification**.
The run was interrupted at the user's request during reconstruction of the
current receipt-backed family, exited 130 and produced no `preparation.json`.
It made no publisher network requests and retained no publisher/model body in
public evidence. Published scorecards were not replaced.

Preserved inputs and outputs:

- Plan: `package/frozen-ila-details-expansion-20261004/plan-v2-reference-bound.json`;
  SHA-256 `2dc261657b5f5c06bb54709bed8c6c2b243aa58ad4e4cd338506036a7b8fface`.
- Attempt: `integration-batch-8-native-profiles/candidate-v3-rating-precision/`.
- Log: `integration-batch-8-native-profiles/preparation-v3-rating-precision.log`.
- Private observations:
  `integration-batch-8-native-profiles/private-observations-v3-rating-precision/`.
- The checkpoint pins written files and the source journal. It does not turn
  interrupted files into a qualified generation. Retry uses fresh directories.

The installed consumer provider remains `0.53.0+scorecards.ff2797d34976`.
The newly pushed producer readers are not yet installed or published through
SpicyRegs.

## Pending source work

| Work | Preserved result | Next required step |
|---|---|---|
| Ready producer readers and retained packages | Reviewed AFGE, Peace Action, Boilermakers, Lugar/McCourt, Radical Middle, National Farmers Union, SACE and Reproductive Freedom inputs; see their campaign entry lists and source guides | Build one selective provider overlay, register named downstream scopes, replay and publish through the normal family pipeline |
| FreedomWorks archived print `/3` | Source-qualified private candidate retains every summary, the native scale and additional explicit amendment/concurrent-resolution occurrences | Finish independent changed-field reconciliation, format module/tests with an AST-equivalence proof, and replay every scope before adoption |
| FreedomWorks WordPress `/2` | Independent review found two source-fidelity issues: an unstated primary-metric designation and omitted score-tooltip occurrences | Preserve unknown `is_primary` as NULL; independently verify the private `/3` tooltip fix, then qualify a corrected complete rendition |
| Preparation reuse | Read-only audit confirms duplicate native-prior materialization and repeated family reconstruction | Measure and implement a bounded change while retaining exact file pins, policy/schema admission, receipt joins, witnesses, hashes and prior-scope checks |

FreedomWorks print `/3` also preserves native `Amdt. H/S` metadata spellings
literally; those spellings remain an explicitly documented unsupported typed
reference shape. Do not infer congressional IDs from them or from publisher
numeric member IDs.

The private WordPress `/3` correction addresses tooltip retention only. It does
not close the separate primary-metric finding. The print follow-up review is
explicitly incomplete. Their frozen handoffs and review hashes are listed in
the machine-readable handoff; none of these candidates is production support.

## Resume in this order

1. Address the verified duplicate preparation reads with a focused regression and
   a comparable benchmark. The audit has a proposal, not an implementation or
   a measured performance gain. Do not remove generation admission checks.
2. Run the pinned retained batch through
   [`prepare_scorecard_integrations.py`](../../../../../scripts/prepare_scorecard_integrations.py)
   using a freshly read public index and fresh output/private-observation paths.
   Preserve every interrupted and failed attempt. Require successful exit,
   exact source references, complete scopes, prior-row preservation and a
   verified `preparation.json`.
3. Commit tested runtime/package changes, then use
   [`publish_candidate.py`](deployment/publish_candidate.py) and
   [`readback_candidate.py`](deployment/readback_candidate.py). Require atomic
   publication, matching public pins, counts, schemas, publisher attribution
   and hosted MCP checks before advancing the publication ledger. Follow the
   [reproduction guide](README.md) for exact commands.
4. Build the next reviewed provider overlay. The shared PDF wrappers require
   `spicy_docs/reading/pdf_bytes.py` at SHA-256
   `889dc357391a000f5857516c8eca7fd75f44607791247e98d1fb80c31e925b4c`.
   The current overlay builder only selects scorecard modules; extend that
   existing builder to admit this explicit reviewed runtime dependency and
   prove all unrelated baseline bytes remain unchanged. Old-primitive refusal
   proofs are retained; importing a wrapper alone is insufficient.
5. Close both FreedomWorks review threads before adopting their readers.
   Regenerate bounded qualification metadata and the work queue from accepted
   receipts rather than editing generated statuses by hand.
6. Rebuild member/item links in the separate `scorecard-analysis` family against
   the accepted source generation and existing congressional datasets. Report
   unresolved/ambiguous rows; exact versioned rules remain authoritative.
7. Continue the remaining publishers and historical renditions in the
   [generated census queue](integration_progress.md). Enable scheduled live
   refresh only after routes, completeness and required private assets qualify.
   An unavailable route does not establish retirement.

The primary checkouts retain unrelated FEC, Senate-expenditure and package work.
The SpicyDocs primary branch and pushed release history have different bases;
the clean release checkout is `source-push-provider-v2/`. Integrate against
the pushed history without resetting mixed changes or force-pushing the older
primary branch. Unadopted FreedomWorks files remain local drafts with known
findings; use their frozen private handoffs for follow-up.
