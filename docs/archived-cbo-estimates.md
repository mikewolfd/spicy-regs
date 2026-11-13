# Archived CBO estimates in the bill family

The ordinary bill-family builder emits `cbo_estimate_cards` beside
`cbo_cost_estimates` and `cbo_feed_items`. Join cards to the estimate index on
`publication_id` for source-stated bill links. Cards remain identifiable when
no bill link resolves. The shared provider schema generates the native columns,
dictionary and receipt policy; windows preserve their printed order and labels.

Set `BILL_FAMILY_CBO_ARCHIVE_MANIFEST` to an explicit retained selection. The
manifest pins one Congress's exact feed (`feed.sha256` and `feed.bytes`),
`crawls`, `patterns`, `index_receipt_keys` and page `receipt_keys`. Select these
from the source catalog. `SPICYGOV_RAW_ROOT` selects the central originals;
the builder's explicit `cbo_archive_root` argument supports local replay.

The builder validates originals and index answers, accounts for every selected
feed item in `cbo_archive_population.json`, and adds only cards from each
publication's latest indexed retained page. Its report keeps observed no-card
pages, refused reads, missing originals, unresolved PDF candidates and unread
indexes distinct. No manifest performs no archive read and preserves prior
cards. A broken supplied manifest fails rather than claiming an empty success.
The native merge preserves a newer prior card when given an older selection.

Permitted acquisition uses `cbo_archive_selection.acquire_missing_pages` with an
explicit, owner-budgeted `WebArchiveAcquirer`, `cbo_archive_evidence(CaptureEvidence(...))`
and an owned checkpoint. It validates retained WARC coordinates before requesting
missing pages. Completed captures and their source journal survive checkpoint
interruption; each checkpoint carries explicit receipt keys and owned journal
references. All new bytes go through `CaptureEvidence`; the small resume receipts
reference the same original without copying it. Credential refusal and budget
exhaustion abort with retained progress. Do not use this function without the
required acquisition/resource admission.

```sql
SELECT e.bill_id, c.publication_id, c.publication_date, c.publication_context,
       c.unit, w.period, w.spending_subject_to_appropriation_outlays_literal,
       w.spending_subject_to_appropriation_outlays_millions, c.answers.pay_go,
       c.flags.pay_go, c.card_source, c.source_sha256, c.source_locator
FROM cbo_estimate_cards c
JOIN cbo_cost_estimates e USING (publication_id), UNNEST(c.windows) AS t(w)
WHERE e.bill_id = '119-s-4429';
```

The retained 119th controls support this local query. Full source qualification,
PDF-only identity, all paired reader/model comparisons, shared gates and public
acceptance remain open under ss-7ovu and ss-938z. The workspace evidence and exact
pins are in `output/source-completion-20261113/lanes/cbo-estimates/`.
