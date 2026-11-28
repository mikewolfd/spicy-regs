# Committee-meeting archive parity — 2026-11-28 (ss-ikn9)

**No-go: keep the nightly rollup and the `committee-meetings` publication.** The bound archive covers most of the standalone generation, but would still lose meeting records, some document and witness associations, and historical read evidence. This audit does not authorize retirement.

[Comparison JSON and disposition ledger](committee-meetings-archive-comparison-2026-11-28.json) · [October baseline](committee-meetings-archive-comparison-2026-10-30.json)

## What matched

The live pointer returned generation `4df3158a0320…`, published **2026-11-27 06:44:56 UTC**. The comparator verified that generation's file hash, CT release `453351a3a57b…`, and the seven archive input hashes. It exited **1** with **3,563** residuals (the same count noted in ss-8odv). Counts below are for those snapshots, not the latest available CT release.

- **Meetings:** 6,160 of 6,182 matched by event ID (3,785 House, 2,303 Senate, 72 no-chamber). The archive had 18,756 meetings: all 12,596 archive-only meetings were either before the standalone date window (12,578) or undated (18).
- **Documents:** 61,166 of 62,857 distinct offered URLs resolved to an archive edition naming the offering event, directly or through a witness. Those hits cover 59,079 distinct editions and 60,795 event–edition pairs. Across the matched events, the archive has 68,404 distinct linked editions regardless of whether the standalone rollup offered them. The full archive has 209,284 meeting–edition pairs and 208,166 witness–edition pairs.
- **Witnesses and people:** 11,130 of 11,155 distinct upstream event/name keys have an exact normalized name match on an archive appearance for that event (including alternate forms using split rank and credentials, and noncanonical appearances). The matched events have 28,274 archive appearances (17,198 canonical), of which 28,256 resolve to 13,871 people. The standalone data does not contain person keys; this is archive join coverage, not a cross-source person identity match. Across the archive, 175,328 of 176,429 appearances resolve to 102,427 people. Senate meetings have 17,148 witness–edition pairs (11,833 distinct editions) and 9,038 distinct linked people with documents.

## Residual dispositions

These are **category occurrences**, not distinct losses. One event can appear under several headings. The JSON's `retirement_audit.residual_disposition` accounts for all **3,563**: **217 safe for the affected category**, **576 follow-up or explicit owner sign-off**, **2,770 blockers**. It includes the individual event IDs and unresolved event/URL pairs.

| Comparator category | Count | Disposition and evidence |
| --- | ---: | --- |
| Standalone meeting absent from CT | 22 | **Block.** 19 are dated Nov 30–Dec 3; 3 are older (338764, 338786, 338808). Refresh and reconcile both groups; not all are release lag. |
| Title | 140 | **139 safe:** case/Unicode whitespace only, including NBSP at 118654 and 119507. **1 block:** 338792 adds S.5289 and judicial nominations to a generic archive title. |
| Status | 1 | **Block:** 119601 is `Rescheduled` upstream, `Postponed` in CT. |
| UTC date versus archive date | 4 | **3 safe:** 116432, 116680, 118295 agree on start and Eastern calendar day. **1 block:** 119601 changed from Nov 19 to Dec 1. |
| Meeting detail | 57 | **44 safe:** archive adds 28 buildings and 16 rooms at the same start and update version (e.g. 115299 and 115382). Of the 13 newer upstream updates, **3 block** (new starts at 119601/338794, changed room at 119453) and **10 need follow-up** (same start; one archive-only building, nine unchanged normalized locations). |
| Hearing jacket sets | 1,362 | **1 safe for association:** 119212's jacket 64287 is on a same-event edition outside `meetings.transcripts`. **1,361 block:** 1,282 events have edition candidates but lack equivalent event links; 79 events have at least one jacket with no matching Congress/number package candidate. In value units: 1 linked, 1,780 candidates lacking same-event links, 82 with no candidate. |
| Video URL set | 1 | **Follow up:** 119575 has the exact YouTube ID in `recordings` (`youtube:R2JXs4FXjHw`), but CT meeting videos omit the upstream links, including the Congress.gov landing URL. |
| Bill ID set | 1 | **Block:** 338792 is missing `119-s-5289`. |
| Canonical witness names | 268 | **30 safe for names:** exact canonical name forms after omitting split rank/credentials (115367). **227 need follow-up:** name evidence survives through other appearances or person name variants (115459); confirm the replacement serves those names. **11 block:** 16 event/name values unverified (115975, for example). This does not establish exact position/organization parity. |
| Events with only standalone witnesses | 3 | **Block:** 119615/119616 are absent from CT (4/5 entries); 119575 is covered but missing all 4 entries. |
| Standalone document-bearing events with no archive meeting-key editions | 13 | **Block:** 12 House events offer URLs but have no direct or witness-linked edition (118252 offers 13); Senate 338792 has 2 URL-less entries and no linked edition. |
| Unresolved offered URLs | 1,691 | **Block:** 1,326 exact refs without a matching linked edition, 27 with no exact reference or matching edition. **338 need follow-up:** matching basename on a linked edition (e.g. 115363 → row 53065 on docs.house.gov), but the original URL doesn't resolve and a basename isn't proof of equal bytes. All 1,691 are Congress.gov URLs on 824 events. |

Zero: archive-only events within the standalone date window, chamber and meeting-type disagreements, loss-side committee/nomination/treaty set differences, unmapped top-level comparator columns, lookup self-check failures (550,031 exact source URLs checked), orphan document witness keys, unresolved non-null person keys, and dangling non-null witness event IDs.

**Two coverage limits matter for retirement.** The unchanged comparator adds nullable document counts; one null operand hides otherwise positive counts. Testing each count separately found **4,422** upstream document-bearing events, **443** with neither direct nor witness-linked editions (**430 more** than the comparator's 13). Of the 443, 21 have a listed archive document reference, not a linked edition; the other 422 have none. Also, CT's pinned `url_state` records 1,113 of the 1,691 unresolved URLs as `stored`, 133 as `request_failed`, 78 as HTTP 404, and has no exact record for 367. A historical capture status is not a working link in this release.

## State without a complete destination

The comparator's `state_without_destination` is a list of checks to make, not a statement that CT has no relevant fields. Both items have partial destinations in this pin, but neither is lossless.

| State | Verified coverage | Retirement gate |
| --- | --- | --- |
| `committee_meetings_detail_reads` and list-only/detail state | **379** published `read` attempts across **255** events, 28 generations. **214** match CT's selected successful and latest detail body SHA (115558, `journal:21`); **144** have a successful CT observation with a different or unwitnessed body (116301); **20** have no CT meeting (118863); **1** has unknown CT state (338820). Across all 6,160 matched meetings, CT reports **6,083 successful**, **77 unknown**, and **0** listing-only or failed. The native table has no `detail_read` column, and **5,927** standalone events have no published attempt; that is not evidence of list-only status. | **Block:** `congress_gov_detail_observation` does not retain the SpicyRegs attempt keys, timestamps, history or field-level states (1,874 stated, 1,841 source-not-stated, 65 stated-empty). Preserve the journal and receipt artifacts and provide a consumer path, or obtain explicit sign-off for their loss. |
| `meeting_documents` description, name and documentType | These map to `meetings.document_references[].{description,name,role}`, not `list.title`. Of **39,107** occurrences across 3,998 events, **37,407** match the full listed tuple; **1,678** have no URL and no matching tuple (115264, occurrence 5); **20** have a URL without a matching reference (118863); **2** share the URL but differ in metadata. CT adds a description at 118809 but replaces the upstream description and name at 119583. Matched/stated by field: description **9,581/10,684**, name **36,759/37,397**, type **37,409/39,107**. | **Block:** expose the three source fields through a replacement consumer path; retain or explicitly waive missing/changed occurrences, including URL-less entries. The derived document views do not project this tuple. |

**Before retirement:** refresh the CT pin, rerun the audit, reconcile the blockers (including the nullable-count cases), and secure explicit owner sign-off on any remaining exceptions, history retention, and consumer cutover. Do not infer byte or attribution parity from filename matches or global CHRG package hits. Leave the nightly workflow and the publication intact until then.

### Reproduction notes

```sh
uv run --frozen python scripts/compare_committee_meetings_archive.py \
  --cache .venv/meeting-audit \
  --inputs-from docs/evidence/committee-meetings-archive-comparison-2026-11-28.json \
  --output .venv/meeting-audit/replay.json
```

Omit `--inputs-from` for a live read. The HTTP fetches use the publication client's explicit `User-Agent: spicy-regs`. Replay reproduces the comparator portion of the JSON; `retirement_audit` is a separate inspection of the cached, digest-verified files. Supplementary member hashes are recorded there. The audit joins archive editions by `list_row` via `meeting_keys` or `witness_keys → witnesses.event_id`; compares journal rows by Congress/chamber/event and body SHA; and compares the listed document tuples as multisets by event, exact URL (including null), whitespace-normalized description/name/role, and case-insensitive format. Neither tuple parity nor receipt state was inferred from missing data.
