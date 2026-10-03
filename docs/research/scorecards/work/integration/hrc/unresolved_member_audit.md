# Why HRC members remain unresolved

The pinned final-118 analysis resolves **458 of 540 HRC members**. The remaining **82 (66 House, 16 Senate)** have no exact name candidate. All have recognized chamber/state/district/period context and compatible historical roster rows; the audit found display-name differences rather than a missing-term explanation. No matches were added.

The pinned `members` table exposes `name_first`, `name_last` and historical `other_names_json`. It does not expose middle, nickname, suffix or official-full-name fields. Every context-comparable roster row inspected for these unresolved names has NULL `other_names_json`. The exact resolver therefore lacks an accepted spelling for the source display names.

| Visible name difference | Rows | HRC display → context-comparable roster display |
| --- | ---: | --- |
| Different given-name spelling | 48 | Jim Himes → James Himes; Daniel Goldman → Dan Goldman |
| Additional given-name words | 11 | Eleanor Holmes Norton → Eleanor Norton; Ben Ray Luján → Ben Luján |
| Quoted or parenthetical nickname text | 9 | Nikki Budzinski → Nicole (Nikki) Budzinski; Gerald “Gerry” Connolly → Gerald Connolly |
| Roster given name is an initial | 6 | French Hill → J. Hill; Morgan Griffith → H. Griffith |
| Source surname includes a suffix | 6 | Frank Pallone Jr. → Frank Pallone; Robert Casey Jr. → Robert Casey |
| Diacritic difference | 2 | Teresa Leger Fernández → Teresa Leger Fernandez; Monica De La Cruz → Mónica De La Cruz |

These are diagnostic comparisons, not newly qualified identities. Categories are mutually exclusive for counting; some names have more than one visible difference. The [audit receipt](unresolved_member_audit.json) includes every unresolved source name grouped by cause, input hashes, the analysis generation and the comparison method. The audit used no network acquisition, fuzzy ranking, overrides or runtime changes.

The next exact-matching improvement is to preserve and qualify richer names from the existing member source, or add explicitly evidenced, versioned overrides. Treating a matching seat or a familiar nickname as proof by itself would exceed the current rules.

There is a separate **amendment coverage gap**. The pinned published `amendments` table contains only Congress 119. HRC cites 19 distinct Congress-118 amendment IDs absent from that table, producing 36 unresolved reference occurrences. Seventeen of those occurrences still retain an independently exact roll-call link; the missing amendment keeps the complete reference unresolved. The [analysis qualification](../deployment/hrc_analysis_qualification.json) records each missing ID. This audit did not acquire or rebuild congressional data.

This report describes analysis generation `4a99801a8a935e1dbe617ddaf219e959b32d46061fa196a36b693806121a8b46` and its captured inputs. Publication status is recorded separately by the deployment receipts.
