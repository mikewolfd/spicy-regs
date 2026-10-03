# HRC 118th Congress: independent source inventory

The original PDF contains 100 named Senate rows and 440 named House rows,
including six delegates, plus one explicit House vacancy row. These are counts
independently checked against the rendered source, not publisher-declared
aggregate counts. House action results continue across each spread without
repeating member names. The vacancy and state breaks must remain in the row
alignment even if a later output omits the vacancy as a non-member.

This review inventories the source and its exceptions. It does **not** qualify
a complete cell transcription, a model response, a production reader, or a
published edition. See [the machine-readable inventory](independent_source_inventory.json)
for page counts, source integrity, and hashes of the private review artifacts.

## Evidence and method

Reviewed the [original publisher PDF](https://hrc-prod-requests.s3-us-west-2.amazonaws.com/files/documents/118th-Congressional-Scorecard-HRC-2024-Final.pdf)
retained by [capture_manifest.json](capture_manifest.json), observed on
2026-10-03 at 20:33:48 UTC. Its SHA-256 is
`804c1157e71da53a00821fdcaf15a1790448c682eb49d44613a577cfce9cb855`.

Existing SpicyDocs `DefaultReader` rendered the original pages. Every Senate
and House member spread and every item-description spread was inspected
visually. House endnotes were also read from 300 dpi crops. Native layout text
from `pypdf` supported name and count checks only; it was not used as action
glyph ground truth. Native text conflates filled and open circles and can omit
other marks. No model output served as reference evidence.

Raw PDF bytes, renders, crops, and diagnostic text remain private under
`~/Work/corpora/supply-2026-09-02/receipts/scorecards-hrc-2026-10-03/independent-source-inventory/`.
This report retains bounded observations and hashes under the existing
`hash_only` policy.

## Page boundaries and source counts

The PDF has 27 physical pages. Physical pages 2–26 each contain two printed
pages; printed left/right numbers are `2n−2` and `2n−1` for physical page `n`.
The covers are physical 1 and 27, printed 1 and 52.

| Physical page | Printed pages | Source content |
| --- | --- | --- |
| 2 | 2–3 | Introductory letter |
| 3 | 4–5 | Senate items: A–G left, H–O right |
| 4 | 6–7 | Senate: AL–GA, 20 members left; HI–MD, 20 right |
| 5 | 8–9 | Senate: MA–NJ, 20 left; NM–SC, 20 right |
| 6 | 10–11 | Senate: SD–WY, 20 left; endnotes 1–7 right |
| 7 | 12–13 | House items: A–F left, G–L right |
| 8 | 14–15 | House items: M–R left, S–Y right |
| 9 | 16–17 | House items: Z, AA–GG left; HH, II, JJ, KK–OO right |
| 10–26 | 18–51 | House grids, detailed below |

Senate halves contain **different members**, each with all three score periods
and all 15 item columns. Counting text lines across a physical spread can
incorrectly merge the two independent panels. The House uses the opposite
layout: each right half continues the same members' item results.

| Physical page | Named House rows | Vacancy rows | Visible state/district spans |
| --- | ---: | ---: | --- |
| 10 | 26 | 0 | AL 1–7; AK AL; AZ 1–9; AR 1–4; CA 1–5 |
| 11 | 31 | 0 | CA 6–36 |
| 12 | 29 | 0 | CA 37–52; CO 1–8; CT 1–5 |
| 13 | 29 | 0 | DE AL; FL 1–28 |
| 14 | 27 | 0 | GA 1–14; HI 1–2; ID 1–2; IL 1–9 |
| 15 | 27 | 0 | IL 10–17; IN 1–9; IA 1–4; KS 1–4; KY 1–2 |
| 16 | 27 | 0 | KY 3–6; LA 1–6; ME 1–2; MD 1–8; MA 1–7 |
| 17 | 27 | 0 | MA 8–9; MI 1–13; MN 1–8; MS 1–4 |
| 18 | 25 | 0 | MO 1–8; MT 1–2; NE 1–3; NV 1–4; NH 1–2; NJ 1–6 |
| 19 | 28 | 1 | NJ 7–12, including vacant 9; NM 1–3; NY 1–20 |
| 20 | 28 | 0 | NY 21–26; NC 1–14; ND AL; OH 1–7 |
| 21 | 28 | 0 | OH 8–15; OK 1–5; OR 1–6; PA 1–9 |
| 22 | 27 | 0 | PA 10–17; RI 1–2; SC 1–7; SD AL; TN 1–9 |
| 23 | 30 | 0 | TX 1–30 |
| 24 | 27 | 0 | TX 31–38; UT 1–4; VT AL; VA 1–11; WA 1–3 |
| 25 | 18 | 0 | WA 4–10; WV 1–2; WI 1–8; WY AL |
| 26 | 6 | 0 | American Samoa; DC; Guam; Northern Mariana Islands; Puerto Rico; Virgin Islands |

Each House left half has member identity, three score columns, and A–N
(14 items). Each right half has O–OO (27 items), with matching row positions
and state gaps. Item headers repeat on physical 10, 13, 16, 19, 22, 25, and 26;
intervening pages inherit their column context. Several pages begin mid-state.
No district has two named source rows in the inspected grids. This is the
publisher's displayed roster, not an exhaustive roster of everyone who served.

Physical 19 shows NJ district 9 as vacant, with three `N/A` scores and 41 blank
item cells. Its right-side blank band lies between the continuations of
Menendez and McIver. Physical 26 wraps Radewagen's name over two text lines;
it remains one source row. State headings, name wraps, and the vacancy therefore
require different treatment.

Expected grid positions are 300 Senate score cells and 1,500 Senate item cells;
1,320 named-House score cells and 18,040 named-House item cells; plus the vacancy's
three score cells and 41 blank item cells. These are coverage checks, not
nonblank counts or completed transcription results.

## Periods, marks, and item identity

The score columns state 118th, 117th, and 116th Congress. A Senate score's `^`
marks an earlier House score; preserve that chamber context with its period.
Member-name superscripts point to member endnotes and do not belong to score
values. `N/A` is a literal source value, with reasons supplied by particular
notes or institutional scope rather than one universal missing-value rule.

The legend distinguishes supported HRC's position, did not support, did not
vote, and present. Filled and open circles are different results. These marks
describe HRC's judgment; they do not automatically state an official Yea or Nay.
Senate endnote 6 explicitly explains that Schumer changed two official votes to
No for procedural reconsideration while HRC treated the positions as supportive.

Senate A–J are ten vote, procedural, or confirmation items; K–O are five
cosponsorship items. House A–JJ are 36 legislative-action items; KK–OO are five
cosponsorship items. Keep publisher item IDs separate when several actions
concern one bill. Senate C preserves an amendment-to-amendment citation.
House W and Senate J concern H.R. 5009 but have different dates and roll calls.
The displayed item counts do not establish a uniform scoring formula.

## Member-specific rules to preserve

House endnotes 1–24 appear below the delegate grid on physical 26. Their
member associations were checked against the source name superscripts:

| Notes | Members | Meaning for ingestion and later comparison |
| --- | --- | --- |
| 1, 4, 12, 13, 16, 17, 19, 22, 23 | Fong; Lopez; McIver; Suozzi; Kennedy; Rulli; Amo; Maloy; Wied | The stated mid-Congress entry leaves each ineligible for a score; retain the displayed member and item cells. |
| 2, 10 | Schiff; Kim | They joined the Senate in December but remain in the House display from the October publication. HRC credits their Senate H.R. 5009 votes toward their House scores. |
| 3, 6, 7, 8, 15, 21 | Porter; Dunn; Wasserman Schultz; Underwood; Tonko; Gonzales | Notes cite intended positions or Congressional Record statements about specific missed or cast votes. Preserve the source explanation separately from an official action. |
| 5 | Gaetz | His score remains after resignation because he was a member at the original October publication. |
| 9, 14 | Johnson; Jeffries | Scores use votes only. Jeffries still has two disclosed cosponsorships, which must not be assumed to participate in his score. |
| 11 | NJ district 9 vacancy | The note dates the vacancy following Pascrell's death. It is not a named-member observation. |
| 18, 24 | Evans; Radewagen | Significant missed votes make the member ineligible for a score. |
| 20 | Lee Carter | Late entry makes her ineligible; the source nevertheless discloses Equality Act cosponsorship. |

The delegate explanation distinguishes House-floor voting from cosponsorship
and Committee of the Whole participation. Thus a delegate can have an overall
score and many `N/A` item cells. Do not invent House-floor actions from them.

Senate endnotes on physical 6 additionally cover Butler and Ricketts entering
mid-Congress; Helmy's ineligibility; McConnell and Schumer having votes-only
scores; Schumer's procedural vote treatment; and Manchin's party change. The
source shows Schumer's cosponsorships despite his votes-only score.

## Remaining validation boundaries

The full result grid still needs source-based cell validation. Page and row
counts alone cannot establish mark fidelity, footnote transcription, or correct
member-to-item joins. The action and period context described here should become
independent checks against semantic extraction, with disagreements retained.
The vacancy requires an explicit downstream disposition before any complete
edition claim. Historical PDFs and publisher-wide completeness are outside
this review.
