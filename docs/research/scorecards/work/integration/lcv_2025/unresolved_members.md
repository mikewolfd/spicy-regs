# Why the LCV members remain unresolved

All 62 unresolved rows were checked against the original LCV CSV and HTML, the retained congressional identity JSON, and the pinned member/term Parquet. The existing links remain unchanged. Candidate names below explain the mismatch; district-based diagnostic candidates are not admitted identity links.

| Reason | Members |
| --- | ---: |
| No retained term overlaps 2025 | 4 |
| Exact full name or nickname exists in retained JSON but is omitted from the member name index | 31 |
| Diacritic difference from the indexed primary name | 7 |
| Nickname appears explicitly in parentheses in retained first name | 2 |
| Different or fewer name components; no complete indexed alias | 5 |
| Different given name with no matching alias in retained name fields | 12 |
| Unexplained source-name discrepancy | 1 |
| **Total** | **62** |

The current resolver found no ambiguous exact matches: 58 rows have no exact-name candidate, and four have an exact-name candidate rejected by historical terms. An empty exact-name result does not mean the person is absent from the underlying records. Forty-three rows also report a prefixed district such as `NJ-11`; that diagnostic is not the sole cause of their unresolved status.

## No retained term overlaps 2025

| LCV name | Retained candidate and name evidence | Why it stays unresolved |
| --- | --- | --- |
| Alan Armstrong | `A000383` — Alan Armstrong | First retained term starts 2026-03-24. |
| Analilia Mejia | `M001246` — Analilia Mejia | First retained term starts 2026-04-20. |
| Christian Menefee | `M001245` — Christian Menefee | First retained term starts 2026-02-02. |
| Clay Fuller | `F000485` — Clay Fuller | First retained term starts 2026-04-14. |

## Exact full name or nickname exists in retained JSON but is omitted from the member name index

| LCV name | Retained candidate and name evidence | Why it stays unresolved |
| --- | --- | --- |
| Alex Padilla | `P000145` — Alejandro Padilla; nickname `Alex`, full name `Alex Padilla` | Source-stated full/nickname form is not in the current index. |
| Anna Paulina Luna | `L000596` — Anna Luna; full name `Anna Paulina Luna` | Source-stated full/nickname form is not in the current index. |
| Ben Ray Luján | `L000570` — Ben Luján; full name `Ben Ray Luján` | Source-stated full/nickname form is not in the current index. |
| Bernie Sanders | `S000033` — Bernard Sanders; nickname `Bernie`, full name `Bernard Sanders` | Source-stated full/nickname form is not in the current index. |
| Brad Schneider | `S001190` — Bradley Schneider; nickname `Brad`, full name `Bradley Scott Schneider` | Source-stated full/nickname form is not in the current index. |
| Buddy Carter | `C001103` — Earl Carter; nickname `Buddy`, full name `Earl L. "Buddy" Carter` | Source-stated full/nickname form is not in the current index. |
| Burgess Owens | `O000086` — Clarence Owens; nickname `Burgess`, full name `Burgess Owens` | Source-stated full/nickname form is not in the current index. |
| Chuck Grassley | `G000386` — Charles Grassley; nickname `Chuck`, full name `Chuck Grassley` | Source-stated full/nickname form is not in the current index. |
| Chuck Edwards | `E000246` — Charles (Chuck) Edwards; full name `Chuck Edwards` | Source-stated full/nickname form is not in the current index. |
| Chuck Fleischmann | `F000459` — Charles Fleischmann; nickname `Chuck`, full name `Charles J. "Chuck" Fleischmann` | Source-stated full/nickname form is not in the current index. |
| Chuck Schumer | `S000148` — Charles Schumer; nickname `Chuck`, full name `Charles E. Schumer` | Source-stated full/nickname form is not in the current index. |
| David McCormick | `M001243` — Dave McCormick; full name `David McCormick` | Source-stated full/nickname form is not in the current index. |
| Ed Markey | `M000133` — Edward Markey; nickname `Ed`, full name `Edward J. Markey` | Source-stated full/nickname form is not in the current index. |
| Gabe Vasquez | `V000136` — Gabriel (Gabe) Vasquez; full name `Gabe Vasquez` | Source-stated full/nickname form is not in the current index. |
| Greg Casar | `C001131` — Gregorio Casar; full name `Greg Casar` | Source-stated full/nickname form is not in the current index. |
| Hank Johnson | `J000288` — Henry Johnson; nickname `Hank`, full name `Henry C. "Hank" Johnson, Jr.` | Source-stated full/nickname form is not in the current index. |
| J.D. Vance | `V000137` — James David Vance; nickname `J.D.`, full name `J.D. Vance` | Source-stated full/nickname form is not in the current index. |
| Jack Reed | `R000122` — John Reed; nickname `Jack`, full name `Jack Reed` | Source-stated full/nickname form is not in the current index. |
| Jan Schakowsky | `S001145` — Janice Schakowsky; nickname `Jan`, full name `Janice D. Schakowsky` | Source-stated full/nickname form is not in the current index. |
| Jim Himes | `H001047` — James Himes; nickname `Jim`, full name `James A. Himes` | Source-stated full/nickname form is not in the current index. |
| Jim McGovern | `M000312` — James McGovern; nickname `Jim`, full name `James P. McGovern` | Source-stated full/nickname form is not in the current index. |
| Kat Cammack | `C001039` — Katherine Cammack; nickname `Kat`, full name `Kat Cammack` | Source-stated full/nickname form is not in the current index. |
| Mike Simpson | `S001148` — Michael Simpson; nickname `Mike`, full name `Michael K. Simpson` | Source-stated full/nickname form is not in the current index. |
| Nick LaLota | `L000598` — Nicolas LaLota; full name `Nick LaLota` | Source-stated full/nickname form is not in the current index. |
| Nikki Budzinski | `B001315` — Nicole (Nikki) Budzinski; full name `Nikki Budzinski` | Source-stated full/nickname form is not in the current index. |
| Pat Fallon | `F000246` — Patrick Fallon; nickname `Pat`, full name `Pat Fallon` | Source-stated full/nickname form is not in the current index. |
| Randy Fine | `F000484` — Randall Fine; nickname `Randy`, full name `Randy Fine` | Source-stated full/nickname form is not in the current index. |
| Rick Crawford | `C001087` — Eric Crawford; nickname `Rick`, full name `Eric A. "Rick" Crawford` | Source-stated full/nickname form is not in the current index. |
| Scott Franklin | `F000472` — C. Franklin; full name `Scott Franklin` | Source-stated full/nickname form is not in the current index. |
| Shelley Moore Capito | `C001047` — Shelley Capito; full name `Shelley Moore Capito` | Source-stated full/nickname form is not in the current index. |
| Tony Gonzales | `G000594` — Ernest Gonzales; nickname `Tony`, full name `Tony Gonzales` | Source-stated full/nickname form is not in the current index. |

## Diacritic difference from the indexed primary name

| LCV name | Retained candidate and name evidence | Why it stays unresolved |
| --- | --- | --- |
| Andre Carson | `C001072` — André Carson | Exact normalization preserves accents; the names differ. |
| Carlos Giménez | `G000593` — Carlos Gimenez | Exact normalization preserves accents; the names differ. |
| Linda Sanchez | `S001156` — Linda Sánchez | Exact normalization preserves accents; the names differ. |
| María  Salazar | `S000168` — Maria Salazar | Exact normalization preserves accents; the names differ. |
| Mario Díaz-Balart | `D000600` — Mario Diaz-Balart | Exact normalization preserves accents; the names differ. |
| Raul Grijalva | `G000551` — Raúl Grijalva | Exact normalization preserves accents; the names differ. |
| Teresa Leger Fernández | `L000273` — Teresa Leger Fernandez | Exact normalization preserves accents; the names differ. |

## Nickname appears explicitly in parentheses in retained first name

| LCV name | Retained candidate and name evidence | Why it stays unresolved |
| --- | --- | --- |
| Jim Moylan | `M001219` — James (Jim) Moylan | Explicit parenthetical form is not indexed. |
| Zach Nunn | `N000193` — Zachary (Zach) Nunn | Explicit parenthetical form is not indexed. |

## Different or fewer name components; no complete indexed alias

| LCV name | Retained candidate and name evidence | Why it stays unresolved |
| --- | --- | --- |
| Amata Radewagen | `R000600` — Aumua Amata Radewagen; full name `Aumua Amata Coleman Radewagen` | Different or omitted name components; no complete exact alias. |
| French Hill | `H001072` — J. Hill; full name `J. French Hill` | Different or omitted name components; no complete exact alias. |
| James Justice | `J000312` — Jim Justice; full name `James C. Justice` | Different or omitted name components; no complete exact alias. |
| Morgan Griffith | `G000568` — H. Griffith; full name `H. Morgan Griffith` | Different or omitted name components; no complete exact alias. |
| Pablo Hernández | `H001103` — Pablo José Hernández Rivera; full name `Pablo José Hernández` | Different or omitted name components; no complete exact alias. |

## Different given name with no matching alias in retained name fields

| LCV name | Retained candidate and name evidence | Why it stays unresolved |
| --- | --- | --- |
| Bill Keating | `K000375` — William Keating | Given-name variant is not explicitly recorded as a matching alias. |
| Bob Latta | `L000566` — Robert Latta | Given-name variant is not explicitly recorded as a matching alias. |
| Chris Coons | `C001088` — Christopher Coons | Given-name variant is not explicitly recorded as a matching alias. |
| Chris Murphy | `M001169` — Christopher Murphy | Given-name variant is not explicitly recorded as a matching alias. |
| Dick Durbin | `D000563` — Richard Durbin | Given-name variant is not explicitly recorded as a matching alias. |
| Greg Murphy | `M001210` — Gregory Murphy | Given-name variant is not explicitly recorded as a matching alias. |
| Greg Steube | `S001214` — W. Steube | Given-name variant is not explicitly recorded as a matching alias. |
| Jim Risch | `R000584` — James Risch | Given-name variant is not explicitly recorded as a matching alias. |
| Joe Morelle | `M001206` — Joseph Morelle | Given-name variant is not explicitly recorded as a matching alias. |
| Nick Langworthy | `L000600` — Nicholas Langworthy | Given-name variant is not explicitly recorded as a matching alias. |
| Rob Wittman | `W000804` — Robert Wittman | Given-name variant is not explicitly recorded as a matching alias. |
| Tom Tiffany | `T000165` — Thomas Tiffany | Given-name variant is not explicitly recorded as a matching alias. |

## Unexplained source-name discrepancy

| LCV name | Retained candidate and name evidence | Why it stays unresolved |
| --- | --- | --- |
| William Ogles | `O000175` — Andrew Ogles | Both original LCV renditions say William; retained name fields provide no William alias. |

## Proposed next steps

- Preserve primary name.middle, name.nickname and name.official_full from the already retained congressional identity source; index exact source-stated forms with historical term context and collision refusal. A proposal, not an applied change. Names and raw observations must remain source-faithful; never generate common nickname dictionaries.
- Consider explicit source-specific parsing of parenthetical name forms for Jim Moylan and Zach Nunn, with literal source fields retained. Do not strip arbitrary parenthetical text or infer unrelated aliases.
- Evaluate a versioned diacritic-folded exact comparison over the full pinned name corpus, requiring unique historical context and preserving the original accented values. Measure candidate collisions before admission. This diagnostic group does not authorize automatic matching.
- Parse a state-prefixed district only when the prefix exactly agrees with the independently stated source state; preserve the literal district. 43 unresolved rows carry this diagnostic, but name or term failures coexist. No claim that parsing districts alone resolves 43 people.
- Keep the four 2026-only members unresolved for the 2025 edition; investigate publisher roster-versus-rating period semantics before changing the temporal rule. Do not assign a 2025 historical term to a later member.
- For remaining name-component, unstated-name-variant and Ogles discrepancies, obtain explicit identifier or publisher-backed alias evidence before considering a versioned override. A shared district or plausible nickname is not sufficient identity evidence.

## Evidence and scope

[`unresolved_members.json`](unresolved_members.json) includes each raw CSV row number, original HTML locator, source hashes, the retained JSON pointer, literal name fields, qualifying term dates and current resolver diagnostics. The source observations are LCV’s original publications and the retained `unitedstates/congress-legislators` community identity source; this audit did not acquire new federal records or infer identity from a general nickname list.

Rerun `uv run --frozen --no-sync python docs/research/scorecards/work/integration/lcv_2025/unresolved_members_audit.py` against the explicitly named retained corpus. No resolver, publication, member link, or source rating was changed.
