# Remaining publisher survey — 2026-10-05

Generated from pinned, completed task outputs. Do not edit this report directly.

Completed 6 tasks covering 48 distinct assigned publishers. The survey produced source candidates and catalog evidence; no new edition was qualified or published.

See [structured inventory](publisher_survey_20261005.json) for original URLs, response hashes, edition uncertainty, source shapes, access, rights, completeness, and task provenance. Original task reports and raw inputs remain private at their pinned paths.

The separately assigned work is excluded: `consumer_federation`, `dav`, `drum_major`, `farm_bureau`, `machinists`, `shriver`, `teamsters`.

## Recommended integration order

These priorities are implementation judgments based on the task evidence, not support status.

| Order | Publisher | Source and remaining work |
|---|---|---|
| 1 | American Bakers Association | VoterVoice catalog offers the 117th House and 116th House/Senate. Native score arrays and all criteria recovered; one 117th score is absent. Integrate 117th then 116th through existing VoterVoice readers with narrow bootstrap extension, exact tenant/stance checks, retained-ID reconciliation and rights decision. |
| 2 | Congressional Quarterly, Inc. | Original 2025 CQ Roll Call support and attendance tables recovered. Different denominators, blank cells and irregular headings require explicit handling. Build a narrowly scoped CQ 2025 presidential-support HTML reader first, using the captured House/Senate pair, source definitions, explicit footnotes and reviewed row counts. Independently qualify completeness and party formatting. Profile attendance separately because of malformed header styling, blank metrics and source spelling. Keep Datawrapper subset unsupported pending count discrepancy review. |
| 3 | Friends Committee on National Legislation | Four Quorum JSON/CSV pairs pass the existing decoder. Current trackers contain selected votes and sponsorships, with no aggregate score. Integrate a bounded FCNL selected-action reader using quorum_public.decode. Add explicit House District handling and preserve mixed periods/current cohort. Refuse automatic reconciliation of introductory SJ Res 83 versus sheet SJ Res 93. Preserve source stance separately from an inferred publisher rating. |
| 4 | Alaska Wilderness Action | 2015 original PDF recovered and sampled; archived 2016 offering remains unrecovered. Legacy Google sheet feed failed. Recover the named 2016 PDF if an alternate original rendition appears; otherwise qualify the retained 2015 PDF as an explicitly historical rendition. Qualify the complete 2015 PDF with retained page observations and independent review; preserve signed/out-of-range percentages, N/A, glyphs, weights, excusals and credit/debit actions. Inspect the later archived spreadsheet rendition before choosing a PDF-only reader. |
| 5 | Independent Petroleum Association of America | 119th Congress item evidence recovered through mplatform. Scores are computed in the publisher's browser; a complete displayed rating table was not retained. Define a narrow mplatform source shape and capture the publisher-rendered scores with explicit member/item reconciliation, or retain item evidence only. Do not recompute aggregate ratings. |
| 6 | United Brotherhood of Carpenters and Joiners of America | 2024 regional Carpenters issue-mark grid recovered. National publisher attribution is unresolved and OCR missed graphic cells. Resolve regional-versus-national publisher identity, then review all cells on physical page 12 with exact colored header/legend mapping and preserve no aggregate score. Register an unsupported-shape/profile record until those gates pass. |

## Shared patterns

| Pattern | Proven reuse and boundary |
|---|---|
| VoterVoice | American Bakers has actual scoring and criterion arrays. Reuse votervoice_public.py and existing readers; add only its observed /Home bootstrap variation. Campaign links from UCC prove no rating API. |
| Quorum public sheets | FCNL has matching public JSON/CSV payloads. Reuse quorum_public.py for row and column reconciliation; add explicit District syntax while keeping source-selected actions, periods and cohort specific. AIA/NAIFA action centers are discovery only. |
| mplatform/Momentum | IPAA and BIPAC have identical client bytes and tenant-specific routes. Share anonymous-authentication and bounded endpoint capture mechanics. IPAA scoring executes in the publisher client; BIPAC's empty array is not a completeness or retirement witness. |
| Native HTML tables | CQ needs explicit selectors, source row types and metric definitions. Reuse bounded capture and literal cell handling; AGC provides a completeness-check example, not reusable CQ selectors. |
| Reviewed PDF observations | Alaska and regional Carpenters reuse DefaultReader, Docling/OvisOCR2 and reviewed private observations. Use data-shaped Gemini outcomes when required. Native/OCR conversion alone is not a complete graphic-cell extraction. |
| Existing qualified source families | ARA, Peace Action and New American have attributable successor/current sources. Reuse their own readers and publisher IDs; store historical relationships separately. Human Events' ACU republication remains an editorial occurrence. |
| Archive and CMS discovery | Share bounded direct/Zyte captures, hashes, exact collection/item locators, literal links, and access metadata. CMS presence and archive holdings are not scorecard schemas. Keep original source-time publisher names, preliminary editions and errata separate. |
| Google Sheets/DataTables and Datawrapper | Alaska's legacy feed syntax is observed but rows are unavailable. CQ's versioned Datawrapper asset is a tab-separated subset with inconsistent counts. Preserve asset versions and raw delimiters; neither is a complete full-member grid. |

## Schema and identity findings

- Retain selected-action grids without an aggregate score. FCNL and regional Carpenters should not require invented ratings or official roll calls.
- Preserve signed percentages, values above 100, annual/lifetime measures, weighted items, credits, penalties and member excusals; Alaska disproves a universal 0–100 constraint.
- Keep metric definitions and denominators separate. CQ support, opposition, presidential-vote participation and general attendance are distinct measures; rounded values need not sum to 100.
- Treat campaign labels, member cohorts and item Congresses separately. FCNL's current roster and mixed-period items do not establish a historical-Congress snapshot; retain its SJ Res 83/93 disagreement.
- Record publisher, affiliate, successor, namesake and republisher relationships explicitly. A relationship never authorizes copying an edition into a second publisher scope.
- Candidate endorsements and election comparisons require an explicit candidate shape. Preserve them as documented scope decisions until supported; do not assign challengers congressional member identities.
- Retain source spelling, blanks, graphic symbols, footnotes, conflicting counts, preliminary editions and errata. CQ's Datawrapper subset remains unsupported pending review.
- Catalog dates describe holdings, not scorecard coverage. Record discovered, recovered, profiled, qualified and published stages separately; preserve unknown latest-publication and historical intervals.

## All surveyed publishers

Each next step comes from its pinned task report. Unknown coverage remains unknown.

### civic-issue

| Publisher | Finding | Next step |
|---|---|---|
| Citizens Committee for the Right to Keep and Bear Arms | Original news search recovered; the named congressional rating edition remains unidentified. | Find a dated original CCRKBA Congressional Ratings publication in newsletter archives; do not substitute NRA, GOA, ACU or other groups with similar subject matter. |
| Friends Committee on National Legislation | Four Quorum JSON/CSV pairs pass the existing decoder. Current trackers contain selected votes and sponsorships, with no aggregate score. | Integrate a bounded FCNL selected-action reader using quorum_public.decode. Add explicit House District handling and preserve mixed periods/current cohort. Refuse automatic reconciliation of introductory SJ Res 83 versus sheet SJ Res 93. Preserve source stance separately from an inferred publisher rating. |
| FlyersRights.org | Current advocacy and alternate WordPress archive verified. A negative scorecard search does not establish absence. | Resolve the catalog’s underlying attribution to a dated FlyersRights original; inspect a targeted archived news edition once located. Current and alternate-site negative searches do not establish nonpublication. |
| League of Women Voters of the United States | Current publisher verified; Political Accountability Rating survives as physical archival holdings. No post-2000 rating edition recovered. | Inspect national LWV archival publication indexes for post-2000 voting records; request a specific digitized original only if an edition is located. |
| March for Life, Inc. | 2020 Senate election comparisons recovered from March for Life Action. Sister-publisher identity and candidate scope need explicit decisions. | Create an explicit election-candidate comparison record and publisher mapping; retain 2020 Georgia runoff period as source wording. Qualify all linked Senate renditions and annotation-to-cell links before complete-series ingestion. |
| National Women's Political Caucus | 2026 endorsements and screening process recovered. Prospective candidates and state/local endorsements need a separate supported shape. | Record an unsupported election-endorsement shape; separately locate the historical voting chart. Do not assign an incumbent identity to challengers or merge state and federal cohorts. |
| National Organization for Women | Current search yields advocacy and externally authored voting reports; the historical NOW rating series remains unverified. | Follow the historical House/Senate Voting Record title in NOW publication archives; keep external organizations and House staff reports attributed to their actual authors. |
| United Church of Christ | Current action center uses VoterVoice campaigns and registration; no scorecard payload established. | Inspect original Washington office publication archives and a dated Voting Record. Historical UCC 1995 catalogue lead is retained in search-discovery.json; no pre-2000 extraction. |

### business

| Publisher | Finding | Next step |
|---|---|---|
| American Institute of Architects | Current Quorum advocacy verified; no public rating sheet found. Modern Punch List policy language does not establish the historical series. | Follow exact historical title and original post-2000 editions. Do not equate a modern policy punch list with a member scorecard. |
| American Bakers Association | VoterVoice catalog offers the 117th House and 116th House/Senate. Native score arrays and all criteria recovered; one 117th score is absent. | Integrate 117th then 116th through existing VoterVoice readers with narrow bootstrap extension, exact tenant/stance checks, retained-ID reconciliation and rights decision. |
| Atlantic Richfield Company Civic Action Program | Circa 1981–1982 civic-action packet is an archival lead. Current ARCO retail domain does not prove scorecard-program continuity. | Follow CRS source record and archival series for post-2000 original ratings, if any. Do not substitute PAC finance records or assume retirement. |
| Business-Industry Political Action Committee | Same mplatform client family as IPAA; its vote catalog returned an empty array. Historical Review of the Congress remains unrecovered. | Follow Review of the Congress provenance and authorized member-portal/archive access; do not substitute another mplatform tenant. |
| Independent Petroleum Association of America | 119th Congress item evidence recovered through mplatform. Scores are computed in the publisher's browser; a complete displayed rating table was not retained. | Define a narrow mplatform source shape and capture the publisher-rendered scores with explicit member/item reconciliation, or retain item evidence only. Do not recompute aggregate ratings. |
| National Association of Life Underwriters / NAIFA | NALU/NAIFA identity context and current Quorum advocacy verified; the historical Voting Report remains unrecovered. | Find original Voting Report or authorized member archive; verify federal-member rating shape before selecting a reader. |
| National Farmers Organization | Current magazine archive and September–October 2026 PDF recovered; no rating edition established. | Use narrow magazine index/page inspection if justified; trace historical series through finding aid. Avoid bulk magazine ingestion and pre-2000 extraction. |
| National Write Your Congressman | Member and vote routes lead to login; terms restrict automated copying and redistribution. Publisher authorization is required. | Obtain authorized publisher access and clarify ratings versus plain vote records before further acquisition. |

### labor

| Publisher | Finding | Next step |
|---|---|---|
| Amalgamated Clothing Workers of America | Cornell collection 5619/033 locates political files through 2002; collection dates do not establish scorecard coverage. Access is restricted. | Use collection 5619/033 and historical title to identify a precise folder/item through the archive; retain catalog-only lead until original publication is available. |
| United Brotherhood of Carpenters and Joiners of America | 2024 regional Carpenters issue-mark grid recovered. National publisher attribution is unresolved and OCR missed graphic cells. | Resolve regional-versus-national publisher identity, then review all cells on physical page 12 with exact colored header/legend mapping and preserve no aggregate score. Register an unsupported-shape/profile record until those gates pass. |
| Food and Beverage Trades Department AFL-CIO | Departmental catalog records recovered; the located press release is not the named voting record. | Use Cornell AUF 6046 box 470 folder 2 and Illinois vertical-file holdings as catalog leads to locate the actual Voting Record; source-specific syntax remains unknown. |
| Brotherhood of Railway, Airline and Steamship Clerks, Freight Handlers, Express and Station Employes | March 1974 Senate voting-record catalog lead; original table remains unrecovered. BRAC/TCU/IAM series continuity is unresolved. | Resolve current Cornell BRAC finding-aid location and locate the RCPL voting-record item. Record pre-2000 holdings only; pursue any distinct 2000-onward publication without substituting IAM. |
| Retail Clerks International Union | UFCW original history verifies organizational succession; no original Retail Clerks rating edition recovered. | Add evidence-backed identity review note and locate exact historical title in archival holdings; require explicit attribution before considering any successor rating edition. |
| United Mine Workers of America | Policy, journal and publisher-linked archive surfaces captured; no named rating table identified. | Use publisher-linked collection catalog and journal archive to locate the named congressional voting-record publication before adding any scorecard adapter; retain current result as bounded original-site inspection, not retirement. |
| United Transportation Union | UTU endorsements, an isolated 2008 rating claim and explicit SMART succession evidence recovered; no complete UTU grid. | Record predecessor/successor evidence with an explicit no-edition-inheritance rule. Pursue the original Washington Report or a named pre-SMART voting grid; candidate endorsements need a separate supported shape and scope decision. |
| US National Student Association | NSA/USSA merger evidence recovered; former USSA domain serves unrelated casino content and is excluded. | Preserve domain-mismatch and identity evidence; locate the named Legislative Scorecard in NSA/USSA holdings, with explicit publication author and date. |

### social-archives

| Publisher | Finding | Next step |
|---|---|---|
| Alaska Wilderness Action | 2015 original PDF recovered and sampled; archived 2016 offering remains unrecovered. Legacy Google sheet feed failed. | Recover the named 2016 PDF if an alternate original rendition appears; otherwise qualify the retained 2015 PDF as an explicitly historical rendition. Qualify the complete 2015 PDF with retained page observations and independent review; preserve signed/out-of-range percentages, N/A, glyphs, weights, excusals and credit/debit actions. Inspect the later archived spreadsheet rendition before choosing a PDF-only reader. |
| Committee for Full Funding of Education Programs | Current CEF context and gala program recovered; exact historical funding voting record and name continuity remain unresolved. | Resolve the historical name and locate the named voting-record title in archival/library holdings; avoid substituting CEF budget books or awards for congressional ratings. |
| Environmental Action | Current identity and historical Dirty Dozen archive leads recovered; selected electoral targets are not a full-member scorecard. | Locate the University of Pittsburgh Environmental Action archival finding aid and exact Dirty Dozen publication; establish year and selected cohort before any reader. No pre-2000 extraction in this campaign. |
| Interreligious Taskforce on US Food Policy | Finding aid and Food Policy Notes packet recovered; the named Hunger Issues voting record remains unidentified. | Use the archival publication title and holdings to locate the exact Hunger Issues in the Congress: How Members Voted issue. Record collection access and rights before acquisition of a relevant edition. |
| National Alliance of Senior Citizens | 1980 Golden Age Index candidate retained without extraction; no post-2000 edition established. | Record 1980 archival original candidate and catalog locator only; seek post-2000 original publication if any. Do not backfill the pre-2000 packet or infer retirement. |
| National Council of Senior Citizens, Inc. | Historical NCSC 2000 edition remains unrecovered. Successor ARA 2025 PDF exactly matches the existing ARA reader pin. | Use ara.py for the separately named ARA 2025 rendition and retain the NCSC predecessor relation. Locate an original NCSC 2000 Voting Record through archival catalogs; verify access restrictions at folder level. |
| SANE | Peace Action history verifies SANE lineage; successor 2024 score-specific APIs sampled. Use the existing Peace Action identity. | Reuse peace_action.py for its separately named successor edition after its full route/member/vote/EOF checks. Historical SANE remains an archive-discovery record; do not create duplicate successor ratings under sane_historical. |
| Taxation with Representation | Historical reporting describes committee and floor ratings; no original House/Senate Rated edition recovered. | Locate original House/Senate Rated issues through tax-policy archival holdings; preserve committee versus floor action scope. Stop before parser work until original member tables are recovered. |

### political-archives

| Publisher | Finding | Next step |
|---|---|---|
| American Cause, Inc. | Historical George Murphy organization and Buchanan's 1993 organization are distinct namesakes; no scorecard continuity established. | Use the Murphy organization and exact directory title to locate an original imprint; keep Buchanan website out of the historical publisher’s source list unless continuity is documented. |
| American Parents Committee, Inc. | Historical children's-legislation newsletter located; archival challenge responses and similar modern names do not establish a scorecard. | Use Georgetown MCH/NARA holdings and the exact CRS voting-record title to locate a later edition; do not interpret newsletter legislation summaries as member ratings. |
| American Security Council | Current ASCF site verified; authority over historical ASC National Security Voting Index remains unresolved. | Retain ASC and ASCF as separate names with unresolved series authority; pursue publisher archive holdings for National Security Voting Index editions later than 1974 before choosing a reader. |
| Americans for Constitutional Action | ACA/ACARI catalog and secondary continuity leads recovered; no original member ratings or post-2000 edition established. | Locate the cited 1984 ACARI imprint or catalog record and verify continuity to ACA from its title and methodology pages. Keep all pre-2000 work catalog-only. |
| Coalition for a New Foreign and Military Policy | Repository identifies 1988 rename and exact voting-record/errata folders; original rating bytes remain unrecovered. | Retain DG 138 voting-record and errata folder locators. Ask the archive for edition-level availability only in a separately authorized follow-up; first seek any post-2000 offerings under the verified 1988 name. |
| Committee for the Survival of a Free Congress | Named committee holdings and a 1985 Free Congress PAC voting-record folder located; imprint and series continuity unresolved. | Resolve publisher and series identity using the title page in DA 1: B58-F03, then seek post-2000 offerings under verified names. No pre-2000 extraction is authorized here. |
| John Birch Society | JBS documents Conservative Index/Freedom Index continuity. Current 119-3 shapes pass existing New American readers; avoid duplicate JBS ratings. | Integrate identity evidence as an explicit historical-series relationship to new_american. Run the existing report reader for report:254 with card/table reconciliation and start/end rereads; do not create duplicate JBS rating rows. |
| Liberty Lobby | Archived publisher notice records closure on July 27, 2001; last rating edition is unknown. Capitol Advantage tools are not a scorecard. | Preserve the dated closure notice and Capitol Advantage source identity. Next probe the actually linked /cgi-bin/issue.pl?dir=spotlight archive route for explicit publisher positions or rating tables; do not replace the publisher with modern same-title newsletters. |

### publications

| Publisher | Finding | Next step |
|---|---|---|
| Congressional Quarterly, Inc. | Original 2025 CQ Roll Call support and attendance tables recovered. Different denominators, blank cells and irregular headings require explicit handling. | Build a narrowly scoped CQ 2025 presidential-support HTML reader first, using the captured House/Senate pair, source definitions, explicit footnotes and reviewed row counts. Independently qualify completeness and party formatting. Profile attendance separately because of malformed header styling, blank metrics and source spelling. Keep Datawrapper subset unsupported pending count discrepancy review. |
| Human Events | 2009 original article republishes ACU's 2008 ratings. Human Events editorial selection must not become a second rating source. | Link this discovery to ACU/CPAC source research as an attributed republishing occurrence. Locate a native Human Events edition before creating any separate reader. |
| Joint Center for Political Studies | FOCUS archive recovered, with November–December 2011 PDF retained but unprofiled; no relevant rating page established. | Inspect the historical congressional-vote series catalog and FOCUS issue contents only after identifying a relevant edition/page. Continue archive pagination for post-2000 scorecard titles before requesting representative DefaultReader pages. |
| National Associated Businessmen, Inc. | 1967/1968 Economy Voting Record archival candidate retained; no post-2000 edition or publisher continuity established. | Retain archival candidate and establish an authoritative publisher/successor catalog covering 2000 onward. Do not build a reader for this pre-2000-only finding in this campaign. |
| National Audio-Visual Association, Inc. | AVIXA original history verifies organizational name changes; no continuity of the historical voting-record series established. | Search ICIA/InfoComm/AVIXA publication catalogs for the exact historical education-funding title and its co-publishing attribution; qualify original member tables only if a post-2000 edition is found. |
| Ripon Society | Current Forum archive and September 2026 issue recovered; historical 1972 rating candidate retained without extraction. | Use the archive index to locate post-2000 issues explicitly containing member ratings; qualify a relevant representative page before selecting PDF observations. Historical 1972 candidate stays catalog-only. |
| The Woman Activist | Precise Library of Virginia and Smithsonian holdings located; original voting-analysis issue remains unrecovered. | Use Library of Virginia Box 5 Folder 24 and Smithsonian item locator to locate the exact voting-analysis issue; ask archive catalog staff about post-2000 holdings only in a separately authorized outreach step. |
| Women's Lobby | US organization identity and Harvard/SMU holdings located; original Voting Chart remains unrecovered. | Use SMU Box 64 Folder 14 and Harvard MC 1042 as precise catalog leads to locate the named Women's Lobby Voting Chart and any post-2000 continuation. No reader until original chart content is recovered. |

## Reproduce and verify

Run `uv run python scripts/build_scorecard_publisher_survey.py --check` from SpicyRegs. The manifest pins all task reports, the dispatch, terminal task statuses and the retained-input verification receipt. Missing or changed inputs refuse regeneration. `--private-output PATH` retains every original task JSON document without field normalization.

The authoritative qualification ledger and publication registry remain unchanged. Public access does not establish redistribution rights. Keep original bodies private and retain failed requests. A failed request, empty search or old catalog date does not establish retirement. Liberty Lobby has a dated publisher closure notice; its last scorecard edition remains unknown.
