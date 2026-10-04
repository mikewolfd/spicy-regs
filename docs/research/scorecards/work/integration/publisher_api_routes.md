# Observed publisher JSON and XML routes

This review combines the [original discovery inventory](publisher_api_inventory.md)
with source captures and qualified readers from the October 3–4, 2026 campaign.
The discovery inventory remains a dated observation; newer routes below do not
silently rewrite its findings. These are endpoints used by public publisher
applications, which need not be documented developer APIs. Qualification applies
to each named edition and rendition, not an entire historical archive.

| Publisher | Interface | Source data | Evidence |
|---|---|---|---|
| Americans for Prosperity | GET JSON at `https://americansforprosperity.org/wp-json/rds-bt50-scorecard/v1/` | Member score sets, scoring items, and member actions; `legislators`, `bills`, and `legislators/detail/` routes | [Current federal API qualification](api_adapters/afp_qualification.json) |
| International Justice Mission | GET JSON at `https://scorecard.ijm.org/wp-json/rds-bt50-scorecard/v1/` | Member score sets, scoring items, member actions, and grade-range methodology; the same plugin routes as AFP | [Current federal API qualification](api_adapters/ijm_qualification.json) |
| Heritage Action | GET JSON at `https://heritageaction.com/api/scorecard/` | Vote and cosponsorship catalogs and item-specific member results; the complete member roster is embedded in publisher HTML | [119th Congress qualification](qualifications/heritage_action--119.json) |
| NumbersUSA | GET JSON at `https://grades.numbersusa.org/v1/` | Congress/filter catalogs, member grades and scores, career/category metrics, component definitions, and member actions | [119th Congress qualification](qualifications/numbersusa--119.json), [118th Congress qualification](qualifications/numbersusa--118.json) |
| CPAC Foundation | Historical GraphQL POST at `http://production.data.conservative.org/v1/graphql`; current Wix JSON POST at `https://www.cpac.org/_api/cloud-data/v2/items/query` | Historical annual/lifetime ratings and item results; current `PeopleDatabase` ratings and `BillDatabase` items. The current CMS does not disclose member item results. Wix requests use the ordinary public visitor bootstrap. | [Legacy example](qualifications/cpac--2022.json), [current 2025 qualification](qualifications/cpac--2025.json) |
| Council for Innovation Promotion | GET JSON at `https://cscp.c4ip.org/public/` | Paginated `members/browse`, `bills`, and `members/{member_id}/complete`; grades and sponsorship facts | [2026 interactive qualification](qualifications/c4ip--2026-interactive.json) |
| American Energy Alliance | Form POST returning JSON at `https://www.americanenergyalliance.org/wp-admin/admin-ajax.php`, with `action=aea_datatable_action` | Paginated chamber/member ratings, vote and bill catalogs, and member actions | [119th Congress qualification](qualifications/american_energy_alliance--119.json) |
| National Parks Action Fund | GET JSON at `https://nationalparksaction.org/wp-json/rds-bt50-scorecard/v1/` | `legislators`, `bills`, `styles`, and `legislators/detail/` expose historical score sets and member actions. The current member directory and historical PDF identities differ; each rendition needs separate qualification. | [Original route observations](publisher_api_route_additions.json); the [API source guide](../../../../../../spicy-docs/docs/sources/scorecards-national-parks-action-api.md) defines the qualified native catalogue boundary. Complete member-detail qualification continues. |
| FRC Action | Public Quorum GET JSON at `https://www.quorum.us/api/sheet/{slug}/` and CSV with `format=csv` | Publisher-linked House and Senate sheets contain member and action caches. CSV exports preserve displayed final percentages where JSON formula caches are empty. | [Original route observations](publisher_api_route_additions.json); the [source guide](../../../../../../spicy-docs/docs/sources/scorecards-frc-action.md) defines the qualified current-sheet boundary |
| FFRF Action Fund | Public Quorum GET JSON and CSV at the same sheet route | Publisher-linked House and Senate sheets expose vote, sponsorship, caucus and adjustment inputs; CSV exports retain the reported total score. | [Original route observations](publisher_api_route_additions.json); the [source guide](../../../../../../spicy-docs/docs/sources/scorecards-ffrf-action.md) defines the qualified current-sheet boundary |
| National Federation of Independent Business | Public Quorum GET JSON and CSV at the same sheet route | Complete current House and Senate sheets; displayed score cells, hidden measurement caches and literal action cells. Continuously updated sheets and dated downloadable PDFs are separate renditions. | [Original route observations](publisher_api_route_additions.json); see the [source guide](../../../../../../spicy-docs/docs/sources/scorecards-nfib.md) for the qualified reader boundary |
| Gun Owners of America | Publisher-linked VoterVoice GET JSON at `https://www.votervoice.net/Api/Scorecards`, `/Scorecards/{id}/Scorings` and `/Scorecards/Criteria/{id}` | Federal edition catalogs, member scores, scoring factors and criterion details. Requests use the ordinary public application's host-scoped bootstrap authorization. A source error on one historical criterion remains separate from complete aggregate data. | [Original route observations](publisher_api_route_additions.json); reader qualification continues |

## Archived publisher JSON

**U.S. Chamber:** the original application selected House, Senate and bill JSON
files under `/sites/all/modules/custom/htv_vue/json/{year}/`. Archived copies of
the original 2018 index, JavaScript and all selected files now establish a
complete native rendition. Current original routes remain unavailable; archive
custody and original publisher URLs stay distinct. The 2019 composite rendition
is being qualified separately. See the [retained route observations](publisher_api_route_additions.json)
and [archived-source guide](../../../../../../spicy-docs/docs/sources/scorecards-us-chamber-archive.md).

## Related structured surfaces

- **AFL-CIO:** `https://aflcio.org/views/ajax` returns Drupal JSON commands containing
  HTML fragments. It helps enumerate scoring items; the qualified complete
  rating rendition uses official workbooks. See its
  [2025 qualification](qualifications/afl_cio--2025.json).
- **AFSCME:** the original application bundle embeds complete JSON member exports
  and scoring-item literals. This is structured source data, rather than a
  separately verified HTTP rating API. See its
  [2025–2026 qualification](qualifications/afscme--2025-2026.json).
- **ACLU:** original HTML embeds base64-encoded JSON for member summaries and
  individual member actions. Its JavaScript defines the displayed percentage;
  this is an embedded source rendition. See the
  [source guide](../../../../../../spicy-docs/docs/sources/scorecards-aclu.md).
- **The New American:** report HTML embeds `window.fiVoteData`; roster pages
  additionally embed issue score/grade JSON. Public WordPress AJAX search at
  `https://freedomindex.us/wp-admin/admin-ajax.php` can return member scores and
  grades, but search does not establish a complete snapshot. See the
  [source guide](../../../../../../spicy-docs/docs/sources/scorecards-new-american.md)
  and [retained search observations](publisher_api_route_additions.json).
- **NRA-PVF:** verified `Umbraco/api/PublicGradesApi` routes provide election
  state/year navigation. The observed responses contain no congressional grades;
  see the [discovery inventory](publisher_api_inventory.md).

The National Parks API's complete member-detail rendition has also been qualified
for selected native periods; see its [detail-source guide](../../../../../../spicy-docs/docs/sources/scorecards-national-parks-action-api-details.md).
Source qualification, locked package adoption and publication remain separate.

No publisher scorecard **XML API** has been verified in these inspected sources.
XML inside an XLSX file and generic WordPress feeds do not establish a rating API.
An API not found in a bounded review remains undiscovered rather than disproven.

The newer route observations retain response metadata and hashes from the
original JSON bodies. API availability does not establish complete archive
coverage, reproduction rights or adapter qualification.

Reader completeness checks follow pagination, declared counts, complete member
detail enumeration, and stable rereads as applicable. Source identity, absence,
and literal values remain intact. Captured publisher and model bodies stay in
the private corpus; these receipts retain metadata and hashes.
