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

## Related structured surfaces

- **AFL-CIO:** `https://aflcio.org/views/ajax` returns Drupal JSON commands containing
  HTML fragments. It helps enumerate scoring items; the qualified complete
  rating rendition uses official workbooks. See its
  [2025 qualification](qualifications/afl_cio--2025.json).
- **AFSCME:** the original application bundle embeds complete JSON member exports
  and scoring-item literals. This is structured source data, rather than a
  separately verified HTTP rating API. See its
  [2025–2026 qualification](qualifications/afscme--2025-2026.json).
- **NRA-PVF:** verified `Umbraco/api/PublicGradesApi` routes provide election
  state/year navigation. The observed responses contain no congressional grades;
  see the [discovery inventory](publisher_api_inventory.md).

No publisher scorecard **XML API** has been verified in these inspected sources.
XML inside an XLSX file and generic WordPress feeds do not establish a rating API.
An API not found in a bounded review remains undiscovered rather than disproven.

Reader completeness checks follow pagination, declared counts, complete member
detail enumeration, and stable rereads as applicable. Source identity, absence,
and literal values remain intact. Captured publisher and model bodies stay in
the private corpus; these receipts retain metadata and hashes.
