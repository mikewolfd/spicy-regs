# Official API reuse review

Verified on 2026-10-03 against public primary documentation and the current local
readers. This was a documentation/code review, not an authenticated API run. No
credentials were read and no API subscriptions were purchased.

Reuse the existing congressional and FEC readers and published tables. These APIs
can supply exact identifiers and official records for downstream resolution;
they do not replace original scorecard publishers. Vote Smart provides useful
discovery and model examples, but its terms do not establish permission for this
project to ingest or redistribute its rating database.

## Vote Smart Rating API

- **Project/API:** [Vote Smart Rating class](https://api.votesmart.org/docs/Rating.html).
- **What it solved:** A category and interest-group directory, a group's rating
  editions, and reported candidate ratings within an edition. This offers a
  reusable discovery sequence without proving item-level methodology coverage.
- **Schema/IDs:** `categoryId`, `sigId`, `parentId`, `ratingId`, and `candidateId`
  are distinct identifiers. Edition metadata includes `timespan`, `ratingName`,
  and `ratingText`; candidate ratings carry their reported `rating`. The Rating
  class does not document scoring items, weights, or member action records.
  [Rating fields](https://api.votesmart.org/docs/Rating.html)
- **Code/endpoint to integrate:** After licensed access is authorized, a thin
  bounded reader could traverse `Rating.getCategories` → `Rating.getSigList` →
  `Rating.getSig` → `Rating.getSigRatings`; `Rating.getRating` retrieves an
  edition's candidate values. The existing RPC interface offers XML/JSON;
  the official helper list contains PHP, Java and VB examples, not a reason to
  add a new Python SDK. [Direct interface](https://api.votesmart.org/docs/direct.html),
  [helper libraries](https://api.votesmart.org/docs/libraries.html)
- **License/access constraints:** The posted terms require an approved
  licensee's own API key; individual membership and organizational fees apply.
  Campaign use is prohibited, attribution to another party for Vote Smart's
  service/data is prohibited, and revoked access requires removing its content.
  These conditions need a project-specific license decision before acquisition.
  The docs identify their general update date as 2014; current service behavior
  and licensed availability were not tested. [Terms](https://api.votesmart.org/docs/terms.html),
  [documentation index](https://api.votesmart.org/docs/index.html)
- **Recommended role:** Architectural reference now; optional licensed discovery
  later. Keep any Vote Smart observation separately attributed and follow its
  publisher URL to verify original scorecards. Do not treat its numeric value
  as the publisher's literal source rendition.

## Congress.gov API

- **Project/API:** [Library of Congress API documentation and examples](https://github.com/LibraryOfCongress/api.congress.gov).
- **What it solved:** Structured congressional members, legislation, amendments,
  sponsors/cosponsors, actions, and House vote information, with pagination and
  source update metadata. [API overview](https://github.com/LibraryOfCongress/api.congress.gov),
  [OpenAPI definitions](https://github.com/LibraryOfCongress/api.congress.gov/blob/main/Documentation/openapi.yaml)
- **Schema/IDs:** `bioguideId`; bills keyed by congress/type/number; amendments
  by congress/type/number; House votes by congress/session/roll-call number.
  Member previous-name intervals and historical terms matter for exact matching.
  A current district/name lookup is not a historical identity proof.
  [Member model](https://github.com/LibraryOfCongress/api.congress.gov/blob/main/Documentation/MemberEndpoint.md),
  [House vote model](https://github.com/LibraryOfCongress/api.congress.gov/blob/main/Documentation/HouseRollCallVoteEndpoint.md)
- **Code/endpoint to integrate:** Keep `CongressListingReader` and
  `list_route_url` in
  [the existing reader](../../../../../../spicy-docs/src/spicy_docs/sources/congress/listing.py).
  Relevant routes include `/v3/member/{bioguideId}`,
  `/v3/bill/{congress}/{billType}/{billNumber}/cosponsors`, and
  `/v3/house-vote/{congress}/{session}/{voteNumber}/members`. Reuse existing
  `members`, `member_terms`, `congress_bills`, `amendments`, `roll_call_votes`,
  `member_votes`, and `bill_cosponsors` in scorecard resolution.
  [Route definitions](https://github.com/LibraryOfCongress/api.congress.gov/blob/main/Documentation/openapi.yaml)
- **License/access constraints:** API access requires an api.data.gov key;
  documentation specifies a request rate limit and bounded pages. The API
  explicitly supports public retrieval/reuse, but this review did not establish
  a separate license for copying the repository's example client code, and the
  Congress.gov copyright page could not be retrieved. No new client-code
  dependency or blanket rights assumption is proposed.
  [API access](https://github.com/LibraryOfCongress/api.congress.gov)
- **Recommended role:** Reuse the existing official-data layer for exact joins.
  Do not infer full chamber or historical coverage from a route name: the House
  endpoint page still describes beta/118th–119th coverage, while the changelog
  reports later additions and beta-label removal. Verify needed coverage in the
  existing reader's qualification before adopting it as a replacement for House
  Clerk/Senate LIS records. [Endpoint page](https://github.com/LibraryOfCongress/api.congress.gov/blob/main/Documentation/HouseRollCallVoteEndpoint.md),
  [changelog](https://github.com/LibraryOfCongress/api.congress.gov/blob/main/ChangeLog.md)

## GovInfo API and bulk data

- **Project/API:** [GPO API](https://github.com/usgpo/api) and
  [GovInfo bulk data](https://www.govinfo.gov/developers).
- **What it solved:** Discovery of changed official publications and retrieval of
  package/granule metadata and renditions; bulk BILLSTATUS XML supplies official
  bill status without a per-bill API traversal. [API documentation](https://github.com/usgpo/api),
  [bulk collections](https://www.govinfo.gov/developers)
- **Schema/IDs:** `packageId` and `granuleId` identify publication objects, not
  scorecard items or people. `lastModified` measures source changes, distinct
  from publication dates. BILLSTATUS carries legislative identifiers and
  source version information. Keep those identifiers and capture locators
  separate from scorecard edition identity. [API model](https://github.com/usgpo/api),
  [BILLSTATUS documentation](https://github.com/usgpo/bill-status)
- **Code/endpoint to integrate:** Reuse
  [GovInfoDiscoveryReader](../../../../../../spicy-docs/src/spicy_docs/sources/govinfo/discovery.py),
  [body acquisition](../../../../../../spicy-docs/src/spicy_docs/sources/govinfo/body_acquisition.py),
  and [bulk status](../../../../../../spicy-docs/src/spicy_docs/sources/congress/bulk_status.py).
  Existing service routes include `/collections/{collection}/{lastModifiedStartDate}`,
  `/packages/{packageId}/summary`, and package granules. Bulk listing is also
  available at `/bulkdata/json/BILLSTATUS/119`; the original data remains XML.
  Follow returned `nextPage`/`offsetMark` pagination rather than invent offsets.
  [API routes](https://github.com/usgpo/api), [bulk listing formats](https://www.govinfo.gov/developers)
- **License/access constraints:** The API uses api.data.gov keys; public bulk
  downloads offer a separate access path. The API documentation project carries
  a CC0 dedication. GovInfo's content policy explicitly distinguishes government
  works from copyrighted material reproduced within government publications;
  it is not a redistribution grant for third-party scorecards.
  [Access](https://www.govinfo.gov/developers),
  [project license](https://github.com/usgpo/api/blob/main/LICENSE.md),
  [content policy](https://www.govinfo.gov/about/policies)
- **Recommended role:** Existing official evidence and exact citation targets.
  Any missing official-data coverage belongs in the existing source pipeline;
  the scorecard rollup should consume its tables rather than start a duplicate
  acquisition path.

## OpenFEC

- **Project/API:** [FEC's OpenFEC service and source code](https://github.com/fecgov/openFEC).
- **What it solved:** Federal candidate/committee records and campaign-finance
  disclosures, including candidate histories and reported candidate–committee
  relationships. It does not supply congressional scorecard ratings.
  [Candidate resources](https://github.com/fecgov/openFEC/blob/develop/webservices/resources/candidates.py)
- **Schema/IDs:** `candidate_id`, `committee_id`, `cycle`, and
  `two_year_period` have separate roles. Candidate histories are cycle-sensitive;
  a candidate identifier is not a Bioguide identifier or proof of congressional
  service. The official code distinguishes current/detail candidate data from
  `CandidateHistory`. [Candidate models and filters](https://github.com/fecgov/openFEC/blob/develop/webservices/resources/candidates.py)
- **Code/endpoint to integrate:** Reuse
  [FecClient](../../../../../../spicy-docs/src/spicy_docs/sources/fec/client.py)
  and the [retained candidate query profile](../../../../../../spicy-docs/src/spicy_docs/sources/fec/candidate_profile.py).
  Relevant service paths are `/v1/candidates/?candidate_id=...`,
  `/v1/candidate/{candidate_id}/`, and
  `/v1/candidate/{candidate_id}/history/{cycle}/`. Prefer already-published FEC
  observations and existing exact crosswalk IDs over another refresh loop.
  [Official resource implementation](https://github.com/fecgov/openFEC/blob/develop/webservices/resources/candidates.py)
- **License/access constraints:** The service uses API Umbrella authentication
  and rate limits. Its terms call for OpenFEC attribution and prohibit misleading
  FEC endorsement/source claims. The repository license covers a mix of public
  domain and separately licensed assets; the default license also identifies
  restrictions on contributor-list use. Code licensing does not erase those
  data-use conditions. [Service implementation](https://github.com/fecgov/openFEC#api-umbrella),
  [terms](https://github.com/fecgov/FEC/blob/master/TERMS-OF-SERVICE.md),
  [license](https://github.com/fecgov/openFEC/blob/develop/LICENSE.md),
  [default license](https://github.com/fecgov/FEC/blob/master/LICENSE.md)
- **Recommended role:** Optional exact-ID crosswalk input when a publisher
  supplies an FEC identifier. No scorecard source role and no inference of a
  publisher's position or member identity from financial relationships.

## Schema impact and integration decision

These are proposed applications of the evidence above, not changes to the frozen
schema or new API integrations:

1. Keep publisher, scorecard edition, metric, member observation, and external
   identifier distinct. Vote Smart's useful edition/value model does not cover
   the richer item/result/methodology shapes already observed in the census.
2. Preserve identifier scheme and original value before downstream matching.
   Vote Smart candidate IDs, FEC candidate IDs and Bioguide IDs are different
   namespaces. A numeric identifier or similar name is not an exact crosswalk.
3. Preserve literal periods and source scope. Election cycles, service terms,
   Congress/session numbers and an interest group's `timespan` are not
   interchangeable. Retain ambiguity rather than guessing one year.
4. Keep official legislative joins separate from publisher facts. A bill-linked
   cosponsorship item stays bill-linked; the existence of official vote APIs
   does not justify inventing a roll call for it.
5. Reuse bounded transport, terminal-page/count checks and capture evidence from
   existing readers. API page completion establishes the requested query scope,
   not a publisher's complete scorecard edition or a frozen historical universe.
6. Keep rights decisions attached to each source. An open-source SDK or a
   government API does not authorize retaining third-party scorecard bytes.
   Continue the scorecard `hash_only` default until a publisher-specific grant
   establishes full redistribution rights.

No API in this review supplies an authorized, source-faithful replacement for
all publisher adapters. The practical reuse is the existing official-data layer,
its readers, and explicit identifier conventions. Broader adapter work should
resume only after the coordinator's reuse review, with the requested existing
document-extraction infrastructure for PDFs.
