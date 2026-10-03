# Blocked publisher acquisition retry

The dated [acquisition receipt](acquisition_results.json) records explicit Zyte
`httpResponseBody` retries of the blocked original URLs in the publisher API
inventory. Original responses remain in the private capture directory named
by the receipt; the public evidence contains hashes and response metadata.

AAUW, ADA, FFRF Action, NFIB and Planned Parenthood returned original publisher
content. Inspection of the retained bytes confirmed a scorecard PDF or relevant
scorecard page rather than an access challenge. NFIB's page links its current
119th Congress pre-election PDF. These observations recover acquisition access;
they do not establish complete member data or qualify a production adapter.

First Focus returned an expired Squarespace website with HTTP 404. Keep this
source blocked pending a replacement original URL; that response does not
establish that the publisher or scorecard program has retired. Entries without
an original scorecard URL were recorded without issuing a request.

[probe.py](probe.py) reproduces the bounded acquisition review. The production
rollup's explicit transport selection and evidence behavior are documented in
[scorecard acquisition](../../../../../scorecard-acquisition.md), with an
[independent transport review](transport_review.md).
