# Comments with a submitter class and a campaign count

Four complete Mirrulations comment objects, read on September 28, 2026 and
copied byte for byte; each object's MD5 equals its listed S3 ETag. The same
bytes are SpicyDocs' `tests/fixtures/regulations_gov_comments/` fixtures.

| File | SHA-256 | `subtype` | `duplicateComments` |
| --- | --- | --- | --- |
| `EPA-HQ-OW-2022-0114-1811.source.json` | `dccedb9f286dfca158b5a2ae6d0a77cda946b7d1fd884098e36a2444391b0261` | `Mass Mail Campaign` | 15851 |
| `EPA-HQ-OW-2022-0114-0017.source.json` | `895d3528bd3bdaa12cd1e43ff9401c8f388724e0a7fa338d8454015fedd43127` | `Company/Organization Comment` | 1 |
| `EPA-HQ-OW-2022-0114-0002.source.json` | `69ec303bb31cac609d99c9619109059c95a8d5e4133e125f1826e7d0b23829af` | `Public Comment` | 1 |
| `CMS-2016-0123-0993.source.json` | `8d6e7d9e6dcbcd02d7c597690836175eb0ace9a5402ac8323df7c65f53961630` | `Public Comment` | 0 |

Each is under `s3://mirrulations/raw-data/<agency>/<docket>/text-<docket>/comments/`.
The EPA records are from the PFAS drinking-water docket: a mass-mail record
standing for 15,851 submissions, an organization EPA classifies while the
record's `organization` is NULL, and a single public comment. CMS states 0,
the value agencies that do not count leave.
