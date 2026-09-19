# Congress.gov index routes: list pages and details

Captured 2026-09-19 by the bounded live runs in
`~/Work/corpora/supply-2026-09-02/receipts/rollups-index-2026-09-19/` (its
`requests.json` names every request; `responses/` holds every page whole).
The api.data.gov key travelled only as an `X-Api-Key` header and appears in
no file here (`scripts/verify_credentials.py` there, 0 hits).

A detail file is the exact response. A list file is the unit's **first page
trimmed** to a handful of records, so the fixture stays small: the records
whose details are here plus the first two others, with `pagination.count`
kept as the publisher declared it and `pagination.next` dropped -- a test
stub serves one page, so the declared count is not walked here and the
transform's declared-versus-walked line is a log line in the tests. The
digest is the full capture's, from `requests.json`, not the trimmed file's.

| File | Request | Full response bytes / SHA-256 | Kept |
| --- | --- | --- | --- |
| `house-communication-119-ec-4751.json` | GET https://api.congress.gov/v3/house-communication/119/ec/4751?format=json&limit=1 | 1,830 / `83a96d5d06bc8e77d46357a4aff0028531b03a0207caa4cc17a3ecd733e778e5` | whole |
| `house-communication-119-ec-4752.json` | GET https://api.congress.gov/v3/house-communication/119/ec/4752?format=json&limit=1 | 1,758 / `444e2e99526f5b1955f7bd8658f6b2981dbabaa95e0c9abc8de3a397c739c2a5` | whole |
| `house-communication-119-ml-136.json` | GET https://api.congress.gov/v3/house-communication/119/ml/136?format=json&limit=1 | 1,500 / `7b1fef19dff76e0fd2b64a2160f0ac7031d6256c7faef0833853240480a4ce1f` | whole |
| `house-communication-119.json` | GET https://api.congress.gov/v3/house-communication/119?format=json&limit=250 | 93,518 / `8613cea5da9b4d3d310aee0e1776d603c0859d5ef25c8b3b6ef49d2553db651a` | 5 of the declared 4,975 records (the 3 whose details are here, from any of the unit's 20 retained pages, plus the first 2 others of page 1); `pagination.next` dropped |
| `committee-meeting-119-house-119569.json` | GET https://api.congress.gov/v3/committee-meeting/119/house/119569?format=json&limit=1 | 2,983 / `d7d7339a8b23caca32e8bcb71deb680185447f244c8b4e6f66dbd62b222abcd4` | whole |
| `committee-meeting-119-senate-338765.json` | GET https://api.congress.gov/v3/committee-meeting/119/senate/338765?format=json&limit=1 | 4,689 / `7ddd60c57fe3b5f0838518eb3fd8586328d1bfba1e471250d543d90eeccf68e6` | whole |
| `committee-meeting-119-senate-338774.json` | GET https://api.congress.gov/v3/committee-meeting/119/senate/338774?format=json&limit=1 | 921 / `744b2e41a6a783439cbfbceb7de515cefa5f57499c457b4cb2376ceae17ef85a` | whole |
| `committee-meeting-119.json` | GET https://api.congress.gov/v3/committee-meeting/119?format=json&limit=250 | 65,725 / `5db24eae5c66115b46d57978aaa6995e9644f0f61a9a53ae69929eda4fa7398b` | 5 of the declared 2,754 records (the 3 whose details are here, from any of the unit's 12 retained pages, plus the first 2 others of page 1); `pagination.next` dropped |
| `daily-congressional-record-172-145.json` | GET https://api.congress.gov/v3/daily-congressional-record/172/145?format=json&limit=1 | 3,032 / `dded024faaff4108eddfa060139946dea2eda4bd3b3986584ebf1288c46e5428` | whole |
| `daily-congressional-record-172-147.json` | GET https://api.congress.gov/v3/daily-congressional-record/172/147?format=json&limit=1 | 3,032 / `9be4216ce375cdc91dd36c14b0f610ebea5a6ff9ed645c7b92ba0f0c4cd38754` | whole |
| `daily-congressional-record-172-148.json` | GET https://api.congress.gov/v3/daily-congressional-record/172/148?format=json&limit=1 | 2,076 / `c1f40d70becc51ae15b5fc6202754f81c718e848d80e5003a20b7e73dc78b541` | whole |
| `daily-congressional-record-171.json` | GET https://api.congress.gov/v3/daily-congressional-record/171?format=json&limit=250 | 75,333 / `6b5f7f7c9593bb516842a9d3b0ad121294005282e9a80cf8bc21b53cb52eefe2` | 2 of the declared 219 records (no detail here; the first 2 records of page 1); `pagination.next` dropped |
| `daily-congressional-record-172.json` | GET https://api.congress.gov/v3/daily-congressional-record/172?format=json&limit=250 | 49,541 / `519f9addea885d8e04776b7cad7b1edfffb616150c9a7fdabe0108a13a5bcb98` | 5 of the declared 144 records (the 3 whose details are here, from any of the unit's 1 retained pages, plus the first 2 others of page 1); `pagination.next` dropped |
| `treaty-119-1.json` | GET https://api.congress.gov/v3/treaty/119/1?format=json&limit=1 | 1,912 / `291022d78bb69231ad032f59f0f167616352983823f6361c2f4d1f99de06d35a` | whole |
| `treaty-119-2.json` | GET https://api.congress.gov/v3/treaty/119/2?format=json&limit=1 | 2,288 / `dd485a871156da2b7131ef433dafda7712dd56998e0e74cfe57023b6fa69dcef` | whole |
| `treaty-119.json` | GET https://api.congress.gov/v3/treaty/119?format=json&limit=250 | 978 / `d1b33e8cbb8f7575f7e42bc53349bdde59672e4214016552c6dac0e22cdd9995` | 2 of the declared 2 records (the 2 whose details are here, from any of the unit's 1 retained pages, and no others); `pagination.next` dropped |
| `nomination-119.json` | GET https://api.congress.gov/v3/nomination/119?format=json&limit=250 | 187,566 / `1b85d77a963cc49d2e99aa1b12282ac05208928c1900e0135762ebfddf20bec6` | 2 of the declared 2,208 records (no detail here; the first 2 records of page 1); `pagination.next` dropped |
