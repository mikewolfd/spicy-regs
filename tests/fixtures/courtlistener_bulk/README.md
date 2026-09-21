# Retained CourtListener courts dump

Original, complete bytes from
`https://storage.courtlistener.com/bulk-data/courts-2026-06-30.csv.bz2`.
Copied from the retained `corpora/courtlistener-bulk-2026-06-30` capture;
SR04 did not make a new network request.

- Compressed: 81,180 bytes; SHA-256 `d5a7a5aa902cb4cdb1b99eb3e6a160867a77ce4b2401682291081499537ea8be`.
- Decompressed: 765,809 bytes; SHA-256 `110a1578a24788b73a9d351992051b40cb8fcf1e95dee72dd5a42054f413e757`.
- 3,361 rows; 20 columns; 16,096 nulls; 11,808 quoted empty strings.

The publisher exports PostgreSQL CSV with UTF-8, HEADER, ESCAPE backslash and
FORCE_QUOTE *. The test independently re-encodes all values and compares every
byte with the decompressed source. It also compares jurisdiction mapping with
the frozen previous reader. This proves one retained dump, not every dataset.

# Original opinion variant witness

`opinion-380204.csv.bz2` contains the original CSV header and complete record for
opinion `380204`, selected from compressed bytes 0–2,097,151 of the publisher's
`https://storage.courtlistener.com/bulk-data/opinions-2026-06-30.csv.bz2` on
2026-09-21. The archive is locally recompressed; the decoded record bytes equal
the publisher's bytes at decompressed offset 151,120, length 56,649.

- Compressed fixture SHA-256: `62cbca0ba7f3fb6c047d5e3448dbf48aad089b26bfceeaadd009d77a7fab44ab`.
- Original CSV record SHA-256: `9bc4d4e6f09a1915ea7c2837fe0141253b81f787fd29c6624b4ec9b3ed85f9ca`.
- Native `html` value: 17,461 code points, SHA-256 `716ae706f3d0aad0c2bc5fd5fa8f5a447e1f9d434fcc32bd007fe21c38084e8b`.

The original compressed prefix, HTTP range/ETag receipt and independent CSV
re-export proof are retained at
`~/Work/corpora/supply-2026-09-02/receipts/remediation-sprint-2026-09-21/court-text/`.
This example demonstrates exact native HTML preservation, not full opinion-dump
coverage. CourtListener identifies these bulk data as free of known copyright
restrictions: <https://www.courtlistener.com/help/api/bulk-data/>.
