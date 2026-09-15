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
