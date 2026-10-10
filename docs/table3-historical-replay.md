# Offline historical Table III replay

`table3_historical_records` preserves every record occurrence in a selected,
retained Office of the Law Revision Counsel (OLRC) Table III bulk member. It is
separate from `table3_records`, whose maintained laws build selects public-law
keys for requested Congresses and Congresses already held by the laws table.
Historical chapter-style keys remain native act identities in the new table.
Replaying a whole member establishes that member's contents, not publisher
completeness or suitability for replacing the current scoped table.

Run from the repository with an original archive digest, release point, and a
fresh local output directory:

```sh
uv run --frozen python -m spicy_regs.sources.table3_history \
  --archive /path/to/retained/archive \
  --sha256 sha256:ORIGINAL_SHA256 \
  --release-point 119-73 \
  --output /path/to/fresh/local-candidate
```

The producer reuses the SpicyDocs ZIP and Table III readers. It validates the
original archive digest, member grammar, release point, decoded bounds, CRC,
and every fragment before exposing subject data. Unknown vocabulary, malformed
late records, digest mismatch, wrong release point, or an empty result refuse
the replay. A failure retains `receipt.json` with the reason and exposes no
subject or ETL receipt member. An existing output directory is refused.

Successful output contains `table3_historical_records.parquet`,
`etl_receipts.parquet`, and `receipt.json`. `DatasetPolicy`, `ReceiptContext`,
and `write_dataset` apply the maintained receipt protocol. Each record carries
archive and decoded-member digests, release point, native act and record
context, and positions. Its ETL receipt witnesses the archive and member;
`source_position` and the witness locator preserve the exact source position.
`raw_source_row` retains all fields supplied by the maintained reader; the
original archive remains the authority for literal XML bytes.

The identity is `(member_sha256, release_point, act_key, act_record_position)`.
The act position runs across fragments sharing the same act key. Fragment and
fragment-record positions separately retain the physical occurrence. Publisher
sequence values remain fields, because they may reset or repeat. Different
member versions have different record identities. Different ZIP packaging of
identical member bytes retains the same record identity and distinct archive
witnesses. Meaningful blank act-section labels remain records.

The reader admits at most 128 MiB of compressed archive, 192 MiB of decoded
member, and 4 MiB per fragment. The shared output writer batches records. The
maintained ZIP reader holds the archive and decoded member in memory; these
bounds do not claim that process resident memory stays below their sum.

The pinned qualification evidence is retained under the canonical corpus's
`metadata/historical-rebuilds/20261010/table3/`: see `prove.py`, `proof.json`,
`registration-proposal.json`, and `receipt-bound-candidate/`. The proof compares
all held acts as full normalized row multisets, including multiplicities and
source order, and enumerates source-backed additions. It lists all literal
field differences separately. This is qualified content recovery after explicit
normalization, not literal page-versus-bulk row equality.

The observed blank-label discrepancy was a stale baseline omission. The
maintained page reader's earlier fix `6c1269f5` and current bulk mapper already
preserve those rows. Two independently retained page captures corroborate the
bulk sequence. This workflow acquires no new bytes, publishes nothing, and
retires no baseline.
