# Receipt history and key-index adoption

Receipt helpers and qualification tools can be included without changing the
history retained by maintained writers or requiring a new publication format.
The current implementation preserves both boundaries.

## History retained by maintained writers

`etl_receipts.inherit_receipt` retains ordered witnesses, prior receipt
identities and exact processing values. `rebind_receipt` gives a carried row the
new generation identity and receipt digest while retaining the original
generation and processing evidence. `resolve_receipt_witness` resolves earlier
`receipt.values.*` and `receipt-processing:` references through that retained
history. Repeated carries keep processing payloads flat and deduplicated.

Court, legislative, government, FEC identity, regulatory and scorecard writers
retain their maintained history paths. The bounded regulatory and vote paths
use `current_receipt_history.inherit_current_receipts` where their input shape
allows it; that helper calls the maintained `inherit_receipt`. Regulatory
correction retains both the prior and fresh receipt inputs, in that order.

The separately included `receipt_history.carry_receipt_history` has different
semantics: it reuses exact unchanged occurrences and gives changed accepted rows
only a direct predecessor reference, without copying earlier witnesses or
processing values. `write_dataset(prior_receipts=...)` exposes this helper to
explicit callers. Its presence does not authorize replacing maintained family
history paths. Such a replacement needs a reviewed source-value and witness
resolution path that preserves the required evidence through repeated refreshes.

Boundary checks are in `tests/test_etl_receipts.py`,
`tests/test_receipt_history.py`, `tests/test_regulatory_base_bulk.py`,
`tests/test_votes_batch_current.py` and the family receipt tests. In particular,
the regulatory checks resolve original API witnesses after repeated row and
bounded rewrites and retain ordered evidence from multiple prior inputs.

## Explicit auxiliary key indexes

`receipt_key_index_writer.build_key_index` builds a separate narrow key file. It
records original physical receipt ordinals, verifies the complete positional
map and receipt pin, and replaces its destination only after verification. It
never sorts or rewrites the receipt member. `force=True` exercises the same
format on a small fixture; the threshold controls an explicit utility call.

`generations.build_generation` does not call this builder automatically.
`remote_generations.prepare_remote_generation` does not accept a
`receipt_key_index` argument or require an index above the threshold. An admitted
optional `keyIndex` can be read by the existing generation readers. Private
qualification can explicitly attach a measured sidecar using
`scripts/qualify_receipt_index.py`; that script is not a scheduled writer.

The retained historical measurement passed exact positional-map, byte-pin and
locked loopback read checks. It measured the explicit utility on one pinned
historical receipt member, not current production membership, hosted upload or
deployed readers. Its result and failed attempts remain under
`~/Work/corpora/session-reports-2026-10-05/` and
`~/Work/corpora/receipt-index-qualification-20261005/`; see
`receipt-historical-index-RESULT.md` and its linked evidence. Automatic writer
activation or a mandatory remote index needs its own current-family
qualification and reader/publication decision. No historical rerun follows from
the inclusion of this documentation.

Boundary checks are in `tests/test_receipt_key_index_writer.py`,
`tests/test_qualify_receipt_index.py`, `tests/test_generation_publication.py` and
`tests/test_remote_generations.py`.

## Replacement of the earlier draft stack

This mapping was checked against merged source
`fd4e80437722c63887c2dfb36c0d7336e4d78542` from
[#60](https://github.com/mikewolfd/spicy-regs/pull/60). It describes disposition
of the earlier proposals, not approval to activate the deferred policies.

| Earlier draft | Included replacement | Unique proposal disposition |
| --- | --- | --- |
| [#28](https://github.com/mikewolfd/spicy-regs/pull/28), `fddbcd41583d9b7cf033f9499c7099e1d759120d` | Shared prior-receipt API and standalone carry helper are included; maintained writers preserve full history. | Do not merge the original writer switch: immutable rebinding and direct-only inheritance remove the current resolution path for earlier witnesses and processing. The original regulatory correction also excludes the fresh receipt input. Retain this draft as reference for a future evidence-preserving design. |
| [#29](https://github.com/mikewolfd/spicy-regs/pull/29), `9a7a6416baf55aa479ea7009e182a5b493a01e31` | The bounded validator dispatcher is included, with the row validator available as the comparison path and parity checks in `tests/test_etl_bulk_validate.py`. | Superseded as a separate delivery PR; no separate merge is needed. |
| [#30](https://github.com/mikewolfd/spicy-regs/pull/30), `cf8ad73b340c753ac4d8ea57a7da5afb2bc4c021` | The exact same builder is included, with the explicit utility and private qualification harness developed in [#34](https://github.com/mikewolfd/spicy-regs/pull/34). | Do not merge the automatic local generation hook, remote sidecar parameter/membership changes, or mandatory above-threshold remote index. These activate publication behavior beyond the retained qualification. Retain the proposal as reference; the explicit utility is delivered separately. |

Close the earlier delivery drafts with this replacement/deferred-policy mapping
after integration of the wrap-up record. Preserve their branches and the
retained qualification evidence; closing a draft does not discard its proposal
or mark deferred activation complete.
