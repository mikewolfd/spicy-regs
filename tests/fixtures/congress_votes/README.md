# Recorded-vote references (the measured sample)

`recorded-votes-119-sample.json` is the 58 `recordedVotes` entries spicy-docs
measured on 2026-09-19 over 20 bills of the 119th Congress through the
Congress.gov `bill/{c}/{type}/{n}/actions` route — the measurement that sealed
`spicy_docs.interpretation.vote_matching`'s reference shape (six fields on 58 of
58 entries, `fullActionName` on none). Extracted from
`spicy-docs/docs/research/billtrax-raw-data-2026-09-19.json`
(SHA-256 `fe219d1ce3755809dec27121813269ee9ff732745a92fa8d297e9ca707281a9c`,
spicy-docs commit `2d5791e`), block `sources.rollCallReferences`: each bill's
`bill` label and its `votes` array, verbatim, with the per-bill field
inventories and action counts left out. The publisher's values are U.S.
government data in the public domain.

| Field | Value |
| --- | --- |
| Bills sampled | 20 |
| Entries | 58 |
| Distinct roll calls | 34 (28 House, 6 Senate) |
| Roll calls claimed by more than one bill | 0 |
| URL hosts | clerk.house.gov 49, www.senate.gov 9 |

Two things the sample shows that the tests rely on. Each bill's entries come
in pairs or more with the *same* roll number: the publisher attaches one
recorded vote to each floor action it settled, so a passage vote appears on
both the "On passage" and the "Motion to reconsider" actions. That is why 58
entries index to 34 roll calls, and why `bill_vote_references` is keyed with
`action_index`. And no roll call is claimed by two bills, so
`index_vote_references` settles the sample with zero conflicts — the count
`tests/test_roll_call_votes.py` asserts on every published row.

This is the JSON actions route's spelling (`rollNumber`, `sessionNumber`,
integers), which `read_recorded_vote` accepts beside the BILLSTATUS parser's
own snake-case attributes. An offline test over it establishes behavior for
this shape; it says nothing about coverage or what the publisher serves today.
