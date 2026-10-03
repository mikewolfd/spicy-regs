# HRC native normalization review

**APPROVE parser 2's exact `NA` → `N/A` mapping.** The requested change affects one native rating in the qualified edition. The original PDF, selected structured observation, and historical parser-1 review remain unchanged. This supplements [the source-reader review](reader_independent_review.md); it does not admit a public generation.

| Trace | Check | Result |
| --- | --- | --- |
| `acquire_scorecard`, `hrc.py:290–303` | Compare every parser-1 and parser-2 native fact, allowing fresh evidence IDs and the declared parser/completeness metadata | Only Foushee's 116th Congress `value_text` changes; `value_number` stays NULL |
| `_qualified` and `_retain`, `hrc.py:78–162` | Compare the exact pinned asset with the privately retained parser-2 asset | Identical digest; original `NA` remains at `/members/385/record/ratings/2` |
| Field locator, `hrc.py:195–201` | Follow the normalized rating's semantic pointer into the retained asset | Physical page 20 and original rating are recoverable |
| `Bundle.finish`, `hrc.py:316–324` | Read snapshot disclosure and parser version | Parser `hrc-118-qualified-gemini/2` explicitly declares the normalization |

The equality comparison also covers member identity, party, state, district, historical score routing, item citations, action glyphs and source locators. Standalone `N/A`, numeric scores and caret values retain their prior behavior. Source validation continues to accept the observed `NA` spelling; it does not rewrite model observations. The focused reader/outcome suite and lint check pass; [the receipt](native_normalization_independent_review.json) records commands and source/input hashes.
