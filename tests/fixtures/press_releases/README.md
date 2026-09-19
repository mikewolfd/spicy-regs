# Appropriations committee press-release feed fixtures (copied from spicy-docs)

Two files, copied byte-for-byte from `spicy-docs/tests/fixtures/press_releases/`
at tag v0.21.1 (`6f8d20e`), the release this repository pins, on 2026-09-19.
spicy-docs captured both keyless with `Accept-Encoding: identity` the same day;
neither was reduced, truncated or reformatted. These U.S. government committee
press releases are public domain.

| File | Publisher response | Bytes | SHA-256 |
| --- | --- | --- | --- |
| `house-rss.xml` | [`appropriations.house.gov/rss.xml`](https://appropriations.house.gov/rss.xml) | 13,808 | `66ff7ac0573b6b3330f001557d27696a6f6949a2bec9b4c190c589d3e8b38c3f` |
| `senate-rss-press.xml` | [`www.appropriations.senate.gov/rss/feeds/?type=press`](https://www.appropriations.senate.gov/rss/feeds/?type=press) | 9,011 | `b5510c06de6f5c7117205fcfd4a86846f3622a9ac52aaaa0ae21a6fecd0c9bf3` |

They exist so `tests/test_press_releases.py` can run the transform end to end
against the two feed shapes with a stubbed acquirer and no network. What each
one carries for the bill linkage, as captured:

- **House**: ten items, each with a `<description>`. Three titles name a bill
  — H.R. 6500, H.R. 9770 (twice) and H.R. 8595 — so, with those bills
  published, four items match on `title` and the other six are `unmatched`.
- **Senate**: fifteen items and **no `<description>` on any of them**, so a
  Senate release can only ever match on its title, and as captured none of
  the fifteen titles names a bill (they say "CR" and "Continuing
  Resolution"). The test that proves a Senate match is a title match derives
  its input from this file by putting a bill number into one title, rather
  than committing a synthetic feed beside the real one.

Offline tests establish behavior for these shapes; they do not establish
continuing live availability. Replace these only by re-copying from the
spicy-docs release this repository pins, so the two do not drift into
different bytes under the same name.
