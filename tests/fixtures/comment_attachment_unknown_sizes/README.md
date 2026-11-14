# Original attachment size observations

These complete Mirrulations JSON objects were retained on 2026-11-14 through
SpicyRegs `MirrulationsOriginals` and `CaptureEvidence`. Conditional GETs matched
the failed hosted reader's ETag, byte length and SHA-256. They are originals,
not edited fixtures. The source URL is
`s3://mirrulations/raw-data/<agency>/<docket>/text-<docket>/comments/<id>.json`.

| File | SHA-256 | Bytes | Original field |
| --- | --- | --- | --- |
| `HHS-OASH-2022-0006-0002.json` | `ce146ada03ede758a871f124ce51ad087ba8fa8d397500750f5a04052db79d06` | 2190 | `included[0].attributes.fileFormats[0].size = -1` |
| `FWS-R4-ES-2020-0109-0210.json` | `c852d64b7d38e4aecbdc0b3bc7b72611cfeaaaf3839fe6153ab644970b26b1f6` | 6162 | `included[1]` and `included[3]`, `attributes.fileFormats[0].size = -1` |

The integer `-1` cannot state a byte count. The extract preserves it, and the
shared normalization helper returns `None` for usable native data. This does
not establish the publisher's intended sentinel meaning or the attachment's
actual length. Other negative values and invalid types continue to refuse.
FWS's original included order differs from `docOrder`; both stay intact.
