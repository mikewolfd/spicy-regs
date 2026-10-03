# Provider artifact review

Reviewed commit `784e231545bda0525d852a8e2019507fe20408ff` for scope and
accidentally committed credentials or private capture bytes. This bounded review
did not repeat the complete behavior review or test suite.

The changed blobs contain no credential signature matches, private-key markers,
large embedded base64 payloads, binary artifacts, or captured PDF, image, body,
pickle, environment, or model-weight files. HRC original source images and model
responses remain in the private receipt corpus. The HRC schema, tests and guide
describe semantic source facts and explicitly exclude full-edition qualification.

The OCR conversion fixtures are intentionally retained model responses, as their
README states. They trace to pages 17 and 21 of the Congress.gov witness attachment
`HHRG-114-II24-Bio-WinkelmanD-20150414.pdf`, original source SHA-256
`7ca3a9ac6183d3535f0d7aecd19fd9e4cd3ffb2d6664ba2006db04bd068d8b0c`.
The prior comparison manifest and document catalog establish that provenance.
These are existing extraction-conversion regressions, not HRC source captures,
synthetic fixtures, or asserted gold transcriptions. This check establishes their
source context; it does not assert copyright status from the hosting domain.

No artifact or scope blocker was found. The scan is a practical credential and
capture check, not proof that arbitrary secrets can never occur in text.
