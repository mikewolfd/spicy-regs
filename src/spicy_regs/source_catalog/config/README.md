# Portable catalog evidence locators

Registry observations and retained-content review evidence describe original
records. Their locator strings are metadata; the importer and classifier do not
open them as filesystem paths. Source content digests, receipt identities,
record coordinates, timestamps, decisions and limits retain their original values.

- `raw-archive:<relative-member>` identifies a member relative to the configured
  central raw archive. It does not assert that the member is available.
- `repo:<repository>/<relative-path>` identifies a source or document within the
  named repository. Existing source pins still identify the reviewed version.
- `private-evidence-locator:sha256:<digest>` identifies the exact original locator
  text by its UTF-8 SHA-256. This is a locator identity, **not a file-content hash**
  and not evidence that the original file remains available.

Exact pre-portability configs and the private locator map are retained in the
catalog evidence archive under `evidence/release-20261028/`. The preservation
manifest pins each exact original by SHA-256 and byte length. Keep that private
archive with the raw/corpus evidence when reproducing the historical review.
A portable locator never upgrades provisional evidence or establishes acquisition,
source validity, processing use or complete coverage.
