# Select scorecard acquisition transport

Use `--zyte-publisher` for an explicitly selected publisher whose public source
needs the existing Zyte transport. Selection does not enable a disabled source
or waive publisher completeness checks. The credential comes from `ZYTE_TOKEN`,
loaded by the CLI's existing environment handling; it never belongs in a URL.

```sh
uv run --frozen run-rollup-scorecards \
  --registry /path/to/qualified-local-sources.yaml \
  --publisher ijm --zyte-publisher ijm --skip-upload
```

Other selected publishers keep direct HTTP. Repeating `--zyte-publisher` selects
additional publishers. The manual workflow exposes the same selection and its
existing secret store supplies the token. No schedule is enabled by this change.

SpicyDocs owns `ZyteHttpFetcher`, `ZyteTransport`, and `BoundedHttpCapture`.
SpicyRegs composes those existing components in
`src/spicy_regs/scorecards/acquisition.py`.
The default Zyte selection uses `httpResponseBody`. For IJM's API only,
`--zyte-browser-publisher ijm` selects a separately identified `browserHtml`
rendition. Its original index and scripts still use `httpResponseBody`.
The two transports share the same publisher request budget. Browser captures
retain their exact rendered DOM, and the IJM reader requires one unambiguous
JSON-bearing `pre` element. Source locators identify that element and its JSON
path. A rendered DOM is never described as the publisher's original response
bytes. This selection is refused for readers without this rendition support,
and cannot be combined with `--zyte-publisher` for the same publisher.

The evidence journal names Zyte, its request identifier, the
publisher response status, and the byte representation. The normal source
capture records the original publisher URL and selected evidence policy.
`metadata_only` omits source hashes; `hash_only` retains hashes without bodies.

Each publisher acquisition has a bounded provider-call budget. Every attempt
counts, and each call makes one provider request without hidden retries or an
automatic direct-to-proxy fallback. Missing credentials, provider failures and
budget exhaustion stop the run. Source credential refusals keep their abort
behavior. Failed or incomplete editions cannot replace previous observations.

Qualification inspects retained original bytes directly and compares row
locators and values against them. A returned page or PDF establishes access;
it does not establish a complete supported scorecard. See the
[blocked-source retry receipts](research/scorecards/work/integration/zyte_retry/)
and [API qualification](research/scorecards/work/integration/api_adapters/README.md).
