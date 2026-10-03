# Current scorecard qualification status

As of 2026-10-03, LCV 2025, AFP edition 3933 and IJM edition 3995 have complete,
source-defined observations that pass their documented reader qualification.
HRC’s final 118th Congress edition now passes its pinned source-reader
qualification. Its normalized native ratings are published; see the independent
[public readback](deployment/hrc_public_normalization_readback.json). These are edition and rendition decisions, not publisher-wide or
historical support claims.

| Publisher and scope | Verified outcome | Boundaries and evidence |
| --- | --- | --- |
| LCV, 2025 House and Senate edition | Complete source member/rating CSVs, item catalogs and item-result tables reconcile with retained originals. Local SpicyRegs publication, exact linking, preservation and MCP checks also passed. | CSV rating literals are authoritative; HTML corroborates them. Methodology disclosure is partial. See [source qualification](lcv_2025/source_qualification.json), [local integration qualification](lcv_2025/integration_qualification.json) and the [unresolved-member audit](lcv_2025/unresolved_members.md). |
| AFP, current federal API edition 3933 | The entire published roster, current score-set literals, declared bill catalog and every roster member's actions were captured twice without source changes. Installed-reader output passes independent raw-locator comparison. | Ungraded members remain present. Numeric API measurements and signed coefficients stay literal; preferred-action conversions, calculated lifetime scores and historical editions are excluded. See [qualification](api_adapters/afp_qualification.json) and [capture manifest](api_adapters/afp_capture_manifest.json). |
| IJM, current federal API edition 3995 | The entire published roster and every member detail completed both passes; repeated catalogs and styles agree. The final installed reader and independent raw-locator verifier pass. | Zyte native response bytes and rendered API JSON have distinct provenance and source locations. Grade thresholds and letters are retained as methodology; individual browser grades are not computed. See [qualification](api_adapters/ijm_qualification.json), [selected captures](api_adapters/ijm_capture_manifest.json), [attempt audit](api_adapters/ijm_zyte_attempt_manifest.json) and [replay instructions](api_adapters/README.md). |
| HRC, original final 118th Congress PDF | All named-member identities, ratings, action results, scored items, legends and endnotes passed original-source readback and independent review. The installed pinned reader reproduces the qualified facts. | Standalone `NA` ratings normalize to `N/A` in native columns while the raw spelling remains in evidence. The vacancy remains an explicit non-member disposition in private evidence; narrative frontmatter is outside typed scope. Other editions require separate qualification. See [reader qualification](hrc/reader_qualification_v2.json), [independent review](hrc/reader_independent_review.md), [local publication candidate](deployment/hrc_candidate_readback.json) and [scope](hrc/README.md). |

HRC source and analysis publication passed public audits and hosted MCP checks;
see the [deployment receipt](deployment/hrc_deployment_receipt.json). Remaining
exact-member name gaps and missing official amendment coverage are documented
in the [unresolved-member audit](hrc/unresolved_member_audit.md).

The [adapter support matrix](adapter_support_matrix.md) remains an earlier,
dated inventory snapshot. The receipts above supersede its AFP, IJM and HRC
qualification dispositions for these exact current scopes. Earlier failure
receipts remain evidence: for example, IJM's [native-response incomplete
attempt](api_adapters/ijm_native_incomplete_qualification.json) correctly refused
partial replacement before the later rendered-response recovery.

Qualification and public deployment are separate. The linked source receipts
record their state at qualification time. The deployment workflow under
[deployment/](deployment/) must separately record accepted source generations,
official-input pins, analysis results, public release admission and hosted
query readback. A pending deployment does not turn a local test or bounded
HRC probe into a public-support claim. Original third-party response bodies
remain private under `hash_only` evidence retention.
