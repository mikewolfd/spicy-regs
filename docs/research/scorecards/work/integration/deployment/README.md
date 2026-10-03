# Qualified scorecard publication

The 2026-10-03 deployment published LCV 2025, AFP scorecard 3933 and IJM scorecard 3995 as one atomic `scorecards` family. The separate `scorecard-analysis` family contains exact identity links. The members family adds the identity fields those links require. See [the measured publication receipt](deployment_receipt.json) for immutable pins, counts, source policies and input dependencies.

Every publisher row was rebuilt with the locked installed SpicyDocs wheel from previously qualified retained captures. The build compared all emitted literals with the qualified reference rows; only opaque observation identifiers were excluded. No congressional source was acquired again. IJM's native JSON and rendered browser JSON remain distinguishable in source locators and capture evidence.

The scorecard evidence uses `hash_only`: URLs, response metadata, hashes and parser identity are public; original publisher bodies stay in the private replay corpus. Public audits admitted the uploaded generation and evidence bytes and reported no failures or review findings. Source completeness was established during the retained-source qualification, not inferred from a public hash.

Before updating members, the deployment compared every old table key and existing value with the qualified rebuilt family. It found no removed identities or changed existing values apart from observation times. The newer retained rosters add `bioguide_previous_json`, `other_names_json` and `votesmart_id`. FEC views do not depend on the members family; the existing hosted release was retained.

The data build uses the committed consumer implementation `3f85f28c` and provider `0.53.0+scorecards.9916a2fc8473`. The deployment helpers are operational recipes, not new production readers. They refuse new output directories that already exist and preserve prior attempts.

## Repeatable operations

Run the helpers from the `spicy-regs` repository with `uv run --frozen --no-sync python`. Their `--help` documents required paths; private inputs belong outside the repository.

1. `prepare_sources.py` accepts a frozen index, the qualified LCV capture directories/reference generation, and AFP/IJM capture sequences. It seals a complete three-publisher family and refuses any changed source literal or unconsumed qualified response.
2. `prepare_members.py` accepts the qualified retained members generation/evidence and verified current table copies. It replays the original bytes through the installed source reader and seals a candidate with current publication lineage.
3. `publish_candidate.py` is the explicit remote-write step. It requires the preparation receipt and implementation commit, verifies runtime files and the deployment target, rejects changed analysis parents, then uses the existing verified family compare-and-swap publisher. It retains before/after authenticated index observations.
4. `prepare_analysis.py` reads one current index and downloads its immutable source and congressional tables. Every file must match its pinned size and SHA-256 before exact linking. Publish the resulting preparation with the same publisher helper.
5. Run `scripts/audit_generation.py --family FAMILY --base https://data.spicygov.ai --output REPORT.json` for each changed family. `readback_mcp.py --output DIRECTORY` downloads the public pinned files and exercises generic MCP locally. Add `--url https://mcp.spicygov.ai/mcp` for the hosted protocol readback.

The publisher preserves unrelated family changes made by concurrent writers. Initial-preflight and final indexes may therefore differ in other families; the receipt distinguishes those changes from this deployment's writes.

## Boundaries

The final hosted check passed: the service lists source and analysis tables, describes their actual schema, and executes attributed rating and item joins for all three publishers against the expected source and analysis pins. It also returns the measured unresolved member counts. See [the final MCP replies and response hashes](hosted_mcp_readback.json). Its newer deployed image has no scorecard-specific dictionary descriptions. Earlier analysis-table refusals came from the prior cached connection and remain in the private observation history; the later successful hosted queries supersede that temporary gap.

The already deployed MCP service is newer than this checkout's deployment configuration; replacing it with the older image would risk unrelated behavior. The existing image and FEC release remain deployed. The independent FEC readback passed every registered view schema query and representative rows; see [the preserved-service receipt](preserved_fec_service_readback.json).

Unresolved identities remain in the analysis tables, with source values intact. That initial deployment excluded HRC. Its subsequently qualified complete edition and identity links were published as a separate update; see [the HRC publication receipt](hrc_deployment_receipt.json), [update procedure](hrc_preparation.md) and [normalized candidate readback](hrc_candidate_v2_readback.json). The HRC receipt distinguishes public data audits from the separate hosted-readback result. The source registry remains disabled for unattended refresh. This deployment publishes no score reproduction or preferred-action comparison.
