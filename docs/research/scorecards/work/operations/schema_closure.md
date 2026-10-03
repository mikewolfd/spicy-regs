# Scorecard schema review closure

Proceed to schema freeze and implementation. F1–F5 are closed for the research source model; the AFL-CIO cross-rendition identity link remains explicitly unsupported. Publication authority remains false.

This review pins `proposal-0.2` before freeze. Its semantic SHA-256 is `3df2a4bc91820aefd7dd455cf645309bc17fbeb6fae4c85e78af8c0cb0e5d861`. The model hash excluding only `version` and `frozen` is `281708ef0f39b7f714c7b6796fb1ed6541b5fdb7df87078581124c6accf8a59f`. See [the machine-readable receipt](schema_closure.json) for exact file pins, tested fixtures, source hashes and closure evidence.

The executable validator passed every bundle in [sample_bundles.json](../gate/sample_bundles.json) and [profile_bundles.json](../gate/profile_bundles.json). Focused counterexample tests and lint/format checks passed. The receipt records measured outcomes rather than a claim of production support.

| Finding | Closure evidence |
| --- | --- |
| F1: metric-free grids | NRF keeps item-level targets and image-alt result strings without fabricating metrics or ratings. Partial metric context and orphan participation fail validation. |
| F2: snapshot grain | Every edition fact matches its selected snapshot and capture membership. A second snapshot or cross-snapshot relationship fails. All physical fields, text types, identity references and source locators are checked. |
| F3: repeated actions | Dated AFL-CIO item URLs stay distinct. Identical workbook headers have no proven URL crosswalk, so their member-result and metric-participation joins were removed. Ordered references retain occurrence identity; NIAC historical actions keep their own period. |
| F4: selected rendition | LCV CSV literals remain unchanged and HTML stays corroborating evidence. Humane check/star glyphs now match the rendered PDF; extraction font codes stay separate with a versioned decoding rule. |
| F5: rolling periods | Planned Parenthood’s relative window stays literal. Live edition identity is separate from its refreshed snapshot. National Parks’ House-only scope comes from explicit source text. |

The original capture bytes matched their recorded hashes for each source readback listed in the receipt. Additional literal checks covered NEA’s published state typo and amendment chain, Heritage’s source identifier and score, and C4IP/Chamber grade and component examples. No official action or numeric adjustment was inferred.

The coordinator must refresh the qualification record’s embedded bundles and hashes after the final fixture corrections, then accept this independent readback. A frozen schema enables implementation. Each adapter must still prove complete source scope, stable identities, error retention and permitted evidence handling before production admission. Humane’s unqualified House chart, LCV’s unqualified full result grid, and the unresolved AFL-CIO crosswalk remain explicit limits.
