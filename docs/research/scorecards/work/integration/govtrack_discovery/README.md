# Pinned GovTrack discovery review

The measured import in [reconciliation.json](reconciliation.json) contains seven
metadata leads: all match existing census publishers, with no novel or ambiguous
publisher proposals. These are historical discovery links, not newly verified
publisher scorecards. The canonical catalog and production status were unchanged.

The selected commit is `fa63a2b5326edd4b8835386082c2317627f058ce`; its commit API
identifies root tree `a87ecf4ecbeb4dde80e348fb0a461ba5c36b07c1`. Acquisition retained
the commit response, resolved tree and all seven listed mixed-format files.
The parser checked exact membership, byte sizes, Git blob SHA-1 and SHA-256.
The parsed prefix ends before each file's CSV rating rows.

| GovTrack metadata file | Census publisher | Match rule | Original scorecard link |
| --- | --- | --- | --- |
| [ACLU.yaml](https://github.com/govtrack/advocacy-organization-scorecards/blob/fa63a2b5326edd4b8835386082c2317627f058ce/scorecards/ACLU.yaml) | `aclu` | `exact_name_or_literal_url` | [Publisher link](https://www.aclu.org/scorecard/?filter=all) |
| [ClubForGrowth.yaml](https://github.com/govtrack/advocacy-organization-scorecards/blob/fa63a2b5326edd4b8835386082c2317627f058ce/scorecards/ClubForGrowth.yaml) | `club_for_growth` | `explicit_alias_with_conflict_check` | [Publisher link](http://www.clubforgrowth.org/scorecards/) |
| [HumanRightsCampaign.yaml](https://github.com/govtrack/advocacy-organization-scorecards/blob/fa63a2b5326edd4b8835386082c2317627f058ce/scorecards/HumanRightsCampaign.yaml) | `hrc` | `exact_name_or_literal_url` | [Publisher link](http://www.hrc.org/resources/congressional-scorecard) |
| [LeagueOfConservationVoters.yaml](https://github.com/govtrack/advocacy-organization-scorecards/blob/fa63a2b5326edd4b8835386082c2317627f058ce/scorecards/LeagueOfConservationVoters.yaml) | `lcv` | `exact_name_or_literal_url` | [Publisher link](http://scorecard.lcv.org/) |
| [NORML.yaml](https://github.com/govtrack/advocacy-organization-scorecards/blob/fa63a2b5326edd4b8835386082c2317627f058ce/scorecards/NORML.yaml) | `norml` | `explicit_alias_with_conflict_check` | [Publisher link](http://norml.org/congressional-scorecard) |
| [PlannedParenthood.yaml](https://github.com/govtrack/advocacy-organization-scorecards/blob/fa63a2b5326edd4b8835386082c2317627f058ce/scorecards/PlannedParenthood.yaml) | `planned_parenthood` | `exact_name_or_literal_url` | [Publisher link](https://www.plannedparenthoodaction.org/congressional-scorecard) |
| [USChamberofCommerce.yaml](https://github.com/govtrack/advocacy-organization-scorecards/blob/fa63a2b5326edd4b8835386082c2317627f058ce/scorecards/USChamberofCommerce.yaml) | `us_chamber` | `explicit_alias_with_conflict_check` | [Publisher link](https://www.uschamber.com/how-they-voted/2018) |

[aliases.json](aliases.json) records explicit historical-name decisions for Club
for Growth, NORML and U.S. Chamber. The Club for Growth association follows the
catalog's existing source URL; it does not resolve a legal-entity distinction
between the organization and foundation or transfer ratings between them.

Observation timestamps come from each capture. The import timestamp is
`2026-10-03T20:00:09.679391+00:00`. Upstream `updated_text` remains literal publication-or-import
metadata. Every report includes the alias-file, catalog and private manifest
hashes; see `input_pins`. Source facts and per-series status remain separate.

The pinned tree has no license grant. No upstream code or rating rows were
copied into the implementation or this report. Whole mixed files remain in
private corpus storage and are not included here:
`/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/govtrack-discovery-2026-10-03/qualified-capture`.

See [the workflow guide](../../../../../scorecard-discovery.md) for repeatable
commands. The final replay used the installed candidate package with no sibling
path override; [verification.json](verification.json) records the package and
artifact hashes. Focused tests cover metadata boundaries, literal dates, Git
pins, ambiguous links and aliases, multiple series per publisher, refusal,
retained-body corruption, and preservation of prior review output. Qualification
applies to discovery metadata only; original publisher acquisition requires its
own review.
