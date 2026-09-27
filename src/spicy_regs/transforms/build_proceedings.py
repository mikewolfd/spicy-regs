"""Transform: build ``proceedings.parquet`` by promoting action-specific evidence into durable proceedings."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path

import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.ontology.agencies import agency_code_for_fr_agencies
from spicy_regs.ontology.citations import (
    action_evidence_rin,
    canonical_cfr_iri,
    normalize_regsgov_identifier,
    normalize_rin,
)
from spicy_regs.ontology.common import (
    ATTESTATION_COLUMNS,
    JsonReadStats,
    RunContext,
    canonical_json,
    eastern_day_text,
    iter_parquet_rows,
    parse_json_list,
    read_parquet_rows,
    stable_id,
    write_parquet_rows,
)

from spicy_regs.ontology.federal_register import (
    FederalRegisterIndex,
    catch_all_docket,
    copy_of,
    document_rule_stage,
    references_json,
    register_rule_stage,
    register_states_type,
    resolved_id,
)
from spicy_regs.ontology.rins import docket_side_holder, specific_rin_holders

OUTPUT = "proceedings.parquet"
# v5: labelled FR docket values join (linked_docket_id), and stage events fall on the
# Eastern day of a Regulations.gov instant rather than its UTC day.
# v6 (one bump over published v5): a docket value naming several dockets joins each
# (linked_docket_ids); a docket is an action docket by a RIN, a docket_type exactly
# Rulemaking, a document of its own that is action evidence or cites an FR document that
# is, or a link from an FR document that is (decision 32); and only an FR document with a
# RIN or a rule stage unites the dockets it names, any other attaching to each (decision 33).
# v7: identity_predecessors_json is all recorded ancestry, carried from each prior row, not
# the prior rows one generation overlapped; a sibling sharing only a cited notice is none.
# v8: SpicyDocs 0.35.0 reads a docket named after prose (D1) and folds Regulations.gov's typed FR-number separators (D2).
# v9 (one bump over published v8): identity_predecessors_json is removed (owner decision
# 2026-09-26, 8477dea): no consumer read the lineage, and supersedes_id with stable ids keeps
# continuity. A Federal Register feed docket (catch_all_docket) takes nothing from the
# documents it posts: no action evidence, RIN, CFR part, stage event or title (bb385d4). And a
# feed forms a proceeding only on a RIN it states, not on Regulations.gov's Rulemaking type,
# no FR link naming it counts, and four title families under ordinary ids are feeds beside
# the _FRDOC_ dockets (ee6af4b). Decision 32 as amended, owner rulings 2026-09-26.
# v10 (one bump over published v9), owner decisions 56 and 58-61: a document's rule stage comes
# only from its own Rule / Proposed Rule type; a Regulations.gov copy of a Register row that
# states a type takes that row's stage; the Register's 1994 Uncategorized rows are typed by
# their "; Final/Proposed/Interim Rule(s) <AGENCY>" title suffix; X-pattern codes (NOAA's
# 0648-X... and, as decision 61 was extended, every agency's) decide nothing. An action Register
# document that names no trusted, non-feed docket joins the one proceeding its Regulations.gov
# copies in such dockets lie in, else the one docketed proceeding whose docket-side evidence
# alone holds one of its RINs;
# else it stands alone, under its Register agency's code (decision 56 and the owner's rulings
# on it). A docket-side RIN is the docket's, its documents', or a copy's (rule_targets'
# document_fr_doc), never a docket-linked Register document's (its fr_cfr_ref; review 2b). On
# the 2026-09-26 parents proceedings fall from 267,965 to 172,742: 85,360 docket-less (40,144
# SEC SRO notices; 28,365 joined by copy and 4,090 by RIN) and 9,874 docketed go, and no
# docketed one merges. 11 appear: three FDA-1977-N dockets a withdrawal Notice no longer
# unites, and eight 1994 rules typed by their suffix. 89,662 of the 97,472 docket-less
# proceedings take a code. Each stage event says how its document joined
# (joined_by), and fr_document_joins_json says it for every Register document, with a
# specific-RIN join's RINs and the docket-side evidence that holds them (docs/ontology.md).
ACTOR_ID = "spicy-regs:proceedings:v10"

COLUMNS = (
    "proceeding_id",
    "rin",
    "docket_ids_json",
    "title",
    "agency_code",
    "current_stage",
    "stage_events_json",
    "fr_document_numbers_json",
    "cfr_refs_json",
    "cfr_target_iris_json",
    "authority_refs_json",
    *ATTESTATION_COLUMNS,
    "fr_document_ids_json",
    "fr_document_joins_json",
    "unresolved_fr_references_json",
    "rins_json",
)

STAGES = frozenset({"prerule", "proposed", "supplemental", "final", "withdrawn", "longterm"})
_STAGE_KIND = {
    "prerule": "proceedingPrerule",
    "proposed": "proceedingProposed",
    "supplemental": "proceedingSupplemental",
    "final": "proceedingFinal",
    "withdrawn": "proceedingWithdrawn",
    "longterm": "proceedingLongterm",
}

#: How a document joined its proceeding, its ``joined_by`` on a stage event and in
#: fr_document_joins_json: a Regulations.gov document of the proceeding's own docket; a
#: Register document through its own docket link, through its Regulations.gov copies (decision
#: 56's B), or through a RIN the proceeding alone holds (its E); or the Register document a
#: docket-less proceeding is.
JOINED_BY = frozenset({"docket", "fr_docket_link", "fr_copy", "specific_rin", "fr_document"})


#: The Federal Register columns that say whether a document is itself action evidence.
_FR_EVIDENCE_COLUMNS = ("document_number", "publication_date", "regulation_id_numbers_json", "document_type", "title")


def _fr_rins_and_stage(row: dict, stats: JsonReadStats) -> tuple[set[str], str | None]:
    """The RINs a Federal Register row states and its rule stage; either makes it action evidence."""
    raw_rins = parse_json_list(
        row.get("regulation_id_numbers_json"),
        stats=stats,
        table="federal_register",
        row_id=str(row.get("document_number") or "").strip(),
        column="regulation_id_numbers_json",
    )
    rins = set() if raw_rins is None else {rin for value in raw_rins if (rin := normalize_rin(value)) is not None}
    return rins, register_rule_stage(row.get("document_type"), row.get("title"))


#: What a Federal Register row that is no action evidence states: no RIN and no stage.
_NO_ACTION: tuple[frozenset[str], None] = (frozenset(), None)


def _fr_action_evidence(
    path: Path, fr_index: FederalRegisterIndex, stats: JsonReadStats
) -> tuple[dict[str, tuple[set[str], str | None]], dict[str, str], set[str]]:
    """The RINs and stage of every action-evidence Federal Register row, and every row's stage.

    One pass, each row read once: a row left out of the first map states neither a RIN
    that decides action evidence nor a stage, so a later pass answers it with
    :data:`_NO_ACTION` instead of reading it again. Membership in the first map is what
    makes an FR document action evidence (decisions 32 and 33): a stage does, and so does
    a RIN except an X-pattern code (0648-X…, 0660-X…), which stays recorded but decides
    nothing (decision 61). The second map holds the stage of every row that carries one, typed as the
    Register reads it (decision 59), and the set holds every row that states no type at all,
    for decision 60's copy resolution: only a Register row that states a type overrides a
    Regulations.gov copy's own.
    """
    evidence: dict[str, tuple[set[str], str | None]] = {}
    stages: dict[str, str] = {}
    untyped: set[str] = set()
    for row in iter_parquet_rows(path, columns=_FR_EVIDENCE_COLUMNS):
        if not str(row.get("document_number") or "").strip():
            continue
        identity = fr_index.record_id(row)
        rins, stage = _fr_rins_and_stage(row, stats)
        if stage:
            stages[identity] = stage
        elif not register_states_type(row.get("document_type"), row.get("title")):
            untyped.add(identity)
        if stage or any(action_evidence_rin(rin) for rin in rins):
            evidence[identity] = (rins, stage)
    return evidence, stages, untyped


def _current_stage_from_events(events: list[dict]) -> str | None:
    """Return the unique stage at the latest evidenced date."""
    for event in events:
        stage = event.get("stage")
        if stage in STAGES and event.get("event_kind") != _STAGE_KIND[stage]:
            raise ValueError(f"stage event kind disagrees with stage: {stage!r} / {event.get('event_kind')!r}")
    dated = [event for event in events if event.get("effective_date")]
    if not dated:
        return None
    latest_date = max(str(event["effective_date"]) for event in dated)
    latest_stages = {
        str(event["stage"])
        for event in dated
        if str(event["effective_date"]) == latest_date and event.get("stage") in STAGES
    }
    return next(iter(latest_stages)) if len(latest_stages) == 1 else None


def _require_inputs(output_dir: Path, names: tuple[str, ...]) -> dict[str, Path]:
    paths = {name: output_dir / f"{name}.parquet" for name in names}
    missing = [path.name for path in paths.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"proceedings inputs missing from {output_dir}: {', '.join(missing)}")
    return paths


def build_proceedings(
    output_dir: Path,
    *,
    run_id: str | None = None,
    asserted_at: str | None = None,
    fr_index: FederalRegisterIndex | None = None,
) -> Path:
    """Build proceedings from dockets and Federal Register action artifacts.

    A RIN identifies a Regulatory Agenda item, not an action: when exactly one
    RIN is observed for an action it is retained as denormalized evidence, but
    it never groups dockets, creates an agenda-only proceeding, or preserves a
    stable proceeding id. A Federal Register artifact that is itself action
    evidence (a RIN or a rule stage) connects the trusted dockets it names into
    one proceeding; any other attaches to each named docket's proceeding as a
    reference and merges none (fork delivery decision 33). One that names no
    trusted, non-feed docket joins the one proceeding its Regulations.gov copies
    in such dockets lie in, or else the one docketed proceeding whose docket-side
    evidence alone holds one of its RINs; otherwise it is its own proceeding,
    under its Register agency (decision 56). None of these unites two
    proceedings. ``fr_index`` is the generation's shared index of
    ``federal_register.parquet``; it is built here when not supplied.

    ``supersedes_id`` is the prior id a row continues.
    """
    paths = _require_inputs(
        output_dir,
        (
            "dockets",
            "documents",
            "federal_register",
            "fr_docket_links",
            "rule_targets",
        ),
    )
    context = RunContext.resolve(
        run_id=run_id,
        asserted_at=asserted_at,
        prefix="proceedings",
    )
    provenance = context.provenance(method="deterministic", actor_id=ACTOR_ID)
    json_stats = JsonReadStats()
    fr_index = fr_index or FederalRegisterIndex(paths["federal_register"])
    prior_file = output_dir / "_proceedings_prior.parquet"
    if not prior_file.exists() and (output_dir / OUTPUT).exists():
        prior_file = output_dir / OUTPUT
    prior_proceedings = read_parquet_rows(prior_file)
    fr_action, fr_stages, fr_untyped = _fr_action_evidence(paths["federal_register"], fr_index, json_stats)

    def empty_group(
        *,
        dockets: set[str],
        identity: tuple[object, ...],
    ) -> dict:
        return {
            "dockets": set(dockets),
            "identity": identity,
            # Docket-side RINs, each with the evidence that holds it (docket_side_holder), and the
            # RINs of the Register documents the proceeding holds, kept apart for decision 56's E.
            "rins": {},
            "fr_rins": set(),
            "titles": [],
            "agencies": [],
            # Each distinct event once, in first-seen order; a list scan per event was
            # quadratic in a proceeding's events (1,635 in the largest).
            "events": {},
            # Each Register document the proceeding holds, by id, and how it joined
            # (fr_document_joins_json).
            "fr_documents": {},
            "fr_document_numbers": set(),
            "unresolved_fr_references": [],
            "cfr_refs": set(),
            "cfr_target_iris": set(),
        }

    # Establish source-backed docket membership before trusting FR link rows.
    trusted_dockets: set[str] = set()
    action_dockets: set[str] = set()
    # Federal Register feed dockets (catch_all_docket): nothing they hold, and no FR document
    # naming them, is evidence of their proceeding (decision 32 as amended 2026-09-26).
    catch_alls: set[str] = set()
    docket_metadata: dict[str, dict] = {}
    for row in iter_parquet_rows(
        paths["dockets"], columns=("docket_id", "rin", "docket_type", "title", "agency_code", "modify_date")
    ):
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        if docket is None:
            continue
        trusted_dockets.add(docket)
        docket_metadata[docket] = row
        # A docket is action evidence by its RIN or its type being exactly Rulemaking: the
        # substring test this replaced also matched Nonrulemaking, and made a single-docket
        # proceeding of every one of those shells (fork delivery decision 32). A feed docket is
        # one only by its RIN: Regulations.gov types every _FRDOC_ feed Rulemaking. X-pattern
        # codes decide nothing (decision 61).
        if catch_all_docket(docket, row.get("title")):
            catch_alls.add(docket)
        elif str(row.get("docket_type") or "").casefold() == "rulemaking":
            action_dockets.add(docket)
        if action_evidence_rin(row.get("rin")):
            action_dockets.add(docket)

    for row in iter_parquet_rows(
        paths["documents"],
        columns=("document_id", "docket_id", "additional_rins", "document_type", "title", "fr_doc_num"),
    ):
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        if docket is None:
            continue
        trusted_dockets.add(docket)
        # A feed docket's documents post other rulemakings' FR documents: none is its evidence.
        if docket not in docket_metadata and catch_all_docket(docket):
            catch_alls.add(docket)
        if docket in catch_alls:
            continue
        raw_rins = parse_json_list(
            row.get("additional_rins"),
            stats=json_stats,
            table="documents",
            row_id=row.get("document_id"),
            column="additional_rins",
        )
        has_rin = raw_rins is not None and any(action_evidence_rin(value) for value in raw_rins)
        # A document of the docket's own that cites an FR document stating a RIN or a rule
        # stage is action evidence too (decision 32 as amended: 392 of the shells it first
        # removed held a RIN that way, through rule_targets' document_fr_doc edges). The
        # document's own stage is the Register row's when its fr_doc_num resolves to one
        # (decision 60); its RINs decide nothing while they are all X-pattern codes (decision 61).
        register_copy = copy_of(row, fr_index)
        if has_rin or register_copy in fr_action or document_rule_stage(row, register_copy, fr_stages, fr_untyped):
            action_dockets.add(docket)

    # An FR link names a docket; it makes the docket an action docket only when the
    # document is itself action evidence (decision 32 as amended). A RIN-less notice, or a
    # reference that resolves to no one document, attaches to the docket's proceeding if it
    # has one and founds none. A link to a feed docket is none of these: DOT filed its 2025
    # denied-boarding rule (2025-02814) under its Miscellaneous feed, DOT-OST-2009-0092.
    linked_dockets_by_fr: dict[str, set[str]] = defaultdict(set)
    unresolved_links_by_docket: dict[str, list[dict]] = defaultdict(list)
    for docket, reference in fr_index.docket_links(paths["fr_docket_links"]):
        if docket not in trusted_dockets or docket in catch_alls:
            continue
        if identity := resolved_id(reference):
            linked_dockets_by_fr[identity].add(docket)
            if identity in fr_action:
                action_dockets.add(docket)
        else:
            unresolved_links_by_docket[docket].append(reference)

    # Docket identity is action-specific. A Federal Register document is the only
    # cross-docket union signal used by this carrier, and only one that is itself action
    # evidence: RIN-less notices named up to 100 dockets apart (FMCSA exemption
    # applications, FDA information collections) and chained them into 400-docket
    # proceedings (decision 33).
    parent: dict[str, str] = {}

    def find(node: str) -> str:
        parent.setdefault(node, node)
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    def union(left: str, right: str) -> None:
        left_root, right_root = find(left), find(right)
        if left_root == right_root:
            return
        winner, loser = sorted((left_root, right_root))
        parent[loser] = winner

    for docket in action_dockets:
        find(docket)
    for identity, dockets in linked_dockets_by_fr.items():
        if len(dockets) > 1 and identity in fr_action:
            ordered = sorted(dockets)
            for docket in ordered[1:]:
                union(ordered[0], docket)

    members_by_root: dict[str, set[str]] = defaultdict(set)
    for docket in parent:
        members_by_root[find(docket)].add(docket)

    groups: dict[str, dict] = {}
    group_key_by_docket: dict[str, str] = {}
    for dockets in sorted(members_by_root.values(), key=lambda values: min(values)):
        anchor = min(dockets)
        key = f"docket:{anchor}"
        groups[key] = empty_group(
            dockets=dockets,
            identity=("docket", anchor),
        )
        for docket in dockets:
            group_key_by_docket[docket] = key
            groups[key]["unresolved_fr_references"].extend(unresolved_links_by_docket[docket])

    def ensure_fr(document_number: str) -> tuple[str, dict]:
        key = f"fr-document:{document_number}"
        return key, groups.setdefault(
            key,
            empty_group(
                dockets=set(),
                identity=("fr-document", document_number),
            ),
        )

    def add_event(
        group: dict,
        *,
        stage: str | None,
        date: object,
        source: str,
        evidence_id: object,
        joined_by: str,
    ) -> None:
        if joined_by not in JOINED_BY:
            raise ValueError(f"unknown joined_by {joined_by!r}")
        if stage not in STAGES:
            return
        event: dict[str, object] = {
            "stage": stage,
            "event_kind": _STAGE_KIND[stage],
            "effective_date": eastern_day_text(date),
            "source": source,
            "evidence_id": None if evidence_id is None else str(evidence_id),
            "joined_by": joined_by,
        }
        # One event per document per way it joined; a Register document's RINs and their
        # holders are in fr_document_joins_json, not repeated here.
        key = (stage, event["effective_date"], source, event["evidence_id"], joined_by)
        group["events"].setdefault(key, event)

    def add_fr(
        group: dict,
        row: dict,
        identity: str,
        rins: set[str] | frozenset[str],
        stage: str | None,
        *,
        joined_by: str,
        joined_rins: list[str] | None = None,
    ) -> None:
        """Hold a Register document in a proceeding, with how it joined, its RINs, title and stage event.

        A document joined by specific RINs (decision 56's E) records every one of them that
        points here and, over them all, the docket-side evidence that holds them.
        """
        join: dict[str, object] = {"fr_document_id": identity, "joined_by": joined_by}
        if joined_rins:
            join["joined_rins"] = joined_rins
            join["holder_sources"] = sorted(set().union(*(group["rins"][rin] for rin in joined_rins)))
        group["fr_documents"].setdefault(identity, join)
        group["fr_document_numbers"].add(str(row["document_number"]).strip())
        group["fr_rins"].update(rins)
        if row.get("title"):
            group["titles"].append((str(row.get("publication_date") or ""), str(row["title"])))
        add_event(
            group,
            stage=stage,
            date=row.get("publication_date"),
            source="federal_register.document_type",
            evidence_id=identity,
            joined_by=joined_by,
        )

    for docket, row in docket_metadata.items():
        key = group_key_by_docket.get(docket)
        if key is None:
            continue
        group = groups[key]
        if rin := normalize_rin(row.get("rin")):
            group["rins"].setdefault(rin, set()).add("docket_rin")
        if row.get("title"):
            group["titles"].append((str(row.get("modify_date") or ""), str(row["title"])))
        if row.get("agency_code"):
            group["agencies"].append(str(row["agency_code"]))

    # The proceedings each action Register document's Regulations.gov copies lie in: the
    # groups of the trusted, non-feed dockets whose own documents resolve to it (decision 56).
    # Every such docket is an action docket through its copy, so every copy has a group.
    copy_groups_by_fr: dict[str, set[str]] = defaultdict(set)
    for row in iter_parquet_rows(
        paths["documents"],
        columns=(
            "document_id",
            "docket_id",
            "additional_rins",
            "document_type",
            "title",
            "agency_code",
            "posted_date",
            "fr_doc_num",
        ),
    ):
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        key = group_key_by_docket.get(docket or "")
        if key is None or docket in catch_alls:
            continue
        group = groups[key]
        register_copy = copy_of(row, fr_index)
        if register_copy is not None and register_copy in fr_action:
            copy_groups_by_fr[register_copy].add(key)
        raw_rins = parse_json_list(
            row.get("additional_rins"),
            stats=json_stats,
            table="documents",
            row_id=row.get("document_id"),
            column="additional_rins",
        )
        for value in raw_rins or ():
            if rin := normalize_rin(value):
                group["rins"].setdefault(rin, set()).add("document_rin")
        if row.get("title"):
            group["titles"].append((str(row.get("posted_date") or ""), str(row["title"])))
        if row.get("agency_code"):
            group["agencies"].append(str(row["agency_code"]))
        add_event(
            group,
            stage=document_rule_stage(row, register_copy, fr_stages, fr_untyped),
            date=row.get("posted_date"),
            source="documents.document_type",
            evidence_id=row.get("document_id"),
            joined_by="docket",
        )

    # An action Register document no trusted docket link names joins the one proceeding its
    # Regulations.gov copies lie in (decision 56, B); copies in several proceedings unite
    # none (decision 33). Any other waits for the RIN attachment below.
    unlinked: list[tuple[dict, str, set[str] | frozenset[str], str | None]] = []
    joined_by_copy = several_copy_groups = 0
    for row in iter_parquet_rows(
        paths["federal_register"], columns=("document_number", "publication_date", "title", "agencies_json")
    ):
        document_number = str(row.get("document_number") or "").strip()
        if not document_number:
            continue
        identity = fr_index.record_id(row)
        rins, stage = fr_action.get(identity, _NO_ACTION)
        linked_keys = sorted(
            {
                group_key_by_docket[docket]
                for docket in linked_dockets_by_fr.get(identity, ())
                if docket in group_key_by_docket
            }
        )
        if linked_keys:
            # An action document's dockets were unioned above; any other document is a
            # reference of each proceeding it names.
            if len(linked_keys) != 1 and (rins or stage):
                raise RuntimeError(f"FR document {document_number} spans unmerged docket components")
            for key in linked_keys:
                add_fr(groups[key], row, identity, rins, stage, joined_by="fr_docket_link")
        elif rins or stage:
            copy_keys = copy_groups_by_fr.get(identity, ())
            if len(copy_keys) == 1:
                add_fr(groups[next(iter(copy_keys))], row, identity, rins, stage, joined_by="fr_copy")
                joined_by_copy += 1
            else:
                several_copy_groups += len(copy_keys) > 1
                unlinked.append((row, identity, rins, stage))

    for row in iter_parquet_rows(
        paths["rule_targets"],
        columns=("docket_id", "rin", "source", "cfr_ref", "cfr_title", "cfr_part", "cfr_section"),
    ):
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        key = group_key_by_docket.get(docket or "")
        # A feed docket's own RIN is read off its docket row above; every other edge of its
        # comes from the documents it posts.
        if key is None or docket in catch_alls:
            continue
        group = groups[key]
        if rin := normalize_rin(row.get("rin")):
            # rule_targets' docket_rin and document_rin rows restate the dockets' and documents'
            # RINs read above and fold into them. Its fr_cfr_ref rows restate each RIN of each
            # Register document a docket links: a document the proceeding took in, whose RINs it
            # records but never holds for E (owner ruling on review 2b).
            if holder := docket_side_holder(row.get("source")):
                group["rins"].setdefault(rin, set()).add(holder)
            else:
                group["fr_rins"].add(rin)
        if row.get("cfr_ref"):
            group["cfr_refs"].add(str(row["cfr_ref"]))
            try:
                group["cfr_target_iris"].add(
                    canonical_cfr_iri(
                        row.get("cfr_title"),
                        row.get("cfr_part"),
                        row.get("cfr_section"),
                    )
                )
            except ValueError:
                logger.warning(
                    "proceedings: retained compact CFR ref but could not project Rulespec target {}",
                    row.get("cfr_ref"),
                )

    # Every group so far is docketed. A RIN one of them alone holds through docket-side evidence
    # is specific: its dockets' rin, its documents' RINs, or a copy's RINs, which rule_targets
    # writes on the copy's docket (document_fr_doc). A RIN it holds only through a Register
    # document it took in by a docket link, or by this attachment, is not. On the 2026-09-26
    # parents such RINs drew 1,016 unrelated USCG safety-zone rules to one zebra-mussel docket
    # through the umbrella 2115-AA97, and, restated as rule_targets' fr_cfr_ref rows, 126 more
    # documents, 56 of the 79 with comparable agency codes into another agency's docket (owner
    # rulings on decision 56). X-pattern codes never count (decision 61).
    specific_holder = specific_rin_holders({key: group["rins"] for key, group in groups.items()})

    # An unlinked action document attaches to the one docketed proceeding its specific RINs
    # point to (decision 56, E); pointing to several, it unites none (decision 33). Any other
    # forms its own proceeding, whose agency is its Register row's one Regulations.gov code
    # (decision 56, C): a docketed proceeding keeps its dockets' and documents' codes.
    attached_by_rin = several_rin_proceedings = 0
    for row, identity, rins, stage in unlinked:
        # Each proceeding the document's specific RINs point to, with those RINs in order.
        specific: dict[str, list[str]] = defaultdict(list)
        for rin in sorted(rins):
            if holder := specific_holder.get(rin):
                specific[holder].append(rin)
        if len(specific) == 1:
            ((key, joined_rins),) = specific.items()
            add_fr(groups[key], row, identity, rins, stage, joined_by="specific_rin", joined_rins=joined_rins)
            attached_by_rin += 1
            continue
        several_rin_proceedings += len(specific) > 1
        group = ensure_fr(identity)[1]
        add_fr(group, row, identity, rins, stage, joined_by="fr_document")
        agencies = parse_json_list(
            row.get("agencies_json"),
            stats=json_stats,
            table="federal_register",
            row_id=row["document_number"],
            column="agencies_json",
        )
        if code := agency_code_for_fr_agencies(agencies or []):
            group["agencies"].append(code)

    group_keys_by_fr_document: dict[str, set[str]] = defaultdict(set)
    for group_key, group in groups.items():
        for identity in group["fr_documents"]:
            group_keys_by_fr_document[identity].add(group_key)

    # Stable partner ids follow action evidence, never RIN equality. Docket
    # overlap preserves ordinary continuity; FR overlap preserves a provisional
    # document-based proceeding when its docket is discovered later.
    prior_identity: list[tuple[str, set[str], set[str]]] = []
    for row in prior_proceedings:
        proceeding_id = str(row.get("proceeding_id") or "")
        if not proceeding_id:
            continue
        raw_dockets = parse_json_list(
            row.get("docket_ids_json"),
            stats=json_stats,
            table="proceedings_prior",
            row_id=proceeding_id,
            column="docket_ids_json",
        )
        prior_fr_documents, unresolved = fr_index.proceeding_ids(row, json_stats)
        prior_docket_groups = {
            group_key_by_docket[docket] for docket in raw_dockets or () if docket in group_key_by_docket
        }
        for reference in unresolved:
            # Preserve an old ambiguous observation for every possible current
            # group, without letting it select a predecessor or stable id. Looked
            # up by candidate and docket: scanning every group per reference was
            # quadratic in proceedings.
            targets = set(prior_docket_groups)
            for candidate in reference["candidate_ids"]:
                targets.update(group_keys_by_fr_document.get(candidate, ()))
            for group_key in targets:
                groups[group_key]["unresolved_fr_references"].append(reference)
        prior_identity.append(
            (
                proceeding_id,
                set() if raw_dockets is None else set(map(str, raw_dockets)),
                prior_fr_documents,
            )
        )

    prior_by_id = {
        prior_id: (prior_dockets, prior_fr_documents) for prior_id, prior_dockets, prior_fr_documents in prior_identity
    }
    prior_ids_by_docket: dict[str, set[str]] = defaultdict(set)
    prior_ids_by_fr: dict[str, set[str]] = defaultdict(set)
    for prior_id, prior_dockets, prior_fr_documents in prior_identity:
        for docket in prior_dockets:
            prior_ids_by_docket[docket].add(prior_id)
        for document_number in prior_fr_documents:
            prior_ids_by_fr[document_number].add(prior_id)

    predecessor_ids_by_group: dict[str, set[str]] = defaultdict(set)
    candidate_edges: list[tuple[int, str, str]] = []
    for group_key, group in groups.items():
        current_dockets = set(group["dockets"])
        current_fr_documents = set(group["fr_documents"])
        plausible_prior_ids: set[str] = set()
        for docket in current_dockets:
            plausible_prior_ids.update(prior_ids_by_docket.get(docket, ()))
        for document_number in current_fr_documents:
            plausible_prior_ids.update(prior_ids_by_fr.get(document_number, ()))
        for prior_id in plausible_prior_ids:
            prior_dockets, prior_fr_documents = prior_by_id[prior_id]
            docket_overlap = len(current_dockets & prior_dockets)
            shared_fr_documents = current_fr_documents & prior_fr_documents
            if not docket_overlap and not shared_fr_documents:
                continue
            predecessor_ids_by_group[group_key].add(prior_id)
            score = docket_overlap * 100 + len(shared_fr_documents) * 10
            candidate_edges.append((-score, prior_id, group_key))

    # The id each group mints when it continues no prior one. A prior id that is some current
    # group's minted id is held for that group while it has no id: handed to a sibling by
    # overlap, it would be minted again for its own group. Splitting the proceedings RIN-less
    # notices had merged (decision 33) duplicated 352 ids that way on the 2026-09-23 parents.
    # The hold is checked when each edge comes up in score order, so it lifts the moment the
    # minting group takes another id. A group that meets a hold waits: it takes none of its
    # lower-scored edges that pass, and tries the held id again on the next. Only when a pass
    # assigns nothing may a waiting group settle for a lower-scored edge.
    minted_by_group = {group_key: stable_id("proceeding", *group["identity"]) for group_key, group in groups.items()}
    group_minting = {minted: group_key for group_key, minted in minted_by_group.items()}
    proceeding_id_by_group: dict[str, str] = {}
    claimed_prior_ids: set[str] = set()
    edges = sorted(candidate_edges)
    waiting_allowed = True
    while edges:
        waiting: set[str] = set()
        for _, prior_id, group_key in edges:
            if group_key in proceeding_id_by_group or group_key in waiting or prior_id in claimed_prior_ids:
                continue
            owner = group_minting.get(prior_id, group_key)
            if owner != group_key and owner not in proceeding_id_by_group:
                if waiting_allowed:
                    waiting.add(group_key)
                continue
            proceeding_id_by_group[group_key] = prior_id
            claimed_prior_ids.add(prior_id)
        remaining = [
            edge for edge in edges if edge[2] not in proceeding_id_by_group and edge[1] not in claimed_prior_ids
        ]
        if len(remaining) < len(edges):
            waiting_allowed = True
        elif waiting_allowed:
            waiting_allowed = False
        else:
            break
        edges = remaining
    for group_key in groups:
        proceeding_id_by_group.setdefault(group_key, minted_by_group[group_key])

    rows: list[dict] = []
    for group_key, group in groups.items():
        events = sorted(
            group["events"].values(),
            key=lambda event: (
                event.get("effective_date") or "",
                event.get("stage") or "",
                event.get("evidence_id") or "",
            ),
        )
        titles = sorted(group["titles"])
        rins = sorted(group["rins"].keys() | group["fr_rins"])
        proceeding_id = proceeding_id_by_group[group_key]
        matched_predecessors = predecessor_ids_by_group.get(group_key, set())
        rows.append(
            {
                "proceeding_id": proceeding_id,
                # Compatibility/query aid only; never the row's identity.
                "rin": rins[0] if len(rins) == 1 else None,
                "rins_json": canonical_json(rins),
                "docket_ids_json": canonical_json(sorted(group["dockets"])),
                "title": titles[-1][1] if titles else None,
                "agency_code": (Counter(group["agencies"]).most_common(1)[0][0] if group["agencies"] else None),
                "current_stage": _current_stage_from_events(events),
                "stage_events_json": canonical_json(events),
                "fr_document_numbers_json": canonical_json(sorted(group["fr_document_numbers"])),
                "fr_document_ids_json": canonical_json(sorted(group["fr_documents"])),
                "fr_document_joins_json": canonical_json(
                    [group["fr_documents"][identity] for identity in sorted(group["fr_documents"])]
                ),
                "unresolved_fr_references_json": references_json(group["unresolved_fr_references"]),
                "cfr_refs_json": canonical_json(sorted(group["cfr_refs"])),
                "cfr_target_iris_json": canonical_json(sorted(group["cfr_target_iris"])),
                # Unified Agenda authority belongs to the editioned agenda
                # observation and is never fanned out to an action.
                "authority_refs_json": "[]",
                **{
                    **provenance,
                    "supersedes_id": (proceeding_id if proceeding_id in matched_predecessors else None),
                },
            }
        )

    rows.sort(key=lambda row: row["proceeding_id"])
    if duplicated := [
        left["proceeding_id"] for left, right in zip(rows, rows[1:]) if left["proceeding_id"] == right["proceeding_id"]
    ]:
        raise RuntimeError(f"proceedings: {len(duplicated):,} ids name more than one proceeding, e.g. {duplicated[:3]}")
    out_file = write_parquet_rows(output_dir / OUTPUT, columns=COLUMNS, rows=rows)
    json_stats.log("proceedings")
    logger.info(
        "Proceedings: {:,} rows ({:,} multi-docket; {:,} FR-only; {:,} with one action-evidenced RIN)",
        len(rows),
        sum(len(json.loads(row["docket_ids_json"])) > 1 for row in rows),
        sum(row["docket_ids_json"] == "[]" for row in rows),
        sum(row["rin"] is not None for row in rows),
    )
    logger.info(
        "Proceedings: unlinked Register actions {:,} joined by copy, {:,} with copies in several; "
        "{:,} attached by a specific RIN, {:,} pointing to several",
        joined_by_copy,
        several_copy_groups,
        attached_by_rin,
        several_rin_proceedings,
    )
    assert pq.ParquetFile(out_file).schema_arrow.names == list(COLUMNS)
    return out_file
