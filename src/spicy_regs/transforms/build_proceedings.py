"""Transform: build ``proceedings.parquet`` by promoting action-specific evidence into durable proceedings."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path

import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.ontology.citations import (
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
    references_json,
    resolved_id,
)

OUTPUT = "proceedings.parquet"
# v5: labelled FR docket values join (linked_docket_id), and stage events fall on the
# Eastern day of a Regulations.gov instant rather than its UTC day.
# v6: a docket value naming several dockets joins each (linked_docket_ids), so one FR
# document's list unites its trusted dockets, as separate link values always did.
# v7: a docket is an action docket by a RIN or docket_type exactly Rulemaking, not by the
# substring that also matched Nonrulemaking (decision 32).
# v8: only an FR document with a RIN or a rule stage unions the dockets it names; any
# other attaches to each named docket's proceeding (decision 33).
ACTOR_ID = "spicy-regs:proceedings:v8"

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
    "identity_predecessors_json",
    *ATTESTATION_COLUMNS,
    "fr_document_ids_json",
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


def _stage_from_document(document_type: object, title: object) -> str | None:
    kind = str(document_type or "").casefold()
    text = f"{kind} {str(title or '').casefold()}"
    if "withdraw" in text:
        return "withdrawn"
    if "supplement" in text and ("proposed" in text or "proposal" in text):
        return "supplemental"
    if kind == "rule" or "final rule" in text:
        return "final"
    if kind == "proposed rule" or "proposed rule" in text:
        return "proposed"
    return None


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
    return rins, _stage_from_document(row.get("document_type"), row.get("title"))


#: What a Federal Register row that is no action evidence states: no RIN and no stage.
_NO_ACTION: tuple[frozenset[str], None] = (frozenset(), None)


def _fr_action_evidence(
    path: Path, fr_index: FederalRegisterIndex, stats: JsonReadStats
) -> dict[str, tuple[set[str], str | None]]:
    """The RINs and stage of every Federal Register row that states either, by dated identity.

    One pass, each row read once: a row left out states neither, so a later pass answers
    it with :data:`_NO_ACTION` instead of reading it again. Membership here is what makes
    an FR document action evidence (decisions 32 and 33).
    """
    evidence: dict[str, tuple[set[str], str | None]] = {}
    for row in iter_parquet_rows(path, columns=_FR_EVIDENCE_COLUMNS):
        if not str(row.get("document_number") or "").strip():
            continue
        rins, stage = _fr_rins_and_stage(row, stats)
        if rins or stage:
            evidence[fr_index.record_id(row)] = (rins, stage)
    return evidence


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
    reference and merges none (fork delivery decision 33). Otherwise docket and
    artifact identities stay separate. ``fr_index`` is the generation's shared
    index of ``federal_register.parquet``; it is built here when not supplied.
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
    fr_action = _fr_action_evidence(paths["federal_register"], fr_index, json_stats)

    def empty_group(
        *,
        dockets: set[str],
        identity: tuple[object, ...],
    ) -> dict:
        return {
            "dockets": set(dockets),
            "identity": identity,
            "rins": set(),
            "titles": [],
            "agencies": [],
            # Each distinct event once, in first-seen order; a list scan per event was
            # quadratic in a proceeding's events (1,635 in the largest).
            "events": {},
            "fr_documents": set(),
            "fr_document_numbers": set(),
            "unresolved_fr_references": [],
            "cfr_refs": set(),
            "cfr_target_iris": set(),
        }

    # Establish source-backed docket membership before trusting FR link rows.
    trusted_dockets: set[str] = set()
    action_dockets: set[str] = set()
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
        # proceeding of every one of those shells (fork delivery decision 32).
        if normalize_rin(row.get("rin")) or str(row.get("docket_type") or "").casefold() == "rulemaking":
            action_dockets.add(docket)

    for row in iter_parquet_rows(
        paths["documents"],
        columns=("document_id", "docket_id", "additional_rins", "document_type", "title", "fr_doc_num"),
    ):
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        if docket is None:
            continue
        trusted_dockets.add(docket)
        raw_rins = parse_json_list(
            row.get("additional_rins"),
            stats=json_stats,
            table="documents",
            row_id=row.get("document_id"),
            column="additional_rins",
        )
        has_rin = raw_rins is not None and any(normalize_rin(value) for value in raw_rins)
        # A document of the docket's own that cites an FR document stating a RIN or a rule
        # stage is action evidence too (decision 32 as amended: 392 of the shells it first
        # removed held a RIN that way, through rule_targets' document_fr_doc edges).
        cited = row.get("fr_doc_num")
        if (
            has_rin
            or _stage_from_document(row.get("document_type"), row.get("title"))
            or (cited and resolved_id(fr_index.reference(str(cited))) in fr_action)
        ):
            action_dockets.add(docket)

    # An FR link names a docket; it makes the docket an action docket only when the
    # document is itself action evidence (decision 32 as amended). A RIN-less notice, or a
    # reference that resolves to no one document, attaches to the docket's proceeding if it
    # has one and founds none.
    linked_dockets_by_fr: dict[str, set[str]] = defaultdict(set)
    unresolved_links_by_docket: dict[str, list[dict]] = defaultdict(list)
    for docket, reference in fr_index.docket_links(paths["fr_docket_links"]):
        if docket not in trusted_dockets:
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
    ) -> None:
        if stage not in STAGES:
            return
        event = {
            "stage": stage,
            "event_kind": _STAGE_KIND[stage],
            "effective_date": eastern_day_text(date),
            "source": source,
            "evidence_id": None if evidence_id is None else str(evidence_id),
        }
        group["events"].setdefault(tuple(event.values()), event)

    for docket, row in docket_metadata.items():
        key = group_key_by_docket.get(docket)
        if key is None:
            continue
        group = groups[key]
        if rin := normalize_rin(row.get("rin")):
            group["rins"].add(rin)
        if row.get("title"):
            group["titles"].append((str(row.get("modify_date") or ""), str(row["title"])))
        if row.get("agency_code"):
            group["agencies"].append(str(row["agency_code"]))

    for row in iter_parquet_rows(
        paths["documents"],
        columns=("document_id", "docket_id", "additional_rins", "document_type", "title", "agency_code", "posted_date"),
    ):
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        key = group_key_by_docket.get(docket or "")
        if key is None:
            continue
        group = groups[key]
        raw_rins = parse_json_list(
            row.get("additional_rins"),
            stats=json_stats,
            table="documents",
            row_id=row.get("document_id"),
            column="additional_rins",
        )
        if raw_rins is not None:
            group["rins"].update(rin for value in raw_rins if (rin := normalize_rin(value)) is not None)
        if row.get("title"):
            group["titles"].append((str(row.get("posted_date") or ""), str(row["title"])))
        if row.get("agency_code"):
            group["agencies"].append(str(row["agency_code"]))
        add_event(
            group,
            stage=_stage_from_document(
                row.get("document_type"),
                row.get("title"),
            ),
            date=row.get("posted_date"),
            source="documents.document_type",
            evidence_id=row.get("document_id"),
        )

    for row in iter_parquet_rows(paths["federal_register"], columns=("document_number", "publication_date", "title")):
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
            targets = [groups[key] for key in linked_keys]
        elif rins or stage:
            targets = [ensure_fr(identity)[1]]
        else:
            continue
        for group in targets:
            group["fr_documents"].add(identity)
            group["fr_document_numbers"].add(document_number)
            group["rins"].update(rins)
            if row.get("title"):
                group["titles"].append((str(row.get("publication_date") or ""), str(row["title"])))
            add_event(
                group,
                stage=stage,
                date=row.get("publication_date"),
                source="federal_register.document_type",
                evidence_id=identity,
            )

    for row in iter_parquet_rows(
        paths["rule_targets"], columns=("docket_id", "rin", "cfr_ref", "cfr_title", "cfr_part", "cfr_section")
    ):
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        key = group_key_by_docket.get(docket or "")
        if key is None:
            continue
        group = groups[key]
        if rin := normalize_rin(row.get("rin")):
            group["rins"].add(rin)
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
            fr_overlap = len(current_fr_documents & prior_fr_documents)
            if not docket_overlap and not fr_overlap:
                continue
            predecessor_ids_by_group[group_key].add(prior_id)
            score = docket_overlap * 100 + fr_overlap * 10
            candidate_edges.append((-score, prior_id, group_key))

    # The id each group mints when it continues no prior one. A prior id that is some current
    # group's minted id is held for that group while it has no id: handed to a sibling by
    # overlap, it would be minted again for its own group. Splitting the proceedings RIN-less
    # notices had merged (decision 33) duplicated 352 ids that way on the 2026-09-23 parents.
    # Once the minting group has taken another id the hold is released and the remaining edges
    # run again, in score order, so no id is left unused that overlap alone would have kept.
    minted_by_group = {group_key: stable_id("proceeding", *group["identity"]) for group_key, group in groups.items()}
    group_minting = {minted: group_key for group_key, minted in minted_by_group.items()}
    proceeding_id_by_group: dict[str, str] = {}
    claimed_prior_ids: set[str] = set()
    edges = sorted(candidate_edges)
    while edges:
        held = {minted for minted, owner in group_minting.items() if owner not in proceeding_id_by_group}
        for _, prior_id, group_key in edges:
            if group_key in proceeding_id_by_group or prior_id in claimed_prior_ids:
                continue
            if prior_id in held and group_minting[prior_id] != group_key:
                continue
            proceeding_id_by_group[group_key] = prior_id
            claimed_prior_ids.add(prior_id)
        remaining = [
            edge for edge in edges if edge[2] not in proceeding_id_by_group and edge[1] not in claimed_prior_ids
        ]
        edges = remaining if len(remaining) < len(edges) else []
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
        rins = sorted(group["rins"])
        proceeding_id = proceeding_id_by_group[group_key]
        matched_predecessors = predecessor_ids_by_group.get(group_key, set())
        predecessors = sorted(matched_predecessors - {proceeding_id})
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
                "unresolved_fr_references_json": references_json(group["unresolved_fr_references"]),
                "cfr_refs_json": canonical_json(sorted(group["cfr_refs"])),
                "cfr_target_iris_json": canonical_json(sorted(group["cfr_target_iris"])),
                # Unified Agenda authority belongs to the editioned agenda
                # observation and is never fanned out to an action.
                "authority_refs_json": "[]",
                "identity_predecessors_json": canonical_json(predecessors),
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
    assert pq.ParquetFile(out_file).schema_arrow.names == list(COLUMNS)
    return out_file
