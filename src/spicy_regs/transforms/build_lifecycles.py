"""Transform: build ``rulemaking_lifecycles.parquet`` and ``lifecycle_events.parquet`` from docketed proceedings.

One lifecycle per docketed proceeding, paired from its cleaned events (owner decisions
54-56c, and 54d and 55a of 2026-10-03). A docket-less proceeding is one Register document
that cannot pair; it is counted and left out.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.ontology.citations import normalize_regsgov_identifier, normalize_rin
from spicy_regs.ontology.common import (
    ATTESTATION_COLUMNS,
    JsonReadStats,
    RunContext,
    canonical_json,
    eastern_day,
    iter_parquet_rows,
    parse_json_list,
)
from spicy_regs.ontology.federal_register import (
    FederalRegisterIndex,
    catch_all_docket,
    copy_of,
    document_form,
    register_states_type,
)
from spicy_regs.ontology.rins import docket_side_holder, specific_rin_holders
from spicy_regs.transforms.build_regulatory_agenda import agenda_item_id
from spicy_regs.transforms.build_unified_agenda import timetable_date

LIFECYCLES_OUTPUT = "rulemaking_lifecycles.parquet"
EVENTS_OUTPUT = "lifecycle_events.parquet"
# v2 (one bump over published v1), owner decisions 2026-10-03: a Register-dated proposal is time zero, and
# an upload-dated one anchors only where the proceeding holds none (54d); routine_family reads the anchor's
# own title at time zero (55a).
# v3 (one bump over published v2): code unchanged; a lifecycle copies its proceeding's agency_code, which moves
# with proceedings v12 (FNS to FNA). The events carry no code, so their actor stays.
# v4 lifecycles, v3 events (one bump each over published v3 and v2): code unchanged; both move with proceedings v13,
# where a withdrawn posting is no stage event. Rebuilt from snapshot_75e70455's own inputs, 783 events go, 121
# lifecycles go with their proceedings, and 293 change kind or anchor (receipt round6/impl-C1/m4a_delta.out).
LIFECYCLES_ACTOR_ID = "spicy-regs:rulemaking-lifecycles:v4"
EVENTS_ACTOR_ID = "spicy-regs:lifecycle-events:v3"

#: Regulations.gov's coverage is thin before this day (decision 54's coverage flag).
COVERAGE_FROM = date(2008, 1, 1)

#: Where a withdrawal may come from (decision 54): the Register, which types a Regulations.gov
#: copy too (decision 60), and the Unified Agenda. Regulations.gov's own typing never withdraws.
WITHDRAWAL_SOURCES = frozenset({"federal_register", "unified_agenda"})

#: The routine families (decision 55), stated once: the agencies whose rules they are, the
#: title phrases that name them, and the phrases that keep a rule out, the first family that
#: matches winning. The phrases are the blind review's, plus EPA's modern "Air Plan Approval"
#: titles (1,216 docketed proceedings on snapshot_9b2c770e its phrases missed) and the Coast
#: Guard's anchorage and regatta rules; the agency keeps FDA's animal-drug "tolerances" and FAA's
#: "damage tolerance" out. A Federal Implementation Plan is EPA's own plan, not a state's (87
#: lifecycles on snapshot_9b2c770e, 7 of them Agenda-significant), and FAA flight prohibitions and
#: special federal aviation regulations are not airspace designations (owner review of decision 55).
#: A pesticide-petition receipt is the tolerance pathway by its own title (decision 55a): its final is
#: "...; Pesticide Tolerances", which is how 1,066 of snapshot_62318069's 14,805 routine survival rows
#: took their family from the final's title before the phrase was stated here.
ROUTINE_FAMILIES: tuple[tuple[str, frozenset[str], tuple[str, ...], tuple[str, ...]], ...] = (
    ("airworthiness_directive", frozenset({"FAA"}), ("airworthiness directive",), ()),
    (
        "airspace",
        frozenset({"FAA"}),
        ("airspace", "instrument approach"),
        (
            "prohibition",
            "special federal aviation regulation",
            "sfar",
            "special flight rules",
            # SFAR 95, the 2002 Winter Olympics rule, whose title names no special regulation.
            "flight operations requirements",
        ),
    ),
    (
        "state_air_plan",
        frozenset({"EPA"}),
        (
            "approval and promulgation",
            "implementation plan",
            "air plan",
            "air quality plan",
            "state plans for designated facilities",
        ),
        ("federal implementation plan", "federal plan"),
    ),
    ("pesticide_tolerance", frozenset({"EPA"}), ("tolerance", "pesticide petition"), ()),
    (
        "coast_guard_local",
        frozenset({"USCG"}),
        (
            "safety zone",
            "security zone",
            "drawbridge",
            "special local regulation",
            "regulated navigation",
            "anchorage",
            "regatta",
        ),
        (),
    ),
)

#: An Agenda entry's open status, strongest first: of a proceeding's specific RINs' latest
#: entries, those of the latest edition among them decide, in this order (``18_open_status``).
AGENDA_SIGNALS = (
    "agenda_completed_withdrawn",
    "agenda_completed_final",
    "agenda_completed_other",
    "agenda_long_term",
    "agenda_active",
    "agenda_dropped_off",
)
#: ``open_signal`` when no Agenda entry speaks: specific RINs none of which the Agenda lists
#: for the proceeding, or no specific RIN at all.
NO_AGENDA_SIGNALS = ("not_on_agenda", "no_specific_rin")

KINDS = (
    "finalized",
    "companion",
    "upload_pair",
    "withdrawn",
    "open",
    "final_without_observed_proposal",
    "no_anchor",
)


def _attested(schema: list[tuple[str, pa.DataType]]) -> pa.Schema:
    return pa.schema([*schema, *((column, pa.string()) for column in ATTESTATION_COLUMNS)])


LIFECYCLE_SCHEMA = _attested(
    [
        ("proceeding_id", pa.string()),
        ("agency_code", pa.string()),
        ("kind", pa.string()),
        ("proposal_date", pa.date32()),
        ("proposal_document_id", pa.string()),
        ("proposal_form", pa.string()),
        ("final_date", pa.date32()),
        ("final_document_id", pa.string()),
        ("final_form", pa.string()),
        ("finals_before_proposal", pa.int32()),
        ("withdrawal_date", pa.date32()),
        ("withdrawal_document_id", pa.string()),
        ("withdrawal_source", pa.string()),
        ("outcome", pa.string()),
        ("duration_days", pa.int32()),
        ("censor_date", pa.date32()),
        ("open_signal", pa.string()),
        ("routine_family", pa.string()),
        ("agenda_priority", pa.string()),
        ("agenda_major", pa.string()),
        ("specific_rins_json", pa.string()),
        ("anchored_by_specific_rin", pa.bool_()),
        ("pre_2008_coverage", pa.bool_()),
    ]
)

EVENT_SCHEMA = _attested(
    [
        ("proceeding_id", pa.string()),
        ("document_id", pa.string()),
        ("stage", pa.string()),
        ("event_date", pa.date32()),
        ("source", pa.string()),
        ("dated_by", pa.string()),
        ("evidence_id", pa.string()),
        ("joined_by", pa.string()),
        ("document_form", pa.string()),
        ("anchor_role", pa.string()),
    ]
)


@dataclass(frozen=True, order=True)
class Event:
    """One document of a proceeding, once: its stage and date as its sources state them.

    ``source`` typed its stage and ``dated_by`` dated it: a Regulations.gov copy of a Register
    row that states no type keeps its own type but takes the Register's day (decision 60a).
    Ordered by day, then document id, the tie-break every anchor takes.
    """

    event_date: date
    document_id: str
    stage: str
    source: str
    evidence_id: str
    joined_by: str
    document_form: str | None
    dated_by: str
    title: str | None = field(default=None, compare=False)


@dataclass(frozen=True)
class Lifecycle:
    """What a proceeding's events say about its rule: its kind, anchors and survival."""

    kind: str
    proposal: Event | None = None
    final: Event | None = None
    withdrawal: Event | None = None
    finals_before_proposal: int | None = None
    outcome: str | None = None
    duration_days: int | None = None

    def anchors(self) -> dict[str, str]:
        """The role of each document the lifecycle anchors on, by document id."""
        roles = (("proposal", self.proposal), ("final", self.final), ("withdrawal", self.withdrawal))
        return {event.document_id: role for role, event in roles if event is not None}


#: The forms of a proposed event that are a proposal's own publication; a comment-period extension
#: or a correction presupposes a proposal already published.
_PROPOSAL_FORMS = frozenset({"proposed", "advance_proposed"})


def _anchoring_proposal(proposals: list[Event], finals: list[Event]) -> Event:
    """The proposal a lifecycle anchors on, of its proceeding's proposals and finals in order (decision 54d).

    The earliest the Register dates. A proposal dated only by its Regulations.gov upload anchors
    only when the Register dates none that could be its publication: an upload day is when a
    document was posted, not when a rule was proposed, and agencies post unnumbered "display"
    copies on the public-inspection day, days before the Register's copy (CMS-2026-2377-0001 on
    2026-07-14, 2026-14327 on 07-16). No window: the Register's proposal is time zero however far
    it follows. Two readings of the proceeding's own events say the Register's is a later
    proposal of its own, and the upload keeps its anchor: a final between the two (the upload's
    proposal was finalized first; 32 of 376 such proceedings on snapshot_62318069's inputs, 25
    of them with the Register's proposal more than a year later), and a Register proposal that
    is itself a comment-period extension or a correction (11), which presupposes a published
    proposal.
    """
    first = proposals[0]
    if first.dated_by == "federal_register":
        return first
    register = next((event for event in proposals if event.dated_by == "federal_register"), None)
    if register is None or register.document_form not in _PROPOSAL_FORMS:
        return first
    if any(first.event_date < final.event_date <= register.event_date for final in finals):
        return first
    return register


def lifecycle(events: Iterable[Event], censor_date: date) -> Lifecycle:
    """Pair a proceeding's events (decision 54).

    The proposal is the earliest proposed event the Register dates, else the earliest, unless
    the proceeding's own events say the Register's is a later proposal
    (:func:`_anchoring_proposal`, decision 54d), and it pairs with the earliest final strictly
    after it (``finalized``). With no such final, a final on the proposal's day makes a
    ``companion`` and pairs nothing, but only when the Register dates both (decision 54b): a
    same-day final dated only by its Regulations.gov upload is no companion. A proposal itself
    dated only by its upload, with a final that same day, is an ``upload_pair`` (decision 54c):
    the day is when both were uploaded, not when either was published, so like a companion it
    has no survival outcome. Else a withdrawal from the Register or the Unified Agenda on or
    after the proposal makes it ``withdrawn``; else it is ``open``, right-censored at
    ``censor_date``. Finals before the proposal are counted, never paired. Without a proposal
    the earliest final anchors a ``final_without_observed_proposal``, and with neither the
    proceeding has ``no_anchor``. Withdrawal competes with a final: a withdrawn proposal never
    becomes final (decision 54a).
    """
    ordered = sorted(events)
    proposals = [event for event in ordered if event.stage == "proposed"]
    finals = [event for event in ordered if event.stage == "final"]
    if not proposals:
        return Lifecycle("final_without_observed_proposal", final=finals[0]) if finals else Lifecycle("no_anchor")
    proposal = _anchoring_proposal(proposals, finals)
    day = proposal.event_date
    before = sum(final.event_date < day for final in finals)
    if later := [final for final in finals if final.event_date > day]:
        return Lifecycle("finalized", proposal, later[0], None, before, "final", (later[0].event_date - day).days)
    same_day = [final for final in finals if final.event_date == day]
    if proposal.dated_by == "federal_register" and (
        register_dated := [final for final in same_day if final.dated_by == "federal_register"]
    ):
        return Lifecycle("companion", proposal, register_dated[0], None, before)
    if proposal.dated_by == "regulations_gov" and same_day:
        return Lifecycle("upload_pair", proposal, same_day[0], None, before)
    withdrawals = [
        event
        for event in ordered
        if event.stage == "withdrawn" and event.source in WITHDRAWAL_SOURCES and event.event_date >= day
    ]
    if withdrawals:
        withdrawal = withdrawals[0]
        return Lifecycle(
            "withdrawn", proposal, None, withdrawal, before, "withdrawn", (withdrawal.event_date - day).days
        )
    return Lifecycle("open", proposal, None, None, before, "censored", (censor_date - day).days)


def pre_2008_coverage(result: Lifecycle) -> bool | None:
    """Whether a lifecycle is anchored before Regulations.gov's thin coverage ends (decision 54).

    Read off the proposal, else the final. An upload pair's day is only when its documents were
    uploaded, which is no earlier than the rule, so it says the rule is old when it falls before
    2008 and says nothing otherwise (decision 54c).
    """
    anchor = result.proposal or result.final
    if anchor is None:
        return None
    if result.kind == "upload_pair" and anchor.event_date >= COVERAGE_FROM:
        return None
    return anchor.event_date < COVERAGE_FROM


def routine_family(agency_code: object, title: object) -> str | None:
    """The first :data:`ROUTINE_FAMILIES` family of the agency that a title names and no keep-out phrase of which it says.

    Read at time zero, from the anchor's own title (decision 55a): a stratum is a survival
    covariate, so a final's or a proceeding's later title must not decide it. On snapshot_62318069
    1,221 of 14,805 routine survival rows took their family from the final's title (1,066) or the
    proceeding's (155), and 2025-12404's 23 petition lifecycles split 19 tolerance to 4 none by
    whether a "Tolerances" final had arrived.
    """
    agency = str(agency_code or "")
    text = " ".join(str(title or "").split()).casefold()
    for family, agencies, phrases, excluded in ROUTINE_FAMILIES:
        if (
            agency in agencies
            and any(phrase in text for phrase in phrases)
            and not any(phrase in text for phrase in excluded)
        ):
            return family
    return None


def agenda_signal(rule_stage: object, actions: Iterable[str], edition: str, latest_edition: str) -> str:
    """What one Agenda entry says of its rule: completed (withdrawn, final or other), long-term, active or dropped off.

    ``actions`` are the entry's timetable actions, casefolded: a completed entry whose
    timetable states "Withdrawn" is withdrawn, one stating a final rule or action is final.
    An entry of the latest edition that is neither completed nor long-term is active; an
    older one has dropped off the Agenda.
    """
    stage = str(rule_stage or "").casefold()
    actions = set(actions)
    if "completed" in stage:
        if "withdrawn" in actions:
            return "agenda_completed_withdrawn"
        if any("final rule" in action or "final action" in action for action in actions):
            return "agenda_completed_final"
        return "agenda_completed_other"
    if "long-term" in stage:
        return "agenda_long_term"
    return "agenda_active" if edition == latest_edition else "agenda_dropped_off"


@dataclass(frozen=True)
class AgendaEntry:
    """A specific RIN's latest Unified Agenda entry, as a lifecycle reads it."""

    rin: str
    edition: str
    signal: str
    priority: str | None
    major: str | None
    #: The latest dated "Withdrawn" action of its timetable.
    withdrawn_on: date | None

    def rank(self) -> tuple[str, int, str]:
        """Of a proceeding's entries, the latest edition decides, then the strongest signal, then the RIN."""
        return self.edition, -AGENDA_SIGNALS.index(self.signal), self.rin


def _agenda_entry(rin: str, row: dict, latest_edition: str) -> AgendaEntry:
    try:
        timetable = json.loads(str(row.get("timetable_json") or "[]"))
    except json.JSONDecodeError:
        timetable = []
    items = [item for item in timetable if isinstance(item, dict)] if isinstance(timetable, list) else []
    actions = [(" ".join(str(item.get("action") or "").split()).casefold(), item.get("date")) for item in items]
    edition = str(row["agenda_edition"])
    return AgendaEntry(
        rin=rin,
        edition=edition,
        signal=agenda_signal(row.get("rule_stage"), (action for action, _ in actions), edition, latest_edition),
        priority=row.get("priority_category"),
        major=row.get("major"),
        withdrawn_on=max(
            (day for action, raw in actions if action == "withdrawn" and (day := timetable_date(raw))), default=None
        ),
    )


def _require_inputs(output_dir: Path, names: tuple[str, ...]) -> dict[str, Path]:
    paths = {name: output_dir / f"{name}.parquet" for name in names}
    missing = [path.name for path in paths.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"lifecycle inputs missing from {output_dir}: {', '.join(missing)}")
    return paths


def _rows_where(path: Path, key: str, values: set[str], columns: tuple[str, ...]) -> list[dict]:
    """The rows whose ``key`` is one of ``values``, with those of ``columns`` the file has."""
    if not values:
        return []
    present = [column for column in columns if column in pq.read_schema(path).names]
    return pq.read_table(path, columns=present, filters=[(key, "in", sorted(values))]).to_pylist()


def _docketed(path: Path, stats: JsonReadStats) -> tuple[dict[str, dict], dict[str, list[dict]], dict[str, str], int]:
    """Docketed proceedings, their stage events and dockets, and how many docket-less ones there are."""
    proceedings: dict[str, dict] = {}
    raw_events: dict[str, list[dict]] = {}
    proceeding_by_docket: dict[str, str] = {}
    docket_less = 0
    for row in iter_parquet_rows(
        path, columns=("proceeding_id", "docket_ids_json", "agency_code", "title", "stage_events_json")
    ):
        proceeding_id = str(row["proceeding_id"])
        dockets = parse_json_list(
            row.get("docket_ids_json"), stats=stats, table="proceedings", row_id=proceeding_id, column="docket_ids_json"
        )
        if not dockets:
            docket_less += 1
            continue
        proceedings[proceeding_id] = row
        proceeding_by_docket.update(dict.fromkeys(map(str, dockets), proceeding_id))
        events = parse_json_list(
            row.get("stage_events_json"),
            stats=stats,
            table="proceedings",
            row_id=proceeding_id,
            column="stage_events_json",
        )
        raw_events[proceeding_id] = [event for event in events or () if isinstance(event, dict)]
    return proceedings, raw_events, proceeding_by_docket, docket_less


def _document_events(
    paths: dict[str, Path], raw_events: dict[str, list[dict]], fr_index: FederalRegisterIndex, as_of: date
) -> tuple[dict[str, list[Event]], Counter[str]]:
    """Each proceeding's stage events, one per document, and the count of each event left out.

    A Regulations.gov copy of a Register document (its own ``fr_doc_num`` resolves to that one
    document) is that document, dated by the Register. The Register's own event wins, else the
    copy with the lowest document id. An event with no day, or dated after the run's own day
    ``as_of``, is left out: it cannot have been observed. Only the rows the events cite are read.
    """
    cited = {
        source: {
            str(event["evidence_id"])
            for events in raw_events.values()
            for event in events
            if event.get("source") == source
        }
        for source in ("federal_register.document_type", "documents.document_type")
    }
    documents: dict[str, dict] = {}
    copies: dict[str, str] = {}
    for row in _rows_where(
        paths["documents"], "document_id", cited["documents.document_type"], ("document_id", "fr_doc_num", "title")
    ):
        documents[row["document_id"]] = row
        if register_copy := copy_of(row, fr_index):
            copies[row["document_id"]] = register_copy
    register_ids = cited["federal_register.document_type"] | set(copies.values())
    # Each Register document's day, title and whether it states a type (decision 60 as amended).
    register: dict[str, tuple[date, str | None, bool]] = {}
    for row in _rows_where(
        paths["federal_register"],
        "document_number",
        {identity.rsplit("@", 1)[0] for identity in register_ids},
        ("document_number", "publication_date", "document_type", "title"),
    ):
        if (identity := fr_index.record_id(row)) in register_ids:
            register[identity] = (
                date.fromisoformat(str(row["publication_date"])[:10]),
                row.get("title"),
                register_states_type(row.get("document_type"), row.get("title")),
            )

    by_proceeding: dict[str, list[Event]] = {}
    counts: Counter[str] = Counter()
    for proceeding_id, events in raw_events.items():
        candidates: dict[str, list[tuple[int, Event]]] = defaultdict(list)
        for event in events:
            stage, evidence = event.get("stage"), str(event.get("evidence_id") or "")
            effective = event.get("effective_date")
            stated = date.fromisoformat(effective) if effective else None
            if event.get("source") == "federal_register.document_type":
                day, title, _ = register.get(evidence) or (stated, None, True)
                key, source, dated_by, rank = evidence, "federal_register", "federal_register", 0
            elif (register_copy := copies.get(evidence)) in register:
                day, title, typed = register[register_copy]
                source = "federal_register" if typed else "regulations_gov"
                key, dated_by, rank = register_copy, "federal_register", 1
            else:
                day, title = stated, (documents.get(evidence) or {}).get("title")
                key, source, dated_by, rank = evidence, "regulations_gov", "regulations_gov", 2
            if day is None or not stage:
                counts["undated"] += 1
                continue
            if day > as_of:
                counts["after_the_run_day"] += 1
                continue
            candidates[key].append(
                (
                    rank,
                    Event(
                        event_date=day,
                        document_id=key,
                        stage=str(stage),
                        source=source,
                        evidence_id=evidence,
                        joined_by=str(event.get("joined_by") or ""),
                        document_form=document_form(str(stage), title),
                        dated_by=dated_by,
                        title=title,
                    ),
                )
            )
        by_proceeding[proceeding_id] = []
        for options in candidates.values():
            counts["typed_two_ways"] += len({event.stage for _, event in options}) > 1
            by_proceeding[proceeding_id].append(min(options, key=lambda option: (option[0], option[1].evidence_id))[1])
    return by_proceeding, counts


def _specific_rins(paths: dict[str, Path], proceeding_by_docket: dict[str, str]) -> dict[str, list[str]]:
    """Each docketed proceeding's specific RINs: those its docket-side evidence alone holds (decision 56a)."""
    titles = {
        docket: row.get("title")
        for row in iter_parquet_rows(paths["dockets"], columns=("docket_id", "title"))
        if (docket := normalize_regsgov_identifier(row.get("docket_id"))) in proceeding_by_docket
    }
    feeds = {docket for docket in proceeding_by_docket if catch_all_docket(docket, titles.get(docket))}
    held: dict[str, set[str]] = defaultdict(set)
    for row in iter_parquet_rows(paths["rule_targets"], columns=("docket_id", "rin", "source")):
        docket = normalize_regsgov_identifier(row.get("docket_id")) or ""
        proceeding_id = proceeding_by_docket.get(docket)
        rin = normalize_rin(row.get("rin"))
        if proceeding_id and rin and docket_side_holder(row.get("source"), feed_docket=docket in feeds):
            held[proceeding_id].add(rin)
    specific: dict[str, list[str]] = defaultdict(list)
    for rin, proceeding_id in sorted(specific_rin_holders(held).items()):
        specific[proceeding_id].append(rin)
    return specific


def _agenda_entries(paths: dict[str, Path], specific: dict[str, list[str]]) -> dict[str, list[AgendaEntry]]:
    """The latest Agenda entry of each specific RIN that ``agenda_item_proceedings`` links to its proceeding."""
    linked: dict[str, set[str]] = defaultdict(set)
    for row in iter_parquet_rows(paths["agenda_item_proceedings"], columns=("rin", "proceeding_id")):
        if row.get("rin") in specific.get(row.get("proceeding_id") or "", ()):
            linked[row["proceeding_id"]].add(row["rin"])
    wanted = set().union(*linked.values())
    latest: dict[str, dict] = {}
    latest_edition = ""
    for row in iter_parquet_rows(
        paths["unified_agenda"],
        columns=("rin", "agenda_edition", "rule_stage", "priority_category", "major", "timetable_json"),
    ):
        edition = str(row.get("agenda_edition") or "")
        latest_edition = max(latest_edition, edition)
        rin = normalize_rin(row.get("rin"))
        if rin in wanted and edition > str(latest.get(rin, {}).get("agenda_edition") or ""):
            latest[rin] = row
    entries = {rin: _agenda_entry(rin, row, latest_edition) for rin, row in latest.items()}
    return {
        proceeding_id: [entries[rin] for rin in sorted(rins) if rin in entries]
        for proceeding_id, rins in linked.items()
    }


def _write(path: Path, schema: pa.Schema, rows: list[dict]) -> Path:
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path, compression="zstd")
    return path


def build_lifecycles(
    output_dir: Path,
    *,
    run_id: str | None = None,
    asserted_at: str | None = None,
    fr_index: FederalRegisterIndex | None = None,
) -> tuple[Path, Path]:
    """Build one lifecycle per docketed proceeding and the cleaned events it was paired from.

    A proceeding's stage events collapse to one per document: a Regulations.gov copy of a
    Register document is that document, dated by the Register and staged as proceedings
    staged it, which is the Register's typing wherever the Register states a type (decisions
    54 and 60). Undated events are dropped, and so are events dated after the run's own
    Eastern day (``asserted_at``), which caps ``censor_date``: the last day of any document
    event, one for the generation.

    A proceeding's specific RINs are the RINs its docket-side evidence alone holds (decision
    56a, :func:`~spicy_regs.ontology.rins.docket_side_holder`); X-pattern codes never count.
    Its ``open_signal``, Agenda priority and major flag come from the latest Unified Agenda
    entry of those RINs that ``agenda_item_proceedings`` links to it, and an entry completed
    as withdrawn adds its dated withdrawal as an event. ``fr_index`` is the generation's
    shared index of ``federal_register.parquet``; it is built here when not supplied.
    """
    paths = _require_inputs(
        output_dir,
        (
            "proceedings",
            "rule_targets",
            "agenda_item_proceedings",
            "dockets",
            "documents",
            "federal_register",
            "unified_agenda",
        ),
    )
    context = RunContext.resolve(run_id=run_id, asserted_at=asserted_at, prefix="lifecycles")
    lifecycle_provenance = context.provenance(method="deterministic", actor_id=LIFECYCLES_ACTOR_ID)
    event_provenance = context.provenance(method="deterministic", actor_id=EVENTS_ACTOR_ID)
    json_stats = JsonReadStats()
    fr_index = fr_index or FederalRegisterIndex(paths["federal_register"])

    proceedings, raw_events, proceeding_by_docket, docket_less = _docketed(paths["proceedings"], json_stats)
    as_of = eastern_day(context.asserted_at)
    if as_of is None:
        raise ValueError(f"lifecycles: unreadable asserted_at {context.asserted_at!r}")
    events_by_proceeding, left_out = _document_events(paths, raw_events, fr_index, as_of)
    # No event is dated after the run's day, so one future-dated document cannot move the censor
    # date past it; with no event at all nothing is censored and the run's day stands in.
    censor_date = max((event.event_date for events in events_by_proceeding.values() for event in events), default=as_of)
    specific = _specific_rins(paths, proceeding_by_docket)
    agenda = _agenda_entries(paths, specific)

    lifecycle_rows: list[dict] = []
    event_rows: list[dict] = []
    agenda_withdrawals = unusable_agenda_withdrawals = 0
    for proceeding_id, proceeding in sorted(proceedings.items()):
        events = events_by_proceeding[proceeding_id]
        rins = specific.get(proceeding_id, [])
        entry = max(agenda.get(proceeding_id, ()), default=None, key=AgendaEntry.rank)
        open_signal = entry.signal if entry else NO_AGENDA_SIGNALS[0] if rins else NO_AGENDA_SIGNALS[1]
        if entry and entry.signal == "agenda_completed_withdrawn":
            if entry.withdrawn_on is None or entry.withdrawn_on > censor_date:
                unusable_agenda_withdrawals += 1
            else:
                agenda_withdrawals += 1
                item = agenda_item_id(entry.rin)
                events = [
                    *events,
                    Event(
                        entry.withdrawn_on,
                        item,
                        "withdrawn",
                        "unified_agenda",
                        f"{item}@{entry.edition}",
                        "agenda_rin",
                        "withdrawn",
                        "unified_agenda",
                    ),
                ]
        result = lifecycle(events, censor_date)
        anchors = [event for event in (result.proposal, result.final) if event is not None]
        # The stratum is read at time zero from the anchor's own title (decision 55a); only a proceeding
        # with no anchor has no title of its rule's own, and takes the proceeding's.
        stratum_title = anchors[0].title if anchors else proceeding.get("title")
        lifecycle_rows.append(
            {
                "proceeding_id": proceeding_id,
                "agency_code": proceeding.get("agency_code"),
                "kind": result.kind,
                "proposal_date": result.proposal.event_date if result.proposal else None,
                "proposal_document_id": result.proposal.document_id if result.proposal else None,
                "proposal_form": result.proposal.document_form if result.proposal else None,
                "final_date": result.final.event_date if result.final else None,
                "final_document_id": result.final.document_id if result.final else None,
                "final_form": result.final.document_form if result.final else None,
                "finals_before_proposal": result.finals_before_proposal,
                "withdrawal_date": result.withdrawal.event_date if result.withdrawal else None,
                "withdrawal_document_id": result.withdrawal.document_id if result.withdrawal else None,
                "withdrawal_source": result.withdrawal.source if result.withdrawal else None,
                "outcome": result.outcome,
                "duration_days": result.duration_days,
                "censor_date": censor_date,
                "open_signal": open_signal,
                "routine_family": routine_family(proceeding.get("agency_code"), stratum_title),
                "agenda_priority": entry.priority if entry else None,
                "agenda_major": entry.major if entry else None,
                "specific_rins_json": canonical_json(rins),
                "anchored_by_specific_rin": any(event.joined_by == "specific_rin" for event in anchors),
                "pre_2008_coverage": pre_2008_coverage(result),
                **lifecycle_provenance,
            }
        )
        roles = result.anchors()
        event_rows.extend(
            {
                "proceeding_id": proceeding_id,
                "document_id": event.document_id,
                "stage": event.stage,
                "event_date": event.event_date,
                "source": event.source,
                "dated_by": event.dated_by,
                "evidence_id": event.evidence_id,
                "joined_by": event.joined_by,
                "document_form": event.document_form,
                "anchor_role": roles.get(event.document_id),
                **event_provenance,
            }
            for event in sorted(events)
        )

    metadata = {b"censor_date": censor_date.isoformat().encode()}
    lifecycles_file = _write(output_dir / LIFECYCLES_OUTPUT, LIFECYCLE_SCHEMA.with_metadata(metadata), lifecycle_rows)
    events_file = _write(output_dir / EVENTS_OUTPUT, EVENT_SCHEMA.with_metadata(metadata), event_rows)
    json_stats.log("lifecycles")
    kinds = Counter(row["kind"] for row in lifecycle_rows)
    logger.info(
        "Lifecycles: {:,} docketed proceedings ({}); {:,} docket-less left out; censor date {}",
        len(lifecycle_rows),
        ", ".join(f"{kinds[kind]:,} {kind}" for kind in KINDS),
        docket_less,
        censor_date,
    )
    logger.info(
        "Lifecycle events: {:,} documents; {:,} undated events and {:,} dated after the run's day {} left out, "
        "{:,} documents typed two ways; {:,} Agenda withdrawals, {:,} undated or after the censor date",
        len(event_rows),
        left_out["undated"],
        left_out["after_the_run_day"],
        as_of,
        left_out["typed_two_ways"],
        agenda_withdrawals,
        unusable_agenda_withdrawals,
    )
    return lifecycles_file, events_file
