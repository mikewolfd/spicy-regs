"""Qualified multi-publisher targets versus pinned official actions; credit stays literal."""

from collections import Counter, defaultdict
from datetime import date, datetime
from hashlib import file_digest
import json
import re

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

RULE_VERSION = "scorecard-roll-call-comparison-v2"
SPONSORSHIP_RULE_VERSION = "common-cause-2022-house-disclose-cutoff-v1"
SPONSORSHIP_SCOPE = "common_cause:2022-archived"
SPONSORSHIP_ITEM = "House:column:15"
SPONSORSHIP_BILL = "117-hr-1334"
TABLE = "scorecard_action_comparisons"
COLUMNS = (
    "scorecard_id", "result_id", "source_snapshot_id", "item_id", "publisher_member_key", "metric_id",
    "reference_id", "vote_id", "bioguide_id", "official_member_key",
    "publisher_target_text", "publisher_action_text", "publisher_credit_text", "publisher_eligibility_text",
    "official_position_text", "official_position_normalized", "comparison_status", "reason",
    "rule_version", "capture_id", "source_url", "source_path",
)
KEYS = ("scorecard_id", "item_id", "publisher_member_key", "result_id")
RESULT_COLUMNS = (
    "scorecard_id", "result_id", "snapshot_id", "item_id", "publisher_member_key", "metric_id",
    "participation_id", "action_text", "result_text", "eligibility_text", "capture_id", "source_url", "source_path",
)
METRIC_COLUMNS = ("scorecard_id", "snapshot_id", "metric_id", "item_id", "participation_id", "publisher_position_text")
VOTE_COLUMNS = ("vote_id", "member_key", "bioguide_id", "lis_id", "position", "position_normalized")
METHODOLOGY_COLUMNS = ("scorecard_id", "snapshot_id", "methodology_id", "chamber_text", "methodology_text",
                       "capture_id", "source_url", "source_path")
COSPONSOR_COLUMNS = ("bill_id", "cosponsor_index", "bioguide_id", "sponsorship_date", "sponsorship_withdrawn_date")
STATUSES = frozenset({"same", "different", "not_voting", "present", "unresolved", "not_applicable"})

_NON_FLOOR_VOTE_KINDS = frozenset({
    "cosponsorship",
    "co_sponsorship",
    "sponsorship",
    "bill",
    "bills_to_cosponsor",
    "committee_action",
    "committee_vote",
    "letter",
    "endorsement",
    "caucus",
    "statement",
    "score",
    "legislation",
    "co-sponsorship",
})

# Qualified per-publisher exact target mappings, empirically verified against pinned
# official roll_call_votes / member_votes and publisher credit polarity.
_EXACT_PUBLISHER_TARGETS: dict[str, dict[str, str]] = {
    "lcv": {"Yes": "yea", "No": "nay"},
    "afge": {"Yes": "yea", "Yea": "yea", "No": "nay", "Nay": "nay"},
    "sba": {"Yes": "yea", "No": "nay"},
    "network": {"Yea": "yea", "Nay": "nay"},
    "teamsters": {"Yes": "yea", "yes": "yea", "No": "nay", "no": "nay"},
    "ntu": {"Yes": "yea", "Y": "yea", "y": "yea", "No": "nay", "N": "nay", "n": "nay"},
    "first_focus": {"SUPPORT": "yea", "OPPOSE": "nay"},
    "peace_action": {"favor": "yea", "opposed": "nay"},
    "farm_bureau": {"approve": "yea", "oppose": "nay", "disapprove": "nay"},
    "us_chamber": {
        "Y": "yea", "Yes": "yea", "Support": "yea", "supports": "yea",
        "N": "nay", "No": "nay", "Oppose": "nay", "opposes": "nay",
    },
    "ara": {
        "A YES vote is the pro-retiree vote.": "yea",
        "A NO vote is the pro-retiree vote.": "nay",
    },
    "sace_action": {
        "YES was the clean energy vote.": "yea",
        "NO was the clean energy vote.": "nay",
        "YES was the pro-global warming action vote.": "yea",
    },
    "cpac": {"1": "yea", "2": "yea", "S": "yea", "-1": "nay", "-2": "nay", "O": "nay"},
}

# Qualified per-publisher phrase-prefix rules for publishers whose item position text
# states an explicit standardized support/oppose or yea/nay clause.
_PREFIX_PUBLISHER_TARGETS: dict[str, tuple[tuple[str, str], ...]] = {
    "frc_action": (
        ("FRC Action supported ", "yea"),
        ("FRC Action opposed ", "nay"),
    ),
    "hrc": (
        ("HRC supported ", "yea"),
        ("HRC opposed ", "nay"),
    ),
    "now": (
        ("FOR ", "yea"),
        ("in support of ", "yea"),
        ("AGAINST ", "nay"),
        ("against ", "nay"),
    ),
    "ffrf_action": (
        ("Voting \u2018Yea\u2019", "yea"),
        ("Voting 'Yea'", "yea"),
        ("Voting \u2018Yes\u2019", "yea"),
        ("Voting \u2018Nay\u2019", "nay"),
        ("Voting 'Nay'", "nay"),
        ("Voting \u2018No\u2019", "nay"),
    ),
    "cpac": (
        ("supported ", "yea"),
        ("opposed ", "nay"),
    ),
}

QUALIFIED_TARGET_PUBLISHERS = frozenset(_EXACT_PUBLISHER_TARGETS) | frozenset(_PREFIX_PUBLISHER_TARGETS)


def _normalized_kind(value: str | None) -> str:
    return re.sub(r"[\s\-]+", "_", (value or "").strip().casefold()).strip("_")


def publisher_target(publisher_id: str | None, item: dict) -> tuple[str | None, str | None, str | None]:
    """Qualify one publisher's preferred roll-call vote target without guessing.

    Returns ``(status_override, target, reason)`` where ``status_override`` is
    ``"not_applicable"`` for explicitly non-floor actions, ``"unresolved"`` for
    missing/unsupported targets, or ``None`` when ``target`` is ``"yea"``/``"nay"``.
    """
    kind_raw = item.get("item_kind_text")
    if publisher_id == "lcv" and kind_raw != "Scorecard Vote":
        return "not_applicable", None, "outside_lcv_floor_vote_method"
    if _normalized_kind(kind_raw) in _NON_FLOOR_VOTE_KINDS:
        return "not_applicable", None, "non_floor_vote_item_kind"
    literal = item.get("publisher_position_text")
    text = (literal or "").strip()
    if not text:
        return "unresolved", None, "missing_publisher_target"
    if publisher_id not in QUALIFIED_TARGET_PUBLISHERS:
        return "unresolved", None, "unsupported_publisher_target_rule"
    exact = _EXACT_PUBLISHER_TARGETS.get(publisher_id, {}).get(text)
    if exact is not None:
        return None, exact, None
    for prefix, mapped in _PREFIX_PUBLISHER_TARGETS.get(publisher_id, ()):
        if text.startswith(prefix):
            return None, mapped, None
    return "unresolved", None, "unsupported_publisher_target"


def selected_native_rows(snapshot, name, paths, columns, filter_column, values):
    """Verify complete native members; convert only the requested Arrow rows to Python.

    Results and votes need literal main fields, not source reconstruction. Their
    selected native schema, digest and complete population remain mandatory.
    """
    from spicy_regs.congress_receipts import policy as congress_policy
    from spicy_regs.local_data import file_signature, file_state_from_stat
    from spicy_regs.scorecards.analysis_inputs import published_input
    from spicy_regs.scorecards.etl import POLICIES
    from spicy_regs.sources import publication

    if name not in {"scorecard_member_item_results", "scorecard_metric_items", "member_votes",
                    "scorecard_methodologies", "bill_cosponsors"}:
        raise ValueError("Unsupported action-comparison native input")
    owner = publication.table_owner(snapshot, name + ".parquet")
    if owner is None or (name not in owner[1].get("etlReceipts", {}).get("datasets", ())
                         and not published_input(owner[1], name)):  # member_votes publishes none (ss-vse3)
        raise ValueError("Action comparison requires selected native inputs")
    members = publication.table_members(snapshot, name + ".parquet")
    if not paths or len(paths) != len(members):
        raise ValueError("Action comparison requires every selected native member")
    policy = POLICIES[name] if name in POLICIES else congress_policy(name)
    rows, population = [], 0
    selected = pa.array(sorted(values), type=pa.string())
    for path, member in zip(paths, members, strict=True):
        staged = file_state_from_stat(path.lstat())
        retained = path.resolve(strict=True)
        before = file_signature(retained)
        with retained.open("rb") as stream:
            digest = "sha256:" + file_digest(stream, "sha256").hexdigest()
        if digest != member.sha256 or retained.stat().st_size != member.byte_size:
            raise ValueError("Action-comparison input differs from its immutable pin")
        with pq.ParquetFile(retained) as source:
            if not source.schema_arrow.equals(policy.subject_schema, check_metadata=False):
                raise ValueError("Action-comparison input differs from its native schema")
            for batch in source.iter_batches(batch_size=2000, columns=list(columns)):
                population += batch.num_rows
                mask = pc.call_function("is_in", [batch.column(filter_column)], pc.SetLookupOptions(selected))
                rows.extend(batch.filter(mask).to_pylist())
        if (file_state_from_stat(path.lstat()) != staged or path.resolve(strict=True) != retained
                or file_signature(retained) != before):
            raise ValueError("Action-comparison input changed during its read")
    if population != owner[1]["tables"][name + ".parquet"]["rows"]:
        raise ValueError("Action-comparison input differs from its complete selected population")
    return rows


def _select_vote_reference(selected_refs: list[dict], snapshot_id: str) -> tuple[dict | None, str | None, str | None]:
    """Select the single agreed resolved roll-call link for an item.

    Corroborating occurrences (for example ``item:direct`` plus a source
    reference link resolving to the exact same ``vote_id``) are accepted;
    disagreeing ``vote_id``s or roll-call conflicts fail closed.
    """
    if not selected_refs:
        return None, "unresolved", "missing_or_repeated_exact_links"
    if any(r.get("source_snapshot_id") != snapshot_id for r in selected_refs):
        return None, "unresolved", "link_snapshot_mismatch"
    if any(r.get("resolution_status") == "conflict" for r in selected_refs):
        return None, "unresolved", "conflicting_roll_call_links"
    resolved_votes = [r for r in selected_refs if r.get("resolution_status") == "resolved" and r.get("vote_id")]
    if not resolved_votes:
        if all(
            r.get("resolution_status") == "resolved"
            and (r.get("bill_id") or r.get("amendment_id"))
            and not r.get("vote_id")
            and "no_roll_selected_from_bill_or_amendment_alone" in (r.get("reason") or "")
            for r in selected_refs
        ):
            return None, "not_applicable", "non_roll_call_item_link"
        return None, "unresolved", "unresolved_member_or_roll_call"
    vote_ids = {r["vote_id"] for r in resolved_votes}
    if len(vote_ids) != 1:
        return None, "unresolved", "conflicting_roll_call_links"
    primary = next((r for r in resolved_votes if r.get("reference_id") == "item:direct"), None)
    if primary is None:
        primary = sorted(resolved_votes, key=lambda r: r.get("reference_id") or "")[0]
    return primary, None, None


def compare_roll_calls(source, links, results, votes, official_members, input_pins, metric_items, *, scopes=None):
    """One descriptive outcome per original member-item result across qualified roll-call scorecards."""
    from spicy_regs.scorecards.resolution import _pins

    _pins(input_pins)
    editions = {
        r["scorecard_id"]: r
        for r in source["scorecards"]
        if scopes is None or r["scorecard_id"] in scopes
    }
    items = {(r["scorecard_id"], r["item_id"]): r for r in source["scorecard_items"]}
    members = defaultdict(list)
    references = defaultdict(list)
    for row in links["scorecard_member_links"]:
        members[row["scorecard_id"], row["publisher_member_key"]].append(row)
    for row in links["scorecard_item_links"]:
        references[row["scorecard_id"], row["item_id"]].append(row)
    official = {r["bioguide_id"]: r for r in official_members}
    lis_people = defaultdict(set)
    for person in official_members:
        if person.get("lis_id"):
            lis_people[person["lis_id"]].add(person["bioguide_id"])
    targets = defaultdict(list)
    for row in metric_items:
        targets[row["scorecard_id"], row["metric_id"], row["item_id"], row["participation_id"]].append(row)
    by_vote_bioguide: dict[tuple[str, str], list[dict]] = defaultdict(list)
    by_vote_lis_only: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in votes:
        vote_id = row["vote_id"]
        if row.get("bioguide_id"):
            by_vote_bioguide[vote_id, row["bioguide_id"]].append(row)
        elif row.get("lis_id"):
            by_vote_lis_only[vote_id, row["lis_id"]].append(row)
    output, seen = [], set()
    for result in results:
        scope = result["scorecard_id"]
        if scope not in editions:
            raise ValueError("Comparison selection includes an unselected scorecard result")
        identity = tuple(result[k] for k in KEYS)
        if not all(identity) or identity in seen:
            raise ValueError("Comparison results require distinct source identities")
        seen.add(identity)
        item = items.get((scope, result["item_id"]))
        edition = editions[scope]
        if (not edition.get("snapshot_id") or result["snapshot_id"] != edition["snapshot_id"]
                or item is None or item["snapshot_id"] != edition["snapshot_id"]):
            raise ValueError("Comparison source facts differ from the selected snapshot")
        row = dict.fromkeys(COLUMNS)
        row.update({key: result[key] for key in
                    ("scorecard_id", "result_id", "item_id", "publisher_member_key", "metric_id",
                     "capture_id", "source_url", "source_path")})
        row.update(source_snapshot_id=result["snapshot_id"], publisher_target_text=item.get("publisher_position_text"),
                   publisher_action_text=result["action_text"], publisher_credit_text=result["result_text"],
                   publisher_eligibility_text=result["eligibility_text"], rule_version=RULE_VERSION,
                   comparison_status="unresolved")
        output.append(row)
        publisher_id = edition.get("publisher_id")
        target_status, target, target_reason = publisher_target(publisher_id, item)
        if target_status == "not_applicable":
            row.update(comparison_status="not_applicable", reason=target_reason)
            continue
        selected_refs = references[scope, result["item_id"]]
        reference, ref_status, ref_reason = _select_vote_reference(selected_refs, result["snapshot_id"])
        if ref_status == "not_applicable":
            row.update(comparison_status="not_applicable", reason=ref_reason)
            continue
        if target is None:
            row["reason"] = target_reason
            continue
        if result["metric_id"]:
            contexts = targets[scope, result["metric_id"], result["item_id"], result["participation_id"]]
            if (len(contexts) != 1 or contexts[0]["snapshot_id"] != result["snapshot_id"]
                    or contexts[0]["publisher_position_text"] != item["publisher_position_text"]):
                row["reason"] = "missing_or_conflicting_metric_target"
                continue
        selected_members = members[scope, result["publisher_member_key"]]
        if len(selected_members) != 1:
            row["reason"] = "missing_or_repeated_exact_links"
            continue
        member = selected_members[0]
        if member.get("source_snapshot_id") != result["snapshot_id"]:
            row["reason"] = "link_snapshot_mismatch"
            continue
        if reference is None:
            row["reason"] = ref_reason
            continue
        if member.get("resolution_status") != "resolved" or not member.get("bioguide_id"):
            row["reason"] = "unresolved_member_or_roll_call"
            continue
        row.update(reference_id=reference["reference_id"], vote_id=reference["vote_id"],
                   bioguide_id=member["bioguide_id"])
        person = official.get(member["bioguide_id"])
        if person is None:
            row["reason"] = "member_absent_from_pinned_crosswalk"
            continue
        lis = person.get("lis_id")
        candidates = list(by_vote_bioguide.get((reference["vote_id"], member["bioguide_id"]), ()))
        if lis and lis_people[lis] == {member["bioguide_id"]}:
            candidates.extend(by_vote_lis_only.get((reference["vote_id"], lis), ()))
        if len(candidates) != 1:
            row["reason"] = "missing_or_ambiguous_official_vote"
            continue
        vote = candidates[0]
        if lis and vote.get("lis_id") and vote["lis_id"] != lis:
            row["reason"] = "conflicting_official_member_ids"
            continue
        row.update(official_member_key=vote["member_key"], official_position_text=vote["position"],
                   official_position_normalized=vote["position_normalized"])
        position = vote["position_normalized"]
        if position in {"yea", "nay"}:
            row.update(comparison_status="same" if position == target else "different", reason="exact_roll_call_position")
        elif position in {"not_voting", "present"}:
            row.update(comparison_status=position, reason="explicit_official_position")
        else:
            row["reason"] = "unsupported_official_position"
    return output


def compare_lcv_2025(source, links, results, votes, official_members, input_pins, metric_items):
    """Compatibility entry point for LCV 2025 roll-call comparison tests."""
    scopes = {
        r["scorecard_id"]
        for r in source["scorecards"]
        if r.get("publisher_id") == "lcv" and r.get("year_text") == "2025"
    }
    return compare_roll_calls(
        source, links, results, votes, official_members, input_pins, metric_items, scopes=scopes
    )


def sponsorship_method(source, methodologies):
    """Bind this one qualified item to the retained publisher's explicit cutoff."""
    editions = [r for r in source["scorecards"] if r["scorecard_id"] == SPONSORSHIP_SCOPE]
    if not editions:
        return None
    if len(editions) != 1 or any(editions[0].get(k) != v for k, v in
                                {"publisher_id": "common_cause", "year_text": "2022", "congress_text": "117"}.items()):
        raise ValueError("Historical sponsorship edition differs from the qualified scope")
    edition = editions[0]
    items = [r for r in source["scorecard_items"]
             if r["scorecard_id"] == SPONSORSHIP_SCOPE and r["item_id"] == SPONSORSHIP_ITEM]
    expected = {"item_kind_text": "BILLS TO COSPONSOR", "publisher_position_text": "BILLS TO COSPONSOR",
                "congress_text": "117", "chamber_text": "House", "bill_citation_text": "HR 1334",
                "title": "HR 1334, DISCLOSE Act", "snapshot_id": edition.get("snapshot_id")}
    if len(items) != 1 or not edition.get("snapshot_id") or any(items[0].get(k) != v for k, v in expected.items()):
        raise ValueError("Historical sponsorship item differs from the qualified scope")
    selected = [r for r in methodologies if r["scorecard_id"] == SPONSORSHIP_SCOPE
                and r["methodology_id"] == "House:methodology"]
    if (len(selected) != 1 or selected[0].get("snapshot_id") != edition["snapshot_id"]
            or selected[0].get("chamber_text") != "House"
            or not all(selected[0].get(k) for k in ("capture_id", "source_url", "source_path"))):
        raise ValueError("Historical sponsorship requires its pinned House methodology")
    methodology = selected[0]
    try:
        text = json.loads(methodology["methodology_text"])
        literal = text["source_cutoff_date_text"]
        statement = " ".join(text["source_methodology_text"].split())
        cutoff = datetime.strptime(literal, "%B %d, %Y").date()
    except (TypeError, KeyError, ValueError, AttributeError) as exc:
        raise ValueError("Historical sponsorship requires its explicit source cutoff") from exc
    if (literal != "September 6, 2022" or
            "All cosponsors listed in this Democracy Scorecard were taken from the Congress.gov website as of "
            + literal + "." not in statement):
        raise ValueError("Historical sponsorship source cutoff changed or is unsupported")
    return {"rule_version": SPONSORSHIP_RULE_VERSION, "scorecard_id": SPONSORSHIP_SCOPE,
            "item_id": SPONSORSHIP_ITEM, "bill_id": SPONSORSHIP_BILL,
            "cutoff_date": cutoff.isoformat(), "source_cutoff_date_text": literal,
            "methodology": {k: methodology[k] for k in METHODOLOGY_COLUMNS},
            "reference_ids": ["item:direct", "reference:header-citation-0"]}


def compare_historical_sponsorship(source, links, results, cosponsors, input_pins, method):
    """One selected bill; dates on the cutoff day and missing rows stay unresolved.

    The existing pins identify the exact methodology and official date rows.
    All reference occurrences remain in item links; item:direct is the primary
    comparison reference only after every occurrence resolves to the same bill.
    """
    from spicy_regs.scorecards.resolution import _pins

    if method is None:
        if results:
            raise ValueError("Historical sponsorship results lack a qualified method")
        return []
    _pins(input_pins)
    cutoff = date.fromisoformat(method["cutoff_date"])
    item = next(r for r in source["scorecard_items"]
                if r["scorecard_id"] == SPONSORSHIP_SCOPE and r["item_id"] == SPONSORSHIP_ITEM)
    references = [r for r in links["scorecard_item_links"]
                  if r["scorecard_id"] == SPONSORSHIP_SCOPE and r["item_id"] == SPONSORSHIP_ITEM]
    members, official = defaultdict(list), defaultdict(list)
    for row in links["scorecard_member_links"]:
        if row["scorecard_id"] == SPONSORSHIP_SCOPE:
            members[row["publisher_member_key"]].append(row)
    for row in cosponsors:
        if row["bill_id"] != SPONSORSHIP_BILL:
            raise ValueError("Historical sponsorship includes an unselected official bill")
        official[row["bioguide_id"]].append(row)
    output, seen = [], set()
    for result in results:
        identity = tuple(result[k] for k in KEYS)
        if (result["scorecard_id"] != SPONSORSHIP_SCOPE or result["item_id"] != SPONSORSHIP_ITEM
                or not all(identity) or identity in seen or result["snapshot_id"] != item["snapshot_id"]):
            raise ValueError("Historical sponsorship result differs from the selected source identity")
        seen.add(identity)
        row = dict.fromkeys(COLUMNS)
        row.update({k: result[k] for k in ("scorecard_id", "result_id", "item_id", "publisher_member_key", "metric_id",
                                         "capture_id", "source_url", "source_path")})
        row.update(source_snapshot_id=result["snapshot_id"], publisher_target_text=item["publisher_position_text"],
                   publisher_action_text=result["action_text"], publisher_credit_text=result["result_text"],
                   publisher_eligibility_text=result["eligibility_text"], rule_version=SPONSORSHIP_RULE_VERSION,
                   comparison_status="unresolved")
        output.append(row)
        if (sorted(r["reference_id"] for r in references) != method["reference_ids"]
                or any(r.get("resolution_status") != "resolved" or r.get("bill_id") != SPONSORSHIP_BILL
                       or r.get("vote_id") or r.get("amendment_id")
                       or r.get("source_snapshot_id") != result["snapshot_id"] for r in references)):
            row["reason"] = "missing_or_conflicting_exact_bill_references"
            continue
        row["reference_id"] = "item:direct"
        candidates = members[result["publisher_member_key"]]
        if (len(candidates) != 1 or candidates[0].get("resolution_status") != "resolved"
                or not candidates[0].get("bioguide_id")
                or candidates[0].get("source_snapshot_id") != result["snapshot_id"]):
            row["reason"] = "missing_or_ambiguous_exact_member"
            continue
        row["bioguide_id"] = candidates[0]["bioguide_id"]
        records = official[row["bioguide_id"]]
        if len(records) != 1:
            row["reason"] = "missing_or_ambiguous_official_cosponsorship"
            continue
        record = records[0]
        row["official_member_key"] = f"{SPONSORSHIP_BILL}/cosponsor_index:{record['cosponsor_index']}"
        try:
            start = date.fromisoformat(record["sponsorship_date"])
            end_text = record["sponsorship_withdrawn_date"]
            end = date.fromisoformat(end_text) if end_text is not None else None
            if (start.isoformat() != record["sponsorship_date"] or
                    (end is not None and (end.isoformat() != end_text or end < start))):
                raise ValueError("Invalid official date interval")
        except (TypeError, ValueError):
            row["reason"] = "missing_or_invalid_official_sponsorship_dates"
            continue
        if start == cutoff or end == cutoff:
            row["reason"] = "same_day_cutoff_time_unknown"
            continue
        active = start < cutoff and (end is None or end > cutoff)
        row.update(official_position_normalized="cosponsor_at_cutoff" if active else "not_cosponsor_at_cutoff",
                   comparison_status="same" if active else "different",
                   reason="dated_official_cosponsorship_at_source_cutoff")
    return output


def build_comparisons(snapshot, paths, source, links, official_members, input_pins):
    editions = {r["scorecard_id"]: r for r in source["scorecards"]}
    vote_linked_scopes = {
        r["scorecard_id"]
        for r in links["scorecard_item_links"]
        if r.get("resolution_status") == "resolved" and r.get("vote_id") and r["scorecard_id"] in editions
    }
    sponsorship_scopes = {SPONSORSHIP_SCOPE} if SPONSORSHIP_SCOPE in editions else set()
    selected_scopes = vote_linked_scopes | sponsorship_scopes
    results = selected_native_rows(snapshot, "scorecard_member_item_results", paths["scorecard_member_item_results"],
                                   RESULT_COLUMNS, "scorecard_id", selected_scopes)
    vote_ids = {
        r["vote_id"]
        for r in links["scorecard_item_links"]
        if r["scorecard_id"] in vote_linked_scopes and r.get("resolution_status") == "resolved" and r.get("vote_id")
    }
    votes = selected_native_rows(snapshot, "member_votes", paths["member_votes"], VOTE_COLUMNS, "vote_id", vote_ids)
    metrics = selected_native_rows(snapshot, "scorecard_metric_items", paths["scorecard_metric_items"],
                                   METRIC_COLUMNS, "scorecard_id", vote_linked_scopes)
    methodologies = selected_native_rows(snapshot, "scorecard_methodologies", paths["scorecard_methodologies"],
                                         METHODOLOGY_COLUMNS, "scorecard_id", sponsorship_scopes)
    method = sponsorship_method(source, methodologies)
    cosponsors = selected_native_rows(snapshot, "bill_cosponsors", paths["bill_cosponsors"],
                                      COSPONSOR_COLUMNS, "bill_id", {SPONSORSHIP_BILL} if method else set())
    roll_call_results = [
        r for r in results
        if r["scorecard_id"] in vote_linked_scopes
        and not (method and r["scorecard_id"] == SPONSORSHIP_SCOPE and r["item_id"] == SPONSORSHIP_ITEM)
    ]
    rows = compare_roll_calls(
        source, links, roll_call_results, votes, official_members, input_pins, metrics, scopes=vote_linked_scopes
    )
    rows.extend(compare_historical_sponsorship(
        source,
        links,
        [r for r in results if r["scorecard_id"] == SPONSORSHIP_SCOPE and r["item_id"] == SPONSORSHIP_ITEM],
        cosponsors,
        input_pins,
        method,
    ))
    by_publisher: dict[str, Counter] = defaultdict(Counter)
    compared_scopes = set()
    for row in rows:
        compared_scopes.add(row["scorecard_id"])
        pub_id = editions[row["scorecard_id"]].get("publisher_id") or "unknown"
        by_publisher[pub_id][row["comparison_status"]] += 1
    coverage = {
        "vote_linked_scopes": sorted(vote_linked_scopes),
        "compared_scopes": sorted(compared_scopes),
        "zero_result_vote_linked_scopes": sorted(vote_linked_scopes - compared_scopes),
        "by_publisher": {pub: dict(sorted(counts.items())) for pub, counts in sorted(by_publisher.items())},
        "historical_sponsorship": method,
    }
    return rows, coverage
