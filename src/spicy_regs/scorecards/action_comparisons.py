"""LCV 2025 explicit targets versus pinned official floor-vote positions."""

from collections import defaultdict
from hashlib import file_digest

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

RULE_VERSION = "lcv-2025-roll-call-comparison-v1"
TABLE = "scorecard_action_comparisons"
COLUMNS = (
    "scorecard_id", "result_id", "source_snapshot_id", "item_id", "publisher_member_key", "metric_id",
    "reference_id", "vote_id", "bioguide_id", "official_member_key",
    "publisher_target_text", "publisher_action_text", "publisher_credit_text", "publisher_eligibility_text",
    "official_position_text", "official_position_normalized", "comparison_status", "reason",
    "rule_version", "input_pins_json", "capture_id", "source_url", "source_path",
)
KEYS = ("scorecard_id", "item_id", "publisher_member_key", "result_id")
RESULT_COLUMNS = (
    "scorecard_id", "result_id", "snapshot_id", "item_id", "publisher_member_key", "metric_id",
    "participation_id", "action_text", "result_text", "eligibility_text", "capture_id", "source_url", "source_path",
)
METRIC_COLUMNS = ("scorecard_id", "snapshot_id", "metric_id", "item_id", "participation_id", "publisher_position_text")
VOTE_COLUMNS = ("vote_id", "member_key", "bioguide_id", "lis_id", "position", "position_normalized")
STATUSES = frozenset({"same", "different", "not_voting", "present", "unresolved", "not_applicable"})


def selected_native_rows(snapshot, name, paths, columns, filter_column, values):
    """Verify complete native members; convert only the requested Arrow rows to Python.

    Results and votes need literal main fields, not source reconstruction. Their
    selected native schema, digest and complete population remain mandatory.
    """
    from spicy_regs.congress_receipts import policy as congress_policy
    from spicy_regs.local_data import file_signature, file_state_from_stat
    from spicy_regs.scorecards.etl import POLICIES
    from spicy_regs.sources import publication

    if name not in {"scorecard_member_item_results", "scorecard_metric_items", "member_votes"}:
        raise ValueError("Unsupported action-comparison native input")
    owner = publication.table_owner(snapshot, name + ".parquet")
    if owner is None or name not in owner[1].get("etlReceipts", {}).get("datasets", ()):
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


def compare_lcv_2025(source, links, results, votes, official_members, input_pins, metric_items):
    """One descriptive outcome per original LCV result, without changing credit."""
    from spicy_regs.scorecards.resolution import _pins

    pins = _pins(input_pins)
    editions = {r["scorecard_id"]: r for r in source["scorecards"]
                if r.get("publisher_id") == "lcv" and r.get("year_text") == "2025"}
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
    by_vote = defaultdict(list)
    for row in votes:
        by_vote[row["vote_id"]].append(row)
    output, seen = [], set()
    for result in results:
        scope = result["scorecard_id"]
        if scope not in editions:
            raise ValueError("Comparison selection includes a non-LCV-2025 result")
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
                   input_pins_json=pins, comparison_status="unresolved")
        output.append(row)
        # Non-floor actions need historical sponsorship/cutoff evidence, deferred here.
        if item.get("item_kind_text") != "Scorecard Vote":
            row.update(comparison_status="not_applicable", reason="outside_lcv_floor_vote_method")
            continue
        target = {"Yes": "yea", "No": "nay"}.get(item.get("publisher_position_text"))
        if target is None:
            row["reason"] = "unsupported_publisher_target"
            continue
        if result["metric_id"]:
            contexts = targets[scope, result["metric_id"], result["item_id"], result["participation_id"]]
            if (len(contexts) != 1 or contexts[0]["snapshot_id"] != result["snapshot_id"]
                    or contexts[0]["publisher_position_text"] != item["publisher_position_text"]):
                row["reason"] = "missing_or_conflicting_metric_target"
                continue
        selected_members = members[scope, result["publisher_member_key"]]
        selected_refs = references[scope, result["item_id"]]
        if len(selected_members) != 1 or len(selected_refs) != 1:
            row["reason"] = "missing_or_repeated_exact_links"
            continue
        member, reference = selected_members[0], selected_refs[0]
        if any(r.get("source_snapshot_id") != result["snapshot_id"] for r in (member, reference)):
            row["reason"] = "link_snapshot_mismatch"
            continue
        if (member.get("resolution_status") != "resolved" or not member.get("bioguide_id")
                or reference.get("resolution_status") != "resolved" or not reference.get("vote_id")):
            row["reason"] = "unresolved_member_or_roll_call"
            continue
        row.update(reference_id=reference["reference_id"], vote_id=reference["vote_id"],
                   bioguide_id=member["bioguide_id"])
        person = official.get(member["bioguide_id"])
        if person is None:
            row["reason"] = "member_absent_from_pinned_crosswalk"
            continue
        lis = person.get("lis_id")
        candidates = [v for v in by_vote[reference["vote_id"]]
                      if (v.get("bioguide_id") == member["bioguide_id"]
                          or (not v.get("bioguide_id") and lis and lis_people[lis] == {member["bioguide_id"]}
                              and v.get("lis_id") == lis))]
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


def build_comparisons(snapshot, paths, source, links, official_members, input_pins):
    scopes = {r["scorecard_id"] for r in source["scorecards"]
              if r.get("publisher_id") == "lcv" and r.get("year_text") == "2025"}
    results = selected_native_rows(snapshot, "scorecard_member_item_results", paths["scorecard_member_item_results"],
                                   RESULT_COLUMNS, "scorecard_id", scopes)
    vote_ids = {r["vote_id"] for r in links["scorecard_item_links"]
                if r["scorecard_id"] in scopes and r.get("resolution_status") == "resolved" and r.get("vote_id")}
    votes = selected_native_rows(snapshot, "member_votes", paths["member_votes"], VOTE_COLUMNS, "vote_id", vote_ids)
    metrics = selected_native_rows(snapshot, "scorecard_metric_items", paths["scorecard_metric_items"],
                                  METRIC_COLUMNS, "scorecard_id", scopes)
    return compare_lcv_2025(source, links, results, votes, official_members, input_pins, metrics)
