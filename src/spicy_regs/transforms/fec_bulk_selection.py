"""Bind the proven individual-bulk representations to their source collections.

This policy selects a retained reported snapshot. It does not apply unqualified
daily correction streams or claim a current, net or source-complete money total.
"""

import hashlib
import json

import pyarrow as pa

from spicy_regs.fec_versions import INDIVIDUAL_SNAPSHOT_POLICY

from .fec_query import _digest, _json

POLICY_VERSION = INDIVIDUAL_SNAPSHOT_POLICY
SELECTION_SCHEMA = pa.schema(
    [
        (name, pa.string())
        for name in (
            "record_id policy_version purpose collection_id source_generation_pin source_sha256 "
            "source_member_name selected_collection_id selection_status selection_reason "
            "equivalence_evidence_sha256 current_record_status correction_applicability_status"
        ).split()
    ]
    + [("source_member_ordinal", pa.int32()), ("source_observations", pa.int64())]
)


def individual_snapshot_selection(proof_bytes, *, proof_sha256, source_generation_pin, collections, source_counts):
    """Validate exact proof membership against the pinned manifest and census.

    The caller must independently verify the receipt's byte-comparison procedure;
    this checks its immutable identity and complete structural agreement with the
    release's selected inputs. Counts alone never establish equivalence.
    """
    _digest(proof_sha256)
    _digest(source_generation_pin)
    if "sha256:" + hashlib.sha256(proof_bytes).hexdigest() != proof_sha256:
        raise ValueError("Individual equivalence receipt digest differs")
    proof = json.loads(proof_bytes)
    if (
        proof["status"] != "passed-exact-complete-member-sequence-equivalence"
        or proof["source_generation_pin"] != source_generation_pin
    ):
        raise ValueError("Individual equivalence receipt has a different qualification or generation")
    archive_pin = _digest(proof["archive_sha256"])
    members = {0: dict(member_name="itcont.txt", linefeeds=proof["main_rows"])}
    offset = total_rows = 0
    for segment in proof["segments"]:
        ordinal = segment["member_ordinal"]
        if (
            type(ordinal) is not int
            or ordinal <= 0
            or ordinal in members
            or segment["main_offset"] != offset
            or segment["bytes"] <= 0
            or segment["linefeeds"] <= 0
            or not segment["ends_with_linefeed"]
        ):
            raise ValueError("Individual equivalence proof has incomplete or ambiguous member coverage")
        _digest(segment["sha256"])
        members[ordinal] = segment
        offset += segment["bytes"]
        total_rows += segment["linefeeds"]
    if offset != proof["main_bytes"] or total_rows != proof["main_rows"] or len(members) != proof["date_members"] + 1:
        raise ValueError("Individual equivalence proof does not cover every main byte and row")
    selected = {}
    for collection in collections:
        scope = collection.get("scope", {})
        if collection.get("profile") != "positional" or scope.get("capture", {}).get("responseSha256") != archive_pin:
            continue
        member = scope.get("member", {})
        ordinal = member.get("ordinal")
        if ordinal in selected or ordinal not in members or member.get("name") != members[ordinal]["member_name"]:
            raise ValueError("Selected collection differs from the exact proven archive member")
        if (
            scope.get("format") != "delimited"
            or scope.get("delimiter") != "|"
            or scope.get("quoting") != "literal"
            or scope.get("encoding") != "utf-8"
            or collection.get("field_mapping")
            != {"header_collection_id": "bulk-2026-indiv-header", "header_row_ordinal": 0, "data_has_header": False}
        ):
            raise ValueError("Individual representations do not share the qualified decoding and dictionary")
        cid = collection["collection_id"]
        if source_counts.get(cid) != members[ordinal]["linefeeds"]:
            raise ValueError("Sealed source membership differs from the proven original rows")
        selected[ordinal] = cid
    if selected.keys() != members.keys() or selected[0] != proof["main_collection_id"]:
        raise ValueError("Individual proof members differ from the selected source collections")
    rows = []
    for ordinal, cid in sorted(selected.items()):
        identity = [POLICY_VERSION, source_generation_pin, archive_pin, cid, proof_sha256]
        rows.append(
            dict(
                record_id="sha256:" + hashlib.sha256(_json(identity).encode()).hexdigest(),
                policy_version=POLICY_VERSION,
                purpose="retained-reported-individual-contribution-snapshot",
                collection_id=cid,
                source_generation_pin=source_generation_pin,
                source_sha256=archive_pin,
                source_member_ordinal=ordinal,
                source_member_name=members[ordinal]["member_name"],
                source_observations=source_counts[cid],
                selected_collection_id=selected[0],
                selection_status="included" if ordinal == 0 else "duplicate",
                selection_reason="selected-main-representation"
                if ordinal == 0
                else "proven-equivalent-date-representation",
                equivalence_evidence_sha256=proof_sha256,
                current_record_status="unqualified",
                correction_applicability_status="unqualified",
            )
        )
    return rows
