"""Bind proven equivalent FEC source representations to their collections.

These policies select canonical retained representations without dropping
distinct source occurrences or provenance. They do not apply unqualified daily
correction streams or claim current, net, or source-complete money totals.
"""

from collections import Counter, defaultdict
import hashlib
import io
import json
from pathlib import Path
import zipfile

import pyarrow as pa

from spicy_regs.fec_versions import CROSS_SOURCE_SELECTION_POLICY, INDIVIDUAL_SNAPSHOT_POLICY

from .fec_query import _digest, _json

POLICY_VERSION = INDIVIDUAL_SNAPSHOT_POLICY
CROSS_SOURCE_POLICY_VERSION = CROSS_SOURCE_SELECTION_POLICY
API_DEDUP_PURPOSE = "cross-source-api-blob-representation"
ARCHIVE_INNER_DEDUP_PURPOSE = "cross-source-archive-inner-member-representation"
WEBALL_WEBL_DEDUP_PURPOSE = "cross-source-candidate-web-superset-representation"
FOIA_EDITION_DEDUP_PURPOSE = "cross-source-foia-annual-edition-representation"
_API_DOMAIN_PROFILES = frozenset({"committee", "candidate", "filing", "legal", "audit"})
_API_REPRESENTATION_PROFILES = _API_DOMAIN_PROFILES | {"document"}

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


def _selection_row(
    *,
    policy_version: str,
    purpose: str,
    collection_id: str,
    source_generation_pin: str,
    source_sha256: str,
    source_member_ordinal: int | None,
    source_member_name: str | None,
    source_observations: int,
    selected_collection_id: str,
    selection_status: str,
    selection_reason: str,
    equivalence_evidence_sha256: str,
) -> dict:
    identity = [
        policy_version,
        purpose,
        source_generation_pin,
        source_sha256,
        collection_id,
        source_member_ordinal,
        source_member_name,
        equivalence_evidence_sha256,
    ]
    return dict(
        record_id="sha256:" + hashlib.sha256(_json(identity).encode()).hexdigest(),
        policy_version=policy_version,
        purpose=purpose,
        collection_id=collection_id,
        source_generation_pin=source_generation_pin,
        source_sha256=_digest(source_sha256),
        source_member_ordinal=source_member_ordinal,
        source_member_name=source_member_name,
        source_observations=source_observations,
        selected_collection_id=selected_collection_id,
        selection_status=selection_status,
        selection_reason=selection_reason,
        equivalence_evidence_sha256=_digest(equivalence_evidence_sha256),
        current_record_status="unqualified",
        correction_applicability_status="unqualified",
    )


def _collection_scope(collection: dict) -> dict:
    scope = collection.get("scope")
    if isinstance(scope, dict):
        return scope
    raw = collection.get("requested_scope_json")
    if isinstance(raw, str) and raw:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
    return {}


def _collection_count(collection: dict, source_counts: dict[str, int] | None) -> int:
    cid = collection["collection_id"]
    if source_counts is not None and cid in source_counts:
        count = source_counts[cid]
    else:
        count = collection.get("record_count", 0)
    if type(count) is not int or count < 0:
        raise ValueError(f"Collection observation count must be a nonnegative integer: {cid}")
    return count


def api_blob_representation_selection(collections, *, source_generation_pin, source_counts=None):
    """Deduplicate cross-registered API blobs sharing exact responseSha256.

    Retains every distinct source collection and provenance entry while marking
    duplicate registrations (e.g., ``retained-api-*`` duplicating domain-profile
    collections such as ``committee``, ``filing``, ``legal``, or ``candidate``,
    or repeated ``retained-api-*`` / ``research-capture-*`` captures of identical
    response bytes) as ``duplicate`` pointing to the canonical
    ``selected_collection_id``.
    """
    _digest(source_generation_pin)
    groups: dict[tuple[str, str | None], list[dict]] = defaultdict(list)
    seen_pairs: set[tuple[str, str, str | None]] = set()
    for collection in collections:
        profile = collection.get("profile")
        if profile not in _API_REPRESENTATION_PROFILES:
            continue
        scope = _collection_scope(collection)
        captures = scope.get("captures")
        if captures is None and isinstance(scope.get("capture"), dict):
            captures = [scope["capture"]]
        if not isinstance(captures, list):
            continue
        member = scope.get("member")
        member_name = member.get("name") if isinstance(member, dict) else None
        member_ordinal = member.get("ordinal") if isinstance(member, dict) else None
        cid = collection["collection_id"]
        count = _collection_count(collection, source_counts)
        for capture in captures:
            sha = capture.get("responseSha256") if isinstance(capture, dict) else None
            if not sha:
                continue
            sha = _digest(sha)
            key = (cid, sha, member_name)
            if key in seen_pairs:
                continue
            seen_pairs.add(key)
            groups[(sha, member_name)].append(
                dict(
                    collection_id=cid,
                    profile=profile,
                    source_sha256=sha,
                    source_member_ordinal=member_ordinal,
                    source_member_name=member_name,
                    request_url=capture.get("requestUrl"),
                    source_observations=count,
                )
            )
    rows = []
    for (sha, member_name), entries in sorted(groups.items()):
        if len(entries) < 2:
            continue

        def rank(entry: dict) -> tuple[int, int, int, str]:
            is_domain = entry["profile"] in _API_DOMAIN_PROFILES
            has_rows = entry["source_observations"] > 0
            is_generic_retained = entry["collection_id"].startswith(("retained-api-", "research-capture-"))
            return (
                0 if (is_domain and has_rows) else 1 if has_rows else 2 if is_domain else 3,
                1 if is_generic_retained else 0,
                0 if is_domain else 1,
                entry["collection_id"],
            )

        ordered = sorted(entries, key=rank)
        winner = ordered[0]
        proof = dict(
            policy_version=CROSS_SOURCE_POLICY_VERSION,
            purpose=API_DEDUP_PURPOSE,
            source_generation_pin=source_generation_pin,
            source_sha256=sha,
            source_member_name=member_name,
            selected_collection_id=winner["collection_id"],
            members=[
                dict(
                    collection_id=e["collection_id"],
                    profile=e["profile"],
                    request_url=e["request_url"],
                    source_observations=e["source_observations"],
                )
                for e in ordered
            ],
        )
        proof_sha = "sha256:" + hashlib.sha256(_json(proof).encode()).hexdigest()
        winner_is_domain = winner["profile"] in _API_DOMAIN_PROFILES
        for entry in ordered:
            included = entry["collection_id"] == winner["collection_id"]
            if included:
                reason = (
                    "selected-domain-api-representation" if winner_is_domain else "selected-primary-api-representation"
                )
            else:
                reason = (
                    "duplicate-api-blob-covered-by-domain-profile"
                    if winner_is_domain and entry["profile"] == "document"
                    else "duplicate-identical-api-response-sha256"
                )
            rows.append(
                _selection_row(
                    policy_version=CROSS_SOURCE_POLICY_VERSION,
                    purpose=API_DEDUP_PURPOSE,
                    collection_id=entry["collection_id"],
                    source_generation_pin=source_generation_pin,
                    source_sha256=sha,
                    source_member_ordinal=entry["source_member_ordinal"],
                    source_member_name=entry["source_member_name"],
                    source_observations=entry["source_observations"],
                    selected_collection_id=winner["collection_id"],
                    selection_status="included" if included else "duplicate",
                    selection_reason=reason,
                    equivalence_evidence_sha256=proof_sha,
                )
            )
    return rows


def archive_inner_member_selection(proof_bytes, *, proof_sha256, source_generation_pin, collections, source_counts):
    """Deduplicate archives wrapping byte-identical inner members (e.g., cm24).

    Two distinct outer ZIP archives may wrap the exact same inner member bytes
    (verified by inner SHA-256, byte size, line count, and identical decoding).
    Both collections and outer archive digests stay retained; the duplicate
    collection is marked ``duplicate`` against the canonical ``selected_collection_id``.
    """
    _digest(proof_sha256)
    _digest(source_generation_pin)
    if "sha256:" + hashlib.sha256(proof_bytes).hexdigest() != proof_sha256:
        raise ValueError("Archive inner-member equivalence receipt digest differs")
    proof = json.loads(proof_bytes)
    if (
        proof.get("status") != "passed-exact-inner-member-byte-equivalence"
        or proof.get("source_generation_pin") != source_generation_pin
    ):
        raise ValueError("Archive inner-member proof has a different qualification or generation")
    inner_sha = _digest(proof["inner_member_sha256"])
    member_name = proof["member_name"]
    inner_bytes = proof["inner_member_bytes"]
    linefeeds = proof["linefeeds"]
    if type(inner_bytes) is not int or inner_bytes <= 0 or type(linefeeds) is not int or linefeeds <= 0:
        raise ValueError("Archive inner-member proof requires positive byte and line counts")
    selected_cid = proof["selected_collection_id"]
    declared = proof.get("collections")
    if not isinstance(declared, list) or len(declared) < 2:
        raise ValueError("Archive inner-member proof requires at least two equivalent collections")
    by_cid = {c["collection_id"]: c for c in collections}
    decoding = None
    verified = []
    seen_cids = set()
    for item in declared:
        cid = item["collection_id"]
        if cid in seen_cids or cid not in by_cid:
            raise ValueError(f"Archive inner-member proof collection missing or repeated: {cid}")
        seen_cids.add(cid)
        outer_sha = _digest(item["outer_archive_sha256"])
        col = by_cid[cid]
        scope = _collection_scope(col)
        capture_sha = (scope.get("capture") or {}).get("responseSha256")
        member = scope.get("member") or {}
        if capture_sha != outer_sha or member.get("name") != member_name:
            raise ValueError(f"Collection scope differs from proven inner member: {cid}")
        if item.get("inner_member_sha256") != inner_sha or item.get("inner_member_bytes") != inner_bytes:
            raise ValueError(f"Declared inner member digest differs for {cid}")
        sig = (scope.get("format"), scope.get("delimiter"), scope.get("quoting"), scope.get("encoding"))
        if decoding is None:
            decoding = sig
        elif sig != decoding:
            raise ValueError("Equivalent archive members do not share identical decoding")
        count = _collection_count(col, source_counts)
        if count != linefeeds:
            raise ValueError(f"Collection row count differs from proven inner member linefeeds: {cid}")
        verified.append((cid, outer_sha, member.get("ordinal"), count))
    if selected_cid not in seen_cids:
        raise ValueError("Selected canonical archive collection is absent from proof")
    rows = []
    for cid, outer_sha, ordinal, count in sorted(verified):
        included = cid == selected_cid
        rows.append(
            _selection_row(
                policy_version=CROSS_SOURCE_POLICY_VERSION,
                purpose=ARCHIVE_INNER_DEDUP_PURPOSE,
                collection_id=cid,
                source_generation_pin=source_generation_pin,
                source_sha256=outer_sha,
                source_member_ordinal=ordinal,
                source_member_name=member_name,
                source_observations=count,
                selected_collection_id=selected_cid,
                selection_status="included" if included else "duplicate",
                selection_reason="selected-canonical-bulk-archive-member"
                if included
                else "proven-byte-identical-inner-archive-member",
                equivalence_evidence_sha256=proof_sha256,
            )
        )
    return rows


def weball_webl_subset_selection(proof_bytes, *, proof_sha256, source_generation_pin, collections, source_counts):
    """Mark proven ``webl`` House/Senate candidate summaries as duplicates of ``weball``.

    Requires an exact row-equivalence proof showing 100% of ``webl`` mapped rows
    match ``weball`` rows identically for the same cycle on identity, coverage end,
    measures, and attributes. Zero rows are deleted from source tables.
    """
    _digest(proof_sha256)
    _digest(source_generation_pin)
    if "sha256:" + hashlib.sha256(proof_bytes).hexdigest() != proof_sha256:
        raise ValueError("Candidate web subset proof digest differs")
    proof = json.loads(proof_bytes)
    if (
        proof.get("status") != "passed-exact-subset-row-equivalence"
        or proof.get("source_generation_pin") != source_generation_pin
    ):
        raise ValueError("Candidate web subset proof has a different qualification or generation")
    pairs = proof.get("pairs")
    if not isinstance(pairs, list) or not pairs:
        raise ValueError("Candidate web subset proof requires at least one cycle pair")
    by_cid = {c["collection_id"]: c for c in collections}
    rows = []
    seen = set()
    for pair in pairs:
        sup_cid, sub_cid = pair["superset_collection_id"], pair["subset_collection_id"]
        if sup_cid == sub_cid or sup_cid in seen or sub_cid in seen:
            raise ValueError("Candidate web subset proof repeats or conflates collection IDs")
        seen.update((sup_cid, sub_cid))
        if sup_cid not in by_cid or sub_cid not in by_cid:
            raise ValueError("Candidate web subset proof names a missing collection")
        sup_rows, sub_rows = pair["superset_rows"], pair["subset_rows"]
        matched_rows = pair["matched_subset_rows"]
        if (
            type(sup_rows) is not int
            or type(sub_rows) is not int
            or type(matched_rows) is not int
            or sub_rows <= 0
            or matched_rows != sub_rows
            or sup_rows < sub_rows
        ):
            raise ValueError("Candidate web subset proof does not cover 100% of subset rows")
        for cid, expected_sha, expected_rows, included in (
            (sup_cid, _digest(pair["superset_sha256"]), sup_rows, True),
            (sub_cid, _digest(pair["subset_sha256"]), sub_rows, False),
        ):
            col = by_cid[cid]
            scope = _collection_scope(col)
            capture_sha = (scope.get("capture") or {}).get("responseSha256")
            member = scope.get("member") or {}
            if capture_sha != expected_sha:
                raise ValueError(f"Candidate web collection digest differs from proof: {cid}")
            count = _collection_count(col, source_counts)
            if count != expected_rows:
                raise ValueError(f"Candidate web collection count differs from proof: {cid}")
            rows.append(
                _selection_row(
                    policy_version=CROSS_SOURCE_POLICY_VERSION,
                    purpose=WEBALL_WEBL_DEDUP_PURPOSE,
                    collection_id=cid,
                    source_generation_pin=source_generation_pin,
                    source_sha256=expected_sha,
                    source_member_ordinal=member.get("ordinal"),
                    source_member_name=member.get("name"),
                    source_observations=count,
                    selected_collection_id=sup_cid,
                    selection_status="included" if included else "duplicate",
                    selection_reason="selected-all-candidates-superset"
                    if included
                    else "proven-house-senate-subset-of-weball",
                    equivalence_evidence_sha256=proof_sha256,
                )
            )
    return rows


def foia_dual_edition_selection(proof_bytes, *, proof_sha256, source_generation_pin, collections, source_counts):
    """Deduplicate FOIA annual report editions whose extracted metrics match identically.

    Preserves both the primary agency publication (e.g. ``fec.gov`` XML) and the
    mirror edition (e.g. ``foia.gov`` ZIP member) along with differing creation
    date provenance, while marking the mirror ``duplicate``.
    """
    _digest(proof_sha256)
    _digest(source_generation_pin)
    if "sha256:" + hashlib.sha256(proof_bytes).hexdigest() != proof_sha256:
        raise ValueError("FOIA dual-edition equivalence proof digest differs")
    proof = json.loads(proof_bytes)
    if (
        proof.get("status") != "passed-exact-foia-metric-multiset-equivalence"
        or proof.get("source_generation_pin") != source_generation_pin
    ):
        raise ValueError("FOIA dual-edition proof has a different qualification or generation")
    metric_count = proof.get("metric_count")
    if type(metric_count) is not int or metric_count <= 0:
        raise ValueError("FOIA dual-edition proof requires a positive metric count")
    if proof.get("symmetric_diff_count") != 0:
        raise ValueError("FOIA dual-edition proof cannot admit non-identical metric sets")
    editions = proof.get("editions")
    if not isinstance(editions, list) or len(editions) < 2:
        raise ValueError("FOIA dual-edition proof requires at least two editions")
    selected_cid = proof["selected_collection_id"]
    by_cid = {c["collection_id"]: c for c in collections}
    seen = set()
    rows = []
    for edition in editions:
        cid = edition["collection_id"]
        if cid in seen or cid not in by_cid:
            raise ValueError(f"FOIA edition missing or repeated: {cid}")
        seen.add(cid)
        sha = _digest(edition["source_sha256"])
        col = by_cid[cid]
        scope = _collection_scope(col)
        capture_sha = (scope.get("capture") or {}).get("responseSha256")
        member = scope.get("member") or {}
        if capture_sha != sha:
            raise ValueError(f"FOIA edition digest differs from proof: {cid}")
        if edition.get("metric_count") != metric_count:
            raise ValueError(f"FOIA edition metric count differs from proof: {cid}")
        count = _collection_count(col, source_counts)
        included = cid == selected_cid
        rows.append(
            _selection_row(
                policy_version=CROSS_SOURCE_POLICY_VERSION,
                purpose=FOIA_EDITION_DEDUP_PURPOSE,
                collection_id=cid,
                source_generation_pin=source_generation_pin,
                source_sha256=sha,
                source_member_ordinal=member.get("ordinal"),
                source_member_name=member.get("name"),
                source_observations=count,
                selected_collection_id=selected_cid,
                selection_status="included" if included else "duplicate",
                selection_reason="selected-primary-agency-foia-edition"
                if included
                else "proven-metric-equivalent-foia-mirror-edition",
                equivalence_evidence_sha256=proof_sha256,
            )
        )
    if selected_cid not in seen:
        raise ValueError("Selected canonical FOIA edition is absent from proof")
    return rows


def cross_source_collection_selection(
    collections,
    *,
    source_generation_pin,
    source_counts=None,
    archive_inner_proofs=(),
    weball_webl_proofs=(),
    foia_edition_proofs=(),
):
    """Build the complete cross-source registration deduplication policy table.

    Combines exact shared-SHA API blob deduplication with any supplied verified
    inner-member (cm24), superset/subset (weball/webl), and FOIA dual-edition
    proofs into ``SELECTION_SCHEMA``-conformant rows without dropping any source
    occurrence or provenance.
    """
    rows = api_blob_representation_selection(
        collections, source_generation_pin=source_generation_pin, source_counts=source_counts
    )
    for proof_bytes, proof_sha in archive_inner_proofs:
        rows.extend(
            archive_inner_member_selection(
                proof_bytes,
                proof_sha256=proof_sha,
                source_generation_pin=source_generation_pin,
                collections=collections,
                source_counts=source_counts,
            )
        )
    for proof_bytes, proof_sha in weball_webl_proofs:
        rows.extend(
            weball_webl_subset_selection(
                proof_bytes,
                proof_sha256=proof_sha,
                source_generation_pin=source_generation_pin,
                collections=collections,
                source_counts=source_counts,
            )
        )
    for proof_bytes, proof_sha in foia_edition_proofs:
        rows.extend(
            foia_dual_edition_selection(
                proof_bytes,
                proof_sha256=proof_sha,
                source_generation_pin=source_generation_pin,
                collections=collections,
                source_counts=source_counts,
            )
        )
    return rows


def build_archive_inner_member_proof(
    archive_paths: dict[str, tuple[str, Path | bytes]],
    *,
    member_name: str,
    selected_collection_id: str,
    source_generation_pin: str,
) -> tuple[bytes, str]:
    """Hash inner ZIP member bytes across archives to build a verifiable proof receipt.

    ``archive_paths`` maps ``collection_id -> (outer_archive_sha256, path_or_bytes)``.
    Raises ``ValueError`` if inner bytes differ across any declared collection.
    """
    _digest(source_generation_pin)
    if selected_collection_id not in archive_paths or len(archive_paths) < 2:
        raise ValueError("Archive inner-member proof requires at least two collections including the canonical choice")
    inner_sha = inner_bytes = linefeeds = None
    members = []
    for cid, (outer_sha, source) in sorted(archive_paths.items()):
        outer_sha = _digest(outer_sha)
        raw = source.read_bytes() if isinstance(source, Path) else bytes(source)
        if "sha256:" + hashlib.sha256(raw).hexdigest() != outer_sha:
            raise ValueError(f"Outer archive bytes differ from pinned digest: {cid}")
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            payload = archive.read(member_name)
        digest = "sha256:" + hashlib.sha256(payload).hexdigest()
        count = payload.count(b"\n")
        if payload and not payload.endswith(b"\n"):
            count += 1
        if inner_sha is None:
            inner_sha, inner_bytes, linefeeds = digest, len(payload), count
        elif (digest, len(payload), count) != (inner_sha, inner_bytes, linefeeds):
            raise ValueError(f"Inner archive member bytes differ for {cid}: {member_name}")
        members.append(
            dict(
                collection_id=cid,
                outer_archive_sha256=outer_sha,
                inner_member_sha256=digest,
                inner_member_bytes=len(payload),
            )
        )
    proof = dict(
        status="passed-exact-inner-member-byte-equivalence",
        source_generation_pin=source_generation_pin,
        member_name=member_name,
        inner_member_sha256=inner_sha,
        inner_member_bytes=inner_bytes,
        linefeeds=linefeeds,
        selected_collection_id=selected_collection_id,
        collections=members,
    )
    body = _json(proof).encode("utf-8")
    return body, "sha256:" + hashlib.sha256(body).hexdigest()


def build_weball_webl_subset_proof(
    pairs_data,
    *,
    source_generation_pin: str,
) -> tuple[bytes, str]:
    """Verify exact multiset subset containment of ``webl`` rows inside ``weball`` rows.

    ``pairs_data`` is a sequence of dicts with keys:
    ``superset_collection_id``, ``superset_sha256``, ``superset_rows``,
    ``subset_collection_id``, ``subset_sha256``, ``subset_rows`` where row values
    are mapped summary dicts (from ``map_summary(..., CANDIDATE_WEB)``).
    """
    _digest(source_generation_pin)

    def row_key(row: dict) -> str:
        measures = [
            (m["native_field"], str(m["value"]), m["raw_value"], m["value_status"], m["unit"])
            for m in (row.get("measures") or [])
        ]
        return _json(
            dict(
                source_cycle=row.get("source_cycle"),
                candidate_native_id=row.get("candidate_native_id"),
                candidate_name=row.get("candidate_name"),
                period_end=row["period_end"].isoformat() if row.get("period_end") is not None else None,
                measures=measures,
                attributes_json=row.get("attributes_json"),
            )
        )

    proof_pairs = []
    for item in pairs_data:
        sup_counts = Counter(row_key(r) for r in item["superset_rows"])
        sub_counts = Counter(row_key(r) for r in item["subset_rows"])
        matched = sum(min(count, sup_counts[key]) for key, count in sub_counts.items())
        if matched != len(item["subset_rows"]):
            raise ValueError(
                f"Subset rows do not all match superset identically: {item['subset_collection_id']} "
                f"({matched}/{len(item['subset_rows'])})"
            )
        proof_pairs.append(
            dict(
                superset_collection_id=item["superset_collection_id"],
                superset_sha256=_digest(item["superset_sha256"]),
                superset_rows=len(item["superset_rows"]),
                subset_collection_id=item["subset_collection_id"],
                subset_sha256=_digest(item["subset_sha256"]),
                subset_rows=len(item["subset_rows"]),
                matched_subset_rows=matched,
            )
        )
    proof = dict(
        status="passed-exact-subset-row-equivalence",
        source_generation_pin=source_generation_pin,
        pairs=proof_pairs,
    )
    body = _json(proof).encode("utf-8")
    return body, "sha256:" + hashlib.sha256(body).hexdigest()


def build_foia_dual_edition_proof(
    editions_metrics,
    *,
    selected_collection_id: str,
    source_generation_pin: str,
) -> tuple[bytes, str]:
    """Verify exact metric-multiset equivalence across FOIA annual report editions.

    ``editions_metrics`` is a sequence of dicts with keys ``collection_id``,
    ``source_sha256``, ``metrics`` (list of mapped ``fec_report_metrics`` rows).
    """
    _digest(source_generation_pin)
    if len(editions_metrics) < 2:
        raise ValueError("FOIA dual-edition proof requires at least two editions")

    def metric_key(row: dict) -> str:
        return _json(
            dict(
                native_field=row.get("native_field"),
                metric_definition_id=row.get("metric_definition_id"),
                fiscal_year=row.get("fiscal_year"),
                raw_value=row.get("raw_value"),
                value=str(row["value"]) if row.get("value") is not None else None,
                value_status=row.get("value_status"),
                value_operator=row.get("value_operator"),
                bound_value=str(row["bound_value"]) if row.get("bound_value") is not None else None,
                unit=row.get("unit"),
            )
        )

    baseline = None
    editions = []
    for item in editions_metrics:
        counts = Counter(metric_key(m) for m in item["metrics"])
        if baseline is None:
            baseline = counts
        else:
            diff = sum((counts - baseline).values()) + sum((baseline - counts).values())
            if diff != 0:
                raise ValueError(f"FOIA edition metrics differ from baseline ({diff} symmetric differences)")
        editions.append(
            dict(
                collection_id=item["collection_id"],
                source_sha256=_digest(item["source_sha256"]),
                metric_count=len(item["metrics"]),
            )
        )
    proof = dict(
        status="passed-exact-foia-metric-multiset-equivalence",
        source_generation_pin=source_generation_pin,
        selected_collection_id=selected_collection_id,
        metric_count=len(editions_metrics[0]["metrics"]),
        symmetric_diff_count=0,
        editions=editions,
    )
    body = _json(proof).encode("utf-8")
    return body, "sha256:" + hashlib.sha256(body).hexdigest()
