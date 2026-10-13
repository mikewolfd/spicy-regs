"""Compare retained publisher inventories without replacing either observation."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path

from .storage import digest


def read_snapshot(path: Path) -> tuple[dict, str]:
    payload = path.read_bytes()
    value = json.loads(payload)
    if value.get("format") != "spicy-source-discovery" or value.get("version") != 1:
        raise ValueError("Expected spicy-source-discovery version 1")
    return value, "sha256:" + hashlib.sha256(payload).hexdigest()


def scope_key(snapshot: dict) -> str:
    observations = snapshot.get("observations", [])
    if not observations:
        raise ValueError("Inventory comparison requires observations")
    scopes = [
        {"capture_url": o.get("capture_url"), "scope": o.get("scope"), "metadata_kind": o.get("metadata_kind")}
        for o in observations
    ]
    if any(not s["capture_url"] or not s["scope"] or not s["metadata_kind"] for s in scopes):
        raise ValueError("Inventory observation requires URL, scope and metadata kind")
    # Page URLs/tokens are part of selected scope. Different traversal selections
    # need qualification before they can be compared as the same inventory.
    return json.dumps(sorted({json.dumps(s, sort_keys=True) for s in scopes}), separators=(",", ":"))


def items(snapshot: dict) -> dict[str, dict]:
    result = {}
    observations = {o["id"] for o in snapshot["observations"]}
    for item in snapshot.get("items", []):
        ident = item.get("id")
        if not ident or ident in result or item.get("observation_id") not in observations:
            raise ValueError("Inventory item requires unique identity and a retained observation")
        # version_id fingerprints the whole capture. Comparing it would mark
        # every unchanged item changed whenever any neighboring metadata changes.
        result[ident] = {k: v for k, v in item.items() if k not in {"observation_id", "version_id"}}
    return result


def complete(snapshot: dict, pin: str, proof: dict | None) -> bool:
    """Accept only an explicit caller-qualified, snapshot-bound traversal proof.

    A terminal listing page or a parser's observed-page flag is insufficient.
    This checks proof binding, not the underlying publisher's stability claim.
    """
    if snapshot.get("errors") or not proof:
        return False
    observation_ids = [o["id"] for o in snapshot["observations"]]
    return bool(
        proof.get("format") == "spicy-source-traversal-completion"
        and proof.get("version") == 1
        and proof.get("status") == "complete"
        and proof.get("snapshot_sha256") == pin
        and proof.get("scope_key") == scope_key(snapshot)
        and proof.get("inventory_stable") is True
        and len(set(observation_ids)) == len(observation_ids)
        and sorted(proof.get("observation_ids", [])) == sorted(observation_ids)
        and proof.get("qualification_input", {}).get("sha256")
    )


def compare(
    previous: Path,
    current: Path,
    *,
    now: datetime,
    max_age_days: int,
    owner: str,
    previous_completion: Path | None = None,
    current_completion: Path | None = None,
) -> dict:
    if max_age_days < 1 or not owner.strip() or now.utcoffset() is None:
        raise ValueError("Refresh requires positive cadence, failure owner and timezone-aware time")
    old, old_pin = read_snapshot(previous)
    new, new_pin = read_snapshot(current)
    if scope_key(old) != scope_key(new):
        raise ValueError("Different selected inventory scopes cannot establish changes")
    old_items, new_items = items(old), items(new)
    proofs = []
    for path in (previous_completion, current_completion):
        if path:
            value = json.loads(path.read_text())
            # Completion proofs are private evidence; validate their referenced
            # qualification bytes before allowing confirmed absence.
            qualification = value.get("qualification_input", {})
            reference = qualification.get("path")
            if reference:
                reference_path = Path(reference)
                if not reference_path.is_absolute():
                    reference_path = path.parent / reference_path
                if digest(reference_path) != qualification.get("sha256"):
                    raise ValueError("Traversal qualification input differs from its pin")
            else:
                raise ValueError("Traversal completion requires a pinned qualification input")
            proofs.append((value, digest(path)))
        else:
            proofs.append((None, None))
    removal_proved = complete(old, old_pin, proofs[0][0]) and complete(new, new_pin, proofs[1][0])
    absent = sorted(old_items.keys() - new_items.keys())
    freshness = []
    for observation in new["observations"]:
        observed = datetime.fromisoformat(observation["observed_at"].replace("Z", "+00:00"))
        if observed.utcoffset() is None or observed > now:
            raise ValueError("Observation time must be timezone-aware and not in the future")
        freshness.append(
            {
                "observation_id": observation["id"],
                "observed_at": observation["observed_at"],
                "stale": now - observed > timedelta(days=max_age_days),
            }
        )
    return {
        "format": "spicy-source-inventory-changes",
        "version": 1,
        "previous_sha256": old_pin,
        "current_sha256": new_pin,
        "completion_proof_sha256": [p[1] for p in proofs],
        "compared_at": now.isoformat(),
        "refresh_policy": {"max_age_days": max_age_days, "failure_owner": owner},
        "new": sorted(new_items.keys() - old_items.keys()),
        "metadata_changed": sorted(k for k in old_items.keys() & new_items.keys() if old_items[k] != new_items[k]),
        "removed": absent if removal_proved else [],
        "not_seen_unconfirmed": [] if removal_proved else absent,
        "removal_basis": "caller-qualified completed stable traversal proofs"
        if removal_proved
        else "incomplete traversal evidence",
        "freshness": freshness,
        "errors": new.get("errors", []),
        "limits": "Metadata observations do not prove dataset content changes. Earlier captures remain authoritative for their times.",
    }


def write_report(path: Path, report: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
