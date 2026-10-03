"""Fail-closed source selection; only qualified, enabled publishers can be read."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import yaml

from spicy_regs.source_evidence import POLICIES

REGISTRY = Path(__file__).with_name("sources.yaml")
ADAPTERS = frozenset({"lcv", "afl_cio", "heritage_action", "humane_world_action", "nea", "c4ip", "afp", "ijm", "hrc"})
CADENCES = {"daily": timedelta(days=1), "weekly": timedelta(days=7), "monthly": timedelta(days=30)}


class RegistryError(ValueError):
    """Invalid or unqualified source selection; never infer an enabled source."""


@dataclass(frozen=True)
class ScorecardSource:
    publisher_id: str
    adapter: str
    enabled: bool
    cadence: str
    historical_backfill: bool
    evidence_policy: str
    rights_status: str
    policy_decision_id: str
    qualification_id: str | None

    def due(self, now: datetime, last_attempt: str | None, *, force: bool = False) -> bool:
        if not self.enabled:
            return False
        if last_attempt is None or force:
            return True
        observed = datetime.fromisoformat(last_attempt.replace("Z", "+00:00"))
        if observed.tzinfo is None or now.tzinfo is None:
            raise RegistryError("Cadence requires timezone-aware observation times")
        return now >= observed + CADENCES[self.cadence]


def load_registry(path: Path = REGISTRY) -> tuple[ScorecardSource, ...]:
    document = yaml.safe_load(path.read_text())
    if (
        not isinstance(document, dict)
        or set(document) != {"version", "sources"}
        or type(document["version"]) is not int
        or document["version"] != 1
    ):
        raise RegistryError("Unknown scorecard registry format")
    if not isinstance(document["sources"], list):
        raise RegistryError("Registry sources must be a list")
    fields = set(ScorecardSource.__dataclass_fields__)
    sources, seen = [], set()
    for row in document["sources"]:
        if not isinstance(row, dict) or set(row) != fields:
            raise RegistryError("Registry source fields differ from the supported configuration")
        if any(
            not isinstance(row[field], str) for field in fields - {"enabled", "historical_backfill", "qualification_id"}
        ):
            raise RegistryError("Registry source text fields must be strings")
        source = ScorecardSource(**row)
        if (
            source.publisher_id not in ADAPTERS
            or source.adapter != source.publisher_id
            or source.publisher_id in seen
            or source.cadence not in CADENCES
            or source.evidence_policy not in POLICIES
            or type(source.enabled) is not bool
            or type(source.historical_backfill) is not bool
            or not isinstance(source.policy_decision_id, str)
            or not source.policy_decision_id.strip()
            or source.rights_status not in {"unreviewed", "metadata_approved", "redistribution_allowed", "blocked"}
        ):
            raise RegistryError("Invalid scorecard source configuration")
        if source.qualification_id is not None and (
            not isinstance(source.qualification_id, str) or not source.qualification_id
        ):
            raise RegistryError("Invalid source qualification identity")
        if source.enabled and (not source.qualification_id or source.rights_status == "blocked"):
            raise RegistryError("Enabled source requires qualification and an unblocked rights disposition")
        if source.evidence_policy == "full" and source.rights_status != "redistribution_allowed":
            raise RegistryError("Full evidence requires an explicit redistribution decision")
        seen.add(source.publisher_id)
        sources.append(source)
    return tuple(sources)


def select_sources(sources, *, publishers=(), historical_backfill=False):
    requested = set(publishers)
    by_id = {source.publisher_id: source for source in sources}
    if requested - by_id.keys():
        raise RegistryError("Requested publisher is absent from the source registry")
    if any(not by_id[p].enabled for p in requested):
        raise RegistryError("Requested publisher is disabled")
    selected = tuple(s for s in sources if s.enabled and (not requested or s.publisher_id in requested))
    if historical_backfill and any(not source.historical_backfill for source in selected):
        raise RegistryError("Historical backfill is disabled for a selected source")
    return selected
