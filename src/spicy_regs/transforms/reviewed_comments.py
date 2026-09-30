"""Apply evidence-backed application exclusions while preserving source acquisition."""

from collections.abc import Iterable, Iterator
from hashlib import sha256
from importlib.resources import files
from typing import Any
import json

from loguru import logger

from spicy_regs.transforms.base import Transform


class ExcludeReviewedComments(Transform):
    """Exclude only reviewed payloads; changed observations require a new review.

    Decisions live in ``reviewed_comment_exclusions.json``, with exact source
    locators, byte digests and reasons. Canonical JSON hashing ignores only
    formatting and key order. Neither a title keyword nor a missing docket is
    an exclusion rule. The source reader still reports consumed keys, so a
    successful batch checkpoints deliberate exclusions rather than retrying them.
    """

    def __init__(self, *, keyed: bool = False) -> None:
        # Keyed ahead of the attribute tee, so an excluded comment gets no attribute row either.
        self.keyed = keyed
        decisions = json.loads(files("spicy_regs").joinpath("reviewed_comment_exclusions.json").read_text())
        self.decisions = {item["comment_id"]: item for item in decisions}
        if len(self.decisions) != len(decisions):
            raise ValueError("Duplicate reviewed comment exclusion")

    def excludes(self, payload: dict) -> bool:
        """Whether ``payload`` is a reviewed exclusion; a reviewed comment whose content changed raises."""
        identity = payload.get("data", {}).get("id")
        decision = self.decisions.get(identity)
        if decision is None:
            return False
        digest = sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        if digest != decision["canonical_sha256"]:
            raise ValueError(f"Reviewed comment {identity} changed; recheck its exclusion before ingestion")
        logger.info("Excluded reviewed comment {} (sha256:{}): {}", identity, digest, decision["reason"])
        return True

    def apply(self, records: Iterable[Any]) -> Iterator[Any]:
        for record in records:
            if not self.excludes(record.payload if self.keyed else record):
                yield record
