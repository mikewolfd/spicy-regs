"""Acquire the reviewed member rosters at their immutable source locations.

SpicyDocs still owns transport, byte budgets, parsing and the current-roster
LIS check. This adapter selects the approved fork inputs and checks their pins;
it never changes a parsed person or substitutes a body under an upstream URL.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
import json

from spicy_docs.sources.legislators import LegislatorsAcquirer, LegislatorsSourceError
from spicy_docs.transport.captured import attach_capture

from spicy_regs.source_evidence import CaptureEvidence

SELECTION = Path(__file__).with_name("member_rosters.json")


class ReviewedMemberRosters(LegislatorsAcquirer):
    """Use unchanged source acquisition with explicit, digest-checked routes."""

    def __init__(self, *, evidence: CaptureEvidence | None = None,
                 selection: Mapping | None = None, **kwargs):
        selected = json.loads(SELECTION.read_bytes()) if selection is None else selection
        self.selection = {key: dict(value) for key, value in selected.items()}
        if set(self.selection) != {"current", "historical"}:
            raise ValueError("Both reviewed member rosters are required")
        commits = set()
        for roster, member in self.selection.items():
            if (set(member) != {"url", "sha256", "bytes"}
                    or not isinstance(member["url"], str)
                    or not member["url"].startswith("https://raw.githubusercontent.com/mikewolfd/congress-legislators/")
                    or not member["url"].endswith(f"/legislators-{roster}.json")
                    or not isinstance(member["sha256"], str) or len(member["sha256"]) != 64
                    or any(value not in "0123456789abcdef" for value in member["sha256"])
                    or type(member["bytes"]) is not int or member["bytes"] <= 0):
                raise ValueError("Invalid reviewed member roster selection")
            commit = member["url"].split("/")[-2]
            if len(commit) != 40 or any(value not in "0123456789abcdef" for value in commit):
                raise ValueError("Reviewed member roster URL must name an immutable commit")
            commits.add(commit)
        if len(commits) != 1:
            raise ValueError("Reviewed member rosters must belong to one source commit")
        self.evidence = evidence
        super().__init__(**kwargs)

    def _acquire(self, url: str, operation: str, max_bytes: int):
        member = self.selection[operation]
        if self.evidence:
            self.evidence.event("member-roster-selection", roster=operation, **member)
        result = super()._acquire(member["url"], operation, max_bytes)
        if self.evidence:
            self.evidence.capture(result.capture, stage=operation)
        if (result.capture.sha256 != "sha256:" + member["sha256"]
                or result.capture.byte_size != member["bytes"]):
            error = LegislatorsSourceError("Reviewed member roster bytes differ from the approved input")
            attach_capture(error, result.capture)
            raise error
        return result
