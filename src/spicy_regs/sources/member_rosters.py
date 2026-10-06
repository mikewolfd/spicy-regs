"""Acquire the reviewed member rosters at their immutable source locations.

SpicyDocs still owns transport, byte budgets, parsing and the current-roster
LIS check. This adapter selects the approved fork inputs and checks their pins;
it never changes a parsed person or substitutes a body under an upstream URL.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType
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
        if not isinstance(selected, Mapping) or any(not isinstance(value, Mapping) for value in selected.values()):
            raise ValueError("Reviewed member roster selection must contain roster mappings")
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
            if member["url"] != f"https://raw.githubusercontent.com/mikewolfd/congress-legislators/{commit}/legislators-{roster}.json":
                raise ValueError("Reviewed member roster URL must name the selected repository file")
            commits.add(commit)
        if len(commits) != 1:
            raise ValueError("Reviewed member rosters must belong to one source commit")
        self.selection = MappingProxyType({key: MappingProxyType(value) for key, value in self.selection.items()})
        self.evidence = evidence
        self._current_capture = None
        super().__init__(**kwargs)

    def acquire_current(self, *, max_bytes: int | None = None):
        self._current_capture = None
        try:
            return super().acquire_current(max_bytes=max_bytes)
        except LegislatorsSourceError as error:
            # The maintained by-LIS postcondition runs after acquisition. Its
            # default URL must follow this adapter's actual captured request.
            if self._current_capture is not None:
                context = dict(getattr(error, "legislators_acquisition", {}))
                context["url"] = self._current_capture.requested_url
                error.__dict__["legislators_acquisition"] = context
                attach_capture(error, self._current_capture)
            raise

    def capture_validated(self, url: str, *, media_types: tuple[str, ...], **kwargs):
        # Raw GitHub serves these JSON files as text/plain. Admit that actual
        # header only for our immutable selected routes; shared capture and
        # JSON validation still run, and _acquire checks both approved pins.
        if url in {member["url"] for member in self.selection.values()} and media_types == ("application/json",):
            media_types = (*media_types, "text/plain")
        return super().capture_validated(url, media_types=media_types, **kwargs)

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
        if operation == "current":
            self._current_capture = result.capture
        return result
