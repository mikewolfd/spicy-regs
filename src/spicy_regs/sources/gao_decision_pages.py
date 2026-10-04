"""GAO's decision pages, read by reference from a local capture of them.

GAO's listing cuts some decisions' number lists and never states the day a decision was decided; the decision's own
page states both in its caption, which spicy-docs reads (``sources.gao.decision_pages.read_decision_caption``).
gao.gov refuses plain clients, so the pages are captured outside the rollup, through Zyte (round 6, 2026-10-03: every
published decision's page, ``~/Work/corpora/gao-decision-pages-2026-10-03/``), into a directory holding:

- ``receipts.jsonl``: one line per fetch attempt (``url``, ``status_code``, ``sha256``, ...);
- ``blobs/sha256/<hex>``: each page's bytes, content-addressed.

A page is checked against its successful capture receipt before reading. The
builder records a digest of exactly the receipt lines read in its source evidence.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path


RECEIPTS = "receipts.jsonl"
BLOBS = Path("blobs") / "sha256"


class DecisionPageCapture:
    """A capture directory's held pages by url, read one at a time; O(receipts) to open, O(page) per read."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory
        #: The campaign is the capture's directory: its status file is rewritten by each retry run.
        self.campaign = directory.name
        self._held: dict[str, tuple[str, str]] = {}
        for line in (directory / RECEIPTS).read_text(encoding="utf-8").splitlines():
            receipt = json.loads(line)
            if receipt.get("status_code") == 200:
                if receipt["url"] in self._held:
                    raise ValueError(f"{directory}: two 200 receipts for {receipt['url']}")
                self._held[receipt["url"]] = (receipt["sha256"], line)
        self._read: dict[str, str] = {}

    def page(self, url: str) -> bytes | None:
        """The page captured for ``url``, checked against its receipt; None where the capture holds none."""
        held = self._held.get(url)
        if held is None:
            return None
        digest, line = held
        body = (self.directory / BLOBS / digest).read_bytes()
        if hashlib.sha256(body).hexdigest() != digest:
            raise ValueError(f"{self.directory}: the page held for {url} differs from its receipt's sha256")
        self._read[url] = line
        return body

    def record(self) -> tuple[str, int]:
        """Digest and byte size of the receipt lines read, in URL order."""
        read = "".join(self._read[url] + "\n" for url in sorted(self._read)).encode()
        digest = "sha256:" + hashlib.sha256(read).hexdigest()
        return digest, len(read)
