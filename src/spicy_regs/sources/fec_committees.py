"""Unfiltered FEC committee observations through the SpicyDocs source reader.

SpicyDocs owns pagination, HTTP retries, header-only credentials and exact raw
response captures. This adapter selects the complete committee registry, checks
that identifiers advance without duplication, and retains a per-run page index.
Only normal iterator exhaustion completes an acquisition. Missing credentials,
page bounds, changed exact counts and failed requests refuse the run.

Set ``FEC_CAPTURE_DIR`` or pass ``capture_dir`` to keep raw evidence outside the
build directory. Each attempt gets its own directory; incomplete attempts remain
marked incomplete. Source traversal is not a frozen publisher snapshot.
"""

from __future__ import annotations

import json
import os
from collections.abc import Generator
from pathlib import Path
from tempfile import mkdtemp
from typing import TYPE_CHECKING

from loguru import logger

from spicy_regs.sources.base import Reader

if TYPE_CHECKING:
    import httpx

API_BASE = "https://api.open.fec.gov/v1"
PER_PAGE = 100
API_KEY_ENV_VARS = ("API_GOV", "DATA_GOV_API_KEY", "FEC_API_KEY", "REGULATIONS_GOV_API_KEY")
_MAX_PAGES = 5_000
_PROGRESS_EVERY = 5_000


def _resolve_api_key() -> str | None:
    """Resolve the repository's shared api.data.gov key without logging it."""
    return next((os.environ[var] for var in API_KEY_ENV_VARS if os.environ.get(var)), None)


class FecCommitteesReader(Reader):
    """Yield native committee metadata, retaining the complete original pages.

    No cycle or activity filters apply. ``max_pages`` is a refusal bound, never
    permission to return a partial census as a successful run. ``capture_dir``
    defaults to ``FEC_CAPTURE_DIR`` or ``.fec-captures`` for direct consumers.
    """

    def __init__(
        self,
        *,
        per_page: int = PER_PAGE,
        max_pages: int | None = None,
        api_key: str | None = None,
        capture_dir: Path | None = None,
        transport: httpx.BaseTransport | None = None,
        min_interval: float = 0.25,
        verbose: bool = False,
    ) -> None:
        if type(per_page) is not int or per_page <= 0:
            raise ValueError("per_page must be a positive integer")
        if max_pages is not None and (type(max_pages) is not int or max_pages <= 0):
            raise ValueError("max_pages must be a positive integer")
        self.per_page = min(per_page, PER_PAGE)
        self.max_pages = _MAX_PAGES if max_pages is None else min(max_pages, _MAX_PAGES)
        self.api_key = api_key if api_key is not None else _resolve_api_key()
        self.capture_dir = Path(capture_dir or os.environ.get("FEC_CAPTURE_DIR", ".fec-captures"))
        self.transport = transport
        self.min_interval = min_interval
        self.verbose = verbose
        self.last_run_path: Path | None = None

    def iter_records(self) -> Generator[dict, None, None]:
        if not self.api_key:
            raise ValueError("FEC committees require an API key; no complete acquisition was attempted")
        try:
            from spicy_docs.sources.fec.client import FecClient
        except ModuleNotFoundError as error:
            if error.name == "spicy_docs":
                raise RuntimeError(
                    "FEC committees require spicy-regs[source-readers]; run `uv sync --frozen`."
                ) from None
            raise

        self.capture_dir.mkdir(parents=True, exist_ok=True)
        run_path = Path(mkdtemp(prefix="committees-", dir=self.capture_dir))
        self.last_run_path = run_path
        state = {
            "status": "incomplete",
            "source": API_BASE + "/committees/",
            "params": {"sort": "committee_id", "per_page": self.per_page, "page": 1},
            "scope": "unfiltered source traversal; no frozen publisher snapshot",
            "max_pages": self.max_pages,
            "pages": 0,
            "records": 0,
            "declared_exact_count": None,
        }
        (run_path / "run.json").write_text(json.dumps(state, indent=2) + "\n")
        previous_id = None
        try:
            with (
                FecClient(
                    store=run_path / "blobs",
                    api_key=self.api_key,
                    # Includes room for the provider's three attempts and redirects;
                    # exhausting either request or page budget refuses acquisition.
                    max_requests=self.max_pages * 12,
                    min_interval=self.min_interval,
                    transport=self.transport,
                ) as client,
                (run_path / "pages.jsonl").open("w") as index,
            ):
                for page in client.api("/v1/committees/", params=state["params"], max_pages=self.max_pages):
                    state["pages"] += 1
                    index.write(
                        json.dumps(
                            {
                                key: page[key]
                                for key in (
                                    "request_url",
                                    "resolved_url",
                                    "observed_at",
                                    "media_type",
                                    "via",
                                    "evidence",
                                    "next_url",
                                )
                            }
                            | {"records": len(page["records"])}
                        )
                        + "\n"
                    )
                    index.flush()
                    pagination = page["pagination"]
                    if pagination.get("is_count_exact") is True:
                        count = pagination.get("count")
                        if type(count) is not int or count < 0:
                            raise ValueError("FEC exact committee count must be a nonnegative integer")
                        if state["declared_exact_count"] not in (None, count):
                            raise ValueError("FEC exact committee count changed during traversal")
                        state["declared_exact_count"] = count
                    for observation in page["records"]:
                        record = observation["metadata"]
                        if not isinstance(record, dict):
                            raise ValueError("FEC committee record must be an object")
                        committee_id = record.get("committee_id")
                        if not isinstance(committee_id, str) or not committee_id:
                            raise ValueError("FEC committee record omitted its identifier")
                        if previous_id is not None and committee_id <= previous_id:
                            raise ValueError("FEC committee identifiers repeated or ceased increasing")
                        previous_id = committee_id
                        state["records"] += 1
                        if state["records"] % _PROGRESS_EVERY == 0:
                            logger.info("FEC committees: {:,} observed so far", state["records"])
                        yield record
                if state["declared_exact_count"] not in (None, state["records"]):
                    raise ValueError("FEC observed committees disagree with the declared exact count")
                state["status"] = "complete"
        finally:
            (run_path / "run.json").write_text(json.dumps(state, indent=2) + "\n")
        logger.info("FEC committees: {:,} observed; evidence at {}", state["records"], run_path)
