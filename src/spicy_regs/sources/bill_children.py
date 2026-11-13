"""Resume explicitly selected older-bill child reads against central originals."""

from __future__ import annotations

import fcntl
import hashlib
from collections.abc import Callable, Mapping
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

import httpx
from spicy_docs.reading.paged_json import PagedJsonBudget, PagedJsonSourceError, PagedJsonUnavailableError
from spicy_docs.sources.congress.bill_children import (
    BILL_CHILD_ROUTES,
    api_action,
    api_summary,
    bill_child_pages,
    bill_child_url,
)
from spicy_docs.sources.congress.bill_status import BillAction, BillChildWitness, BillIdentity, BillSummary
from spicy_docs.sources.congress.listing import CongressListingReader
from spicy_docs.transport.captured import attach_capture
from spicy_docs.transport.credentials import CredentialRefusedError
from spicy_docs.transport.resume import ResumeError, TraversalCheckpoint

from spicy_regs.etl_receipts import exact_json
from spicy_regs.source_evidence import CaptureEvidence

_ACTIVE_ROUTES = ("summaries", "actions")
_MAX_PAGE_BYTES = 2 * 1024**2
_MAX_PAGES = 8


@dataclass(frozen=True)
class OlderBillChildren:
    actions: tuple[BillAction, ...] | None
    summaries: tuple[BillSummary, ...] | None
    routes: tuple[dict, ...]


class ChildRequestLimit(Exception):
    """The caller's shared network budget is exhausted."""


class OlderBillChildSource:
    """A caller-selected set of observations, with a separate checkpoint for each route."""

    def __init__(
        self,
        directory: Path,
        *,
        api_key: str,
        evidence: CaptureEvidence,
        transport: Callable[[], httpx.BaseTransport] = httpx.HTTPTransport,
        action_page_size: int = 250,
    ):
        self.directory = directory
        self.api_key = api_key
        self.evidence = evidence
        self.transport = transport
        self.action_page_size = action_page_size

    def read(
        self, identity: BillIdentity, detail: Mapping, *, parent_observed_at: str, charge: Callable[[], None]
    ) -> OlderBillChildren:
        """Keep the parent declarations and the route's own response states separate."""
        results: dict = {}
        inventory = []
        self.evidence.credential = self.api_key
        for route in BILL_CHILD_ROUTES:
            link = detail.get(route)
            advertised = link.get("url") if isinstance(link, Mapping) else None
            declared = link.get("count") if isinstance(link, Mapping) else None
            row = {
                "route": route,
                "advertised_url": advertised if isinstance(advertised, str) else None,
                "parent_declared_count": declared if type(declared) is int and declared >= 0 else None,
                "parent_observed_at": parent_observed_at,
                "outcome": "unrequested",
                "reason": "outside the summaries/actions slice for Congresses 82–107",
                "observed_count": None,
                "http_status": None,
                "page_pins": None,
            }
            inventory.append(row)
            if route not in _ACTIVE_ROUTES:
                continue
            if advertised is None:
                row["reason"] = "parent did not advertise a child locator"
                continue
            pins: list[dict] = []
            try:
                url = bill_child_url(
                    identity, route, advertised, limit=self.action_page_size if route == "actions" else 250
                )
                records = self._read_route(identity, route, url, pins=pins, charge=charge)
            except CredentialRefusedError as error:
                self.evidence.refusal(error, stage=f"bill-children:{route}")
                raise
            except PagedJsonUnavailableError as error:
                self.evidence.refusal(error, stage=f"bill-children:{route}")
                row.update(
                    outcome="refused" if pins else "unavailable",
                    reason=type(error).__name__,
                    http_status=error.capture.status_code,
                )
            except ChildRequestLimit as error:
                self.evidence.refusal(error, stage=f"bill-children:{route}")
                row.update(outcome="refused" if pins else "unrequested", reason=type(error).__name__)
            except (PagedJsonSourceError, ConnectionError, httpx.HTTPError) as error:
                self.evidence.refusal(error, stage=f"bill-children:{route}")
                row.update(outcome="refused", reason=type(error).__name__)
            else:
                results[route] = records
                row.update(
                    outcome="populated" if records else "requested-empty",
                    reason=None,
                    observed_count=len(records),
                )
            row["page_pins"] = pins or None
        return OlderBillChildren(results.get("actions"), results.get("summaries"), tuple(inventory))

    def _read_route(self, identity: BillIdentity, route: str, url: str, *, pins: list[dict], charge):
        """Retain each validated page before advancing the maintained traversal checkpoint."""
        selected = {"url": url, "route": route, "max_pages": _MAX_PAGES, "max_page_bytes": _MAX_PAGE_BYTES}
        selection = hashlib.sha256(exact_json(selected).encode()).hexdigest()
        folder = self.directory / f"{identity.congress}-{identity.bill_type}-{identity.number}" / selection
        folder.mkdir(parents=True, exist_ok=True)
        with (folder / "run.lock").open("a+b") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise ResumeError("Bill child checkpoint is already in use") from error
            checkpoint = TraversalCheckpoint(
                folder / "checkpoint.json", selection_sha256="sha256:" + selection, first_url=url
            )
            prefix_count = len(checkpoint.state["pages"])
            records = []
            summary_keys = set()
            parse = api_action if route == "actions" else api_summary

            def retain(capture):
                if capture.content_encoding != "identity":
                    error = PagedJsonSourceError("Bill child checkpoint requires identity-encoded JSON")
                    attach_capture(error, capture)
                    raise error
                self.evidence.capture(capture, stage=f"bill-children:{route}", source_validation="validated")

            def typed_pages(reader):
                for page in bill_child_pages(reader, identity, route, url, max_pages=_MAX_PAGES):
                    try:
                        for index, row in enumerate(page.records):
                            record = parse(row, BillChildWitness(page.sha256, page.capture.observed_at, index))
                            if isinstance(record, BillSummary):
                                key = (record.version_code, record.action_date, record.update_date)
                                if key in summary_keys:
                                    raise PagedJsonSourceError("Congress.gov bill summaries repeated a revision key")
                                summary_keys.add(key)
                            records.append(record)
                    except PagedJsonSourceError as error:
                        attach_capture(error, page.capture)
                        raise
                    yield page

            with closing(self.transport()) as live:

                class ChargedTransport(httpx.BaseTransport):
                    def handle_request(self, request):
                        charge()
                        return live.handle_request(request)

                transport = checkpoint.replay_transport(None, ChargedTransport(), open_blob=self.evidence.store.open)
                budget = PagedJsonBudget(
                    max_requests=1, max_page_bytes=_MAX_PAGE_BYTES, timeout_seconds=30, min_request_interval_seconds=0.2
                )
                with CongressListingReader(budget=budget, api_key=self.api_key, transport=transport) as reader:
                    for page in checkpoint.track(typed_pages(reader), retain_capture=retain, api_keys=(self.api_key,)):
                        capture = page.capture
                        if page.page_index < prefix_count:
                            self.evidence.retain_bytes(
                                capture.body,
                                stage=f"bill-children:{route}:replayed",
                                raw_source=True,
                                original_location=capture.requested_url,
                                source_observed_at=capture.observed_at,
                            )
                        pins.append(
                            {
                                "url": capture.requested_url,
                                "sha256": page.sha256,
                                "observed_at": capture.observed_at,
                                "records": len(page.records),
                                "declared_count": page.declared_count,
                            }
                        )
            return tuple(records)
