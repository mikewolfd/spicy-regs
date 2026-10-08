"""Explicit publisher selection for bounded, evidenced Zyte acquisition."""

from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
import time
from urllib.parse import urljoin

import httpx
from spicy_docs.transport.captured import CapturedBodyResponse
from typing import cast
from urllib.parse import urlsplit

from spicy_regs.scorecards.errors import ScorecardRefreshError

from spicy_docs.sources.zyte import (
    BROWSER_HTML,
    HTTP_RESPONSE_BODY,
    ZyteHttpFetcher,
    require_zyte_token_from_environment,
)
from spicy_docs.transport.capture import BoundedHttpCapture
from spicy_docs.transport.credentials import CredentialRefusedError
from spicy_docs.transport.zyte import ZyteBudget, ZyteTransport


class ScorecardTransportError(RuntimeError):
    """Selected paid transport failed; stop the run and preserve prior editions."""


MAX_BYTES = 20 * 1024 * 1024
MAX_REQUESTS = 2000


class RequestFetcher:
    """Expose the shared callable and explicit-request acquisition interface."""

    def __init__(self, request):
        self.request = request

    def __call__(self, url):
        return self.request(url)


def validate_limits(max_bytes, max_requests):
    if any(type(value) is not int or value < 1 for value in (max_bytes, max_requests)):
        raise ValueError("Scorecard byte and request limits must be positive integers")


@contextmanager
def zyte_fetch(source, *, max_requests=MAX_REQUESTS, max_bytes=MAX_BYTES, fetcher=None, browser_api=False):
    """Keep publisher response bytes and explicitly selected browser DOM distinct.

    There is no implicit direct-to-proxy fallback or hidden retry. Every provider
    attempt consumes the same bounded budget, and proxy provenance is journaled
    separately from the source capture without retaining third-party bodies.
    """
    validate_limits(max_bytes, max_requests)
    if browser_api and source.publisher_id != "ijm":
        raise ValueError("Browser API rendition is only supported for IJM")
    try:
        token = require_zyte_token_from_environment()
    except Exception:
        raise ScorecardTransportError("Selected Zyte transport requires a configured credential") from None
    source.root.credential = token
    provider = fetcher or ZyteHttpFetcher(token=token)
    target_headers: ContextVar[tuple[str, dict[str, str]] | None] = ContextVar("scorecard_target_headers", default=None)

    class HeaderFetcher:
        """Pass source-selected app headers through the existing provider SDK."""

        def fetch(self, url, **options):
            selected = target_headers.get()
            if selected is not None:
                selected_url, headers = selected
                if selected_url != url:
                    raise ScorecardTransportError("Publisher headers escaped the selected request")
                options["target_headers"] = tuple(headers.items())
                options["extra_secrets"] = tuple(
                    value for name, value in headers.items() if name.lower() in {"authorization", "x-api-key"}
                )
            return provider.fetch(url, **options)

    header_provider = HeaderFetcher()
    budget = ZyteBudget(max_requests)
    routes = {}
    for mode in (HTTP_RESPONSE_BODY, BROWSER_HTML) if browser_api else (HTTP_RESPONSE_BODY,):
        # The provider SDK names its concrete fetcher type; this wrapper delegates
        # that same fetch interface while adding scoped publisher headers.
        transport = ZyteTransport(
            cast(ZyteHttpFetcher, header_provider), max_bytes=max_bytes, timeout_seconds=90, mode=mode, budget=budget
        )
        client = BoundedHttpCapture(
            max_requests=max_requests,
            timeout_seconds=90,
            min_request_interval_seconds=0.1,
            user_agent="SpicyRegs/scorecards",
            error_type=ScorecardTransportError,
            transport=transport,
            clock=lambda: datetime.now(UTC),
        )
        routes[mode] = (transport, client)

    def request(url, *, method="GET", content=None, request_headers=None):
        if method != "GET" or content is not None:
            raise ScorecardTransportError("Selected Zyte transport supports publisher GET requests only")
        parts = urlsplit(url)
        browser_route = (
            browser_api
            and parts.scheme == "https"
            and parts.netloc == "scorecard.ijm.org"
            and parts.path.startswith("/wp-json/rds-bt50-scorecard/v1/")
        )
        mode = BROWSER_HTML if browser_route else HTTP_RESPONSE_BODY
        if request_headers and (
            mode != HTTP_RESPONSE_BODY
            or source.publisher_id != "c4ip"
            or parts.scheme != "https"
            or parts.netloc != "cscp.c4ip.org"
            or not parts.path.startswith("/public/")
        ):
            raise ScorecardTransportError("Publisher app headers require the selected original API host")
        transport, client = routes[mode]
        before = len(transport.records)
        header_token = target_headers.set((url, dict(request_headers)) if request_headers else None)
        try:
            return client.capture(url, max_bytes=max_bytes, max_attempts=1)
        except CredentialRefusedError:
            raise
        except Exception:
            # Provider failures may include credentials; do not copy exception
            # text into public diagnostics or continue to another publisher.
            source.event("scorecard-proxy-failure", provider="zyte", mode=mode)
            raise ScorecardTransportError("Selected Zyte acquisition failed; prior edition preserved") from None
        finally:
            target_headers.reset(header_token)
            for record in transport.records[before:]:
                fields = dict(
                    provider="zyte",
                    mode=record.mode,
                    proxied_client=record.proxied_client,
                    body_is_publisher_bytes=record.body_is_publisher_bytes,
                    zyte_request_id=record.zyte_request_id,
                    requested_url=record.requested_url,
                    resolved_url=record.resolved_url,
                    status_code=record.status_code,
                    byte_size=record.byte_size,
                    body_retained=False,
                )
                if source.policy != "metadata_only":
                    fields["sha256"] = "sha256:" + record.sha256
                source.event("scorecard-proxy", **fields)

    try:
        yield RequestFetcher(request)
    finally:
        for _, client in routes.values():
            client.close()


def fetch_for_publishers(publishers, *, browser_publishers=(), max_bytes=MAX_BYTES, max_requests=MAX_REQUESTS):
    """Select transport explicitly per publisher without enabling any source."""
    from spicy_regs.scorecards.registry import ADAPTERS

    validate_limits(max_bytes, max_requests)
    selected = frozenset(publishers)
    browser_selected = frozenset(browser_publishers)
    if (selected | browser_selected) - ADAPTERS:
        raise ValueError("Unknown publisher selected for Zyte")
    if selected & browser_selected:
        raise ValueError("Choose one Zyte rendition per publisher")
    if browser_selected - {"ijm"}:
        raise ValueError("Browser API rendition is only supported for IJM")

    @contextmanager
    def factory(source):
        from spicy_regs.transforms.build_scorecards import bounded_fetch

        proxied = source.publisher_id in selected | browser_selected
        chosen = zyte_fetch if proxied else bounded_fetch
        options = {"browser_api": True} if source.publisher_id in browser_selected else {}
        with chosen(source, max_bytes=max_bytes, max_requests=max_requests, **options) as fetch:
            yield fetch

    return factory


@contextmanager
def bounded_fetch(source, *, max_bytes=MAX_BYTES, max_requests=MAX_REQUESTS):
    """Bound exact HTTP payloads in memory, outside all workflow upload trees."""
    validate_limits(max_bytes, max_requests)
    count = 0
    with httpx.Client(
        timeout=httpx.Timeout(30, connect=10),
        follow_redirects=False,
        headers={"Accept-Encoding": "identity", "User-Agent": "SpicyRegs/scorecards"},
    ) as client:

        def request(url, *, method="GET", content=None, request_headers=None):
            nonlocal count
            if method not in {"GET", "POST"} or (content is not None) != (method == "POST"):
                raise ScorecardRefreshError("Publisher request method and body disagree")
            requested, current = url, url
            host = urlsplit(url).hostname
            # The reader validates whether this exact URL permits HTTP before
            # calling transport. Transport never infers a publisher exception.
            schemes = {urlsplit(url).scheme} & {"http", "https"}
            started = time.monotonic()
            for _ in range(6):
                parsed = urlsplit(current)
                if (
                    parsed.scheme not in schemes
                    or parsed.hostname not in {host, "www." + str(host), str(host).removeprefix("www.")}
                    or parsed.username
                    or parsed.password
                    or parsed.port
                    or (request_headers and parsed.hostname != host)
                ):
                    raise ScorecardRefreshError("Publisher redirect leaves the selected host")
                if count >= max_requests:
                    raise ScorecardRefreshError("Scorecard HTTP request budget exhausted")
                count += 1
                observed = datetime.now(UTC).isoformat()
                raw = bytearray()
                try:
                    headers = {"Content-Type": "application/json"} if method == "POST" else {}
                    if request_headers:
                        headers.update(request_headers)
                    with client.stream(method, current, content=content, headers=headers) as response:
                        for chunk in response.iter_raw():
                            if len(raw) + len(chunk) > max_bytes or time.monotonic() - started > 90:
                                raise ScorecardRefreshError("Source HTTP response exceeds its acquisition bound")
                            raw.extend(chunk)
                        capture = CapturedBodyResponse(
                            requested,
                            str(response.url),
                            response.status_code,
                            response.headers.get("content-type"),
                            observed,
                            bytes(raw),
                            content_encoding=response.headers.get("content-encoding", "identity"),
                            method=method,
                            request_body=content,
                        )
                        if response.is_redirect:
                            source.capture(capture, stage="redirect")
                            if method == "POST":
                                raise ScorecardRefreshError("Publisher POST redirects require an explicit source rule")
                            location = response.headers.get("location")
                            if not location:
                                raise ScorecardRefreshError("Publisher redirect has no location")
                            current = urljoin(current, location)
                            continue
                        return capture
                except BaseException as error:
                    source.event(
                        "capture-incomplete",
                        stage="http",
                        error_type=type(error).__name__,
                        response_complete=False,
                        body_retained=False,
                        requested_url=current,
                        bytes_received=len(raw),
                    )
                    raise
            raise ScorecardRefreshError("Publisher redirect limit exceeded")

        yield RequestFetcher(request)
