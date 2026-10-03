"""Explicit publisher selection for bounded, evidenced Zyte acquisition."""

from contextlib import contextmanager
from datetime import UTC, datetime
from urllib.parse import urlsplit

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


@contextmanager
def zyte_fetch(source, *, max_requests=2000, max_bytes=20 * 1024 * 1024, fetcher=None, browser_api=False):
    """Keep publisher response bytes and explicitly selected browser DOM distinct.

    There is no implicit direct-to-proxy fallback or hidden retry. Every provider
    attempt consumes the same bounded budget, and proxy provenance is journaled
    separately from the source capture without retaining third-party bodies.
    """
    if browser_api and source.publisher_id != "ijm":
        raise ValueError("Browser API rendition is only supported for IJM")
    try:
        token = require_zyte_token_from_environment()
    except Exception:
        raise ScorecardTransportError("Selected Zyte transport requires a configured credential") from None
    source.root.credential = token
    provider = fetcher or ZyteHttpFetcher(token=token)
    budget = ZyteBudget(max_requests)
    routes = {}
    for mode in (HTTP_RESPONSE_BODY, BROWSER_HTML) if browser_api else (HTTP_RESPONSE_BODY,):
        transport = ZyteTransport(provider, max_bytes=max_bytes, timeout_seconds=90, mode=mode, budget=budget)
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

    def fetch(url):
        parts = urlsplit(url)
        browser_route = (
            browser_api
            and parts.scheme == "https"
            and parts.netloc == "scorecard.ijm.org"
            and parts.path.startswith("/wp-json/rds-bt50-scorecard/v1/")
        )
        mode = BROWSER_HTML if browser_route else HTTP_RESPONSE_BODY
        transport, client = routes[mode]
        before = len(transport.records)
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
        yield fetch
    finally:
        for _, client in routes.values():
            client.close()


def fetch_for_publishers(publishers, *, browser_publishers=()):
    """Select transport explicitly per publisher without enabling any source."""
    from spicy_regs.scorecards.registry import ADAPTERS

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
        with chosen(source, **options) as fetch:
            yield fetch

    return factory
