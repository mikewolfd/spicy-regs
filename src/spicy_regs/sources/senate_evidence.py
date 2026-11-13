"""One serving-rung record for successful Senate roster and vote reads."""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from spicy_docs.transport.captured import CapturedBodyResponse


def record_serving_rung(
    evidence: Any,
    capture: "CapturedBodyResponse",
    *,
    operation: str,
    original_url: str,
    transport: str,
    provider_request_id: str | None = None,
) -> None:
    """Link the successful rung to its exact capture without retaining the body twice.

    Emit only after source validation. Legacy stage/event names remain separate
    compatibility records; readers need only this event to identify the rung.
    """
    if evidence is not None:
        evidence.event(
            "source-serving-rung",
            operation=operation,
            original_url=original_url,
            transport=transport,
            provider_request_id=provider_request_id,
            requested_url=capture.requested_url,
            resolved_url=capture.resolved_url,
            observed_at=capture.observed_at,
            sha256=capture.sha256,
        )
