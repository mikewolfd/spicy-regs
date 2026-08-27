"""Injected source semantics for the single source-native publisher."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, Protocol


class SourceNativePage(Protocol):
    traversal_index: int
    page_index: int
    window_index: int
    window_page_index: int
    request_key: str
    source_cursor: str | None
    response_bytes: bytes
    evidence_media_type: str


class TraversalCheck(Protocol):
    def add(self, response: Mapping[str, Any], *, page_index: int) -> None: ...

    def finish(self) -> None: ...


class NextPage(Protocol):
    def __call__(
        self,
        response: Mapping[str, Any],
        *,
        seen_urls: set[str],
    ) -> str | None: ...


class WrapRecord(Protocol):
    def __call__(
        self,
        record: Mapping[str, Any],
        *,
        schema_digest: str,
    ) -> Mapping[str, Any]: ...


class ValidateRecordScope(Protocol):
    def __call__(
        self,
        record: Mapping[str, Any],
        *,
        query_scope: Mapping[str, Any],
        page_window: object | None,
    ) -> None: ...


class RecordsIncluded(Protocol):
    def __call__(
        self,
        response: Mapping[str, Any],
        *,
        query_scope: Mapping[str, Any],
        page_window: object | None,
    ) -> bool: ...


class AcquisitionCheck(Protocol):
    def add_window(
        self,
        response: Mapping[str, Any],
        *,
        page_window: object | None,
        records_included: bool,
        response_bytes: bytes,
    ) -> None: ...

    def finish(self, *, query_scope: Mapping[str, Any]) -> None: ...


class ObservationVersion(Protocol):
    """Return the exact source version used to select one public observation."""

    def __call__(self, record: Mapping[str, Any]) -> str | None: ...


@dataclass(frozen=True, slots=True)
class SourceNativeProfile:
    """Source-owned functions injected into common publication machinery."""

    name: str
    source_system_id: str
    source_system_version: str
    acquisition_policy_id: str
    acquisition_policy_version: str
    scope_id: str
    source_schema_key: str
    source_schema: Mapping[str, Any]
    record_stem: str
    max_traversals: int
    source_state_scope: Literal["complete-snapshot", "observed-crawl"]
    traversal_acceptance: Literal[
        "single-observed-traversal",
        "source-enumeration",
        "stable-consecutive-traversals",
    ]
    acquisition_policy: Callable[[Mapping[str, Any]], Mapping[str, Any]]
    validate_query_scope: Callable[[Mapping[str, Any]], Mapping[str, Any]]
    parse_page_response: Callable[[bytes], Mapping[str, Any]]
    next_page: NextPage
    traversal_check: Callable[[], TraversalCheck]
    classify_record: Callable[[object], Mapping[str, Any]]
    wrap_record: WrapRecord
    record_digest: Callable[[Mapping[str, Any]], str]
    rendition_rows: Callable[[Mapping[str, Any]], Sequence[Mapping[str, Any]]]
    source_schema_declaration: Callable[[], Mapping[str, Any]]
    source_schema_digest: Callable[[], str]
    validate_record_scope: ValidateRecordScope
    records_included: RecordsIncluded
    acquisition_check: Callable[[], AcquisitionCheck]
    page_window: Callable[[str], object] | None = None
    observation_version: ObservationVersion | None = None
    refuse_equal_observation_versions: bool = False

    def __post_init__(self) -> None:
        if not self.name or not self.source_system_id or not self.source_system_version:
            raise ValueError("source-native profile identity must be nonempty")
        if not self.acquisition_policy_id or not self.acquisition_policy_version:
            raise ValueError("source-native acquisition policy identity must be nonempty")
        if not self.scope_id or not self.source_schema_key or not self.record_stem:
            raise ValueError("source-native profile member identity must be nonempty")
        if self.max_traversals < 1:
            raise ValueError("source-native profile traversal bound must be positive")
        if self.refuse_equal_observation_versions and self.observation_version is None:
            raise ValueError(
                "equal observation versions can be refused only by a versioned profile"
            )

        if (
            self.source_state_scope == "complete-snapshot"
            and self.traversal_acceptance != "source-enumeration"
        ):
            raise ValueError(
                "complete source state requires a source-enumeration acceptance proof"
            )


__all__ = [
    "AcquisitionCheck",
    "ObservationVersion",
    "RecordsIncluded",
    "SourceNativePage",
    "SourceNativeProfile",
    "TraversalCheck",
]
