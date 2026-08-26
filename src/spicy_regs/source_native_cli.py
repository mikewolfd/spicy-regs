"""Publish or independently verify one SpicyRegs source-native release."""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Final, TextIO

import httpx
from rulespec_artifacts import (
    ArtifactPin,
    LocalMemberSource,
    Producer,
    admit_artifact,
    canonical_json_bytes,
)

from spicy_regs.federal_register_source_native import (
    FederalRegisterFetch,
    FederalRegisterSourceError,
    iter_federal_register_pages,
)
from spicy_regs.publication import ImmutablePublicationError
from spicy_regs.regulations_gov_source_native import (
    COMMENT_COLLECTION,
    DOCUMENT_COLLECTION,
    DOCKET_COLLECTION,
    MirrulationsObjectReader,
    RegulationsGovSourceError,
    iter_regulations_gov_comment_pages,
    iter_regulations_gov_document_pages,
    iter_regulations_gov_docket_pages,
)
from spicy_regs.schemas import COMMENT, DOCUMENT, DOCKET
from spicy_regs.source_native import (
    VERIFIER_ID,
    VERIFIER_VERSION,
    SourceNativeReleaseBuild,
    SourceNativeReleaseError,
    SourceNativeReleasePublisher,
    verify_source_native_release,
)
from spicy_regs.source_native_profile import SourceNativePage, SourceNativeProfile
from spicy_regs.source_native_profiles import (
    FEDERAL_REGISTER_PROFILE,
    REGULATIONS_GOV_COMMENT_PROFILE,
    REGULATIONS_GOV_DOCUMENT_PROFILE,
    REGULATIONS_GOV_DOCKET_PROFILE,
)
from spicy_regs.sources import mirrulations

SOURCE_FEDERAL_REGISTER: Final = "federal-register"
SOURCE_REGULATIONS_DOCUMENTS: Final = "regulations-documents"
SOURCE_REGULATIONS_DOCKETS: Final = "regulations-dockets"
SOURCE_REGULATIONS_COMMENTS: Final = "regulations-comments"
SOURCE_CHOICES: Final = (
    SOURCE_FEDERAL_REGISTER,
    SOURCE_REGULATIONS_DOCUMENTS,
    SOURCE_REGULATIONS_DOCKETS,
    SOURCE_REGULATIONS_COMMENTS,
)

_USER_AGENT = "spicy-regs-source-native/1.0 (https://github.com/civictechdc/spicy-regs)"
_MAX_HTTP_ATTEMPTS = 5

RegulationsReaderFactory = Callable[[str, str], MirrulationsObjectReader]


def _date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error
    if parsed.isoformat() != value:
        raise argparse.ArgumentTypeError("date must use YYYY-MM-DD")
    return parsed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    publish = subparsers.add_parser(
        "publish",
        help="Acquire, publish, and producer-verify one release",
    )
    publish.add_argument("--source", choices=SOURCE_CHOICES, required=True)
    publish.add_argument("--since", type=_date, required=True)
    publish.add_argument("--until", type=_date, required=True)
    publish.add_argument(
        "--agency",
        action="append",
        help="Regulations.gov agency code; repeat for multiple agencies",
    )
    publish.add_argument("--destination", type=Path, required=True)
    publish.add_argument("--implementation-id", required=True)

    verify = subparsers.add_parser(
        "verify",
        help="Independently replay and verify one immutable release",
    )
    verify.add_argument("--source", choices=SOURCE_CHOICES, required=True)
    verify.add_argument("--release", type=Path, required=True)
    verify.add_argument("--logical-id", required=True)
    verify.add_argument("--artifact-digest", required=True)
    verify.add_argument(
        "--accepted-verifier-implementation-id",
        action="append",
        required=True,
    )
    return parser


def _profile(source: str) -> SourceNativeProfile:
    if source == SOURCE_FEDERAL_REGISTER:
        return FEDERAL_REGISTER_PROFILE
    if source == SOURCE_REGULATIONS_DOCUMENTS:
        return REGULATIONS_GOV_DOCUMENT_PROFILE
    if source == SOURCE_REGULATIONS_DOCKETS:
        return REGULATIONS_GOV_DOCKET_PROFILE
    if source == SOURCE_REGULATIONS_COMMENTS:
        return REGULATIONS_GOV_COMMENT_PROFILE
    raise SourceNativeReleaseError(f"unsupported source {source!r}")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _instant(clock: Callable[[], datetime]) -> str:
    value = clock()
    if value.tzinfo is None or value.utcoffset() is None:
        raise SourceNativeReleaseError("CLI clock must return a timezone-aware instant")
    return (
        value.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _fetch_with_retries(client: httpx.Client, url: str) -> bytes:
    for attempt in range(1, _MAX_HTTP_ATTEMPTS + 1):
        try:
            response = client.get(url)
            if response.status_code == 429 or response.status_code >= 500:
                raise httpx.HTTPStatusError(
                    "retryable Federal Register response",
                    request=response.request,
                    response=response,
                )
            response.raise_for_status()
            if not response.content:
                raise FederalRegisterSourceError(
                    "Federal Register returned an empty response"
                )
            return response.content
        except (httpx.HTTPError, FederalRegisterSourceError):
            if attempt == _MAX_HTTP_ATTEMPTS:
                raise
            time.sleep(min(2**attempt, 30))
    raise AssertionError("unreachable")


@contextmanager
def _fetcher(injected: FederalRegisterFetch | None) -> Iterator[FederalRegisterFetch]:
    if injected is not None:
        yield injected
        return
    with httpx.Client(
        headers={"Accept": "application/json", "User-Agent": _USER_AGENT},
        timeout=httpx.Timeout(60.0, connect=30.0),
        follow_redirects=True,
    ) as client:
        yield lambda url: _fetch_with_retries(client, url)


def _default_regulations_reader(agency: str, collection: str) -> MirrulationsObjectReader:
    if collection == DOCUMENT_COLLECTION:
        record_type = DOCUMENT
    elif collection == DOCKET_COLLECTION:
        record_type = DOCKET
    elif collection == COMMENT_COLLECTION:
        record_type = COMMENT
    else:
        raise RegulationsGovSourceError(f"unsupported Mirrulations collection {collection!r}")
    return mirrulations.MirrulationsReader(
        mirrulations.s3_resource(),
        mirrulations.BUCKET,
        mirrulations.PREFIX,
        agency,
        record_type,
        processed_keys=None,
        since_year=None,
        retain_keys=False,
        fail_fast=True,
    )


def _success(
    command: str,
    source_name: str,
    release: Path,
    *,
    pin: ArtifactPin,
    spec: Mapping[str, Any],
) -> dict[str, object]:
    return {
        "artifactDigest": pin.artifact_digest,
        "command": command,
        "logicalId": pin.logical_id,
        "ok": True,
        "release": str(release.resolve()),
        "source": source_name,
        "sourceNativeSchemaSetDigest": spec["sourceNativeSchemaSetDigest"],
        "sourceStateDigest": spec["sourceStateDigest"],
        "sourceStateScope": spec["sourceStateScope"],
        "sourceSystemId": spec["sourceSystemId"],
        "sourceSystemVersion": spec["sourceSystemVersion"],
    }


def _emit(stream: TextIO, value: Mapping[str, object]) -> None:
    stream.write(canonical_json_bytes(value).decode("utf-8") + "\n")


def _query_scope(args: argparse.Namespace) -> dict[str, Any]:
    if args.source == SOURCE_FEDERAL_REGISTER:
        if args.agency:
            raise SourceNativeReleaseError("--agency is only valid for Regulations.gov")
        return {
            "publishedFrom": args.since.isoformat(),
            "publishedThrough": args.until.isoformat(),
        }
    agencies = sorted(set(args.agency or []))
    if not agencies:
        raise SourceNativeReleaseError(
            "at least one --agency is required for Regulations.gov"
        )
    if args.source == SOURCE_REGULATIONS_DOCUMENTS:
        return {
            "agencies": agencies,
            "publishedFrom": args.since.isoformat(),
            "publishedThrough": args.until.isoformat(),
        }
    if args.source == SOURCE_REGULATIONS_COMMENTS:
        return {
            "agencies": agencies,
            "postedFrom": args.since.isoformat(),
            "postedThrough": args.until.isoformat(),
        }
    return {
        "agencies": agencies,
        "modifiedFrom": args.since.isoformat(),
        "modifiedThrough": args.until.isoformat(),
    }


def _regulations_pages(
    args: argparse.Namespace,
    query_scope: Mapping[str, Any],
    read_regulations: RegulationsReaderFactory,
) -> Iterator[SourceNativePage]:
    if args.source == SOURCE_REGULATIONS_DOCUMENTS:
        return iter_regulations_gov_document_pages(
            lambda agency: read_regulations(agency, DOCUMENT_COLLECTION),
            query_scope=query_scope,
        )
    if args.source == SOURCE_REGULATIONS_COMMENTS:
        return iter_regulations_gov_comment_pages(
            lambda agency: read_regulations(agency, COMMENT_COLLECTION),
            query_scope=query_scope,
        )
    return iter_regulations_gov_docket_pages(
        lambda agency: read_regulations(agency, DOCKET_COLLECTION),
        query_scope=query_scope,
    )


def _publish(
    args: argparse.Namespace,
    *,
    fetch: FederalRegisterFetch | None,
    read_regulations: RegulationsReaderFactory | None,
    clock: Callable[[], datetime],
) -> dict[str, object]:
    profile = _profile(args.source)
    query_scope = _query_scope(args)
    if args.destination.exists() or args.destination.is_symlink():
        raise FileExistsError(f"refusing to replace immutable release: {args.destination}")
    started_at = _instant(clock)
    producer = Producer(
        product="spicy-regs",
        implementation_id=args.implementation_id,
        verifier_id=VERIFIER_ID,
        verifier_version=VERIFIER_VERSION,
        verifier_implementation_id=args.implementation_id,
    )
    build = SourceNativeReleaseBuild(
        query_scope=query_scope,
        producer=producer,
        started_at=started_at,
    )
    if args.source == SOURCE_FEDERAL_REGISTER:
        with _fetcher(fetch) as active_fetch:
            published = SourceNativeReleasePublisher(profile, clock=clock).publish(
                iter_federal_register_pages(active_fetch, query_scope=query_scope),
                build=build,
                destination=args.destination,
            )
    else:
        active_reader = read_regulations or _default_regulations_reader
        published = SourceNativeReleasePublisher(profile, clock=clock).publish(
            _regulations_pages(args, query_scope, active_reader),
            build=build,
            destination=args.destination,
        )
    return _success(
        "publish",
        args.source,
        published.root,
        pin=published.artifact.pin,
        spec=published.artifact.root["spec"],
    )


def _verify(args: argparse.Namespace) -> dict[str, object]:
    profile = _profile(args.source)
    expected_pin = ArtifactPin(args.logical_id, args.artifact_digest)
    source = LocalMemberSource(args.release)
    artifact = admit_artifact(
        source,
        expected_pin=expected_pin,
        semantic_verifier=lambda artifact, source: verify_source_native_release(
            artifact,
            source,
            profile=profile,
        ),
    )
    accepted = frozenset(args.accepted_verifier_implementation_id)
    producer = artifact.root["producer"]
    if producer["verifierImplementationId"] not in accepted:
        raise SourceNativeReleaseError(
            "source-native verifier implementation is not accepted"
        )
    return _success(
        "verify",
        args.source,
        args.release,
        pin=artifact.pin,
        spec=artifact.root["spec"],
    )


def _error_code(error: Exception) -> str:
    if isinstance(error, (FileExistsError, ImmutablePublicationError)):
        return "destination-exists"
    if isinstance(error, (FederalRegisterSourceError, RegulationsGovSourceError)):
        return "acquisition-failed"
    if isinstance(error, SourceNativeReleaseError):
        return "release-invalid"
    if isinstance(error, httpx.HTTPError):
        return "transport-failed"
    return "operation-failed"


def main(
    argv: list[str] | None = None,
    *,
    fetch: FederalRegisterFetch | None = None,
    read_regulations: RegulationsReaderFactory | None = None,
    clock: Callable[[], datetime] = _now,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    """Run the one source-native operator command through injected adapters."""

    output = stdout or sys.stdout
    errors = stderr or sys.stderr
    args = _parser().parse_args(argv)
    try:
        result = (
            _publish(
                args,
                fetch=fetch,
                read_regulations=read_regulations,
                clock=clock,
            )
            if args.command == "publish"
            else _verify(args)
        )
    except (
        FileExistsError,
        ImmutablePublicationError,
        FederalRegisterSourceError,
        RegulationsGovSourceError,
        SourceNativeReleaseError,
        httpx.HTTPError,
        OSError,
        ValueError,
    ) as error:
        _emit(
            errors,
            {
                "command": args.command,
                "error": {"code": _error_code(error), "message": str(error)},
                "ok": False,
            },
        )
        return 1
    _emit(output, result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
