"""One source-native operator CLI across all supported source profiles."""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path

import pytest

from spicy_regs.regulations_gov_source_native import RegulationsGovSourceError
from spicy_regs.source_native_cli import main

IMPLEMENTATION_ID = "git+https://example.test/spicy-regs@" + "a" * 40
FIXED_NOW = datetime(2026, 8, 25, tzinfo=timezone.utc)


def _federal_document() -> dict[str, object]:
    return {
        "agencies": [],
        "body_html_url": None,
        "document_number": "2026-00001",
        "html_url": "https://www.federalregister.gov/d/2026-00001",
        "pdf_url": None,
        "publication_date": "2026-08-25",
        "regulation_id_numbers": [],
        "title": "CLI source-native rule",
        "topics": [],
        "type": "Rule",
    }


def _federal_response(*, count: int = 1, total_pages: int = 1) -> bytes:
    return json.dumps(
        {
            "count": count,
            "next_page_url": None,
            "results": [_federal_document()],
            "total_pages": total_pages,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _regulations_document() -> dict[str, object]:
    return {
        "data": {
            "id": "EPA-2026-0001-0001",
            "type": "documents",
            "attributes": {
                "agencyId": "EPA",
                "docketId": "EPA-2026-0001",
                "fileFormats": [
                    {
                        "fileUrl": (
                            "https://downloads.regulations.gov/"
                            "EPA-2026-0001-0001/content.pdf"
                        ),
                        "format": "pdf",
                        "size": 10,
                    }
                ],
                "frDocNum": "2026-00001",
                "modifyDate": "2026-08-25T00:00:00Z",
                "postedDate": "2026-08-25T00:00:00Z",
                "reasonWithdrawn": None,
                "topics": ["Air"],
                "withdrawn": False,
            },
        }
    }


def _regulations_docket() -> dict[str, object]:
    return {
        "data": {
            "id": "EPA-2026-0001",
            "type": "dockets",
            "attributes": {
                "agencyId": "EPA",
                "dkAbstract": "Exact docket facts",
                "keywords": ["Air"],
                "modifyDate": "2026-08-25T00:00:00Z",
                "rin": None,
                "title": "CLI docket",
            },
        }
    }


@dataclass(frozen=True, slots=True)
class _Object:
    key: str
    etag: str
    version_id: str | None
    content: bytes


class _Reader:
    def __init__(self, source_object: _Object) -> None:
        self.source_object = source_object

    def iter_source_objects(self, *, max_bytes: int) -> Iterator[_Object]:
        assert max_bytes == 16 * 1024 * 1024
        yield self.source_object


def _regulations_factory(agency: str, collection: str) -> _Reader:
    assert agency == "EPA"
    if collection == "documents":
        value = _regulations_document()
        identity = "EPA-2026-0001-0001"
        key = f"raw-data/EPA/EPA-2026-0001/text-1/documents/{identity}.json"
    else:
        assert collection == "dockets"
        value = _regulations_docket()
        identity = "EPA-2026-0001"
        key = f"raw-data/EPA/{identity}/text-1/docket/{identity}.json"
    return _Reader(
        _Object(
            key=key,
            etag=f'"{collection}-etag"',
            version_id=f"{collection}-version",
            content=json.dumps(value, indent=2).encode(),
        )
    )


def _publish_args(destination: Path, source: str) -> list[str]:
    args = [
        "publish",
        "--source",
        source,
        "--since",
        "2026-08-25",
        "--until",
        "2026-08-25",
    ]
    if source.startswith("regulations-"):
        args.extend(["--agency", "EPA"])
    return [
        *args,
        "--destination",
        str(destination),
        "--implementation-id",
        IMPLEMENTATION_ID,
    ]


def _verify_args(destination: Path, source: str, published: dict[str, object]) -> list[str]:
    return [
        "verify",
        "--source",
        source,
        "--release",
        str(destination),
        "--logical-id",
        str(published["logicalId"]),
        "--artifact-digest",
        str(published["artifactDigest"]),
        "--accepted-verifier-implementation-id",
        IMPLEMENTATION_ID,
    ]


@pytest.mark.parametrize(
    "source",
    ["federal-register", "regulations-documents", "regulations-dockets"],
)
def test_cli_publishes_and_independently_verifies_with_machine_output(
    tmp_path: Path,
    source: str,
) -> None:
    destination = tmp_path / source
    publish_output = StringIO()
    publish_errors = StringIO()

    assert main(
        _publish_args(destination, source),
        fetch=lambda _url: _federal_response(),
        read_regulations=_regulations_factory,
        clock=lambda: FIXED_NOW,
        stdout=publish_output,
        stderr=publish_errors,
    ) == 0
    assert publish_errors.getvalue() == ""
    published = json.loads(publish_output.getvalue())
    assert published["command"] == "publish"
    assert published["source"] == source
    assert published["ok"] is True
    assert published["logicalId"].startswith(
        "urn:spicy:artifact:spicyregs-source-native-release:"
    )
    assert published["artifactDigest"].startswith("sha256:")
    assert published["release"] == str(destination.resolve())

    verify_output = StringIO()
    assert main(
        _verify_args(destination, source, published),
        stdout=verify_output,
        stderr=StringIO(),
    ) == 0
    verified = json.loads(verify_output.getvalue())
    assert verified["command"] == "verify"
    assert verified["source"] == source
    assert verified["logicalId"] == published["logicalId"]
    assert verified["artifactDigest"] == published["artifactDigest"]
    assert verified["sourceStateDigest"] == published["sourceStateDigest"]


def test_cli_reports_capped_day_failure_without_partial_release(tmp_path: Path) -> None:
    destination = tmp_path / "capped"
    output = StringIO()
    errors = StringIO()

    assert main(
        _publish_args(destination, "federal-register"),
        fetch=lambda _url: _federal_response(count=10_000, total_pages=10),
        clock=lambda: FIXED_NOW,
        stdout=output,
        stderr=errors,
    ) == 1
    assert output.getvalue() == ""
    failure = json.loads(errors.getvalue())
    assert failure["ok"] is False
    assert failure["error"]["code"] == "acquisition-failed"
    assert "result cap is ambiguous" in failure["error"]["message"]
    assert not destination.exists()


def test_cli_reports_regulations_reader_failure_without_partial_release(
    tmp_path: Path,
) -> None:
    destination = tmp_path / "failed-documents"

    def fail(_agency: str, _collection: str):
        raise RegulationsGovSourceError("source listing failed")

    errors = StringIO()
    assert main(
        _publish_args(destination, "regulations-documents"),
        read_regulations=fail,
        clock=lambda: FIXED_NOW,
        stdout=StringIO(),
        stderr=errors,
    ) == 1
    failure = json.loads(errors.getvalue())
    assert failure["error"]["code"] == "acquisition-failed"
    assert "source listing failed" in failure["error"]["message"]
    assert not destination.exists()


@pytest.mark.parametrize(
    "source",
    ["federal-register", "regulations-documents", "regulations-dockets"],
)
def test_cli_refuses_to_replace_release_before_reacquisition(
    tmp_path: Path,
    source: str,
) -> None:
    destination = tmp_path / source
    calls: list[str] = []

    def fetch(url: str) -> bytes:
        calls.append(url)
        return _federal_response()

    def read(agency: str, collection: str):
        calls.append(f"{agency}:{collection}")
        return _regulations_factory(agency, collection)

    assert main(
        _publish_args(destination, source),
        fetch=fetch,
        read_regulations=read,
        clock=lambda: FIXED_NOW,
        stdout=StringIO(),
        stderr=StringIO(),
    ) == 0
    call_count = len(calls)
    root_before = (destination / "artifact.json").read_bytes()
    errors = StringIO()

    assert main(
        _publish_args(destination, source),
        fetch=fetch,
        read_regulations=read,
        clock=lambda: FIXED_NOW,
        stdout=StringIO(),
        stderr=errors,
    ) == 1
    failure = json.loads(errors.getvalue())
    assert failure["error"]["code"] == "destination-exists"
    assert len(calls) == call_count
    assert (destination / "artifact.json").read_bytes() == root_before


def test_cli_verify_refuses_a_mismatched_source_profile(tmp_path: Path) -> None:
    destination = tmp_path / "documents"
    output = StringIO()
    assert main(
        _publish_args(destination, "regulations-documents"),
        read_regulations=_regulations_factory,
        clock=lambda: FIXED_NOW,
        stdout=output,
        stderr=StringIO(),
    ) == 0
    published = json.loads(output.getvalue())
    errors = StringIO()

    assert main(
        _verify_args(destination, "regulations-dockets", published),
        stdout=StringIO(),
        stderr=errors,
    ) == 1
    failure = json.loads(errors.getvalue())
    assert failure["error"]["code"] == "release-invalid"
    assert "unsupported Regulations.gov dockets profile" in failure["error"]["message"]
