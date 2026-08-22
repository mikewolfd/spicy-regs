"""Hermetic contracts for official Supreme Court opinion ingestion."""

from __future__ import annotations

from pathlib import Path

import httpx
import pyarrow.parquet as pq
import pytest

from spicy_regs.sources import r2
from spicy_regs.sources.supreme_court_opinions import (
    SupremeCourtOpinionsReader,
    parse_term_index,
)
from spicy_regs.transforms.build_supreme_court_opinions import (
    _shape,
    build_supreme_court_opinions,
)
from tests.pdf_fixtures import make_pdf

INDEX_HTML = """
<table>
  <tr>
    <td>58</td>
    <td>6/30/23</td>
    <td>21-476</td>
    <td><a href="/opinions/22pdf/600us1r58_7khn.pdf"
      title="The First Amendment limits compelled expression.">
      303 Creative LLC v. Elenis
    </a></td>
    <td>NG</td>
    <td><span>600 U.S. 570</span></td>
  </tr>
</table>
"""


def _reader_record() -> dict:
    pdf = make_pdf(["303 Creative LLC v. Elenis\nOfficial opinion text."])

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/opinions/slipopinion/22":
            return httpx.Response(
                200,
                text=INDEX_HTML,
                headers={"content-type": "text/html"},
                request=request,
            )
        if request.url.path == "/opinions/22pdf/600us1r58_7khn.pdf":
            return httpx.Response(
                200,
                content=pdf,
                headers={
                    "content-type": "application/pdf",
                    "etag": '"fixture-pdf"',
                    "last-modified": "Fri, 30 Jun 2023 12:00:00 GMT",
                },
                request=request,
            )
        return httpx.Response(404, request=request)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        return next(
            SupremeCourtOpinionsReader(
                term_years=(2022,),
                max_records=1,
                client=client,
                request_delay=0,
            ).iter_records()
        )


def test_term_index_uses_official_metadata_and_rejects_unsafe_pdf_urls():
    rows = parse_term_index(INDEX_HTML, term_year=2022)

    assert rows == [
        {
            "term_year": "2022",
            "release_number": "58",
            "date_decided": "2023-06-30",
            "docket_number": "21-476",
            "case_name": "303 Creative LLC v. Elenis",
            "holding": "The First Amendment limits compelled expression.",
            "author_code": "NG",
            "citation": "600 U.S. 570",
            "source_index_url": (
                "https://www.supremecourt.gov/opinions/slipopinion/22"
            ),
            "source_url": (
                "https://www.supremecourt.gov/opinions/22pdf/"
                "600us1r58_7khn.pdf"
            ),
            "source_document_kind": "slip-opinion",
            "source_page_start": "",
            "source_page_end": "",
        }
    ]
    with pytest.raises(ValueError, match="unsafe Supreme Court opinion URL"):
        parse_term_index(
            INDEX_HTML.replace(
                "/opinions/22pdf/600us1r58_7khn.pdf",
                "https://example.test/untrusted.pdf",
            ),
            term_year=2022,
        )


# One preliminary-print volume, three opinions, exactly as OT2019's index links
# them: the same PDF three times with a different ``#page=`` anchor each.
VOLUME_INDEX_HTML = """
<table>
  <tr>
    <td>60</td><td>7/9/20</td><td>18-1323</td>
    <td><a href="/opinions/preliminaryprint/591US2PP_web.pdf#page=537"
      title="Third.">Barr v. Lee</a></td>
    <td>PC</td><td><span>591 U.S. 979</span></td>
  </tr>
  <tr>
    <td>58</td><td>7/9/20</td><td>18-9526</td>
    <td><a href="/opinions/preliminaryprint/591US2PP_web.pdf#page=453"
      title="First.">McGirt v. Oklahoma</a></td>
    <td>NG</td><td><span>591 U.S. 894</span></td>
  </tr>
  <tr>
    <td>59</td><td>7/9/20</td><td>17-1107</td>
    <td><a href="/opinions/preliminaryprint/591US2PP_web.pdf#page=536"
      title="Second.">Sharp v. Murphy</a></td>
    <td>PC</td><td><span>591 U.S. 977</span></td>
  </tr>
</table>
"""


def test_term_index_reads_the_volume_layout_the_pre_2021_terms_use():
    """OT2017-OT2020 link *into* a volume PDF, and were unreachable for it.

    ``parse_term_index`` refused those URLs outright, so a term with a single
    volume link produced nothing at all — which is why OT2020 looked unreachable
    when in fact 53 of its 68 rows are ordinary slip PDFs. The guard was right;
    it was missing a shape.
    """
    rows = parse_term_index(VOLUME_INDEX_HTML, term_year=2019)
    by_case = {row["case_name"]: row for row in rows}

    assert set(by_case) == {"McGirt v. Oklahoma", "Sharp v. Murphy", "Barr v. Lee"}
    for row in rows:
        assert row["source_document_kind"] == "preliminary-print"
        # The fragment says where the opinion is; the document is what you GET.
        assert row["source_url"] == (
            "https://www.supremecourt.gov/opinions/preliminaryprint/591US2PP_web.pdf"
        )
        assert "#" not in row["source_url"]

    # The index gives a start and no end, so each opinion is closed against the
    # next one in the same volume — and the last runs to the document's end.
    assert (by_case["McGirt v. Oklahoma"]["source_page_start"],
            by_case["McGirt v. Oklahoma"]["source_page_end"]) == ("453", "535")
    assert (by_case["Sharp v. Murphy"]["source_page_start"],
            by_case["Sharp v. Murphy"]["source_page_end"]) == ("536", "536")
    assert (by_case["Barr v. Lee"]["source_page_start"],
            by_case["Barr v. Lee"]["source_page_end"]) == ("537", "")


def test_term_index_still_refuses_everything_it_refused_before():
    """The branch adds recognised shapes. It must give up nothing else."""
    volume = "/opinions/preliminaryprint/591US2PP_web.pdf#page=453"
    refused = {
        # Another host, wearing the right path.
        "https://example.test/opinions/preliminaryprint/x.pdf#page=1": "unsafe",
        # The Court's host, but a path this parser does not recognise.
        "/opinions/relatingtoorders/20/20a1_new.pdf": "unsafe",
        "/casehand/21pdf/x.pdf": "unsafe",
        # Right path, wrong protocol.
        "http://www.supremecourt.gov/opinions/preliminaryprint/x.pdf#page=1": "unsafe",
        # Right path, not a PDF.
        "/opinions/preliminaryprint/591US2PP_web.html#page=1": "unsafe",
        # A volume link with no page anchor names a volume and claims to be one
        # opinion — refused rather than stored whole.
        "/opinions/preliminaryprint/591US2PP_web.pdf": "no page anchor",
        "/opinions/preliminaryprint/591US2PP_web.pdf#section=intro": "no page anchor",
        "/opinions/preliminaryprint/591US2PP_web.pdf#page=0": "no page anchor",
        "/opinions/preliminaryprint/591US2PP_web.pdf#page=iv": "no page anchor",
    }
    for href, expected in refused.items():
        with pytest.raises(ValueError, match=expected):
            parse_term_index(
                VOLUME_INDEX_HTML.replace(volume, href), term_year=2019
            )

    # A slip opinion is its own PDF; an anchor on one is an unseen layout.
    with pytest.raises(ValueError, match="unsafe"):
        parse_term_index(
            INDEX_HTML.replace(
                "/opinions/22pdf/600us1r58_7khn.pdf",
                "/opinions/22pdf/600us1r58_7khn.pdf#page=3",
            ),
            term_year=2022,
        )


def test_an_index_that_is_not_the_requested_term_is_refused():
    """The Court's site does not always serve the term the URL names.

    Measured 2026-08-22: a client that had already fetched one term got the
    **OT2023** index back from ``/opinions/slipopinion/21`` — sixty real
    opinions, parsed correctly, every one about to be stamped
    ``term_year=2021``, because the term comes from the caller's loop and not
    from the page. Nothing errors. The table just quietly says three wrong
    years, which is the same class of failure as a CSV that desyncs instead of
    raising.

    A slip row is caught exactly, by its ``/opinions/{code}pdf/`` path. A volume
    row carries no term at all, so it is caught by the term's date window.
    """
    # A volume index whose decisions are a whole term late.
    later = VOLUME_INDEX_HTML.replace("7/9/20", "7/9/21")
    with pytest.raises(ValueError, match="not the term that was requested"):
        parse_term_index(later, term_year=2019)

    # And the same page asked for under its real term still parses.
    assert len(parse_term_index(later, term_year=2020)) == 3

    # A slip index served under the wrong term is refused by the path guard.
    with pytest.raises(ValueError, match="unsafe Supreme Court opinion URL"):
        parse_term_index(INDEX_HTML, term_year=2021)


def test_volume_row_stores_its_own_pages_not_the_whole_volume(tmp_path, monkeypatch):
    """A volume PDF under sixty case names would be sixty wrong rows.

    This is the reason the parser change alone would have made things worse:
    recognising the URL without slicing the pages stores a whole volume of the
    U.S. Reports as the text of each opinion in it.
    """
    volume = make_pdf([f"PAGE {n}" for n in range(1, 11)])

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/opinions/slipopinion/19":
            return httpx.Response(
                200,
                text=VOLUME_INDEX_HTML.replace("453", "3")
                .replace("536", "5")
                .replace("537", "6"),
                headers={"content-type": "text/html"},
                request=request,
            )
        if request.url.path.endswith("591US2PP_web.pdf"):
            return httpx.Response(
                200,
                content=volume,
                headers={"content-type": "application/pdf"},
                request=request,
            )
        return httpx.Response(404, request=request)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        reader = SupremeCourtOpinionsReader(term_years=(2019,), client=client, request_delay=0)
        records = list(reader.iter_records())

    rows = {row["case_name"]: _shape(row) for row in records}
    assert rows["McGirt v. Oklahoma"]["pdf_text"] == "PAGE 3\n\nPAGE 4"
    assert rows["Sharp v. Murphy"]["pdf_text"] == "PAGE 5"
    # Last in the volume: runs to the end of the document, not past it.
    assert rows["Barr v. Lee"]["pdf_text"] == "\n\n".join(
        f"PAGE {n}" for n in range(6, 11)
    )
    # The document's page count stays the document's; the slice is its own column.
    assert rows["McGirt v. Oklahoma"]["pdf_page_count"] == "10"
    assert rows["McGirt v. Oklahoma"]["source_document_kind"] == "preliminary-print"

    monkeypatch.setattr(r2, "download", lambda *_: False)
    stored = pq.read_table(
        build_supreme_court_opinions(tmp_path, records=records)
    ).to_pylist()
    assert len(stored) == 3
    assert len({row["opinion_id"] for row in stored}) == 3


def test_reader_fetches_a_shared_volume_once_and_records_a_dead_link():
    """29 index rows share one volume PDF, and four volumes 404 upstream."""
    volume = make_pdf([f"PAGE {n}" for n in range(1, 11)])
    fetches: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        fetches.append(request.url.path)
        if request.url.path == "/opinions/slipopinion/19":
            return httpx.Response(
                200,
                text=VOLUME_INDEX_HTML.replace("453", "3")
                .replace("536", "5")
                .replace("537", "6"),
                headers={"content-type": "text/html"},
                request=request,
            )
        if request.url.path.endswith("591US2PP_web.pdf"):
            return httpx.Response(
                200,
                content=volume,
                headers={"content-type": "application/pdf"},
                request=request,
            )
        return httpx.Response(404, request=request)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        records = list(
            SupremeCourtOpinionsReader(term_years=(2019,), client=client, request_delay=0).iter_records()
        )
    assert len(records) == 3
    assert fetches.count("/opinions/preliminaryprint/591US2PP_web.pdf") == 1

    # A volume the Court's own server does not serve. Loud by default...
    def missing(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/opinions/slipopinion/19":
            return httpx.Response(
                200,
                text=VOLUME_INDEX_HTML,
                headers={"content-type": "text/html"},
                request=request,
            )
        return httpx.Response(404, request=request)

    with httpx.Client(transport=httpx.MockTransport(missing)) as client:
        with pytest.raises(ValueError, match="missing upstream"):
            list(
                SupremeCourtOpinionsReader(
                    term_years=(2019,), client=client, request_delay=0
                ).iter_records()
            )

    # ...and recorded, not silent, when a run is told to survive it.
    with httpx.Client(transport=httpx.MockTransport(missing)) as client:
        reader = SupremeCourtOpinionsReader(
            term_years=(2019,), client=client, skip_missing_documents=True, request_delay=0
        )
        assert list(reader.iter_records()) == []
        assert sum(reader.missing_documents.values()) == 3


def test_reader_and_transform_preserve_identity_digest_and_extracted_text(
    tmp_path: Path,
    monkeypatch,
):
    record = _reader_record()
    row = _shape(record)
    monkeypatch.setattr(r2, "download", lambda *_: False)

    output = build_supreme_court_opinions(
        tmp_path,
        records=[record],
    )
    stored = pq.read_table(output).to_pylist()

    assert row["opinion_id"] == "scotus-2022-58-21-476"
    assert row["pdf_sha256"]
    assert row["pdf_text"] == (
        "303 Creative LLC v. Elenis\nOfficial opinion text."
    )
    assert row["source_etag"] == '"fixture-pdf"'
    assert stored == [row]
