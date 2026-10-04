"""Retained page replay preserves raw evidence and refuses changed PDF scope."""

from hashlib import sha256

import pytest
from spicy_docs.extraction.model import Box, Observation, PageContent, PageResult, Raster, TableObservation, TextBlock

from spicy_regs.etl_receipts import decode_exact_json, exact_json
from spicy_regs.scorecards.extraction_replay import PageObservationReplay, page_bytes, read_page
from spicy_regs.scorecards.replay import ScorecardReplayError

SOURCE = b"%PDF-synthetic-input"


def page(number=1, total=1):
    table = TableObservation(number, Box(), 1, 2, ((None, ""),), ((None, Box()),), raw={"native": "A+"})
    block = TextBlock("A+", confidence=0.1, observation="synthetic-observation")
    observation = Observation(
        "synthetic-observation",
        "A+",
        {"backend": "docling"},
        {"status": "SUCCESS", "bytes": b"\x00\xff", "float": 0.1, "collision": ["bytes", "text"]},
        (block,),
        images=(Raster(b"retained-image", 1, 1),),
        tables=(table,),
    )
    return PageResult(
        dict(
            page=number,
            page_count=total,
            source_sha256=sha256(SOURCE).hexdigest(),
            source_size_bytes=len(SOURCE),
            media_type="application/pdf",
        ),
        PageContent((block,), (observation,)),
        (table,),
    )


def test_raw_page_roundtrip_preserves_bytes_floats_geometry_and_null_cells():
    original = page()
    encoded = page_bytes(original)
    restored = read_page(encoded)
    assert restored == original
    assert restored.tables[0].cells == ((None, ""),)
    assert restored.content.observations[0].raw["bytes"] == b"\x00\xff"
    assert restored.content.observations[0].raw["float"].hex() == (0.1).hex()
    assert page_bytes(restored) == encoded


def test_replay_uses_existing_extractor_interface_without_a_model_request():
    original = page()
    extractor = PageObservationReplay([read_page(page_bytes(original))])
    with pytest.raises(ScorecardReplayError, match="Not every"):
        extractor.complete()
    assert list(extractor.extract(SOURCE, media_type="application/pdf")) == [original]
    extractor.complete()


@pytest.mark.parametrize(
    "source,media,pages",
    [(b"different", "application/pdf", None), (SOURCE, "image/png", None), (SOURCE, "application/pdf", [1])],
)
def test_changed_source_or_partial_selection_refuses(source, media, pages):
    with pytest.raises(ScorecardReplayError, match="request differs"):
        list(PageObservationReplay([page()]).extract(source, media_type=media, pages=pages))


@pytest.mark.parametrize("pages", [[page(2, 2)], [page(2, 2), page(1, 2)], [page(), page()]])
def test_incomplete_duplicate_or_reordered_observations_refuse(pages):
    with pytest.raises(ScorecardReplayError, match="incomplete"):
        PageObservationReplay(pages)


def test_unknown_fields_and_ambiguous_evidence_encoding_refuse():
    from dataclasses import asdict

    record = dict(format_version="scorecard-page-observations/1", page=asdict(page()))
    assert isinstance(record["page"], dict)
    record["page"]["unrecognized_field"] = "retained but unsupported"
    with pytest.raises(ScorecardReplayError, match="typed shape"):
        read_page(exact_json(record).encode())
    with pytest.raises(ValueError):
        decode_exact_json('["int",true]')
    with pytest.raises(ValueError):
        decode_exact_json('["dict",[["x",["str","first"]],["x",["str","second"]]]]')
