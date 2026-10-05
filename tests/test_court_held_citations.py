"""Native retained PDF derivations qualify bounded court adapters, not all courts."""

from collections import Counter
import hashlib
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.citation_sources import document_key, source_digests
from spicy_regs.court_subjects import SUBJECT_SCHEMAS, normalize_court_row, opinion_body_id
from spicy_regs.transforms.held_citations import Selection, build_held_citations, NAMESPACE
from spicy_regs.transforms.read_checkpoints import read_checkpoints


def test_retained_court_text_pdf_keys_spans_and_source_specific_abstention(tmp_path):
    fixture = json.loads((Path(__file__).parent / "fixtures/court_citations/derived-text-cohort.json").read_text())
    source = [normalize_court_row("court_opinion_pdf_extractions", row) for row in fixture["rows"]]
    by_body = {row["opinion_body_id"]: row for row in source}
    kind = "court_opinion_derived_pdf"
    with duckdb.connect() as con:
        con.register("court_opinion_pdf_extractions", pa.Table.from_pylist(source, schema=SUBJECT_SCHEMAS["court_opinion_pdf_extractions"]))
        path = build_held_citations(
            tmp_path,
            cursor=con,
            selections=[Selection(kind, (r["opinion_body_id"],)) for r in source],
            input_pins={"court_opinion_pdf_extractions": {"artifactDigest": fixture["publication"]}},
            download_prior=lambda *_: False,
        )
        rows = pq.read_table(path).to_pylist()
        assert Counter(by_body[r["document_key"]]["opinion_id"] for r in rows) == {"11264531": 12, "11209378": 6}
        assert not any(r["cite_kind"] == "case_docket_number" for r in rows)  # Governor proclamation No.20-50.
        for r in rows:
            native = by_body[r["document_key"]]
            pdf_sha = native["source_sha256"]
            text = native["text_content"]
            digest = "sha256:" + hashlib.sha256(text.encode()).hexdigest()
            assert r["text_sha256"] == digest and digest != pdf_sha
            assert text[int(r["span_start"]) : int(r["span_end"])] == r["matched_text"]
            assert r["body_rendition"] == "pdf" and r["body_derivation"].startswith("derived_pdf:")
            assert source_digests(con, kind, r["document_key"]) == [(digest,)]
        # Changing only the PDF digest must not select the old body's text.
        assert source_digests(con, kind, document_key(kind, (opinion_body_id("11264531", "sha256:" + "0" * 64),))) == []
    checkpoints = read_checkpoints(path, NAMESPACE)
    assert len(checkpoints) == 3
    zero = next(c for c in checkpoints if by_body[c["document_key"]]["opinion_id"] == "11264529")
    assert zero["findings"] == 0
    assert all(
        "case_docket_number" in json.loads(c["processing_version"])["excluded_source_rules"] for c in checkpoints
    )


def test_converted_historical_court_scopes_reread_once_without_duplicate_native_keys(tmp_path):
    import shutil
    from spicy_regs.legislative_receipts import restore_prior, write_legislative_outputs
    from spicy_regs.transforms.held_citations import write_citation_reads
    from spicy_regs.transforms.read_checkpoints import checkpoint_metadata

    fixture = json.loads((Path(__file__).parent / 'fixtures/court_citations/derived-text-cohort.json').read_text())
    source = [normalize_court_row('court_opinion_pdf_extractions', row) for row in fixture['rows']]
    legacy = {r['opinion_body_id']: json.dumps([r['opinion_id'], r['source_sha256']], separators=(',', ':')) for r in source}
    kind = 'court_opinion_derived_pdf'
    initial = tmp_path / 'historical'
    with duckdb.connect() as con:
        con.register('court_opinion_pdf_extractions', pa.Table.from_pylist(source, schema=SUBJECT_SCHEMAS['court_opinion_pdf_extractions']))
        path = build_held_citations(initial, cursor=con, selections=[Selection(kind, (r['opinion_body_id'],)) for r in source],
                                    input_pins={'court_opinion_pdf_extractions': {'artifactDigest': fixture['publication']}},
                                    download_prior=lambda *_: False)
        # Model the exact historical capture shape before the court key changed.
        checkpoints = [{**s, 'document_key': legacy[s['document_key']]} for s in read_checkpoints(path, NAMESPACE)]
        table = pq.read_table(path)
        table = table.set_column(table.schema.get_field_index('document_key'), 'document_key',
                                 pa.array([legacy[key] for key in table['document_key'].to_pylist()]))
        table = table.replace_schema_metadata(checkpoint_metadata(path, NAMESPACE, checkpoints))
        pq.write_table(table, path)
        reads = write_citation_reads(initial, path)
        bundle = tmp_path / 'converted'
        write_legislative_outputs([path, reads], bundle, generation_id='converted')
        restored = restore_prior(bundle, tmp_path / 'processing')
        assert pq.read_table(restored['document_citations'])['document_key'][0].as_py() in legacy.values()

        def download(key, target):
            name = key.removesuffix('.parquet')
            if name not in restored:
                return False
            shutil.copyfile(restored[name], target)
            return True

        work = tmp_path / 'reread'
        for generation in ('first-reread', 'repeat-reread'):
            result = build_held_citations(work, cursor=con, selections=[Selection(kind, (r['opinion_body_id'],)) for r in source],
                                         input_pins={'court_opinion_pdf_extractions': {'artifactDigest': fixture['publication']}},
                                         download_prior=download)
            rows = pq.read_table(result).to_pylist()
            assert len(rows) == 18 and {r['document_key'] for r in rows} <= set(legacy)
            assert len(read_checkpoints(result, NAMESPACE)) == 3
            reads = write_citation_reads(work, result)
            # The production writer previously refused this first native-key reread.
            write_legislative_outputs([result, reads], tmp_path / generation, generation_id=generation)
