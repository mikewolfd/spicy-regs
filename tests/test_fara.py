"""Real FARA source releases through generation/publication admission and the ordinary SQL consumer."""

import hashlib
import io
import json
from copy import deepcopy
from xml.sax.saxutils import escape
from zipfile import ZipFile

import httpx
import pyarrow.parquet as pq
import pytest

from spicy_regs import data_dictionary, mcp_server
from spicy_regs.pipelines import fara
from spicy_regs.source_evidence import acquisition_evidence
from spicy_regs.sources import publication
from tests.generation_fakes import Store

URL = "https://efile.fara.gov/docs/0012-Exhibit-AB-20160224-3.pdf"


def export(kind, rows):
    from spicy_docs.sources.fara import XML_ROOTS

    text = '<?xml version="1.0"?>' + f"<{XML_ROOTS[kind]}>"
    for row in rows:
        text += "<ROW>" + "".join(f"<{key}>{escape(value)}</{key}>" for key, value in row.items()) + "</ROW>"
    text += f"</{XML_ROOTS[kind]}>"
    body = io.BytesIO()
    with ZipFile(body, "w") as archive:
        archive.writestr(f"FARA_All_{kind}.xml", text.encode("cp1252"))
    return body.getvalue()


def originals(*, ambiguous=False):
    principal = {"Registration_number": "0012", "Foreign_principal": "Ilir Meta", "FP_registration_date": "02/24/2016",
                 "FP_termination_date": "06/01/2017", "Country_location_represented": "ALBANIA"}
    registrants = [{"Registration_Number": "0012", "Name": "Agent", "Registration_Date": "01/08/2016",
                    "Termination_Date": "06/01/2017"}]
    filings = [{"REGISTRATION_NUMBER": "0012", "CDATE_STAMPED": "02/24/2016", "DOCUMENT_TYPE": "Exhibit AB",
                "FOREIGN_PRINCIPAL_NAME": "Ilir Meta", "FOREIGN_PRINCIPAL_COUNTRY": "ALBANIA", "URL": URL},
               {"REGISTRATION_NUMBER": "0012", "CDATE_STAMPED": "12/01/2026", "DOCUMENT_TYPE": "Amendment",
                "URL": "https://efile.fara.gov/docs/0012-Amendment-20261201-1.pdf"}]
    return {"Registrants": export("Registrants", registrants),
            "ForeignPrincipals": export("ForeignPrincipals", [principal, principal] if ambiguous else [principal]),
            "RegistrantDocs": export("RegistrantDocs", filings)}


def retained(tmp_path, *, ambiguous=False):
    from spicy_docs.sources.fara import export_url, fara_scope

    releases = []
    with acquisition_evidence("fara", output_dir=tmp_path / "inputs") as evidence:
        for kind, raw in originals(ambiguous=ambiguous).items():
            url = export_url(kind)
            evidence.retain_bytes(raw, stage="fara-export", raw_source=True, original_location=url)
            capture = {"request_url": url, "response_sha256": "sha256:" + hashlib.sha256(raw).hexdigest(),
                       "byte_size": len(raw), "observed_at": "2026-11-13T10:00:00Z"}
            releases.append(fara_scope(capture, kind, ["0012"]))
        pdf = b"%PDF-1.4\nfixture only\n"
        evidence.retain_bytes(pdf, stage="fara-document", raw_source=True, original_location=URL)
    return {"releases": releases, "documents": {URL: {
        "request_url": URL, "response_sha256": "sha256:" + hashlib.sha256(pdf).hexdigest(),
        "byte_size": len(pdf), "observed_at": "2026-11-13T10:00:00Z"}}}


def test_native_source_generation_publication_and_ordinary_consumer(tmp_path, monkeypatch):
    selected = retained(tmp_path)
    artifact, evidence = fara.build(selected, tmp_path / "build")
    generation = tmp_path / "build/generation"
    index = publication.publish_generation(generation, client=Store(), bucket="fixture",
                                           prior_index=publication.empty_index(), evidence_directories=(evidence,))
    assert index["families"]["fara"]["artifactDigest"] == artifact.pin.artifact_digest
    monkeypatch.setattr(mcp_server, "DATA_DIR", generation)
    with mcp_server._build_connection() as connection:
        actual = connection.execute("""
            SELECT r.registration_number, p.principal_name, p.native_principal_id, f.document_url,
                   f.association_status, p.relationship_status, f.document_status
            FROM fara_registrants r JOIN fara_foreign_principals p USING (registration_number)
            JOIN fara_filings f ON f.foreign_principal_observation_id=p.source_record_id
        """).fetchall()
    assert actual == [("0012", "Ilir Meta", None, URL, "export_label_unique", "terminated", "retained")]
    rows = pq.read_table(generation / "fara_filings.parquet").to_pylist()
    assert rows[1]["association_status"] == "registrant_only" and rows[1]["date_after_capture"]
    assert rows[1]["document_status"] == "not_selected" and rows[1]["document_sha256"] is None
    assert set(fara.SCHEMAS) <= set(data_dictionary.expected_schemas())
    assert all(r["scanned_rows"] == r["selected_rows"] for r in
               pq.read_table(generation / "fara_snapshots.parquet").to_pylist())


def test_duplicate_principal_labels_never_resolve_an_entity(tmp_path):
    selected = retained(tmp_path, ambiguous=True)
    fara.build(selected, tmp_path / "build")
    rows = pq.read_table(tmp_path / "build/generation/fara_filings.parquet").to_pylist()
    assert rows[0]["association_status"] == "ambiguous_label"
    assert rows[0]["foreign_principal_observation_id"] is None
    assert len(json.loads(rows[0]["principal_candidates_json"])) == 2


def test_changed_original_or_missing_cohort_never_creates_generation(tmp_path):
    selected = retained(tmp_path)
    selected["releases"][0]["capture"]["byte_size"] += 1
    with pytest.raises(ValueError):
        fara.build(selected, tmp_path / "changed")
    assert not (tmp_path / "changed/generation").exists()
    selected = retained(tmp_path / "again")
    for scope in selected["releases"]:
        scope["registration_numbers"] = ["999"]
    selected["documents"] = {}
    with pytest.raises(ValueError, match="missing"):
        fara.build(selected, tmp_path / "missing")
    assert not (tmp_path / "missing/generation").exists()


def test_unlisted_document_or_unbound_review_refuses(tmp_path):
    selected = retained(tmp_path)
    changed = deepcopy(selected)
    capture = changed["documents"].pop(URL)
    other = "https://efile.fara.gov/docs/99-Exhibit-AB-20160224-3.pdf"
    capture["request_url"] = other
    changed["documents"][other] = capture
    with pytest.raises(ValueError, match="not listed"):
        fara.build(changed, tmp_path / "unlisted")
    changed = deepcopy(selected)
    changed["documents"][URL]["review"] = {"status": "principal_label_differs"}
    with pytest.raises(ValueError, match="visual review"):
        fara.build(changed, tmp_path / "review")


def test_capture_refusal_and_challenge_do_not_create_a_selection(tmp_path):
    from spicy_docs.transport.credentials import CredentialRefusedError
    from spicy_docs.transport.download import AcquisitionError

    for status in (403, 200):
        with pytest.raises((AcquisitionError, CredentialRefusedError)):
            fara.acquire(["0012"], tmp_path / str(status), transport=httpx.MockTransport(
                lambda request: httpx.Response(status, content=b"<html>denied</html>")))
        assert not (tmp_path / str(status) / "generation").exists()


def test_successful_acquisition_reuses_normal_archive_transport_and_build(tmp_path):
    from spicy_docs.sources.fara import export_url

    bodies = {export_url(kind): body for kind, body in originals().items()}
    bodies[URL] = b"%PDF-1.4\nfixture only\n"
    requests = []

    def serve(request):
        url = str(request.url)
        requests.append(url)
        return httpx.Response(200, content=bodies[url], headers={"content-type": "application/octet-stream"})

    selected = fara.acquire(["0012"], tmp_path / "acquire", document_urls=[URL], transport=httpx.MockTransport(serve))
    assert requests == [*bodies]
    assert len(selected["releases"]) == 3 and list(selected["documents"]) == [URL]
    artifact, evidence = fara.build(selected, tmp_path / "built")
    assert artifact.pin.artifact_digest.startswith("sha256:") and evidence.is_dir()
