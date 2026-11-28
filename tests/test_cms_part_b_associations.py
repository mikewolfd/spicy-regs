"""CMS Part B manufacturer and drug-use associations: literal fidelity, refusals, and non-inflation."""

from __future__ import annotations

from copy import deepcopy
from decimal import Decimal
import hashlib
import io
import json
import shutil
from types import SimpleNamespace
from xml.sax.saxutils import escape
import zipfile

import httpx
import pyarrow.parquet as pq
import pytest

from spicy_regs import local_publication, mcp_server
from spicy_regs.explorer_metadata import build_bundle
from spicy_regs.pipelines import cms_part_b as spending_pipeline
from spicy_regs.pipelines import cms_part_b_associations as assoc_pipeline
from spicy_regs.sources import publication
from spicy_regs.transforms import build_cms_part_b as spending_module
from spicy_regs.transforms import build_cms_part_b_associations as module
from tests.generation_fakes import Store
from tests.test_cms_part_b import Reader as SpendingReader

RESOURCES_URL = "https://data.cms.gov/data-api/v1/dataset-resources/f48ced88-541a-4c51-947b-149197e5eaf3"
WORKBOOK_URL = (
    "https://data.cms.gov/sites/default/files/2026-06/"
    "Medicare-Part-B-Drug-Dashboard-Manufacturer-Drug_use%20-%20260602.xlsx"
)
RELEASE_LOGICAL_ID = (
    "urn:spicy:artifact:spicyregs-source-native-release:"
    "b2f79f361b4f96e9aa9636e96eb7f35b8805d35c8746b86cee023333afb9a502"
)
RELEASE_DIGEST = "sha256:fe92f2976cdbe42b44af36023cc04765fd9050ab436b6248400581dec63140c8"

# Exact literal rows verified in raw visual review of the 2024 CMS Part B supporting workbook.
MS_ROWS_90371 = [
    ("90371", "Hepatitis b immune globulin for injection into muscle", "HyperHEP B", "Hepatitis B Immune Globulin", "Grifols Therape"),
    ("90371", "Hepatitis b immune globulin for injection into muscle", "HyperHEP B S-D", "Hepatitis B Immune Globulin", "Grifols Therape"),
    ("90371", "Hepatitis b immune globulin for injection into muscle", "Nabi-HB", "Hepatitis B Immune Globulin", "Adma Biologics,"),
    ("90371", "Hepatitis b immune globulin for injection into muscle", "Nabi-HB", "Hepatitis B Immune Globulin", "Biotest Pharm"),
]
MS_ROWS_90375 = [
    ("90375", "Rabies immune globulin for injection", "HyperRAB", "Rabies Immune Globulin/PF", "Grifols Therape"),
    ("90375", "Rabies immune globulin for injection", "HyperRAB S-D", "Rabies Immune Globulin/PF", "Grifols Therape"),
]
MS_ROW_Q4104 = ("Q4104", "Integra bilayer matrix wound dressing (bmwd), per square centimeter", "", "", "")

DU_ROW_90371 = (
    "90371",
    "Hepatitis b immune globulin for injection into muscle",
    "HyperHEP B*",
    "Hepatitis B Immune Globulin*",
    "This medication is used to prevent a certain serious virus infection (hepatitis B).",
)
DU_ROW_90375 = (
    "90375",
    "Rabies immune globulin for injection",
    "HyperRAB*",
    "Rabies Immune Globulin/PF*",
    "This medication is used to prevent a certain serious virus infection, rabies.",
)
DU_ROW_Q4104 = (
    "Q4104",
    "Integra bilayer matrix wound dressing (bmwd), per square centimeter",
    "Integra bilayer matrix wound dressing (bmwd), per square centimeter",
    "",
    "Drug uses not available",
)


DEFAULT_MERGED = {
    module.MANUFACTURER_SHEET: ["A2:E2"],
    module.DRUG_USE_SHEET: ["A2:E2"],
}


def _xlsx_bytes(
    sheets: dict[str, dict[int, dict[str, object]]], *, merged: dict[str, list[str]] | None = DEFAULT_MERGED
) -> bytes:
    """Build a minimal valid OOXML workbook with shared strings, optional formulas, and merged ranges."""
    shared: list[str] = []
    shared_index: dict[str, int] = {}

    def intern(text: str) -> int:
        if text not in shared_index:
            shared_index[text] = len(shared)
            shared.append(text)
        return shared_index[text]

    sheet_entries = list(sheets.items())
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        wb_sheets = []
        rels = []
        for idx, (name, rows) in enumerate(sheet_entries, start=1):
            wb_sheets.append(f'<sheet name="{escape(name)}" sheetId="{idx}" r:id="rId{idx}"/>')
            rels.append(
                f'<Relationship Id="rId{idx}" '
                'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
                f'Target="worksheets/sheet{idx}.xml"/>'
            )
            row_xml = []
            for row_num in sorted(rows):
                cells_xml = []
                for col, val in sorted(rows[row_num].items()):
                    coord = f"{col}{row_num}"
                    if val is None:
                        cells_xml.append(f'<c r="{coord}"/>')
                    elif isinstance(val, tuple) and val[0] == "formula":
                        cells_xml.append(f'<c r="{coord}" t="str"><f>{escape(str(val[1]))}</f><v>{escape(str(val[2]))}</v></c>')
                    elif isinstance(val, tuple) and val[0] == "shared_formula":
                        s_idx = intern(str(val[1]))
                        cells_xml.append(f'<c r="{coord}" t="s"><f t="shared" si="0"/><v>{s_idx}</v></c>')
                    elif isinstance(val, tuple) and val[0] == "error":
                        cells_xml.append(f'<c r="{coord}" t="e"><v>{escape(str(val[1]))}</v></c>')
                    elif isinstance(val, tuple) and val[0] == "num":
                        cells_xml.append(f'<c r="{coord}" t="n"><v>{escape(str(val[1]))}</v></c>')
                    else:
                        s_idx = intern(str(val))
                        cells_xml.append(f'<c r="{coord}" t="s"><v>{s_idx}</v></c>')
                row_xml.append(f'<row r="{row_num}">{"".join(cells_xml)}</row>')
            merge_xml = ""
            if merged and name in merged:
                refs = "".join(f'<mergeCell ref="{escape(ref)}"/>' for ref in merged[name])
                merge_xml = f'<mergeCells count="{len(merged[name])}">{refs}</mergeCells>'
            zf.writestr(
                f"xl/worksheets/sheet{idx}.xml",
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
                f'<sheetData>{"".join(row_xml)}</sheetData>{merge_xml}</worksheet>',
            )

        si_xml = "".join(f'<si><t xml:space="preserve">{escape(s)}</t></si>' for s in shared)
        zf.writestr(
            "xl/sharedStrings.xml",
            '<?xml version="1.0" encoding="UTF-8"?>'
            f'<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="{len(shared)}" uniqueCount="{len(shared)}">'
            f"{si_xml}</sst>",
        )
        zf.writestr(
            "xl/workbook.xml",
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
            'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
            f'<sheets>{"".join(wb_sheets)}</sheets></workbook>',
        )
        zf.writestr(
            "xl/_rels/workbook.xml.rels",
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            f'{"".join(rels)}</Relationships>',
        )
    return buf.getvalue()


def _default_sheet_dicts(*, include_q4104: bool = True) -> dict[str, dict[int, dict[str, object]]]:
    ms_rows = [*MS_ROWS_90371, *MS_ROWS_90375, *([MS_ROW_Q4104] if include_q4104 else [])]
    du_rows = [DU_ROW_90371, DU_ROW_90375, *([DU_ROW_Q4104] if include_q4104 else [])]

    ms_sheet: dict[int, dict[str, object]] = {
        1: {"A": "Medicare Part B Brand, Generic and Manufacturer Information"},
        2: {"A": "This table identifies the manufacturers associated with a given HCPCS.", "B": None, "C": None, "D": None, "E": None},
        3: {"A": None},
        4: dict(zip(module.COLUMNS, module.MANUFACTURER_HEADERS, strict=True)),
    }
    for offset, row in enumerate(ms_rows, start=5):
        ms_sheet[offset] = dict(zip(module.COLUMNS, row, strict=True))

    du_sheet: dict[int, dict[str, object]] = {
        1: {"A": "Medicare Part B Drug Use Information", "B": None, "C": None, "D": None, "E": None},
        2: {"A": "*** Drug use note: summary only.", "B": None, "C": None, "D": None, "E": None},
        3: {"A": "*Indicates multiple brand and/or generic names.", "B": None, "C": None, "D": None, "E": None},
        4: {"A": "**Indicates brand/generic names unavailable.", "B": None, "C": None, "D": None, "E": None},
        5: {"A": None, "B": None, "C": None, "D": None, "E": None},
        6: dict(zip(module.COLUMNS, module.DRUG_USE_HEADERS, strict=True)),
    }
    for offset, row in enumerate(du_rows, start=7):
        du_sheet[offset] = dict(zip(module.COLUMNS, row, strict=True))

    return {module.MANUFACTURER_SHEET: ms_sheet, module.DRUG_USE_SHEET: du_sheet}


def _resources_json(workbook_bytes: bytes) -> bytes:
    payload = {
        "meta": {"success": True},
        "data": [
            {
                "name": "Medicare Part B Spending by Drug 2024",
                "fileSize": 319191,
                "downloadURL": "https://data.cms.gov/sites/default/files/2026-06/part_b.csv",
            },
            {
                "name": "Medicare Part B Spending by Drug-Manufacturer and Drug Use Information 2024",
                "fileSize": len(workbook_bytes),
                "downloadURL": WORKBOOK_URL,
            },
        ],
        "links": {"self": {"href": RESOURCES_URL}},
    }
    return json.dumps(payload).encode("utf-8")


def test_exact_literal_90371_multiplicity_blank_states_and_locators(tmp_path):
    wb = _xlsx_bytes(_default_sheet_dicts(include_q4104=True))
    wb_sha = "sha256:" + hashlib.sha256(wb).hexdigest()
    res_sha = "sha256:" + hashlib.sha256(_resources_json(wb)).hexdigest()

    files = module.build_cms_part_b_associations(
        wb,
        tmp_path,
        year=2024,
        source_workbook_url=WORKBOOK_URL,
        source_resources_url=RESOURCES_URL,
        source_release_logical_id=RELEASE_LOGICAL_ID,
        source_release_digest=RELEASE_DIGEST,
        source_workbook_sha256=wb_sha,
        source_resources_sha256=res_sha,
        observed_at="2026-11-27T00:00:00Z",
    )
    assert [p.name for p in files] == [module.MANUFACTURER_OUTPUT, module.DRUG_USE_OUTPUT]
    assert pq.read_schema(files[0]).equals(module.MANUFACTURER_SCHEMA)
    assert pq.read_schema(files[1]).equals(module.DRUG_USE_SCHEMA)

    ms_rows = pq.read_table(files[0]).to_pylist()
    du_rows = pq.read_table(files[1]).to_pylist()
    assert len(ms_rows) == 7
    assert len(du_rows) == 3

    # HCPCS 90371 retains all 4 exact literal manufacturer association rows at rows 5..8.
    rows_90371 = [r for r in ms_rows if r["hcpcs_code"] == "90371"]
    assert len(rows_90371) == 4
    assert [
        (r["source_sheet"], r["source_row_number"], r["brand_name"], r["generic_name"], r["manufacturer_name"])
        for r in rows_90371
    ] == [
        ("Manufacturer Summary", 5, "HyperHEP B", "Hepatitis B Immune Globulin", "Grifols Therape"),
        ("Manufacturer Summary", 6, "HyperHEP B S-D", "Hepatitis B Immune Globulin", "Grifols Therape"),
        ("Manufacturer Summary", 7, "Nabi-HB", "Hepatitis B Immune Globulin", "Adma Biologics,"),
        ("Manufacturer Summary", 8, "Nabi-HB", "Hepatitis B Immune Globulin", "Biotest Pharm"),
    ]
    assert len({r["association_id"] for r in rows_90371}) == 4
    assert all(r["spending_allocation_status"] == "not_allocated_in_source" for r in rows_90371)
    assert all(r["source_workbook_sha256"] == wb_sha for r in rows_90371)
    assert all(r["source_release_logical_id"] == RELEASE_LOGICAL_ID for r in rows_90371)
    assert all(r["source_release_digest"] == RELEASE_DIGEST for r in rows_90371)
    assert all("total_spending_usd" not in r for r in rows_90371)

    loc5 = json.loads(rows_90371[0]["source_locator_json"])
    assert loc5["sheet"] == "Manufacturer Summary" and loc5["row"] == 5
    assert loc5["workbook_sha256"] == wb_sha and loc5["resources_sha256"] == res_sha
    assert loc5["cells"]["A"] == "A5" and loc5["cells"]["E"] == "E5"
    assert json.loads(rows_90371[2]["raw_row_json"])["Manufacturer"] == "Adma Biologics,"

    # Blank skin-substitute row (Q4104) stays empty string with explicit value_states_json.
    q4104_ms = next(r for r in ms_rows if r["hcpcs_code"] == "Q4104")
    assert (q4104_ms["brand_name"], q4104_ms["generic_name"], q4104_ms["manufacturer_name"]) == ("", "", "")
    assert json.loads(q4104_ms["value_states_json"]) == {
        "hcpcs_code": "value",
        "drug_description": "value",
        "brand_name": "empty",
        "generic_name": "empty",
        "manufacturer_name": "empty",
    }

    # Drug Use Information keeps exact 1:1 row per HCPCS, literal '*' markers, and explicit drug_use_status.
    du_90371 = next(r for r in du_rows if r["hcpcs_code"] == "90371")
    assert (du_90371["source_sheet"], du_90371["source_row_number"]) == ("Drug Use Information", 7)
    assert (du_90371["brand_name"], du_90371["generic_name"]) == ("HyperHEP B*", "Hepatitis B Immune Globulin*")
    assert du_90371["drug_use_status"] == "summary_provided"
    q4104_du = next(r for r in du_rows if r["hcpcs_code"] == "Q4104")
    assert q4104_du["drug_uses"] == "Drug uses not available"
    assert q4104_du["drug_use_status"] == "not_available"
    assert q4104_du["generic_name"] == ""
    assert json.loads(q4104_du["value_states_json"])["generic_name"] == "empty"


@pytest.mark.parametrize(
    ("mutation", "pattern"),
    [
        ("missing_hcpcs_ms", "missing its literal HCPCS key"),
        ("blank_hcpcs_du", "missing its literal HCPCS key"),
        ("blank_row_gap", "is blank"),
        ("changed_ms_header", "headers changed"),
        ("changed_du_header", "headers changed"),
        ("extra_column", "unexpected columns"),
        ("missing_sheet", "sheets changed"),
        ("extra_sheet", "sheets changed"),
        ("duplicate_du_hcpcs", "Duplicate HCPCS code"),
        ("cross_sheet_mismatch", "HCPCS keys differ across sheets"),
        ("formula_cell", "contains a formula"),
        ("merged_range", "merged range"),
        ("numeric_cell", "literal string"),
        ("bad_sha", "differs from its pinned SHA-256"),
    ],
)
def test_refuses_missing_keys_changed_headers_or_layout_and_preserves_prior(tmp_path, mutation, pattern):
    sheets = _default_sheet_dicts(include_q4104=False)
    merged = DEFAULT_MERGED
    expected_sha = None
    if mutation == "missing_hcpcs_ms":
        sheets[module.MANUFACTURER_SHEET][6]["A"] = None
    elif mutation == "blank_hcpcs_du":
        sheets[module.DRUG_USE_SHEET][7]["A"] = "   "
    elif mutation == "blank_row_gap":
        sheets[module.MANUFACTURER_SHEET][12] = deepcopy(sheets[module.MANUFACTURER_SHEET][10])
    elif mutation == "changed_ms_header":
        sheets[module.MANUFACTURER_SHEET][4]["E"] = "Manufacturer Name"
    elif mutation == "changed_du_header":
        sheets[module.DRUG_USE_SHEET][6]["E"] = "Drug Uses"
    elif mutation == "extra_column":
        sheets[module.MANUFACTURER_SHEET][4]["F"] = "Spending"
    elif mutation == "missing_sheet":
        sheets.pop(module.DRUG_USE_SHEET)
    elif mutation == "extra_sheet":
        sheets["Notes"] = {1: {"A": "extra"}}
    elif mutation == "duplicate_du_hcpcs":
        sheets[module.DRUG_USE_SHEET][9] = deepcopy(sheets[module.DRUG_USE_SHEET][7])
    elif mutation == "cross_sheet_mismatch":
        sheets[module.DRUG_USE_SHEET][9] = dict(zip(module.COLUMNS, DU_ROW_Q4104, strict=True))
    elif mutation == "formula_cell":
        sheets[module.MANUFACTURER_SHEET][5]["E"] = ("formula", 'UPPER("Grifols")', "GRIFOLS")
    elif mutation == "merged_range":
        merged = {module.MANUFACTURER_SHEET: ["A2:E2", "A5:B5"], module.DRUG_USE_SHEET: ["A2:E2"]}
    elif mutation == "numeric_cell":
        sheets[module.MANUFACTURER_SHEET][5]["A"] = ("num", 90371)
    elif mutation == "bad_sha":
        expected_sha = "sha256:" + "0" * 64

    wb = _xlsx_bytes(sheets, merged=merged)
    prior_ms = tmp_path / module.MANUFACTURER_OUTPUT
    prior_du = tmp_path / module.DRUG_USE_OUTPUT
    prior_ms.write_bytes(b"prior-ms")
    prior_du.write_bytes(b"prior-du")

    with pytest.raises(ValueError, match=pattern):
        module.build_cms_part_b_associations(
            wb,
            tmp_path,
            year=2024,
            source_workbook_url=WORKBOOK_URL,
            source_resources_url=RESOURCES_URL,
            source_release_logical_id=RELEASE_LOGICAL_ID,
            source_release_digest=RELEASE_DIGEST,
            source_workbook_sha256=expected_sha,
        )
    assert prior_ms.read_bytes() == b"prior-ms"
    assert prior_du.read_bytes() == b"prior-du"


def test_querying_associations_leaves_part_b_spending_totals_unchanged(tmp_path, monkeypatch):
    # Build spending generation (90371 = 53811.85, 90375 = 10531197.21; total = 10585009.06).
    spending_dir = tmp_path / "spending"
    spending_pipeline.build(SpendingReader(), spending_dir, year=2024)

    # Build associations tables for the same two HCPCS codes (90371 has 4 rows, 90375 has 2 rows).
    wb = _xlsx_bytes(_default_sheet_dicts(include_q4104=False))
    data_dir = tmp_path / "served"
    data_dir.mkdir()
    (data_dir / spending_module.OUTPUT).write_bytes((spending_dir / "generation" / spending_module.OUTPUT).read_bytes())
    module.build_cms_part_b_associations(
        wb,
        data_dir,
        year=2024,
        source_workbook_url=WORKBOOK_URL,
        source_resources_url=RESOURCES_URL,
        source_release_logical_id=RELEASE_LOGICAL_ID,
        source_release_digest=RELEASE_DIGEST,
    )

    monkeypatch.setattr(mcp_server, "DATA_DIR", data_dir)
    with mcp_server._build_connection() as con:
        baseline_row = con.execute(
            "SELECT SUM(total_spending_usd) FROM cms_part_b_drug_spending"
        ).fetchone()
        assert baseline_row is not None
        baseline_total = baseline_row[0]
        assert baseline_total == Decimal("10585009.060000000000")

        # Safe non-inflating query: aggregate manufacturer associations per (benefit_year, hcpcs_code)
        # before joining to spending, and join 1:1 drug_use_information.
        joined = con.execute(
            """
            WITH m AS (
                SELECT benefit_year, hcpcs_code,
                       COUNT(*) AS association_rows,
                       LIST(manufacturer_name ORDER BY source_row_number) AS manufacturers
                FROM cms_part_b_manufacturer_associations
                GROUP BY benefit_year, hcpcs_code
            )
            SELECT s.hcpcs_code, s.total_spending_usd, m.association_rows, m.manufacturers, u.drug_use_status
            FROM cms_part_b_drug_spending AS s
            LEFT JOIN m USING (benefit_year, hcpcs_code)
            LEFT JOIN cms_part_b_drug_use_information AS u USING (benefit_year, hcpcs_code)
            ORDER BY s.hcpcs_code
            """
        ).fetchall()
        assert len(joined) == 2
        assert sum(row[1] for row in joined) == baseline_total
        row_90371, row_90375 = joined
        assert row_90371[0] == "90371" and row_90371[1] == Decimal("53811.850000000000")
        assert row_90371[2] == 4
        assert row_90371[3] == ["Grifols Therape", "Grifols Therape", "Adma Biologics,", "Biotest Pharm"]
        assert row_90371[4] == "summary_provided"
        assert row_90375[0] == "90375" and row_90375[2] == 2

        # Explicitly demonstrate why associations live in a separate table: a naive flat join
        # would multiply 90371 by 4x and 90375 by 2x.
        naive_row = con.execute(
            """
            SELECT SUM(s.total_spending_usd)
            FROM cms_part_b_drug_spending AS s
            JOIN cms_part_b_manufacturer_associations AS m USING (benefit_year, hcpcs_code)
            """
        ).fetchone()
        assert naive_row is not None
        naive_inflated = naive_row[0]
        assert naive_inflated > baseline_total


def test_acquire_build_publish_and_dry_run_end_to_end(tmp_path, monkeypatch):
    monkeypatch.setattr(
        shutil,
        "disk_usage",
        lambda _: SimpleNamespace(free=local_publication.DRY_RUN_FREE_FLOOR_BYTES + 10 * 1024**3),
    )
    wb = _xlsx_bytes(_default_sheet_dicts(include_q4104=True))
    res = _resources_json(wb)

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == RESOURCES_URL:
            return httpx.Response(200, content=res, headers={"content-type": "application/json"})
        if str(request.url) == WORKBOOK_URL:
            return httpx.Response(
                200,
                content=wb,
                headers={"content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
            )
        return httpx.Response(404)

    selection = assoc_pipeline.acquire(
        RESOURCES_URL,
        year=2024,
        source_release_logical_id=RELEASE_LOGICAL_ID,
        source_release_digest=RELEASE_DIGEST,
        output_dir=tmp_path / "acq",
        transport=httpx.MockTransport(handler),
    )
    assert selection["format"] == assoc_pipeline.FORMAT
    assert selection["workbook_capture"]["byte_size"] == len(wb)

    build_dir = tmp_path / "candidate"
    artifact, evidence = assoc_pipeline.build(selection, build_dir)
    index = publication.publish_generation(
        build_dir / "generation",
        client=Store(),
        bucket="fixture",
        prior_index=publication.empty_index(),
        evidence_directories=(evidence,),
    )
    entry = index["families"][module.FAMILY]
    assert entry["artifactDigest"] == artifact.pin.artifact_digest
    assert entry.get("etlReceipts") is None
    assert set(entry["tables"]) == {module.MANUFACTURER_OUTPUT, module.DRUG_USE_OUTPUT}

    bundle = build_bundle(index)
    for table_name, id_col in (
        (module.MANUFACTURER_TABLE, "association_id"),
        (module.DRUG_USE_TABLE, "drug_use_id"),
    ):
        meta = bundle["tables"][table_name]
        assert meta["metadataStatus"] == meta["sourceStatus"] == "documented"
        assert meta["recordIdentity"]["columns"] == [id_col]
        assert meta["family"] == module.FAMILY

    # Verify publish-local-generation --dry-run accepts the candidate cleanly.
    live = tmp_path / "live-publication.v2.json"
    live.write_text(json.dumps(publication.empty_index()))
    dry_out = tmp_path / "dry-run-out"
    local_publication.main(
        [
            "--generation",
            str(build_dir / "generation"),
            "--family",
            module.FAMILY,
            "--evidence",
            str(evidence),
            "--output",
            str(dry_out),
            "--dry-run",
            "--scratch-raw",
            "--live-index",
            str(live),
        ]
    )
    receipt = json.loads((dry_out / "dry-run.json").read_text())
    assert receipt["status"] == "dry_run_accepted"
    assert receipt["family"] == module.FAMILY
    assert receipt["etlReceipts"] is None
    assert receipt["tables"][module.MANUFACTURER_OUTPUT]["rows"] == 7
    assert receipt["tables"][module.DRUG_USE_OUTPUT]["rows"] == 3


def test_refuses_shared_formula_follower_and_error_cell():
    sheets_shared = _default_sheet_dicts()
    sheets_shared[module.MANUFACTURER_SHEET][5]["E"] = ("shared_formula", "Grifols Therape")
    with pytest.raises(ValueError, match="formula"):
        module.parse_workbook(
            _xlsx_bytes(sheets_shared),
            year=2024,
            source_workbook_url=WORKBOOK_URL,
            source_resources_url=RESOURCES_URL,
            source_release_logical_id=RELEASE_LOGICAL_ID,
            source_release_digest=RELEASE_DIGEST,
        )

    sheets_err = _default_sheet_dicts()
    sheets_err[module.MANUFACTURER_SHEET][5]["E"] = ("error", "#REF!")
    with pytest.raises(ValueError, match="error cell"):
        module.parse_workbook(
            _xlsx_bytes(sheets_err),
            year=2024,
            source_workbook_url=WORKBOOK_URL,
            source_resources_url=RESOURCES_URL,
            source_release_logical_id=RELEASE_LOGICAL_ID,
            source_release_digest=RELEASE_DIGEST,
        )


def test_expected_hcpcs_codes_guard():
    wb = _xlsx_bytes(_default_sheet_dicts())
    m_rows, d_rows = module.parse_workbook(
        wb,
        year=2024,
        source_workbook_url=WORKBOOK_URL,
        source_resources_url=RESOURCES_URL,
        source_release_logical_id=RELEASE_LOGICAL_ID,
        source_release_digest=RELEASE_DIGEST,
        expected_hcpcs_codes={"90371", "90375", "Q4104"},
    )
    assert len(m_rows) == 7 and len(d_rows) == 3

    with pytest.raises(ValueError, match="HCPCS"):
        module.parse_workbook(
            wb,
            year=2024,
            source_workbook_url=WORKBOOK_URL,
            source_resources_url=RESOURCES_URL,
            source_release_logical_id=RELEASE_LOGICAL_ID,
            source_release_digest=RELEASE_DIGEST,
            expected_hcpcs_codes={"90371", "90375"},
        )


def test_pipeline_archive_root_isolation_guards_and_failure_sealing(tmp_path):
    wb = _xlsx_bytes(_default_sheet_dicts())
    res = _resources_json(wb)
    isolated_raw = tmp_path / "isolated-raw"

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == RESOURCES_URL:
            return httpx.Response(200, content=res, headers={"content-type": "application/json"})
        if str(request.url) == WORKBOOK_URL:
            return httpx.Response(
                200,
                content=wb,
                headers={"content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
            )
        return httpx.Response(404)

    selection = assoc_pipeline.acquire(
        RESOURCES_URL,
        year=2024,
        source_release_logical_id=RELEASE_LOGICAL_ID,
        source_release_digest=RELEASE_DIGEST,
        output_dir=tmp_path / "acq",
        transport=httpx.MockTransport(handler),
        archive_root=isolated_raw,
    )
    assert isolated_raw.exists()
    build_dir = tmp_path / "candidate"
    artifact, _ = assoc_pipeline.build(selection, build_dir, archive_root=isolated_raw)
    assert artifact.pin.artifact_digest.startswith("sha256:")

    # Existing output directory is refused.
    with pytest.raises(ValueError, match="fresh output directory"):
        assoc_pipeline.build(selection, build_dir, archive_root=isolated_raw)

    # Mismatched workbook byte_size or URL fails closed and seals failure outcome.
    bad_size = deepcopy(selection)
    bad_size["workbook_capture"]["byte_size"] = len(wb) + 1
    failed_dir = tmp_path / "failed-size"
    with pytest.raises(ValueError, match="byte_size"):
        assoc_pipeline.build(bad_size, failed_dir, archive_root=isolated_raw)
    outcomes = list(failed_dir.glob("source-evidence/*/run-outcome.json"))
    assert len(outcomes) == 1
    assert json.loads(outcomes[0].read_text())["outcome"] != "build-complete"

    bad_url = deepcopy(selection)
    bad_url["workbook_capture"]["requested_url"] = "https://data.cms.gov/sites/default/files/other.xlsx"
    with pytest.raises(ValueError, match="workbook URL"):
        assoc_pipeline.build(bad_url, tmp_path / "failed-url", archive_root=isolated_raw)


def test_select_workbook_resource_and_url_refusals():
    with pytest.raises(ValueError, match="successful data resource list"):
        assoc_pipeline.select_workbook_resource(b'{"meta": null, "data": []}', year=2024)
    with pytest.raises(ValueError, match="data.cms.gov"):
        assoc_pipeline._require_cms_url("https://data.cms.gov:443/data-api/v1/x", label="test")
    with pytest.raises(ValueError, match="data.cms.gov"):
        assoc_pipeline._require_cms_url("http://data.cms.gov/data-api/v1/x", label="test")
    with pytest.raises(ValueError, match="data.cms.gov"):
        assoc_pipeline._require_cms_url("https://evil.example/x", label="test")
