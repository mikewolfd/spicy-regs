"""The one-time family conversion: what it proves before it publishes, what it refuses, and how it rolls back."""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import canonical_json_bytes

from spicy_regs import native_conversion as conversion
from spicy_regs.sources import publication, r2
from tests.regulatory_publication_fakes import install

BASE = "https://test.invalid"
MAIN = "0f" * 20
WHEEL = "0.56.0"
STATE = {"checkout": MAIN, "uncommitted": [], "main": MAIN, "spicy_docs": WHEEL, "pinned_spicy_docs": WHEEL}

CFR = [
    {"granule_id": "CFR-2025-title1-vol1-sec1-1", "part_granule": "false", "package_id": "CFR-2025-title1-vol1",
     "cfr_ref": "1-1.1", "title": "1", "part": "1", "section": "1.1", "heading": "Definitions.",
     "structure_level": "section", "edition_year": "2025", "last_modified": "2025-06-01T00:00:00Z",
     "url": "https://www.govinfo.gov/app/details/CFR-2025-title1-vol1/CFR-2025-title1-vol1-sec1-1"},
    {"granule_id": "CFR-2025-title1-vol1-part2", "part_granule": "true", "package_id": "CFR-2025-title1-vol1",
     "cfr_ref": None, "title": "1", "part": "2", "section": None, "heading": "", "structure_level": "part",
     "edition_year": "2025", "last_modified": None, "url": None},
]
DOCKET = {
    "cl_docket_id": "73613631", "case_name": "County v. Department", "case_name_full": "", "court_id": "dcd",
    "court": "District Court, District of Columbia", "court_citation_string": "D.D.C.", "docket_number": "1:26-cv-02460",
    "date_filed": "2026-07-14", "date_terminated": None, "date_argued": None, "nature_of_suit": "899",
    "cause": "05:551 Administrative Procedure Act", "jurisdiction_type": "U.S. Government Defendant",
    "jury_demand": "None", "assigned_to": "A Judge", "referred_to": None, "parties_json": '["County", null, ""]',
    "attorneys_json": None, "firms_json": "[]", "pacer_case_id": "294455", "date_created": "2026-07-14T17:38:04Z",
    "absolute_url": "https://www.courtlistener.com/docket/73613631/county-v-department/", "case_type": "cv",
}


@pytest.fixture
def bucket(monkeypatch):
    """The shared in-memory bucket, also answering the public reads the conversion makes outside the index."""
    store = install(monkeypatch)

    def stored(url):
        return url.removeprefix(BASE + "/")

    monkeypatch.setattr(r2, "public_object_version", lambda url: (
        {"etag": store._etag(stored(url)), "bytes": len(store.objects[stored(url)])} if stored(url) in store.objects else None))
    monkeypatch.setattr(publication, "_bounded_get", lambda url, **_: store.objects.get(stored(url)))
    return store


def publish_old(store, monkeypatch, directory, family, tables):
    """Publish ``tables`` as they were published before receipts existed: admitted, indexed, no ``etlReceipts``."""
    from spicy_regs import etl_policy_registry
    from spicy_regs.generations import build_generation

    files = []
    for name, rows in tables.items():
        files.append(directory / "old" / f"{name}.parquet")
        files[-1].parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows, schema=pa.schema([(column, pa.string()) for column in rows[0]])), files[-1])
    with monkeypatch.context() as patch:
        patch.setattr(etl_policy_registry, "require_registered_receipts", lambda *_: None)
        build_generation(directory / "old-generation" / family, family=family, files=files,
                         expected_keys=[path.name for path in files])
        index = publication.publish_generation(directory / "old-generation" / family, client=store,
                                               bucket="spicy-regs", prior_index=publication.current_index(BASE))
    return index["families"][family]


def convert(family, work, **options):
    options.setdefault("allowed", [family])
    options.setdefault("state", lambda remote: dict(STATE))
    return conversion.convert(family, work=work, expected_main=MAIN, expected_spicy_docs=WHEEL, **options)


def refused_read(tmp_path, dataset):
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors

    with pytest.raises(ValueError, match="selected native input requires one ETL receipt generation"):
        SelectedPriors(tmp_path / "refused" / dataset, public_url=BASE).get(dataset)


def test_a_rollup_family_converts_publishes_and_reads_back(tmp_path, monkeypatch, bucket):
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors

    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    refused_read(tmp_path, "cfr_sections")

    receipt = convert("cfr-sections", tmp_path / "work", publish=True)

    entry = publication.current_index(BASE)["families"]["cfr-sections"]
    assert entry == receipt["published"]["entry"] and entry["etlReceipts"]["datasets"] == ["cfr_sections"]
    assert entry["tables"]["cfr_sections.parquet"]["rows"] == 2
    assert receipt["captured"]["entry"] == old and "etlReceipts" not in old
    assert receipt["policy_versions"] == {"cfr_sections": "regulations-native-v1"}
    assert receipt["tables"]["cfr_sections"] == {
        "retained_rows": 2, "subject_rows": 2, "rows_only_in_restored": 0, "rows_only_in_retained": 0,
        "columns_only_in_restored": [], "columns_only_in_retained": [], "type_changes": {}}
    assert receipt["read_back"]["anonymous_read_rows"] == {"cfr_sections": 2}
    assert json.loads((tmp_path / "work" / conversion.RECEIPT).read_text()) == receipt
    # The read every scheduled run makes now restores exactly the rows the builder last wrote.
    restored = pq.read_table(SelectedPriors(tmp_path / "after", public_url=BASE).get("cfr_sections")).to_pylist()
    assert sorted(restored, key=lambda row: row["granule_id"]) == sorted(CFR, key=lambda row: row["granule_id"])
    assert pq.read_schema(receipt["generation"]["directory"] + "/cfr_sections.parquet").field("part_granule").type == pa.bool_()


def test_without_publish_nothing_leaves_the_work_directory(tmp_path, monkeypatch, bucket):
    publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    written = list(bucket.writes)

    receipt = convert("cfr-sections", tmp_path / "work")

    assert receipt["published"] is None and "read_back" not in receipt
    assert receipt["tables"]["cfr_sections"]["rows_only_in_retained"] == 0
    assert bucket.writes == written
    refused_read(tmp_path, "cfr_sections")


def test_a_court_family_converts_through_the_court_writer(tmp_path, monkeypatch, bucket):
    from spicy_regs.court_receipts import prior_receipt_selection, restore_processing_input

    old = publish_old(bucket, monkeypatch, tmp_path, "courtlistener", {"court_dockets": [DOCKET]})

    receipt = convert("courtlistener", tmp_path / "work", publish=True)

    entry = publication.current_index(BASE)["families"]["courtlistener"]
    assert entry["etlReceipts"]["datasets"] == ["court_dockets"] and receipt["policy_versions"] == {"court_dockets": "courts/1"}
    assert receipt["read_back"]["anonymous_read_rows"] == {"court_dockets": 1}
    (member,) = publication.table_members(publication.current_index(BASE), "court_dockets.parquet")
    subject = tmp_path / "published" / "court_dockets.parquet"
    subject.parent.mkdir()
    publication.fetch_member(BASE, member, subject, member.path)
    assert pq.read_table(subject).to_pylist()[0]["parties"] == ["County", None, ""]
    receipts, generation = prior_receipt_selection(subject, dataset="court_dockets")
    restored = restore_processing_input(subject, tmp_path / "restored.parquet", dataset="court_dockets",
                                        schema=pa.schema([(name, pa.string()) for name in DOCKET]),
                                        receipt_path=receipts, generation_id=generation)
    assert pq.read_table(restored).to_pylist() == [DOCKET]
    # Each receipt's witness is the published object it was converted from, not a path on the converting machine.
    (witness,) = pq.read_table(receipts)["witnesses"][0].as_py()
    (retained,) = publication.table_members({"families": {"courtlistener": old}}, "court_dockets.parquet")
    assert (witness["source_id"], witness["sha256"], witness["source_uri"]) == (retained.path, retained.sha256, None)


@pytest.mark.parametrize("state, message", [
    ({"main": "1e" * 20}, "main is 1e1e1e1e1e1e, not the 0f0f0f0f0f0f this run was started for"),
    ({"checkout": "2d" * 20}, "this checkout is 2d2d2d2d2d2d, not main 0f0f0f0f0f0f"),
    ({"uncommitted": [" M src/spicy_regs/etl_receipts.py"]}, "uncommitted changes"),
    ({"spicy_docs": "0.55.0"}, "SpicyDocs is 0.55.0 installed and 0.56.0 pinned"),
    ({"spicy_docs": "0.57.0", "pinned_spicy_docs": "0.57.0"}, "not the 0.56.0 this run was started for"),
])
def test_a_moved_main_or_wheel_refuses_before_anything_is_read(tmp_path, monkeypatch, bucket, state, message):
    publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    with pytest.raises(conversion.ConversionRefused, match=message):
        convert("cfr-sections", tmp_path / "work", publish=True, state=lambda remote: STATE | state)
    assert not (tmp_path / "work").exists()


def test_main_moving_during_the_conversion_refuses_the_publication(tmp_path, monkeypatch, bucket):
    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    states = iter([dict(STATE), STATE | {"main": "1e" * 20}])
    with pytest.raises(conversion.ConversionRefused, match="Main or the pinned wheel moved"):
        convert("cfr-sections", tmp_path / "work", publish=True, state=lambda remote: next(states))
    assert publication.current_index(BASE)["families"]["cfr-sections"] == old


def test_only_a_named_old_shape_family_with_a_writer_converts(tmp_path, monkeypatch, bucket):
    publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    publish_old(bucket, monkeypatch, tmp_path, "dockets", {"dockets": [{"docket_id": "D-1", "agency_code": "A"}]})
    written = list(bucket.writes)
    for family, options, message in [
        ("cfr-sections", {"allowed": ["amendments", "treaties"]}, "not in this run's allow-list"),
        ("nominations", {}, "nominations is not a published family"),
        ("dockets", {}, "no subject/receipt rollup or court writer"),
    ]:
        with pytest.raises(conversion.ConversionRefused, match=message):
            convert(family, tmp_path / family, publish=True, **options)
    assert bucket.writes == written

    convert("cfr-sections", tmp_path / "first", publish=True)
    with pytest.raises(conversion.ConversionRefused, match="already carries ETL receipts"):
        convert("cfr-sections", tmp_path / "second", publish=True)
    with pytest.raises(conversion.ConversionRefused, match="is not empty"):
        convert("cfr-sections", tmp_path / "first", publish=True)


def test_a_row_the_writer_refuses_stops_the_conversion(tmp_path, monkeypatch, bucket):
    unnamed = CFR[0] | {"granule_id": None}
    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": [CFR[1], unnamed]})
    with pytest.raises(conversion.ConversionRefused, match="cfr-sections: Subject conversion refused"):
        convert("cfr-sections", tmp_path / "rollup", publish=True)
    courts = publish_old(bucket, monkeypatch, tmp_path, "courtlistener",
                         {"court_dockets": [DOCKET, DOCKET | {"cl_docket_id": "9", "parties_json": "not a list"}]})
    with pytest.raises(conversion.ConversionRefused, match=r"receipts hold \{'refused': 1\}"):
        convert("courtlistener", tmp_path / "court", publish=True)
    assert json.loads((tmp_path / "court" / conversion.RECEIPT).read_text())["receipt_outcomes"] == {"accepted": 1, "refused": 1}
    families = publication.current_index(BASE)["families"]
    assert (families["cfr-sections"], families["courtlistener"]) == (old, courts)


def test_rollback_restores_the_captured_entry_and_keeps_the_converted_objects(tmp_path, monkeypatch, bucket):
    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    receipt = convert("cfr-sections", tmp_path / "work", publish=True)
    converted = receipt["published"]["entry"]

    rolled = conversion.rollback(tmp_path / "work" / conversion.RECEIPT)

    assert publication.current_index(BASE)["families"]["cfr-sections"] == old == rolled["rolled_back"]["entry"]
    assert json.loads(bucket.objects[publication.INDEX_KEY])["families"]["cfr-sections"]["artifactDigest"] == old["artifactDigest"]
    assert f"{converted['prefix']}/etl_receipts.parquet" in bucket.objects
    refused_read(tmp_path, "cfr_sections")
    # Rolling back again changes nothing, and a dry receipt has nothing to roll back.
    writes = list(bucket.writes)
    conversion.rollback(tmp_path / "work" / conversion.RECEIPT)
    assert bucket.writes == writes
    convert("cfr-sections", tmp_path / "dry")
    with pytest.raises(conversion.ConversionRefused, match="records no publication"):
        conversion.rollback(tmp_path / "dry" / conversion.RECEIPT)


def test_rollback_does_not_discard_a_later_generation_unasked(tmp_path, monkeypatch, bucket):
    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    convert("cfr-sections", tmp_path / "work", publish=True)
    index = publication.current_index(BASE)
    later = index["families"]["cfr-sections"] | {"artifactDigest": "sha256:" + "c" * 64, "prefix": "generations/cfr-sections/" + "c" * 64}
    bucket.objects[publication.INDEX_V2_KEY] = canonical_json_bytes(index | {"families": index["families"] | {"cfr-sections": later}})

    with pytest.raises(publication.PublicationError, match="names another generation than the one being rolled back"):
        conversion.rollback(tmp_path / "work" / conversion.RECEIPT)
    assert publication.current_index(BASE)["families"]["cfr-sections"] == later
    conversion.rollback(tmp_path / "work" / conversion.RECEIPT, discard_newer=True)
    assert publication.current_index(BASE)["families"]["cfr-sections"] == old
    # An earlier generation whose table is gone cannot be pointed at.
    del bucket.objects[f"{old['prefix']}/cfr_sections.parquet"]
    with pytest.raises(publication.PublicationError, match="cfr_sections.parquet is no longer stored as published"):
        publication.restore_family(bucket, "spicy-regs", "cfr-sections", old, expected=None)


def test_the_command_line_names_one_family_and_its_allow_list(tmp_path, monkeypatch, bucket, capsys):
    publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    monkeypatch.setattr(conversion, "source_state", lambda remote: dict(STATE))
    common = ["--work", str(tmp_path / "work"), "--expect-main", MAIN, "--expect-spicy-docs", WHEEL]

    assert conversion.main(["cfr-sections", "--allow", "amendments,treaties", *common]) == 1
    assert "REFUSED: cfr-sections is not in this run's allow-list (amendments, treaties)" in capsys.readouterr().err
    with pytest.raises(SystemExit):
        conversion.main(["cfr-sections", "--allow", "cfr-sections", "--work", str(tmp_path / "work")])
    assert conversion.main(["cfr-sections", "--allow", "amendments,cfr-sections", *common]) == 0
    assert "not published (no --publish)" in capsys.readouterr().out
    assert "etlReceipts" not in publication.current_index(BASE)["families"]["cfr-sections"]
