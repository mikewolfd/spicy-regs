"""A published receipt member is ordered for keyed reads, and no reader depends on that order for meaning.

The reference order below is computed in Python from the rows themselves, so no assertion here asks the sort
whether it sorted.
"""

from collections import Counter
from dataclasses import replace
import json
import random
import shutil

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import (
    RECEIPT_KEY,
    RECEIPT_ROW_GROUP_ROWS,
    RECEIPT_SCHEMA,
    DatasetPolicy,
    ReceiptContext,
    ReceiptLineage,
    _digest,
    combine_receipts,
    exact_json,
    failure_receipt,
    observation_receipt,
    receipts_sorted,
    sort_receipts,
    split_record,
    validate_receipt_bundle,
    write_dataset,
)
from spicy_regs.generations import build_generation, verify_generation
from spicy_regs.transforms.parquet_rows import write_rows

WITNESS = {"source_id": "held", "source_uri": None, "sha256": "a" * 64, "locator": None, "body_version": None}


def published_order(rows):
    """``dataset``, then ``record_id`` with NULL last, then ``receipt_id``, each by UTF-8 bytes."""
    return sorted(
        rows,
        key=lambda r: (
            r["dataset"].encode(),
            r["record_id"] is None,
            (r["record_id"] or "").encode(),
            r["receipt_id"].encode(),
        ),
    )


def rows_of(path):
    with pq.ParquetFile(path) as parquet:  # one file as written: no partition column inferred from its directory
        return parquet.read().to_pylist()


def groups_of(path):
    """Each row group's rows, read one group at a time."""
    with pq.ParquetFile(path) as parquet:
        return [parquet.read_row_group(index).to_pylist() for index in range(parquet.metadata.num_row_groups)]


def assert_published(member):
    """Every row in published order, one dataset per bounded row group, and each digest still its row's."""
    rows = rows_of(member)
    assert pq.ParquetFile(member).schema_arrow.equals(RECEIPT_SCHEMA)
    assert rows == published_order(rows)
    for group in groups_of(member):
        assert len(group) <= RECEIPT_ROW_GROUP_ROWS and len({row["dataset"] for row in group}) == 1
    assert all(_digest({k: v for k, v in row.items() if k != "receipt_id"}) == row["receipt_id"] for row in rows)
    assert receipts_sorted(member)
    return rows


def sealed(row):
    """A hand-built receipt row with the digest ``_load_receipts`` rechecks."""
    body = {name: row.get(name) for name in RECEIPT_SCHEMA.names if name != "receipt_id"}
    return {"receipt_id": _digest(body), **body}


def policy_for(dataset, *, receipt_only=False):
    schema = pa.schema([] if receipt_only else [("id", pa.string()), ("value", pa.string())])
    return DatasetPolicy(dataset, schema, () if receipt_only else ("id",), ("note",), receipt_only=receipt_only)


def context(attempt, generation="g"):
    return ReceiptContext(generation, attempt, "fixture/1", [WITNESS])


def varied_receipts():
    """Receipts of four datasets in builder order: keyed, unkeyed, failed with and without identity, odd shapes."""
    rng = random.Random(20261005)
    alpha, beta, gamma = policy_for("alpha"), policy_for("al_pha"), policy_for("gamma", receipt_only=True)
    rows = []
    for index in range(2 * RECEIPT_ROW_GROUP_ROWS + 300):
        note = "".join(rng.choice("abcdef0123456789 é𝄞") for _ in range(40))
        rows.append(split_record(alpha, {"id": f"a{index}", "value": note, "note": note}, context(f"alpha:{index}"))[1])
    for index in range(350):  # failed attempts with no subject: NULL record_id
        rows.append(failure_receipt(alpha, context(f"alpha:lost:{index}"), outcome="error", raw_fields={"note": None}))
    for index in range(5):  # a refusal that names its identity shares a record_id with nothing accepted
        rows.append(failure_receipt(alpha, context(f"alpha:refused:{index}"), outcome="refused",
                                    raw_fields={"note": "x"}, identity={"id": f"refused{index}"}))
    for index in range(7):
        rows.append(split_record(beta, {"id": str(index), "value": None, "note": ""}, context(f"beta:{index}"))[1])
    for index in range(40):
        rows.append(observation_receipt(gamma, context(f"gamma:{index}"), processing_fields={"note": str(index)}))
    # Shapes no writer emits today, which a sort must still carry unchanged.
    odd = dict(dataset="odd", policy_version="1", generation_id="g", outcome="observed", processor="fixture/1",
               processing_json=exact_json({}), diagnostic_json=exact_json({}))
    rows += [
        sealed(odd | dict(attempt_id="null-list", witnesses=None)),
        sealed(odd | dict(attempt_id="empty-list", witnesses=[])),
        sealed(odd | dict(attempt_id="null-item", witnesses=[None, dict.fromkeys(WITNESS)])),
        sealed(odd | dict(attempt_id="nul\x00 and \U0001d11e", witnesses=[WITNESS | {"locator": "a\x00b"}])),
        sealed(odd | dict(attempt_id="", witnesses=[WITNESS], record_id="", identity_json="")),
        sealed(odd | dict(attempt_id="big", witnesses=[WITNESS], processing_json=exact_json({"text": "y" * 300_000}))),
    ]
    return rows


@pytest.fixture(scope="module")
def varied(tmp_path_factory):
    """The varied receipts as two builder-ordered files, the second repeating one row, and their sorted member."""
    directory = tmp_path_factory.mktemp("varied")
    rows = varied_receipts()
    cut = len(rows) // 3
    first = write_rows(rows[:cut], directory / "first.parquet", RECEIPT_SCHEMA)
    second = write_rows([*rows[cut:], rows[0]], directory / "second.parquet", RECEIPT_SCHEMA)
    member = sort_receipts([first, second], directory / RECEIPT_KEY)
    return [*rows, rows[0]], (first, second), member


def test_sorting_changes_no_row_digest_or_schema(varied):
    rows, sources, member = varied
    assert Counter(map(exact_json, rows_of(member))) == Counter(map(exact_json, rows))
    assert_published(member)
    source, written = pq.ParquetFile(sources[0]), pq.ParquetFile(member)
    assert written.schema_arrow.equals(RECEIPT_SCHEMA)
    # As read back: the same field order, nullability, list element naming and stored Arrow schema as the
    # writers' own files.
    assert written.schema_arrow.equals(source.schema_arrow, check_metadata=True)
    assert written.schema.equals(source.schema)
    assert written.metadata.metadata == source.metadata.metadata
    assert rows != published_order(rows)  # the sources were not already in published order


def test_row_groups_hold_one_dataset_and_a_narrow_record_range(varied):
    _, _, member = varied
    groups = groups_of(member)
    alpha = [group for group in groups if group[0]["dataset"] == "alpha"]
    # 4,656 alpha rows: two full groups, then a third that ends in the unkeyed rows.
    assert [len(group) for group in alpha] == [RECEIPT_ROW_GROUP_ROWS, RECEIPT_ROW_GROUP_ROWS, 656]
    keyed = [[row["record_id"] for row in group if row["record_id"] is not None] for group in alpha]
    assert max(keyed[0]) <= min(keyed[1]) and max(keyed[1]) <= min(keyed[2])
    assert [group[0]["dataset"] for group in groups] == sorted(
        (group[0]["dataset"] for group in groups), key=str.encode
    )
    assert {group[0]["dataset"] for group in groups} == {"al_pha", "alpha", "gamma", "odd"}


def test_unkeyed_receipts_follow_every_keyed_receipt_of_their_dataset(varied):
    _, _, member = varied
    alpha = [row for row in rows_of(member) if row["dataset"] == "alpha"]
    keyed = [row["record_id"] is not None for row in alpha]
    assert keyed == sorted(keyed, reverse=True) and keyed.count(False) == 350
    unkeyed = [row["receipt_id"] for row in alpha if row["record_id"] is None]
    assert unkeyed == sorted(unkeyed, key=str.encode)
    # An empty string is a key, not an absent one: it sorts first, before the dataset's NULL keys.
    odd = [row["record_id"] for row in rows_of(member) if row["dataset"] == "odd"]
    assert odd == ["", None, None, None, None, None]


def test_sort_refuses_another_schema_and_writes_an_empty_member_from_no_rows(tmp_path):
    other = tmp_path / "other.parquet"
    pq.write_table(pa.table({"dataset": ["a"], "record_id": ["b"]}), other)
    with pytest.raises(ValueError, match="schema"):
        sort_receipts([other], tmp_path / "member.parquet")
    assert not (tmp_path / "member.parquet").exists()
    for sources in ([], [write_rows([], tmp_path / "none.parquet", RECEIPT_SCHEMA)]):
        empty = sort_receipts(sources, tmp_path / "empty.parquet")
        assert pq.ParquetFile(empty).schema_arrow.equals(RECEIPT_SCHEMA) and rows_of(empty) == []
        assert receipts_sorted(empty)
    assert not list(tmp_path.glob(".receipt-sort-*"))


def test_a_member_can_be_sorted_in_place(varied, tmp_path):
    rows, sources, _ = varied
    member = combine_receipts(sources, tmp_path / RECEIPT_KEY)
    assert not receipts_sorted(member)
    assert sort_receipts([member], member) == member
    assert Counter(map(exact_json, assert_published(member))) == Counter(map(exact_json, rows))


def write_groups(path, groups, **options):
    with pq.ParquetWriter(path, RECEIPT_SCHEMA, compression="zstd", **options) as writer:
        for group in groups:
            writer.write_table(pa.Table.from_pylist(group, schema=RECEIPT_SCHEMA))
    return path


def test_the_footer_alone_tells_a_sorted_member_from_any_other_assembly(varied, tmp_path):
    rows, _, member = varied
    assert receipts_sorted(member)
    ordered = published_order(rows)
    by_dataset = {}
    for row in ordered:
        by_dataset.setdefault(row["dataset"], []).append(row)
    groups = [
        held[start:start + RECEIPT_ROW_GROUP_ROWS]
        for held in by_dataset.values()
        for start in range(0, len(held), RECEIPT_ROW_GROUP_ROWS)
    ]
    assert receipts_sorted(write_groups(tmp_path / "same.parquet", groups))
    alpha = by_dataset["alpha"]
    assemblies = {
        "builder order": [rows[start:start + 2000] for start in range(0, len(rows), 2000)],
        "groups swapped": [groups[1], groups[0], *groups[2:]],
        "datasets descending": groups[::-1],
        "unkeyed first": [alpha[-350:], alpha[:2000], alpha[2000:-350]],
        "a group spanning two datasets": [ordered[start:start + 2000] for start in range(0, len(ordered), 2000)],
        "overlapping key ranges": [alpha[0:2000:2], alpha[1:2000:2]],
    }
    for label, assembly in assemblies.items():
        assert not receipts_sorted(write_groups(tmp_path / "other.parquet", assembly)), label
    assert not receipts_sorted(write_groups(tmp_path / "bare.parquet", groups, write_statistics=False))
    pq.write_table(pa.table({"dataset": ["a"]}), tmp_path / "foreign.parquet")
    with pytest.raises(ValueError, match="schema"):
        receipts_sorted(tmp_path / "foreign.parquet")


def test_the_sorted_check_reads_no_row_and_cannot_see_inside_a_group(varied, tmp_path):
    _, _, member = varied
    reads = []

    class Recorded:
        def __init__(self, stream):
            self.stream = stream

        def read(self, size=-1):
            reads.append((self.stream.tell(), size))
            return self.stream.read(size)

        def __getattr__(self, name):
            return getattr(self.stream, name)

    class Opened:
        def __enter__(self):
            self.stream = member.open("rb")
            return Recorded(self.stream)

        def __exit__(self, *_):
            self.stream.close()

    assert receipts_sorted(Opened)
    footer = pq.read_metadata(member).serialized_size
    size = member.stat().st_size
    assert reads and footer < size // 4
    # Parquet readers fetch a fixed tail before they know the footer's length; nothing earlier is read.
    assert all(offset >= size - max(footer + 8, 65536) for offset, _ in reads)
    # What the footer cannot show: rows shuffled inside one group keep that group's minimum and maximum.
    groups = groups_of(member)
    random.Random(1).shuffle(groups[0])
    shuffled = write_groups(tmp_path / "shuffled.parquet", groups)
    assert receipts_sorted(shuffled) and rows_of(shuffled) != published_order(rows_of(shuffled))


# Each family's own writer, admitted the way that family admits a generation. Returns the generation
# directory and the receipt file the family handed to admission.


def shared_family(tmp_path):
    first, second = policy_for("payments"), policy_for("notices", receipt_only=True)
    records = [({"id": str(i), "value": "v", "note": "n"}, context(f"p:{i}")) for i in range(12)]
    lost = [failure_receipt(first, context(f"lost:{i}"), outcome="error", raw_fields={"note": "x"}) for i in range(3)]
    subject, paid = write_dataset(records, tmp_path / "payments", first, failures=lost)
    assert subject is not None
    notices = [({"note": str(i)}, context(f"n:{i}")) for i in range(12)]
    _, noticed = write_dataset(notices, tmp_path / "notices", second)
    receipts = combine_receipts([paid, noticed], tmp_path / "combined.parquet")
    build_generation(
        tmp_path / "generation", family="payments", files=[subject], expected_keys=["payments.parquet"],
        receipt_path=receipts, receipt_policies=[first, second], receipt_generation_id="g",
    )
    return tmp_path / "generation", receipts


def legislative_family(tmp_path):
    from spicy_regs.legislative_receipts import admit_bundle, write_legislative_outputs
    from tests.test_legislative_receipts import retained, row

    laws = [row("laws", congress="119", law_type="public", number=str(i), law_id=f"119-public-{i}") for i in range(12)]
    reads = [row("committee_report_reads", package_id=f"CRPT-119hrpt{i}", outcome="complete", rule_version="1")
             for i in range(12)]
    outputs = [retained(tmp_path / "source", "laws", laws), retained(tmp_path / "source", "committee_report_reads", reads)]
    write_legislative_outputs(outputs, tmp_path / "bundle", generation_id="g")
    admit_bundle(tmp_path / "bundle", tmp_path / "generation")
    return tmp_path / "generation", tmp_path / "bundle" / RECEIPT_KEY


def congress_family(tmp_path):
    from spicy_regs.congress_receipts import CongressBuild
    from tests.test_congress_receipts import shaped

    def builder(work):
        rows = [{"bioguide_id": f"B{i:03}", "fec_ids_json": "[]", "observed_at": "then"} for i in range(12)]
        return shaped(work / "members.parquet", rows)

    build = CongressBuild("g")
    subject = build.run(builder, tmp_path / "build")
    build.admit(tmp_path / "generation", family="members", subjects=[subject])
    assert build.receipt_path is not None
    return tmp_path / "generation", build.receipt_path


def regulations_family(tmp_path):
    from spicy_regs.pipelines.regulatory_publication import finish_dataset
    from spicy_regs.schemas import DOCKET

    work = tmp_path / ".processing"
    work.mkdir()
    rows = [dict.fromkeys(DOCKET.schema) | dict(docket_id=f"D{i}", agency_code="EPA", title="T") for i in range(12)]
    schema = pa.schema([(name, pa.string()) for name in DOCKET.schema])
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), work / "dockets.parquet")
    return finish_dataset(tmp_path, "dockets", work / "dockets.parquet", publish=False).parent, None


def checkpoint_family(tmp_path):
    from spicy_regs.pipelines.regulatory_publication import finish_checkpoints

    return finish_checkpoints(tmp_path, {"failed_keys": failed_keys(12), "pending_comment_text": []}, publish=False), None


def court_family(tmp_path):
    from spicy_regs.court_receipts import build_court_generation, local_receipt_selection, write_court_rows

    rows = [{"citing_opinion_id": str(i), "cited_opinion_id": "2", "depth": "3"} for i in range(12)]
    path = write_court_rows("court_citation_map", rows, tmp_path / "court", witnesses=[WITNESS], generation_id="g")
    build_court_generation(tmp_path / "generation", family="court-citation-map", files=[path])
    return tmp_path / "generation", local_receipt_selection(path)[0]


def fec_family(tmp_path):
    from spicy_regs.transforms.fec_identity_context_fields import REGISTRY
    from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter, seal_identity_context

    with IdentityReceiptWriter(tmp_path / "identity", generation_id="g", tables=REGISTRY) as writer:
        for name, rules in reversed(REGISTRY.items()):  # the registry itself is in dataset order
            values = dict.fromkeys(rules["input_fields"])
            for key in rules["identity_fields"]:
                if key in values:
                    values[key] = "2024" if key == "cycle" else name + key
            if name == "fec_research_source_pages":
                values.update(content_status="body_extracted", text="Public document")
            writer.emit(name, values, input_witness=WITNESS, source_input={"original": None})
    seal_identity_context(tmp_path / "identity", tmp_path / "generation")
    return tmp_path / "generation", tmp_path / "identity" / RECEIPT_KEY


def government_family(tmp_path):
    from spicy_regs.native_types import described_schema
    from spicy_regs.transforms.government_receipts import generation_receipt_args, migrate_outputs
    from spicy_regs.transforms.government_source_shapes import SUBJECT_SCHEMAS
    from tests.test_government_receipts import literal

    rows = [{"report_id": f"R{i}", "version": "1", "url": f"https://source.test/r{i}"} for i in range(12)]
    path = literal(tmp_path / "government", "crs_reports", rows)
    migrate_outputs((path,), generation_id="g")
    options = generation_receipt_args((path,))
    build_generation(
        tmp_path / "generation", family="crs-reports", files=[path], expected_keys=[path.name],
        schemas={"crs_reports": described_schema(SUBJECT_SCHEMAS["crs_reports"])}, **options,
    )
    return tmp_path / "generation", options["receipt_path"]


def scorecard_family(tmp_path):
    from spicy_regs.scorecards.etl import SOURCE_NAMES, generation_options, write_family
    from tests.test_scorecard_refresh import edition, tables

    files = write_family(tmp_path / "candidate", tables(edition("2025"), members=6))
    options = generation_options(tmp_path / "candidate", SOURCE_NAMES)
    build_generation(
        tmp_path / "generation", family="scorecards", files=files, expected_keys=tuple(p.name for p in files), **options
    )
    return tmp_path / "generation", options["receipt_path"]


def attribute_family(tmp_path):
    from spicy_regs.transforms.regulations_attribute_receipts import build_attribute_generation
    from tests.test_regulations_receipts import context as source_context

    attributes = {"modifyDate": "2026-01-01", "openForComment": True, "topics": ["Air"],
                  "displayProperties": [{"name": "x", "label": "Display"}]}
    copies = [
        ({"data": {"id": f"D{i}", "type": "documents", "attributes": attributes | {"pageCount": i}}},
         source_context("g", str(i)), 1_000_000 + i)
        for i in range(12)
    ]
    build_attribute_generation(
        "document_attributes", copies, tmp_path / "generation", scan_context=source_context("g", "scan")
    )
    return tmp_path / "generation", None


FAMILIES = [
    shared_family, legislative_family, congress_family, regulations_family, checkpoint_family, court_family,
    fec_family, government_family, scorecard_family, attribute_family,
]


def failed_keys(count):
    return [
        dict(agency="EPA", record_type="dockets", key=f"raw/EPA/D{i}/docket/D{i}.json", status="unreadable",
             reason="truncated", attempted_at="2026-10-03", attempts=i)
        for i in range(count)
    ]


@pytest.mark.parametrize("family", FAMILIES, ids=lambda family: family.__name__)
def test_every_family_admits_its_receipts_in_published_order(tmp_path, family):
    generation, handed = family(tmp_path)
    member = generation / RECEIPT_KEY
    rows = assert_published(member)
    assert len(rows) >= 12
    if handed is not None:
        before = rows_of(handed)
        # The family handed admission the same rows in another order, so this test would see a missing sort.
        # (The footer check alone would not: a file this small is one row group, whose inner order it cannot see.)
        assert Counter(map(exact_json, before)) == Counter(map(exact_json, rows))
        assert before != published_order(before)
    artifact = verify_generation(generation)  # rechecks every subject and receipt join against the sorted member
    assert artifact.root["spec"]["etlReceipts"]["rows"] == len(rows)


def test_paired_validation_passes_on_the_sorted_member_and_still_refuses_a_changed_row(tmp_path):
    from spicy_regs.congress_receipts import policy as congress_policy
    from spicy_regs.court_receipts import POLICIES as COURT

    court, _ = court_family(tmp_path / "court-family")
    congress, _ = congress_family(tmp_path / "congress-family")
    pairs = [
        (court, "court_citation_map", COURT["court_citation_map"]),
        (congress, "members", congress_policy("members")),
    ]
    for generation, dataset, dataset_policy in pairs:
        spec = json.loads((generation / "artifact.json").read_text())["spec"]["etlReceipts"]
        member = generation / RECEIPT_KEY
        scoped = [row for row in rows_of(member) if row["dataset"] == dataset]
        selected = write_rows(scoped, generation.parent / "scoped.parquet", RECEIPT_SCHEMA)
        subjects = {dataset: [generation / f"{dataset}.parquet"]}
        validate_receipt_bundle(subjects, [selected], [dataset_policy], generation_id=spec["generationId"])
        accepted = next(index for index, row in enumerate(scoped) if row["outcome"] == "accepted")
        scoped[accepted] = scoped[accepted] | {"processor": "changed"}
        changed = write_rows(scoped, generation.parent / "changed.parquet", RECEIPT_SCHEMA)
        with pytest.raises(ValueError, match="digest"):
            validate_receipt_bundle(subjects, [changed], [dataset_policy], generation_id=spec["generationId"])


def test_admission_copies_subjects_byte_for_byte_and_reorders_only_receipts(tmp_path):
    generation, handed = shared_family(tmp_path)
    assert (generation / "payments.parquet").read_bytes() == (tmp_path / "payments" / "payments.parquet").read_bytes()
    assert (generation / RECEIPT_KEY).read_bytes() != handed.read_bytes()
    assert not list(generation.glob(".receipt-sort-*"))


# Readers that replay rows in the order a builder emitted them take it from each attempt's recorded position.


def test_congress_inputs_restore_in_source_order_from_a_sorted_member(tmp_path):
    from spicy_regs.congress_receipts import policy as congress_policy
    from spicy_regs.congress_receipts import restore_processing_input, write_congress_dataset
    from tests.test_congress_receipts import shaped

    members = [{"bioguide_id": f"B{i:03}", "fec_ids_json": "[]", "observed_at": "then"} for i in range(40)]
    archives = [{"congress": "119", "bill_type": "hr", "size": str(i), "modified_at": "2026-01-01", "observed_at": "t"}
                for i in range(40)]
    for dataset, source_rows in (("members", members), ("bill_family_archives", archives)):
        source = shaped(tmp_path / f"{dataset}-source.parquet", source_rows)
        subject, receipts = write_congress_dataset(source, tmp_path / dataset, dataset=dataset, generation_id="g")
        member = sort_receipts([receipts], tmp_path / f"{dataset}-member.parquet")
        emitted = [row["attempt_id"] for row in rows_of(receipts)]
        assert [row["attempt_id"] for row in rows_of(member)] != emitted
        validate_receipt_bundle(
            {dataset: [] if subject is None else [subject]}, [member], [congress_policy(dataset)], generation_id="g"
        )
        for held in (receipts, member):
            restored = restore_processing_input(
                subject, held, tmp_path / f"{dataset}-{held.stem}-restored.parquet", dataset=dataset, generation_id="g"
            )
            assert rows_of(restored) == source_rows


def test_congress_inputs_from_several_files_keep_the_sequence_of_their_subject_members(tmp_path):
    from spicy_regs.congress_receipts import restore_processing_input, write_congress_dataset
    from tests.test_congress_receipts import shaped

    def several(dataset, parts):
        subjects, shards = [], []
        for index, part in enumerate(parts):
            source = shaped(tmp_path / f"{dataset}-source-{index}.parquet", part)
            subject, receipts = write_congress_dataset(
                source, tmp_path / f"{dataset}-part-{index}", dataset=dataset, generation_id="g"
            )
            subjects.append(subject)
            shards.append(receipts)
        combined = combine_receipts(shards, tmp_path / f"{dataset}-combined.parquet")
        return subjects, combined, sort_receipts([combined], tmp_path / f"{dataset}-published.parquet")

    def restore(dataset, subjects, held, label):
        target = tmp_path / f"{dataset}-{label}.parquet"
        subject = None if subjects[0] is None else tuple(subjects)
        return restore_processing_input(subject, held, target, dataset=dataset, generation_id="g")

    # A receipt records its row's place in one source file; nothing records which file came first. The digest
    # of "Z" sorts after that of "A" or before it by chance, so neither file order nor digest order is the sequence.
    members = [[{"bioguide_id": f"{part}{i:02}", "fec_ids_json": "[]"} for i in range(8)] for part in "ZA"]
    subjects, combined, member = several("members", members)
    assert rows_of(restore("members", subjects, combined, "combined")) == members[0] + members[1]
    assert rows_of(restore("members", subjects, member, "member")) == members[0] + members[1]
    assert rows_of(restore("members", subjects[::-1], member, "turned")) == members[1] + members[0]
    # Rows that have no subject have no member to place their file by: still restored from the combined file,
    # refused from a sorted one.
    archives = [[{"congress": part, "bill_type": "hr", "size": str(i), "modified_at": "2026-01-01"} for i in range(8)]
                for part in ("119", "118")]
    subjects, combined, member = several("bill_family_archives", archives)
    assert rows_of(restore("bill_family_archives", subjects, combined, "combined")) == archives[0] + archives[1]
    with pytest.raises(ValueError, match="sequence"):
        restore("bill_family_archives", subjects, member, "refused")
    assert not (tmp_path / "bill_family_archives-refused.parquet").exists()


def test_legislative_priors_restore_each_member_in_source_order_from_a_sorted_member(tmp_path):
    from spicy_regs.legislative_receipts import restore_prior, write_legislative_outputs
    from tests.test_legislative_receipts import retained, row

    # A partitioned receipt-only table: which rows belong to which member is known only by position.
    reads = {
        f"part={part}": [row("committee_report_reads", package_id=f"CRPT-{part}-{i}", outcome="complete",
                             rule_version="1") for i in range(count)]
        for part, count in (("b", 30), ("a", 3), ("c", 11))
    }
    for part, part_rows in reads.items():
        retained(tmp_path / "source" / "committee_report_reads" / part, "committee_report_reads", part_rows)
    laws = [row("laws", congress="119", law_type="public", number=str(i), law_id=f"119-public-{i}") for i in range(30)]
    outputs = [tmp_path / "source" / "committee_report_reads", retained(tmp_path / "source", "laws", laws)]
    write_legislative_outputs(outputs, tmp_path / "bundle", generation_id="g")
    shutil.copytree(tmp_path / "bundle", tmp_path / "published")
    sort_receipts([tmp_path / "bundle" / RECEIPT_KEY], tmp_path / "published" / RECEIPT_KEY)
    assert rows_of(tmp_path / "published" / RECEIPT_KEY) != rows_of(tmp_path / "bundle" / RECEIPT_KEY)
    for held in ("bundle", "published"):
        restored = restore_prior(tmp_path / held, tmp_path / f"{held}-prior")
        assert rows_of(restored["laws"]) == laws
        for part, part_rows in reads.items():
            assert rows_of(restored["committee_report_reads"] / part / "committee_report_reads.parquet") == part_rows


def test_a_retry_checkpoint_restores_in_the_order_it_was_saved(tmp_path):
    from spicy_regs.pipelines.regulatory_publication import finish_checkpoints, restore_checkpoint

    saved = failed_keys(40)
    generation = finish_checkpoints(tmp_path, {"failed_keys": saved, "pending_comment_text": []}, publish=False)
    assert len(assert_published(generation / RECEIPT_KEY)) == 41  # forty retries and one empty marker
    assert restore_checkpoint(tmp_path, "failed_keys") == saved
    assert restore_checkpoint(tmp_path, "pending_comment_text") == []


def test_inherited_observations_do_not_depend_on_the_order_of_the_prior_file(tmp_path):
    reads = policy_for("reads", receipt_only=True)
    same = {"note": "unchanged"}
    prior = [observation_receipt(reads, replace(context(f"earlier:{i}", "g0"), witnesses=[WITNESS | {"locator": str(i)}]),
                                 processing_fields=same) for i in range(6)]
    inherited = []
    for label, rows in (("emitted", prior), ("reversed", prior[::-1])):
        path = write_rows(rows, tmp_path / f"{label}.parquet", RECEIPT_SCHEMA)
        with ReceiptLineage([path], dataset="reads") as lineage:
            inherited.append(lineage.inherit_processing(context("now", "g1"), same))
    assert inherited[0] == inherited[1]
    assert len(inherited[0].witnesses) > len(prior)

