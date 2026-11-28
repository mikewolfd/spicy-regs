"""Local consumers read the captured native selection, never candidate copies."""
from argparse import Namespace
import shutil

import pyarrow.parquet as pq
import pytest

from spicy_regs import cli, mcp_server
from spicy_regs.citation_resolution import resolve_citations
from spicy_regs.congress_receipts import write_congress_dataset
from spicy_regs.etl_policy_registry import installed_policies
from spicy_regs.generations import build_generation
from spicy_regs.legislative_documents import field_registry
from spicy_regs.legislative_rollups import LegislativeTableRollup
from spicy_regs.local_data import local_selection
from spicy_regs.selected_generations import SelectedDataset, SelectedInputs, remember_generation, remember_selection
from spicy_regs.transforms.table_merge import Partitioning, merge_partitioned_table, published_members
from tests.test_citation_resolution import DIGESTS, occurrence
from tests.test_congress_receipts import shaped
from tests.test_subject_receipt_rollups import MixedRollup, mixed_builder

# The votes trio's tables publish no receipts since ss-vse3; here they stand in for the receipt route it last
# published under (tests/conftest.py).
pytestmark = pytest.mark.usefixtures("votes_receipts_unretired")


def selected_family(root, monkeypatch):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    pipeline = MixedRollup(output_dir=root)
    pipeline.build_receipts(root, mixed_builder)
    return local_selection(root)


def connection(root, monkeypatch):
    monkeypatch.setattr(mcp_server, "DATA_DIR", root)
    monkeypatch.setattr(mcp_server, "TABLES", ())
    return mcp_server._build_connection()


def test_refused_candidate_and_same_name_loose_file_cannot_replace_selected_cli_rows(tmp_path, monkeypatch, capsys):
    selected = selected_family(tmp_path, monkeypatch)
    original = pq.read_table(selected.paths("members")[0]).to_pylist()

    def bad(work, **kwargs):
        members, law, archive = mixed_builder(work, **kwargs)
        shaped(members, [{"bioguide_id": "X", "fec_ids_json": "broken"}])
        return members, law, archive

    with pytest.raises(ValueError, match="conversion refused"):
        MixedRollup(output_dir=tmp_path).build_receipts(tmp_path, bad)
    # Even a separate writer's broken convenience copy cannot redirect a consumer.
    shaped(tmp_path / "members.parquet", [{"bioguide_id": "unselected-candidate"}])
    current = local_selection(tmp_path)
    assert pq.read_table(current.paths("members")[0]).to_pylist() == original
    cli.cmd_sample(Namespace(output_dir=str(tmp_path), data_type="members", agency=None, n=1))
    output = capsys.readouterr().out
    assert "native-selected" in output and "unselected-candidate" not in output


def test_receipt_free_pin_preserves_loose_siblings_for_cli_and_mcp(tmp_path, monkeypatch, capsys):
    generation = tmp_path / "generations" / "laws-generation"
    generation.mkdir(parents=True)
    pinned = shaped(generation / "laws.parquet", [{"law_id": "119-public-1"}])
    loose = shaped(tmp_path / "members.parquet", [{"bioguide_id": "X"}])
    partition = tmp_path / "bills" / "congress=119"
    partition.mkdir(parents=True)
    part = shaped(partition / "part-000000.parquet", [{"bill_id": "119-hr-1"}])
    shaped(tmp_path / "laws.parquet", [{"law_id": "unselected"}])
    assert local_selection(tmp_path).paths("members") == (loose,)

    remember_selection(tmp_path, [SelectedDataset("laws", (pinned,), None, None)])

    selection = local_selection(tmp_path)
    assert set(selection.files) == {"laws", "members", "bills"}
    assert selection.paths("members") == (loose,)
    assert selection.paths("bills") == (part,)
    assert selection.paths("laws") == (pinned,)
    for table in ("members", "bills"):
        cli.cmd_sample(Namespace(output_dir=str(tmp_path), data_type=table, agency=None, n=1))
        assert f"Sample from {table} (1 total rows; local-unmanaged)" in capsys.readouterr().out
    with connection(tmp_path, monkeypatch) as con:
        assert con.execute("SELECT bioguide_id, bill_id, law_id FROM members, bills, laws").fetchall() == [
            ("X", "119-hr-1", "119-public-1"),
        ]
        publications = mcp_server._publication_status(con)["publication"]
        assert publications["members"] == publications["bills"] == {"status": "local_unversioned"}


def test_local_mcp_resolves_public_law_against_receipt_free_pin(tmp_path, monkeypatch):
    generation = tmp_path / "generations" / "law-generation"
    generation.mkdir(parents=True)
    law = shaped(generation / "laws.parquet", [
        {"law_id": "114-public-254", "congress": "114", "law_type": "public", "number": "254"},
    ])
    remember_selection(tmp_path, [SelectedDataset("laws", (law,), None, None)])

    with connection(tmp_path, monkeypatch) as con:
        state = mcp_server._publication_status(con)
        assert "etl_receipts" not in state["tables"]
        result = resolve_citations(con, [occurrence()], state["publication"], source_digests=DIGESTS)
        match, = result["occurrences"]
        assert match["target_status"] == "found", match
        assert match["target_snapshot"] == state["publication"]["laws"]
        assert match["target_snapshot"]["status"] == "native_selected"
        assert match["candidate_keys"] == [{"congress": "114", "law_type": "public", "number": "254"}]


def test_registering_receipt_free_generation_preserves_receipt_backed_pin(tmp_path):
    old_source = shaped(tmp_path / "old-source.parquet", [{"bioguide_id": "X"}])
    old_member, old_receipts = write_congress_dataset(
        old_source, tmp_path / "old", dataset="members", generation_id="old",
    )
    assert old_member is not None
    old_law = shaped(tmp_path / "old" / "laws.parquet", [{"law_id": "119-public-1"}])
    root = tmp_path / "output"
    member_pin = SelectedDataset("members", (old_member,), old_receipts, "old")
    remember_selection(root, [
        member_pin,
        SelectedDataset("laws", (old_law,), None, None),
    ])
    new_law = shaped(tmp_path / "laws.parquet", [{"law_id": "119-public-2"}])
    generation = root / "generations" / "new-laws"
    artifact = build_generation(generation, family="laws", files=[new_law], expected_keys=["laws.parquet"])

    remember_generation(root, generation, artifact)

    selected = local_selection(root)
    assert selected.native == {
        "members": member_pin,
        "laws": SelectedDataset("laws", (generation / "laws.parquet",), None, None),
    }

    # Reverse order: registering a receipt-backed generation preserves an existing receipt-free pin.
    reverse_root = tmp_path / "reverse"
    reverse_law_dir = reverse_root / "generations" / "laws"
    reverse_law_art = build_generation(reverse_law_dir, family="laws", files=[new_law], expected_keys=["laws.parquet"])
    remember_generation(reverse_root, reverse_law_dir, reverse_law_art)
    remember_selection(reverse_root, [member_pin])

    reverse_selected = local_selection(reverse_root)
    assert reverse_selected.native == {
        "members": member_pin,
        "laws": SelectedDataset("laws", (reverse_law_dir / "laws.parquet",), None, None),
    }


def test_same_workspace_cutover_drops_orphaned_shared_receipt_only_pin(tmp_path, monkeypatch):
    from spicy_regs import subject_catalog
    from spicy_regs.legislative_receipts import write_legislative_outputs
    from tests.test_legislative_receipts import retained, row

    monkeypatch.setattr(subject_catalog, "retired_receipt_datasets", lambda: frozenset())
    old_law = retained(
        tmp_path / "src1",
        "laws",
        [row("laws", law_id="119-public-1", congress="119", law_type="public", number="1")],
    )
    old_bundle = tmp_path / "old-laws"
    write_legislative_outputs([old_law], old_bundle, generation_id="old-laws")
    old_laws_file, old_receipts = old_bundle / "laws.parquet", old_bundle / "etl_receipts.parquet"
    root = tmp_path / "workspace"
    remember_selection(root, [
        SelectedDataset("laws", (old_laws_file,), old_receipts, "old-laws"),
        SelectedDataset("legislative_document_file_states", (), old_receipts, "old-laws"),
    ])
    monkeypatch.undo()

    new_law = shaped(tmp_path / "laws.parquet", [{"law_id": "119-public-2"}])
    generation = root / "generations" / "laws-receipt-free"
    artifact = build_generation(generation, family="laws", files=[new_law], expected_keys=["laws.parquet"])
    remember_generation(root, generation, artifact)

    # Removing the superseded receipt file must not break selection or local MCP once all owning tables moved off it.
    old_receipts.unlink()
    selected = local_selection(root)
    assert set(selected.native) == {"laws"}
    assert selected.native["laws"] == SelectedDataset("laws", (generation / "laws.parquet",), None, None)
    with connection(root, monkeypatch) as con:
        state = mcp_server._publication_status(con)
        assert "etl_receipts" not in state["tables"]
        assert con.execute("SELECT law_id FROM laws").fetchall() == [("119-public-2",)]

    # If a shared-log pin belongs to a different generation's receipt file, laws' cutover must leave it intact.
    other_root = tmp_path / "workspace-separate-log"
    monkeypatch.setattr(subject_catalog, "retired_receipt_datasets", lambda: frozenset())
    old_bundle_2 = tmp_path / "old-laws-2"
    write_legislative_outputs([old_law], old_bundle_2, generation_id="old-laws-2")
    old_laws_file_2, old_receipts_2 = old_bundle_2 / "laws.parquet", old_bundle_2 / "etl_receipts.parquet"
    other_budget = retained(
        tmp_path / "src2",
        "budget_volumes",
        [row("budget_volumes", package_id="BUDGET-2026-BUD", associated_bills_json="[]")],
    )
    other_bundle = tmp_path / "other-gen"
    write_legislative_outputs([other_budget], other_bundle, generation_id="other-gen")
    other_budget_file, other_receipts = other_bundle / "budget_volumes.parquet", other_bundle / "etl_receipts.parquet"
    remember_selection(other_root, [
        SelectedDataset("laws", (old_laws_file_2,), old_receipts_2, "old-laws-2"),
        SelectedDataset("budget_volumes", (other_budget_file,), other_receipts, "other-gen"),
        SelectedDataset("legislative_document_file_states", (), other_receipts, "other-gen"),
    ])
    monkeypatch.undo()
    other_gen = other_root / "generations" / "laws-receipt-free"
    other_art = build_generation(other_gen, family="laws", files=[new_law], expected_keys=["laws.parquet"])
    remember_generation(other_root, other_gen, other_art)
    old_receipts_2.unlink()
    other_selected = local_selection(other_root)
    assert set(other_selected.native) == {"laws", "budget_volumes", "legislative_document_file_states"}
    assert other_selected.native["legislative_document_file_states"].receipts == other_receipts

    # An empty subject table or independently readable checkpoint on the shared receipt still owns it;
    # only once that last owner moves off the receipt is the catalog-declared shared log dropped.
    partial_root = tmp_path / "workspace-empty-owner"
    monkeypatch.setattr(subject_catalog, "retired_receipt_datasets", lambda: frozenset())
    mixed_bundle = tmp_path / "mixed-old"
    write_legislative_outputs([old_law], mixed_bundle, generation_id="mixed-old")
    mixed_laws, mixed_receipts = mixed_bundle / "laws.parquet", mixed_bundle / "etl_receipts.parquet"
    remember_selection(partial_root, [
        SelectedDataset("laws", (mixed_laws,), mixed_receipts, "mixed-old"),
        SelectedDataset("members", (), mixed_receipts, "mixed-old"),
        SelectedDataset("bill_family_archives", (), mixed_receipts, "mixed-old"),
        SelectedDataset("legislative_document_file_states", (), mixed_receipts, "mixed-old"),
    ])
    monkeypatch.undo()
    partial_gen = partial_root / "generations" / "laws-receipt-free"
    partial_art = build_generation(partial_gen, family="laws", files=[new_law], expected_keys=["laws.parquet"])
    remember_generation(partial_root, partial_gen, partial_art)
    # laws moved off, but empty members + bill_family_archives still own mixed_receipts, so all stay pinned.
    after_laws = SelectedInputs(partial_root, tmp_path / "read-1", public_url="").local
    assert set(after_laws) == {"laws", "members", "bill_family_archives", "legislative_document_file_states"}
    # Moving empty members off still leaves bill_family_archives owning mixed_receipts.
    remember_selection(partial_root, [SelectedDataset("members", (new_law,), None, None)])
    after_members = SelectedInputs(partial_root, tmp_path / "read-2", public_url="").local
    assert set(after_members) == {"laws", "members", "bill_family_archives", "legislative_document_file_states"}
    # Once the final owner (bill_family_archives) moves off mixed_receipts, the shared log is pruned cleanly.
    remember_selection(partial_root, [SelectedDataset("bill_family_archives", (new_law,), None, None)])
    after_final = SelectedInputs(partial_root, tmp_path / "read-3", public_url="").local
    assert set(after_final) == {"laws", "members", "bill_family_archives"}


def test_mcp_captures_multiple_generations_and_scopes_shared_receipts(tmp_path, monkeypatch):
    old = selected_family(tmp_path, monkeypatch)
    con = connection(tmp_path, monkeypatch)
    raw = shaped(tmp_path / "next-source.parquet", [{"bioguide_id": "Y", "fec_ids_json": "[]"}])
    subject, receipt = write_congress_dataset(raw, tmp_path / "next", dataset="members", generation_id="next")
    assert subject is not None
    remember_selection(tmp_path, [SelectedDataset("members", (subject,), receipt, "next")])
    newer = connection(tmp_path, monkeypatch)
    try:
        assert con.execute("SELECT bioguide_id FROM members").fetchall() == [("X",)]
        assert newer.execute("SELECT bioguide_id FROM members").fetchall() == [("Y",)]
        assert newer.execute("SELECT law_id FROM laws").fetchall() == [("119-public-1",)]
        assert newer.execute("SELECT DISTINCT generation_id FROM etl_receipts WHERE dataset='members'").fetchall() == [("next",)]
        assert newer.execute("SELECT DISTINCT generation_id FROM etl_receipts WHERE dataset='laws'").fetchall() == [(old.native["laws"].generation_id,)]
        assert mcp_server._publication_status(newer)["publication"]["members"]["status"] == "native_selected"
    finally:
        con.close()
        newer.close()


@pytest.mark.parametrize("damage", ["subject", "receipt", "generation", "policy"])
def test_native_selection_refuses_damage_without_convenience_fallback(tmp_path, monkeypatch, damage):
    selected = selected_family(tmp_path, monkeypatch)
    member = selected.native["members"]
    if damage == "policy":
        import pyarrow as pa
        from spicy_regs.etl_receipts import RECEIPT_SCHEMA, _digest
        rows = pq.read_table(member.receipts).to_pylist()
        for row in rows:
            if row["dataset"] == "members":
                row["policy_version"] = "undeclared"
                row["receipt_id"] = _digest({k: v for k, v in row.items() if k != "receipt_id"})
        changed = tmp_path / "different-policy.parquet"
        pq.write_table(pa.Table.from_pylist(rows, schema=RECEIPT_SCHEMA), changed)
        remember_selection(tmp_path, [SelectedDataset("members", member.subjects, changed, member.generation_id)])
    elif damage == "generation":
        remember_selection(tmp_path, [SelectedDataset("members", member.subjects, member.receipts, "")])
    else:
        path = member.subjects[0] if damage == "subject" else member.receipts
        path.write_bytes(b"changed")
    with pytest.raises(ValueError, match="changed|generation|policy"):
        local_selection(tmp_path)


def test_native_multipart_selection_reads_all_members(tmp_path, monkeypatch):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    parts, receipts = [], []
    for key in ("A", "B"):
        raw = shaped(tmp_path / f"{key}.parquet", [{"bioguide_id": key, "fec_ids_json": "[]"}])
        subject, receipt = write_congress_dataset(raw, tmp_path / key, dataset="members", generation_id="parts")
        parts.append(subject)
        receipts.append(receipt)
    # Each member's source metadata receipt has identical identity: retain it once.
    import pyarrow as pa
    from spicy_regs.etl_receipts import RECEIPT_SCHEMA
    rows = {row["receipt_id"]: row for path in receipts for row in pq.read_table(path).to_pylist()}
    shared = tmp_path / "shared.parquet"
    pq.write_table(pa.Table.from_pylist(list(rows.values()), schema=RECEIPT_SCHEMA), shared)
    remember_selection(tmp_path, [SelectedDataset("members", tuple(parts), shared, "parts")])
    con = connection(tmp_path, monkeypatch)
    try:
        assert con.execute("SELECT bioguide_id FROM members ORDER BY bioguide_id").fetchall() == [("A",), ("B",)]
    finally:
        con.close()


def test_receipt_free_partitioned_legislative_rollup_carries_its_local_generation(tmp_path, monkeypatch):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    root = tmp_path / "output"
    key = "laws.parquet"
    columns = tuple(field["name"] for field in field_registry()["laws"]["fields"])
    carried = []

    class PartitionedLaws(LegislativeTableRollup):
        name = "partitioned-laws"
        output = key
        partitioned = {key: ("congress",)}

        def build(self, output_dir):
            return self.build_tables(output_dir, self.build_partitioned_laws)

        def build_partitioned_laws(self, work, *, download_members):
            prior = published_members(work, "laws", download_members)
            carried.append([path.relative_to(work / "_laws_prior").as_posix() for path in prior or ()])
            return merge_partitioned_table(
                work, name="laws", columns=columns, identity=("law_id",), version_column=None,
                rows=() if prior else (
                    {"law_id": "119-public-1", "law_type": "public", "number": "1"},
                    {"law_id": "118-public-2", "law_type": "public", "number": "2"},
                ),
                partitioning=Partitioning("congress", "law_id", lambda law_id: law_id.split("-", 1)[0]),
                prior=prior,
            )

    PartitionedLaws(output_dir=root, skip_upload=True).run()
    (generation,) = (root / "generations").iterdir()
    expected = [f"laws/congress={congress}/part-000000.parquet" for congress in (118, 119)]
    selected = local_selection(root)
    assert selected.paths("laws") == tuple(generation / path for path in expected)
    assert selected.receipts == {}

    unregistered = tmp_path / "unregistered"
    shutil.copytree(root / "laws", unregistered / "laws")
    loose = SelectedInputs(unregistered, tmp_path / "select-loose", public_url="").select("laws")
    assert loose.subjects == tuple(unregistered / path for path in expected)

    shutil.rmtree(root / "laws")
    PartitionedLaws(output_dir=root, skip_upload=True).run()
    assert carried == [[], expected]
    assert tuple((root / "generations").iterdir()) == (generation,)
    assert [pq.ParquetFile(root / path).read()["law_id"].to_pylist() for path in expected] == [
        ["118-public-2"], ["119-public-1"],
    ]
    assert not list(root.rglob("etl_receipts.parquet"))


@pytest.mark.parametrize("surface", ["current", "download.json"])
def test_native_and_download_selection_require_explicit_directory(tmp_path, monkeypatch, surface):
    selected_family(tmp_path, monkeypatch)
    (tmp_path / surface).write_text("unselected")
    with pytest.raises(RuntimeError, match="Ambiguous local selection"):
        local_selection(tmp_path)


def test_empty_native_table_has_its_declared_columns(tmp_path, monkeypatch):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    raw = shaped(tmp_path / "raw.parquet", [])
    _, receipts = write_congress_dataset(raw, tmp_path / "empty", dataset="members", generation_id="empty")
    remember_selection(tmp_path, [SelectedDataset("members", (), receipts, "empty")])
    con = connection(tmp_path, monkeypatch)
    try:
        assert con.execute("SELECT bioguide_id FROM members").fetchall() == []
    finally:
        con.close()


@pytest.mark.parametrize("damage", [None, "unknown_schema", "mixed_schema"])
def test_earlier_gao_selection_reads_original_fields_and_refuses_unknown_or_mixed_schemas(tmp_path, monkeypatch, damage):
    import duckdb
    import pyarrow as pa
    from spicy_regs.etl_receipts import ReceiptContext, receipt_policies, write_dataset
    from spicy_regs.fec_receipt_adapter import ReceiptAdapter

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    current, *historical = receipt_policies(installed_policies()["gao_reports"])
    earlier = next(policy for policy in historical if policy.policy_version == "government-sources/1")
    original = {**dict.fromkeys(earlier.subject_schema.names), "report_id": "gao-26-1", "title": "As published"}
    witness = {"source_id": "original", "source_uri": None, "sha256": "sha256:" + "0" * 64,
               "locator": None, "body_version": None}
    context = ReceiptContext("original", "report", "original-reader", [witness])
    subject, receipts = write_dataset([({**original, "raw_record": {"literal": "unchanged"}}, context)],
                                     tmp_path / "old", earlier)
    assert subject is not None
    from spicy_regs import subject_catalog
    # A new artifact declaring /2 must never admit the older subject shape.
    with monkeypatch.context() as unretired:
        retired = subject_catalog.retired_receipt_datasets() - {"gao_reports"}
        unretired.setattr(subject_catalog, "retired_receipt_datasets", lambda: retired)
        with pytest.raises(ValueError, match="Subject schema differs"):
            build_generation(tmp_path / "false-current", family="gao-reports", files=[subject],
                             expected_keys=[subject.name], receipt_path=receipts, receipt_policies=[current],
                             receipt_generation_id="original")
    paths = [subject]
    if damage == "unknown_schema":
        pq.write_table(pq.read_table(subject).append_column("unknown", pa.array(["value"])), subject)
    elif damage == "mixed_schema":
        newer, _ = write_dataset([({**dict.fromkeys(current.subject_schema.names), **original,
                                  "report_id": "gao-26-2"}, context)], tmp_path / "new", current)
        assert newer is not None
        paths.append(newer)
    remember_selection(tmp_path, [SelectedDataset("gao_reports", tuple(paths), receipts, "original")])
    if damage:
        with pytest.raises(ValueError, match="schema|Mixed subject policies"):
            local_selection(tmp_path)
        return
    selection = local_selection(tmp_path)
    selected = selection.native["gao_reports"]
    assert pq.read_table(selection.paths("gao_reports")[0]).to_pylist() == [original]
    with duckdb.connect() as con:
        adapter = ReceiptAdapter(con, {}, "", local_native={"gao_reports": {
            "subjects": selected.subjects, "receipts": selected.receipts, "generation_id": selected.generation_id}})
        assert list(adapter.selected_rows("gao_reports")) == [{**original, "raw_record": {"literal": "unchanged"}}]
