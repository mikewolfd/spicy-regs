"""The bulk validator accepts and refuses what the row validator does, raising the same error."""

import copy
import random
from dataclasses import dataclass, field, replace
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import etl_bulk
from spicy_regs.etl_receipts import (
    RECEIPT_SCHEMA,
    DatasetPolicy,
    ReceiptContext,
    _digest,
    exact_json,
    failure_receipt,
    observation_receipt,
    select_receipts,
    validate_receipt_bundle,
    write_dataset,
    combine_receipts,
)

SCHEMA = pa.schema([
    ("id", pa.string()), ("part", pa.string()), ("n", pa.int32()), ("kept", pa.bool_()), ("tags", pa.list_(pa.string())),
    ("people", pa.list_(pa.struct([("name", pa.string()), ("roles", pa.list_(pa.string())), ("age", pa.int64())]))),
    ("note", pa.string()),
])
WITNESS = {"source_id": "s", "source_uri": None, "sha256": "ab" * 32, "locator": "row", "body_version": None}
READS = DatasetPolicy("reads", pa.schema([]), (), ("checkpoint",), policy_version="reads/1", receipt_only=True)
#: A note per row: plain, NULL, and the control characters DuckDB spells differently, which go to the row functions.
NOTES = [None, "plain", "tab\there", "é😀", "esc\x1b[0m", "\x0b\x0e\x0f\x1a\x1f", 'q"\\']


def _policy(name: str) -> DatasetPolicy:
    return DatasetPolicy(name, SCHEMA, ("id", "part"), ("raw", "source_url"), policy_version=name + "/1",
                         nullable_identity_fields=("part",))


def _row(index: int, rng: random.Random) -> dict:
    return {
        "id": f"k{index}" + ("\x1b" if index % 5 == 4 else ""), "part": None if index % 3 == 0 else f"p{index}",
        "n": None if index % 4 == 0 else index - 3, "kept": [None, True, False][index % 3],
        "tags": None if index % 4 == 1 else ["a", 'q"', None][: index % 4],
        "people": None if index % 3 == 1 else [None, {"name": rng.choice(NOTES), "roles": ["r", None][: index % 3], "age": 2**40 + index}],
        "note": NOTES[index % len(NOTES)],
        "raw": {"i": index, "nested": [{"deep": [rng.choice(NOTES), index]}], "flag": True}, "source_url": rng.choice(NOTES),
    }


@dataclass
class Bundle:
    """One bundle as rows, so a test can damage it before the files are written."""

    policies: list[DatasetPolicy]
    subjects: dict[str, list[list[dict]]]
    receipts: list[list[dict]]
    generation_id: str | None = "g1"
    shape: dict = field(default_factory=dict)

    def receipt(self, dataset: str, outcome: str = "accepted", index: int = 0) -> dict:
        return [r for rows in self.receipts for r in rows if r["dataset"] == dataset and r["outcome"] == outcome][index]

    def write(self, directory: Path, opened: bool = False):
        directory.mkdir(parents=True)

        def member(name, rows, schema):
            table = self.shape.get(name, lambda t: t)(pa.Table.from_pylist(rows, schema=schema))
            pq.write_table(table, directory / name, row_group_size=4)
            return (lambda path=directory / name: open(path, "rb")) if opened else directory / name

        policies = {p.dataset: p for p in self.policies}
        return (
            {name: [member(f"{name}-{i}.parquet", rows, policies[name].subject_schema) for i, rows in enumerate(files)]
             for name, files in self.subjects.items()},
            [member(f"receipts-{i}.parquet", rows, RECEIPT_SCHEMA) for i, rows in enumerate(self.receipts)],
            self.policies,
        )


def seal(receipt: dict, **changes) -> dict:
    receipt.update(changes)
    receipt["receipt_id"] = _digest({key: value for key, value in receipt.items() if key != "receipt_id"})
    return receipt


def build(tmp_path: Path, seed: int = 0, rows: int = 7, generations: dict | None = None) -> Bundle:
    """A valid bundle from the row writer: two subject datasets and a receipt-only one in one receipt file."""
    rng = random.Random(seed)
    generations = generations or {}
    subjects, shards, policies = {}, [], [_policy("things"), _policy("others"), READS]
    for policy in policies[:2]:
        def context(attempt, diagnostics=None, policy=policy):
            return ReceiptContext(generations.get(policy.dataset, "g1"), f"{policy.dataset}:{attempt}", "test/1",
                                  [WITNESS, {**WITNESS, "sha256": None, "body_version": "v\x1b"}], diagnostics or {})
        records = [_row(index, rng) for index in range(rows)]
        failures = [
            failure_receipt(policy, context("rejected", {"reason": "no\x1fsubject"}), outcome="rejected",
                            raw_fields={"raw": {"i": -1}, "id": "bad"}, identity={"id": "bad", "part": None}),
            failure_receipt(policy, context("refused"), outcome="refused", raw_fields=_row(99, rng)),
            failure_receipt(policy, context("error", {"depth": [[[[1]]]]}), outcome="error", raw_fields={}),
            observation_receipt(policy, context("observed"), processing_fields={"raw": [1, None, "x"]}),
        ]
        subject, receipts = write_dataset(((row, context(i, {"ordinal": i})) for i, row in enumerate(records)),
                                          tmp_path / f"written-{seed}-{policy.dataset}", policy, failures=failures, batch_size=3)
        subjects[policy.dataset] = [pq.read_table(subject).to_pylist()]
        shards.append(receipts)
    read = ReceiptContext(generations.get("reads", "g1"), "reads:0", "test/1", [WITNESS])
    shards.append(write_dataset([({"checkpoint": [None, "", 0]}, read), ({}, replace(read, attempt_id="reads:1"))],
                                tmp_path / f"written-{seed}-reads", READS)[1])
    combined = combine_receipts(shards, tmp_path / f"combined-{seed}.parquet")
    return Bundle(policies, subjects | {"reads": []}, [pq.read_table(combined).to_pylist()])


def outcome(validate, *args, **kwargs):
    try:
        validate(*args, **kwargs)
    except Exception as error:
        return type(error).__name__, str(error)
    return None


@pytest.fixture(autouse=True)
def small_batches(monkeypatch):
    """Every file here is many batches long, so a stream that ended early would lose rows a test looks for."""
    monkeypatch.setattr(etl_bulk, "_BATCH", 3)


def agree(bundle: Bundle, tmp_path: Path, **kwargs):
    """Both validators' verdict on the bundle, read from paths and again from opened files."""
    verdicts = set()
    for opened in (False, True):
        args = bundle.write(tmp_path / f"bundle-{opened}", opened)
        row = outcome(validate_receipt_bundle, *args, generation_id=bundle.generation_id, **kwargs)
        assert outcome(etl_bulk.validate_bundle, *args, generation_id=bundle.generation_id, **kwargs) == row
        verdicts.add(row)
    assert len(verdicts) == 1
    return verdicts.pop()


def test_both_accept_what_the_row_writer_wrote(tmp_path, monkeypatch) -> None:
    bundle = build(tmp_path)
    referred = []
    held = etl_bulk._held
    monkeypatch.setattr(etl_bulk, "_held", lambda spans, numbers: held(spans, referred.append(list(numbers)) or referred[-1]))
    assert agree(bundle, tmp_path) is None
    receipts, things, others = referred[:3]
    # The escapes SQL spells differently are in receipts and subjects, and exactly those rows went to the row functions.
    assert receipts and len(receipts) < len(bundle.receipts[0])
    assert len(things) == sum("\\u001" in exact_json(row).lower() and any(
        c in exact_json(row) for c in ("\\u000b", "\\u000e", "\\u000f", "\\u001a", "\\u001b", "\\u001f")) for row in bundle.subjects["things"][0])
    assert things and others and len(things) < len(bundle.subjects["things"][0])
    bundle.generation_id = None
    assert agree(bundle, tmp_path / "unselected") is None


def test_both_accept_an_empty_generation_and_several_files(tmp_path) -> None:
    bundle = build(tmp_path)
    rows = bundle.receipts[0]
    bundle.receipts = [rows[:5], [], rows[5:]]
    bundle.subjects["things"] = [bundle.subjects["things"][0][:2], bundle.subjects["things"][0][2:]]
    assert agree(bundle, tmp_path / "split") is None
    assert agree(Bundle([READS], {"reads": []}, [[]]), tmp_path / "empty") is None


def _first(bundle, dataset, outcome="accepted"):
    return bundle.receipt(dataset, outcome)


def _subject(bundle, dataset, index=0):
    return bundle.subjects[dataset][0][index]


def _drop_receipt(bundle, dataset):
    bundle.receipts[0].remove(_first(bundle, dataset))


def _reshape_receipts(change):
    return lambda bundle, dataset: bundle.shape.update({"receipts-0.parquet": change})


def _reshape_subjects(change):
    return lambda bundle, dataset: bundle.shape.update({f"{dataset}-0.parquet": change})


#: name -> (what the row validator raises, how to damage a valid bundle so that it does). ``dataset`` is the dataset
#: damaged, so the scoped test can aim the same damage at a dataset it did or did not select.
REFUSALS = {
    "receipt_digest_flipped": ("Receipt digest differs", lambda b, d: _first(b, d).update(receipt_id="sha256:" + "0" * 64)),
    "receipt_digest_null": ("Receipt digest differs", lambda b, d: _first(b, d).update(receipt_id=None)),
    "receipt_field_changed_unsealed": ("Receipt digest differs", lambda b, d: _first(b, d).update(processor="other/2")),
    "receipt_witness_changed_unsealed": ("Receipt digest differs", lambda b, d: _first(b, d, "observed")["witnesses"][0].update(locator="x")),
    "policy_version_differs": ("no matching dataset policy", lambda b, d: seal(_first(b, d), policy_version="other/9")),
    "policy_version_null": ("no matching dataset policy", lambda b, d: seal(_first(b, d), policy_version=None)),
    "diagnostic_null": ("JSON object must be str", lambda b, d: seal(_first(b, d), diagnostic_json=None)),
    "diagnostic_not_json": ("Expecting value", lambda b, d: seal(_first(b, d), diagnostic_json="nope")),
    "diagnostic_untagged": ("unpack", lambda b, d: seal(_first(b, d), diagnostic_json='["dict",[["a",5]]]')),
    "diagnostic_unknown_tag": ("'set'", lambda b, d: seal(_first(b, d), diagnostic_json='["set",[]]')),
    "diagnostic_trailing_text": ("Extra data", lambda b, d: seal(_first(b, d), diagnostic_json='["dict",[]]]')),
    "generation_empty": ("generation, attempt and processor", lambda b, d: seal(_first(b, d), generation_id="")),
    "attempt_null": ("generation, attempt and processor", lambda b, d: seal(_first(b, d), attempt_id=None)),
    "processor_empty": ("generation, attempt and processor", lambda b, d: seal(_first(b, d), processor="")),
    "witnesses_empty": ("at least one input witness", lambda b, d: seal(_first(b, d), witnesses=[])),
    "witnesses_null": ("at least one input witness", lambda b, d: seal(_first(b, d), witnesses=None)),
    "witness_null": ("'NoneType' object has no attribute 'get'", lambda b, d: seal(_first(b, d), witnesses=[WITNESS, None])),
    "witness_without_source": ("Witness needs source identity", lambda b, d: seal(_first(b, d), witnesses=[{**WITNESS, "source_id": ""}])),
    "witness_without_digest_or_version": ("Witness needs source identity", lambda b, d: seal(_first(b, d), witnesses=[{**WITNESS, "sha256": None}])),
    "witness_digest_malformed": ("Invalid witness SHA-256", lambda b, d: seal(_first(b, d), witnesses=[{**WITNESS, "sha256": "AB" * 32}])),
    "witness_digest_empty_beside_version": ("Invalid witness SHA-256", lambda b, d: seal(_first(b, d), witnesses=[{**WITNESS, "sha256": "", "body_version": "v"}])),
    "outcome_unknown": ("Invalid receipt outcome", lambda b, d: seal(_first(b, d, "observed"), outcome="skipped")),
    "outcome_null": ("Invalid receipt outcome", lambda b, d: seal(_first(b, d, "observed"), outcome=None)),
    "accepted_without_version": ("Invalid receipt outcome", lambda b, d: seal(_first(b, d), subject_version=None)),
    "observed_with_version": ("Invalid receipt outcome", lambda b, d: seal(_first(b, d, "observed"), subject_version="sha256:" + "1" * 64)),
    "accepted_without_record_id": ("Invalid receipt outcome", lambda b, d: seal(_first(b, d), record_id="")),
    "accepted_without_identity": ("Invalid receipt outcome", lambda b, d: seal(_first(b, d), identity_json=None)),
    "processing_null": ("JSON object must be str", lambda b, d: seal(_first(b, d), processing_json=None)),
    "processing_not_json": ("Expecting", lambda b, d: seal(_first(b, d), processing_json='["dict",[')),
    "processing_not_a_mapping": ("unclassified processing fields", lambda b, d: seal(_first(b, d), processing_json='["list",[]]')),
    "processing_value_untagged": ("unpack", lambda b, d: seal(_first(b, d), processing_json='["dict",[["raw","x"]]]')),
    "processing_key_unclassified": ("unclassified processing fields", lambda b, d: seal(_first(b, d), processing_json=exact_json({"raw": 1, "extra": 2}))),
    "processing_subject_field_on_accepted": ("unclassified processing fields", lambda b, d: seal(_first(b, d), processing_json=exact_json({"note": "x"}))),
    "processing_key_unclassified_on_failure": ("unclassified processing fields", lambda b, d: seal(_first(b, d, "refused"), processing_json=exact_json({"note": "x", "extra": 2}))),
    "processing_key_escaped_unclassified": ("unclassified processing fields", lambda b, d: seal(_first(b, d), processing_json='["dict",[["r\\u0061w!",["null",null]]]]')),
    "receipt_repeated": ("Duplicate or ambiguous", lambda b, d: b.receipts[0].append(copy.deepcopy(_first(b, d, "rejected")))),
    "accepted_receipt_repeated": ("Duplicate or ambiguous", lambda b, d: b.receipts[0].append(copy.deepcopy(_first(b, d)))),
    "two_accepted_receipts_for_one_record": ("Duplicate or ambiguous", lambda b, d: b.receipts[0].append(seal(copy.deepcopy(_first(b, d)), attempt_id="again"))),
    "subject_value_changed": ("Missing, ambiguous or reused subject receipt", lambda b, d: _subject(b, d, 1).update(n=41)),
    "subject_nested_value_changed": ("Missing, ambiguous or reused subject receipt", lambda b, d: _subject(b, d, 0)["people"][1].update(age=1)),
    "subject_with_escape_changed": ("Missing, ambiguous or reused subject receipt", lambda b, d: _subject(b, d, 4).update(note="esc\x1b[1m")),
    "subject_null_became_empty": ("Missing, ambiguous or reused subject receipt", lambda b, d: _subject(b, d, 0).update(note="")),
    "subject_identity_changed": ("Missing, ambiguous or reused subject receipt", lambda b, d: _subject(b, d, 2).update(id="other")),
    "subject_identity_null": ("null record identity", lambda b, d: _subject(b, d, 2).update(id=None)),
    "subject_repeated": ("Missing, ambiguous or reused subject receipt", lambda b, d: b.subjects[d][0].append(dict(_subject(b, d, 3)))),
    "receipt_dropped": ("Missing, ambiguous or reused subject receipt", _drop_receipt),
    "receipt_version_differs": ("Missing, ambiguous or reused subject receipt", lambda b, d: seal(_first(b, d), subject_version="sha256:" + "2" * 64)),
    "receipt_identity_text_differs": ("Missing, ambiguous or reused subject receipt", lambda b, d: seal(_first(b, d), identity_json='["list",[]]')),
    "subject_dropped": ("Accepted receipt has no matching subject", lambda b, d: b.subjects[d][0].pop(1)),
    "subject_file_omitted": ("Accepted receipt has no matching subject", lambda b, d: b.subjects.update({d: []})),
    "subject_schema_differs": ("Subject schema differs from policy", _reshape_subjects(lambda t: t.rename_columns([*t.column_names[:-1], "remark"]))),
    "subject_column_order_differs": ("Subject schema differs from policy", _reshape_subjects(lambda t: t.select(t.column_names[::-1]))),
}
#: Refusals of the bundle as a whole, which no one dataset's receipts carry.
WHOLE = {
    "receipt_schema_extra_column": ("Receipt schema differs", _reshape_receipts(lambda t: t.append_column("extra", pa.nulls(len(t), pa.string())))),
    "receipt_schema_type_differs": ("Receipt schema differs", _reshape_receipts(lambda t: t.set_column(1, "dataset", t["dataset"].cast(pa.large_string())))),
    "receipt_of_unregistered_dataset": ("no matching dataset policy", lambda b, d: seal(_first(b, d, "observed"), dataset="elsewhere")),
    "receipt_dataset_null": ("no matching dataset policy", lambda b, d: seal(_first(b, d, "observed"), dataset=None)),
    "accepted_under_receipt_only_policy": ("Invalid receipt outcome", lambda b, d: seal(_first(b, "reads", "observed"), outcome="accepted", subject_version="v", record_id="r", identity_json="i")),
    "receipt_only_dataset_with_subject_table": ("Processing-only dataset cannot publish", lambda b, d: b.subjects.update(reads=[[]])),
    "policy_registered_twice": ("Bundle datasets differ", lambda b, d: b.policies.append(b.policies[0])),
    "subjects_without_policy": ("Bundle datasets differ", lambda b, d: b.subjects.update(stray=[])),
    "policy_without_subjects": ("Bundle datasets differ", lambda b, d: b.subjects.pop("reads")),
    "no_receipt_file": ("Bundle datasets differ", lambda b, d: b.receipts.clear()),
}


@pytest.mark.parametrize("name", [*REFUSALS, *WHOLE])
def test_both_refuse_alike(tmp_path, name) -> None:
    message, damage = (REFUSALS | WHOLE)[name]
    bundle = build(tmp_path)
    damage(bundle, "things")
    verdict = agree(bundle, tmp_path)
    assert verdict is not None and message in verdict[1], verdict


def test_a_later_fault_does_not_hide_an_earlier_one(tmp_path) -> None:
    bundle = build(tmp_path)
    REFUSALS["subject_dropped"][1](bundle, "others")
    REFUSALS["receipt_repeated"][1](bundle, "others")
    REFUSALS["outcome_unknown"][1](bundle, "things")
    assert "Invalid receipt outcome" in agree(bundle, tmp_path)[1]


def test_random_bundles_and_random_damage_agree(tmp_path) -> None:
    rng = random.Random(20261005)
    names = sorted(REFUSALS)
    verdicts = []
    for trial in range(60):
        bundle = build(tmp_path, seed=trial, rows=rng.randrange(1, 9))
        for _ in range(rng.choice([0, 1, 1, 2])):
            try:
                REFUSALS[rng.choice(names)][1](bundle, rng.choice(["things", "others"]))
            except (IndexError, TypeError, ValueError):
                pass  # the bundle is too small to carry this damage, or earlier damage removed its target
        rng.shuffle(bundle.receipts[0]) if rng.random() < 0.3 else None
        bundle.generation_id = rng.choice(["g1", None])
        verdicts.append(agree(bundle, tmp_path / f"trial-{trial}"))
    assert 10 < sum(verdict is None for verdict in verdicts) < 50


@pytest.mark.parametrize("name", sorted(REFUSALS))
def test_scoped_read_agrees_with_a_selected_copy(tmp_path, name) -> None:
    """A fault in the selected dataset refuses, and the same fault in another dataset of the shared file is not seen."""
    for target in ("things", "others"):
        bundle = build(tmp_path, seed=target == "things")
        REFUSALS[name][1](bundle, target)
        subjects, receipts, _ = bundle.write(tmp_path / f"shared-{target}")
        policy = bundle.policies[0]
        copied = [select_receipts(path, tmp_path / f"copy-{target}-{i}.parquet", dataset="things") for i, path in enumerate(receipts)]
        row = outcome(validate_receipt_bundle, {"things": subjects["things"]}, copied, [policy], generation_id="g1")
        bulk = outcome(etl_bulk.validate_bundle, {"things": subjects["things"]}, receipts, [policy], generation_id="g1", scoped=True)
        assert bulk == row
        assert (row is not None) == (target == "things"), (target, row)


def test_what_sql_cannot_decide_is_not_bulk_eligible(tmp_path) -> None:
    measured = DatasetPolicy("measured", pa.schema([("id", pa.string()), ("ratio", pa.float64())]), ("id",), ())
    context = ReceiptContext("g1", "a", "test/1", [WITNESS])
    subject, receipts = write_dataset([({"id": "a", "ratio": 0.5}, context)], tmp_path / "measured", measured)
    assert subject is not None
    validate_receipt_bundle({"measured": [subject]}, [receipts], [measured])
    with pytest.raises(etl_bulk.NotBulkEligible, match="double"):
        etl_bulk.validate_bundle({"measured": [subject]}, [receipts], [measured])
    cased = DatasetPolicy("cased", pa.schema([("id", pa.string()), ("pair", pa.struct([("a", pa.string()), ("A", pa.string())]))]), ("id",), ())
    cased_subject, cased_receipts = write_dataset(
        [({"id": "one", "pair": {"a": "lower", "A": "upper"}}, context)], tmp_path / "cased", cased)
    assert cased_subject is not None
    with pytest.raises(etl_bulk.NotBulkEligible, match="case"):
        etl_bulk.validate_bundle({"cased": [cased_subject]}, [cased_receipts], [cased])
    # Text the row reader cannot decode: the row validator raises on it, and the bulk one hands the bundle back.
    bundle = build(tmp_path)
    subjects, paths, policies = bundle.write(tmp_path / "undecodable")
    table = pq.read_table(paths[0])
    notes = table["processor"].combine_chunks()
    data = bytearray(notes.buffers()[2].to_pybytes())
    data[0] = 0xFF
    broken = pa.Array.from_buffers(pa.string(), len(notes), [notes.buffers()[0], notes.buffers()[1], pa.py_buffer(bytes(data))])
    pq.write_table(table.set_column(table.schema.get_field_index("processor"), "processor", broken), paths[0])
    assert outcome(validate_receipt_bundle, subjects, paths, policies)[0] == "UnicodeDecodeError"
    with pytest.raises(etl_bulk.NotBulkEligible):
        etl_bulk.validate_bundle(subjects, paths, policies)


def test_a_row_lost_on_the_way_to_sql_is_not_accepted(tmp_path, monkeypatch) -> None:
    args = build(tmp_path).write(tmp_path / "bundle")
    numbered = etl_bulk._numbered

    def lossy(parquet, start, number, wanted, read):
        reader = numbered(parquet, start, number, wanted, read)
        return pa.RecordBatchReader.from_batches(reader.schema, list(reader)[:-1])

    monkeypatch.setattr(etl_bulk, "_numbered", lossy)
    with pytest.raises(etl_bulk.NotBulkEligible, match="every row"):
        etl_bulk.validate_bundle(*args, generation_id="g1")


def test_sql_proves_a_text_decodes_only_when_the_row_decoder_accepts_it() -> None:
    """Damage canonical encodings at random: SQL may fail to prove one the decoder accepts, never the reverse."""
    import json

    from spicy_regs.etl_receipts import _unpack

    rng = random.Random(7)
    alphabet = list('[]",\\:u0019afnt{} -') + ["null", "true", '"dict"', '"list"', '"str"', '"int"', "\x01", "\x1b", "é", '["null",null]']

    def value(depth=0):
        kind = rng.randrange(8 if depth < 3 else 5)
        if kind == 0:
            return None
        if kind == 1:
            return rng.choice([True, False])
        if kind == 2:
            return rng.choice([0, -1, 7, 10**17, 10**18, 10**30])
        if kind in (3, 4):
            return "".join(rng.choice(['a', '"', "\\", "\n", "\x1b", "é", "]", "[", ","]) for _ in range(rng.randrange(4)))
        if kind == 5:
            return [value(depth + 1) for _ in range(rng.randrange(3))]
        return {rng.choice(["raw", "k", 'q"', "", "é"]): value(depth + 1) for _ in range(rng.randrange(3))}

    texts = []
    for _ in range(6000):
        text = exact_json(value())
        for _ in range(rng.choice([0, 0, 1, 1, 2])):
            at = rng.randrange(len(text) + 1)
            text = text[:at] + rng.choice(alphabet) + text[at + rng.randrange(3):]
        texts.append(text)
    con = duckdb.connect()
    con.register("t", pa.table({"i": list(range(len(texts))), "v": texts}))
    proven = con.execute(f"SELECT {etl_bulk.decodes_sql('v')}, {etl_bulk.mapping_keys_sql('v')} FROM t ORDER BY i").fetchall()
    decoded = mapped = 0
    for text, (decodes, keys) in zip(texts, proven):
        try:
            held = _unpack(json.loads(text))
        except Exception:
            assert not decodes and keys is None, text
            continue
        decoded += bool(decodes)
        if keys is not None:
            mapped += 1
            assert isinstance(held, dict) and set(keys) == set(held), text
    assert decoded > 1000 and mapped > 300


def test_missing_file_and_corrupt_pages_fall_back_to_exact_row_error(tmp_path):
    bundle = build(tmp_path)
    subjects, receipts, policies = bundle.write(tmp_path / "inputs")
    missing = tmp_path / "missing.parquet"
    for files in ([missing], [*receipts, missing]):
        assert outcome(etl_bulk.validate_bundle, subjects, files, policies, generation_id="g1") == outcome(
            validate_receipt_bundle, subjects, files, policies, generation_id="g1")
    damaged = tmp_path / "damaged.parquet"
    content = bytearray(receipts[0].read_bytes())
    content[4:12] = b"\xff" * 8
    damaged.write_bytes(content)
    def admitted(*args, **kwargs):
        try:
            return etl_bulk.validate_bundle(*args, **kwargs)
        except etl_bulk.NotBulkEligible:
            return validate_receipt_bundle(*args, **kwargs)
    assert outcome(admitted, subjects, [damaged], policies, generation_id="g1") == outcome(
        validate_receipt_bundle, subjects, [damaged], policies, generation_id="g1")


def test_scoped_multiple_policies_and_receipt_only(tmp_path):
    bundle = build(tmp_path)
    subjects, receipts, policies = bundle.write(tmp_path / "inputs")
    selected = [p for p in policies if p.dataset in {"things", "reads"}]
    copied = [select_receipts(receipts[0], tmp_path / (p.dataset + "-selected.parquet"), dataset=p.dataset)
              for p in selected]
    wanted = {p.dataset: subjects[p.dataset] for p in selected}
    with pytest.raises(etl_bulk.NotBulkEligible, match="first-error"):
        etl_bulk.validate_bundle(wanted, receipts, selected, generation_id="g1", scoped=True)
    assert outcome(validate_receipt_bundle, wanted, copied, selected, generation_id="g1") is None


@pytest.mark.parametrize("opened", [False, True])
def test_selected_member_carries_original_receipt_generations(tmp_path, opened):
    bundle = build(tmp_path, generations={"things": "old-g1", "others": "old-g2", "reads": "old-g3"})
    seal(bundle.receipt("things", "observed"), generation_id="new-g4")
    args = bundle.write(tmp_path / "selected", opened)
    assert outcome(validate_receipt_bundle, *args, generation_id="publisher-now") is None
    assert outcome(etl_bulk.validate_bundle, *args, generation_id="publisher-now") is None


def test_bulk_and_row_accept_explicit_earlier_policy(tmp_path):
    from spicy_regs.transforms.government_receipts import POLICIES
    from spicy_regs.etl_receipts import receipt_policies
    current = POLICIES["gao_decisions"]
    earlier = receipt_policies(current)[1]
    context = ReceiptContext("original", "a", "p", [WITNESS])
    receipts = write_dataset([], tmp_path / "empty", earlier,
                             failures=[observation_receipt(earlier, context, processing_fields={})])[1]
    args = ({current.dataset: []}, [receipts], [current])
    assert outcome(validate_receipt_bundle, *args, generation_id="published") is None
    assert outcome(etl_bulk.validate_bundle, *args, generation_id="published") is None


def test_reference_rows_are_streamed_in_bounded_batches(tmp_path):
    table = pa.table({"value": list(range(31))})
    member = tmp_path / "large-group.parquet"
    pq.write_table(table, member, row_group_size=31)
    held = list(etl_bulk._held([(0, 31, member)], range(31)))
    assert [n for numbers, _ in held for n in numbers] == list(range(31))
    assert max(batch.num_rows for _, batch in held) <= etl_bulk._BATCH


def test_nested_nonnullable_subject_requires_reference(tmp_path):
    dtype = pa.struct([pa.field("required", pa.string(), nullable=False)])
    selected = DatasetPolicy("nested", pa.schema([("id", pa.string()), ("nested", dtype)]), ("id",), ())
    subject, receipts = write_dataset([({"id": "one", "nested": {"required": "present"}},
                                        ReceiptContext("g", "a", "p", [WITNESS]))], tmp_path / "bundle", selected)
    assert subject is not None
    validate_receipt_bundle({selected.dataset: [subject]}, [receipts], [selected])
    with pytest.raises(etl_bulk.NotBulkEligible, match="nonnullable"):
        etl_bulk.validate_bundle({selected.dataset: [subject]}, [receipts], [selected])
