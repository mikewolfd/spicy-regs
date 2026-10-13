"""A full conversion can refuse an impossible reference index without weakening admission."""
from contextlib import contextmanager

import pytest

from spicy_regs import conversion_reads, etl_bulk, etl_receipts
from spicy_regs.congress_receipts import policy


def test_limits_apply_to_real_duckdb_and_reset_after_failure():
    with pytest.raises(RuntimeError, match="exit control"):
        with etl_bulk.bulk_limits(threads=1, memory_limit="512MiB", require_bulk=True):
            with etl_bulk.bulk_connection() as (con, _):
                assert con.execute("SELECT current_setting('threads')").fetchone()[0] == 1
                assert con.execute("SELECT current_setting('memory_limit')").fetchone()[0] == "512.0 MiB"
            raise RuntimeError("exit control")
    with etl_bulk.bulk_connection(threads=2, memory_limit="64MiB") as (con, _):
        assert con.execute("SELECT current_setting('threads')").fetchone()[0] == 2
        assert con.execute("SELECT current_setting('memory_limit')").fetchone()[0] == "64.0 MiB"
    etl_bulk.reference_fallback(etl_bulk.NotBulkEligible("ordinary fallback retained"))


@pytest.mark.parametrize("operation", ["validate", "subjects", "attempts", "family"])
def test_required_bulk_stops_before_reference_index(monkeypatch, operation):
    def unavailable(*args, **kwargs):
        raise etl_bulk.NotBulkEligible("capacity control")

    @contextmanager
    def unavailable_bundle(*args, **kwargs):
        unavailable()
        yield

    def unexpected(*args, **kwargs):
        pytest.fail("Required bulk must refuse before reference admission")

    monkeypatch.setattr(etl_bulk, "validate_bundle", unavailable)
    monkeypatch.setattr(etl_bulk, "read_with_receipts", unavailable)
    monkeypatch.setattr(etl_bulk, "read_attempts", unavailable)
    monkeypatch.setattr(etl_bulk, "_validated_bundle", unavailable_bundle)
    monkeypatch.setattr(etl_receipts, "_validate_receipt_bundle_rows", unexpected)
    monkeypatch.setattr(etl_receipts, "_read_with_receipts_rows", unexpected)
    monkeypatch.setattr(etl_receipts, "_read_attempts_rows", unexpected)
    monkeypatch.setattr(conversion_reads, "_bundle_policies", lambda *args: {})
    monkeypatch.setattr(conversion_reads.sqlite3, "connect", unexpected)
    with etl_bulk.bulk_limits(threads=1, memory_limit="512MiB", require_bulk=True):
        with pytest.raises(ValueError, match="reference fallback disabled: capacity control"):
            if operation == "validate":
                etl_receipts.validate_receipt_bundle({}, [], [])
            elif operation == "subjects":
                list(etl_receipts.read_with_receipts([], [], policy("members"), generation_id="g"))
            elif operation == "attempts":
                list(etl_receipts.read_attempts([], policy("members"), generation_id="g"))
            else:
                with conversion_reads._open_family_reader({}, [], [], generation_id="g", guard=lambda: None):
                    pytest.fail("Required bulk returned an admitted reader")


@pytest.mark.parametrize("explicit", [False, True])
def test_cli_limits_are_opt_in_for_the_complete_conversion(tmp_path, monkeypatch, explicit):
    from spicy_regs import native_conversion

    observed = []

    def convert(*args, **kwargs):
        with etl_bulk.bulk_connection(threads=2, memory_limit="64MiB") as (con, _):
            observed.append(con.execute("SELECT current_setting('threads'), current_setting('memory_limit')").fetchone())
        return {"tables": {}, "family": "fec-observations", "receipt_outcomes": {},
                "generation": {"artifactDigest": "fixture"}, "published": False}

    monkeypatch.setattr(native_conversion, "convert", convert)
    args = ["fec-observations", "--allow", "fec-observations", "--work", str(tmp_path / "work"),
            "--expect-main", "a" * 40, "--expect-spicy-docs", "0.65.0"]
    if explicit:
        args += ["--bulk-threads", "1", "--bulk-memory-limit", "512MiB", "--require-bulk"]
    assert native_conversion.main(args) == 0
    assert observed == [(1, "512.0 MiB") if explicit else (2, "64.0 MiB")]


def test_cli_empty_memory_limit_refuses_before_conversion(tmp_path, monkeypatch):
    from spicy_regs import native_conversion

    def unexpected(*args, **kwargs):
        pytest.fail("Invalid memory limit must refuse before conversion")

    monkeypatch.setattr(native_conversion, "convert", unexpected)
    assert native_conversion.main(["fec-observations", "--allow", "fec-observations",
        "--work", str(tmp_path / "work"), "--expect-main", "a" * 40,
        "--expect-spicy-docs", "0.65.0", "--bulk-memory-limit", ""]) == 2


def test_explicit_limits_bound_wide_identity_admission_and_replay(tmp_path, monkeypatch):
    import pyarrow.parquet as pq
    from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter, dataset_policy

    table = 'fec_collections'
    generation = 'wide-collection-fixture'
    output = tmp_path / 'writer'
    witness = {'source_id': 'constructed', 'sha256': 'sha256:' + 'a' * 64}
    payload = '{"body":"' + 'x' * (256 * 1024) + '"}'
    scans = []
    scan = etl_bulk._scan
    def recorded(*args, **kwargs):
        scans.append(kwargs.get('bounded_insert', False))
        return scan(*args, **kwargs)
    def no_fallback(error):
        pytest.fail('Wide explicit-limit admission must finish in SQL: ' + str(error))
    monkeypatch.setattr(etl_bulk, '_scan', recorded)
    monkeypatch.setattr(etl_bulk, 'reference_fallback', no_fallback)
    with etl_bulk.bulk_limits(threads=1, memory_limit='512MiB', require_bulk=True):
        with IdentityReceiptWriter(output, generation_id=generation, tables=[table]) as writer:
            for ordinal in range(256):
                writer.emit(table, {'collection_id': str(ordinal), 'collection_outcome_json': payload},
                            input_witness=witness)
        subject = output / (table + '.parquet')
        receipts = output / 'etl_receipts.parquet'
        policy = dataset_policy(table)
        # Full family preparation uses the same helper with retained processing.
        with conversion_reads._open_family_reader({table: [subject]}, [receipts], [policy],
                                                  generation_id=generation, guard=lambda: None) as reader:
            assert sum(1 for _ in reader.read_subjects(table)) == 256
    assert scans and all(scans)
    stored = pq.read_table(subject).to_pylist()
    assert [row['collection_id'] for row in stored] == [str(i) for i in range(256)]
    assert all(row['collection_outcome_json'] == payload for row in stored)
    # Independent complete row validation preserves the acceptance semantics.
    etl_receipts.validate_receipt_bundle({table: [subject]}, [receipts], [policy], generation_id=generation, bulk=False)
    scans.clear()
    with IdentityReceiptWriter(tmp_path / 'ordinary', generation_id='ordinary', tables=[table]) as writer:
        writer.emit(table, {'collection_id': 'ordinary'}, input_witness=witness)
    assert scans and not any(scans), 'Explicit-limit selection must reset on context exit'


def test_bounded_admission_refuses_duplicate_join_across_insert_slices(tmp_path, monkeypatch):
    from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter
    monkeypatch.setattr(etl_bulk, '_INSERT_BYTES', 32)
    output = tmp_path / 'duplicate'
    with etl_bulk.bulk_limits(threads=1, memory_limit='512MiB', require_bulk=True):
        with pytest.raises(ValueError, match='Duplicate or ambiguous receipt join'):
            with IdentityReceiptWriter(output, generation_id='duplicate', tables=['fec_collections']) as writer:
                for key in ('same', 'other', 'same'):
                    writer.emit('fec_collections', {'collection_id': key},
                        input_witness={'source_id': 'fixture', 'sha256': 'sha256:' + 'a' * 64})
    assert not output.exists()
