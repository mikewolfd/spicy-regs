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
