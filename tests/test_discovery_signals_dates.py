"""Discovery counts activity within its run-time windows, excluding future dates."""

from datetime import datetime, timedelta
from hashlib import sha256

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.transforms.build_discovery_signals import build_discovery_signals
from spicy_regs.transforms.build_agency_monthly_volume import build_agency_monthly_volume


def test_temporal_boundaries_and_unusable_dates(tmp_path, monkeypatch):
    # Freeze the existing SQL clock at the connection boundary; the production
    # API and DuckDB's comparison/aggregation logic stay unchanged.
    connect = duckdb.connect

    class FixedClockConnection:
        def __init__(self):
            self.connection = connect()
            self.connection.execute("SET TimeZone='UTC'")

        def execute(self, query, parameters=None):
            return self.connection.execute(
                query.replace("CURRENT_TIMESTAMP", "TIMESTAMPTZ '2026-09-21 12:00:00+00'"), parameters
            )

        def close(self):
            self.connection.close()

    monkeypatch.setattr(duckdb, "connect", FixedClockConnection)
    now = datetime(2026, 9, 21, 12)
    recent_lower = now - timedelta(days=30)
    prior_lower = datetime(2025, 8, 21, 12)
    prior_upper = datetime(2026, 8, 21, 12)
    tick = timedelta(microseconds=1)
    rows: list[dict[str, str | None]] = []

    def add(agency: str | None, dates: list[datetime | None]) -> None:
        rows.extend({"agency_code": agency, "posted_date": value.isoformat() if value else None} for value in dates)

    middle = [datetime(2026, 6, 1)] * 24
    add("BOUNDARIES", middle + [recent_lower] * 2 + [now] * 2)
    add("BEFORE_RECENT", middle + [now] * 3 + [recent_lower - tick])
    add("AFTER_NOW", middle + [now] * 3 + [now + tick])
    add("FUTURE", middle + [now + timedelta(days=60)] * 5)
    add("PRIOR_LOWER", middle[:23] + [prior_lower] + [now] * 4)
    add("BEFORE_PRIOR", middle[:23] + [prior_lower - tick] + [now] * 4)
    add("PRIOR_UPPER", middle[:23] + [prior_upper] + [now] * 4)
    add("INVALID", middle + [now] * 3 + [None])
    rows.extend([{"agency_code": "INVALID", "posted_date": value} for value in ("bad-date", "2026-02-30")])
    add(None, middle + [now] * 5)
    pq.write_table(pa.Table.from_pylist(rows), tmp_path / "documents.parquet")

    output = build_discovery_signals(tmp_path)
    result = {row["agency_code"]: row for row in pq.read_table(output).to_pylist()}

    assert result == {
        agency: {"agency_code": agency, "recent_30d": 4, "baseline": 2.0, "ratio": 2.0}
        for agency in ("BOUNDARIES", "PRIOR_LOWER")
    }
    metadata = pq.read_schema(output).metadata
    assert metadata is not None
    assert metadata[b"spicy_regs.discovery_signals.as_of"] == b"2026-09-21 12:00:00+00"
    assert metadata[b"spicy_regs.discovery_signals.timezone"] == b"UTC"
    assert metadata[b"spicy_regs.input.documents.sha256"].decode() == (
        "sha256:" + sha256((tmp_path / "documents.parquet").read_bytes()).hexdigest()
    )
    assert metadata[b"spicy_regs.input.documents.rows"].decode() == str(len(rows))


def test_monthly_excludes_year_zero_and_records_every_omitted_parent_row(tmp_path):
    dates = ["2026-09-01", "2026-09-05", "1988-01-01", "0000-12-30T00:00:00Z", None, "bad-date", "2026-02-30"]
    source = tmp_path / "documents.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [{"agency_code": "EPA", "document_type": "Notice", "posted_date": date} for date in dates]
        ),
        source,
    )

    output = build_agency_monthly_volume(tmp_path)

    assert pq.read_table(output).to_pylist() == [
        {"agency_code": "EPA", "year": 1988, "month": 1, "document_type": "Notice", "document_count": 1},
        {"agency_code": "EPA", "year": 2026, "month": 9, "document_type": "Notice", "document_count": 2},
    ]
    metadata = pq.read_schema(output).metadata
    assert metadata is not None
    assert (
        metadata[b"spicy_regs.input.documents.sha256"].decode() == "sha256:" + sha256(source.read_bytes()).hexdigest()
    )
    assert metadata[b"spicy_regs.input.documents.rows"] == b"7"
    assert {
        key.decode().split(".")[-1]: value.decode()
        for key, value in metadata.items()
        if b"agency_monthly_volume" in key
    } == {
        "included_rows": "3",
        "omitted_rows": "4",
        "missing_dates": "1",
        "invalid_dates": "2",
        "year_zero_dates": "1",
    }
    assert pq.read_table(source)["posted_date"].to_pylist() == dates
