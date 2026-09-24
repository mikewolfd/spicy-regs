"""Retry failed attachment-text reads without replaying raw comment ingestion."""

from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Lock

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger
from spicy_docs.transport.credentials import scrub_credential

from spicy_regs.enrich_pdf import apply_text_updates, with_text_columns
from spicy_regs.schemas import RECORD_TYPES
from spicy_regs.sources import iceberg, r2
from spicy_regs.transforms.comment_partitions import comment_partition_path
from spicy_regs.transforms.comment_text_updates import write_text_updates
from spicy_regs.transforms.derived_text_pool import DerivedTextPool, TextResult

PENDING_TEXT_FILE = "pending_comment_text.parquet"
TEXT_RULE_VERSION = "mirrulations-derived-v1"
_COORDINATES = ["agency_code", "docket_id", "comment_id"]
_COLUMNS = [*_COORDINATES, "posted_date", "text_content", "text_extraction_status"]
_SCHEMA = pa.schema([
    *((name, pa.string()) for name in [
        *_COORDINATES, "posted_date", "phase", "reason", "source_json", "rule_version", "attempted_at",
    ]),
    ("attempts", pa.int64()),
])


class PendingCommentText:
    """Persist unresolved text work only after the comment data has committed.

    This checkpoint is separate from the raw-key manifest. Source coordinates,
    failed selection (including ETags when listed), rule version and attempts
    explain each retry. Only failures enter it; valid missing/blank reads do not.
    A fresh run lists again so a changed extraction can recover from a 412.
    """

    def __init__(self, output_dir: Path) -> None:
        self.path = output_dir / PENDING_TEXT_FILE
        if not self.path.exists():
            r2.download(self.path.name, self.path)
        self.rows = {row["comment_id"]: row for row in pq.read_table(self.path).to_pylist()} if self.path.exists() else {}
        self._retry = dict(self.rows)
        self._lock = Lock()

    def observe(self, result: TextResult, *, retry: bool = False) -> None:
        row = result.record
        identity = row.get("comment_id")
        if not identity:
            return
        with self._lock:
            # Do not immediately repeat a failure in this run. Inline success
            # needs a persisted-row check before retiring a prior failure: an
            # older raw row can lose the metadata merge. This also protects a
            # crash after a chunk checkpoint but before the final text retry.
            if result.error is not None or retry:
                self._retry.pop(identity, None)
            if result.error is None:
                if retry:
                    self.rows.pop(identity, None)
                elif identity in self.rows:
                    self._retry[identity] = self.rows[identity]
                return
            previous = self.rows.get(identity, {})
            self.rows[identity] = {
                **{column: row.get(column) for column in [*_COORDINATES, "posted_date"]},
                "phase": result.error.phase,
                "reason": scrub_credential(str(result.error), ""),
                "source_json": result.error.source_json,
                "rule_version": TEXT_RULE_VERSION,
                "attempted_at": datetime.now(UTC).isoformat(),
                "attempts": previous.get("attempts", 0) + 1,
            }

    def save(self) -> None:
        # Empty state must replace the remote failures too, otherwise a fresh
        # runner resurrects already completed retries.
        with self._lock:
            temporary = self.path.with_suffix(".tmp.parquet")
            pq.write_table(pa.Table.from_pylist(list(self.rows.values()), schema=_SCHEMA), temporary, compression="zstd")
            temporary.replace(self.path)
            logger.info("Retained {} failed comment-text reads for retry", len(self.rows))

    def retry(self, pool: DerivedTextPool, output_dir: Path, agencies: list[str], *, use_iceberg: bool) -> list[Path]:
        """Retry prior failures still untouched by ingestion; preserve current metadata.

        Re-read only the existing row's coordinates and text state. Rows already
        filled by another path are resolved without fetching or overwriting them.
        Missing/moved rows remain pending until their source ingestion resolves.
        """
        rows = [row for row in self._retry.values() if row["agency_code"] in agencies]
        if not rows:
            return []
        logger.info("Retrying {} prior comment-text failures independently of raw keys", len(rows))
        pending = pl.from_dicts(rows, schema={name: pl.String for name in _COORDINATES})
        with TemporaryDirectory(dir=output_dir, prefix=".text-retry-") as staging:
            updates = Path(staging) / "updates.parquet"
            if use_iceberg:
                self._retry_catalog(pool, pending, updates)
                return []
            changed = []
            for path in sorted({_partition(output_dir, row) for row in rows}):
                if not path.exists():
                    path.parent.mkdir(parents=True, exist_ok=True)
                    r2.download(path.relative_to(output_dir).as_posix(), path)
                if not path.exists():
                    continue
                selected = with_text_columns(pl.scan_parquet(path)).select(_COLUMNS).join(
                    pending.lazy(), on=_COORDINATES, how="semi", nulls_equal=True,
                )
                candidates = selected.collect(engine="streaming")
                assert isinstance(candidates, pl.DataFrame)
                stats = self._updates(pool, candidates, updates)
                if stats["derived"]:
                    replacement = Path(staging) / "comments.parquet"
                    apply_text_updates(with_text_columns(pl.scan_parquet(path)), pl.scan_parquet(updates),
                                       "comment_id").sink_parquet(replacement, compression="zstd")
                    replacement.replace(path)
                    changed.append(path)
            return changed

    def _updates(self, pool: DerivedTextPool, candidates: pl.DataFrame, updates: Path) -> dict[str, int]:
        # No attachment gate: the retained failure proves the original row was
        # eligible. The current persisted text/status protects completed work.
        return write_text_updates(
            pool.map(candidates.unique(subset="comment_id").iter_rows(named=True), select=lambda row: (
                row["text_content"] is None and not row["text_extraction_status"]
            )), updates, observe=lambda result: self.observe(result, retry=True),
        )

    def _retry_catalog(self, pool: DerivedTextPool, pending: pl.DataFrame, updates: Path) -> None:
        record_type = RECORD_TYPES["comments"]
        con = iceberg._connect_for_table(record_type)
        try:
            con.register("_text_pending", pending.to_arrow())
            for agency in pending["agency_code"].unique().sort():
                candidates = con.execute(f"""
                    SELECT {', '.join('r.' + column for column in _COLUMNS)}
                    FROM {iceberg._qualified(record_type)} r JOIN _text_pending p
                      ON r.agency_code = p.agency_code AND r.comment_id = p.comment_id
                     AND r.docket_id IS NOT DISTINCT FROM p.docket_id
                    WHERE r.agency_code = ?
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY r.comment_id ORDER BY r.modify_date DESC NULLS LAST) = 1
                """, [agency]).pl()
                stats = self._updates(pool, candidates, updates)
                if stats["derived"]:
                    iceberg.upsert_comment_text(con, record_type, agency, updates)
        finally:
            con.close()


def _partition(output_dir: Path, row: dict) -> Path:
    posted = datetime.fromisoformat(row["posted_date"]) if row["posted_date"] else None
    docket = row["docket_id"]
    return comment_partition_path(
        output_dir / "comments", row["agency_code"], docket.strip('"') if docket is not None else None,
        posted.year if posted else None, posted.month if posted else None,
    )
