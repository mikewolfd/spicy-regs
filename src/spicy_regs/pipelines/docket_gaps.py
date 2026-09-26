"""Fill dockets the Mirrulations mirror never captured, from the Regulations.gov API.

Documents and comments can name a docket whose record the mirror lacks: 165 on
2026-09-26, although every one of them had mirrored documents, comments or
attachments (receipt ``join-map-2026-09-26/docket-orphans.json``). This step
reads the ETL's working copies, asks the publisher once for each missing
docket, and merges what it serves into the catalog through the ETL's own docket
extraction. The mirror retains the same JSON:API object per docket, so the rows
are identical in shape.

Every other answer is recorded in ``docket_gap_outcomes.parquet`` and not asked
again for ``RETRY_AFTER``: 404 means the publisher does not publish that docket,
and 400 "Invalid ID" means the id is outside its grammar (legacy ``-RULEMAKING``
suffixes). A transport failure is not an answer: nothing is recorded, the
served dockets are still merged, and the step fails so the run shows it.
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated, Any

import duckdb
import polars as pl
from cyclopts import App, Parameter
from dotenv import load_dotenv
from loguru import logger

from spicy_regs.schemas.regulations import RECORD_TYPES
from spicy_regs.sources import iceberg, r2
from spicy_regs.transforms import ExtractRecords, write_staging

OUTCOMES = "docket_gap_outcomes.parquet"
OUTCOME_SCHEMA = {"docket_id": pl.Utf8, "outcome": pl.Utf8, "http_status": pl.Int64,
                  "observed_at": pl.Utf8, "body_sha256": pl.Utf8}
RETRY_AFTER = timedelta(days=30)
DEFAULT_MAX_DOCKETS = 500
DOCKETS = RECORD_TYPES["dockets"]


@dataclass(frozen=True)
class Answer:
    """What the publisher said about one docket id."""

    docket_id: str
    outcome: str  # "served" | "absent" | "invalid"
    http_status: int
    body: bytes
    payload: Mapping[str, Any] | None = None


def missing_dockets(documents: Path, comments_index: Path, dockets: Path) -> list[str]:
    """Docket ids that documents or comments name and the dockets table lacks, in id order."""
    con = duckdb.connect()
    rows = con.execute(
        """
        SELECT DISTINCT docket_id FROM (
            SELECT docket_id FROM read_parquet(?) UNION ALL SELECT docket_id FROM read_parquet(?)
        ) WHERE docket_id IS NOT NULL
          AND docket_id NOT IN (SELECT docket_id FROM read_parquet(?) WHERE docket_id IS NOT NULL)
        ORDER BY 1
        """,
        [str(documents), str(comments_index), str(dockets)],
    ).fetchall()
    return [row[0] for row in rows]


def due(missing: Iterable[str], outcomes: pl.DataFrame, now: datetime) -> list[str]:
    """Missing ids not answered absent or invalid within ``RETRY_AFTER``."""
    recent = {
        row["docket_id"]
        for row in outcomes.iter_rows(named=True)
        if row["outcome"] in {"absent", "invalid"} and now - datetime.fromisoformat(row["observed_at"]) < RETRY_AFTER
    }
    return [docket for docket in missing if docket not in recent]


def ask(reader, docket_id: str) -> Answer | None:
    """The publisher's answer, or ``None`` for a transport failure that answered nothing."""
    from spicy_docs.reading.paged_json import PagedJsonSourceError
    from spicy_docs.sources.regulations_gov.api import RegulationsGovApiUnavailableError

    try:
        detail = reader.docket(docket_id)
    except RegulationsGovApiUnavailableError as error:
        return Answer(docket_id, "absent", error.capture.status_code, error.capture.body)
    except PagedJsonSourceError as error:
        capture = getattr(error, "capture", None)
        if capture is not None and capture.status_code == 400 and b"Invalid ID" in capture.body:
            return Answer(docket_id, "invalid", 400, capture.body)
        logger.warning("docket gaps: {} answered nothing usable ({})", docket_id, error)
        return None
    return Answer(docket_id, "served", detail.capture.status_code, detail.capture.body, {"data": dict(detail.data)})


def fill(
    output_dir: Path,
    reader,
    *,
    max_dockets: int = DEFAULT_MAX_DOCKETS,
    skip_upload: bool = True,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> dict[str, int]:
    """Ask for each due missing docket, merge what is served, and record every answer."""
    output_dir.mkdir(parents=True, exist_ok=True)
    local = {key: output_dir / key for key in ("documents.parquet", "comments_index.parquet", "dockets.parquet")}
    for key, path in local.items():
        if not path.exists() and not r2.download_working_copy(key, path):
            raise RuntimeError(f"docket gaps: working copy {key} is missing")
    outcome_file = output_dir / OUTCOMES
    if not outcome_file.exists():
        r2.download_working_copy(OUTCOMES, outcome_file)
    prior = pl.read_parquet(outcome_file) if outcome_file.exists() else pl.DataFrame(schema=OUTCOME_SCHEMA)

    missing = missing_dockets(local["documents.parquet"], local["comments_index.parquet"], local["dockets.parquet"])
    asked = due(missing, prior, now())[:max_dockets]
    answers, failures = [], 0
    for docket_id in asked:
        answer = ask(reader, docket_id)
        if answer is None:
            failures += 1
        else:
            answers.append(answer)
    served = [answer for answer in answers if answer.outcome == "served"]
    counts = {"missing": len(missing), "asked": len(asked), "served": len(served),
              "absent": sum(a.outcome == "absent" for a in answers),
              "invalid": sum(a.outcome == "invalid" for a in answers), "unanswered": failures}
    logger.info("docket gaps: {}", counts)

    if served and not skip_upload:
        rows = list(ExtractRecords(DOCKETS).apply([answer.payload for answer in served]))
        with TemporaryDirectory(prefix="docket-gaps-", dir=output_dir) as staging:
            write_staging("regulations-gov-api", DOCKETS.name, rows, Path(staging), DOCKETS.schema)
            exported = iceberg.merge_and_export(Path(staging), output_dir, DOCKETS)
        if exported is not None:
            r2.preflight_uploads(output_dir, [exported])
            r2.upload_file(exported, remote_key=exported.name)

    observed_at = now().isoformat()
    fresh = pl.DataFrame(
        [{"docket_id": a.docket_id, "outcome": a.outcome, "http_status": a.http_status, "observed_at": observed_at,
          "body_sha256": hashlib.sha256(a.body).hexdigest()} for a in answers],
        schema=OUTCOME_SCHEMA,
    )
    merged = pl.concat([prior.join(fresh.select("docket_id"), on="docket_id", how="anti"), fresh]).sort("docket_id")
    merged.write_parquet(outcome_file)
    if not skip_upload:
        r2.upload_file(outcome_file, remote_key=OUTCOMES)
    if failures:
        raise RuntimeError(f"docket gaps: {failures} docket request(s) answered nothing; they will be asked again")
    return counts


app = App(name="fill-docket-gaps", help="Fill dockets the mirror lacks from the Regulations.gov API.")


@app.default
def main(
    output_dir: Annotated[Path, Parameter(help="Working directory")] = Path("output"),
    max_dockets: Annotated[int, Parameter(help="Most dockets to ask for in one run")] = DEFAULT_MAX_DOCKETS,
    skip_upload: Annotated[bool, Parameter(help="Ask and report only; no catalog merge or upload")] = True,
) -> None:
    from spicy_docs.reading.paged_json import PagedJsonBudget
    from spicy_docs.sources.regulations_gov.api import RegulationsGovApiReader

    load_dotenv()
    key = os.environ.get("DATA_GOV_API_KEY")
    if not key:
        raise RuntimeError("DATA_GOV_API_KEY is required to ask the Regulations.gov API")
    budget = PagedJsonBudget(max_requests=3, max_page_bytes=1 << 20, timeout_seconds=60, min_request_interval_seconds=1.0)
    with RegulationsGovApiReader(budget=budget, api_key=key) as reader:
        fill(output_dir, reader, max_dockets=max_dockets, skip_upload=skip_upload)


if __name__ == "__main__":
    app()
