"""Transform: build ``bill_subjects.parquet`` — the Library of Congress subject assignment per bill.

Closes the seam stated in :mod:`~spicy_regs.transforms.build_congress_bills`:
list-level ``congress_bills`` rows carry no subject assignment (one
``policy_area`` from a ~33-term controlled list, plus any number of legislative
subjects). A sibling table rather than extra columns, because the two artifacts
have different writers and fetch economics; it publishes on its own cron, keyed
by the same ``bill_id`` as ``congress_bills``, joined with a one-line ``LEFT JOIN``.

**The Congress picks the route.** From the 108th (``BULK_STATUS_FLOOR``) the
assignment is BILLSTATUS's, read without a per-bill request:

* a bill whose ``congress_bills`` row the bill family filled from BILLSTATUS
  (``schema_version`` set) is projected from that row;
* a bill in a folder (Congress and bill type) the family has read, whose own
  row is still list-level, waits for the family's next run rather than
  downloading the zip the family is about to read;
* a bill in a folder the family has not read comes from that folder's bulk zip
  through spicy-docs, one request per folder, at most
  :data:`MAX_FOLDERS_PER_RUN` folders a run.

Below the 108th only Congress.gov's ``/subjects`` holds it: one keyed request
per bill, at most :data:`MAX_API_BILLS_PER_RUN` a run; without a key those
bills stay pending.

**Incremental and resumable.** Best-effort prior from R2; a bill is pending when
the prior has no row for it, or has an empty row from the other carrier; newest
Congress first. Only an answer becomes a row: a failed folder, an unreadable
BILLSTATUS member or an API timeout writes nothing, so the bill is asked again
next run. A bill the carrier definitively lacks (an API 404, or a bulk folder
that does not list it) gets a row with a null ``policy_area`` and its carrier,
so it is not asked again. Prior and new rows merge on ``bill_id``, preferring
the fresh one.
"""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Callable, Iterator
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple, Protocol

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.sources.bill_subjects import (
    API_FIRST_CONGRESS,
    CARRIER_API,
    CARRIER_BULKDATA,
    BillSubjects,
    BillSubjectsFetcher,
    FetchCounts,
    assignment,
)
from spicy_regs.sources.congress_bills import _resolve_api_key
from spicy_regs.transforms.congress_scope import BULK_STATUS_FLOOR

if TYPE_CHECKING:
    from spicy_docs.sources.congress.bulk_status import BulkStatusArchive

OUTPUT = "bill_subjects.parquet"
BILLS_INPUT = "congress_bills.parquet"

#: Congress.gov bills per run. Its documented 5,000-requests-an-hour budget caps
#: the crawl at 1.33 a second, so 2,000 bills is ~25 minutes, inside the
#: reusable rollup workflow's 30-minute timeout, and leaves budget for retries.
MAX_API_BILLS_PER_RUN = 2_000

#: BILLSTATUS folder zips per run. Measured 2026-09-23 through the family's
#: ``BULK_BUDGET``: the 96 folders of Congresses 108-119 read in 169 s, the
#: largest (118 hr, 35.5 MB, 10,564 members) in 11 s at 343 MB peak RSS. 32
#: folders is four Congresses, about a minute; the 108th-117th backfill is three
#: runs.
MAX_FOLDERS_PER_RUN = 32

#: The published schema: all VARCHAR, keyed by ``bill_id``, joinable straight to
#: ``congress_bills``. The subject list is a JSON string so the table stays flat
#: and portable, matching ``lobbying_filings``' JSON-valued columns.
COLUMNS = (
    "bill_id",
    "policy_area",
    "subjects_json",
    "subject_count",
    "carrier",
    "enriched_at",
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])

#: Reads one folder's zip into its archive: the real acquirer, or a test's.
FolderReader = Callable[[int, str], "BulkStatusArchive"]


class PendingBill(NamedTuple):
    """One bill this run owes an answer, with what the bill family already holds for it."""

    bill_id: str
    congress: int
    bill_type: str
    bill_number: str
    from_family: bool
    family_folder: bool
    policy_area: str | None
    subjects_json: str | None


class SubjectsFetcher(Protocol):
    """What the API route needs from a fetcher: the real one, or a test's."""

    def subjects_for(self, congress: str, bill_type: str, bill_number: str) -> BillSubjects | None: ...
    def close(self) -> None: ...


def _shape(bill_id: str, policy_area: str | None, subjects: tuple[str, ...], carrier: str, now: str) -> dict:
    """Map one fetched assignment onto the published column shape."""
    return {
        "bill_id": bill_id,
        "policy_area": policy_area,
        "subjects_json": json.dumps(list(subjects)),
        "subject_count": str(len(subjects)),
        "carrier": carrier,
        "enriched_at": now,
    }


def _pending_bills(bills_file: Path, prior_file: Path, *, have_prior: bool) -> list[PendingBill]:
    """Bills still owed an answer, newest Congress first, with the family's BILLSTATUS fields.

    A bill is pending when the prior table has no row for it, or has a row that
    the *other* carrier left empty — the carrier its Congress now routes to may
    well hold what that one did not. Bills below the API's floor are never
    selected; the request could only 404.
    """
    import duckdb

    route = f"CASE WHEN bill.congress_number >= {BULK_STATUS_FLOOR} THEN '{CARRIER_BULKDATA}' ELSE '{CARRIER_API}' END"
    if have_prior:
        # The OR group is parenthesized on purpose: without it the floor filter
        # would bind only to the second branch.
        prior_join = f"LEFT JOIN read_parquet('{prior_file}') AS prior ON prior.bill_id = bill.bill_id"
        pending = f"(prior.bill_id IS NULL OR (prior.policy_area IS NULL AND prior.carrier IS DISTINCT FROM {route}))"
    else:
        prior_join, pending = "", "TRUE"

    rows = duckdb.sql(
        f"""
        WITH bill AS (
            SELECT *, TRY_CAST(congress AS INTEGER) AS congress_number, lower(bill_type) AS folder_type
            FROM read_parquet('{bills_file}')
            WHERE bill_id IS NOT NULL AND bill_type IS NOT NULL AND bill_number IS NOT NULL
        ),
        family_folder AS (
            SELECT DISTINCT congress_number, folder_type FROM bill WHERE schema_version IS NOT NULL
        )
        SELECT bill.bill_id, bill.congress_number, bill.folder_type, bill.bill_number,
               bill.schema_version IS NOT NULL, family.congress_number IS NOT NULL,
               bill.policy_area, bill.subjects_json
        FROM bill
        LEFT JOIN family_folder AS family
          ON family.congress_number = bill.congress_number AND family.folder_type = bill.folder_type
        {prior_join}
        WHERE {pending} AND bill.congress_number >= {API_FIRST_CONGRESS}
        ORDER BY bill.congress_number DESC, bill.bill_id
        """
    ).fetchall()
    return [PendingBill(str(a), int(b), str(c), str(d), bool(e), bool(f), g, h) for a, b, c, d, e, f, g, h in rows]


def _read_folders(
    folders: dict[tuple[int, str], list[PendingBill]], read_folder: FolderReader
) -> list[tuple[str, BillSubjects | None]]:
    """Answer each folder's pending bills from its one zip.

    A member the reader refuses (1.0.0-schema files, 13 of the 96 folders'
    members on 2026-09-23) is no answer, and neither is a bill the zip might
    hold under a name the reader could not read; a bill absent from a zip whose
    every member was named is one the carrier does not hold.
    """
    from spicy_docs.sources.congress.bill_acquisition import BillSourceUnavailableError
    from spicy_docs.sources.congress.bill_status import BillSourceError
    from spicy_docs.transport.credentials import CredentialRefusedError

    answers: list[tuple[str, BillSubjects | None]] = []
    for (congress, bill_type), bills in folders.items():
        try:
            archive = read_folder(congress, bill_type)
        except CredentialRefusedError:
            raise
        except BillSourceUnavailableError:
            logger.warning("Bill subjects: BILLSTATUS {} {} folder is not published", congress, bill_type)
            answers.extend((bill.bill_id, BillSubjects(None, (), CARRIER_BULKDATA, held=False)) for bill in bills)
            continue
        except (BillSourceError, httpx.HTTPError, ConnectionError) as error:
            logger.warning(
                "Bill subjects: BILLSTATUS {} {} folder not read: {}", congress, bill_type, type(error).__name__
            )
            answers.extend((bill.bill_id, None) for bill in bills)
            continue
        statuses = {m.identity.number: m.status for m in archive.members if m.identity is not None}
        every_member_named = all(m.identity is not None for m in archive.members)
        for bill in bills:
            number = int(bill.bill_number) if bill.bill_number.isdigit() else None
            if number in statuses:
                status = statuses[number]
                answer = None if status is None else assignment(status.policy_area, status.subjects, CARRIER_BULKDATA)
            elif number is not None and every_member_named:
                answer = BillSubjects(None, (), CARRIER_BULKDATA, held=False)
            else:
                answer = None
            answers.append((bill.bill_id, answer))
        logger.info(
            "Bill subjects: BILLSTATUS {} {} read for {:,} pending bills ({:,} members, {:,} refused by the reader)",
            congress,
            bill_type,
            len(bills),
            len(archive.members),
            archive.refused_count,
        )
    return answers


@contextmanager
def _folder_reader(injected: FolderReader | None) -> Iterator[FolderReader]:
    """The injected reader, or the keyless bulk route on the bill family's own per-zip budget."""
    if injected is not None:
        yield injected
        return
    from spicy_docs.sources.congress.bulk_status import BulkStatusAcquirer

    from spicy_regs.transforms.build_bill_family import BULK_BUDGET

    with BulkStatusAcquirer(budget=BULK_BUDGET) as acquirer:

        def read(congress: int, bill_type: str) -> BulkStatusArchive:
            archive = acquirer.acquire(congress, bill_type).archive
            assert archive is not None  # only an ``unchanged_since`` skip returns none, and none is passed
            return archive

        yield read


def _adopt_local_output(out_file: Path, prior_file: Path) -> bool:
    """Treat an artifact left by a previous local run as this run's prior."""
    if not out_file.exists():
        return False
    out_file.replace(prior_file)
    logger.info("Bill subjects: adopting the local {} as this run's prior", out_file.name)
    return True


def _log_counts(counts: FetchCounts, elapsed: float) -> None:
    """Report the run the way the repo's other incremental transforms do."""
    logger.info(
        "Bill subjects: answered={:,} with_policy_area={:,} subjects_only={:,} unassigned={:,} "
        "not_held={:,} failed={:,} in {:.0f}s",
        counts.answered,
        counts.with_policy_area,
        counts.subjects_only,
        counts.unassigned,
        counts.not_held,
        counts.failed,
        elapsed,
    )
    for name, n in sorted(counts.policy_areas.items(), key=lambda kv: (-kv[1], kv[0]))[:10]:
        logger.info("Bill subjects:   {:>6,}  {}", n, name)


def _log_coverage(out_file: Path, bills_file: Path) -> None:
    """State enriched/total against the bill table, in named numbers."""
    import duckdb

    row = duckdb.sql(
        f"""
        SELECT
            (SELECT count(*) FROM read_parquet('{bills_file}')) AS bills,
            (SELECT count(*) FROM read_parquet('{out_file}')) AS rows,
            (SELECT count(*) FROM read_parquet('{out_file}') WHERE policy_area IS NOT NULL) AS with_area
        """
    ).fetchone()
    if not row:
        return
    bills, rows, with_area = row
    share = (100.0 * with_area / bills) if bills else 0.0
    logger.info(
        "Bill subjects: {:,} rows, {:,} with a policy area, against {:,} bills ({:.1f}% of the archive)",
        rows,
        with_area,
        bills,
        share,
    )


def enrich_bill_subjects(
    output_dir: Path,
    *,
    max_bills: int | None = None,
    max_folders: int = MAX_FOLDERS_PER_RUN,
    fetcher: SubjectsFetcher | None = None,
    read_folder: FolderReader | None = None,
) -> Path:
    """Build ``bill_subjects.parquet`` (bounded, resumable enrichment pass).

    ``max_bills`` caps the Congress.gov bills asked this run (default
    :data:`MAX_API_BILLS_PER_RUN`); ``max_folders`` caps the BILLSTATUS zips
    read. Projecting the bill family's own rows costs no request and is not capped.
    """
    import duckdb

    out_file = output_dir / OUTPUT
    bills_file = output_dir / BILLS_INPUT
    prior_file = output_dir / "_bill_subjects_prior.parquet"
    max_bills = MAX_API_BILLS_PER_RUN if max_bills is None else max_bills

    if not bills_file.exists():
        raise RuntimeError(f"Bill subjects: {BILLS_INPUT} not found in {output_dir} — prime it from R2 first")

    # 1. Pull the prior table (best effort — absence just means a fresh backfill).
    #    A backfill spread over many runs must never lose ground, so a local
    #    artifact from a previous run counts as a prior in its own right: without
    #    this, an R2-less re-run would silently overwrite everything enriched so
    #    far and start again from the top of the archive.
    have_prior = prior_file.exists() or r2.download(OUTPUT, prior_file) or _adopt_local_output(out_file, prior_file)
    if have_prior:
        logger.info("Bill subjects: merging against prior table {}", prior_file)
    else:
        logger.info("Bill subjects: no prior table found — starting the backfill")

    # 2. Route each pending bill by its Congress, newest first.
    family: list[PendingBill] = []
    folders: dict[tuple[int, str], list[PendingBill]] = defaultdict(list)
    api: list[PendingBill] = []
    waiting = 0
    for bill in _pending_bills(bills_file, prior_file, have_prior=have_prior):
        if bill.congress < BULK_STATUS_FLOOR:
            api.append(bill)
        elif bill.from_family:
            family.append(bill)
        elif bill.family_folder:
            waiting += 1
        else:
            folders[(bill.congress, bill.bill_type)].append(bill)
    read = dict(list(folders.items())[:max_folders])
    routes = {
        "family": len(family),
        "folders": len(read),
        "folder_bills": sum(map(len, read.values())),
        "folders_beyond_cap": len(folders) - len(read),
        "waiting_on_family": waiting,
        "api": min(len(api), max_bills),
        "api_beyond_cap": max(len(api) - max_bills, 0),
    }
    logger.info("Bill subjects: pending by route {}", routes)

    # 3. Answer. Only an answer becomes a row; a failure leaves the bill for next run.
    now = datetime.now(UTC).isoformat(timespec="seconds")
    started = datetime.now(UTC)
    counts = FetchCounts()
    answers: list[tuple[str, BillSubjects | None]] = [
        (bill.bill_id, assignment(bill.policy_area, json.loads(bill.subjects_json or "[]"), CARRIER_BULKDATA))
        for bill in family
    ]
    if read:
        with _folder_reader(read_folder) as reader:
            answers += _read_folders(read, reader)
    api = api[:max_bills]
    if api and fetcher is None and not _resolve_api_key():
        logger.warning(
            "Bill subjects: {:,} bills below the {}th Congress wait for an api.data.gov key",
            len(api),
            BULK_STATUS_FLOOR,
        )
        api = []
    if api:
        with nullcontext(fetcher) if fetcher is not None else BillSubjectsFetcher() as api_fetcher:
            for seen, bill in enumerate(api, start=1):
                result = api_fetcher.subjects_for(str(bill.congress), bill.bill_type, bill.bill_number)
                answers.append((bill.bill_id, result))
                if seen % 1_000 == 0:
                    logger.info("Bill subjects: {:,}/{:,} API bills fetched...", seen, len(api))
    rows = []
    for bill_id, result in answers:
        counts.record(result)
        if result is not None:
            rows.append(_shape(bill_id, result.policy_area, result.subjects, result.carrier, now))
    _log_counts(counts, (datetime.now(UTC) - started).total_seconds())

    # 4. Merge prior + new, dedup on bill_id preferring the fresh row.
    new_file = output_dir / "_bill_subjects_new.parquet"
    table = pa.Table.from_pylist(rows, schema=_SCHEMA) if rows else _SCHEMA.empty_table()
    pq.write_table(table, new_file, compression="zstd")

    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    cols = ", ".join(COLUMNS)
    if have_prior:
        union = (
            f"SELECT {cols}, 0 AS _src FROM read_parquet('{prior_file}') "
            f"UNION ALL BY NAME "
            f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_file}')"
        )
    else:
        union = f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_file}')"

    con.execute(
        f"""
        COPY (
            SELECT {cols} FROM (
                SELECT {cols}, ROW_NUMBER() OVER (
                    PARTITION BY bill_id ORDER BY _src DESC
                ) AS _rn
                FROM ({union})
                WHERE bill_id IS NOT NULL
            )
            WHERE _rn = 1
            ORDER BY enriched_at DESC, bill_id
        ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
        """
    )
    con.close()

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    _log_coverage(out_file, bills_file)
    return out_file
