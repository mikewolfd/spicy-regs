#!/usr/bin/env python3
"""Compare published column values against the value lists their publishers document.

GSA and reginfo.gov define these values, not this repository. Run with no
arguments to diff the documented domains — parsed from the pinned publisher
captures — against the checked-in observed snapshot. It fails on any finding the
ledger in :mod:`spicy_regs.sources.source_domains` leaves unrecorded, in either
direction. It touches no network and no parquet, so it runs anywhere in well
under a second.

Be precise about what that default run is. Both inputs are files in this
repository: a publisher capture pinned by digest, and one dated observation of
the published tables. Nothing refetches either here, so the verdict is a function
of the tree and changes only when someone changes one of those files. That makes
this a lock on a dated finding rather than a live drift detector — it fails the
moment a re-pin moves either half and leaves the ledger behind, which is exactly
when a human is watching. The test suite runs the offline half in CI; refreshing
the observed half is the manual step below.

``--observe --data-dir DIR`` re-observes the published tables from parquet and
prints the same report against that data; add ``--write-snapshot`` to re-pin the
checked-in snapshot from that observation. Each dated snapshot file observes its
own domains: ``--snapshot FILE`` selects which one an observation covers (the
2026-09-22 file by default), and ``--domains`` names them for a new file; each file observes whole tables.
``--unpublished`` records no publisher URL, for an observation of a built but
not yet published generation that must be re-pinned once it is published. ``DIR`` holds the published tables
under their own names (``documents.parquet`` and so on) — a fresh download of
the published corpus, or any directory built from it. The recorded publisher
URLs resolve through the publication index of the host the reader chain selects
(``SPICY_REGS_R2_URL``, then ``R2_PUBLIC_URL``), so an observation records where
the bytes actually live. Re-pinning a capture is
the other manual step: refetch it from the ``source_url`` its manifest entry
names, and record the digest and length of what came back.

Exit status: 0 when the ledger records every finding, 1 otherwise.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import tempfile
from collections.abc import Iterable, Mapping
from pathlib import Path

from spicy_regs.data_dictionary import TABLES
from spicy_regs.public_url import resolve_r2_base_url
from spicy_regs.sources.publication import current_index, single_member
from spicy_regs.sources.source_domains import (
    DEFAULT_SOURCE_DOMAIN_DIR,
    OBSERVED_SNAPSHOT_FILENAME,
    OBSERVED_SNAPSHOT_FILENAMES,
    OBSERVED_SNAPSHOT_FORMAT_VERSION,
    DocumentedDomain,
    DomainFinding,
    ObservedDomain,
    ObservedSnapshot,
    SourceDomainError,
    documented_domains,
    domain_findings,
    load_observed_snapshot,
    load_observed_snapshots,
    merged_domains,
    stale_accepted_findings,
    unrecorded_findings,
)

ROOT = Path(__file__).resolve().parents[1]
SOURCE_DOMAIN_DIR = ROOT / DEFAULT_SOURCE_DOMAIN_DIR


def table_urls(index: Mapping, base_url: str, tables: Iterable[str]) -> dict[str, str]:
    """Where each published table is served, resolved through the publication index.

    A family-published table lives under its generation prefix; a legacy table
    resolves to its root name. Only the ``--observe`` path calls this, so the
    default run's offline property is untouched.
    """

    base = base_url.rstrip("/")
    return {table: f"{base}/{single_member(index, f'{table}.parquet').path}" for table in tables}


def _connect():
    import duckdb

    connection = duckdb.connect()
    connection.execute(f"SET home_directory='{tempfile.gettempdir()}'")
    return connection


def _file_identity(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    length = 0
    with path.open("rb") as handle:
        while chunk := handle.read(1 << 20):
            digest.update(chunk)
            length += len(chunk)
    return "sha256:" + digest.hexdigest(), length


def _value_query(path: Path, column: str) -> tuple[str, str]:
    """The distinct-value query for a scalar column or a ``list_column.member`` path, and its row-count query."""

    if "." not in column:
        return (
            f"SELECT \"{column}\" AS value, count(*) AS n FROM read_parquet('{path}') GROUP BY 1 ORDER BY n DESC, 1",
            f"SELECT count(*) FROM read_parquet('{path}')",
        )
    list_column, member = column.split(".", 1)
    elements = f"FROM read_parquet('{path}'), UNNEST(\"{list_column}\") AS t(item)"
    return (
        f"SELECT item.\"{member}\" AS value, count(*) AS n {elements} GROUP BY 1 ORDER BY n DESC, 1",
        f"SELECT count(*) {elements}",
    )


def observe(
    data_dir: Path,
    documented: Mapping[str, DocumentedDomain],
    *,
    base_url: str | None,
    observed_at: str,
    producer_revision: str,
) -> ObservedSnapshot:
    """Scan the published tables in ``data_dir`` for each declared domain's distinct values.

    Takes the register the caller already parsed, so the observation and the diff
    it feeds cover the same domains, read from the same captures once. With no
    ``base_url`` the tables are an unpublished build and no publisher URL is recorded.
    """

    tables = sorted({domain.table for domain in documented.values()})
    urls = table_urls(current_index(base_url), base_url, tables) if base_url else dict.fromkeys(tables)
    connection = _connect()
    sources = []
    row_counts: dict[str, int] = {}
    for table in tables:
        # Where a table is published comes from the registry that publishes it,
        # never from a second list kept here. A hand-kept copy is how the first
        # version of this snapshot recorded a hostname this project never served.
        if table not in TABLES:
            raise SourceDomainError(f"{table!r} is not one of the published tables in spicy_regs.data_dictionary")
        path = data_dir / f"{table}.parquet"
        if not path.is_file():
            raise SourceDomainError(f"{data_dir} lacks the published table {table}.parquet")
        digest, byte_length = _file_identity(path)
        (row_count,) = connection.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()
        row_counts[table] = int(row_count)
        sources.append(
            {
                "byte_length": byte_length,
                "bytes_digest": digest,
                "publisher_url": urls[table],
                "row_count": int(row_count),
                "table": table,
            }
        )

    observed: dict[str, ObservedDomain] = {}
    for key, domain in documented.items():
        values, count_rows = _value_query(data_dir / f"{domain.table}.parquet", domain.column)
        rows = connection.execute(values).fetchall()
        null_count = sum(int(count) for value, count in rows if value is None)
        if "." in domain.column:
            (element_count,) = connection.execute(count_rows).fetchone()
            row_counts_for_domain = int(element_count)
        else:
            row_counts_for_domain = row_counts[domain.table]
        observed[key] = ObservedDomain(
            key=key,
            table=domain.table,
            column=domain.column,
            value_counts=tuple((str(value), int(count)) for value, count in rows if value is not None),
            null_count=null_count,
            row_count=row_counts_for_domain,
        )
    return ObservedSnapshot(
        observed_at=observed_at,
        producer_revision=producer_revision,
        sources=tuple(sources),
        domains=observed,
    )


def render_snapshot(snapshot: ObservedSnapshot) -> str:
    payload = {
        "domains": [
            {
                "column": domain.column,
                "key": domain.key,
                "null_count": domain.null_count,
                "row_count": domain.row_count,
                "table": domain.table,
                "value_counts": [[value, count] for value, count in domain.value_counts],
            }
            for domain in sorted(snapshot.domains.values(), key=lambda one: one.key)
        ],
        "format_version": OBSERVED_SNAPSHOT_FORMAT_VERSION,
        "observed_at": snapshot.observed_at,
        "producer_revision": snapshot.producer_revision,
        "sources": list(snapshot.sources),
    }
    return json.dumps(payload, sort_keys=True, indent=2) + "\n"


def _report(findings: tuple[DomainFinding, ...], unrecorded: tuple[DomainFinding, ...]) -> None:
    for finding in findings:
        support = "" if finding.row_count is None else f" on {finding.row_count:,} rows"
        flag = "UNRECORDED" if finding in unrecorded else "recorded"
        print(f"  [{flag}] {finding.domain_key}: {finding.kind} {finding.value!r}{support}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.observe and not args.data_dir:
        parser.error("--observe needs --data-dir")
    if args.write_snapshot and not args.observe:
        parser.error("--write-snapshot re-pins an observation; it needs --observe")
    if args.write_snapshot and not (args.observed_at and args.producer_revision):
        parser.error("--write-snapshot needs --observed-at and --producer-revision; a snapshot states its provenance")
    if args.snapshot not in OBSERVED_SNAPSHOT_FILENAMES:
        parser.error(f"--snapshot must be one of {', '.join(OBSERVED_SNAPSHOT_FILENAMES)}")

    documented = documented_domains(SOURCE_DOMAIN_DIR)
    if not args.observe:
        snapshots = load_observed_snapshots(SOURCE_DOMAIN_DIR)
        held = snapshots
    else:
        # An observation re-pins one dated file: the domains it holds, or those --domains names for a new one.
        # The other files stay as checked in, so the diff still covers every declared domain.
        held = tuple(
            load_observed_snapshot(SOURCE_DOMAIN_DIR, filename=name)
            for name in OBSERVED_SNAPSHOT_FILENAMES
            if name != args.snapshot
        )
        if args.domains:
            selected = args.domains.split(",")
        else:
            selected = list(load_observed_snapshot(SOURCE_DOMAIN_DIR, filename=args.snapshot).domains)
        if unknown := sorted(set(selected) - set(documented)):
            parser.error(f"--domains names undeclared domains {unknown}")
        snapshot = observe(
            Path(args.data_dir),
            {key: documented[key] for key in selected},
            base_url=None if args.unpublished else resolve_r2_base_url(),
            observed_at=args.observed_at,
            producer_revision=args.producer_revision,
        )
        snapshots = (snapshot,)
        held = (*held, snapshot)

    findings = domain_findings(documented, merged_domains(held))
    unrecorded = unrecorded_findings(findings)
    stale = stale_accepted_findings(findings)

    dates = ", ".join(f"{one.observed_at} ({one.producer_revision[:12]})" for one in snapshots)
    print(f"{len(documented)} documented domains against {dates}: {len(findings)} findings")
    _report(findings, unrecorded)

    if args.write_snapshot:
        target = SOURCE_DOMAIN_DIR / args.snapshot
        target.write_text(render_snapshot(snapshot), encoding="utf-8")
        print(f"wrote {target.relative_to(ROOT)}")

    for finding in unrecorded:
        print(
            f"error: {finding.domain_key} {finding.kind} {finding.value!r} is not in "
            "ACCEPTED_DOMAIN_FINDINGS; record it with a reason or fix the drift",
            file=sys.stderr,
        )
    for entry in stale:
        print(
            f"error: ACCEPTED_DOMAIN_FINDINGS records {entry.domain_key} {entry.kind} {entry.value!r}, "
            "which the data no longer produces; delete the entry",
            file=sys.stderr,
        )
    return 1 if unrecorded or stale else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--observe",
        action="store_true",
        help="re-observe from parquet in --data-dir instead of reading the checked-in snapshot",
    )
    parser.add_argument("--data-dir", help="directory holding the published tables as <table>.parquet")
    parser.add_argument("--observed-at", default="", help="observation timestamp to record in a written snapshot")
    parser.add_argument(
        "--producer-revision",
        default="",
        help="SpicyRegs revision that produced the observed tables",
    )
    parser.add_argument("--write-snapshot", action="store_true", help="re-pin the checked-in observed snapshot")
    parser.add_argument(
        "--snapshot",
        default=OBSERVED_SNAPSHOT_FILENAME,
        help="the dated snapshot file an observation covers and --write-snapshot re-pins",
    )
    parser.add_argument("--domains", help="comma-separated domain keys to observe for a snapshot file not yet written")
    parser.add_argument(
        "--unpublished",
        action="store_true",
        help="the data is a built generation not yet published: record no publisher URL",
    )
    return parser


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SourceDomainError as error:  # pragma: no cover - CLI surface
        print(f"source-domain error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
