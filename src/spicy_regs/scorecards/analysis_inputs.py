"""Explicit conversion of complete published congressional shaped inputs."""

from collections.abc import Mapping, Sequence
from hashlib import file_digest, sha256
from itertools import zip_longest
from pathlib import Path

import pyarrow.parquet as pq

from spicy_regs.congress_receipts import CongressInput, write_congress_dataset
from spicy_regs.etl_receipts import combine_receipts, exact_json
from spicy_regs.scorecards.resolution import OFFICIAL_COLUMNS
from spicy_regs.sources import publication


def _rows(paths):
    for path in paths:
        with pq.ParquetFile(path) as reader:
            for batch in reader.iter_batches(batch_size=2000):
                yield from batch.to_pylist()


def convert_published_official_input(
    snapshot: Mapping,
    name: str,
    paths: Sequence[Path],
    directory: Path,
    *,
    generation_id: str,
    published_url: str,
) -> tuple[CongressInput, dict]:
    """Convert pinned shaped bytes, admitting every native subject and receipt.

    The original public pins remain analysis parents. These witnesses name
    published shaped data, never an original publisher HTTP response. Refused
    conversions, incomplete populations, schema/footer changes and changed
    values stop the preparation. Native published inputs use their own existing
    receipt path instead of this explicitly selected conversion.
    """
    if name not in OFFICIAL_COLUMNS or not generation_id or not published_url:
        raise ValueError("Published official conversion requires an explicit dataset, generation and source URL")
    owner = publication.table_owner(snapshot, name + ".parquet")
    if owner is None or "etlReceipts" in owner[1]:
        raise ValueError("Convert only explicitly selected published shaped official inputs")
    members = publication.table_members(snapshot, name + ".parquet")
    if not paths or len(paths) != len(members):
        raise ValueError("Published conversion requires every selected table member")
    schema = pq.read_schema(paths[0])
    if set(OFFICIAL_COLUMNS[name]) - set(schema.names):
        raise ValueError("Published official input lacks required resolver fields: " + name)
    for path, member in zip(paths, members, strict=True):
        with path.open("rb") as stream:
            digest = "sha256:" + file_digest(stream, "sha256").hexdigest()
        if digest != member.sha256 or path.stat().st_size != member.byte_size:
            raise ValueError("Published conversion input differs from its immutable pin")
        if not pq.read_schema(path).equals(schema, check_metadata=True):
            raise ValueError("Published input members have inconsistent source schemas or footers")
    directory.mkdir(parents=True, exist_ok=False)
    subjects, receipts, witnesses = [], [], []
    for ordinal, (path, member) in enumerate(zip(paths, members, strict=True)):
        witness = dict(source_id="published-shaped-observation:" + name,
                       source_uri=published_url.rstrip("/") + "/" + member.path,
                       sha256=member.sha256, locator=member.key, body_version=None)
        subject, receipt = write_congress_dataset(
            path, directory / f"member-{ordinal}", dataset=name,
            generation_id=generation_id, witnesses=[witness],
        )
        if subject is None:
            raise ValueError("Official conversion produced no domain subject table")
        subjects.append(subject)
        receipts.append(receipt)
        witnesses.append(witness)
    combined = combine_receipts(receipts, directory / "etl_receipts.parquet")
    if any(row["outcome"] == "refused" for row in _rows([combined])):
        raise ValueError("Published official conversion has refused native values; retained attempts require review")
    selected = CongressInput(tuple(subjects), combined, generation_id)
    restored = selected.materialize(name, directory / "restored.parquet")
    if not pq.read_schema(restored).equals(schema, check_metadata=True):
        raise ValueError("Published official conversion changed source schema or footer")
    before, after, count = sha256(), sha256(), 0
    absent = object()
    for original, returned in zip_longest(_rows(paths), _rows([restored]), fillvalue=absent):
        if original is absent or returned is absent:
            raise ValueError("Published official conversion changed the complete source population")
        left, right = exact_json(original), exact_json(returned)
        if left != right:
            raise ValueError("Published official conversion changed an original source field")
        before.update((left + "\n").encode())
        after.update((right + "\n").encode())
        count += 1
    declared = owner[1]["tables"][name + ".parquet"]["rows"]
    if count != declared:
        raise ValueError("Published official conversion population differs from the selected table count")
    return selected, dict(dataset=name, generation_id=generation_id, rows=count,
                         original_table_pin=publication.table_pin(snapshot, name + ".parquet"),
                         original_rows_sha256=before.hexdigest(), restored_rows_sha256=after.hexdigest(),
                         exact_schema_and_footer=True, required_resolver_columns=list(OFFICIAL_COLUMNS[name]),
                         witnesses=witnesses, source_acquisition_requests=0,
                         scope="Verified conversion of complete published shaped inputs; no original acquisition is asserted")
