"""Mirror committed local raw acquisitions to the existing R2 archive, add-only."""

from __future__ import annotations

import argparse
from collections.abc import Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import dataclass
from itertools import batched
import json
from os import getenv
from pathlib import Path
from zlib import error as ZlibError

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from spicy_docs.storage.raw_archive import CentralRawArchiveStore, iter_acquisition_receipts

from spicy_regs.sources.publication import _head, _put_immutable_stream
from spicy_regs.sources.r2 import VERIFIED_BUCKET, VERIFIED_ENDPOINT

PREFIX = "spicyregs/raw/"
USER_AGENT = "spicy-regs-srjv/1.0 (+https://spicygov.ai)"
HEAD_WORKERS = 16


@dataclass(frozen=True)
class Member:
    key: str
    size: int
    sha256: str


def _r2_client():
    endpoint, bucket = getenv("R2_ENDPOINT"), getenv("R2_BUCKET_NAME", VERIFIED_BUCKET)
    if endpoint != VERIFIED_ENDPOINT or bucket != VERIFIED_BUCKET:
        raise ValueError("R2_ENDPOINT and R2_BUCKET_NAME must select the verified spicy-regs deployment")
    access_key, secret = getenv("R2_ACCESS_KEY_ID"), getenv("R2_SECRET_ACCESS_KEY")
    if not access_key or not secret:
        raise ValueError("R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY are required")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="auto",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret,
        config=Config(
            user_agent_extra=USER_AGENT,
            max_pool_connections=HEAD_WORKERS,
            connect_timeout=10,
            read_timeout=60,
            retries={"mode": "standard", "total_max_attempts": 3},
            request_checksum_calculation="when_required",
        ),
    )


def _inventory(
    store: CentralRawArchiveStore, receipt_keys: Sequence[str] | None,
) -> tuple[dict[str, dict], list[Member]]:
    """Replay the acquisition journal, not the already-mirrored frozen base."""
    objects: dict[str, dict] = {}
    receipts = []
    for path, document in iter_acquisition_receipts(store.root, receipt_keys=receipt_keys):
        key = document["receipt_object"]
        with store.open_member(key) as stream:
            receipts.append(Member(key, stream.seek(0, 2), path.name))
        for descriptor in document["objects"]:
            key = descriptor["object"]
            if objects.setdefault(key, descriptor) != descriptor:
                raise ValueError(f"Conflicting local object descriptors for {key}")
    return objects, receipts


def _present(client, member: Member) -> bool:
    key = PREFIX + member.key
    try:
        head = _head(client, VERIFIED_BUCKET, key)
    except ClientError as exc:
        raise ValueError(f"HEAD {key} failed: {exc}") from exc
    if head is None:
        return False
    size = head.get("ContentLength")
    if type(size) is not int or size != member.size:
        raise ValueError(f"Refusing {key}: remote size {size!r} differs from local size {member.size}")
    return True


def _missing_members(client, members: Iterable[Member], pool: ThreadPoolExecutor) -> list[Member]:
    def missing(member: Member) -> bool:
        return not _present(client, member)

    # Bound queued HEADs so a large journal or a refusal can't flood the client.
    return [
        member
        for batch in batched(members, HEAD_WORKERS * 4)
        for member, absent in zip(batch, pool.map(missing, batch), strict=True)
        if absent
    ]


def _ensure_members(store: CentralRawArchiveStore, client, members: list[Member]) -> int:
    for member in members:
        key = PREFIX + member.key
        # The shared uploader checks the bytes actually sent, using a create-only
        # PUT (or create-only multipart completion). Never fall back to overwrite.
        _put_immutable_stream(
            client, VERIFIED_BUCKET, key, lambda: store.open_member(member.key),
            member.size, sha256=f"sha256:{member.sha256}",
        )
        if not _present(client, member):
            raise ValueError(f"{key} is missing after conditional upload")
    # Includes matching objects created concurrently. HEAD proves size, not authorship.
    return len(members)


def sync(store: CentralRawArchiveStore, client, *, execute: bool = False, receipt_keys: Sequence[str] | None = None):
    descriptors, receipts = _inventory(store, receipt_keys)
    objects = [Member(key, item["stored_bytes"], item["stored_sha256"]) for key, item in descriptors.items()]
    with ThreadPoolExecutor(max_workers=HEAD_WORKERS) as pool:
        missing_objects = _missing_members(client, objects, pool)
        missing_receipts = _missing_members(client, receipts, pool)
    for member in missing_objects:
        expected = descriptors[member.key]
        try:
            observed = store.describe_blob(expected["sha256"], expected["bytes"])
        except (OSError, ValueError, ZlibError, EOFError) as exc:
            raise ValueError(f"Local blob {member.key} failed integrity verification: {exc}") from exc
        if observed != expected:
            raise ValueError(f"Local blob {member.key} differs from its acquisition descriptor")

    summary = {}
    for kind, selected, missing in (("objects", objects, missing_objects), ("acquisitions", receipts, missing_receipts)):
        # Finish all original objects before making any acquisition receipt visible.
        summary[kind] = {
            "selected": len(selected),
            "skipped_size_match": len(selected) - len(missing),
            "missing_at_preflight": len(missing),
            "missing_bytes": sum(member.size for member in missing),
            "ensured_after_conditional_write": _ensure_members(store, client, missing) if execute else 0,
        }
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-root", type=Path, help="Override SPICYGOV_RAW_ROOT (default: ~/Work/corpora/spicyregs/raw)")
    parser.add_argument("--receipt-key", action="append", help="Only sync this canonical receipt key; repeat as needed")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_false", dest="execute", help="Check only, no writes (default)")
    mode.add_argument("--execute", "--no-dry-run", action="store_true", help="Create missing R2 objects and receipts")
    parser.set_defaults(execute=False)
    args = parser.parse_args(argv)

    receipt = {"dry_run": not args.execute, "destination": f"s3://{VERIFIED_BUCKET}/{PREFIX}"}
    try:
        store = CentralRawArchiveStore(args.raw_root.expanduser() if args.raw_root else None, create=False)
        receipt["raw_root"] = str(store.root)
        with closing(_r2_client()) as client:
            receipt.update(sync(store, client, execute=args.execute, receipt_keys=args.receipt_key))
    except (BotoCoreError, ClientError, OSError, ValueError, TypeError, KeyError, RuntimeError) as exc:
        print(json.dumps({**receipt, "status": "failed", "error": str(exc)}, sort_keys=True))
        return 1
    print(json.dumps({**receipt, "status": "ok"}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
