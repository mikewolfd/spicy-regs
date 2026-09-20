"""Require a regulations.gov record identity before accepting a mirror key."""

from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from itertools import batched, chain
from typing import Any

from spicy_docs.sources.mirrulations import KeyOutcome, MirrulationsReader


class _CountAttempts:
    """Count supplier GET attempts, excluding retries internal to the S3 SDK."""

    def __init__(self, resource: Any) -> None:
        self.resource = resource
        self.attempts = 0

    def Object(self, bucket: str, key: str):  # noqa: N802 — boto3's API
        self.attempts += 1
        return self.resource.Object(bucket, key)


class IdentityCheckedReader:
    """Keep supplier retries and key accounting, then validate each key's record."""

    def __init__(self, reader: MirrulationsReader, previous: Iterable[KeyOutcome]) -> None:
        self.reader = reader
        self.previous = {item.key: item for item in previous}
        self.last_keys: list[str] = []
        self.unresolved: list[KeyOutcome] = []

    @property
    def failed_keys(self) -> list[str]:
        return [item.key for item in self.unresolved]

    @property
    def parse_failed_keys(self) -> list[str]:
        return [item.key for item in self.unresolved if item.status != "transport"]

    def iter_records(self) -> Iterator[dict]:
        reader = self.reader
        if reader.key_lister is None:
            raise ValueError("IdentityCheckedReader requires the pipeline's key lister")
        self.last_keys = []
        self.unresolved = []
        keys = chain(self.previous, (key for key in reader.key_lister() if key not in self.previous))

        def read_key(key: str) -> tuple[str, list[dict], list[KeyOutcome]]:
            prior = self.previous.get(key)
            resource = _CountAttempts(reader.s3_resource)
            # The supplier yields unkeyed records. A one-key reader keeps the
            # record tied to its key while preserving supplier retry/refusal rules.
            source = MirrulationsReader(
                resource, reader.bucket, reader.prefix, reader.agency, reader.record_type,
                key_lister=lambda: [key], download_workers=1,
                unresolved_keys=[prior] if prior is not None else [],
            )
            records = list(source.iter_records())
            for record in records:
                data = record.get("data")
                identifier = data.get("id") if isinstance(data, dict) else None
                # spicy-docs is adding the same shape check. At the next
                # adoption this becomes belt-and-braces, not a permanent
                # restatement of supplier parsing and retry behavior.
                if not isinstance(identifier, str) or not identifier.strip():
                    return key, [], [KeyOutcome(
                        key, "requested-empty", "missing-record-identity: expected a non-blank data.id string",
                        datetime.now(UTC).isoformat(timespec="seconds"),
                        (prior.attempts if prior else 0) + resource.attempts,
                    )]
            return key, records, source.unresolved

        # Bound submitted work and retain retry priority without buffering all
        # payloads. The supplier still owns downloads, retries and refusals.
        workers = max(1, reader.download_workers)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for batch in batched(keys, workers):
                for key, records, outcomes in executor.map(read_key, batch):
                    self.unresolved.extend(outcomes)
                    if records:
                        yield from records
                        self.last_keys.append(key)
