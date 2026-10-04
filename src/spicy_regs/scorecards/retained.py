"""Run qualified private captures through installed, publisher-specific readers."""

from collections import Counter
from hashlib import sha256
import json
from pathlib import Path
import re

from spicy_docs.sources.scorecards import ADAPTER_PUBLISHERS, ScorecardEdition, get_adapter

from spicy_regs.scorecards.replay import RetainedScorecardSequence, ScorecardReplayError
from spicy_regs.scorecards.acquisition import MAX_BYTES, MAX_REQUESTS, validate_limits


def pinned_bytes(path: Path, pin: str, *, max_bytes=200 * 1024**2):
    with path.open("rb") as stream:
        body = stream.read(max_bytes + 1)
    if len(body) > max_bytes or sha256(body).hexdigest() != pin:
        raise ScorecardReplayError("Qualified input exceeds its bound or differs from its pin")
    return body


def qualified_reader_class(adapter, name):
    constructor = getattr(adapter, name, None)
    if not re.fullmatch(r"[A-Z][A-Za-z0-9_]*Reader", name) or not isinstance(constructor, type):
        raise ScorecardReplayError("Unknown qualified semantic reader injection")
    return constructor


def qualified_reader_inputs(entry, resolve_path):
    """Load named, pinned private assets without interpreting publisher fields."""
    inputs = entry.get("reader_inputs")
    if not isinstance(inputs, dict) or not inputs or entry.get("observations_file"):
        raise ScorecardReplayError("Qualified reader inputs must use one explicit asset shape")

    def read(value, depth=0):
        if not isinstance(value, dict) or not value or depth > 4:
            raise ScorecardReplayError("Qualified reader input has an invalid pinned shape")
        if set(value) == {"file", "sha256"}:
            return pinned_bytes(resolve_path(value["file"]), value["sha256"])
        if any(not isinstance(key, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", key) for key in value):
            raise ScorecardReplayError("Qualified reader input names changed")
        return {key: read(item, depth + 1) for key, item in value.items()}

    if "retain_observations" in inputs:
        raise ScorecardReplayError("Private retention callbacks belong to the host")
    return read(inputs)


def canonical(rows, *, observation_ids=False):
    omitted = {"snapshot_id", "capture_id", "capture_ids_json", "capture_roles_json"} if observation_ids else set()
    uuid = r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}"

    def value(item, field=""):
        if isinstance(item, dict):
            return {key: value(text, key) for key, text in item.items()}
        if isinstance(item, list):
            return [value(text, field) for text in item]
        if isinstance(item, str) and observation_ids:
            if field.endswith("source_path"):
                item = re.sub(r"(?<=;observation=)" + uuid + r"(?=[;/]|$)", "<observation>", item)
                item = re.sub(r"((?:extraction|observation)\[)" + uuid + r"(\])", r"\1<observation>\2", item)
            elif field.endswith("_json"):
                return json.dumps(value(json.loads(item)), sort_keys=True)
        return item

    return Counter(
        json.dumps(value({k: v for k, v in row.items() if k not in omitted}), sort_keys=True) for row in rows
    )


def check_reference(actual, reference):
    if set(actual) != set(reference):
        raise ScorecardReplayError("Qualified reference does not contain the complete source table family")
    for name in actual:
        if canonical(actual[name], observation_ids=True) != canonical(reference[name], observation_ids=True):
            raise ScorecardReplayError("Reader facts differ from the qualified source reference: " + name)


def check_preserved(prior, current, *, scorecard_ids, publisher_ids):
    if set(prior) != set(current):
        raise ScorecardReplayError("Preservation check requires the entire source table family")
    counts = {}
    for name in current:
        column, replaced = (
            ("publisher_id", publisher_ids)
            if name == "scorecard_publishers"
            else (
                "scorecard_id",
                scorecard_ids,
            )
        )
        old = [row for row in prior[name] if row[column] not in replaced]
        kept = [row for row in current[name] if row[column] not in replaced]
        if canonical(old) != canonical(kept):
            raise ScorecardReplayError("Unselected source rows changed: " + name)
        counts[name] = len(old)
    return counts


class RetainedScorecardBatch:
    """Select explicit qualified editions without scraping a discovery mirror.

    Each entry pins its installed reader, ordered captures and qualified table
    reference. Source listing is replayed only when present in that capture
    sequence. Static selection is a qualified-input choice, not fresh discovery.
    Publisher parsing and completeness checks always run again.
    """

    parser_version = "scorecard-retained-scope-selection/1"

    def __init__(
        self, corpus: Path, entries, *, retain_observations=None, max_requests=MAX_REQUESTS, max_bytes=MAX_BYTES
    ):
        validate_limits(max_bytes, max_requests)
        self.corpus = corpus.resolve()
        self.entries = {}
        self.active = None
        self.checked = []
        self.reference_publishers = {}
        total = 0
        for selected in entries:
            entry = dict(selected)
            edition = ScorecardEdition(**entry["edition"])
            adapter_name = entry["adapter"]
            if ADAPTER_PUBLISHERS.get(adapter_name) != edition.publisher_id or edition.scorecard_id in self.entries:
                raise ScorecardReplayError("Qualified edition escaped its publisher or repeats a selected scope")
            root = self.path(entry["captures_root"])
            adapter = get_adapter(adapter_name)
            pinned_bytes(Path(adapter.__file__), entry["reader_sha256"], max_bytes=1024**2)
            replay = RetainedScorecardSequence(root, expected_sha256=entry["manifest_sha256"], max_bytes=max_bytes)
            total += len(replay.records)
            if total > max_requests:
                raise ScorecardReplayError("Qualified publisher batch exceeds the shared acquisition budget")
            if entry.get("reader_class"):
                constructor = qualified_reader_class(adapter, entry["reader_class"])
                if retain_observations is None:
                    raise ScorecardReplayError("Qualified semantic readers require private observation retention")
                if entry.get("reader_inputs"):
                    adapter = constructor(
                        **qualified_reader_inputs(entry, self.path), retain_observations=retain_observations
                    )
                else:
                    asset = pinned_bytes(self.path(entry["observations_file"]), entry["observations_sha256"])
                    adapter = constructor(asset, retain_observations)
            entry.update(edition_object=edition, reader=adapter, replay=replay)
            self.entries[edition.scorecard_id] = entry
        if not self.entries or len({e["edition_object"].publisher_id for e in self.entries.values()}) != 1:
            raise ScorecardReplayError("A retained batch must select exactly one publisher")

    def path(self, relative):
        if not isinstance(relative, str) or Path(relative).is_absolute():
            raise ScorecardReplayError("Qualified input path must be relative to the private corpus")
        path = (self.corpus / relative).resolve()
        if not path.is_relative_to(self.corpus):
            raise ScorecardReplayError("Qualified input path leaves the private corpus")
        return path

    def list_scorecards(self, context):
        result = []
        for entry in self.entries.values():
            self.active = entry
            edition = entry["edition_object"]
            if entry.get("list_source") and edition not in entry["reader"].list_scorecards(context):
                raise ScorecardReplayError("Qualified edition is absent from its retained source listing")
            result.append(edition)
        return tuple(result)

    def for_edition(self, edition):
        entry = self.entries.get(edition.scorecard_id)
        if entry is None or edition != entry["edition_object"]:
            raise ScorecardReplayError("Reader selection differs from the qualified edition")
        self.active = entry
        batch = self

        class CheckedReader:
            parser_version = entry["reader"].parser_version

            def acquire_scorecard(self, edition, context):
                result = entry["reader"].acquire_scorecard(edition, context)
                entry["replay"].complete()
                reference = json.loads(pinned_bytes(batch.path(entry["reference_file"]), entry["reference_sha256"]))
                check_reference(result.tables, reference)
                batch.checked.append(edition.scorecard_id)
                for publisher in reference["scorecard_publishers"]:
                    old = batch.reference_publishers.get(publisher["publisher_id"])
                    if old is None or (publisher.get("observed_at") or "") > (old.get("observed_at") or ""):
                        batch.reference_publishers[publisher["publisher_id"]] = publisher
                return result

        return CheckedReader()

    def __call__(self, url):
        return self.request(url, method="GET", content=None)

    def request(self, url, **options):
        if self.active is None:
            raise ScorecardReplayError("No qualified edition selected for acquisition")
        return self.active["replay"].request(url, **options)

    def complete(self):
        if set(self.checked) != set(self.entries) or len(self.checked) != len(self.entries):
            raise ScorecardReplayError("Not every qualified edition completed reader and source-reference checks")
        for entry in self.entries.values():
            entry["replay"].complete()
