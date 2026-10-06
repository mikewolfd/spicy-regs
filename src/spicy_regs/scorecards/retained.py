"""Run qualified private captures through installed, publisher-specific readers."""

from collections import Counter
from hashlib import sha256
from inspect import signature
import json
from pathlib import Path
import re

from spicy_docs.sources.scorecards import ADAPTER_PUBLISHERS, ScorecardEdition, get_adapter

from spicy_regs.scorecards.replay import RetainedScorecardSequence, ScorecardReplayError
from spicy_regs.scorecards.acquisition import MAX_BYTES, MAX_REQUESTS, validate_limits
from spicy_regs.scorecards.subject_shapes import IDENTITIES, source_row_index

QUALIFIED_INPUT_MAX_BYTES = 200 * 1024**2


def pinned_bytes(path: Path, pin: str, *, max_bytes=QUALIFIED_INPUT_MAX_BYTES):
    with path.open("rb") as stream:
        body = stream.read(max_bytes + 1)
    if len(body) > max_bytes or sha256(body).hexdigest() != pin:
        raise ScorecardReplayError("Qualified input exceeds its bound or differs from its pin")
    return body


def qualified_reference(entry, resolve_path):
    """Read a pinned table reference under its own explicitly selected byte bound.

    A complete derived table family can exceed any individual HTTP response.
    Its plan-selected limit never changes source capture or request budgets.
    """
    limit = entry.get("reference_max_bytes", QUALIFIED_INPUT_MAX_BYTES)
    validate_limits(limit, 1)
    return json.loads(pinned_bytes(resolve_path(entry["reference_file"]), entry["reference_sha256"], max_bytes=limit))


def qualified_reader_class(adapter, name, *, entry=None):
    constructor = getattr(adapter, name, None)
    if not re.fullmatch(r"[A-Z][A-Za-z0-9_]*Reader", name) or not isinstance(constructor, type):
        raise ScorecardReplayError("Unknown qualified named reader injection")
    if entry is not None:
        # Metadata generation and replay use the same constructor shape. Bind
        # placeholders without executing a reader or exposing private assets.
        inputs, observations = entry.get("reader_inputs"), entry.get("observations_file")
        if inputs and observations:
            raise ScorecardReplayError("Qualified reader inputs must select one explicit asset shape")
        args: tuple[object, ...] = ()
        kwargs: dict[str, object] = {}
        if inputs:
            if not isinstance(inputs, dict):
                raise ScorecardReplayError("Qualified reader inputs must be a named asset mapping")
            for field in inputs:
                if not isinstance(field, str):
                    raise ScorecardReplayError("Qualified reader inputs must have text names")
                kwargs[field] = None
            kwargs["retain_observations"] = None
        elif observations:
            args = (None, None)
        try:
            signature(constructor).bind(*args, **kwargs)
        except (TypeError, ValueError):
            raise ScorecardReplayError("Qualified reader inputs do not bind to the selected reader") from None
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


def canonical(rows, *, observation_ids=False, capture_ids=None):
    omitted = {"snapshot_id", "capture_id", "capture_ids_json", "capture_roles_json"} if observation_ids else set()
    uuid = r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}"

    def locator(match, scope):
        if capture_ids is None:
            return match[0]
        selected = capture_ids.get(match[0])
        if selected is None or (scope is not None and selected[0] != scope):
            raise ScorecardReplayError("Qualified source reference locator cites an unselected capture")
        return json.dumps(selected, separators=(",", ":"))

    def value(item, field="", scope=None):
        if isinstance(item, dict):
            return {key: value(text, key, scope) for key, text in item.items()}
        if isinstance(item, list):
            return [value(text, field, scope) for text in item]
        if isinstance(item, str) and observation_ids:
            if field.endswith("source_path"):
                item = re.sub(
                    r"(?<=capture:)([^#;/]+)(?=[#;/]|$)",
                    lambda match: locator(match, scope),
                    item,
                )
                item = re.sub(r"(?<=;observation=)" + uuid + r"(?=[;/]|$)", "<observation>", item)
                item = re.sub(r"((?:extraction|observation)\[)" + uuid + r"(\])", r"\1<observation>\2", item)
            elif field.endswith("_json"):
                return json.dumps(value(json.loads(item), scope=scope), sort_keys=True)
        return item

    return Counter(
        json.dumps(
            value({k: v for k, v in row.items() if k not in omitted}, scope=row.get("scorecard_id")),
            sort_keys=True,
        )
        for row in rows
    )


def _capture_locator_ids(tables):
    """Give selected captures stable keys without changing publisher identifiers.

    Source paths may cite another document in the same snapshot. Match that
    document by its ordered capture selection, rather than dropping the link
    or treating every fresh capture ID as the same document.
    """
    result = {}
    for snapshot in tables.get("scorecard_snapshots", []):
        selected = json.loads(snapshot.get("capture_ids_json", "[]"))
        if (
            not isinstance(selected, list)
            or any(not isinstance(identity, str) or not identity for identity in selected)
            or len(set(selected)) != len(selected)
        ):
            raise ScorecardReplayError("Qualified snapshot capture selection is malformed")
        for ordinal, identity in enumerate(selected):
            stable = (snapshot["scorecard_id"], ordinal)
            if identity in result and result[identity] != stable:
                raise ScorecardReplayError("Qualified reference repeats a capture in different snapshot positions")
            result[identity] = stable
    return result


def check_reference(actual, reference):
    if set(actual) != set(reference):
        raise ScorecardReplayError("Qualified reference does not contain the complete source table family")
    actual_ids, reference_ids = _capture_locator_ids(actual), _capture_locator_ids(reference)
    for name in actual:
        if canonical(actual[name], observation_ids=True, capture_ids=actual_ids) != canonical(
            reference[name], observation_ids=True, capture_ids=reference_ids
        ):
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
        try:
            old = source_row_index(name, (row for row in prior[name] if row[column] not in replaced))
            counts[name] = len(old)
            for row in current[name]:
                if row[column] in replaced:
                    continue
                key = tuple(row[field] for field in IDENTITIES[name])
                expected = old.pop(key, None)
                if expected is None or canonical([expected]) != canonical([row]):
                    raise ValueError("Changed, additional or duplicated row")
            if old:
                raise ValueError("Missing row")
            old.clear()
        except (ValueError, KeyError, TypeError) as error:
            raise ScorecardReplayError("Unselected source rows changed: " + name) from error
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
                if entry.get("reader_inputs") or entry.get("observations_file"):
                    if retain_observations is None:
                        raise ScorecardReplayError("Qualified semantic readers require private observation retention")
                constructor = qualified_reader_class(adapter, entry["reader_class"], entry=entry)
                if entry.get("reader_inputs"):
                    adapter = constructor(
                        **qualified_reader_inputs(entry, self.path), retain_observations=retain_observations
                    )
                elif entry.get("observations_file"):
                    asset = pinned_bytes(self.path(entry["observations_file"]), entry["observations_sha256"])
                    adapter = constructor(asset, retain_observations)
                else:
                    adapter = constructor()
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
                reference = qualified_reference(entry, batch.path)
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
