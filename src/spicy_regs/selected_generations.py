"""One immutable native subject/receipt selection for local and published builds."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import file_digest
import json
import os
from pathlib import Path
from uuid import uuid4

from spicy_regs.sources import publication


def _pin(path):
    with Path(path).open("rb") as stream:
        return "sha256:" + file_digest(stream, "sha256").hexdigest()


@dataclass(frozen=True)
class SelectedDataset:
    dataset: str
    subjects: tuple[Path, ...]
    receipts: Path
    generation_id: str


def unique_build_directory(root: Path) -> Path:
    directory = Path(root) / ".builds" / uuid4().hex
    directory.mkdir(parents=True)
    return directory


def remember_selection(root: Path, selections) -> None:
    """Pin immutable admitted members; mutable public convenience copies are never priors."""
    directory = Path(root) / ".native-state"
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / "selection.json"
    values = json.loads(target.read_text()) if target.exists() else {}
    for selected in selections:

        def member(path):
            return {"path": str(Path(path).resolve()), "sha256": _pin(path)}

        values[selected.dataset] = {
            "generation_id": selected.generation_id,
            "subjects": [member(p) for p in selected.subjects],
            "receipts": member(selected.receipts),
        }
    temporary = target.with_name(target.name + "." + uuid4().hex)
    temporary.write_text(json.dumps(values))
    temporary.replace(target)


class SelectedInputs:
    """A captured remote index is authoritative; otherwise select pinned local native data.

    Pass public_url="" for an explicitly local run, even with environment credentials.
    Missing native receipts refuse: operational readers never migrate old tables.
    """

    def __init__(self, root, directory, *, index=None, public_url=None):
        self.root, self.directory = Path(root), Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.public_url = os.getenv("R2_PUBLIC_URL") if public_url is None else public_url
        self.index = (
            index if index is not None else (publication.current_index(self.public_url) if self.public_url else None)
        )
        pointer = self.root / ".native-state" / "selection.json"
        self.local = json.loads(pointer.read_text()) if self.index is None and pointer.exists() else {}
        self.cache = {}

    def select(self, dataset):
        if dataset in self.cache:
            return self.cache[dataset]
        if self.index is None:
            value = self.local.get(dataset)
            if value is None:
                return None

            def checked(member):
                path = Path(member["path"])
                if _pin(path) != member["sha256"]:
                    raise ValueError("Selected local native member changed")
                return path

            result = SelectedDataset(
                dataset,
                tuple(checked(p) for p in value["subjects"]),
                checked(value["receipts"]),
                value["generation_id"],
            )
        else:
            owner = publication.table_owner(self.index, dataset + ".parquet")
            owners = [
                f for f in self.index["families"].values() if dataset in f.get("etlReceipts", {}).get("datasets", ())
            ]
            if not owners and owner is None:
                return None
            if len(owners) != 1:
                raise ValueError(f"{dataset}: selected native input requires one ETL receipt generation")
            if not self.public_url:
                raise ValueError("Selected remote inputs require their public base URL")
            public_url = self.public_url
            directory = self.directory / dataset
            directory.mkdir(parents=True, exist_ok=True)

            def fetch(member):
                target = directory / member.key
                target.parent.mkdir(parents=True, exist_ok=True)
                if not publication.fetch_member(public_url, member, target, member.path):
                    raise ValueError(f"{dataset}: selected native member unavailable")
                return target

            subjects = (
                tuple(fetch(m) for m in publication.table_members(self.index, dataset + ".parquet")) if owner else ()
            )
            receipts = publication.receipt_members(self.index, dataset=dataset)
            if len(receipts) != 1:
                raise ValueError(f"{dataset}: ambiguous selected receipt member")
            result = SelectedDataset(dataset, subjects, fetch(receipts[0]), owners[0]["etlReceipts"]["generationId"])
        self.cache[dataset] = result
        return result


def remember_generation(root: Path, directory: Path, artifact) -> None:
    """Select every dataset from one already admitted immutable generation."""
    from rulespec_artifacts import LocalMemberSource, iter_member_descriptors
    from spicy_regs.sources.publication import member_table

    specification = artifact.root["spec"].get("etlReceipts")
    if specification is None:
        return
    members = list(iter_member_descriptors(artifact, LocalMemberSource(directory)))
    selected = []
    for policy in specification["policies"]:
        dataset = policy["dataset"]
        subjects = []
        for member in members:
            key = member.object_key
            if key is None:
                raise ValueError("Native generation member has no object key")
            if key != specification["key"] and member_table(key) == dataset + ".parquet":
                subjects.append(directory / key)
        selected.append(
            SelectedDataset(dataset, tuple(subjects), directory / specification["key"], specification["generationId"])
        )
    remember_selection(root, selected)
