"""One immutable native subject/receipt selection for local and published builds."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
from uuid import uuid4

from spicy_regs.sources import publication
from spicy_regs.runtime_bounds import stream_sha256


def _pin(path):
    with Path(path).open("rb") as stream:
        return "sha256:" + stream_sha256(stream)


def _member_pin(path, checked):
    """Hash unique regular bytes once within this call/session; refuse concurrent mutation."""
    from spicy_regs.local_data import file_signature

    path = Path(path).absolute()
    before = file_signature(path)
    held = checked.get(path)
    if held is not None:
        if held[0] != before:
            raise ValueError("Selected native member changed during its read session")
        return held[1]
    digest = _pin(path)
    if file_signature(path) != before:
        raise ValueError("Selected native member changed while hashing")
    checked[path] = (before, digest)
    return digest


@dataclass(frozen=True)
class SelectedDataset:
    dataset: str
    subjects: tuple[Path, ...]
    receipts: Path
    generation_id: str
    key_index: Path | None = None
    key_index_descriptor: dict | None = None


def unique_build_directory(root: Path) -> Path:
    directory = Path(root) / ".builds" / uuid4().hex
    directory.mkdir(parents=True)
    return directory


def remember_selection(root: Path, selections) -> None:
    """Pin immutable admitted members; mutable public convenience copies are never priors."""
    _remember_selection(root, selections, {})


def _remember_selection(root, selections, checked):
    directory = Path(root) / ".native-state"
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / "selection.json"
    values = json.loads(target.read_text()) if target.exists() else {}
    for selected in selections:

        def member(path):
            return {"path": str(Path(path).resolve()), "sha256": _member_pin(path, checked)}

        values[selected.dataset] = {
            "generation_id": selected.generation_id,
            "subjects": [member(p) for p in selected.subjects],
            "receipts": member(selected.receipts),
            **({"key_index": member(selected.key_index), "key_index_descriptor": selected.key_index_descriptor}
               if selected.key_index is not None else {}),
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
        self.checked = {}
        self.fetched = {}

    def member_pin(self, path):
        """Return a byte pin from this captured session, checking that its bytes remain unchanged."""
        digest = _member_pin(path, self.checked)
        signature, _ = self.checked[Path(path).absolute()]
        return {"sha256": digest, "byteSize": signature[2]}

    def select(self, dataset):
        if dataset in self.cache:
            selected = self.cache[dataset]
            for path in (*selected.subjects, selected.receipts,
                         *((selected.key_index,) if selected.key_index is not None else ())):
                _member_pin(path, self.checked)
            return self.cache[dataset]
        if self.index is None:
            value = self.local.get(dataset)
            if value is None:
                return None

            def checked(member):
                path = Path(member["path"])
                if _member_pin(path, self.checked) != member["sha256"]:
                    raise ValueError("Selected local native member changed")
                return path

            result = SelectedDataset(
                dataset,
                tuple(checked(p) for p in value["subjects"]),
                checked(value["receipts"]),
                value["generation_id"],
                checked(value["key_index"]) if "key_index" in value else None,
                value.get("key_index_descriptor"),
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
                from rulespec_artifacts import validate_object_key
                from spicy_regs.local_data import file_signature

                identity = (member.path, member.sha256, member.byte_size)
                held = self.fetched.get(identity)
                if held is not None:
                    target, signature = held
                    if file_signature(target) != signature:
                        raise ValueError("Selected remote native member changed during its read session")
                    return target
                # All datasets of one captured generation share its receipt member.
                # Its immutable object path preserves both family and generation.
                key = validate_object_key(member.path, path="selectedMember")
                target = self.directory / ".members" / key
                target.parent.mkdir(parents=True, exist_ok=True)
                downloaded = publication.fetch_member(public_url, member, target, member.path)
                if not downloaded:
                    raise ValueError(f"{dataset}: selected native member unavailable")
                if isinstance(downloaded, publication.DownloadedMember):
                    from spicy_regs.local_data import file_state_from_stat
                    if file_state_from_stat(target.lstat()) != downloaded.state:
                        raise ValueError("Selected native member changed after download verification")
                    digest = downloaded.sha256
                else:
                    # Injected fetch adapters must prove their final bytes too.
                    digest = _member_pin(target, self.checked)
                if digest != member.sha256:
                    raise ValueError("Selected remote native member differs from its pin")
                signature = file_signature(target)
                if isinstance(downloaded, publication.DownloadedMember):
                    if file_state_from_stat(target.lstat()) != downloaded.state:
                        raise ValueError("Selected native member changed during verification handoff")
                elif signature != self.checked[target.absolute()][0]:
                    raise ValueError("Selected native member changed during adapter verification handoff")
                if signature[2] != member.byte_size:
                    raise ValueError("Selected remote native member differs from its byte size pin")
                self.fetched[identity] = (target, signature)
                self.checked[target.absolute()] = (signature, digest)
                return target

            subjects = (
                tuple(fetch(m) for m in publication.table_members(self.index, dataset + ".parquet")) if owner else ()
            )
            receipts = publication.receipt_members(self.index, dataset=dataset)
            if len(receipts) != 1:
                raise ValueError(f"{dataset}: ambiguous selected receipt member")
            specification = owners[0]["etlReceipts"]
            indexes = publication.receipt_key_members(self.index, dataset=dataset)
            result = SelectedDataset(dataset, subjects, fetch(receipts[0]), specification["generationId"],
                                     fetch(indexes[0]) if indexes else None, specification.get("keyIndex"))
        if (result.key_index is None) != (result.key_index_descriptor is None):
            raise ValueError("Selected receipt key index declaration is incomplete")
        if result.key_index is not None:
            import duckdb
            import pyarrow.parquet as pq
            assert result.key_index_descriptor is not None
            from spicy_regs.receipt_key_index import check_reader
            with duckdb.connect() as con:
                check_reader(con, str(result.key_index), result.key_index_descriptor,
                             {"sha256": _member_pin(result.receipts, self.checked), "byteSize": result.receipts.stat().st_size,
                              "rows": pq.ParquetFile(result.receipts).metadata.num_rows})
        self.cache[dataset] = result
        return result


def remember_generation(root: Path, directory: Path, artifact) -> None:
    """Select every dataset from one already admitted immutable generation."""
    from rulespec_artifacts import LocalFileState, LocalMemberSource, iter_member_descriptors
    from spicy_regs.local_data import file_signature
    from spicy_regs.sources.publication import member_table

    specification = artifact.root["spec"].get("etlReceipts")
    if specification is None:
        return
    members = list(iter_member_descriptors(artifact, LocalMemberSource(directory)))
    selected = []
    checked = {}
    for member in members:
        if member.object_key is None:
            raise ValueError("Native generation member has no object key")
        path = directory / member.object_key
        state = (artifact.local_member_states or {}).get(member.object_key)
        if state is not None:
            signature = file_signature(path)
            if LocalFileState.from_stat(path.lstat()) != state:
                raise ValueError("Native generation changed after byte admission")
            checked[path.absolute()] = (signature, member.sha256)
    for policy in specification["policies"]:
        dataset = policy["dataset"]
        subjects = []
        for member in members:
            key = member.object_key
            if key is None:
                raise ValueError("Native generation member has no object key")
            if key not in {specification["key"], specification.get("keyIndex", {}).get("key")} and member_table(key) == dataset + ".parquet":
                subjects.append(directory / key)
        selected.append(
            SelectedDataset(dataset, tuple(subjects), directory / specification["key"], specification["generationId"],
                            directory / specification["keyIndex"]["key"] if "keyIndex" in specification else None,
                            specification.get("keyIndex"))
        )
    _remember_selection(root, selected, checked)
