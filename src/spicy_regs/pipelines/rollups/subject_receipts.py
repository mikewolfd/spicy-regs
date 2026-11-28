"""Scheduled subject writers with exact, selected-generation processing priors.

Only private working files restore processing fields. The returned outputs are
native subjects and one generation-bound receipt member; no public mirrors.
"""

from __future__ import annotations

import inspect
import os
import json
import shutil
from pathlib import Path
from typing import ClassVar

import pyarrow.parquet as pq

from spicy_regs import etl_bulk
from spicy_regs.congress_subjects import INPUT_COLUMNS as CONGRESS
from spicy_regs.congress_receipts import (
    ACQUISITION_POLICY,
    CongressInput,
    _build_events,
    policy as congress_policy,
    restore_processing_input,
    write_congress_dataset,
    has_unmatched_failures,
)
from spicy_regs.etl_receipts import (
    combine_receipts, read_with_receipts, select_pinned_receipts, validate_receipt_bundle, selected_subject_policy,
)
from spicy_regs.legislative_documents import field_registry
from spicy_regs.legislative_receipts import (
    FILE_POLICY,
    FILE_STATES,
    write_legislative_outputs,
    policy as legislative_policy,
    input_policy as legislative_input_policy,
    restore_prior,
)
from spicy_regs.native_types import described_schema
from spicy_regs.phase_log import phase_log
from spicy_regs.policy_migrations import REPLAYS, PriorReplayRequired, dropped_replay_rows, replay_reason
from spicy_regs.navigation_read_outcomes import (
    POLICIES as NAVIGATION_OUTCOME_POLICIES, READ_TABLES, FILE_TABLES, family_tables, write_recorded_outcomes,
)
from spicy_regs.pipelines.rollups.base import RollupPipeline
from spicy_regs.transforms.regulations_shape import SOURCE_COLUMNS as REGULATIONS
from spicy_regs.transforms.fec_identity_receipts import dataset_policy as fec_policy
from spicy_regs.transforms.fec_identity_context_fields import REGISTRY as FEC
from spicy_regs.transforms.regulations_receipts import (
    BATCH_RESTORE_DATASETS,
    ReceiptInput,
    materialize_internal,
    policy as regulations_policy,
    write_held_dataset,
)


def dataset_policy(name):
    if name in CONGRESS:
        return congress_policy(name)
    if name in field_registry():
        return legislative_policy(name)
    if name in REGULATIONS:
        return regulations_policy(name)
    if name in FEC:
        return fec_policy(name)
    raise ValueError(f"No coordinated processing reader for {name}")


class SelectedPriors:
    """Capture one publication index and fetch only its pinned subject/receipt pairs."""

    def __init__(self, directory, *, index=None, public_url=None, root=None, ranged=frozenset()):
        """``ranged`` names the Congress datasets whose receipts may be read by row group (``SelectedInputs.select``):
        another family's soft inputs, each read for its own rows. A family's own priors read many datasets of one
        member, so they fetch it whole once, verify its SHA-256, and then read each dataset's row groups alone."""
        from spicy_regs.selected_generations import SelectedInputs

        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.selected = SelectedInputs(
            root or self.directory, self.directory / "native", index=index, public_url=public_url
        )
        from spicy_regs.congress_subjects import PROMOTED
        from spicy_regs.subject_catalog import retired_receipt_datasets

        #: Retired Congress tables with fields their receipts alone held: while their live generation still lists
        #: those receipts (its first receipts-free night), the prior is restored through them, by row group, which
        #: back-fills the promoted columns (``congress_tables``).
        self.last_receipts = frozenset(PROMOTED) & retired_receipt_datasets()
        self.ranged = (frozenset(ranged) | self.last_receipts) & set(CONGRESS)
        self.restored = {}
        self.selections = {}
        self.scoped = {}
        self.processing_members = {}
        #: dataset -> metadata for untouched historical partitions carried directly in native form.
        self.carried_partitions = {}
        #: dataset -> (subjects, scoped receipts, generation, policy version, reason) of a replay-required prior.
        self.replaced = {}

    def select(self, dataset):
        """The selection of ``dataset``, read as this reader reads it (:attr:`ranged`, :attr:`last_receipts`)."""
        from spicy_regs.subject_catalog import retired_receipt_datasets

        if dataset in READ_TABLES and dataset in retired_receipt_datasets():
            return self.selected.select(READ_TABLES[dataset])
        return self.selected.select(dataset, ranged=dataset in self.ranged, last_receipts=dataset in self.last_receipts)

    def get(self, dataset):
        if dataset in self.carried_partitions:
            raise ValueError(f"{dataset}: partial partition restore already selected; cannot serve full dataset")
        if dataset in self.restored:
            return self._get(dataset)
        with phase_log("prior-restore", dataset=dataset):
            return self._get(dataset)

    def _get(self, dataset):
        if dataset in self.restored:
            if self.restored[dataset] is not None:
                self.select(dataset)
                self._check_processing(dataset)
            return self.restored[dataset]
        selected_policy = dataset_policy(dataset)
        with phase_log("prior-select", dataset=dataset):
            selection = self.select(dataset)
        if selection is None:
            self.restored[dataset] = None
            return None
        subjects, receipt_path, generation = list(selection.subjects), selection.receipts, selection.generation_id
        if receipt_path is None:
            return self._published(dataset, subjects)
        if dataset in field_registry():
            selected_policy = legislative_input_policy(dataset, subjects)
        directory = self.directory / dataset
        directory.mkdir()
        # A ranged read already wrote exactly this dataset's receipts, in source order, through the same selector.
        # A whole member is pinned, so each dataset reads its own row groups, not all of a family's receipts.
        with phase_log("prior-scope", dataset=dataset, ranged=dataset in self.selected.ranged):
            scoped = (receipt_path if dataset in self.selected.ranged
                      else select_pinned_receipts(receipt_path, directory / "scoped.parquet", dataset=dataset))
        selected_policy = selected_subject_policy(selected_policy, subjects)
        if (reason := replay_reason(selected_policy)) is not None:
            # Never restored or carried: the builder rebuilds these rows from originals, and the rollup retains
            # this exact pair as evidence first (``retain_replaced_priors``).
            self.replaced[dataset] = (tuple(subjects), scoped, generation, selected_policy.policy_version, reason)
            self.restored[dataset] = None
            return None
        # The regulatory restore below admits the pair itself: complete admission in
        # regulations_bulk._materialize_selected, or the full read_with_receipts pass in _qualified_rows.
        if dataset in CONGRESS or dataset in FEC:
            with phase_log("prior-admit", dataset=dataset):
                validate_receipt_bundle({dataset: subjects}, [scoped], [selected_policy], generation_id=generation)
        self.selections[dataset] = (subjects, receipt_path, generation)
        self.scoped[dataset] = scoped
        output = directory / "processing" / (dataset + ".parquet")
        if dataset in CONGRESS:
            output.parent.mkdir(parents=True, exist_ok=True)
            with phase_log("prior-restore-input", dataset=dataset):
                restore_processing_input(
                    tuple(subjects), scoped, output, dataset=dataset, generation_id=generation,
                    bulk=True, validate=False, scoped=True,  # this exact pair was admitted just above
                )
        elif dataset in field_registry():
            with phase_log("prior-scope", dataset=FILE_STATES, owner=dataset):
                files = select_pinned_receipts(receipt_path, directory / "file-receipts.parquet", dataset=FILE_STATES)
            bundle = directory / "legislative"
            bundle.mkdir()
            combine_receipts([scoped, files], bundle / "etl_receipts.parquet")
            relative = []
            for i, subject in enumerate(subjects):
                target = bundle / f"subject-{i}.parquet"
                shutil.copyfile(subject, target)
                relative.append(target.name)
            manifest = dict(
                generation_id=generation,
                datasets=[dataset],
                subjects={dataset: relative},
                receipt_file="etl_receipts.parquet",
                unowned_outputs=[],
                refused_rows={},
            )
            (bundle / "legislative-bundle.json").write_text(json.dumps(manifest))
            with phase_log("prior-restore-input", dataset=dataset):
                output = restore_prior(bundle, directory / "processing")[dataset]
        elif dataset == "fec_committees":
            from spicy_regs.transforms.build_fec_committees import COLUMNS, _SCHEMA
            from spicy_regs.transforms.parquet_rows import write_rows

            def original_rows():
                for row in read_with_receipts(subjects, [scoped], selected_policy, generation_id=generation):
                    original = row["conversion_inputs"]
                    yield {name: original[name] for name in COLUMNS}

            output.parent.mkdir(parents=True, exist_ok=True)
            write_rows(original_rows(), output, _SCHEMA)
        elif dataset == "fec_committee_history":
            import pyarrow as pa
            from spicy_regs.transforms.parquet_rows import write_rows

            # Its builder writes these columns as text and each is a subject column. The mapper converts one of them
            # (cycle) and keeps that original in the receipt, so the rows org-committee-links reads restore exactly.
            columns = FEC[dataset]["input_fields"]

            def stated_rows():
                for row in read_with_receipts(subjects, [scoped], selected_policy, generation_id=generation):
                    originals = row["conversion_inputs"]
                    yield {name: originals.get(name, row[name]) for name in columns}

            output.parent.mkdir(parents=True, exist_ok=True)
            write_rows(stated_rows(), output, pa.schema([(name, pa.string()) for name in columns]))
        elif dataset in FEC:
            raise ValueError(f"No processing reconstruction declared for {dataset}")
        else:
            output.parent.mkdir(parents=True, exist_ok=True)
            with phase_log("prior-restore-input", dataset=dataset):
                materialize_internal(
                    ReceiptInput(dataset, tuple(subjects), scoped, generation), output,
                    bulk=dataset in BATCH_RESTORE_DATASETS, scoped=True,
                )
        self.restored[dataset] = output
        members = tuple(sorted(output.rglob("*.parquet"))) if output.is_dir() else (output,)
        self.processing_members[dataset] = members
        self._check_processing(dataset)
        return output

    def _published(self, dataset, subjects):
        """A retired dataset's prior: its pinned public table, as its mapper's builders read their input.

        The regulations mapper (``regulations_shape.published_as_source``), the Congress mapper
        (``congress_tables.as_builder_input``, for its ``READABLE`` tables), the FEC identity mapper
        (``fec_tables.published_as_fec_source``, for its ``FEC_PUBLISHED_READABLE`` tables), and the legislative mapper
        (``legislative_tables.as_builder_input``, for its ``READABLE`` tables) read a published table back into
        their builders' shape; any other dataset refuses by name rather than hand a builder a table it would
        misread. Its consumers then wait for such a reader here, or switch first.
        """
        from spicy_regs import legislative_tables
        from spicy_regs.congress_tables import READABLE, as_builder_input
        from spicy_regs.pipelines.rollups.fec_tables import FEC_PUBLISHED_READABLE, published_as_fec_source
        from spicy_regs.transforms.regulations_shape import SOURCE_COLUMNS, published_as_source

        if (
            dataset not in SOURCE_COLUMNS
            and dataset not in READABLE
            and dataset not in FEC_PUBLISHED_READABLE
            and dataset not in legislative_tables.READABLE
        ):
            raise ValueError(f"{dataset}: its receipts are retired and its mapper has no reader of the published "
                             "table (SelectedPriors._published); add one before its consumers read it")
        output = self.directory / dataset / "processing" / (dataset + ".parquet")
        output.parent.mkdir(parents=True)
        with phase_log("prior-published-input", dataset=dataset):
            if dataset in READABLE:
                as_builder_input(dataset, subjects, output)
            elif dataset in FEC_PUBLISHED_READABLE:
                published_as_fec_source(dataset, subjects, output)
            elif dataset in legislative_tables.READABLE:
                file_outcomes: tuple[Path, ...] = ()
                if (
                    subjects
                    and (file_table := legislative_tables.FILE_TABLE_FOR_DATASET.get(dataset)) is not None
                    and not legislative_tables.has_checkpoint_metadata(subjects[0])
                ):
                    outcome_selection = self.selected.select(file_table)
                    if outcome_selection is not None:
                        file_outcomes = outcome_selection.subjects
                legislative_tables.as_builder_input(dataset, subjects, output, file_outcomes=file_outcomes)
            else:
                published_as_source(dataset, subjects, output)
        self.selections[dataset] = (subjects, None, None)
        self.restored[dataset] = output
        self.processing_members[dataset] = (output,)
        self._check_processing(dataset)
        return output

    def _check_processing(self, dataset):
        output = self.restored[dataset]
        members = tuple(sorted(output.rglob("*.parquet"))) if output.is_dir() else (output,)
        if output.is_symlink() or members != self.processing_members[dataset]:
            raise ValueError("Selected restored processing member set changed")
        for path in members:
            self.selected.member_pin(path)

    def _required(self, dataset):
        source = self.get(dataset)
        if dataset in self.replaced:
            _, _, _, version, reason = self.replaced[dataset]
            raise PriorReplayRequired(dataset, version, reason)
        return source

    def _partition_carry_spec(self, dataset, subjects, receipt_path):
        """Return ``(partition_column, by_subject)`` when untouched partitions may be carried forward directly."""
        if receipt_path is None or dataset not in field_registry():
            return None
        current_policy = legislative_policy(dataset)
        if current_policy.identity_fields[:1] != ("bill_id",):
            return None
        selected_policy = selected_subject_policy(legislative_input_policy(dataset, subjects), subjects)
        if replay_reason(selected_policy) is not None or selected_policy != current_policy:
            return None
        columns, by_subject = set(), []
        for subject in subjects:
            column, separator, value = subject.parent.name.partition("=")
            if subject.parent.parent.name != dataset or not separator or column != "congress" or not value:
                return None
            if not pq.read_schema(subject).equals(current_policy.subject_schema):
                return None
            columns.add(column)
            by_subject.append((subject, value))
        return ("congress", by_subject) if len(columns) == 1 and by_subject else None

    def _get_partitions(self, dataset, partitions):
        """Restore only ``partitions`` of ``dataset``, leaving untouched partitions in native form."""
        partitions = frozenset(str(value) for value in partitions)
        if dataset in self.restored:
            carried = self.carried_partitions.get(dataset)
            if carried is None or carried["restore_partitions"] != partitions:
                raise ValueError(f"{dataset}: inconsistent restored partition selection")
            if self.restored[dataset] is not None:
                self.select(dataset)
                self._check_processing(dataset)
            return self.restored[dataset]
        with phase_log("prior-restore", dataset=dataset, partitions=len(partitions)):
            with phase_log("prior-select", dataset=dataset):
                selection = self.select(dataset)
            if selection is None:
                self.restored[dataset] = None
                return None
            subjects, receipt_path, generation = list(selection.subjects), selection.receipts, selection.generation_id
            spec = self._partition_carry_spec(dataset, subjects, receipt_path)
            if spec is None:
                return self._get(dataset)
            partition_column, by_subject = spec
            restore_subjects = [subject for subject, value in by_subject if value in partitions]
            carried_pairs = [(subject, value) for subject, value in by_subject if value not in partitions]
            if not carried_pairs:
                return self._get(dataset)
            import pyarrow as pa
            import pyarrow.compute as pc
            from typing import Any
            from spicy_regs.etl_receipts import decode_exact_json, open_receipt_writer
            from spicy_regs.legislative_receipts import _carried_bill_id_regex

            compute: Any = pc

            directory = self.directory / dataset
            directory.mkdir()
            with phase_log("prior-scope", dataset=dataset, ranged=dataset in self.selected.ranged):
                scoped = (receipt_path if dataset in self.selected.ranged
                          else select_pinned_receipts(receipt_path, directory / "scoped.parquet", dataset=dataset))
            with phase_log("prior-scope", dataset=FILE_STATES, owner=dataset):
                files = select_pinned_receipts(receipt_path, directory / "file-receipts.parquet", dataset=FILE_STATES)

            carried_values = frozenset(value for _, value in carried_pairs)
            carried_pattern = _carried_bill_id_regex(carried_values)
            restore_scoped = directory / "restore-scoped.parquet"
            carried_receipts = directory / "carried-receipts.parquet"
            carried_receipt_count = 0
            with (
                pq.ParquetFile(scoped) as source,
                open_receipt_writer(restore_scoped) as restore_writer,
                open_receipt_writer(carried_receipts) as carried_writer,
            ):
                for batch in source.iter_batches(batch_size=50_000):
                    table = pa.Table.from_batches([batch])
                    accepted = compute.equal(table["outcome"], "accepted")
                    matched = compute.fill_null(
                        compute.match_substring_regex(table["identity_json"], carried_pattern), False
                    )
                    in_carried = compute.and_kleene(accepted, matched)
                    carried_rows = table.filter(in_carried)
                    restore_rows = table.filter(compute.invert(in_carried))
                    if carried_rows.num_rows:
                        carried_receipt_count += carried_rows.num_rows
                        carried_writer.write_table(carried_rows)
                    if restore_rows.num_rows:
                        restore_writer.write_table(restore_rows)

            expected_carried_rows = sum(pq.read_metadata(subject).num_rows for subject, _ in carried_pairs)
            if carried_receipt_count != expected_carried_rows:
                raise ValueError(
                    f"{dataset}: carried partition rows ({expected_carried_rows}) differ from receipts ({carried_receipt_count})"
                )

            restore_files = directory / "restore-file-receipts.parquet"
            carried_file_states: dict[str, dict] = {}
            with pq.ParquetFile(files) as source, open_receipt_writer(restore_files) as restore_file_writer:
                for batch in source.iter_batches(batch_size=2000):
                    keep_indices = []
                    rows_list = batch.to_pylist()
                    for index, receipt in enumerate(rows_list):
                        state = decode_exact_json(receipt["processing_json"])["file_state"]
                        if state["dataset"] != dataset:
                            continue
                        relative = state["relative_path"]
                        if relative is None:
                            raise ValueError(f"{dataset}: cannot carry partitions over an empty-state file receipt")
                        part_col, sep, part_val = Path(relative).parts[0].partition("=")
                        if not sep or part_col != partition_column:
                            raise ValueError(f"{dataset}: unexpected retained file state path {relative!r}")
                        if part_val in carried_values:
                            if relative in carried_file_states:
                                raise ValueError(f"{dataset}: duplicate carried file state {relative!r}")
                            carried_file_states[relative] = {
                                "relative_path": relative,
                                "state": state,
                                "processor": receipt["processor"],
                                "witnesses": receipt["witnesses"],
                            }
                        else:
                            keep_indices.append(index)
                    if keep_indices:
                        restore_file_writer.write_table(pa.Table.from_batches([batch]).take(keep_indices))

            carried_subjects = [subject for subject, _ in carried_pairs]
            expected_carried_files = {
                f"{subject.parent.name}/{subject.name}": pq.read_metadata(subject).num_rows
                for subject in carried_subjects
            }
            actual_carried_files = {rel: info["state"]["rows"] for rel, info in carried_file_states.items()}
            if actual_carried_files != expected_carried_files:
                raise ValueError(f"{dataset}: carried file states differ from carried native partition members")

            self.selections[dataset] = (subjects, receipt_path, generation)
            self.scoped[dataset] = scoped
            output = directory / "processing" / dataset
            if restore_subjects:
                bundle = directory / "legislative"
                bundle.mkdir()
                combine_receipts([restore_scoped, restore_files], bundle / "etl_receipts.parquet")
                relative = []
                for i, subject in enumerate(restore_subjects):
                    target = bundle / f"subject-{i}.parquet"
                    shutil.copyfile(subject, target)
                    relative.append(target.name)
                manifest = dict(
                    generation_id=generation,
                    datasets=[dataset],
                    subjects={dataset: relative},
                    receipt_file="etl_receipts.parquet",
                    unowned_outputs=[],
                    refused_rows={},
                )
                (bundle / "legislative-bundle.json").write_text(json.dumps(manifest))
                with phase_log("prior-restore-input", dataset=dataset):
                    output = restore_prior(bundle, directory / "processing")[dataset]
            else:
                if pq.read_metadata(restore_scoped).num_rows or pq.read_metadata(restore_files).num_rows:
                    raise ValueError(f"{dataset}: prior has active-partition receipts without subject members")
                output.mkdir(parents=True, exist_ok=True)
            self.restored[dataset] = output
            self.processing_members[dataset] = tuple(sorted(output.rglob("*.parquet")))
            self.carried_partitions[dataset] = {
                "subjects": tuple(carried_subjects),
                "receipts": carried_receipts,
                "file_states": [
                    carried_file_states[f"{subject.parent.name}/{subject.name}"] for subject in carried_subjects
                ],
                "partitions": carried_values,
                "restore_partitions": partitions,
                "partition_column": partition_column,
            }
            self._check_processing(dataset)
            return output

    def download(self, key, target):
        source = self._required(key.removesuffix(".parquet"))
        if source is None:
            return False
        if source.is_dir():
            raise ValueError("Partitioned prior requested as one file")
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        return True

    def input_provenance(self, dataset):
        """Bind a restored processing input to the selected native subjects and receipts."""
        from spicy_regs.sources import publication, r2

        source = self.get(dataset)
        if source is None:
            return None
        subjects, receipts, generation = self.selections[dataset]

        def pin(path):
            return self.selected.member_pin(path)

        processing = ({"members": [{"key": path.relative_to(source).as_posix(), **pin(path)}
                                    for path in self.processing_members[dataset]]}
                      if source.is_dir() else pin(source))
        binding = {"dataset": dataset, "generationId": generation,
                   "subjects": [pin(path) for path in subjects],
                   "receipts": None if receipts is None else self.selected.receipts_pin(dataset, receipts),
                   "processing": processing}
        if verification := self.selected.receipts_verification(dataset):
            # The parent pin below stays the index's {sha256, byteSize}, the only shape a generation parent admits;
            # this record says that sha256 was not checked against the bytes read.
            binding["receiptsVerified"] = verification
        index = self.selected.index
        key = dataset + ".parquet"
        published = publication.table_pin(index, key) if index is not None and publication.table_owner(index, key) else None
        binding["publication"] = published
        binding["publicationDisposition"] = (
            "local-selection" if index is None else "captured-publication" if published else "no-published-subject"
        )
        if published is not None:
            r2.note_pinned_read(key, published)
        else:
            for ordinal, subject in enumerate(binding["subjects"]):
                r2.note_pinned_read(f"{dataset}/subjects/{ordinal}.parquet", subject)
        if binding["receipts"] is not None:
            r2.note_pinned_read(f"{dataset}/etl_receipts.parquet", binding["receipts"])
        return binding

    def download_members(self, key, target, *, partitions=None):
        dataset = key.removesuffix(".parquet")
        if partitions is not None:
            source = self._get_partitions(dataset, partitions)
            if dataset in self.replaced:
                _, _, _, version, reason = self.replaced[dataset]
                raise PriorReplayRequired(dataset, version, reason)
        else:
            source = self._required(dataset)
        if source is None:
            return ()
        members = sorted(source.rglob("*.parquet")) if source.is_dir() else [source]
        result = []
        for member in members:
            relative = member.relative_to(source) if source.is_dir() else Path(member.name)
            destination = target / source.name / relative if source.is_dir() else target / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(member, destination)
            result.append(destination)
        return tuple(result)


class NativeReceiptLifecycle(RollupPipeline):
    """Prime and build from the same captured native inputs, never convenience copies."""

    output_dir: Path | None
    inputs: ClassVar[tuple[str, ...]]

    def _new_receipt_work(self, output_dir, snapshot=None):
        from spicy_regs.selected_generations import unique_build_directory

        private = unique_build_directory(output_dir)
        prior = SelectedPriors(
            private / "selected-priors",
            root=self.output_dir or output_dir,
            index=snapshot if os.getenv("R2_PUBLIC_URL") else None,
            # Another family's soft inputs (votes' bill_vote_references); never this family's own priors.
            ranged=frozenset(key.removesuffix(".parquet") for key in self.soft_inputs
                             if key not in (self.outputs or (self.output,))),
        )
        return private, prior

    def _receipt_work(self, output_dir):
        selected = getattr(self, "_primed_receipt_work", None)
        self._primed_receipt_work = None
        if selected is not None:
            directory, private, prior = selected
            if directory != output_dir:
                raise ValueError("Primed native inputs belong to a different build directory")
            return private, prior
        return self._new_receipt_work(output_dir)

    def _call_builder(self, prior, work, builder, kwargs):
        """Hand ``builder`` its declared inputs in ``work`` and the selected priors' readers it asks for; run it."""
        for key in self.inputs:
            if not prior.download(key, work / key):
                raise ValueError(f"Required selected processing input unavailable: {key}")
        parameters = inspect.signature(builder).parameters
        if "download_prior" in parameters or any(p.kind == inspect.Parameter.VAR_KEYWORD for p in parameters.values()):
            kwargs["download_prior"] = prior.download
        if "download_members" in parameters:
            kwargs["download_members"] = prior.download_members
        if "selected_input" in parameters:
            kwargs["selected_input"] = prior.input_provenance
        with phase_log("builder", family=self.name):
            return builder(work, **kwargs)

    def _prime(self, output_dir, snapshot=None):
        from spicy_regs.sources import publication

        private, prior = self._new_receipt_work(output_dir, snapshot)
        parents = {}
        for key in self.inputs:
            dataset = key.removesuffix(".parquet")
            if prior.get(dataset) is None:
                raise ValueError(f"Required selected processing input unavailable: {key}")
            selection = prior.select(dataset)
            index = prior.selected.index
            owner = publication.table_owner(index, key) if index is not None else None
            if self._own(key, owner[0] if owner else None):
                continue
            if owner is not None:
                parents[key] = publication.table_pin(index, key)
            else:
                for ordinal, path in enumerate(selection.subjects):
                    parents[f"{dataset}/subjects/{ordinal}.parquet"] = prior.selected.member_pin(path)
            if selection.receipts is not None:
                parents[f"{dataset}/etl_receipts.parquet"] = prior.selected.member_pin(selection.receipts)
        self._primed_receipt_work = output_dir, private, prior
        return parents


class SubjectReceiptRollup(NativeReceiptLifecycle):
    """Congress, legislative and regulatory producers sharing a complete family."""

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        keys = cls.outputs or (cls.output,)
        cls.source_outputs = keys
        if any(not isinstance(key, str) for key in keys):
            raise ValueError("Receipt rollup requires explicit output names")
        names = tuple(str(key).removesuffix(".parquet") for key in keys)
        policies = [dataset_policy(name) for name in names]
        outcomes = family_tables(names)
        outcomes += tuple(READ_TABLES[name] for name in names if name in READ_TABLES)
        family = getattr(cls, "publication_family", None) or cls.name
        if any(name in field_registry() for name in names) and family in FILE_TABLES:
            outcomes += (FILE_TABLES[family],)
        policies.extend(NAVIGATION_OUTCOME_POLICIES[name] for name in outcomes)
        cls.added_tables = tuple(dict.fromkeys((*cls.added_tables, *(name + ".parquet" for name in outcomes))))
        if any(name in field_registry() for name in names):
            policies.append(FILE_POLICY)
        if any(name in CONGRESS for name in names):
            policies.append(ACQUISITION_POLICY)
        cls.receipt_policies = tuple(policies)
        cls.receipt_only_tables = tuple(
            p.dataset + ".parquet" for p in policies if p.receipt_only and p.dataset in names
        )
        visible = tuple(key for key in keys if key not in cls.receipt_only_tables) + tuple(name + ".parquet" for name in outcomes)
        if cls.outputs or outcomes:
            cls.outputs = visible
        elif visible:
            cls.output = visible[0]

    def generation_schemas(self):
        return {p.dataset: described_schema(p.subject_schema) for p in self.receipt_policies if not p.receipt_only}

    def retain_replaced_priors(self, prior, paths):
        """Keep each replay-required prior pair in this run's evidence before the new generation replaces it.

        The builder has rebuilt those rows from originals; the exact selected subject and its scoped receipts
        go into the generation's own evidence (content-addressed blobs, uploaded with it), so nothing selected
        is lost when the pointer moves. Each prior identity the rebuild no longer holds is journaled, and more
        than the replay's stated bound refuses the run.
        """
        if not prior.replaced:
            return
        if self.source_evidence is None:
            raise ValueError("A replay-required prior can be replaced only by a family that retains source evidence")
        for dataset, (subjects, scoped, generation, version, reason) in prior.replaced.items():
            replay = REPLAYS.get(dataset)
            rebuilt = next((path for path in paths if path.stem == dataset and not path.is_dir()), None)
            if replay is None or rebuilt is None:
                raise ValueError(f"Replay-required {dataset} has no registered replay output")
            members = [*((path, "subject") for path in subjects), (scoped, "receipts")]
            for ordinal, (path, role) in enumerate(members):
                self.source_evidence.retain_file(
                    path, stage="replaced-prior", dataset=dataset, role=role, ordinal=ordinal,
                    generation_id=generation, policy_version=version, reason=reason,
                )
            dropped = dropped_replay_rows(dataset, dataset_policy(dataset).identity_fields, subjects, rebuilt)
            self.source_evidence.event("replay-dropped-rows", dataset=dataset, generation_id=generation,
                                       count=len(dropped), bound=replay.max_dropped_rows, identities=dropped)
            if len(dropped) > replay.max_dropped_rows:
                raise ValueError(f"Replay of {dataset} drops {len(dropped):,} prior rows, above its bound of "
                                 f"{replay.max_dropped_rows} ({replay.bound_basis}); see the replay-dropped-rows event")

    def build_receipts(self, output_dir, builder, **kwargs):
        from spicy_regs.selected_generations import remember_selection, SelectedDataset

        private, prior = self._receipt_work(output_dir)
        work = private / "source-output"
        work.mkdir()
        candidate = private / "candidate"
        candidate.mkdir()
        try:
            built = self._call_builder(prior, work, builder, kwargs)
        except Exception as error:
            if any(p.dataset == ACQUISITION_POLICY.dataset for p in self.receipt_policies):
                _build_events(candidate, self.receipt_generation_id, builder.__name__, self.source_evidence, error)
            raise
        paths = built if isinstance(built, tuple) else (built,)
        self.retain_replaced_priors(prior, paths)
        observed = {path.name if path.is_dir() else path.stem for path in paths}
        if observed != {key.removesuffix(".parquet") for key in self.source_outputs}:
            raise ValueError("Producer outputs differ from the complete classified family")
        receipts, subjects, legislative = [], [], []
        fresh_failure_datasets = []
        for path in paths:
            dataset = path.name if path.is_dir() else path.stem
            if dataset in field_registry():
                legislative.append(path)
                continue
            destination = private / "datasets" / dataset
            if dataset in CONGRESS:
                selection = prior.selections.get(dataset)
                # The prior's receipts as scoped when selected, so the writer never rescans the family's member.
                held = None if selection is None else CongressInput(tuple(selection[0]), prior.scoped[dataset],
                                                                    selection[2])
                with phase_log("write-dataset", dataset=dataset, writer="congress"):
                    subject, receipt = write_congress_dataset(
                        path, destination, dataset=dataset, generation_id=self.receipt_generation_id, prior=held,
                        bulk=True, on_fresh_failure=lambda: fresh_failure_datasets.append(dataset),
                        validate=False,  # the prior was admitted when selected; the new pair is admitted with its generation
                    )
            else:
                etl_bulk.log_row_eligibility(
                    "regulatory write", dataset=dataset, rows=pq.ParquetFile(path).metadata.num_rows,
                    reason="bulk not requested by subject receipt rollup",
                )
                with phase_log("write-dataset", dataset=dataset, writer="held"):
                    subject, receipt = write_held_dataset(
                        dataset, path, destination, generation_id=self.receipt_generation_id
                    )
            receipts.append(receipt)
            if subject is not None:
                target = candidate / subject.name
                shutil.copyfile(subject, target)
                subjects.append(target)
        if legislative:
            bundle = private / "legislative-bundle"
            with phase_log("write-legislative", datasets=len(legislative)):
                manifest = write_legislative_outputs(
                    legislative,
                    bundle,
                    generation_id=self.receipt_generation_id,
                    prior_receipts=list(dict.fromkeys(v[1] for v in prior.selections.values() if v[1] is not None)),
                    replayed=frozenset(prior.replaced),
                    file_outcomes_table=FILE_TABLES.get(getattr(self, "publication_family", None) or self.name),
                    carried_partitions=prior.carried_partitions or None,
                    validate=False,  # the completed generation admits these exact bytes once
                )
            if manifest["unowned_outputs"] or any(manifest["fresh_refused_rows"].values()):
                raise ValueError("Legislative output conversion refused")
            receipts.append(bundle / "etl_receipts.parquet")
            for name, members in manifest["subjects"].items():
                if legislative_policy(name).receipt_only:
                    continue
                if (bundle / name).is_dir():
                    target = candidate / name
                    shutil.copytree(bundle / name, target)
                else:
                    target = candidate / (name + ".parquet")
                    shutil.copyfile(bundle / members[0], target)
                subjects.append(target)
            for name, members in manifest.get("outcome_subjects", {}).items():
                target = candidate / (name + ".parquet")
                shutil.copyfile(bundle / members[0], target)
                subjects.append(target)
        if any(p.dataset == ACQUISITION_POLICY.dataset for p in self.receipt_policies):
            with phase_log("write-acquisition-events"):
                receipts.append(
                    _build_events(candidate, self.receipt_generation_id, builder.__name__, self.source_evidence)
                )
        outcome_names = family_tables(tuple(key.removesuffix(".parquet") for key in self.source_outputs))
        if outcome_names:
            journal = None if self.source_evidence is None else self.source_evidence.artifact_dir / "journal.jsonl"
            with phase_log("write-read-outcomes", tables=len(outcome_names)):
                outcome_subjects, outcome_receipts = write_recorded_outcomes(
                    journal, private / "read-outcomes", generation_id=self.receipt_generation_id, tables=outcome_names,
                    priors={name: prior.selected.select(name) for name in outcome_names})
            receipts.extend(outcome_receipts)
            for source in outcome_subjects:
                target = candidate / source.name
                shutil.copyfile(source, target)
                subjects.append(target)
        with phase_log("combine-receipts", shards=len(receipts)):
            combined = combine_receipts(receipts, candidate / "etl_receipts.parquet")
        with phase_log("unmatched-failures"):
            unmatched = has_unmatched_failures(
                combined, {name: [selection[1]] for name, selection in prior.selections.items()
                           if selection[1] is not None})  # a retired parent has no receipts to match
        if fresh_failure_datasets or unmatched:
            raise ValueError("Subject conversion refused; retained receipts require review")
        by_dataset = {p.dataset: [] for p in self.receipt_policies}
        for path in subjects:
            by_dataset[path.name if path.is_dir() else path.stem] = (
                sorted(path.rglob("*.parquet")) if path.is_dir() else [path]
            )
        # ``_defer_native_selection`` is set only by ``_run_tables``, whose ``build_generation`` admits this same pair
        # (every policy, every subject member) before anything is visible or selected. That admission is the one
        # validation; outside a run, ``remember_selection`` below pins "admitted members", so validate here.
        if not getattr(self, "_defer_native_selection", False):
            with phase_log("admission", family=self.name):
                validate_receipt_bundle(by_dataset, [combined], self.receipt_policies,
                                        generation_id=self.receipt_generation_id)
        # Private files are final here; the copies below make them visible.
        with phase_log("visible-copies", files=len(subjects)):
            visible = []
            for source in subjects:
                target = output_dir / source.name
                if source.is_dir():
                    if target.exists():
                        previous = private / "previous" / source.name
                        previous.parent.mkdir(parents=True, exist_ok=True)
                        target.rename(previous)
                    shutil.copytree(source, target)
                else:
                    shutil.copyfile(source, target)
                visible.append(target)
            shutil.copyfile(combined, output_dir / "etl_receipts.parquet")
        if not getattr(self, "_defer_native_selection", False):
            remember_selection(
                self.output_dir or output_dir,
                [
                    SelectedDataset(
                        policy.dataset, tuple(by_dataset[policy.dataset]), combined, self.receipt_generation_id
                    )
                    for policy in self.receipt_policies
                ],
            )
        return tuple(visible) if self.outputs else visible[0]


class SubjectTableRollup(NativeReceiptLifecycle):
    """A Congress family whose tables publish no per-row receipts (ss-vse3; its policies are ``receipts_retired``).

    The builder runs as under :class:`SubjectReceiptRollup`, on the same selected priors and inputs: each published
    table read back as builder input, or, on the night its live generation still lists receipts for a field they
    alone held, restored through them (:class:`SelectedPriors`). Each output it writes is mapped to its public table
    by ``congress_tables.to_public``; nothing writes or reads a receipt of this family afterwards. A run refuses at
    its start, before any read, while ``docs/pins/receipt-archive.json`` does not pin the live generation that still
    lists this family's receipts (``publication.require_live_receipt_pins``).
    """

    def __init_subclass__(cls, **kwargs):
        from spicy_regs.subject_catalog import retired_receipt_datasets

        super().__init_subclass__(**kwargs)
        names = {str(key).removesuffix(".parquet") for key in (cls.outputs or (cls.output,))}
        if unretired := sorted(names - retired_receipt_datasets()):
            raise ValueError(f"{cls.__name__}: {unretired} still publish receipts; use SubjectReceiptRollup")

    def _prime(self, output_dir, snapshot=None):
        if snapshot is not None:
            from spicy_regs.sources import publication

            publication.require_live_receipt_pins(snapshot, [key.removesuffix(".parquet")
                                                             for key in (self.outputs or (self.output,))])
        return super()._prime(output_dir, snapshot)

    def build_tables(self, output_dir, builder, **kwargs):
        from loguru import logger

        from spicy_regs.congress_tables import summary, to_public

        private, prior = self._receipt_work(output_dir)
        work = private / "source-output"
        work.mkdir()
        built = self._call_builder(prior, work, builder, kwargs)
        paths = built if isinstance(built, tuple) else (built,)
        keys = self.outputs or (self.output,)
        if {path.stem for path in paths} != {key.removesuffix(".parquet") for key in keys}:
            raise ValueError("Producer outputs differ from the complete classified family")
        visible = []
        for path in paths:
            with phase_log("public-table", dataset=path.stem):
                left_out, shown = to_public(path.stem, path, output_dir / path.name)
            if left_out:
                logger.info(summary(path.stem, left_out, shown))
            visible.append(output_dir / path.name)
        return tuple(visible) if self.outputs else visible[0]


class LegislativeTableRollup(NativeReceiptLifecycle):
    """A legislative-document family whose tables publish no per-row receipts (ss-vse3; policies ``receipts_retired``).

    The builder runs as under :class:`SubjectReceiptRollup`, on the same selected priors and inputs: each published
    table (and retired read-checkpoint outcome table) is read back as builder input via ``legislative_tables``. Each
    output it writes is mapped directly to its public Parquet table by ``legislative_tables.to_public``, with
    read-outcome and document-file-outcome tables written without ``etl_receipts.parquet`` or
    ``legislative_document_file_states``. A run refuses at its start, before any build work, while
    ``docs/pins/receipt-archive.json`` does not pin the live generation that still lists this family's receipts
    (``publication.require_live_receipt_pins``).
    """

    source_outputs: ClassVar[tuple[str, ...]]
    retired_receipt_datasets: ClassVar[tuple[str, ...]]

    def __init_subclass__(cls, **kwargs):
        from spicy_regs.subject_catalog import retired_receipt_datasets

        super().__init_subclass__(**kwargs)
        keys = cls.outputs or (cls.output,)
        if any(not isinstance(key, str) for key in keys):
            raise ValueError("Legislative table rollup requires explicit output names")
        cls.source_outputs = tuple(str(key) for key in keys)
        names = tuple(key.removesuffix(".parquet") for key in cls.source_outputs)
        registry = field_registry()
        if unknown := sorted(set(names) - set(registry)):
            raise ValueError(f"{cls.__name__}: {unknown} are not registered legislative-document datasets")
        processing_only = frozenset(name for name in names if registry[name]["processing_only"])
        outcomes = tuple(READ_TABLES[name] for name in names if name in READ_TABLES)
        family = getattr(cls, "publication_family", None) or cls.name
        if family in FILE_TABLES:
            outcomes += (FILE_TABLES[family],)
        retired_names = tuple(dict.fromkeys((*names, *outcomes)))
        if unretired := sorted(set(retired_names) - retired_receipt_datasets()):
            raise ValueError(f"{cls.__name__}: {unretired} still publish receipts; use LegislativeReceiptRollup")
        cls.retired_receipt_datasets = retired_names
        cls.receipt_policies = ()
        cls.receipt_only_tables = ()
        cls.added_tables = tuple(dict.fromkeys((*cls.added_tables, *(name + ".parquet" for name in outcomes))))
        visible = tuple(
            key for key in cls.source_outputs if key.removesuffix(".parquet") not in processing_only
        ) + tuple(name + ".parquet" for name in outcomes)
        if cls.outputs or outcomes:
            cls.outputs = visible
        elif visible:
            cls.output = visible[0]

    def generation_schemas(self):
        from spicy_regs.legislative_documents import subject_schema

        schemas = {}
        for key in self.outputs or (self.output,):
            dataset = key.removesuffix(".parquet")
            if dataset in NAVIGATION_OUTCOME_POLICIES:
                schemas[dataset] = described_schema(NAVIGATION_OUTCOME_POLICIES[dataset].subject_schema)
            else:
                schemas[dataset] = described_schema(subject_schema(dataset))
        return schemas

    def _prime(self, output_dir, snapshot=None):
        if snapshot is not None:
            from spicy_regs.sources import publication

            publication.require_live_receipt_pins(dict(snapshot), self.retired_receipt_datasets)
        return super()._prime(output_dir, snapshot)

    def build_tables(self, output_dir, builder, **kwargs):
        from spicy_regs import legislative_tables

        private, prior = self._receipt_work(output_dir)
        work = private / "source-output"
        work.mkdir()
        candidate = private / "candidate"
        candidate.mkdir()
        built = self._call_builder(prior, work, builder, kwargs)
        paths = built if isinstance(built, tuple) else (built,)
        observed = {path.name if path.is_dir() else path.stem for path in paths}
        if observed != {key.removesuffix(".parquet") for key in self.source_outputs}:
            raise ValueError("Producer outputs differ from the complete classified family")
        registry = field_registry()
        mapped_subjects: dict[str, list[str]] = {}
        source_outputs: list[tuple[str, Path]] = []
        subjects: list[Path] = []
        for path in paths:
            dataset = path.name if path.is_dir() else path.stem
            source_outputs.append((dataset, path))
            if registry[dataset]["processing_only"]:
                mapped_subjects[dataset] = []
            else:
                target = candidate / (dataset if path.is_dir() else f"{dataset}.parquet")
                with phase_log("public-table", dataset=dataset):
                    mapped_subjects[dataset] = legislative_tables.to_public(dataset, path, target)
                subjects.append(target)
            if dataset in READ_TABLES:
                outcome_name = READ_TABLES[dataset]
                outcome_target = candidate / f"{outcome_name}.parquet"
                with phase_log("read-outcomes", dataset=outcome_name):
                    legislative_tables.write_read_outcomes(
                        dataset, path, outcome_target, generation_id=self.receipt_generation_id
                    )
                mapped_subjects[outcome_name] = [f"{outcome_name}.parquet"]
                subjects.append(outcome_target)
        family = getattr(self, "publication_family", None) or self.name
        if file_table := FILE_TABLES.get(family):
            file_target = candidate / f"{file_table}.parquet"
            with phase_log("file-outcomes", dataset=file_table):
                legislative_tables.write_file_outcomes(
                    file_table,
                    source_outputs,
                    mapped_subjects,
                    candidate,
                    file_target,
                    generation_id=self.receipt_generation_id,
                )
            subjects.append(file_target)
        visible: list[Path] = []
        with phase_log("visible-copies", files=len(subjects)):
            for source in subjects:
                target = output_dir / source.name
                if source.is_dir():
                    if target.exists():
                        previous = private / "previous" / source.name
                        previous.parent.mkdir(parents=True, exist_ok=True)
                        target.rename(previous)
                    shutil.copytree(source, target)
                else:
                    shutil.copyfile(source, target)
                visible.append(target)
        return tuple(visible) if self.outputs else visible[0]
