"""The model-facing vocabulary path reads a local interface, not RefSpec code."""

from __future__ import annotations

import builtins
import json
import subprocess
import sys
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from spicy_regs.candidate_release import (
    CandidateReleaseError,
    CandidateSelectionReceipt,
    VocabularyAtlasCandidateSource,
)
from spicy_regs.docpipeline.rkaf_projection import (
    candidate_release_vocabulary,
)
from tests.atlas_candidate_support import (
    FACET,
    RELEASE_DIGEST,
    ROLE,
    ROUTE,
    canonical as _canonical,
    digest as _digest,
    open_atlas as _open_atlas,
    write_atlas as _write_atlas,
)

ASSET_DIGEST = "a" * 64


@dataclass(frozen=True)
class _Member:
    member_iri: str
    release_iri: str
    scheme_iri: str
    record: Mapping[str, Any]


@dataclass(frozen=True)
class _Expression:
    member_iri: str
    original_literal: str
    language_tag: str | None
    semantic_property_iri: str


class _PublishedFileReader:
    """Small stand-in for a product-local reader of published release files."""

    usage_ceiling = "candidateUseOnly"
    lookup_index_manifest = {
        "id": "urn:test:lookup-index:subjects:v1",
        "digest": "sha256:" + "4" * 64,
    }

    def __init__(self) -> None:
        self._member = _Member(
            member_iri="urn:test:concept:poultry-inspection",
            release_iri="urn:test:release:subjects:v1",
            scheme_iri="urn:test:scheme:subjects",
            record={
                "@id": "urn:test:concept:poultry-inspection",
                "@type": "skos:Concept",
            },
        )
        self._expressions = (
            _Expression(
                member_iri=self._member.member_iri,
                original_literal="Poultry inspection",
                language_tag="en",
                semantic_property_iri=("http://www.w3.org/2004/02/skos/core#prefLabel"),
            ),
            _Expression(
                member_iri=self._member.member_iri,
                original_literal="Slaughter inspection",
                language_tag="en",
                semantic_property_iri=("http://www.w3.org/2004/02/skos/core#altLabel"),
            ),
        )
        self.candidate_selection = CandidateSelectionReceipt(
            source_asset={
                "type": "VocabularyAtlasAsset",
                "assetId": "urn:ref:vocabulary-atlas:" + ASSET_DIGEST,
                "manifestDigest": "sha256:" + "5" * 64,
                "outputDigest": "sha256:" + "6" * 64,
            },
            resource_route="document",
            reference_resource_release={
                "id": self._member.release_iri,
                "digest": "sha256:" + "1" * 64,
            },
            facet_iri=FACET,
            assignment_role_iri=ROLE,
        )

    def lookup_member(self, member_iri: str) -> _Member | None:
        return self._member if member_iri == self._member.member_iri else None

    def iter_expressions(
        self,
        *,
        member_iri: str | None = None,
    ) -> Iterator[_Expression]:
        for expression in self._expressions:
            if member_iri is None or expression.member_iri == member_iri:
                yield expression


def test_model_path_import_does_not_require_refspec() -> None:
    script = """
import builtins
original_import = builtins.__import__
def reject_refspec(name, *args, **kwargs):
    if name == 'refspec' or name.startswith('refspec.'):
        raise AssertionError(f'model path imported {name}')
    return original_import(name, *args, **kwargs)
builtins.__import__ = reject_refspec
from spicy_regs.docpipeline.rkaf_projection import candidate_release_vocabulary
assert callable(candidate_release_vocabulary)
"""

    result = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_ordinary_enrichment_imports_do_not_require_refspec() -> None:
    script = """
import builtins
original_import = builtins.__import__
def reject_refspec(name, *args, **kwargs):
    if name == 'refspec' or name.startswith('refspec.'):
        raise AssertionError(f'ordinary enrichment imported {name}')
    return original_import(name, *args, **kwargs)
builtins.__import__ = reject_refspec
import spicy_regs.enrichment as enrichment
from spicy_regs.enrichment.experiment_artifacts import DEVELOPMENT_DECISIONS
assert callable(enrichment.select_connected_candidate_concepts)
assert DEVELOPMENT_DECISIONS == {'continue', 'investigate', 'stop'}
assert not hasattr(enrichment, 'ManagedReleaseCandidateSource')
assert not hasattr(enrichment, 'authorize_managed_accepted_assignment')
"""

    result = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_candidate_vocabulary_accepts_file_reader_without_importing_refspec(
    monkeypatch,
) -> None:
    reader = _PublishedFileReader()
    original_import = builtins.__import__

    def reject_refspec(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "refspec" or name.startswith("refspec."):
            raise AssertionError(f"candidate lookup imported {name}")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", reject_refspec)

    vocabulary = candidate_release_vocabulary(
        reader,
        default_language="en",
    )

    assert tuple(vocabulary.concepts) == ("urn:test:concept:poultry-inspection",)
    assert vocabulary.selector_rows == (
        {
            "concept_id": "urn:test:concept:poultry-inspection",
            "facet": "subject",
            "source_vocabulary": "urn:test:scheme:subjects",
            "scheme": "subject",
            "pref_label": "Poultry inspection",
            "alt_labels_json": '["Slaughter inspection"]',
            "definition": "",
            "status": "active",
            "external_ids_json": "[]",
        },
    )


def test_atlas_reader_supplies_only_selected_release_candidates(
    tmp_path: Path,
) -> None:
    root = tmp_path / "atlas"
    pins = _write_atlas(root)

    source = _open_atlas(root, pins)
    vocabulary = candidate_release_vocabulary(
        source,
        default_language="en",
    )

    assert source.usage_ceiling == "diagnosticCandidateOnly"
    assert source.candidate_selection.reference_resource_release == {
        "id": pins["release_id"],
        "digest": pins["release_digest"],
    }
    assert tuple(vocabulary.concepts) == (pins["member_id"],)
    concept = vocabulary.concepts[pins["member_id"]]
    assert concept.preferred_labels == {"en": "Poultry inspection"}
    assert concept.alternate_labels == {"en": "Slaughter inspection"}
    assert concept.hidden_labels == {"en": "Bird inspection"}
    assert concept.definitions == {"en": "Inspection of poultry processing."}
    assert not vocabulary.candidate_mappings


def test_atlas_reader_rejects_an_external_output_pin_mismatch(
    tmp_path: Path,
) -> None:
    root = tmp_path / "atlas"
    pins = _write_atlas(root)

    with pytest.raises(CandidateReleaseError, match="selected pin"):
        VocabularyAtlasCandidateSource.open(
            root / "atlas-manifest.json",
            expected_asset_id=pins["asset_id"],
            expected_manifest_digest=pins["manifest_digest"],
            expected_output_digest="sha256:" + "f" * 64,
            reference_release_id=pins["release_id"],
            reference_release_digest=pins["release_digest"],
            facet_iri=FACET,
            assignment_role_iri=ROLE,
            resource_route=ROUTE,
            lookup_index_manifest={
                "id": "urn:test:lookup-index:atlas:v1",
                "digest": "sha256:" + "6" * 64,
            },
        )


def test_atlas_reader_rejects_boolean_semantic_counts(tmp_path: Path) -> None:
    root = tmp_path / "atlas"
    pins = _write_atlas(root)
    manifest_path = root / "atlas-manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    manifest["counts"]["feedback"] = False
    manifest.pop("canonicalPayloadDigest")
    manifest["canonicalPayloadDigest"] = _digest(_canonical(manifest))
    manifest_raw = _canonical(manifest) + b"\n"
    manifest_path.write_bytes(manifest_raw)
    pins["manifest_digest"] = _digest(manifest_raw)

    with pytest.raises(CandidateReleaseError, match="nonnegative integers"):
        _open_atlas(root, pins)


def test_atlas_reader_rejects_noncanonical_nquads_spelling(tmp_path: Path) -> None:
    root = tmp_path / "atlas"
    pins = _write_atlas(root)
    nquads_path = root / "atlas.nq"
    nquads = nquads_path.read_bytes().replace(
        b'"Poultry inspection"@en',
        b'"\\u0050oultry inspection"@en',
    )
    assert nquads != nquads_path.read_bytes()
    nquads_path.write_bytes(nquads)

    manifest_path = root / "atlas-manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    manifest["output"].update(
        {
            "digest": _digest(nquads),
            "byteLength": len(nquads),
        }
    )
    manifest.pop("canonicalPayloadDigest")
    manifest["canonicalPayloadDigest"] = _digest(_canonical(manifest))
    manifest_raw = _canonical(manifest) + b"\n"
    manifest_path.write_bytes(manifest_raw)
    pins["manifest_digest"] = _digest(manifest_raw)
    pins["output_digest"] = _digest(nquads)

    with pytest.raises(CandidateReleaseError, match="bytes are not canonical"):
        _open_atlas(root, pins)


def test_atlas_reader_cannot_be_hand_constructed() -> None:
    with pytest.raises(
        CandidateReleaseError,
        match="must be opened from pinned files",
    ):
        VocabularyAtlasCandidateSource(
            candidate_selection=CandidateSelectionReceipt(
                source_asset={"type": "VocabularyAtlasAsset"},
                reference_resource_release={
                    "id": "urn:test:release",
                    "digest": RELEASE_DIGEST,
                },
                facet_iri=FACET,
                assignment_role_iri=ROLE,
                resource_route=ROUTE,
            ),
            lookup_index_manifest={
                "id": "urn:test:index",
                "digest": "sha256:" + "6" * 64,
            },
            members={},
            expressions=(),
            _verification_token=object(),
        )


def test_atlas_candidate_execution_blocks_all_refspec_imports(
    tmp_path: Path,
) -> None:
    root = tmp_path / "atlas"
    pins = _write_atlas(root)
    script = f"""
import builtins
original_import = builtins.__import__
def reject_refspec(name, *args, **kwargs):
    if name == 'refspec' or name.startswith('refspec.'):
        raise AssertionError(f'candidate execution imported {{name}}')
    return original_import(name, *args, **kwargs)
builtins.__import__ = reject_refspec
from spicy_regs.candidate_release import VocabularyAtlasCandidateSource
from spicy_regs.docpipeline.rkaf_projection import candidate_release_vocabulary
source = VocabularyAtlasCandidateSource.open(
    {str(root / "atlas-manifest.json")!r},
    expected_asset_id={pins["asset_id"]!r},
    expected_manifest_digest={pins["manifest_digest"]!r},
    expected_output_digest={pins["output_digest"]!r},
    reference_release_id={pins["release_id"]!r},
    reference_release_digest={pins["release_digest"]!r},
    facet_iri={FACET!r},
    assignment_role_iri={ROLE!r},
    resource_route={ROUTE!r},
    lookup_index_manifest={{'id': 'urn:test:index', 'digest': 'sha256:' + '6' * 64}},
)
vocabulary = candidate_release_vocabulary(source, default_language='en')
assert tuple(vocabulary.concepts) == ({pins["member_id"]!r},)
"""

    result = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
