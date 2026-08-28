"""One small published-atlas fixture shared by SpicyRegs consumer tests."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from spicy_regs.candidate_release import VocabularyAtlasCandidateSource

FACET = "urn:ref:facet:general-subject"
ROLE = "https://rulespec.org/ns/v1#assignmentPrimary"
ROUTE = "document"
RELEASE_DIGEST = "sha256:" + "b" * 64


def canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def digest(raw: bytes) -> str:
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def write_atlas(root: Path) -> dict[str, str]:
    implementation = {
        "id": "urn:test:implementation:atlas-reader-fixture",
        "version": "1.0",
        "sourceModules": [
            {
                "path": "fixture/generator",
                "digest": "sha256:" + "1" * 64,
            }
        ],
        "runtime": {"fixture": "1"},
    }
    inputs = [
        {
            "role": "ManagedReleaseView",
            "manifestDigest": "sha256:" + "2" * 64,
            "publicationReleaseId": "urn:test:publication:subjects:v1",
            "rulespecGraph": {
                "id": "urn:test:rulespec-graph:subjects:v1",
                "digest": "sha256:" + "3" * 64,
            },
        },
        {
            "role": "RulespecCoreRelease",
            "fileDigest": "sha256:" + "4" * 64,
            "releaseId": "urn:rulespec:core:" + "5" * 64,
            "releaseDigest": "sha256:" + "5" * 64,
        },
    ]
    policies = {
        "releaseFacts": "copiedManagedReleaseFactsOnly",
        "analysis": "replaceableMachineAnalysis",
        "labelEquality": "clusterOnly",
        "mappingEligibility": "twoIndependentMachinesSearchOnly",
        "humanFeedback": "appendOnlyNonAuthorizing",
    }
    generation = {
        "format": "refspec-vocabulary-atlas-nquads-1.0",
        "inputs": inputs,
        "implementation": implementation,
        "policies": policies,
    }
    generation_digest = digest(canonical(generation))
    asset_id = "urn:ref:vocabulary-atlas:" + generation_digest.removeprefix("sha256:")
    release = "urn:test:release:subjects:v1"
    member = "urn:test:concept:poultry-inspection"
    scheme = "urn:test:scheme:subjects"
    release_graph = asset_id + ":release-facts"
    analysis_graph = asset_id + ":analysis"
    lines = sorted(
        [
            f"<{member}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://rulespec.org/ns/v1#RegisteredConcept> <{release_graph}> .",
            f"<{member}> <http://www.w3.org/2004/02/skos/core#inScheme> <{scheme}> <{release_graph}> .",
            f'<{member}> <http://www.w3.org/2004/02/skos/core#prefLabel> "Poultry inspection"@en <{release_graph}> .',
            f'<{member}> <http://www.w3.org/2004/02/skos/core#altLabel> "Slaughter inspection"@en <{release_graph}> .',
            f'<{member}> <http://www.w3.org/2004/02/skos/core#hiddenLabel> "Bird inspection"@en <{release_graph}> .',
            f'<{member}> <http://www.w3.org/2004/02/skos/core#definition> "Inspection of poultry processing."@en <{release_graph}> .',
            f"<{release}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://rulespec.org/ns/v1#ReferenceResourceRelease> <{release_graph}> .",
            f'<{release}> <https://rulespec.org/ns/v1#referenceReleaseDigest> "{RELEASE_DIGEST}" <{release_graph}> .',
            f"<{release}> <http://www.w3.org/ns/prov#hadMember> <{member}> <{release_graph}> .",
            f"<{member}> <https://refspec.org/ns/vocabulary-atlas/v1#memberOfRelease> <{release}> <{analysis_graph}> .",
            f"<{analysis_graph}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://refspec.org/ns/vocabulary-atlas/v1#ReplaceableAnalysis> <{analysis_graph}> .",
            f"<{analysis_graph}> <http://www.w3.org/ns/prov#wasDerivedFrom> <urn:test:publication:subjects:v1> <{analysis_graph}> .",
        ]
    )
    nquads = ("\n".join(lines) + "\n").encode()
    output_digest = digest(nquads)
    manifest: dict[str, Any] = {
        "id": asset_id,
        "type": "urn:ref:type:VocabularyAtlasManifest",
        "schemaVersion": "1.0",
        "format": "refspec-vocabulary-atlas-nquads-1.0",
        "generationDigest": generation_digest,
        "inputs": inputs,
        "implementation": implementation,
        "policies": policies,
        "graphs": [
            {
                "role": "releaseFacts",
                "id": release_graph,
                "quadCount": 9,
            },
            {
                "role": "analysis",
                "id": analysis_graph,
                "quadCount": 3,
            },
        ],
        "output": {
            "path": "atlas.nq",
            "mediaType": "application/n-quads",
            "digest": output_digest,
            "byteLength": len(nquads),
            "quadCount": 12,
        },
        "counts": {
            "managedReleases": 1,
            "releaseFacts": 9,
            "analysisFacts": 3,
            "labelClusters": 0,
            "mappingCandidates": 0,
            "searchOnlyMappings": 0,
            "machineValidations": 0,
            "feedback": 0,
        },
    }
    manifest["canonicalPayloadDigest"] = digest(canonical(manifest))
    manifest_raw = canonical(manifest) + b"\n"
    root.mkdir()
    (root / "atlas.nq").write_bytes(nquads)
    (root / "atlas-manifest.json").write_bytes(manifest_raw)
    return {
        "asset_id": asset_id,
        "manifest_digest": digest(manifest_raw),
        "output_digest": output_digest,
        "release_id": release,
        "release_digest": RELEASE_DIGEST,
        "member_id": member,
    }


def open_atlas(
    root: Path,
    pins: Mapping[str, str],
) -> VocabularyAtlasCandidateSource:
    return VocabularyAtlasCandidateSource.open(
        root / "atlas-manifest.json",
        expected_asset_id=pins["asset_id"],
        expected_manifest_digest=pins["manifest_digest"],
        expected_output_digest=pins["output_digest"],
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
