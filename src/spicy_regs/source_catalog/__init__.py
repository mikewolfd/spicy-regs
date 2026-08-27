"""Producer machinery for the sealed ``SourceCatalogRelease`` v1 record.

Rulespec Core owns the wire schemas; SpicyRegs owns the records they carry
(REF-024).  The schema bytes are pinned byte-identically under
``spicy_regs/fixtures/rulespec/source-catalog-release-v1/`` and read through a
product-local reader: no import of, and no path into, the sibling checkout.

The universe this catalog covers arrives as configuration.  Nothing in this
package names a corpus, an agency, a docket, or a date window; a
:class:`~spicy_regs.source_catalog.universe.UniverseSpec` supplies all of it and
digests itself into the release's selection policy.
"""

from spicy_regs.source_catalog.discovery import (
    CandidateRendition,
    DiscoveredItem,
    NormalizedDraft,
    Observation,
    ObservedTopic,
    SourceOutcome,
)
from spicy_regs.source_catalog.records import (
    FORMAT,
    FORMAT_VERSION,
    RELEASE_ID_PREFIX,
    SELECTION_DISPOSITIONS,
    release_identity,
    set_digest,
)
from spicy_regs.source_catalog.release import (
    SourceCatalogBundle,
    build_source_catalog_release,
    publish_source_catalog_release,
    select_source_items,
)
from spicy_regs.source_catalog.schema_pins import (
    PINNED_SCHEMA_DIR,
    SCHEMA_ROLES,
    pinned_schema_bytes,
    pinned_schemas,
    schema_set_identity,
)
from spicy_regs.source_catalog.published_catalog import (
    discover_published_catalog,
    discovered_item,
)
from spicy_regs.source_catalog.universe import (
    COMPLETE_NATIVE_METADATA_PROFILE,
    NormalizationPolicy,
    PinnedSource,
    PublicationWindow,
    SampleCandidate,
    SamplePolicy,
    SourceCatalogError,
    UniverseScope,
    UniverseSpec,
    composite_source_version,
    load_universe_spec,
)
from spicy_regs.source_catalog.validate import validate_bundle_records
from spicy_regs.source_catalog.verify import verify_bundle_directory

__all__ = [
    "COMPLETE_NATIVE_METADATA_PROFILE",
    "FORMAT",
    "FORMAT_VERSION",
    "PINNED_SCHEMA_DIR",
    "RELEASE_ID_PREFIX",
    "SCHEMA_ROLES",
    "SELECTION_DISPOSITIONS",
    "CandidateRendition",
    "DiscoveredItem",
    "NormalizationPolicy",
    "NormalizedDraft",
    "Observation",
    "ObservedTopic",
    "PinnedSource",
    "PublicationWindow",
    "SampleCandidate",
    "SamplePolicy",
    "SourceCatalogBundle",
    "SourceCatalogError",
    "SourceOutcome",
    "UniverseScope",
    "UniverseSpec",
    "build_source_catalog_release",
    "composite_source_version",
    "discover_published_catalog",
    "discovered_item",
    "load_universe_spec",
    "pinned_schema_bytes",
    "pinned_schemas",
    "publish_source_catalog_release",
    "release_identity",
    "schema_set_identity",
    "select_source_items",
    "set_digest",
    "validate_bundle_records",
    "verify_bundle_directory",
]
