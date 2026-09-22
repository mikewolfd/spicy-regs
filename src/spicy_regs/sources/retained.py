"""Thin observers of SpicyDocs acquisition results, before later validation."""

from spicy_docs.sources.govinfo.body_acquisition import GovInfoBodyAcquirer
from spicy_docs.sources.govinfo.discovery import GovInfoDiscoveryReader
from spicy_docs.sources.congress.listing import CongressListingReader
from spicy_docs.sources.legislators import LegislatorsAcquirer

from spicy_regs.source_evidence import CaptureEvidence


class RetainedLegislatorsAcquirer(LegislatorsAcquirer):
    def __init__(self, *, evidence: CaptureEvidence, **kwargs):
        super().__init__(**kwargs)
        self.evidence = evidence

    def _acquire(self, url: str, operation: str, max_bytes: int):
        result = super()._acquire(url, operation, max_bytes)
        # acquire_current's no-LIS postcondition runs after this method.
        self.evidence.capture(result.capture, stage=operation)
        return result


class RetainedGovInfoBodyAcquirer(GovInfoBodyAcquirer):
    def __init__(self, *, evidence: CaptureEvidence, **kwargs):
        super().__init__(**kwargs)
        self.evidence = evidence
        self.evidence.credential = self._credential

    def _capture(self, url: str, *, keyed: bool, max_bytes: int):
        capture = super()._capture(url, keyed=keyed, max_bytes=max_bytes)
        self.evidence.capture(capture, stage="govinfo-response")
        return capture


class RetainedGovInfoDiscoveryReader(GovInfoDiscoveryReader):
    def __init__(self, *, evidence: CaptureEvidence, **kwargs):
        super().__init__(**kwargs)
        self.evidence = evidence
        self.evidence.credential = self._key or ""

    def page(self, url: str, **kwargs):
        page = super().page(url, **kwargs)
        # pages() can reject count drift/overflow before yielding this page.
        self.evidence.capture(page.capture, stage="govinfo-listing-response")
        return page


class RetainedCongressListingReader(CongressListingReader):
    def __init__(self, *, evidence: CaptureEvidence, **kwargs):
        super().__init__(**kwargs)
        self.evidence = evidence
        self.evidence.credential = self._key or ""

    def page(self, url: str, **kwargs):
        page = super().page(url, **kwargs)
        self.evidence.capture(page.capture, stage="congress-listing-response")
        return page
