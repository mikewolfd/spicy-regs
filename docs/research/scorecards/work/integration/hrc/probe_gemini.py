"""Retain HRC page outcomes using the existing bounded Gemini extraction path.

Run from the SpicyDocs environment; outputs contain private source/model bytes.
The observed 118th Congress PDF is fixed by SHA-256. This is a qualification
probe, not a production reader or an edition completeness assertion.
"""

from __future__ import annotations

import argparse
import json
import pickle
import time
from contextlib import nullcontext
from hashlib import sha256
from ipaddress import ip_address
from pathlib import Path

from spicy_docs.extraction import DefaultReader, DocumentExtractor, FullPage
from spicy_docs.extraction.gemini import Gemini, GeminiClient
from spicy_docs.extraction.model import ExtractionError
from spicy_docs.sources.scorecards.hrc_outcomes import SCHEMA_VERSION, outcome_prompt, outcome_schema, validate_outcome
from spicy_docs.transport.credentials import CredentialRefusedError, read_api_key
from spicy_docs.transport.download import BoundedAcquirer

SOURCE_SHA256 = "804c1157e71da53a00821fdcaf15a1790448c682eb49d44613a577cfce9cb855"
GEMINI_HOST = "generativelanguage.googleapis.com"
DNS_URL = "https://dns.google/resolve?name=generativelanguage.googleapis.com&type=A"


def resolved_client(address: str, output: Path):
    """Explicit diagnostic DNS override; original TLS hostname remains verified."""
    import httpx

    if not ip_address(address).is_global:
        raise ValueError("diagnostic address must be a public IP")

    def dns_url(url: str) -> str:
        if url != DNS_URL:
            raise ValueError("unexpected diagnostic DNS URL")
        return url

    with BoundedAcquirer(validate_url=dns_url, max_requests=1, timeout=30) as acquirer:
        captured = acquirer.capture(DNS_URL, max_bytes=32768)
    (output / "dns-response.json").write_bytes(captured.body)
    dns = json.loads(captured.body)
    answers = [
        row for row in dns.get("Answer", []) if row.get("type") == 1 and row.get("name", "").rstrip(".") == GEMINI_HOST
    ]
    if dns.get("Status") != 0 or not any(row.get("data") == address and row.get("TTL", 0) > 0 for row in answers):
        raise ValueError("address is not in the current original-host DNS answer")
    (output / "dns-receipt.json").write_text(
        json.dumps(
            {
                "url": DNS_URL,
                "sha256": captured.sha256,
                "observed_at": captured.observed_at,
                "selected_ip": address,
                "original_host": GEMINI_HOST,
                "tls_verification": "default verification with original SNI hostname",
                "credential_sent_to_dns": False,
            },
            indent=2,
        )
    )

    class ResolvedTransport(httpx.BaseTransport):
        def __init__(self):
            self.inner = httpx.HTTPTransport()

        def handle_request(self, request):
            if request.url.host != GEMINI_HOST or request.url.scheme != "https" or request.url.port not in (None, 443):
                raise ValueError("diagnostic transport accepts only the original Gemini HTTPS host")
            request.headers["Host"] = GEMINI_HOST
            request.extensions["sni_hostname"] = GEMINI_HOST
            request.url = request.url.copy_with(host=address)
            return self.inner.handle_request(request)

        def close(self):
            self.inner.close()

    return httpx.Client(transport=ResolvedTransport(), follow_redirects=False, trust_env=False)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--credentials", type=Path, required=True)
    parser.add_argument("--credential-name", default="GEMINI_API_KEY")
    parser.add_argument("--pages", type=int, nargs="+", required=True)
    parser.add_argument("--model", default="gemini-3.8-flash")
    parser.add_argument(
        "--media-resolution", choices=("MEDIA_RESOLUTION_LOW", "MEDIA_RESOLUTION_MEDIUM", "MEDIA_RESOLUTION_HIGH")
    )
    parser.add_argument("--thinking-level", choices=("low", "medium", "high"), default="low")
    parser.add_argument("--resolve-ip", help="Explicit diagnostic IP verified through fresh original-host HTTPS DNS")
    args = parser.parse_args()
    body = args.source.read_bytes()
    if sha256(body).hexdigest() != SOURCE_SHA256:
        parser.error("source is not the checked HRC 118th PDF")
    if len(set(args.pages)) != len(args.pages):
        parser.error("duplicate requested pages")
    for number in args.pages:
        outcome_schema(number)
    args.output.mkdir(parents=True, exist_ok=False)
    receipts = []
    credential = read_api_key(args.credentials, args.credential_name)
    transport_client = resolved_client(args.resolve_ip, args.output) if args.resolve_ip else None
    with (
        transport_client or nullcontext(),
        GeminiClient(api_key=credential, timeout=240, http_client=transport_client) as client,
    ):
        for number in args.pages:
            started = time.monotonic()
            generation = {
                "maxOutputTokens": 32768,
                "temperature": 0,
                "thinkingConfig": {"thinkingLevel": args.thinking_level},
            }
            if args.media_resolution:
                generation["mediaResolution"] = args.media_resolution
            backend = Gemini(
                client,
                model=args.model,
                mode="structured",
                outcome_schema=outcome_schema(number),
                prompt=outcome_prompt(number),
                generation=generation,
            )
            extractor = DocumentExtractor(FullPage(backend), reader=DefaultReader(dpi=200))
            try:
                pages = list(extractor.extract(body, media_type="application/pdf", pages=[number]))
                if len(pages) != 1:
                    raise ValueError("expected one physical page")
                page = pages[0]
                with (args.output / f"page-{number}.pickle").open("wb") as stream:
                    pickle.dump(page, stream)
                observation = page.content.observations[0]
                outcome = observation.raw["outcome"]
                (args.output / f"page-{number}.json").write_text(json.dumps(outcome, ensure_ascii=False, indent=2))
                validate_outcome(number, outcome)
                receipt = {
                    "page": number,
                    "source_sha256": SOURCE_SHA256,
                    "source_page_count": page.metadata["page_count"],
                    "model": args.model,
                    "outcome_schema_version": SCHEMA_VERSION,
                    "status": "schema_validated_not_source_qualified",
                    "member_count": len(outcome.get("members", [])),
                    "item_count": len(outcome.get("scored_items", [])),
                    "uncertainty_count": len(outcome["uncertainties"]),
                    "seconds": round(time.monotonic() - started, 2),
                }
            except (ExtractionError, CredentialRefusedError, ValueError) as error:
                (args.output / f"page-{number}-error.txt").write_text(str(error))
                details = getattr(error, "details", None)
                if details:
                    with (args.output / f"page-{number}-failure.pickle").open("wb") as stream:
                        pickle.dump(details, stream)
                receipt = {"page": number, "status": "refused", "error_type": type(error).__name__}
                receipts.append(receipt)
                (args.output / "receipt.json").write_text(json.dumps(receipts, indent=2))
                print(json.dumps(receipt), flush=True)
                raise SystemExit(1) from None
            receipts.append(receipt)
            (args.output / "receipt.json").write_text(json.dumps(receipts, indent=2))
            print(json.dumps(receipt), flush=True)


if __name__ == "__main__":
    main()
