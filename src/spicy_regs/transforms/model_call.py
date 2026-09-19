"""Adapter: a Gemini client as the ``ModelCall`` seam the interpretation package takes.

``spicy_docs.extraction.gemini.GeminiClient`` satisfies ``GenerationClient``
(``generate(model, body) -> dict``), which is the raw transport. The two
model-backed interpretation modules take a different, narrower seam —
``spicy_docs.interpretation.model_call.ModelCall``, which is
``(*, model, prompt) -> ModelResponse`` with the answer already parsed. The two
do not fit directly, so this is the adapter between them: it builds the request
body, asks for JSON back, pulls the answer text out of the candidate envelope
and parses it, and carries the token counts the summary tables publish.

Keeping it here rather than in the transform means the bill family reads as the
bill family, and this can be tested without a model.

The key never appears in a log line or a row: it is read from the environment
by :func:`resolve_gemini_key` and handed to the client, which sends it as a
header.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from typing import Any

from spicy_docs.extraction.gemini import GenerationClient
from spicy_docs.interpretation.model_call import ModelCall, ModelCallError, ModelResponse

#: Checked in order; the first non-empty value wins. A run with none of them
#: set skips the model-backed tables rather than failing.
GEMINI_KEY_ENV_VARS = ("GEMINI_API_KEY", "GOOGLE_API_KEY")

DEFAULT_MODEL = "gemini-3.8-flash"


def resolve_gemini_key() -> str | None:
    """The first Gemini key set in :data:`GEMINI_KEY_ENV_VARS`, or None."""
    for name in GEMINI_KEY_ENV_VARS:
        value = os.environ.get(name)
        if value:
            return value
    return None


def _answer_text(payload: Mapping[str, Any]) -> str:
    """The single text part of the first candidate, or a refusal naming what came back."""
    candidates = payload.get("candidates")
    if not isinstance(candidates, list) or not candidates:
        raise ModelCallError("model answer carried no candidates", details=payload)
    content = candidates[0].get("content") if isinstance(candidates[0], Mapping) else None
    parts = content.get("parts") if isinstance(content, Mapping) else None
    if not isinstance(parts, list) or not parts:
        raise ModelCallError("model answer carried no content parts", details=payload)
    texts = [part["text"] for part in parts if isinstance(part, Mapping) and isinstance(part.get("text"), str)]
    if not texts:
        raise ModelCallError("model answer carried no text", details=payload)
    return "".join(texts)


def model_call(client: GenerationClient, *, response_mime_type: str = "application/json") -> ModelCall:
    """Wrap a ``GenerationClient`` as a ``ModelCall``.

    ``client`` is any ``GenerationClient``: ``generate(model, body) -> Mapping``.
    The real ``GeminiClient`` satisfies it, and so does a stub in a test — the
    protocol is structural, which is the whole reason this seam exists.
    """

    def call(*, model: str, prompt: str) -> ModelResponse:
        body = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {"responseMimeType": response_mime_type},
        }
        payload = client.generate(model, body)
        if not isinstance(payload, Mapping):
            raise ModelCallError("model answer was not a mapping", details=payload)
        text = _answer_text(payload)
        try:
            data = json.loads(text)
        except json.JSONDecodeError as error:
            # The text is deliberately not in the message: it is model output,
            # and a prompt echo in a log is how a document leaks into a log.
            raise ModelCallError(f"model answer was not JSON: {error.msg}") from error
        usage = payload.get("usageMetadata")
        usage = usage if isinstance(usage, Mapping) else {}
        return ModelResponse(
            data=data,
            input_tokens=usage.get("promptTokenCount"),
            output_tokens=usage.get("candidatesTokenCount"),
        )

    return call
