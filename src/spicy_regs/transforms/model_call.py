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

import functools
import json
import os
from collections.abc import Callable, Mapping
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


def survive_refused_answer[T](
    seam: Callable[..., T],
    *,
    empty: T,
    refused: Callable[[ModelCallError], None],
) -> Callable[..., T]:
    """Wrap one interpretation seam so a refused answer costs its own rows, not the run.

    ``classify_sections``, ``summarize_bill`` and ``summarize_diff`` raise
    ``ModelCallError`` when the answer does not satisfy the shape their prompt
    asked for — the measured failure spicy-docs 0.21.3's ``AnswerField``
    declaration exists for, where the first live call came back spelling
    ``audience`` as ``most_affected_audience``. All three call the model
    *outside* ``interpretation.bill_family``'s own refusal guard, which only
    wraps the row shapers, so without this the exception leaves
    ``build_bill_family`` and aborts the whole rollup: one bad answer about one
    printing would cost every status-derived row of every bill in the run. An
    unstated key set was one defect; losing a run to it is another.

    ``empty`` is what the generator's caller already reads as "nothing to
    store" — ``()`` for the classifier, ``None`` for the two summarizers —
    so ``bill_family`` files its own named refusal and the remaining tables
    are published as usual. It files the summarizer's under the one reason it
    has for a ``None``, "its text is below the minimum it will summarize",
    which is not why this one came back empty; spicy-regs therefore logs the
    answer's real refusal itself, and only the count reaches anything
    retained. Naming the true reason in the record is spicy-docs' to do, by
    catching ``ModelCallError`` in ``_summarize_version``.

    Only ``ModelCallError`` is caught, and it is the narrowest catch that
    works: ``GeminiClient`` raises ``CredentialRefusedError`` on 401/403 and
    ``ExtractionError`` on any other HTTP status, and neither is an answer.
    A credential refusal must abort, and an empty model table must never be
    what a 502 looks like.
    """

    @functools.wraps(seam)
    def guarded(*args: Any, **kwargs: Any) -> T:
        try:
            return seam(*args, **kwargs)
        except ModelCallError as error:
            refused(error)
            return empty

    return guarded
