"""Hermetic tests for the Gemini-to-ModelCall adapter.

No network and no key: the client is a stub that returns whatever envelope the
case needs. What matters here is that the adapter satisfies the protocol the
interpretation package actually calls, and that a malformed answer is refused
rather than passed on as data.
"""

from __future__ import annotations

from typing import Any

import pytest
from spicy_docs.interpretation.model_call import ModelCallError

from spicy_regs.transforms.model_call import model_call, resolve_gemini_key


class StubClient:
    def __init__(self, payload):
        self.payload = payload
        self.calls: list[tuple[str, dict]] = []

    def generate(self, model, body):
        self.calls.append((model, body))
        return self.payload


def _envelope(text: str, *, usage: dict | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"candidates": [{"content": {"parts": [{"text": text}]}}]}
    if usage is not None:
        payload["usageMetadata"] = usage
    return payload


def test_the_answer_is_parsed_into_data():
    client = StubClient(_envelope('{"summary": "ok", "audience": "everyone"}'))
    response = model_call(client)(model="m", prompt="p")
    assert response.data == {"summary": "ok", "audience": "everyone"}


def test_token_counts_are_carried_through():
    """bill_summaries publishes these two columns, so they must survive the hop."""
    client = StubClient(_envelope("{}", usage={"promptTokenCount": 11, "candidatesTokenCount": 7}))
    response = model_call(client)(model="m", prompt="p")
    assert (response.input_tokens, response.output_tokens) == (11, 7)


def test_absent_token_counts_are_none_not_zero():
    """A missing count is unknown; zero would be a measurement nobody made."""
    response = model_call(StubClient(_envelope("{}")))(model="m", prompt="p")
    assert response.input_tokens is None
    assert response.output_tokens is None


def test_the_prompt_and_model_reach_the_client():
    client = StubClient(_envelope("{}"))
    model_call(client)(model="gemini-x", prompt="the prompt")
    model, body = client.calls[0]
    assert model == "gemini-x"
    assert body["contents"][0]["parts"][0]["text"] == "the prompt"
    assert body["generationConfig"]["responseMimeType"] == "application/json"


def test_a_list_answer_is_accepted():
    """classify_sections asks for rows, so a top-level list is a valid answer."""
    response = model_call(StubClient(_envelope('[{"sectionId": "a"}]')))(model="m", prompt="p")
    assert response.data == [{"sectionId": "a"}]


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"candidates": []},
        {"candidates": [{}]},
        {"candidates": [{"content": {"parts": []}}]},
        {"candidates": [{"content": {"parts": [{"inlineData": "x"}]}}]},
    ],
    ids=["empty", "no-candidates", "no-content", "no-parts", "no-text"],
)
def test_a_malformed_envelope_is_refused(payload):
    with pytest.raises(ModelCallError):
        model_call(StubClient(payload))(model="m", prompt="p")


def test_a_non_json_answer_is_refused_without_echoing_it():
    """The answer is model output; a prompt echo in a log is how a document leaks."""
    client = StubClient(_envelope("I'm afraid I can't do that"))
    with pytest.raises(ModelCallError) as caught:
        model_call(client)(model="m", prompt="p")
    assert "I'm afraid" not in str(caught.value)


def test_no_key_set_resolves_to_none(monkeypatch):
    """A keyless run must skip the model tables, not fail and not guess."""
    for name in ("GEMINI_API_KEY", "GOOGLE_API_KEY"):
        monkeypatch.delenv(name, raising=False)
    assert resolve_gemini_key() is None


def test_the_first_set_key_wins(monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "first")
    monkeypatch.setenv("GOOGLE_API_KEY", "second")
    assert resolve_gemini_key() == "first"
